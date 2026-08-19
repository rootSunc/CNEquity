"""EastMoney ST / suspension status for trading_status dataset.

The suspension leg queries ``RPT_CUSTOM_SUSPEND_DATA_INTERFACE`` under the
2026-08 datacenter contract: the filter MUST carry ``(DATETIME='<D>')`` (single
quotes) plus ``(MARKET="<market>")`` (double quotes), and the report's output
columns were renamed (``STOP_DATE → SUSPEND_START_DATE``,
``RESUME_DATE → SUSPEND_END_TIME``). Five markets are queried and deduplicated
by ``SECURITY_CODE``; an all-empty batch raises rather than silently meaning
"no suspensions".
"""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cnequity.adapters.eastmoney.clist import clist_rows_to_symbols, fetch_clist_pages
from cnequity.adapters.eastmoney.datacenter import (
    EastMoneyDatacenterError,
    fetch_datacenter,
)
from cnequity.adapters.eastmoney.em_auth import EastMoneyClient
from cnequity.domain.symbols import format_symbol, infer_exchange_from_code, is_all_a_symbol

logger = logging.getLogger(__name__)

# Risk-warning board (ST / *ST), the fs behind quote.eastmoney.com st_board.
# Do NOT use all-A market fs here.
_ST_FS = "m:0+f:4,m:1+f:4"
_SUSPEND_REPORT = "RPT_CUSTOM_SUSPEND_DATA_INTERFACE"
# Only the columns the coverage decision needs; the report's extra metadata
# (SUSPEND_EXPIRE / REASON / PREDICT_RESUME_DATE / SECURITY_NAME_ABBR /
# TRADE_MARKET) is deliberately NOT stored in TRADING_STATUS_SCHEMA.
_SUSPEND_COLUMNS = "SECURITY_CODE,SUSPEND_START_DATE,SUSPEND_END_TIME"
# Market labels accepted by the datacenter contract. 深市A股 already contains
# 创业板 rows; the separate 创业板 query is kept for completeness and is empty
# when the superset market returned its rows.
_SUSPEND_MARKETS = ("沪市A股", "深市A股", "科创板", "创业板", "京市A股")
# Smaller pages: large pz on push2 often 502s (esp. overseas).
_ST_PAGE_SIZE = 100


def _exchange_from_code(code: str) -> str:
    return infer_exchange_from_code(code)


def _em_date(value: object) -> date | None:
    text = str(value or "").strip()
    if len(text) < 10:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _suspension_covers(item: dict, trade_date: date) -> bool:
    stop_date = _em_date(item.get("SUSPEND_START_DATE"))
    if stop_date is None or stop_date > trade_date:
        return False

    resume_raw = str(item.get("SUSPEND_END_TIME") or "").strip().lower()
    if not resume_raw or resume_raw == "null":
        return True
    resume_date = _em_date(resume_raw)
    return resume_date is not None and resume_date >= trade_date


def _fetch_st_symbols(client: EastMoneyClient) -> set[str]:
    """Current ST-tagged symbols via clist (push2 → push2delay failover)."""
    # An empty ST set is valid; a failed request is not. Treating transport or
    # malformed responses as an empty set silently labels every ST name as
    # normal, which is materially worse than failing the snapshot.
    rows = fetch_clist_pages(
        client,
        fields="f12,f13,f14",
        fs=_ST_FS,
        page_size=_ST_PAGE_SIZE,
    )

    symbols: set[str] = set()
    symbols.update(sym for sym, _item in clist_rows_to_symbols(rows))
    return symbols


def _fetch_suspended_symbols(client: EastMoneyClient, trade_date: date) -> set[str]:
    ds = trade_date.strftime("%Y-%m-%d")
    rows: list[dict] = []
    seen_codes: set[str] = set()
    empty_markets: list[str] = []
    for market in _SUSPEND_MARKETS:
        market_rows = fetch_datacenter(
            client,
            _SUSPEND_REPORT,
            _SUSPEND_COLUMNS,
            filter_expr=f"(DATETIME='{ds}')(MARKET=\"{market}\")",
        )
        if not market_rows:
            empty_markets.append(market)
            continue
        for item in market_rows:
            code = str(item.get("SECURITY_CODE", "")).strip().zfill(6)
            if not code or code in seen_codes:
                continue
            seen_codes.add(code)
            rows.append(item)

    if not rows:
        # A fully empty batch is ambiguous (server-side data not generated or a
        # transient 9201), so it must never be converted into "every symbol is
        # trading".
        raise EastMoneyDatacenterError(
            f"EastMoney suspension batch for {trade_date.isoformat()} is empty across all "
            f"markets ({', '.join(_SUSPEND_MARKETS)}); refusing to treat it as 'no suspensions'"
        )
    if empty_markets:
        logger.warning(
            "EastMoney suspension empty for market(s) %s on %s; using %d row(s) from the rest",
            ", ".join(empty_markets),
            ds,
            len(rows),
        )

    matching_rows = [item for item in rows if _suspension_covers(item, trade_date)]
    if rows and not matching_rows:
        raise RuntimeError(
            f"EastMoney suspension response contains no row covering {trade_date.isoformat()}"
        )
    symbols: set[str] = set()
    for item in matching_rows:
        code = str(item.get("SECURITY_CODE", "")).zfill(6)
        exch = _exchange_from_code(code)
        if is_all_a_symbol(code, exch):
            symbols.add(format_symbol(code, exch))
    return symbols


def fetch_trading_status_eastmoney(
    symbols: list[str],
    trade_date: date,
    *,
    client: EastMoneyClient | None = None,
    config=None,
) -> pl.DataFrame:
    owns = client is None
    if client is None:
        client = EastMoneyClient(min_interval=0.3, config=config)

    try:
        st_set = _fetch_st_symbols(client)
        suspended = _fetch_suspended_symbols(client, trade_date)

        rows = []
        for sym in symbols:
            if sym in suspended:
                rows.append(
                    {
                        "symbol": sym,
                        "trade_date": trade_date,
                        "is_trading": False,
                        "status": "suspended",
                    }
                )
            elif sym in st_set:
                rows.append(
                    {
                        "symbol": sym,
                        "trade_date": trade_date,
                        "is_trading": True,
                        "status": "st",
                    }
                )
            else:
                rows.append(
                    {
                        "symbol": sym,
                        "trade_date": trade_date,
                        "is_trading": True,
                        "status": "normal",
                    }
                )
        return pl.DataFrame(rows).unique(subset=["symbol", "trade_date"], keep="last")
    finally:
        if owns:
            client.close()
