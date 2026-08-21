"""EastMoney ST / suspension status for trading_status dataset."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.adapters.eastmoney.clist import clist_rows_to_symbols, fetch_clist_pages
from cnequity.adapters.eastmoney.datacenter import fetch_datacenter
from cnequity.adapters.eastmoney.em_auth import EastMoneyClient
from cnequity.domain.symbols import format_symbol, infer_exchange_from_code, is_all_a_symbol

# Risk-warning board (ST / *ST), the fs behind quote.eastmoney.com st_board.
# Do NOT use all-A market fs here.
_ST_FS = "m:0+f:4,m:1+f:4"
_SUSPEND_REPORT = "RPT_CUSTOM_SUSPEND_DATA_INTERFACE"
_SUSPEND_COLUMNS = "SECURITY_CODE,TRADE_MARKET,SUSPEND_START_DATE,PREDICT_RESUME_DATE"
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

    resume_raw = str(item.get("PREDICT_RESUME_DATE") or "").strip().lower()
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
    rows = fetch_datacenter(
        client,
        _SUSPEND_REPORT,
        _SUSPEND_COLUMNS,
        filter_expr=f'(MARKET="全部")(DATETIME=\'{ds}\')',
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
