"""EastMoney ST / suspension status for trading_status dataset."""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cn_market_lake.adapters.eastmoney.clist import fetch_clist_pages
from cn_market_lake.adapters.eastmoney.common import symbol_from_clist
from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient
from cn_market_lake.domain.symbols import format_symbol, is_all_a_symbol

logger = logging.getLogger(__name__)

# Risk-warning board (ST / *ST), the fs behind quote.eastmoney.com st_board.
# Do NOT use all-A market fs here.
_ST_FS = "m:0+f:4,m:1+f:4"
_SUSPEND_REPORT = "RPT_CUSTOM_SUSPEND_DATA_INTERFACE"
_DATACENTER = "https://datacenter-web.eastmoney.com/api/data/v1/get"
# Smaller pages: large pz on push2 often 502s (esp. overseas).
_ST_PAGE_SIZE = 100


def _exchange_from_code(code: str) -> str:
    if code.startswith(("60", "68")):
        return "SH"
    if code.startswith("92"):
        return "BJ"
    return "SZ"


def _fetch_st_symbols(client: EastMoneyClient) -> set[str]:
    """Current ST-tagged symbols via clist (push2 → push2delay failover)."""
    try:
        rows = fetch_clist_pages(
            client,
            fields="f12,f13,f14",
            fs=_ST_FS,
            page_size=_ST_PAGE_SIZE,
        )
    except Exception as exc:
        # Non-fatal: caller still labels the universe, just without ST flags.
        logger.warning("EastMoney ST list failed: %s", exc)
        return set()

    symbols: set[str] = set()
    for item in rows:
        sym = symbol_from_clist(str(item.get("f12", "")), int(item.get("f13") or 0))
        if sym:
            symbols.add(sym)
    return symbols


def _fetch_suspended_symbols(client: EastMoneyClient, trade_date: date) -> set[str]:
    symbols: set[str] = set()
    ds = trade_date.strftime("%Y-%m-%d")
    url = (
        f"{_DATACENTER}?reportName={_SUSPEND_REPORT}"
        f"&columns=SECURITY_CODE,TRADE_MARKET,STOP_DATE,RESUME_DATE"
        f"&pageSize=5000&pageNumber=1"
        f"&filter=(STOP_DATE<='{ds}')(RESUME_DATE>='{ds}'~RESUME_DATE='null')"
    )
    try:
        resp = client.get(url)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.debug("EastMoney suspend list unavailable: %s", exc)
        return symbols

    result = payload.get("result") or {}
    for item in result.get("data") or []:
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
) -> pl.DataFrame:
    owns = client is None
    if client is None:
        client = EastMoneyClient(min_interval=0.3)

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

    if owns:
        client.close()
    return pl.DataFrame(rows)
