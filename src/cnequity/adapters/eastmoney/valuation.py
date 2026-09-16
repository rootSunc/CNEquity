"""EastMoney valuation metrics (PE/PB/PS/market cap)."""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cnequity.adapters.eastmoney.clist import clist_rows_to_symbols, fetch_clist_pages
from cnequity.adapters.eastmoney.common import _to_float
from cnequity.adapters.eastmoney.em_auth import EastMoneyClient

# f130 is 市销率 TTM. f45 — which this used to read as ps_ttm — is an amount in
# yuan, not a ratio: it put a median of 2.05e7 into `ps_ttm` for every EastMoney
# row while baostock's median for the same column was 3.2. Verified against the
# feed's own numbers, twice: total_mv / f132 (营业总收入 TTM) equals f130 exactly
# for 600519 (1.591e12 / 1.732e11 = 9.184) and for 000001 (2.294e11 / 1.327e11
# = 1.729).
_VALUATION_FIELDS = "f12,f13,f9,f23,f130,f20,f21"
logger = logging.getLogger(__name__)


def fetch_valuation_metrics(
    trade_date: date,
    *,
    client: EastMoneyClient | None = None,
    config=None,
) -> pl.DataFrame:
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)
    try:
        rows_raw = fetch_clist_pages(client, fields=_VALUATION_FIELDS)
        mapped_rows = clist_rows_to_symbols(rows_raw)
        if len(mapped_rows) != len(rows_raw):
            logger.warning(
                "EastMoney valuation_metrics clist dropped %d non-security row(s)",
                len(rows_raw) - len(mapped_rows),
            )
        rows = []
        for sym, item in mapped_rows:
            rows.append(
                {
                    "symbol": sym,
                    "trade_date": trade_date,
                    "pe_ttm": _to_float(item.get("f9")),
                    "pb": _to_float(item.get("f23")),
                    "ps_ttm": _to_float(item.get("f130")),
                    "total_mv": _to_float(item.get("f20")),
                    "float_mv": _to_float(item.get("f21")),
                }
            )
    finally:
        if owns:
            client.close()
    return (
        pl.DataFrame(rows).unique(subset=["symbol", "trade_date"], keep="last")
        if rows
        else pl.DataFrame()
    )
