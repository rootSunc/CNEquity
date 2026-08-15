"""EastMoney valuation metrics (PE/PB/PS/market cap)."""

from __future__ import annotations

from datetime import date

import polars as pl

from cn_market_lake.adapters.eastmoney.clist import clist_rows_to_symbols, fetch_clist_pages
from cn_market_lake.adapters.eastmoney.common import _to_float
from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient

_VALUATION_FIELDS = "f12,f13,f9,f23,f45,f20,f21"


def fetch_valuation_metrics(
    trade_date: date,
    *,
    client: EastMoneyClient | None = None,
    config=None,
) -> pl.DataFrame:
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)
    rows_raw = fetch_clist_pages(client, fields=_VALUATION_FIELDS)
    rows = []
    for sym, item in clist_rows_to_symbols(rows_raw):
        rows.append(
            {
                "symbol": sym,
                "trade_date": trade_date,
                "pe_ttm": _to_float(item.get("f9")),
                "pb": _to_float(item.get("f23")),
                "ps_ttm": _to_float(item.get("f45")),
                "total_mv": _to_float(item.get("f20")),
                "float_mv": _to_float(item.get("f21")),
            }
        )
    if owns:
        client.close()
    return pl.DataFrame(rows) if rows else pl.DataFrame()
