"""EastMoney industry classification membership."""

from __future__ import annotations

from datetime import date

import polars as pl

from cn_market_lake.adapters.eastmoney.common import exchange_from_datacenter, symbol_from_em
from cn_market_lake.adapters.eastmoney.datacenter import fetch_datacenter
from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient

_BOARD_REPORT = "RPT_BOARD_CONSTITUENT"
_BOARD_COLUMNS = "SECURITY_CODE,BOARD_CODE,BOARD_NAME,BOARD_TYPE_NEW"
_INDUSTRY_BOARD_TYPE = "2"


def fetch_industry_members(
    as_of_date: date,
    *,
    client: EastMoneyClient | None = None,
) -> pl.DataFrame:
    owns = client is None
    if client is None:
        client = EastMoneyClient()

    raw = fetch_datacenter(
        client,
        _BOARD_REPORT,
        _BOARD_COLUMNS,
        filter_expr=f'(BOARD_TYPE_NEW="{_INDUSTRY_BOARD_TYPE}")',
        # Same report as sector_members, same measured 5000-row page. The
        # industry slice is only ~17k rows today, but at the 500 clamp that is
        # 34 pages and a third of the way to the pageNumber cap that broke
        # sector_members; 5000 keeps it at 4.
        page_size=5000,
        trust_page_size=True,
    )
    rows: list[dict] = []
    for item in raw:
        code = str(item.get("SECURITY_CODE", "")).zfill(6)
        exch = exchange_from_datacenter(item)
        sym = symbol_from_em(code, 1 if exch == "SH" else (2 if exch == "BJ" else 0))
        if not sym:
            continue
        rows.append(
            {
                "symbol": sym,
                "classification_system": "eastmoney",
                "industry_code": str(item.get("BOARD_CODE") or ""),
                "industry_name": str(item.get("BOARD_NAME") or ""),
                "as_of_date": as_of_date,
            }
        )

    if owns:
        client.close()
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(
        subset=["symbol", "classification_system", "as_of_date"], keep="last"
    )
