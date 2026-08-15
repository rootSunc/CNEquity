"""EastMoney index constituents and weights."""

from __future__ import annotations

from datetime import date

import polars as pl

from cn_market_lake.adapters.eastmoney.common import exchange_from_datacenter, symbol_from_em
from cn_market_lake.adapters.eastmoney.datacenter import fetch_datacenter
from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient
from cn_market_lake.domain.symbols import format_symbol

DEFAULT_INDICES = [
    "000001.SH",
    "000300.SH",
    "000688.SH",
    "399001.SZ",
    "399006.SZ",
]

_INDEX_CODE_MAP = {
    "000001": "000001.SH",
    "000300": "000300.SH",
    "000688": "000688.SH",
    "399001": "399001.SZ",
    "399006": "399006.SZ",
}

_REPORT = "RPT_INDEX_CONSTITUENT"
_COLUMNS = "INDEX_CODE,SECURITY_CODE,TRADE_DATE"


def _index_symbol(index_code: str) -> str:
    code = str(index_code).zfill(6)
    return _INDEX_CODE_MAP.get(code, format_symbol(code, "SH" if code.startswith("0") else "SZ"))


def fetch_index_constituents(
    as_of_date: date,
    *,
    indices: list[str] | None = None,
    client: EastMoneyClient | None = None,
) -> pl.DataFrame:
    owns = client is None
    if client is None:
        client = EastMoneyClient()

    target_indices = indices or DEFAULT_INDICES
    rows: list[dict] = []
    for index_sym in target_indices:
        index_code = index_sym.split(".")[0]
        raw = fetch_datacenter(
            client,
            _REPORT,
            _COLUMNS,
            filter_expr=f'(INDEX_CODE="{index_code}")',
            page_size=5000,
        )
        for item in raw:
            code = str(item.get("SECURITY_CODE", "")).zfill(6)
            exch = exchange_from_datacenter(item)
            sym = symbol_from_em(code, 1 if exch == "SH" else (2 if exch == "BJ" else 0))
            if not sym:
                continue
            rows.append(
                {
                    "index_symbol": _index_symbol(item.get("INDEX_CODE") or index_code),
                    "symbol": sym,
                    "as_of_date": as_of_date,
                    # EastMoney RPT_INDEX_CONSTITUENT no longer exposes constituent weights.
                    "weight": 0.0,
                }
            )

    if owns:
        client.close()
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["index_symbol", "symbol", "as_of_date"], keep="last")
