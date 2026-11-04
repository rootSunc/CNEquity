"""Daily limit-up / limit-down prices from Tushare ``stk_limit``.

The exchange's price band for each stock and session, from 2007-01-04. A bar
can say a stock closed at its high; only the band says whether that high was
the limit, which is what limit-hit, unfillable-order and board-streak studies
need. Checked 2026-11-04: no close on that session lies outside its band.

Beijing-exchange stocks are listed under their pre-2024 codes on older
sessions and are stored under the current 920xxx code, as the rest of the lake
does. Codes outside the lake's universe (B shares) are left to the caller.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.adapters.eastmoney.corporate_actions_migration import _code_mapping
from cnequity.adapters.tushare.client import SOURCE, TushareClient

__all__ = ["LIMIT_PRICES_FLOOR", "SOURCE", "fetch_limit_prices", "normalize"]

#: First session ``stk_limit`` serves.
LIMIT_PRICES_FLOOR = date(2007, 1, 4)

_FIELDS = "trade_date,ts_code,up_limit,down_limit"
_PAGE_SIZE = 3000
_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "up_limit": pl.Float64,
    "down_limit": pl.Float64,
}


def normalize(names: list[str], items: list[list]) -> pl.DataFrame:
    """Source rows as ``limit_prices`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=_SCHEMA)
    legacy_bj = {f"{old}.BJ": f"{new}.BJ" for old, new in _code_mapping().items()}
    frame = pl.DataFrame(items, schema=names, orient="row")
    return (
        frame.select(
            pl.col("ts_code").cast(pl.Utf8).str.strip_chars().replace(legacy_bj).alias("symbol"),
            pl.col("trade_date").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d", strict=False),
            pl.col("up_limit").cast(pl.Float64, strict=False),
            pl.col("down_limit").cast(pl.Float64, strict=False),
        )
        .filter(
            pl.col("symbol").is_not_null()
            & pl.col("trade_date").is_not_null()
            & pl.col("up_limit").is_not_null()
            & pl.col("down_limit").is_not_null()
        )
        .unique(["symbol", "trade_date"], keep="last", maintain_order=True)
        .cast(_SCHEMA)
    )


def fetch_limit_prices(
    trade_date: date, *, config, client: TushareClient | None = None
) -> pl.DataFrame:
    """One session's bands. Raises ``TushareNotConfigured`` without a token."""
    params = {"trade_date": trade_date.strftime("%Y%m%d")}
    if client is not None:
        return normalize(*client.all_rows("stk_limit", params, _FIELDS, page_size=_PAGE_SIZE))
    with TushareClient(config) as own:
        return normalize(*own.all_rows("stk_limit", params, _FIELDS, page_size=_PAGE_SIZE))
