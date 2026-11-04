"""Board money flow from Tushare ``moneyflow_ind_dc`` (EastMoney's boards).

The boards are EastMoney's own (``BKxxxx``), so the rows sit in
``sector_fund_flow`` next to the lake's EastMoney rows: on 2026-09-21 net
inflow, change and name agreed on all 1,000 industry and concept boards.
Tushare also carries EastMoney's 31 region boards, stored as
``board_type="region"``. It has no turnover, which stays null.

Depth is uneven (probed 2026-11-04): from 2023-09-12 only the 86 industry
boards of the older classification; concept and region boards from 2025-02-26;
about 1,000 boards a session since 2025-10.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.adapters.tushare.client import SOURCE, TushareClient

__all__ = ["MONEYFLOW_IND_DC_FLOOR", "SOURCE", "fetch_sector_fund_flow", "normalize"]

#: First session ``moneyflow_ind_dc`` serves (2023-09-11 is empty).
MONEYFLOW_IND_DC_FLOOR = date(2023, 9, 12)

_FIELDS = "trade_date,content_type,ts_code,name,pct_change,net_amount"
_PAGE_SIZE = 3000
_BOARD_TYPES = {"行业": "industry", "概念": "concept", "地域": "region"}
_SCHEMA = {
    "sector_code": pl.Utf8,
    "sector_name": pl.Utf8,
    "board_type": pl.Utf8,
    "trade_date": pl.Date,
    "main_net_inflow": pl.Float64,
    "change_pct": pl.Float64,
    "turnover_pct": pl.Float64,
}


def normalize(names: list[str], items: list[list]) -> pl.DataFrame:
    """Source rows as ``sector_fund_flow`` columns, minus provenance.

    A board type outside industry/concept/region is dropped rather than
    guessed at.
    """
    if not items:
        return pl.DataFrame(schema=_SCHEMA)
    frame = pl.DataFrame(items, schema=names, orient="row")
    return (
        frame.select(
            pl.col("ts_code")
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.split(".")
            .list.first()
            .alias("sector_code"),
            pl.col("name").cast(pl.Utf8).str.strip_chars().alias("sector_name"),
            pl.col("content_type")
            .cast(pl.Utf8)
            .replace_strict(_BOARD_TYPES, default=None)
            .alias("board_type"),
            pl.col("trade_date").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d", strict=False),
            pl.col("net_amount").cast(pl.Float64, strict=False).alias("main_net_inflow"),
            pl.col("pct_change").cast(pl.Float64, strict=False).alias("change_pct"),
            pl.lit(None, dtype=pl.Float64).alias("turnover_pct"),
        )
        .filter(
            pl.col("sector_code").str.starts_with("BK")
            & pl.col("board_type").is_not_null()
            & pl.col("trade_date").is_not_null()
        )
        .unique(["sector_code", "trade_date"], keep="last", maintain_order=True)
        .cast(_SCHEMA)
    )


def fetch_sector_fund_flow(
    trade_date: date, *, config, client: TushareClient | None = None
) -> pl.DataFrame:
    """One session's board flows. Raises ``TushareNotConfigured`` without a token."""
    params = {"trade_date": trade_date.strftime("%Y%m%d")}
    if client is not None:
        return normalize(
            *client.all_rows("moneyflow_ind_dc", params, _FIELDS, page_size=_PAGE_SIZE)
        )
    with TushareClient(config) as own:
        return normalize(*own.all_rows("moneyflow_ind_dc", params, _FIELDS, page_size=_PAGE_SIZE))
