"""Security name history from Tushare ``namechange``.

One row per name a security has carried, with the dates it was in force. The
whole market's history is a handful of pages (about 14,000 rows in 2026), so
every run reads it whole instead of tracking a watermark.

A name is the exchange's own ST label: ``ST``/``*ST`` (with the share-reform
``S`` or ``G`` prefixes of 2005–2007) is how a risk warning is shown. Unlike a
daily feed, an interval also covers the sessions a security was halted, which
is the one place the lake had no ST evidence at all. Checked on 2026-11-04
against baostock's ``isST`` on 16.9 million traded stock-days, the label read
from these names disagreed on 7, each a change dated one to four sessions
early.
"""

from __future__ import annotations

import polars as pl

from cnequity.adapters.tushare.client import SOURCE, TushareClient

__all__ = ["SOURCE", "ST_NAME_PATTERN", "fetch_name_history", "normalize"]

_FIELDS = "ts_code,name,start_date,end_date,ann_date,change_reason"

#: ``ST``, ``*ST``, and the 2005–2007 share-reform forms ``SST``, ``S*ST``,
#: ``GST``, ``G*ST``. ``PT`` (particular transfer, before 2002) is not ST.
ST_NAME_PATTERN = r"^[GS]?\*?ST"


def normalize(names: list[str], items: list[list]) -> pl.DataFrame:
    """Rows from the source as the ``name_history`` schema, minus provenance."""
    frame = pl.DataFrame(items, schema=names, orient="row") if items else None
    if frame is None or frame.is_empty():
        return pl.DataFrame(
            schema={
                "symbol": pl.Utf8,
                "name": pl.Utf8,
                "start_date": pl.Date,
                "end_date": pl.Date,
                "announce_date": pl.Date,
                "change_reason": pl.Utf8,
                "risk_warning": pl.Boolean,
            }
        )

    def day(column: str) -> pl.Expr:
        return pl.col(column).cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d", strict=False)

    return (
        frame.select(
            pl.col("ts_code").cast(pl.Utf8).str.strip_chars().alias("symbol"),
            pl.col("name").cast(pl.Utf8).str.strip_chars().alias("name"),
            day("start_date").alias("start_date"),
            day("end_date").alias("end_date"),
            day("ann_date").alias("announce_date"),
            pl.col("change_reason").cast(pl.Utf8).alias("change_reason"),
        )
        .filter(
            pl.col("symbol").is_not_null()
            & (pl.col("symbol") != "")
            & pl.col("name").is_not_null()
            & (pl.col("name") != "")
            & pl.col("start_date").is_not_null()
        )
        .with_columns(pl.col("name").str.contains(ST_NAME_PATTERN).alias("risk_warning"))
        .unique(["symbol", "start_date", "name"], keep="last", maintain_order=True)
        .sort("symbol", "start_date", "name")
    )


def fetch_name_history(config, *, client: TushareClient | None = None) -> pl.DataFrame:
    """Every security's name history. Raises ``TushareNotConfigured`` without a token."""
    if client is not None:
        return normalize(*client.all_rows("namechange", {}, _FIELDS))
    with TushareClient(config) as own:
        return normalize(*own.all_rows("namechange", {}, _FIELDS))
