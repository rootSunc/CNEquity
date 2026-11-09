"""Shareholder trades, buybacks and share pledges from Tushare.

* ``holder_trades`` (``stk_holdertrade``): announced increases and decreases by
  major holders and executives, one row per announced trade. The source has no
  natural key (the same holder can report several trades on one day) and
  repeats rows verbatim, so exact duplicates are dropped and ``event_id`` is a
  hash of every field.
* ``share_repurchases`` (``repurchase``): each announcement of a buyback's
  progress, from plan to completion.
* ``pledge_stats`` (``pledge_stat``): weekly pledge totals per stock, from
  2014-03-07. Shares arrive in 万股 and are stored as shares.

Beijing-exchange legacy codes are stored under the current 920xxx code.
"""

from __future__ import annotations

import hashlib
from datetime import date

import polars as pl

from cnequity.adapters.tushare.client import SOURCE, TushareClient
from cnequity.domain.bse_codes import legacy_symbol_map

__all__ = [
    "HOLDER_TRADES_FLOOR",
    "PLEDGE_STATS_FLOOR",
    "SHARE_REPURCHASES_FLOOR",
    "SOURCE",
    "fetch_holder_trades",
    "fetch_pledge_stats",
    "fetch_share_repurchases",
    "normalize_holder_trades",
    "normalize_pledge_stats",
    "normalize_share_repurchases",
]

HOLDER_TRADES_FLOOR = date(2006, 1, 1)
SHARE_REPURCHASES_FLOOR = date(2005, 1, 1)
PLEDGE_STATS_FLOOR = date(2014, 3, 7)

_PAGE_SIZE = 3000
_HOLDER_TYPES = {"C": "company", "P": "individual", "G": "executive"}
_DIRECTIONS = {"IN": "increase", "DE": "decrease"}
# i18n-ignore-start: source values
_STAGES = {
    "预案": "plan",
    "股东大会通过": "approved",
    "实施": "in_progress",
    "完成": "completed",
    "停止": "stopped",
}
# i18n-ignore-end

HOLDER_TRADES_SCHEMA = {
    "event_id": pl.Utf8,
    "symbol": pl.Utf8,
    "ann_date": pl.Date,
    "holder_name": pl.Utf8,
    "holder_type": pl.Utf8,
    "direction": pl.Utf8,
    "change_shares": pl.Float64,
    "change_ratio": pl.Float64,
    "after_shares": pl.Float64,
    "after_ratio": pl.Float64,
    "avg_price": pl.Float64,
}
SHARE_REPURCHASES_SCHEMA = {
    "event_id": pl.Utf8,
    "symbol": pl.Utf8,
    "ann_date": pl.Date,
    "progress_date": pl.Date,
    "stage": pl.Utf8,
    "expiry_date": pl.Date,
    "shares": pl.Float64,
    "amount": pl.Float64,
    "price_high": pl.Float64,
    "price_low": pl.Float64,
}
PLEDGE_STATS_SCHEMA = {
    "symbol": pl.Utf8,
    "end_date": pl.Date,
    "pledge_count": pl.Int64,
    "unrestricted_pledged_shares": pl.Float64,
    "restricted_pledged_shares": pl.Float64,
    "total_shares": pl.Float64,
    "pledge_ratio": pl.Float64,
}


def _day(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d", strict=False)


def _symbol() -> pl.Expr:
    legacy_bj = legacy_symbol_map()
    return pl.col("ts_code").cast(pl.Utf8).str.strip_chars().replace(legacy_bj).alias("symbol")


def _frame(names: list[str], items: list[list]) -> pl.DataFrame:
    return pl.DataFrame(items, schema=names, orient="row", infer_schema_length=None)


def _with_event_id(frame: pl.DataFrame, fields: list[str]) -> pl.DataFrame:
    """Drop exact repeats, then key each row by a hash of all its fields."""
    frame = frame.unique(fields, keep="first", maintain_order=True)
    keys = [
        hashlib.sha1("\x1f".join("" if v is None else str(v) for v in row).encode()).hexdigest()[
            :20
        ]
        for row in frame.select(fields).iter_rows()
    ]
    return frame.with_columns(pl.Series("event_id", keys, dtype=pl.Utf8))


def normalize_holder_trades(names: list[str], items: list[list]) -> pl.DataFrame:
    """``stk_holdertrade`` rows as ``holder_trades`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=HOLDER_TRADES_SCHEMA)
    frame = _frame(names, items).select(
        _symbol(),
        _day("ann_date").alias("ann_date"),
        pl.col("holder_name").cast(pl.Utf8),
        pl.col("holder_type").cast(pl.Utf8).replace_strict(_HOLDER_TYPES, default=None),
        pl.col("in_de").cast(pl.Utf8).replace_strict(_DIRECTIONS, default=None).alias("direction"),
        pl.col("change_vol").cast(pl.Float64, strict=False).alias("change_shares"),
        pl.col("change_ratio").cast(pl.Float64, strict=False),
        pl.col("after_share").cast(pl.Float64, strict=False).alias("after_shares"),
        pl.col("after_ratio").cast(pl.Float64, strict=False),
        pl.col("avg_price").cast(pl.Float64, strict=False),
    )
    frame = frame.filter(
        pl.col("symbol").is_not_null()
        & pl.col("ann_date").is_not_null()
        & pl.col("holder_name").is_not_null()
        & pl.col("direction").is_not_null()
    )
    fields = [c for c in HOLDER_TRADES_SCHEMA if c != "event_id"]
    return (
        _with_event_id(frame, fields).select(list(HOLDER_TRADES_SCHEMA)).cast(HOLDER_TRADES_SCHEMA)
    )


def normalize_share_repurchases(names: list[str], items: list[list]) -> pl.DataFrame:
    """``repurchase`` rows as ``share_repurchases`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=SHARE_REPURCHASES_SCHEMA)
    frame = _frame(names, items).select(
        _symbol(),
        _day("ann_date").alias("ann_date"),
        _day("end_date").alias("progress_date"),
        pl.col("proc").cast(pl.Utf8).replace_strict(_STAGES, default=None).alias("stage"),
        _day("exp_date").alias("expiry_date"),
        pl.col("vol").cast(pl.Float64, strict=False).alias("shares"),
        pl.col("amount").cast(pl.Float64, strict=False),
        pl.col("high_limit").cast(pl.Float64, strict=False).alias("price_high"),
        pl.col("low_limit").cast(pl.Float64, strict=False).alias("price_low"),
    )
    frame = frame.filter(
        pl.col("symbol").is_not_null()
        & pl.col("ann_date").is_not_null()
        & pl.col("stage").is_not_null()
    )
    fields = [c for c in SHARE_REPURCHASES_SCHEMA if c != "event_id"]
    return (
        _with_event_id(frame, fields)
        .select(list(SHARE_REPURCHASES_SCHEMA))
        .cast(SHARE_REPURCHASES_SCHEMA)
    )


def normalize_pledge_stats(names: list[str], items: list[list]) -> pl.DataFrame:
    """``pledge_stat`` rows as ``pledge_stats`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=PLEDGE_STATS_SCHEMA)
    return (
        _frame(names, items)
        .select(
            _symbol(),
            _day("end_date").alias("end_date"),
            pl.col("pledge_count").cast(pl.Int64, strict=False),
            (pl.col("unrest_pledge").cast(pl.Float64, strict=False) * 10_000).alias(
                "unrestricted_pledged_shares"
            ),
            (pl.col("rest_pledge").cast(pl.Float64, strict=False) * 10_000).alias(
                "restricted_pledged_shares"
            ),
            (pl.col("total_share").cast(pl.Float64, strict=False) * 10_000).alias("total_shares"),
            pl.col("pledge_ratio").cast(pl.Float64, strict=False),
        )
        .filter(pl.col("symbol").is_not_null() & pl.col("end_date").is_not_null())
        .unique(["symbol", "end_date"], keep="last", maintain_order=True)
        .cast(PLEDGE_STATS_SCHEMA)
    )


def _fetch(api: str, params: dict, normalize, config, client: TushareClient | None):
    if client is not None:
        return normalize(*client.all_rows(api, params, "", page_size=_PAGE_SIZE))
    with TushareClient(config) as own:
        return normalize(*own.all_rows(api, params, "", page_size=_PAGE_SIZE))


def fetch_holder_trades(day: date, *, config, client: TushareClient | None = None):
    """Trades announced on one day. Raises ``TushareNotConfigured`` without a token."""
    params = {"ann_date": day.strftime("%Y%m%d")}
    return _fetch("stk_holdertrade", params, normalize_holder_trades, config, client)


def fetch_share_repurchases(day: date, *, config, client: TushareClient | None = None):
    """Buyback announcements of one day. Raises ``TushareNotConfigured`` without a token."""
    params = {"ann_date": day.strftime("%Y%m%d")}
    return _fetch("repurchase", params, normalize_share_repurchases, config, client)


def fetch_pledge_stats(day: date, *, config, client: TushareClient | None = None):
    """Pledge totals as of one week's end date (empty on other days)."""
    params = {"end_date": day.strftime("%Y%m%d")}
    return _fetch("pledge_stat", params, normalize_pledge_stats, config, client)
