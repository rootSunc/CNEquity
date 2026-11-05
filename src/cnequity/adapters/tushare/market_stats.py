"""Chip distribution and the northbound top-10 list from Tushare.

* ``chip_distribution`` (``cyq_perf``), from 2018-01-02: Tushare's estimate of
  each stock's holding-cost distribution per session — the 5/15/50/85/95th
  cost percentiles, their weighted mean and ``winner_rate``, the percent of
  holdings bought below the close. A model output, not exchange data.
* ``northbound_top10`` (``hsgt_top10``), from 2014-11-17: the ten most traded
  Shanghai and Shenzhen Connect stocks each session. ``net_amount``, ``buy`` and
  ``sell`` end on 2024-08-16, when the exchanges stopped publishing them.

Beijing-exchange legacy codes are stored under the current 920xxx code.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.adapters.eastmoney.corporate_actions_migration import _code_mapping
from cnequity.adapters.tushare.client import SOURCE, TushareClient

__all__ = [
    "CHIP_DISTRIBUTION_FLOOR",
    "NORTHBOUND_TOP10_FLOOR",
    "SOURCE",
    "fetch_chip_distribution",
    "fetch_northbound_top10",
    "normalize_chip_distribution",
    "normalize_northbound_top10",
]

CHIP_DISTRIBUTION_FLOOR = date(2018, 1, 2)
NORTHBOUND_TOP10_FLOOR = date(2014, 11, 17)

_PAGE_SIZE = 5000
_CHANNELS = {1: "sh_connect", 3: "sz_connect"}
_COST_COLUMNS = ("cost_5pct", "cost_15pct", "cost_50pct", "cost_85pct", "cost_95pct")

CHIP_DISTRIBUTION_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "history_low": pl.Float64,
    "history_high": pl.Float64,
    **{c: pl.Float64 for c in _COST_COLUMNS},
    "cost_mean": pl.Float64,
    "winner_rate": pl.Float64,
}
NORTHBOUND_TOP10_SCHEMA = {
    "trade_date": pl.Date,
    "channel": pl.Utf8,
    "symbol": pl.Utf8,
    "rank": pl.Int64,
    "close": pl.Float64,
    "change": pl.Float64,
    "amount": pl.Float64,
    "net_amount": pl.Float64,
    "buy_amount": pl.Float64,
    "sell_amount": pl.Float64,
}


def _day(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d", strict=False)


def _symbol() -> pl.Expr:
    legacy_bj = {f"{old}.BJ": f"{new}.BJ" for old, new in _code_mapping().items()}
    return pl.col("ts_code").cast(pl.Utf8).str.strip_chars().replace(legacy_bj).alias("symbol")


def _float(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.Float64, strict=False)


def normalize_chip_distribution(names: list[str], items: list[list]) -> pl.DataFrame:
    """``cyq_perf`` rows as ``chip_distribution`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=CHIP_DISTRIBUTION_SCHEMA)
    frame = pl.DataFrame(items, schema=names, orient="row", infer_schema_length=None)
    return (
        frame.select(
            _symbol(),
            _day("trade_date").alias("trade_date"),
            _float("his_low").alias("history_low"),
            _float("his_high").alias("history_high"),
            *[_float(c) for c in _COST_COLUMNS],
            _float("weight_avg").alias("cost_mean"),
            _float("winner_rate"),
        )
        .filter(pl.col("symbol").is_not_null() & pl.col("trade_date").is_not_null())
        .unique(["symbol", "trade_date"], keep="last", maintain_order=True)
        .cast(CHIP_DISTRIBUTION_SCHEMA)
    )


def normalize_northbound_top10(names: list[str], items: list[list]) -> pl.DataFrame:
    """``hsgt_top10`` rows as ``northbound_top10`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=NORTHBOUND_TOP10_SCHEMA)
    frame = pl.DataFrame(items, schema=names, orient="row", infer_schema_length=None)
    return (
        frame.select(
            _day("trade_date").alias("trade_date"),
            pl.col("market_type")
            .cast(pl.Int64, strict=False)
            .replace_strict(_CHANNELS, default=None, return_dtype=pl.Utf8)
            .alias("channel"),
            _symbol(),
            pl.col("rank").cast(pl.Int64, strict=False),
            _float("close"),
            _float("change"),
            _float("amount"),
            _float("net_amount"),
            _float("buy").alias("buy_amount"),
            _float("sell").alias("sell_amount"),
        )
        .filter(
            pl.col("trade_date").is_not_null()
            & pl.col("channel").is_not_null()
            & pl.col("symbol").is_not_null()
        )
        .unique(["trade_date", "channel", "symbol"], keep="last", maintain_order=True)
        .cast(NORTHBOUND_TOP10_SCHEMA)
    )


def _fetch(api: str, day: date, normalize, config, client: TushareClient | None):
    params = {"trade_date": day.strftime("%Y%m%d")}
    if client is not None:
        return normalize(*client.all_rows(api, params, "", page_size=_PAGE_SIZE))
    with TushareClient(config) as own:
        return normalize(*own.all_rows(api, params, "", page_size=_PAGE_SIZE))


def fetch_chip_distribution(day: date, *, config, client: TushareClient | None = None):
    """One session. Raises ``TushareNotConfigured`` without a token."""
    return _fetch("cyq_perf", day, normalize_chip_distribution, config, client)


def fetch_northbound_top10(day: date, *, config, client: TushareClient | None = None):
    """One session's two lists. Raises ``TushareNotConfigured`` without a token."""
    return _fetch("hsgt_top10", day, normalize_northbound_top10, config, client)
