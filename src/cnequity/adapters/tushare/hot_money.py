"""Hot-money (游资) traders and their daily trades, from Tushare.

* ``hot_money_traders`` (``hm_list``): Tushare's roster of known short-term
  traders, each with a description and the broker branches (seats) they use.
  Re-read whole; ``seats`` is the source's JSON list as text.
* ``hot_money_trades`` (``hm_detail``), from 2022-08: per session, each
  roster trader's buys and sells of a stock as seen on the exchange's
  dragon-tiger lists. A trader trading through several seats has one row per
  seat group. Amounts are in yuan.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.adapters.eastmoney.corporate_actions_migration import _code_mapping
from cnequity.adapters.tushare.client import SOURCE, TushareClient

__all__ = [
    "HOT_MONEY_TRADES_FLOOR",
    "SOURCE",
    "fetch_hot_money_traders",
    "fetch_hot_money_trades",
    "normalize_hot_money_traders",
    "normalize_hot_money_trades",
]

HOT_MONEY_TRADES_FLOOR = date(2022, 8, 1)
_PAGE_SIZE = 2000

HOT_MONEY_TRADERS_SCHEMA = {
    "trader": pl.Utf8,
    "description": pl.Utf8,
    "seats": pl.Utf8,
}
HOT_MONEY_TRADES_SCHEMA = {
    "trade_date": pl.Date,
    "symbol": pl.Utf8,
    "trader": pl.Utf8,
    "seats": pl.Utf8,
    "buy_amount": pl.Float64,
    "sell_amount": pl.Float64,
    "net_amount": pl.Float64,
}


def _frame(names: list[str], items: list[list]) -> pl.DataFrame:
    return pl.DataFrame(items, schema=names, orient="row", infer_schema_length=None)


def _text(column: str) -> pl.Expr:
    value = pl.col(column).cast(pl.Utf8).str.strip_chars()
    return pl.when(value == "").then(None).otherwise(value).alias(column)


def normalize_hot_money_traders(names: list[str], items: list[list]) -> pl.DataFrame:
    """``hm_list`` rows as ``hot_money_traders`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=HOT_MONEY_TRADERS_SCHEMA)
    return (
        _frame(names, items)
        .select(
            _text("name").alias("trader"),
            _text("desc").alias("description"),
            _text("orgs").alias("seats"),
        )
        .filter(pl.col("trader").is_not_null())
        .unique("trader", keep="last", maintain_order=True)
        .cast(HOT_MONEY_TRADERS_SCHEMA)
    )


def normalize_hot_money_trades(names: list[str], items: list[list]) -> pl.DataFrame:
    """``hm_detail`` rows as ``hot_money_trades`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=HOT_MONEY_TRADES_SCHEMA)
    legacy_bj = {f"{old}.BJ": f"{new}.BJ" for old, new in _code_mapping().items()}
    key = ["trade_date", "symbol", "trader", "seats"]
    return (
        _frame(names, items)
        .select(
            pl.col("trade_date").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d", strict=False),
            _text("ts_code").replace(legacy_bj).alias("symbol"),
            _text("hm_name").alias("trader"),
            _text("hm_orgs").fill_null("").alias("seats"),
            *[
                pl.col(c).cast(pl.Float64, strict=False)
                for c in ("buy_amount", "sell_amount", "net_amount")
            ],
        )
        .filter(pl.all_horizontal(pl.col(k).is_not_null() for k in key[:3]))
        .unique(key, keep="last", maintain_order=True)
        .cast(HOT_MONEY_TRADES_SCHEMA)
    )


def fetch_hot_money_traders(*, config, client: TushareClient | None = None) -> pl.DataFrame:
    """The whole roster. Raises ``TushareNotConfigured`` without a token."""
    if client is not None:
        return normalize_hot_money_traders(
            *client.all_rows("hm_list", {}, "", page_size=_PAGE_SIZE)
        )
    with TushareClient(config) as own:
        return normalize_hot_money_traders(*own.all_rows("hm_list", {}, "", page_size=_PAGE_SIZE))


def fetch_hot_money_trades(day: date, *, config, client: TushareClient | None = None):
    """One session's trades. Raises ``TushareNotConfigured`` without a token."""
    params = {"trade_date": day.strftime("%Y%m%d")}
    if client is not None:
        return normalize_hot_money_trades(
            *client.all_rows("hm_detail", params, "", page_size=_PAGE_SIZE)
        )
    with TushareClient(config) as own:
        return normalize_hot_money_trades(
            *own.all_rows("hm_detail", params, "", page_size=_PAGE_SIZE)
        )
