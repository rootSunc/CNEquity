"""Convertible and exchangeable bonds from Tushare (``cb_basic``, ``cb_issue``, ``cb_daily``).

``convertible_bonds`` holds one row per bond: terms from ``cb_basic`` and the
public offering from ``cb_issue`` (privately placed bonds have no offering and
no listing date). ``conversion_price`` is the price in force when the list was
read, not a history; ``first_conversion_price`` is the price at issue.

``convertible_bond_bars`` holds daily bars from 2004-01-02. Prices are per 100
of face value; volume arrives in lots of 10 bonds and is stored in bonds;
amount arrives in 万元 and is stored in CNY (on 2026-11-04 amount / volume /
close lay between 0.90 and 1.02 for every traded bond).
"""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.adapters.tushare.client import SOURCE, TushareClient

__all__ = [
    "CONVERTIBLE_BOND_BARS_FLOOR",
    "SOURCE",
    "bonds_from_source",
    "fetch_bars",
    "fetch_bonds",
    "normalize_bars",
]

#: First session ``cb_daily`` serves.
CONVERTIBLE_BOND_BARS_FLOOR = date(2004, 1, 2)

_PAGE_SIZE = 2000
_BOND_TYPES = {"CB": "convertible", "EB": "exchangeable"}

BONDS_SCHEMA = {
    "symbol": pl.Utf8,
    "name": pl.Utf8,
    "full_name": pl.Utf8,
    "bond_type": pl.Utf8,
    "stock_symbol": pl.Utf8,
    "exchange": pl.Utf8,
    "par_value": pl.Float64,
    "issue_price": pl.Float64,
    "issue_size": pl.Float64,
    "remaining_size": pl.Float64,
    "term_years": pl.Float64,
    "coupon_rate": pl.Float64,
    "coupon_schedule": pl.Utf8,
    "value_date": pl.Date,
    "maturity_date": pl.Date,
    "list_date": pl.Date,
    "delist_date": pl.Date,
    "conversion_start_date": pl.Date,
    "conversion_end_date": pl.Date,
    "conversion_stop_date": pl.Date,
    "first_conversion_price": pl.Float64,
    "conversion_price": pl.Float64,
    "issue_announce_date": pl.Date,
    "issue_method": pl.Utf8,
    "subscription_date": pl.Date,
    "holder_allotment_record_date": pl.Date,
}

BARS_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "pre_close": pl.Float64,
    "volume": pl.Int64,
    "amount": pl.Float64,
}


def _day(column: str) -> pl.Expr:
    text = pl.col(column).cast(pl.Utf8).str.replace_all("-", "")
    return text.str.strptime(pl.Date, "%Y%m%d", strict=False)


def bonds_from_source(basic: pl.DataFrame, issue: pl.DataFrame) -> pl.DataFrame:
    """``cb_basic`` rows with their ``cb_issue`` offering, as ``convertible_bonds`` columns."""
    if basic.is_empty():
        return pl.DataFrame(schema=BONDS_SCHEMA)
    bonds = basic.unique("ts_code", keep="last", maintain_order=True).select(
        pl.col("ts_code").alias("symbol"),
        pl.col("bond_short_name").alias("name"),
        pl.col("bond_full_name").alias("full_name"),
        pl.col("cb_type").replace_strict(_BOND_TYPES, default=None).alias("bond_type"),
        pl.col("stk_code").alias("stock_symbol"),
        pl.col("exchange").cast(pl.Utf8),
        pl.col("par").cast(pl.Float64).alias("par_value"),
        pl.col("issue_price").cast(pl.Float64),
        pl.col("issue_size").cast(pl.Float64),
        pl.col("remain_size").cast(pl.Float64).alias("remaining_size"),
        pl.col("maturity").cast(pl.Float64).alias("term_years"),
        pl.col("coupon_rate").cast(pl.Float64),
        pl.col("rate_clause").cast(pl.Utf8).alias("coupon_schedule"),
        _day("value_date").alias("value_date"),
        _day("maturity_date").alias("maturity_date"),
        _day("list_date").alias("list_date"),
        _day("delist_date").alias("delist_date"),
        _day("conv_start_date").alias("conversion_start_date"),
        _day("conv_end_date").alias("conversion_end_date"),
        _day("conv_stop_date").alias("conversion_stop_date"),
        pl.col("first_conv_price").cast(pl.Float64).alias("first_conversion_price"),
        pl.col("conv_price").cast(pl.Float64).alias("conversion_price"),
    )
    if issue.is_empty():
        offering = pl.DataFrame(
            schema={k: BONDS_SCHEMA[k] for k in ("symbol", *list(BONDS_SCHEMA)[-4:])}
        )
    else:
        offering = issue.unique("ts_code", keep="last", maintain_order=True).select(
            pl.col("ts_code").alias("symbol"),
            _day("ann_date").alias("issue_announce_date"),
            pl.col("issue_type").cast(pl.Utf8).alias("issue_method"),
            _day("onl_date").alias("subscription_date"),
            _day("shd_ration_record_date").alias("holder_allotment_record_date"),
        )
    return (
        bonds.join(offering, on="symbol", how="left")
        .filter(pl.col("symbol").is_not_null() & pl.col("bond_type").is_not_null())
        .select(list(BONDS_SCHEMA))
        .cast(BONDS_SCHEMA)
    )


def normalize_bars(names: list[str], items: list[list]) -> pl.DataFrame:
    """``cb_daily`` rows as ``convertible_bond_bars`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=BARS_SCHEMA)
    frame = pl.DataFrame(items, schema=names, orient="row")
    return (
        frame.select(
            pl.col("ts_code").cast(pl.Utf8).alias("symbol"),
            _day("trade_date").alias("trade_date"),
            *[
                pl.col(c).cast(pl.Float64, strict=False)
                for c in ("open", "high", "low", "close", "pre_close")
            ],
            (pl.col("vol").cast(pl.Float64, strict=False) * 10).round(0).alias("volume"),
            (pl.col("amount").cast(pl.Float64, strict=False) * 10_000.0).alias("amount"),
        )
        .filter(
            pl.col("symbol").is_not_null()
            & pl.col("trade_date").is_not_null()
            & pl.col("close").is_not_null()
        )
        .unique(["symbol", "trade_date"], keep="last", maintain_order=True)
        .cast(BARS_SCHEMA)
    )


def fetch_bonds(config, *, client: TushareClient | None = None) -> pl.DataFrame:
    """Every listed, delisted and privately placed bond. Raises without a token."""

    def run(c: TushareClient) -> pl.DataFrame:
        basic = c.query_all("cb_basic", {}, page_size=_PAGE_SIZE)
        issue = c.query_all("cb_issue", {}, page_size=_PAGE_SIZE)
        return bonds_from_source(basic, issue)

    if client is not None:
        return run(client)
    with TushareClient(config) as own:
        return run(own)


def fetch_bars(trade_date: date, *, config, client: TushareClient | None = None) -> pl.DataFrame:
    """One session's bars. Raises ``TushareNotConfigured`` without a token."""
    params = {"trade_date": trade_date.strftime("%Y%m%d")}
    if client is not None:
        return normalize_bars(*client.all_rows("cb_daily", params, "", page_size=_PAGE_SIZE))
    with TushareClient(config) as own:
        return normalize_bars(*own.all_rows("cb_daily", params, "", page_size=_PAGE_SIZE))
