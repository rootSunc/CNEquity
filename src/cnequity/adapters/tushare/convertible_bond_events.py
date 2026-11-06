"""Convertible bond redemptions, conversions and coupons from Tushare.

* ``convertible_bond_calls`` (``cb_call``): each announcement about a forced
  (强赎) or maturity (到赎) redemption: the condition being met, a reminder, a
  decision not to call, or the call itself with its price, size and dates.
  ``call_volume`` is in bonds; ``call_amount`` arrives in 万元 and is stored
  in yuan.
* ``convertible_bond_conversions`` (``cb_share``): each periodic conversion
  report: what was converted in the period and in total, and what remains.
  Ratios are percents of the issue size.
* ``convertible_bond_coupons`` (``cb_rate``): the coupon rate of each interest
  year.

These APIs ignore dates, so every fetch reads one bond's whole history.
"""

from __future__ import annotations

import polars as pl

from cnequity.adapters.tushare.client import SOURCE, TushareClient

__all__ = [
    "SOURCE",
    "fetch_bond_events",
    "normalize_calls",
    "normalize_conversions",
    "normalize_coupons",
]

_PAGE_SIZE = 2000
# i18n-ignore-start: source values
_CALL_TYPES = {"强赎": "forced", "到赎": "maturity"}
_CALL_STAGES = {
    "已满足强赎条件": "condition_met",
    "公告提示强赎": "reminder",
    "公告实施强赎": "calling",
    "公告不强赎": "not_calling",
    "公告到期赎回": "maturity_redemption",
}
# i18n-ignore-end

CALLS_SCHEMA = {
    "symbol": pl.Utf8,
    "ann_date": pl.Date,
    "call_type": pl.Utf8,
    "stage": pl.Utf8,
    "call_date": pl.Date,
    "call_price": pl.Float64,
    "call_price_after_tax": pl.Float64,
    "call_volume": pl.Float64,
    "call_amount": pl.Float64,
    "record_date": pl.Date,
    "payment_date": pl.Date,
}
CONVERSIONS_SCHEMA = {
    "symbol": pl.Utf8,
    "end_date": pl.Date,
    "publish_date": pl.Date,
    "issue_size": pl.Float64,
    "first_conversion_price": pl.Float64,
    "conversion_price": pl.Float64,
    "converted_amount": pl.Float64,
    "converted_shares": pl.Float64,
    "converted_pct": pl.Float64,
    "total_converted_amount": pl.Float64,
    "total_converted_shares": pl.Float64,
    "total_converted_pct": pl.Float64,
    "remaining_size": pl.Float64,
    "total_shares": pl.Float64,
}
COUPONS_SCHEMA = {
    "symbol": pl.Utf8,
    "start_date": pl.Date,
    "end_date": pl.Date,
    "coupon_rate": pl.Float64,
    "payments_per_year": pl.Int64,
}


def _day(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d", strict=False)


def _float(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.Float64, strict=False)


def _frame(names: list[str], items: list[list]) -> pl.DataFrame:
    return pl.DataFrame(items, schema=names, orient="row", infer_schema_length=None)


def normalize_calls(names: list[str], items: list[list]) -> pl.DataFrame:
    """``cb_call`` rows as ``convertible_bond_calls`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=CALLS_SCHEMA)
    key = ["symbol", "ann_date", "call_type", "stage"]
    return (
        _frame(names, items)
        .select(
            pl.col("ts_code").cast(pl.Utf8).alias("symbol"),
            _day("ann_date").alias("ann_date"),
            pl.col("call_type").cast(pl.Utf8).replace_strict(_CALL_TYPES, default=None),
            pl.col("is_call")
            .cast(pl.Utf8)
            .replace_strict(_CALL_STAGES, default=None)
            .alias("stage"),
            _day("call_date").alias("call_date"),
            _float("call_price"),
            _float("call_price_tax").alias("call_price_after_tax"),
            _float("call_vol").alias("call_volume"),
            (_float("call_amount") * 10_000).alias("call_amount"),
            _day("call_reg_date").alias("record_date"),
            _day("payment_date").alias("payment_date"),
        )
        .filter(pl.all_horizontal(pl.col(k).is_not_null() for k in key))
        .unique(key, keep="last", maintain_order=True)
        .cast(CALLS_SCHEMA)
    )


def normalize_conversions(names: list[str], items: list[list]) -> pl.DataFrame:
    """``cb_share`` rows as ``convertible_bond_conversions`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=CONVERSIONS_SCHEMA)
    return (
        _frame(names, items)
        .select(
            pl.col("ts_code").cast(pl.Utf8).alias("symbol"),
            _day("end_date").alias("end_date"),
            _day("publish_date").alias("publish_date"),
            _float("issue_size"),
            _float("convert_price_initial").alias("first_conversion_price"),
            _float("convert_price").alias("conversion_price"),
            _float("convert_val").alias("converted_amount"),
            _float("convert_vol").alias("converted_shares"),
            _float("convert_ratio").alias("converted_pct"),
            _float("acc_convert_val").alias("total_converted_amount"),
            _float("acc_convert_vol").alias("total_converted_shares"),
            _float("acc_convert_ratio").alias("total_converted_pct"),
            _float("remain_size").alias("remaining_size"),
            _float("total_shares"),
        )
        .filter(pl.col("symbol").is_not_null() & pl.col("end_date").is_not_null())
        .unique(["symbol", "end_date"], keep="last", maintain_order=True)
        .cast(CONVERSIONS_SCHEMA)
    )


def normalize_coupons(names: list[str], items: list[list]) -> pl.DataFrame:
    """``cb_rate`` rows as ``convertible_bond_coupons`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=COUPONS_SCHEMA)
    return (
        _frame(names, items)
        .select(
            pl.col("ts_code").cast(pl.Utf8).alias("symbol"),
            _day("rate_start_date").alias("start_date"),
            _day("rate_end_date").alias("end_date"),
            _float("coupon_rate"),
            pl.col("rate_freq")
            .cast(pl.Float64, strict=False)
            .cast(pl.Int64)
            .alias("payments_per_year"),
        )
        .filter(pl.col("symbol").is_not_null() & pl.col("start_date").is_not_null())
        .unique(["symbol", "start_date"], keep="last", maintain_order=True)
        .cast(COUPONS_SCHEMA)
    )


_APIS = {
    "convertible_bond_calls": ("cb_call", normalize_calls),
    "convertible_bond_conversions": ("cb_share", normalize_conversions),
    "convertible_bond_coupons": ("cb_rate", normalize_coupons),
}


def fetch_bond_events(
    dataset: str, symbol: str, *, config, client: TushareClient | None = None
) -> pl.DataFrame:
    """One bond's whole history for *dataset*. Raises without a token."""
    api, normalize = _APIS[dataset]

    def run(c: TushareClient) -> pl.DataFrame:
        return normalize(*c.all_rows(api, {"ts_code": symbol}, "", page_size=_PAGE_SIZE))

    if client is not None:
        return run(client)
    with TushareClient(config) as own:
        return run(own)
