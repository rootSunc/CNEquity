"""Popularity ranks, limit-up lists and THS money flow, from Tushare.

* ``popularity_ranks`` (``ths_hot`` from 2023-11, ``dc_hot`` from 2024-03):
  the THS and EastMoney popularity lists per session. Several snapshots can
  arrive for one day; each symbol keeps its latest ranking of the day. THS's
  futures list is dropped because its codes are not contract codes. Codes
  outside the A-share market (HK, US, boards) are stored as the source sends
  them.
* ``limit_ladder`` (``limit_step``), from 2023-11: stocks on a run of
  consecutive limit-ups and the length of the run.
* ``strongest_sectors`` (``limit_cpt_list``), from 2023-11: THS concept boards
  ranked by limit-up activity.
* ``kpl_limit_ups`` (``kpl_list``), from 2018-01: 开盘啦's limit-up list, with
  seal times, reasons, themes, seal order amounts and main-force net flow.
  Only the limit-up list is collected (``tag`` 涨停); the source leaves its
  auction and intraday-change fields empty, so they are not stored.
* ``fund_flow_ths_tushare`` (``moneyflow_ths``), from 2024-12: THS's net flow
  per stock split into large, medium and small orders (the three add up to
  ``net_amount``). Amounts arrive in 万元 and are stored in yuan.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.adapters.tushare.client import SOURCE, TushareClient
from cnequity.domain.bse_codes import legacy_symbol_map

__all__ = [
    "FUND_FLOW_THS_TUSHARE_FLOOR",
    "KPL_LIMIT_UPS_FLOOR",
    "LIMIT_LADDER_FLOOR",
    "POPULARITY_RANKS_FLOOR",
    "SOURCE",
    "STRONGEST_SECTORS_FLOOR",
    "fetch_day",
    "normalize_fund_flow_ths",
    "normalize_kpl_limit_ups",
    "normalize_limit_ladder",
    "normalize_popularity_ranks",
    "normalize_strongest_sectors",
]

POPULARITY_RANKS_FLOOR = date(2023, 10, 1)
LIMIT_LADDER_FLOOR = date(2023, 10, 1)
STRONGEST_SECTORS_FLOOR = date(2023, 10, 1)
KPL_LIMIT_UPS_FLOOR = date(2017, 10, 1)
FUND_FLOW_THS_TUSHARE_FLOOR = date(2024, 10, 1)

# i18n-ignore-start: source values
_LISTS = {
    "ths": {
        "热股": "stock",
        "ETF": "etf",
        "可转债": "convertible_bond",
        "热基": "fund",
        "港股": "hk_stock",
        "美股": "us_stock",
        "概念板块": "concept_board",
        "行业板块": "industry_board",
    },
    "eastmoney": {
        "A股市场": "stock",
        "ETF基金": "etf",
        "港股市场": "hk_stock",
        "美股市场": "us_stock",
    },
}
# i18n-ignore-end

POPULARITY_RANKS_SCHEMA = {
    "trade_date": pl.Date,
    "provider": pl.Utf8,
    "list": pl.Utf8,
    "symbol": pl.Utf8,
    "name": pl.Utf8,
    "rank": pl.Int64,
    "pct_change": pl.Float64,
    "price": pl.Float64,
    "heat": pl.Float64,
    "concepts": pl.Utf8,
    "reason": pl.Utf8,
    "rank_time": pl.Utf8,
}
LIMIT_LADDER_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "consecutive_limits": pl.Int64,
}
STRONGEST_SECTORS_SCHEMA = {
    "trade_date": pl.Date,
    "sector_code": pl.Utf8,
    "sector_name": pl.Utf8,
    "rank": pl.Int64,
    "days_listed": pl.Int64,
    "limit_stat": pl.Utf8,
    "consecutive_limit_stocks": pl.Int64,
    "limit_up_stocks": pl.Int64,
    "pct_change": pl.Float64,
}
KPL_LIMIT_UPS_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "status": pl.Utf8,
    "first_limit_time": pl.Utf8,
    "last_limit_time": pl.Utf8,
    "reason": pl.Utf8,
    "theme": pl.Utf8,
    "amount": pl.Float64,
    "turnover_rate": pl.Float64,
    "free_float_cap": pl.Float64,
    "main_net_amount": pl.Float64,
    "limit_order_amount": pl.Float64,
    "max_limit_order_amount": pl.Float64,
}
FUND_FLOW_THS_TUSHARE_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "close": pl.Float64,
    "change_pct": pl.Float64,
    "net_amount": pl.Float64,
    "net_amount_5d": pl.Float64,
    "large_net_amount": pl.Float64,
    "large_net_rate": pl.Float64,
    "medium_net_amount": pl.Float64,
    "medium_net_rate": pl.Float64,
    "small_net_amount": pl.Float64,
    "small_net_rate": pl.Float64,
}

_PAGE_SIZE = {
    "ths_hot": 2000,
    "dc_hot": 2000,
    "limit_step": 2000,
    "limit_cpt_list": 2000,
    "kpl_list": 8000,
    "moneyflow_ths": 6000,
}


def _day(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d", strict=False)


def _float(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.Float64, strict=False)


def _int(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.Float64, strict=False).cast(pl.Int64)


def _text(column: str) -> pl.Expr:
    value = pl.col(column).cast(pl.Utf8).str.strip_chars()
    return pl.when(value == "").then(None).otherwise(value).alias(column)


def _symbol() -> pl.Expr:
    legacy_bj = legacy_symbol_map()
    return _text("ts_code").replace(legacy_bj).alias("symbol")


def _frame(names: list[str], items: list[list]) -> pl.DataFrame:
    return pl.DataFrame(items, schema=names, orient="row", infer_schema_length=None)


def normalize_popularity_ranks(provider: str, names: list[str], items: list[list]) -> pl.DataFrame:
    """``ths_hot`` (*provider* ``ths``) or ``dc_hot`` (``eastmoney``) rows."""
    if not items:
        return pl.DataFrame(schema=POPULARITY_RANKS_SCHEMA)
    frame = _frame(names, items)
    if "rank_reason" not in frame.columns:
        frame = frame.with_columns(pl.lit(None, dtype=pl.Utf8).alias("rank_reason"))
    key = ["trade_date", "provider", "list", "symbol"]
    return (
        frame.select(
            _day("trade_date").alias("trade_date"),
            pl.lit(provider).alias("provider"),
            pl.col("data_type")
            .cast(pl.Utf8)
            .replace_strict(_LISTS[provider], default=None)
            .alias("list"),
            _symbol(),
            _text("ts_name").alias("name"),
            _int("rank"),
            _float("pct_change"),
            _float("current_price").alias("price"),
            _float("hot").alias("heat"),
            _text("concept").alias("concepts"),
            _text("rank_reason").alias("reason"),
            _text("rank_time").alias("rank_time"),
        )
        .filter(pl.all_horizontal(pl.col(k).is_not_null() for k in key))
        .sort("rank_time", nulls_last=False, maintain_order=True)
        .unique(key, keep="last", maintain_order=True)
        .cast(POPULARITY_RANKS_SCHEMA)
    )


def normalize_limit_ladder(names: list[str], items: list[list]) -> pl.DataFrame:
    """``limit_step`` rows as ``limit_ladder`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=LIMIT_LADDER_SCHEMA)
    return (
        _frame(names, items)
        .select(
            _symbol(),
            _day("trade_date").alias("trade_date"),
            _int("nums").alias("consecutive_limits"),
        )
        .filter(pl.col("symbol").is_not_null() & pl.col("trade_date").is_not_null())
        .unique(["symbol", "trade_date"], keep="last", maintain_order=True)
        .cast(LIMIT_LADDER_SCHEMA)
    )


def normalize_strongest_sectors(names: list[str], items: list[list]) -> pl.DataFrame:
    """``limit_cpt_list`` rows as ``strongest_sectors`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=STRONGEST_SECTORS_SCHEMA)
    return (
        _frame(names, items)
        .select(
            _day("trade_date").alias("trade_date"),
            _text("ts_code").str.replace(r"\.TI$", "").alias("sector_code"),
            _text("name").alias("sector_name"),
            _int("rank"),
            _int("days").alias("days_listed"),
            _text("up_stat").alias("limit_stat"),
            _int("cons_nums").alias("consecutive_limit_stocks"),
            _int("up_nums").alias("limit_up_stocks"),
            _float("pct_chg").alias("pct_change"),
        )
        .filter(pl.col("trade_date").is_not_null() & pl.col("sector_code").is_not_null())
        .unique(["trade_date", "sector_code"], keep="last", maintain_order=True)
        .cast(STRONGEST_SECTORS_SCHEMA)
    )


def normalize_kpl_limit_ups(names: list[str], items: list[list]) -> pl.DataFrame:
    """``kpl_list`` (tag 涨停) rows as ``kpl_limit_ups`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=KPL_LIMIT_UPS_SCHEMA)
    return (
        _frame(names, items)
        .select(
            _symbol(),
            _day("trade_date").alias("trade_date"),
            _text("status"),
            _text("lu_time").alias("first_limit_time"),
            _text("last_time").alias("last_limit_time"),
            _text("lu_desc").alias("reason"),
            _text("theme"),
            _float("amount"),
            _float("turnover_rate"),
            _float("free_float").alias("free_float_cap"),
            _float("net_change").alias("main_net_amount"),
            _float("limit_order").alias("limit_order_amount"),
            _float("lu_limit_order").alias("max_limit_order_amount"),
        )
        .filter(pl.col("symbol").is_not_null() & pl.col("trade_date").is_not_null())
        .unique(["symbol", "trade_date"], keep="last", maintain_order=True)
        .cast(KPL_LIMIT_UPS_SCHEMA)
    )


def normalize_fund_flow_ths(names: list[str], items: list[list]) -> pl.DataFrame:
    """``moneyflow_ths`` rows as ``fund_flow_ths_tushare`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=FUND_FLOW_THS_TUSHARE_SCHEMA)

    def yuan(column: str) -> pl.Expr:
        return _float(column) * 10_000

    return (
        _frame(names, items)
        .select(
            _symbol(),
            _day("trade_date").alias("trade_date"),
            _float("latest").alias("close"),
            _float("pct_change").alias("change_pct"),
            yuan("net_amount").alias("net_amount"),
            yuan("net_d5_amount").alias("net_amount_5d"),
            yuan("buy_lg_amount").alias("large_net_amount"),
            _float("buy_lg_amount_rate").alias("large_net_rate"),
            yuan("buy_md_amount").alias("medium_net_amount"),
            _float("buy_md_amount_rate").alias("medium_net_rate"),
            yuan("buy_sm_amount").alias("small_net_amount"),
            _float("buy_sm_amount_rate").alias("small_net_rate"),
        )
        .filter(pl.col("symbol").is_not_null() & pl.col("trade_date").is_not_null())
        .unique(["symbol", "trade_date"], keep="last", maintain_order=True)
        .cast(FUND_FLOW_THS_TUSHARE_SCHEMA)
    )


_NORMALIZERS = {
    "popularity_ranks": None,
    "limit_ladder": ("limit_step", normalize_limit_ladder),
    "strongest_sectors": ("limit_cpt_list", normalize_strongest_sectors),
    "kpl_limit_ups": ("kpl_list", normalize_kpl_limit_ups),
    "fund_flow_ths_tushare": ("moneyflow_ths", normalize_fund_flow_ths),
}


def fetch_day(dataset: str, day: date, *, config, client: TushareClient | None = None):
    """One session of *dataset*. Raises ``TushareNotConfigured`` without a token."""
    params = {"trade_date": day.strftime("%Y%m%d")}

    def run(c: TushareClient) -> pl.DataFrame:
        if dataset == "popularity_ranks":
            frames = [
                normalize_popularity_ranks(
                    provider, *c.all_rows(api, params, "", page_size=_PAGE_SIZE[api])
                )
                for provider, api in (("ths", "ths_hot"), ("eastmoney", "dc_hot"))
            ]
            return pl.concat(frames)
        api, normalize = _NORMALIZERS[dataset]
        return normalize(*c.all_rows(api, params, "", page_size=_PAGE_SIZE[api]))

    if client is not None:
        return run(client)
    with TushareClient(config) as own:
        return run(own)
