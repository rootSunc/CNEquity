"""Company disclosures from Tushare: earnings forecasts, revenue segments, investor visits.

* ``earnings_forecasts`` (``forecast``): each 业绩预告, by announcement date,
  from 2004. Profits arrive in 万元 and are stored in CNY. The source sends
  some announcements twice, once with ``update_flag`` 1 and once with 0;
  the flagged copy is kept.
* ``business_segments`` (``fina_mainbz_vip``): revenue, cost and gross profit
  per product, region and industry segment, per report period from 2004.
  Amounts are in ``currency`` units (almost all CNY; a few hundred rows USD).
  Beijing stocks are often sent under both the legacy and the 920xxx code;
  the row filed under the current code wins.
* ``investor_visits`` (``stk_surv``): one row per visiting organisation per
  institutional visit, from 2021-08. The source has no natural key, so exact
  repeats are dropped and ``event_id`` is a hash of every field. ``--`` in
  the source means "not given" and is stored as null.

Beijing-exchange legacy codes are stored under the current 920xxx code.
"""

from __future__ import annotations

import hashlib
from datetime import date

import polars as pl

from cnequity.adapters.eastmoney.corporate_actions_migration import _code_mapping
from cnequity.adapters.tushare.client import SOURCE, TushareClient

__all__ = [
    "BUSINESS_SEGMENTS_FLOOR",
    "EARNINGS_FORECASTS_FLOOR",
    "INVESTOR_VISITS_FLOOR",
    "SEGMENT_TYPES",
    "SOURCE",
    "fetch_business_segments",
    "fetch_earnings_forecasts",
    "fetch_investor_visits",
    "normalize_business_segments",
    "normalize_earnings_forecasts",
    "normalize_investor_visits",
    "repeated_page",
]

EARNINGS_FORECASTS_FLOOR = date(2004, 1, 1)
BUSINESS_SEGMENTS_FLOOR = date(2004, 3, 31)
INVESTOR_VISITS_FLOOR = date(2021, 8, 1)

#: ``fina_mainbz_vip`` ``type`` → stored ``segment_type``.
SEGMENT_TYPES = {"P": "product", "D": "region", "I": "industry"}
# i18n-ignore-start: source values
_FORECAST_TYPES = {
    "预增": "increase",
    "略增": "slight_increase",
    "续盈": "continued_profit",
    "扭亏": "turnaround",
    "预减": "decrease",
    "略减": "slight_decrease",
    "首亏": "first_loss",
    "续亏": "continued_loss",
    "减亏": "loss_narrowed",
    "增亏": "loss_widened",
    "不确定": "uncertain",
    "其他": "other",
}
_NOT_GIVEN = "--"
# i18n-ignore-end

EARNINGS_FORECASTS_SCHEMA = {
    "symbol": pl.Utf8,
    "ann_date": pl.Date,
    "period_end": pl.Date,
    "forecast_type": pl.Utf8,
    "change_min_pct": pl.Float64,
    "change_max_pct": pl.Float64,
    "net_profit_min": pl.Float64,
    "net_profit_max": pl.Float64,
    "prior_net_profit": pl.Float64,
    "first_ann_date": pl.Date,
    "summary": pl.Utf8,
    "change_reason": pl.Utf8,
}
BUSINESS_SEGMENTS_SCHEMA = {
    "symbol": pl.Utf8,
    "period_end": pl.Date,
    "segment_type": pl.Utf8,
    "segment": pl.Utf8,
    "revenue": pl.Float64,
    "cost": pl.Float64,
    "gross_profit": pl.Float64,
    "currency": pl.Utf8,
}
INVESTOR_VISITS_SCHEMA = {
    "event_id": pl.Utf8,
    "symbol": pl.Utf8,
    "visit_date": pl.Date,
    "visitor_org": pl.Utf8,
    "org_type": pl.Utf8,
    "visitors": pl.Utf8,
    "place": pl.Utf8,
    "method": pl.Utf8,
    "company_hosts": pl.Utf8,
}

_PAGE_SIZE = {"forecast": 3000, "fina_mainbz_vip": 10000, "stk_surv": 400}


def _day(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d", strict=False)


def _symbol() -> pl.Expr:
    legacy_bj = {f"{old}.BJ": f"{new}.BJ" for old, new in _code_mapping().items()}
    return pl.col("ts_code").cast(pl.Utf8).str.strip_chars().replace(legacy_bj).alias("symbol")


def _float(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.Float64, strict=False)


def _text(column: str) -> pl.Expr:
    """Source text with blanks and ``--`` as null."""
    value = pl.col(column).cast(pl.Utf8).str.strip_chars()
    return pl.when(value.is_in(["", _NOT_GIVEN])).then(None).otherwise(value).alias(column)


def _frame(names: list[str], items: list[list]) -> pl.DataFrame:
    return pl.DataFrame(items, schema=names, orient="row", infer_schema_length=None)


def normalize_earnings_forecasts(names: list[str], items: list[list]) -> pl.DataFrame:
    """``forecast`` rows as ``earnings_forecasts`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=EARNINGS_FORECASTS_SCHEMA)
    key = ["symbol", "ann_date", "period_end"]
    frame = (
        _frame(names, items)
        .select(
            _symbol(),
            _day("ann_date").alias("ann_date"),
            _day("end_date").alias("period_end"),
            pl.col("type")
            .cast(pl.Utf8)
            .replace_strict(_FORECAST_TYPES, default=None)
            .alias("forecast_type"),
            _float("p_change_min").alias("change_min_pct"),
            _float("p_change_max").alias("change_max_pct"),
            (_float("net_profit_min") * 10_000).alias("net_profit_min"),
            (_float("net_profit_max") * 10_000).alias("net_profit_max"),
            (_float("last_parent_net") * 10_000).alias("prior_net_profit"),
            _day("first_ann_date").alias("first_ann_date"),
            _text("summary"),
            _text("change_reason"),
            pl.col("update_flag").cast(pl.Utf8).eq("1").fill_null(False).alias("_flagged"),
        )
        .filter(pl.all_horizontal(pl.col(k).is_not_null() for k in key))
    )
    return (
        frame.sort("_flagged", maintain_order=True)
        .unique(key, keep="last", maintain_order=True)
        .sort(key)
        .select(list(EARNINGS_FORECASTS_SCHEMA))
        .cast(EARNINGS_FORECASTS_SCHEMA)
    )


def normalize_business_segments(names: list[str], items: list[list]) -> pl.DataFrame:
    """``fina_mainbz_vip`` rows as ``business_segments`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=BUSINESS_SEGMENTS_SCHEMA)
    key = ["symbol", "period_end", "segment_type", "segment"]
    return (
        _frame(names, items)
        .select(
            _symbol(),
            _day("end_date").alias("period_end"),
            pl.col("bz_code")
            .cast(pl.Utf8)
            .replace_strict(SEGMENT_TYPES, default=None)
            .alias("segment_type"),
            pl.col("bz_item").cast(pl.Utf8).str.strip_chars().alias("segment"),
            _float("bz_sales").alias("revenue"),
            _float("bz_cost").alias("cost"),
            _float("bz_profit").alias("gross_profit"),
            pl.col("curr_type").cast(pl.Utf8).alias("currency"),
            (pl.col("ts_code").cast(pl.Utf8).str.strip_chars() == _symbol()).alias("_current"),
        )
        .filter(pl.all_horizontal(pl.col(k).is_not_null() for k in key))
        .sort("_current", maintain_order=True)
        .unique(key, keep="last", maintain_order=True)
        .select(list(BUSINESS_SEGMENTS_SCHEMA))
        .cast(BUSINESS_SEGMENTS_SCHEMA)
    )


def normalize_investor_visits(names: list[str], items: list[list]) -> pl.DataFrame:
    """``stk_surv`` rows as ``investor_visits`` columns, minus provenance."""
    if not items:
        return pl.DataFrame(schema=INVESTOR_VISITS_SCHEMA)
    frame = (
        _frame(names, items)
        .select(
            _symbol(),
            _day("surv_date").alias("visit_date"),
            _text("rece_org").alias("visitor_org"),
            _text("org_type"),
            _text("fund_visitors").alias("visitors"),
            _text("rece_place").alias("place"),
            _text("rece_mode").alias("method"),
            _text("comp_rece").alias("company_hosts"),
        )
        .filter(pl.col("symbol").is_not_null() & pl.col("visit_date").is_not_null())
    )
    fields = [c for c in INVESTOR_VISITS_SCHEMA if c != "event_id"]
    frame = frame.unique(fields, keep="first", maintain_order=True)
    keys = [
        hashlib.sha1("\x1f".join("" if v is None else str(v) for v in row).encode()).hexdigest()[
            :20
        ]
        for row in frame.select(fields).iter_rows()
    ]
    return (
        frame.with_columns(pl.Series("event_id", keys, dtype=pl.Utf8))
        .select(list(INVESTOR_VISITS_SCHEMA))
        .cast(INVESTOR_VISITS_SCHEMA)
    )


def _fetch(api: str, params: dict, normalize, config, client: TushareClient | None):
    page = _PAGE_SIZE[api]
    if client is not None:
        return normalize(*client.all_rows(api, params, "", page_size=page))
    with TushareClient(config) as own:
        return normalize(*own.all_rows(api, params, "", page_size=page))


def fetch_earnings_forecasts(day: date, *, config, client: TushareClient | None = None):
    """Forecasts announced on one day. Raises ``TushareNotConfigured`` without a token."""
    params = {"ann_date": day.strftime("%Y%m%d")}
    return _fetch("forecast", params, normalize_earnings_forecasts, config, client)


def fetch_business_segments(period_end: date, *, config, client: TushareClient | None = None):
    """Every segment of one report period, all three segment types."""

    def run(c: TushareClient) -> pl.DataFrame:
        frames = [
            normalize_business_segments(
                *c.all_rows(
                    "fina_mainbz_vip",
                    {"period": period_end.strftime("%Y%m%d"), "type": code},
                    "",
                    page_size=_PAGE_SIZE["fina_mainbz_vip"],
                )
            )
            for code in SEGMENT_TYPES
        ]
        return pl.concat(frames)

    if client is not None:
        return run(client)
    with TushareClient(config) as own:
        return run(own)


def repeated_page(items: list[list], page_size: int) -> int | None:
    """Index of the first page that mostly repeats earlier pages, else ``None``.

    Some relays answer an offset page with rows already sent, which drops the
    rows that belonged there; a day with such a page is incomplete.
    """
    seen: set[tuple] = set()
    for start in range(0, len(items), page_size):
        page = [tuple(row) for row in items[start : start + page_size]]
        if start and sum(row in seen for row in page) * 2 > len(page):
            return start // page_size
        seen.update(page)
    return None


def fetch_investor_visits(day: date, *, config, client: TushareClient | None = None):
    """Visits held on one day. ``stk_surv`` serves at most 400 rows a call.

    Raises ``RuntimeError`` when a page repeats earlier ones, rather than
    storing an incomplete day.
    """
    params = {"trade_date": day.strftime("%Y%m%d")}
    page = _PAGE_SIZE["stk_surv"]

    def run(c: TushareClient) -> pl.DataFrame:
        names, items = c.all_rows("stk_surv", params, "", page_size=page)
        bad = repeated_page(items, page)
        if bad is not None:
            raise RuntimeError(f"tushare stk_surv {day}: page {bad} repeats earlier rows")
        return normalize_investor_visits(names, items)

    if client is not None:
        return run(client)
    with TushareClient(config) as own:
        return run(own)
