"""SSE / SZSE ETF options from Tushare (``opt_basic`` + ``opt_daily``).

The commodity and index options in ``option_bars`` come from the futures
exchanges' own files; the two stock exchanges publish no such archive, so
their ETF options (510050 from 2015-02-09, then 510300, 510500, 588000,
588080, 159901, 159915, 159919, 159922) come from Tushare. Rows are stored
under exchange ``SH`` / ``SZ`` with the exchange's contract code as symbol
(``10004401.SH``), the ETF as ``underlying_symbol`` and the ETF code as
``product``.

The contract list is authoritative for dates and terms (European, contract
unit usually 10,000 shares, adjusted after an ETF dividend). Daily volume and
open interest are in contracts; amount arrives in 万元 and is stored in CNY
(5,228 contracts at about 0.12 traded 620.1 on 2026-11-04).
"""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.adapters.tushare.client import SOURCE, TushareClient

__all__ = [
    "ETF_OPTION_EXCHANGES",
    "ETF_OPTIONS_FLOOR",
    "SOURCE",
    "contracts_from_basic",
    "fetch_contracts",
    "fetch_day",
    "normalize_daily",
]

#: The lake's exchange codes for the two stock exchanges' options.
ETF_OPTION_EXCHANGES = ("SH", "SZ")
#: 50ETF options began trading on 2015-02-09.
ETF_OPTIONS_FLOOR = date(2015, 2, 9)

_TUSHARE_EXCHANGES = {"SSE": "SH", "SZSE": "SZ"}
_PAGE_SIZE = 3000


def _day(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d", strict=False)


def contracts_from_basic(frame: pl.DataFrame) -> pl.DataFrame:
    """``opt_basic`` rows as ``option_contracts`` columns (minus seen dates and provenance)."""
    if frame.is_empty():
        return pl.DataFrame()
    underlying = pl.col("opt_code").cast(pl.Utf8).str.strip_prefix("OP")
    return (
        frame.filter(pl.col("exchange").is_in(list(_TUSHARE_EXCHANGES)))
        .select(
            pl.col("ts_code").alias("symbol"),
            pl.col("exchange").replace_strict(_TUSHARE_EXCHANGES).alias("exchange"),
            pl.col("symbol").cast(pl.Utf8).alias("exchange_code"),
            underlying.str.split(".").list.first().alias("product"),
            (underlying.str.split(".").list.first() + "ETF期权").alias(
                "product_name"
            ),  # i18n-ignore
            underlying.alias("underlying_symbol"),
            pl.lit("etf").alias("underlying_kind"),
            pl.col("call_put").cast(pl.Utf8).alias("option_type"),
            pl.col("exercise_price").cast(pl.Float64).alias("strike"),
            pl.when(pl.col("exercise_type") == "欧式")  # i18n-ignore: source value
            .then(pl.lit("european"))
            .when(pl.col("exercise_type") == "美式")  # i18n-ignore: source value
            .then(pl.lit("american"))
            .otherwise(None)
            .alias("exercise_style"),
            (pl.col("s_month").cast(pl.Utf8) + "01")
            .str.strptime(pl.Date, "%Y%m%d", strict=False)
            .alias("expiry_month"),
            _day("list_date").alias("list_date"),
            _day("maturity_date").alias("expiry_date"),
            pl.col("per_unit").cast(pl.Float64).alias("multiplier"),
            pl.col("min_price_chg").cast(pl.Float64).alias("tick_size"),
            pl.lit("exchange").alias("dates_basis"),
            # Observation dates belong to the bars; callers that hold them fill these.
            pl.lit(None, dtype=pl.Date).alias("first_seen_date"),
            pl.lit(None, dtype=pl.Date).alias("last_seen_date"),
        )
        .unique("symbol", keep="last")
    )


def normalize_daily(names: list[str], items: list[list], contracts: pl.DataFrame) -> pl.DataFrame:
    """``opt_daily`` rows joined to their contract terms, as ``option_bars`` columns."""
    if not items:
        return pl.DataFrame()
    frame = pl.DataFrame(items, schema=names, orient="row")
    bars = frame.select(
        pl.col("ts_code").alias("symbol"),
        _day("trade_date").alias("trade_date"),
        *[pl.col(c).cast(pl.Float64, strict=False) for c in ("open", "high", "low", "close")],
        pl.col("settle").cast(pl.Float64, strict=False),
        pl.col("pre_settle").cast(pl.Float64, strict=False),
        pl.col("vol").cast(pl.Float64, strict=False).round(0).cast(pl.Int64).alias("volume"),
        (pl.col("amount").cast(pl.Float64, strict=False) * 10_000.0).alias("amount"),
        pl.col("oi").cast(pl.Float64, strict=False).round(0).cast(pl.Int64).alias("open_interest"),
    )
    terms = contracts.select(
        "symbol",
        "exchange",
        "exchange_code",
        "product",
        "underlying_symbol",
        "option_type",
        "strike",
    )
    return bars.join(terms, on="symbol", how="inner").with_columns(
        pl.lit(None, dtype=pl.Int64).alias("oi_change"),
        pl.lit(None, dtype=pl.Int64).alias("exercise_volume"),
        pl.lit(None, dtype=pl.Float64).alias("delta"),
        pl.lit(None, dtype=pl.Float64).alias("implied_vol"),
        pl.lit(None, dtype=pl.Float64).alias("series_implied_vol"),
    )


def fetch_contracts(config, *, client: TushareClient | None = None) -> pl.DataFrame:
    """Every listed and expired ETF option contract on both exchanges."""

    def run(c: TushareClient) -> pl.DataFrame:
        frames = [
            c.query_all("opt_basic", {"exchange": ex}, page_size=_PAGE_SIZE)
            for ex in _TUSHARE_EXCHANGES
        ]
        return contracts_from_basic(pl.concat(frames, how="diagonal_relaxed"))

    if client is not None:
        return run(client)
    with TushareClient(config) as own:
        return run(own)


def fetch_day(
    trade_date: date, contracts: pl.DataFrame, *, config, client: TushareClient | None = None
) -> pl.DataFrame:
    """One session's bars on both exchanges."""

    def run(c: TushareClient) -> pl.DataFrame:
        parts = []
        for ex in _TUSHARE_EXCHANGES:
            names, items = c.all_rows(
                "opt_daily",
                {"trade_date": trade_date.strftime("%Y%m%d"), "exchange": ex},
                "",
                page_size=_PAGE_SIZE,
            )
            part = normalize_daily(names, items, contracts)
            if not part.is_empty():
                parts.append(part)
        return pl.concat(parts, how="diagonal_relaxed") if parts else pl.DataFrame()

    if client is not None:
        return run(client)
    with TushareClient(config) as own:
        return run(own)
