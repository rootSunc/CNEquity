"""Universe filtering for the query reader."""

from __future__ import annotations

from datetime import date

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.domain.symbols import (
    CDR_PREFIXES,
    ETF_PREFIXES,
    EXCLUDED_PREFIXES,
    PREFIX_WHITELIST,
)
from cn_market_lake.query.parquet_scan import (
    collect_parquet_root,
    coverage_start_from_partitions,
    scan_parquet_root,
)

EXCLUDED_STATUSES = frozenset({"st", "*st", "suspended"})


def _all_a_symbol_expr(symbol_col: str = "symbol") -> pl.Expr:
    code = pl.col(symbol_col).str.split(".").list.first()
    exchange = pl.col(symbol_col).str.split(".").list.last()
    excluded = pl.lit(False)
    # 81–89 is a SH/SZ reservation; do not apply it to BJ (legacy 83xxxx).
    for prefix in EXCLUDED_PREFIXES:
        excluded = excluded | (exchange.is_in(["SH", "SZ"]) & code.str.starts_with(prefix))
    # CDRs (SH 689xxx) are depositary receipts, not common stock; they stay in
    # the lake but are not part of the all_a selection universe.
    for prefix in CDR_PREFIXES:
        excluded = excluded | ((exchange == "SH") & code.str.starts_with(prefix))
    # ETFs/LOFs stay in instruments + daily_bars for UI/quotes, but never enter
    # the all_a research universe (PREFIX_WHITELIST also omits them).
    for exch, prefixes in ETF_PREFIXES.items():
        for prefix in prefixes:
            excluded = excluded | ((exchange == exch) & code.str.starts_with(prefix))
    allowed = pl.lit(False)
    for exch, prefixes in PREFIX_WHITELIST.items():
        for prefix in prefixes:
            allowed = allowed | ((exchange == exch) & code.str.starts_with(prefix))
    return (~excluded) & allowed


def _load_instruments(config: Config) -> pl.DataFrame:
    root = config.curated_root / "instruments"
    if not root.exists():
        return pl.DataFrame()
    try:
        return collect_parquet_root(root, hive=False)
    except FileNotFoundError:
        return pl.DataFrame()


def _load_trading_status(
    config: Config,
    *,
    trade_date: date | None = None,
) -> pl.DataFrame:
    root = config.curated_root / "trading_status"
    if not root.exists():
        return pl.DataFrame()
    try:
        return collect_parquet_root(
            root,
            partition_col="trade_date",
            start=trade_date,
            end=trade_date,
        )
    except FileNotFoundError:
        return pl.DataFrame()


def coverage_start_date(
    config: Config,
    dataset: str,
    *,
    date_col: str = "trade_date",
) -> date | None:
    """Earliest *date_col* present in curated *dataset*, if any."""
    root = config.curated_root / dataset
    min_dt = coverage_start_from_partitions(root, date_col)
    if min_dt is not None:
        return min_dt
    if not root.exists():
        return None
    try:
        return (
            scan_parquet_root(root, partition_col=date_col)
            .select(pl.col(date_col).min())
            .collect()
            .item()
        )
    except FileNotFoundError:
        return None


def trading_status_coverage_start(config: Config) -> date | None:
    """First trade_date with curated trading_status rows."""
    return coverage_start_date(config, "trading_status")


def st_coverage_start(config: Config) -> date | None:
    """First trade_date with an ``st``/``*st`` label in trading_status.

    Suspension is reconstructed from bar gaps (whole history), but ST labels
    come only from live ST snapshots, so this is the real ST-history boundary.
    """
    root = config.curated_root / "trading_status"
    if not root.exists():
        return None
    try:
        return (
            scan_parquet_root(root, partition_col="trade_date")
            .filter(pl.col("status").is_in(["st", "*st"]))
            .select(pl.col("trade_date").min())
            .collect()
            .item()
        )
    except FileNotFoundError:
        return None


def tradable_symbols_on_date(
    config: Config,
    trade_date: date,
    *,
    universe: str = "all_a",
) -> pl.DataFrame | None:
    """Return ``symbol`` rows tradable on *trade_date* for the given universe rule.

    Applies list/delist dates from ``instruments``. ST/suspended filtering uses
    ``trading_status`` only when rows exist for *trade_date*; dates before
    :func:`trading_status_coverage_start` are not ST-filtered.
    """
    if universe != "all_a":
        raise ValueError(f"unsupported universe: {universe!r} (supported: 'all_a')")

    instruments = _load_instruments(config)
    if instruments.is_empty():
        return None

    out = (
        instruments.filter(_all_a_symbol_expr())
        .filter(pl.col("list_date").is_null() | (pl.col("list_date") <= trade_date))
        .filter(pl.col("delist_date").is_null() | (pl.col("delist_date") >= trade_date))
        .select("symbol")
    )
    if out.is_empty():
        return pl.DataFrame(schema={"symbol": pl.Utf8})

    status = _load_trading_status(config, trade_date=trade_date)
    if status.is_empty():
        return out

    bad = status.filter((~pl.col("is_trading")) | pl.col("status").is_in(list(EXCLUDED_STATUSES)))[
        "symbol"
    ]
    if not bad.is_empty():
        out = out.filter(~pl.col("symbol").is_in(bad))
    return out


def apply_universe_filter(
    df: pl.DataFrame,
    config: Config,
    *,
    universe: str,
    date_col: str = "trade_date",
) -> pl.DataFrame:
    """Filter bar-like frames to tradable universe rows per *date_col*.

    ``instruments`` list/delist rules always apply. ST/suspended removal via
    ``trading_status`` only affects dates with status rows; earlier history
    passes through unchanged (see :func:`trading_status_coverage_start`).
    """
    if df.is_empty() or universe != "all_a":
        return df

    instruments = _load_instruments(config)
    if instruments.is_empty():
        return df

    valid_symbols = instruments.filter(_all_a_symbol_expr())["symbol"]
    inst = instruments.select(["symbol", "list_date", "delist_date"])
    df = df.join(inst, on="symbol", how="left")
    df = df.filter(
        pl.col("list_date").is_null() | (pl.col("list_date") <= pl.col(date_col))
    ).filter(pl.col("delist_date").is_null() | (pl.col("delist_date") >= pl.col(date_col)))
    if not valid_symbols.is_empty():
        df = df.filter(pl.col("symbol").is_in(valid_symbols.to_list()))

    if date_col not in df.columns:
        return df.drop(["list_date", "delist_date"], strict=False)

    try:
        status = scan_parquet_root(
            config.curated_root / "trading_status",
            partition_col="trade_date",
        )
    except FileNotFoundError:
        return df.drop(["list_date", "delist_date"], strict=False)

    bad = (
        status.filter((~pl.col("is_trading")) | pl.col("status").is_in(list(EXCLUDED_STATUSES)))
        .select(["symbol", pl.col("trade_date").alias(date_col)])
        .collect()
    )
    if bad.is_empty():
        return df.drop(["list_date", "delist_date"], strict=False)

    df = df.join(bad, on=["symbol", date_col], how="anti")
    return df.drop(["list_date", "delist_date"], strict=False)
