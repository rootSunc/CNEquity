"""Read-only, per-security daily-bar coverage; watermarks cannot prove this."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.domain.symbols import filter_ingest_universe, is_subscription_placeholder
from cnequity.domain.trading_status import independent_suspension_keys
from cnequity.query.calendar import list_trading_dates
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.instrument_catalog import (
    load_curated_instruments,
    load_curated_trading_status,
)

KEYS = {"symbol": pl.Utf8, "trade_date": pl.Date}


def missing_bar_keys(
    instruments: pl.DataFrame,
    bars: pl.DataFrame,
    status: pl.DataFrame,
    sessions: list[date],
    *,
    no_print_keys: set[tuple[str, date]] | None = None,
) -> pl.DataFrame:
    """Find absent keys without suspension or verified fund no-print evidence.

    Source-empty negative caches only suppress retries; they do not establish
    that a security did not trade. They deliberately cannot certify coverage.
    Conflicting trading-status rows cannot certify suspension either.
    """
    if not sessions or instruments.is_empty():
        return pl.DataFrame(schema=KEYS)
    expected = (
        instruments.select("symbol", "list_date", "delist_date")
        .join(pl.DataFrame({"trade_date": sessions}, schema={"trade_date": pl.Date}), how="cross")
        .filter(
            pl.col("list_date").is_not_null()
            & (pl.col("trade_date") >= pl.col("list_date"))
            & (pl.col("delist_date").is_null() | (pl.col("trade_date") < pl.col("delist_date")))
        )
        .select(*KEYS)
        .unique()
    )
    covered = bars.select(*KEYS).unique()
    if no_print_keys:
        covered = pl.concat(
            [covered, pl.DataFrame(sorted(no_print_keys), schema=KEYS, orient="row")]
        ).unique()
    if not status.is_empty():
        suspended = independent_suspension_keys(status)
        covered = pl.concat([covered, suspended]).unique()
    return expected.join(covered, on=list(KEYS), how="anti").sort(*KEYS)


def daily_bar_coverage(config: Config, start: date, end: date) -> dict:
    """Inspect all configured securities, including wholly absent series.

    Bound cross products to 64 sessions, so a historical audit does not build
    the entire market's decades of expected keys in memory at once.
    """
    if start > end:
        raise ValueError("start must be on or before end")
    instruments = load_curated_instruments(config)
    if instruments is None:
        instruments = pl.DataFrame(
            schema={"symbol": pl.String, "list_date": pl.Date, "delist_date": pl.Date}
        )
    if not instruments.is_empty():
        instruments = instruments.filter(
            pl.Series(
                [
                    not is_subscription_placeholder(row.get("name"), row.get("symbol"))
                    for row in instruments.iter_rows(named=True)
                ]
            )
        )
    for column in ("list_date", "delist_date"):
        if column not in instruments.columns:
            instruments = instruments.with_columns(pl.lit(None, dtype=pl.Date).alias(column))
    scope = filter_ingest_universe(instruments["symbol"].to_list(), config.ingest_universe)
    if config.ingest_listed_funds:
        from cnequity.storage.instrument_catalog import listed_fund_symbols

        scope = sorted(set(scope) | set(listed_fund_symbols(instruments, end, start=start)))
    instruments = instruments.filter(pl.col("symbol").is_in(scope))
    unknown = instruments.filter(pl.col("list_date").is_null())["symbol"].unique().sort().to_list()
    sessions = list_trading_dates(config, start, end)
    root = config.curated_root / "daily_bars"
    has_bars = dataset_has_parquet(root)
    summaries: dict[str, dict] = {}
    total = 0
    for offset in range(0, len(sessions), 64):
        chunk = sessions[offset : offset + 64]
        bars = (
            scan_parquet_root(
                root, partition_col="trade_date", start=chunk[0], end=chunk[-1], traded_only=True
            )
            .select(*KEYS)
            .collect(engine="streaming")
            if has_bars
            else pl.DataFrame(schema=KEYS)
        )
        status = load_curated_trading_status(config, start=chunk[0], end=chunk[-1], symbols=scope)
        if status is None:
            status = pl.DataFrame(schema={**KEYS, "is_trading": pl.Boolean})
        from cnequity.storage.fund_no_print import approved_no_print_keys, quiet_fund_keys

        no_print = approved_no_print_keys(config, start=chunk[0], end=chunk[-1]) | quiet_fund_keys(
            config, start=chunk[0], end=chunk[-1]
        )
        missing = missing_bar_keys(instruments, bars, status, chunk, no_print_keys=no_print)
        total += missing.height
        for symbol, day in missing.iter_rows():
            row = summaries.setdefault(
                symbol,
                {
                    "symbol": symbol,
                    "missing_sessions": 0,
                    "start": str(day),
                    "end": str(day),
                    "sample": [],
                },
            )
            row["missing_sessions"] += 1
            row["end"] = str(day)
            if len(row["sample"]) < 10:
                row["sample"].append(str(day))
    return {
        "window": {"start": str(start), "end": str(end)},
        "universe": config.ingest_universe,
        "scope_symbols": len(scope),
        "trading_sessions": len(sessions),
        "unresolved_keys": total,
        "unresolved_symbols": len(summaries),
        "unknown_listing_symbols": unknown,
        "calendar_available": bool(sessions),
        "complete": bool(sessions) and not total and not unknown and bool(scope),
        "gaps": [summaries[symbol] for symbol in sorted(summaries)],
        "limitations": [
            "Known listing spans and explicit trading-status evidence define expected keys.",
            "Missing source responses do not prove suspension; unresolved keys require source verification.",
        ],
    }
