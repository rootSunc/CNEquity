"""Read-only, per-security daily-bar coverage; watermarks cannot prove this."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.domain.symbols import filter_ingest_universe
from cnequity.domain.trading_status import (
    CURRENT_SNAPSHOT_SOURCES,
    DERIVED_BAR_GAP_SOURCE,
    EVIDENCE_POINT_IN_TIME,
    evidence_rank_expr,
)
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.steps.common import (
    instrument_metadata,
    list_trading_dates,
    load_curated_trading_status,
)

KEYS = {"symbol": pl.Utf8, "trade_date": pl.Date}


def missing_bar_keys(
    instruments: pl.DataFrame,
    bars: pl.DataFrame,
    status: pl.DataFrame,
    sessions: list[date],
) -> pl.DataFrame:
    """Find absent keys with known listing spans and no explicit suspension.

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
    if not status.is_empty():
        # Legacy gap-derived halts are circular evidence: a missing bar must
        # not prove its own absence is legitimate. Restated live boards also
        # cannot establish a historical session's status.
        trusted = pl.lit(False)
        if "source" in status.columns:
            rank = evidence_rank_expr(status.schema)
            trusted = pl.col("source").is_not_null() & (
                (rank == EVIDENCE_POINT_IN_TIME)
                if rank is not None
                else ~pl.col("source").is_in([DERIVED_BAR_GAP_SOURCE, *CURRENT_SNAPSHOT_SOURCES])
            )
        # Judge on the trusted rows alone, and let a trusted disagreement — not
        # an untrusted row's presence — be what withholds the verdict.
        # Requiring *every* row on the key to be a trusted halt makes an
        # untrusted one a veto: a circular `derived_bar_gap` row sitting beside
        # a vendor's explicit suspension would drag the key back to "missing"
        # even though the vendor answered. Today the two rarely share a key —
        # on the 2005-2015 repair the gap-derived and Baostock rows landed on
        # disjoint symbols — but they converge as the vendor backfill catches
        # up with what the lake had inferred, and that is exactly when the
        # inference must stop outvoting the source.
        states = pl.col("is_trading").is_not_null() & trusted
        suspended = (
            status.group_by(*KEYS)
            .agg(
                (states & ~pl.col("is_trading")).any().alias("halted"),
                (states & pl.col("is_trading")).any().alias("traded"),
            )
            .filter(pl.col("halted") & ~pl.col("traded"))
            .select(*KEYS)
        )
        covered = pl.concat([covered, suspended]).unique()
    return expected.join(covered, on=list(KEYS), how="anti").sort(*KEYS)


def daily_bar_coverage(config: Config, start: date, end: date) -> dict:
    """Inspect all configured securities, including wholly absent series.

    Bound cross products to 64 sessions, so a historical audit does not build
    the entire market's decades of expected keys in memory at once.
    """
    if start > end:
        raise ValueError("start must be on or before end")
    instruments = instrument_metadata(config)
    scope = filter_ingest_universe(instruments["symbol"].to_list(), config.ingest_universe)
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
        missing = missing_bar_keys(instruments, bars, status, chunk)
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
