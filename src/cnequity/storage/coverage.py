"""Coverage evidence derived from the exact candidate or committed bytes."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from cnequity.domain.datasets import DATASETS
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root


def resolved_bj_session_dates(config, payload: dict, *, read_context=None) -> set[date]:
    """Retire only single-session BSE debts proved by committed observations.

    The roster is the known listing spans used to create this debt. Unknown
    listing dates remain a separate per-security coverage limitation, rather
    than retroactively making every past market session owed. An inferred
    suspension or unpublished repair cannot certify a missing session.
    """
    from cnequity.quality.bar_coverage import KEYS, missing_bar_keys
    from cnequity.query.canonical import dedupe_lazy_by_primary_key
    from cnequity.storage.instrument_catalog import (
        load_curated_instruments,
        load_curated_trading_status,
    )
    from cnequity.storage.read_context import ReadContext, read_root

    days = sorted(
        {
            date.fromisoformat(row["start"])
            for row in payload.get("missing_ranges", [])
            if row.get("last_failure") == "bj_session_incomplete"
            and row.get("start") == row.get("end")
        }
    )
    if not days:
        return set()
    context = read_context or ReadContext.capture(
        config, ("instruments", "daily_bars", "trading_status", "announcement_index"), {}
    )
    instruments = load_curated_instruments(config, context)
    if instruments is None or not {"symbol", "list_date", "delist_date"} <= set(
        instruments.columns
    ):
        return set()
    instruments = instruments.filter(pl.col("symbol").str.ends_with(".BJ"))
    root = read_root(config, "daily_bars", context)
    if instruments.is_empty() or not dataset_has_parquet(root, committed=False):
        return set()
    resolved = set()
    for day in days:
        roster = instruments.filter(
            pl.col("list_date").is_not_null()
            & (pl.col("list_date") <= day)
            & (pl.col("delist_date").is_null() | (pl.col("delist_date") > day))
        )
        if roster.is_empty():
            continue
        bars = (
            dedupe_lazy_by_primary_key(
                scan_parquet_root(
                    root,
                    partition_col="trade_date",
                    start=day,
                    end=day,
                    symbols=roster["symbol"].to_list(),
                    committed=False,
                ),
                "daily_bars",
            )
            .filter(pl.col("volume") > 0)
            .select(*KEYS)
            .collect()
        )
        status = load_curated_trading_status(
            config, start=day, end=day, symbols=roster["symbol"].to_list(), read_context=context
        )
        if status is None:
            status = pl.DataFrame(schema={**KEYS, "is_trading": pl.Boolean})
        if missing_bar_keys(roster, bars, status, [day]).is_empty():
            resolved.add(day)
    return resolved


def resolved_outstanding_keys(
    dataset: str, root: Path, owed: list[dict], *, committed: bool = True
) -> set[tuple[str, str]]:
    """Return only owed symbol/date identities actually present in this view."""
    if not owed or not dataset_has_parquet(root, committed=committed):
        return set()
    column = DATASETS[dataset].partition_col
    if not column or any(not row.get("symbol") or not row.get("trade_date") for row in owed):
        raise ValueError(f"invalid outstanding scope for {dataset}")
    symbols = sorted({row["symbol"] for row in owed})
    days = sorted({row["trade_date"] for row in owed})
    present = set(
        scan_parquet_root(root, partition_col=column, symbols=symbols, committed=committed)
        .select("symbol", pl.col(column).cast(pl.String).alias("_date"))
        .filter(pl.col("_date").is_between(pl.lit(days[0]), pl.lit(days[-1])))
        .unique()
        .collect()
        .iter_rows()
    )
    return present & {(row["symbol"], row["trade_date"]) for row in owed}


def resolved_prelisting_keys(
    config, owed: list[dict], *, read_context=None
) -> set[tuple[str, str]]:
    """Retire premature bar requests only when committed listing evidence agrees.

    A price preceding the listing date disproves that date. Unknown dates and
    disappeared instruments never certify absence.
    """
    from cnequity.domain.trading_status import split_before_listing
    from cnequity.storage.instrument_catalog import load_curated_instruments
    from cnequity.storage.read_context import ReadContext, read_root

    if not owed:
        return set()
    context = read_context or ReadContext.capture(config, ("instruments", "daily_bars"), {})
    listings = load_curated_instruments(config, context)
    if listings is None or not {"symbol", "list_date"} <= set(listings.columns):
        return set()
    rows = pl.DataFrame(owed).select("symbol", pl.col("trade_date").str.to_date())
    listings = listings.filter(pl.col("symbol").is_in(rows["symbol"].unique().to_list())).select(
        "symbol", "list_date"
    )
    root = read_root(config, "daily_bars", context)
    first = (
        scan_parquet_root(
            root,
            partition_col="trade_date",
            symbols=listings["symbol"].to_list(),
            traded_only=True,
            committed=False,
        )
        .group_by("symbol")
        .agg(pl.col("trade_date").min().alias("first_traded"))
        .collect()
        if not listings.is_empty() and dataset_has_parquet(root, committed=False)
        else pl.DataFrame(schema={"symbol": pl.String, "first_traded": pl.Date})
    )
    _, early = split_before_listing(rows, listings.join(first, on="symbol", how="left"))
    return {(symbol, str(day)) for symbol, day in early.iter_rows()}


def candidate_gaps(
    payload: dict, run_id: str, resolved: set[tuple[str, str]], *, full_staging: bool
) -> dict:
    """Describe post-publication gaps without mutating the current index."""
    days = set(payload.get("staged_request_days", {}).get(run_id, [])) if full_staging else set()
    units = set(payload.get("staged_units", {}).get(run_id, [])) if full_staging else set()
    return {
        "missing_ranges": [r for r in payload.get("missing_ranges", []) if r["start"] not in days],
        "missing_units": {
            key: value
            for key, value in payload.get("missing_units", {}).items()
            if key not in units
        },
        "outstanding_keys": [
            r
            for r in payload.get("outstanding_keys", [])
            if (r["symbol"], r["trade_date"]) not in resolved
        ],
    }


GAP_SAMPLE_LIMIT = 20


def gap_summary(gaps: dict) -> dict:
    """Bound a gap description for an immutable revision receipt.

    The full gap index already lives in dataset state and grows with the
    lake; copying it into every receipt made each publication rewrite it
    whole. A receipt keeps the counts and the first few entries of each kind.
    """
    summary: dict = {"counts": {}, "sample": {}}
    for kind, value in gaps.items():
        items = list(value.items()) if isinstance(value, dict) else list(value)
        summary["counts"][kind] = len(items)
        sample = items[:GAP_SAMPLE_LIMIT]
        summary["sample"][kind] = dict(sample) if isinstance(value, dict) else sample
    summary["truncated"] = any(count > GAP_SAMPLE_LIMIT for count in summary["counts"].values())
    return summary
