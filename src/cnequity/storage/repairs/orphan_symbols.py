"""Drop factor and action rows for securities the lake holds no prices for.

Open-end fund codes (519xxx) once reached ``daily_bars`` as NAV series. The
bars were purged, but their ``adj_factors`` and ``corporate_actions`` rows
stayed: a factor with no price to adjust, and dividends no factor can step
on. Every such row reads as a factor/action contradiction and adjusts
nothing. Listed funds whose prices the lake never collected land here too.

The repair reads the committed generations, rewrites only partitions that
hold such rows, and publishes one revision per dataset through the repair
gate. Previous revisions stay readable.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from cnequity.config import Config
from cnequity.file_lock import lake_mutation_lock
from cnequity.query.parquet_scan import scan_parquet_root
from cnequity.storage.repairs.publish import publish_repair
from cnequity.storage.revisions import RevisionStore

logger = logging.getLogger(__name__)

_DATASETS = {"adj_factors": "trade_date", "corporate_actions": "ex_date"}


def _symbols(root: Path, partition_col: str) -> set[str]:
    if not root.is_dir():
        return set()
    return set(
        scan_parquet_root(root, partition_col=partition_col)
        .select("symbol")
        .unique()
        .collect()
        .get_column("symbol")
        .to_list()
    )


def repair_orphan_symbols(config: Config, *, apply: bool = False) -> dict:
    """Plan (default) or publish removal of rows for securities with no bars."""
    with lake_mutation_lock(config.meta_root, blocking=True):
        return _repair_locked(config, apply=apply)


def _repair_locked(config: Config, *, apply: bool) -> dict:
    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
    bars = store.repair_base("daily_bars", register=False)
    priced = _symbols(bars, "trade_date") if bars is not None else set()
    report: dict = {"applied": False, "datasets": {}}
    if not priced:
        report["note"] = "no daily_bars data; nothing to repair"
        return report
    for dataset, partition_col in _DATASETS.items():
        base = store.repair_base(dataset, register=apply)
        if base is None:
            report["datasets"][dataset] = {
                "orphan_symbols": 0,
                "sample": [],
                "rows": 0,
                "note": "no data; nothing to repair",
            }
            continue
        orphans = sorted(_symbols(base, partition_col) - priced)
        entry: dict = {"orphan_symbols": len(orphans), "sample": orphans[:20], "rows": 0}
        report["datasets"][dataset] = entry
        if not orphans:
            continue
        hit = (
            scan_parquet_root(base, partition_col=partition_col)
            .filter(pl.col("symbol").is_in(orphans))
            .group_by(partition_col)
            .len()
            .collect()
        )
        entry.update(rows=int(hit.get_column("len").sum()), partitions=hit.height)
        if apply:
            dropped = list(orphans)
            entry.update(
                publish_repair(
                    config,
                    store,
                    dataset,
                    partition_col,
                    base,
                    keep=lambda frame, d=dropped: frame.filter(~pl.col("symbol").is_in(d)),
                    reason="orphan_symbol_repair",
                    metadata={"orphan_symbols": len(orphans)},
                )
            )
    report["applied"] = apply
    return report


def repair_stale_derived_suspensions(config: Config, *, apply: bool = False) -> dict:
    """Plan (default) or remove ``derived_bar_gap`` rows a traded bar contradicts.

    Such a row says a security was suspended because its bar was missing. Once
    the bar is fetched it is wrong: 223 Beijing rows for 2026-08-07 were
    inferred from a run that lost the Beijing leg, not from a halt.
    """
    with lake_mutation_lock(config.meta_root, blocking=True):
        return _repair_stale_locked(config, apply=apply)


def _repair_stale_locked(config: Config, *, apply: bool) -> dict:
    from cnequity.domain.canonical import dedupe_lazy_by_primary_key
    from cnequity.domain.trading_status import DERIVED_BAR_GAP_SOURCE

    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
    base = store.repair_base("trading_status", register=apply)
    bars_root = store.repair_base("daily_bars", register=False)
    report: dict = {"applied": False, "rows": 0}
    if base is None or bars_root is None:
        report["note"] = "no trading_status or daily_bars data; nothing to repair"
        return report
    derived = (
        scan_parquet_root(base, partition_col="trade_date")
        .filter(pl.col("source") == DERIVED_BAR_GAP_SOURCE)
        .select("symbol", "trade_date")
        .unique()
        .collect()
    )
    if derived.is_empty():
        return report
    traded = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(
                bars_root,
                partition_col="trade_date",
                symbols=derived.get_column("symbol").unique().to_list(),
            ),
            "daily_bars",
        )
        .filter(pl.col("volume").fill_null(0) > 0)
        .select("symbol", "trade_date")
        .collect()
    )
    stale = derived.join(traded, on=["symbol", "trade_date"])
    report.update(rows=stale.height, sample=[f"{s}|{d}" for s, d in stale.head(20).iter_rows()])
    if not apply or stale.is_empty():
        return report
    marked = stale.with_columns(pl.lit(True).alias("_stale"))

    def keep(frame: pl.DataFrame) -> pl.DataFrame:
        flagged = frame.join(marked, on=["symbol", "trade_date"], how="left")
        drop = (pl.col("source") == DERIVED_BAR_GAP_SOURCE) & pl.col("_stale").fill_null(False)
        return flagged.filter(~drop).drop("_stale")

    report.update(
        publish_repair(
            config,
            store,
            "trading_status",
            "trade_date",
            base,
            keep=keep,
            reason="stale_derived_suspensions",
            metadata={"rows": stale.height},
        )
    )
    report["applied"] = True
    return report
