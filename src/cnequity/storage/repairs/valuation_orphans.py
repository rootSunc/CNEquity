"""Drop valuation_metrics rows for symbols that never appear in daily_bars.

Early baostock backfills swept the instruments list and wrote PE/PB for delisted
/ never-traded names (e.g. 退市创兴). The daily snapshot and later backfills
filter to ``load_bar_universe``, but historical orphan rows remain until purged.
Audit flag: ``valuation_bars_orphan_symbol``.
"""

from __future__ import annotations

import logging

import polars as pl

from cnequity.config import Config
from cnequity.file_lock import lake_mutation_lock
from cnequity.query.parquet_scan import scan_parquet_root
from cnequity.steps.common import load_bar_universe
from cnequity.storage.repairs.orphan_symbols import _publish
from cnequity.storage.revisions import RevisionStore

logger = logging.getLogger(__name__)

_DATASET = "valuation_metrics"
_PARTITION_COL = "trade_date"


def purge_valuation_orphan_symbols(config: Config) -> dict:
    """Publish a revision without the valuation rows of symbols absent from bars.

    Returns ``{orphan_symbols, partitions_rewritten, rows_removed}`` plus the
    published ``revision``. No-op when bars are empty (cannot reconcile) or
    when no orphans exist.
    """
    # This is a read-modify-write over curated partitions and must not race
    # compact, repartition, or another maintenance operation.
    with lake_mutation_lock(config.meta_root, blocking=True):
        return _purge_valuation_orphan_symbols_locked(config)


def _purge_valuation_orphan_symbols_locked(config: Config) -> dict:
    """Implementation of :func:`purge_valuation_orphan_symbols` under lock."""
    nothing = {"orphan_symbols": [], "partitions_rewritten": 0, "rows_removed": 0}
    bar_universe = load_bar_universe(config)
    if not bar_universe:
        return {**nothing, "note": "no daily_bars; skipped"}

    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
    # Read the published generation: editing the mutable tree in place is
    # invisible to readers and undone by the next compact's materialize.
    base = store.repair_base(_DATASET, register=False)
    if base is None:
        return nothing
    scan = scan_parquet_root(base, partition_col=_PARTITION_COL)
    val_syms = set(scan.select("symbol").unique().collect()["symbol"].to_list())
    orphans = sorted(val_syms - bar_universe)
    if not orphans:
        return nothing
    removed = scan.filter(pl.col("symbol").is_in(orphans)).select(pl.len()).collect().item()

    base = store.repair_base(_DATASET, register=True)
    published = _publish(
        config,
        store,
        _DATASET,
        _PARTITION_COL,
        base,
        keep=lambda frame: frame.filter(~pl.col("symbol").is_in(orphans)),
        reason="valuation_orphan_purge",
        metadata={"orphan_symbols": len(orphans)},
    )
    logger.info(
        "purged %d valuation orphan symbol(s) (%d rows): %s",
        len(orphans),
        removed,
        ", ".join(orphans[:8]) + ("…" if len(orphans) > 8 else ""),
    )
    return {
        "orphan_symbols": orphans,
        "partitions_rewritten": published["partitions_rewritten"] + published["partitions_emptied"],
        "rows_removed": int(removed),
        "revision": published["revision"],
    }
