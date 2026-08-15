"""Drop valuation_metrics rows for symbols that never appear in daily_bars.

Early baostock backfills swept the instruments list and wrote PE/PB for delisted
/ never-traded names (e.g. 退市创兴). The daily snapshot and later backfills
filter to ``load_bar_universe``, but historical orphan rows remain until purged.
Audit flag: ``valuation_bars_orphan_symbol``.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.file_lock import lake_mutation_lock
from cn_market_lake.steps.common import load_bar_universe
from cn_market_lake.storage.atomic import write_parquet_atomic

logger = logging.getLogger(__name__)


def purge_valuation_orphan_symbols(config: Config) -> dict:
    """Rewrite curated valuation partitions, dropping symbols absent from bars.

    Returns ``{orphan_symbols, partitions_rewritten, rows_removed}``. No-op when
    bars are empty (cannot reconcile) or when no orphans exist.
    """
    # This is a read-modify-write over curated partitions and must not race
    # compact, repartition, or another maintenance operation.
    with lake_mutation_lock(config.meta_root, blocking=True):
        return _purge_valuation_orphan_symbols_locked(config)


def _purge_valuation_orphan_symbols_locked(config: Config) -> dict:
    """Implementation of :func:`purge_valuation_orphan_symbols` under lock."""
    bar_universe = load_bar_universe(config)
    if not bar_universe:
        return {
            "orphan_symbols": [],
            "partitions_rewritten": 0,
            "rows_removed": 0,
            "note": "no daily_bars; skipped",
        }

    val_root = config.curated_root / "valuation_metrics"
    if not val_root.exists():
        return {"orphan_symbols": [], "partitions_rewritten": 0, "rows_removed": 0}

    files = list(val_root.glob("**/*.parquet"))
    if not files:
        return {"orphan_symbols": [], "partitions_rewritten": 0, "rows_removed": 0}

    val_syms = set(pl.scan_parquet(files).select("symbol").unique().collect()["symbol"].to_list())
    orphans = sorted(val_syms - bar_universe)
    if not orphans:
        return {"orphan_symbols": [], "partitions_rewritten": 0, "rows_removed": 0}

    orphan_set = set(orphans)
    rewritten = 0
    removed = 0
    for path in files:
        df = pl.read_parquet(path)
        if "symbol" not in df.columns:
            continue
        hit = df.filter(pl.col("symbol").is_in(list(orphan_set)))
        if hit.is_empty():
            continue
        keep = df.filter(~pl.col("symbol").is_in(list(orphan_set)))
        removed += hit.height
        if keep.is_empty():
            path.unlink(missing_ok=True)
            _maybe_rmdir(path.parent)
        else:
            write_parquet_atomic(path, keep, compression="zstd")
        rewritten += 1

    logger.info(
        "purged %d valuation orphan symbol(s) (%d rows across %d partitions): %s",
        len(orphans),
        removed,
        rewritten,
        ", ".join(orphans[:8]) + ("…" if len(orphans) > 8 else ""),
    )
    return {
        "orphan_symbols": orphans,
        "partitions_rewritten": rewritten,
        "rows_removed": removed,
    }


def _maybe_rmdir(path: Path) -> None:
    try:
        if path.is_dir() and not any(path.iterdir()):
            path.rmdir()
    except OSError:
        pass
