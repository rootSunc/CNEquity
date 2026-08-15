"""Rewrite a dataset's partitions at its configured granularity.

Reads keep working whatever period the directories on disk span (see
``domain/partitions.parse_partition``), so collapsing fine leftovers into the
configured period is usually an optimisation — it reclaims the space and the
file opens that a too-fine partitioning wastes. When a granularity flip left
day directories beside year/month ones, the rewrite also PK-dedupes (same rule
as compact) so the overlap is not baked into the new files.

New writes already land at the configured granularity; running this once
brings the history into line. The rewrite is staged then swapped: the new
partitions are built under a sibling ``.repartition-tmp`` directory and only
replace the live ones once every partition is written and the (post-dedupe)
row count is confirmed. A crash mid-way leaves the original untouched.
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.domain.datasets import DATASETS
from cn_market_lake.domain.partitions import partition_value
from cn_market_lake.domain.schemas import PRIMARY_KEYS, validate_dataframe
from cn_market_lake.file_lock import lake_mutation_lock
from cn_market_lake.query.parquet_scan import list_partitions, partition_dir
from cn_market_lake.storage.atomic import write_parquet_atomic

logger = logging.getLogger(__name__)

_TMP_SUFFIX = ".repartition-tmp"


class RepartitionError(RuntimeError):
    """Raised when a rewrite would lose rows or the dataset cannot be rewritten."""


@dataclass
class RepartitionResult:
    dataset: str
    rows: int
    partitions_before: int
    partitions_after: int
    files_before: int
    files_after: int
    bytes_before: int
    bytes_after: int
    changed: bool

    @property
    def bytes_saved(self) -> int:
        return self.bytes_before - self.bytes_after


def _dir_bytes(paths: list[Path]) -> int:
    return sum(f.stat().st_size for f in paths if f.is_file())


def repartition_dataset(
    config: Config,
    dataset: str,
    *,
    dry_run: bool = False,
) -> RepartitionResult:
    """Rewrite *dataset* so each directory spans its configured period."""
    # Repartition swaps the live dataset root after reading the old one.  It
    # must share compact's mutation lock or it can promote a stale snapshot and
    # discard rows compact wrote concurrently.
    with lake_mutation_lock(config.meta_root, blocking=True):
        return _repartition_dataset_locked(config, dataset, dry_run=dry_run)


def _repartition_dataset_locked(
    config: Config,
    dataset: str,
    *,
    dry_run: bool = False,
) -> RepartitionResult:
    """Implementation of :func:`repartition_dataset` under the mutation lock."""
    spec = DATASETS.get(dataset)
    if spec is None:
        raise RepartitionError(f"unknown dataset {dataset!r}")
    if spec.partition_col is None:
        raise RepartitionError(f"{dataset} is merge-style (single file); nothing to repartition")

    root = (config.derived_root if spec.layer == "derived" else config.curated_root) / dataset
    partitions = list_partitions(root, spec.partition_col)
    if not partitions:
        raise RepartitionError(f"no partitions under {root}")

    files_before = sorted(root.glob("**/*.parquet"))
    target_values = {partition_value(p.start, spec.partition_granularity) for p in partitions}
    already = all(
        p.value == partition_value(p.start, spec.partition_granularity) for p in partitions
    )

    result = RepartitionResult(
        dataset=dataset,
        rows=0,
        partitions_before=len(partitions),
        partitions_after=len(target_values),
        files_before=len(files_before),
        files_after=0,
        bytes_before=_dir_bytes(files_before),
        bytes_after=0,
        changed=False,
    )
    if already:
        result.partitions_after = result.partitions_before
        result.files_after = result.files_before
        result.bytes_after = result.bytes_before
        return result

    # Whole-dataset read: the datasets that need this are the small ones, and a
    # partial rewrite cannot guarantee the row-count check below.
    frames = [validate_dataframe(pl.read_parquet(f), dataset) for f in files_before]
    combined = pl.concat(frames, how="diagonal_relaxed")
    # Same PK dedupe as compact: a granularity flip often leaves day dirs beside
    # the new year/month dirs, and a naive concat would bake those overlaps into
    # the rewritten files. Keep the freshest row per PK.
    pk = PRIMARY_KEYS.get(dataset, [])
    if pk and all(c in combined.columns for c in pk):
        before = combined.height
        combined = combined.sort("fetched_at").unique(subset=pk, keep="last")
        dropped = before - combined.height
        if dropped:
            logger.info(
                "repartition %s: dropped %d duplicate PK row(s) across overlapping partitions",
                dataset,
                dropped,
            )
    rows = combined.height
    result.rows = rows

    col = combined.get_column(spec.partition_col)
    if col.dtype != pl.Date:
        raise RepartitionError(
            f"{dataset}.{spec.partition_col} is {col.dtype}, not a date; "
            "period partitioning applies to date keys only"
        )
    granularity = spec.partition_granularity
    if granularity == "day":
        part_values = col.dt.strftime("%Y-%m-%d")
    elif granularity == "month":
        part_values = col.dt.strftime("%Y-%m")
    elif granularity == "quarter":
        # strftime has no quarter directive, so derive it from the month. Only
        # reachable for a Date-keyed dataset: report_period is a String column
        # and is rejected by the dtype check above.
        part_values = col.dt.strftime("%Y") + "Q" + ((col.dt.month() - 1) // 3 + 1).cast(pl.String)
    else:
        part_values = col.dt.strftime("%Y")

    tmp_root = root.parent / f"{dataset}{_TMP_SUFFIX}"
    if tmp_root.exists():
        shutil.rmtree(tmp_root)

    _PART = "__partition__"
    written = 0
    staged = combined.with_columns(part_values.alias(_PART))
    for key, group in staged.partition_by(_PART, as_dict=True).items():
        value = str(key[0] if isinstance(key, tuple) else key)
        out_dir = tmp_root / f"{spec.partition_col}={value}"
        out_dir.mkdir(parents=True, exist_ok=True)
        write_parquet_atomic(out_dir / "part-merged.parquet", group.drop(_PART), compression="zstd")
        written += group.height

    if written != rows:
        shutil.rmtree(tmp_root, ignore_errors=True)
        raise RepartitionError(
            f"{dataset}: rewrite produced {written} rows from {rows}; refusing to swap"
        )

    new_files = sorted(tmp_root.glob("**/*.parquet"))
    result.files_after = len(new_files)
    result.bytes_after = _dir_bytes(new_files)
    result.partitions_after = len(list(tmp_root.iterdir()))

    if dry_run:
        shutil.rmtree(tmp_root, ignore_errors=True)
        return result

    # Swap: move the old aside, promote the new, then drop the old. The window
    # where neither is at `root` is a single rename.
    old_root = root.parent / f"{dataset}{_TMP_SUFFIX}.old"
    if old_root.exists():
        shutil.rmtree(old_root)
    root.rename(old_root)
    try:
        tmp_root.rename(root)
    except Exception:
        old_root.rename(root)  # put it back before re-raising
        raise
    shutil.rmtree(old_root, ignore_errors=True)

    result.changed = True
    logger.info(
        "repartitioned %s to %s: %d→%d files, %.1f→%.1f MB",
        dataset,
        spec.partition_granularity,
        result.files_before,
        result.files_after,
        result.bytes_before / 1e6,
        result.bytes_after / 1e6,
    )
    return result


def repartition_candidates(config: Config) -> list[str]:
    """Datasets whose on-disk partitions do not match their configured period."""
    out: list[str] = []
    for name, spec in sorted(DATASETS.items()):
        if spec.partition_col is None:
            continue
        root = (config.derived_root if spec.layer == "derived" else config.curated_root) / name
        parts = list_partitions(root, spec.partition_col)
        if not parts:
            continue
        if any(p.value != partition_value(p.start, spec.partition_granularity) for p in parts):
            out.append(name)
    return out


def partition_dir_for(config: Config, dataset: str, value: str) -> Path:
    spec = DATASETS[dataset]
    root = (config.derived_root if spec.layer == "derived" else config.curated_root) / dataset
    return partition_dir(root, spec.partition_col or "", value)
