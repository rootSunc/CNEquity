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

from cnequity.config import Config
from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.domain.datasets import DATASETS
from cnequity.domain.partitions import partition_value, partition_value_expr
from cnequity.domain.schemas import PRIMARY_KEYS, sanitize_dataset_rows, validate_dataframe
from cnequity.file_lock import lake_mutation_lock
from cnequity.query.parquet_scan import list_partitions
from cnequity.storage.atomic import swap_with_backup, write_parquet_atomic
from cnequity.storage.locator import logical_root

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


def _has_sanitizable_rows(dataset: str, files: list[Path]) -> bool:
    """Detect rows that need cleanup even when partition layout is current."""
    if dataset not in {"daily_bars", "index_bars", "trading_calendar"}:
        return False
    for path in files:
        scan = pl.scan_parquet(str(path))
        if "trade_date" not in scan.collect_schema().names():
            continue
        predicate = pl.col("trade_date").dt.weekday() > 5
        if dataset in {"daily_bars", "index_bars"}:
            from cnequity.domain.holidays_cn import CLOSED_DATES

            predicate |= pl.col("trade_date").dt.strftime("%Y-%m-%d").is_in(CLOSED_DATES)
        if dataset == "trading_calendar":
            if "is_trading" not in scan.collect_schema().names():
                continue
            from cnequity.domain.holidays_cn import CLOSED_DATES

            predicate |= pl.col("trade_date").dt.strftime("%Y-%m-%d").is_in(CLOSED_DATES)
            predicate &= pl.col("is_trading")
        count = scan.filter(predicate).select(pl.len()).collect().item()
        if count:
            return True
    return False


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
        from cnequity.storage.revisions import RevisionStore

        store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
        if store.current_pointer(dataset) is not None:
            # A published dataset is read through its committed generation;
            # swapping the mutable tree would change nothing a reader sees and
            # the next compact would overwrite it. Publish a revision instead.
            return _repartition_published(config, store, dataset, dry_run=dry_run)
        return _repartition_dataset_locked(config, dataset, dry_run=dry_run)


def _repartition_published(
    config: Config,
    store,
    dataset: str,
    *,
    dry_run: bool = False,
) -> RepartitionResult:
    """Rewrite a published dataset's history at its period, one period at a time.

    Starts from the committed generation (``materialize_current``), builds each
    target partition from the old ones it covers — read in file order and
    PK-deduped exactly as compact merges — and publishes the result as one
    gated revision. Rows are never sanitised here: a layout change must not
    change data. Memory holds one target period, not the dataset, which is
    what makes this usable on daily_bars (20M rows).
    """
    from datetime import datetime, timezone

    from cnequity.domain.contracts import contract_fingerprint, dataset_contract
    from cnequity.quality.publication import check_repair_publication
    from cnequity.storage.parquet import CuratedWriter

    spec = DATASETS.get(dataset)
    if spec is None or spec.partition_col is None:
        raise RepartitionError(f"{dataset} has no partitions to rewrite")
    col = spec.partition_col
    granularity = spec.partition_granularity
    if not dry_run:
        store.materialize_current(dataset)
    root = logical_root(config, spec) if not dry_run else store.current_root(dataset)
    partitions = list_partitions(root, col, resolve=False)
    if not partitions:
        raise RepartitionError(f"no partitions under {root}")

    targets: dict[str, list] = {}
    for part in partitions:
        targets.setdefault(partition_value(part.start, granularity), []).append(part)
    files_before = sorted(root.glob("**/*.parquet"))
    result = RepartitionResult(
        dataset=dataset,
        rows=0,
        partitions_before=len(partitions),
        partitions_after=len(targets),
        files_before=len(files_before),
        files_after=0,
        bytes_before=_dir_bytes(files_before),
        bytes_after=0,
        changed=False,
    )
    stale = {
        value: parts for value, parts in targets.items() if [p.value for p in parts] != [value]
    }
    if not stale:
        result.files_after, result.bytes_after = result.files_before, result.bytes_before
        return result

    writer = CuratedWriter(root.parent)
    changed: list[Path] = []
    rows_in = rows_out = 0
    for value, parts in sorted(targets.items()):
        files = [
            path
            for part in sorted(parts, key=lambda p: (p.start, p.value))
            for path in sorted((root / f"{col}={part.value}").glob("**/*.parquet"))
        ]
        frame = pl.concat([pl.read_parquet(path) for path in files], how="diagonal_relaxed")
        rows_in += frame.height
        merged = dedupe_by_primary_key(frame, dataset)
        rows_out += merged.height
        if value not in stale:
            continue
        if dry_run:
            result.files_after += 1
            continue
        target = writer.write_partition(dataset, col, value, merged, "part-merged.parquet")
        if merged.height != pl.read_parquet(target).height:
            raise RepartitionError(f"{dataset}: {col}={value} did not read back whole")
        changed.append(target)
        for part in parts:
            if part.value != value:
                shutil.rmtree(root / f"{col}={part.value}")
    result.rows = rows_out
    if rows_in != rows_out:
        logger.info(
            "repartition %s: %d duplicate PK row(s) folded, as compact would",
            dataset,
            rows_in - rows_out,
        )
    if dry_run:
        result.files_after += sum(1 for value in targets if value not in stale)
        return result

    run_id = f"repartition-{dataset}-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    publication = check_repair_publication(config, dataset, run_id, changed)
    contract = dataset_contract(dataset)
    revision = store.commit(
        dataset,
        run_id=run_id,
        changed_files=changed,
        schema_version=int(contract["schema_version"]),
        contract_fingerprint=contract_fingerprint(contract),
        metadata={
            "reason": "repartition",
            "granularity": granularity,
            "partitions_before": result.partitions_before,
            "partitions_after": result.partitions_after,
            "duplicate_rows_folded": rows_in - rows_out,
            "publication_audit": publication["report_path"],
        },
        _locked=True,
    )
    if revision is None:
        raise RepartitionError(f"{dataset}: repartition produced no revision")
    files_after = sorted(root.glob("**/*.parquet"))
    result.files_after = len(files_after)
    result.bytes_after = _dir_bytes(files_after)
    result.changed = True
    logger.info(
        "repartitioned %s to %s as revision %s: %d→%d files",
        dataset,
        granularity,
        revision.revision,
        result.files_before,
        result.files_after,
    )
    return result


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

    root = logical_root(config, spec)
    partitions = list_partitions(root, spec.partition_col)
    if not partitions:
        raise RepartitionError(f"no partitions under {root}")

    files_before = sorted(root.glob("**/*.parquet"))
    needs_cleanup = _has_sanitizable_rows(dataset, files_before)
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
    if already and not needs_cleanup:
        result.partitions_after = result.partitions_before
        result.files_after = result.files_before
        result.bytes_after = result.bytes_before
        return result

    # Whole-dataset read: the datasets that need this are the small ones, and a
    # partial rewrite cannot guarantee the row-count check below.
    frames = [
        sanitize_dataset_rows(validate_dataframe(pl.read_parquet(f), dataset), dataset)
        for f in files_before
    ]
    combined = pl.concat(frames, how="diagonal_relaxed")
    # Same PK dedupe as compact: a granularity flip often leaves day dirs beside
    # the new year/month dirs, and a naive concat would bake those overlaps into
    # the rewritten files. Keep the freshest row per PK.
    pk = PRIMARY_KEYS.get(dataset, [])
    if pk and all(c in combined.columns for c in pk):
        before = combined.height
        combined = dedupe_by_primary_key(combined, dataset)
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
    part_values = combined.select(
        partition_value_expr(pl.col(spec.partition_col), spec.partition_granularity)
    ).to_series()

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
    swap_with_backup(tmp_root, root, old_root)
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
        root = logical_root(config, spec)
        parts = list_partitions(root, spec.partition_col)
        if not parts:
            continue
        if any(p.value != partition_value(p.start, spec.partition_granularity) for p in parts):
            out.append(name)
    return out
