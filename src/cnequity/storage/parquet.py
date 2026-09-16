from __future__ import annotations

from pathlib import Path

import polars as pl

from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.domain.datasets import granularity_for_dataset
from cnequity.domain.partitions import Granularity
from cnequity.domain.pit import (
    PIT_DATASET_NAMES,
    PIT_STORAGE_COLUMNS,
    normalize_pit_storage_columns,
)
from cnequity.domain.schemas import PRIMARY_KEYS, sanitize_dataset_rows, validate_dataframe
from cnequity.storage.atomic import write_parquet_atomic
from cnequity.storage.revisions import sha256_file


def _business_equal(left: pl.DataFrame, right: pl.DataFrame) -> bool:
    """Compare business rows exactly, ignoring only observation-time churn.

    Reconciliation re-fetches the tail; fetched_at/observed_at alone must not
    mint a revision. Source, types, multiplicity and source-side PIT timestamps
    still matter. Sort inside Polars rather than serializing every row into
    Python dictionaries and JSON strings. This comparison is local to one
    compact operation, so no persisted digest or revision contract changes.
    """
    ignored = {"fetched_at", "observed_at"}
    columns = sorted(set(left.columns) - ignored)
    if left.height != right.height or columns != sorted(set(right.columns) - ignored):
        return False
    if any(left.schema[column] != right.schema[column] for column in columns):
        return False
    if not columns:
        return True
    return left.select(columns).sort(columns).equals(right.select(columns).sort(columns))


class StagingWriter:
    def __init__(self, staging_root: Path | str):
        # Process-pool workers may pass a str path across the boundary.
        self.staging_root = Path(staging_root)

    def write_batch(
        self,
        dataset: str,
        run_id: str,
        batch_id: str,
        df: pl.DataFrame,
    ) -> Path:
        df = sanitize_dataset_rows(validate_dataframe(df, dataset), dataset)
        out_dir = self.staging_root / dataset / f"run_id={run_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"part-{batch_id}.parquet"
        write_parquet_atomic(path, df, compression="zstd")
        return path

    def list_run_files(self, dataset: str, run_id: str) -> list[Path]:
        run_dir = self.staging_root / dataset / f"run_id={run_id}"
        if not run_dir.exists():
            return []
        return sorted(run_dir.rglob("*.parquet"))


class CuratedWriter:
    def __init__(self, curated_root: Path):
        self.curated_root = curated_root

    def partition_path(self, dataset: str, partition_col: str, partition_value: str) -> Path:
        return self.curated_root / dataset / f"{partition_col}={partition_value}"

    def write_partition(
        self,
        dataset: str,
        partition_col: str,
        partition_value: str,
        df: pl.DataFrame,
        part_name: str = "part-0.parquet",
    ) -> Path:
        out_dir = self.partition_path(dataset, partition_col, partition_value)
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / part_name
        write_parquet_atomic(path, df, compression="zstd")
        # A previous run may have left fragment files (for example after a
        # staging write used ``part-<batch>.parquet``), possibly under a
        # temporary subdirectory. Keeping them beside the canonical file makes
        # every later recursive scan read the same rows twice, even though the
        # merge above already deduplicated them. Remove only old parquet
        # descendants, and only after the atomic replacement has succeeded so
        # a failed write leaves the old partition readable.
        for stale in out_dir.rglob("*.parquet"):
            if stale != path:
                stale.unlink()
        return path


def _pit_columns_absent(path: Path, dataset: str) -> bool:
    """True when *path* is a PIT dataset file that predates the PIT columns.

    Reads the parquet footer only — the whole point is to avoid the row-wise
    derivation that a file without these columns forces on every reader.
    """
    if dataset not in PIT_DATASET_NAMES or not path.is_file():
        return False
    try:
        stored = set(pl.read_parquet_schema(path))
    except Exception:
        return False
    return not set(PIT_STORAGE_COLUMNS).issubset(stored)


def _partition_values(df: pl.DataFrame, partition_col: str, granularity: Granularity) -> pl.Series:
    """Directory value per row for *partition_col*.

    Date columns map through the dataset's period (day/month/year); non-date
    keys like ``report_period`` ("2024Q1") are already period labels and are
    used verbatim.
    """
    col = df.get_column(partition_col)
    if col.dtype != pl.Date:
        return col.cast(pl.Utf8)
    if granularity == "year":
        return col.dt.strftime("%Y")
    if granularity == "month":
        return col.dt.strftime("%Y-%m")
    return col.dt.strftime("%Y-%m-%d")


def compact_dataset(
    staging_root: Path,
    curated_root: Path,
    dataset: str,
    run_id: str,
    partition_col: str | None = "trade_date",
    granularity: Granularity | None = None,
    changed_files: list[Path] | None = None,
    base_root: Path | None = None,
) -> int:
    """Merge staging batches into curated partitions, dedupe by PK.

    Granularity decides how many dates share one partition directory; it comes
    from the dataset's registry entry unless overridden. See
    ``domain/partitions.py`` for why it is not always a day.
    """
    if granularity is None:
        granularity = granularity_for_dataset(dataset)
    staging = StagingWriter(staging_root)
    curated = CuratedWriter(curated_root)
    # ``base_root`` is the immutable committed generation selected before the
    # write.  It is optional for backwards compatibility with direct callers;
    # finalize passes it so a retry after a crash cannot merge from a half-
    # rewritten mutable compatibility directory.
    read_root = Path(base_root) if base_root is not None else curated_root / dataset
    files = staging.list_run_files(dataset, run_id)
    if not files:
        return 0

    # Re-validate on read: staging/curated written before a schema change
    # (e.g. fetched_at str → timestamp) must be normalized before concat.
    combined = pl.concat(
        [
            sanitize_dataset_rows(validate_dataframe(pl.read_parquet(f), dataset), dataset)
            for f in files
        ],
        how="diagonal_relaxed",
    )
    pk = PRIMARY_KEYS.get(dataset, [])
    if pk:
        combined = dedupe_by_primary_key(combined, dataset)

    def _pit(frame: pl.DataFrame) -> pl.DataFrame:
        """Materialize the bitemporal columns so they reach disk.

        They were previously derived on every read instead (19 s of row-wise
        hashing for one `financial_statement_items` scan). Applied to both
        sides of the digest comparison below so adding them is never mistaken
        for a business change.
        """
        return normalize_pit_storage_columns(frame, dataset)

    combined = _pit(combined)

    if partition_col not in combined.columns:
        out_dir = curated.curated_root / dataset
        out_path = out_dir / "part-merged.parquet"
        existing_dir = read_root
        existing_files = sorted(existing_dir.rglob("*.parquet")) if existing_dir.exists() else []
        before_digest = sha256_file(out_path) if out_path.is_file() else None
        had_fragments = any(
            path.relative_to(existing_dir) != Path(out_path.name) for path in existing_files
        )
        existing = pl.DataFrame(schema=combined.schema)
        if existing_files:
            existing = pl.concat(
                [
                    sanitize_dataset_rows(
                        validate_dataframe(pl.read_parquet(path), dataset), dataset
                    )
                    for path in existing_files
                ],
                how="diagonal_relaxed",
            )
            existing = _pit(existing)
            combined = pl.concat([existing, combined], how="diagonal_relaxed")
            if pk:
                combined = dedupe_by_primary_key(combined, dataset)
        out_dir.mkdir(parents=True, exist_ok=True)
        business_changed = not _business_equal(existing, combined)
        # One-time migration: a file written before the bitemporal columns
        # existed keeps deriving them on every read until it is rewritten, and
        # the digest above cannot see that because both sides are normalized.
        pit_missing = _pit_columns_absent(out_path, dataset)
        should_write = business_changed or had_fragments or pit_missing or not out_path.is_file()
        if should_write:
            write_parquet_atomic(out_path, combined, compression="zstd")
            for stale in out_dir.rglob("*.parquet"):
                if stale != out_path:
                    stale.unlink()
        if changed_files is not None and (
            business_changed or had_fragments or before_digest is None
        ):
            changed_files.append(out_path)
        return combined.height

    _PART = "__partition__"
    combined = combined.with_columns(
        _partition_values(combined, partition_col, granularity).alias(_PART)
    )

    total = 0
    for key, group in combined.partition_by(_PART, as_dict=True).items():
        val_str = str(key[0] if isinstance(key, tuple) else key)
        group = group.drop(_PART)
        existing_dir = curated.partition_path(dataset, partition_col, val_str)
        read_partition_dir = read_root / f"{partition_col}={val_str}"
        out_path = existing_dir / "part-merged.parquet"
        before_digest = sha256_file(out_path) if out_path.is_file() else None
        had_fragments = False
        frames = [group]
        existing = pl.DataFrame(schema=group.schema)
        if read_partition_dir.exists():
            existing_files = []
            for existing_path in read_partition_dir.rglob("*.parquet"):
                # A committed generation has no output path in the mutable
                # destination, so every source file is a legitimate fragment.
                had_fragments = had_fragments or existing_path.name != "part-merged.parquet"
                existing_files.append(
                    sanitize_dataset_rows(
                        validate_dataframe(pl.read_parquet(existing_path), dataset), dataset
                    )
                )
            if existing_files:
                existing = pl.concat(existing_files, how="diagonal_relaxed")
                if pk:
                    existing = dedupe_by_primary_key(existing, dataset)
                existing = _pit(existing)
                frames.append(existing)
        merged = pl.concat(frames, how="diagonal_relaxed")
        if pk:
            merged = dedupe_by_primary_key(merged, dataset)
        merged = _pit(merged)
        business_changed = not _business_equal(existing, merged)
        # See the unpartitioned branch: rewrite once so the columns stop being
        # recomputed on every read.
        pit_missing = _pit_columns_absent(out_path, dataset)
        should_write = business_changed or had_fragments or pit_missing or not out_path.is_file()
        if should_write:
            written = curated.write_partition(
                dataset, partition_col, val_str, merged, "part-merged.parquet"
            )
        else:
            # Keep the existing inode untouched when reconciliation changed
            # only fetched_at; this is the physical no-op that prevents a
            # spurious changed_files entry and revision.
            written = out_path
        if changed_files is not None and (
            business_changed or had_fragments or before_digest is None
        ):
            changed_files.append(written)
        total += merged.height
    return total
