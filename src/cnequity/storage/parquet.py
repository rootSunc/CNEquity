from __future__ import annotations

import json
import logging
import re
import zlib
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4

import polars as pl

from cnequity.domain.action_evidence import clear_invalid_payment_evidence
from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.domain.datasets import granularity_for_dataset
from cnequity.domain.partitions import Granularity
from cnequity.domain.pit import (
    PIT_DATASET_NAMES,
    PIT_STORAGE_COLUMNS,
    normalize_pit_storage_columns,
    retain_pit_vintages,
)
from cnequity.domain.schemas import (
    DATASET_SCHEMAS,
    OPTIONAL_COLUMNS,
    PRIMARY_KEYS,
    SchemaValidationError,
    sanitize_dataset_rows,
    validate_dataframe,
)
from cnequity.storage.atomic import write_json_atomic, write_parquet_atomic
from cnequity.storage.changes import summarize_changes
from cnequity.storage.revisions import sha256_file

logger = logging.getLogger(__name__)


def _guard_staged_status(curated_root: Path, rows: pl.DataFrame) -> pl.DataFrame:
    """Keep an old successful staging batch from resurrecting invalid statuses."""
    from cnequity.domain.trading_status import (
        DELISTED_SOURCE,
        impossible_delistings,
        split_before_listing,
    )
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    instruments = curated_root / "instruments"
    if rows.is_empty() or not dataset_has_parquet(instruments):
        return rows
    spans = scan_parquet_root(instruments, hive=False).collect()
    for column in ("list_date", "delist_date"):
        if column not in spans.columns:
            spans = spans.with_columns(pl.lit(None, dtype=pl.Date).alias(column))
    spans = dedupe_by_primary_key(spans, "instruments").select("symbol", "list_date", "delist_date")
    impossible = impossible_delistings(spans)
    flagged = rows.join(spans.select("symbol", "delist_date"), on="symbol", how="left")
    invalid = (pl.col("source") == DELISTED_SOURCE) & (
        pl.col("delist_date").is_null()
        | (pl.col("trade_date") < pl.col("delist_date"))
        | pl.col("symbol").is_in(sorted(impossible))
    )
    kept = flagged.filter(~invalid).drop("delist_date")
    candidates = kept.join(spans.select("symbol", "list_date"), on="symbol").filter(
        pl.col("trade_date") < pl.col("list_date")
    )
    late = kept.join(spans.select("symbol", "delist_date"), on="symbol").filter(
        pl.col("is_trading") & (pl.col("trade_date") > pl.col("delist_date"))
    )
    if not candidates.is_empty() or not late.is_empty():
        affected = sorted(set(candidates["symbol"]) | set(late["symbol"]))
        bars = curated_root / "daily_bars"
        if dataset_has_parquet(bars):
            first = (
                scan_parquet_root(
                    bars,
                    partition_col="trade_date",
                    traded_only=True,
                    symbols=affected,
                )
                .group_by("symbol")
                .agg(
                    pl.col("trade_date").min().alias("first_traded"),
                    pl.col("trade_date").max().alias("last_traded"),
                )
                .collect()
            )
            spans = spans.join(first, on="symbol", how="left")
        else:
            spans = spans.with_columns(
                pl.lit(None, dtype=pl.Date).alias("first_traded"),
                pl.lit(None, dtype=pl.Date).alias("last_traded"),
            )
        kept, _ = split_before_listing(kept, spans)
        retired = spans.filter(
            pl.col("delist_date").is_not_null()
            & ~pl.col("symbol").is_in(sorted(impossible))
            & (pl.col("last_traded").is_null() | (pl.col("last_traded") <= pl.col("delist_date")))
        ).select("symbol", pl.col("delist_date").alias("_delist"))
        kept = (
            kept.join(retired, on="symbol", how="left")
            .filter(
                ~(
                    pl.col("is_trading").fill_null(False)
                    & pl.col("_delist").is_not_null()
                    & (pl.col("trade_date") > pl.col("_delist"))
                )
            )
            .drop("_delist")
        )
    if kept.height < rows.height:
        logger.warning(
            "trading_status: dropped %d staged row(s) outside the listed life",
            rows.height - kept.height,
        )
    return kept


def _dedupe_storage(frame: pl.DataFrame, dataset: str) -> pl.DataFrame:
    if dataset in PIT_DATASET_NAMES:
        return retain_pit_vintages(frame, dataset)
    return dedupe_by_primary_key(frame, dataset)


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
        # A crash during replacement must leave an unsealed file, never the
        # previous attempt's seal attached to different bytes.
        path.with_suffix(".sealed.json").unlink(missing_ok=True)
        write_parquet_atomic(path, df, compression="zstd")
        self._write_seal(path, dataset, run_id, batch_id=batch_id, rows=df.height)
        return path

    def seal_file(
        self, path: Path, dataset: str, run_id: str, *, batch_id: str | None = None
    ) -> None:
        """Seal only readable schema-valid bytes, including filtered candidates."""
        frame = validate_dataframe(pl.read_parquet(path), dataset)
        self._write_seal(path, dataset, run_id, batch_id=batch_id, rows=frame.height)

    @staticmethod
    def _write_seal(
        path: Path, dataset: str, run_id: str, *, batch_id: str | None, rows: int
    ) -> None:
        write_json_atomic(
            path.with_suffix(".sealed.json"),
            {
                "result_schema_version": 2,
                "dataset": dataset,
                "run_id": run_id,
                "batch_id": batch_id or path.stem.removeprefix("part-"),
                "sha256": sha256_file(path),
                "rows": rows,
            },
        )

    def is_sealed(self, path: Path, dataset: str, run_id: str) -> bool:
        """A validation seal names the exact atomically written bytes."""
        receipt = path.with_suffix(".sealed.json")
        if not receipt.exists():
            return False
        payload = json.loads(receipt.read_text(encoding="utf-8"))
        if (
            payload.get("result_schema_version") != 2
            or payload.get("dataset") != dataset
            or payload.get("run_id") != run_id
            or payload.get("sha256") != sha256_file(path)
        ):
            raise ValueError(f"staging seal does not match validated bytes: {path}")
        return True

    def write_usable_batch(
        self,
        dataset: str,
        run_id: str,
        batch_id: str,
        df: pl.DataFrame,
    ) -> dict:
        """Stage the valid rows of a batch whose rows are independent facts.

        Invalid rows are isolated by bisection, so one bad row costs a few
        extra validations rather than the batch. The full input and the rejects
        are kept under ``_quarantine`` as evidence. Accepting some rows never
        certifies coverage: the caller records the rejected dates as missing.
        Only datasets registered with ``partial_rows`` may use this path.
        """
        from cnequity.domain.datasets import DATASETS

        spec = DATASETS.get(dataset)
        if spec is None or not spec.partial_rows:
            raise ValueError(f"{dataset} does not accept partial batches")
        accepted: list[pl.DataFrame] = []
        rejected: list[pl.DataFrame] = []
        reasons: set[str] = set()
        required = DATASET_SCHEMAS.get(dataset, {})

        def split(frame: pl.DataFrame) -> None:
            try:
                accepted.append(validate_dataframe(frame, dataset))
            except SchemaValidationError as exc:
                # A missing column is a batch-level fault; bisecting it would
                # only reject every row one at a time.
                if frame.height <= 1 or any(
                    column not in frame.columns and column not in OPTIONAL_COLUMNS
                    for column in required
                ):
                    rejected.append(frame)
                    reasons.add(str(exc))
                else:
                    middle = frame.height // 2
                    split(frame.head(middle))
                    split(frame.slice(middle))

        split(df)
        count = sum(frame.height for frame in rejected)
        report: dict = {
            "dataset": dataset,
            "run_id": run_id,
            "batch_id": batch_id,
            "rows_read": df.height,
            "rows_rejected": count,
        }
        if rejected:
            safe_run = "".join(c if c.isalnum() or c in "-_" else "_" for c in run_id)
            quarantine = (
                self.staging_root.parent
                / "_quarantine"
                / f"{dataset}-{safe_run}-rows-{uuid4().hex}"
            )
            quarantine.mkdir(parents=True)
            bad = pl.concat(rejected, how="diagonal_relaxed")
            write_parquet_atomic(quarantine / "input.parquet", df, compression="zstd")
            write_parquet_atomic(quarantine / "rejected.parquet", bad, compression="zstd")
            date_col = spec.date_col or spec.partition_col
            days: set[str] = set()
            if date_col and date_col in bad.columns:
                values = (
                    bad.select(
                        pl.col(date_col).cast(pl.String).str.slice(0, 10).str.to_date(strict=False)
                    )
                    .to_series()
                    .drop_nulls()
                )
                days.update(value.isoformat() for value in values)
            report.update(
                quarantine=str(quarantine),
                reasons=sorted(reasons),
                rejected_dates=sorted(days),
            )
            write_json_atomic(quarantine / "report.json", report, indent=2)
        if not accepted:
            raise SchemaValidationError(
                f"{dataset}: all {count} rows rejected; retained at {report.get('quarantine')}"
            )
        good = pl.concat(accepted, how="diagonal_relaxed")
        path = self.write_batch(dataset, run_id, batch_id, good)
        report.update(rows_written=good.height, path=str(path))
        if rejected:
            receipt = self.quality_receipt(dataset, run_id, batch_id)
            write_json_atomic(receipt, report, indent=2)
        return report

    def quality_receipt(self, dataset: str, run_id: str, batch_id: str) -> Path:
        """Where a partially accepted batch records what it left out.

        Inside the run directory, so staging cleanup removes it with the run.
        """
        return self.staging_root / dataset / f"run_id={run_id}" / "_quality" / f"{batch_id}.json"

    def quality_receipts(self, dataset: str, run_id: str) -> list[dict]:
        """Every partial-batch receipt of one run, oldest batch id first."""
        root = self.staging_root / dataset / f"run_id={run_id}" / "_quality"
        return [
            json.loads(path.read_text(encoding="utf-8")) for path in sorted(root.glob("*.json"))
        ]

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


#: The file a partition is merged into, and the name a fresh partition starts with.
MERGED_FILE = "part-merged.parquet"
#: A write that would leave more files than this in one partition merges them
#: all instead. `cne run maintain` merges partitions well before this.
MAX_PARTITION_FILES = 8
#: A partition written whole with more rows than this is split into buckets of
#: a key column, so that an update rewrites one bucket rather than, say, a
#: whole 14M-row day of ticks. Partitions below it stay one file.
BUCKET_ROWS = 200_000
_BUCKET_FILE = re.compile(r"^part-b(\d{3})-of-(\d{3})\.parquet$")


def bucket_name(index: int, count: int) -> str:
    return f"part-b{index:03d}-of-{count:03d}.parquet"


def parse_bucket(name: str) -> tuple[int, int] | None:
    """``(index, count)`` of a bucket file name, or None."""
    match = _BUCKET_FILE.match(name)
    return (int(match.group(1)), int(match.group(2))) if match else None


def is_base_file(name: str, merged_name: str = MERGED_FILE) -> bool:
    """The partition's merged file or one of its buckets — not an appended file."""
    return name == merged_name or parse_bucket(name) is not None


def bucket_column(frame: pl.DataFrame, dataset: str) -> str | None:
    """The first string key column, which keeps a merge group in one bucket."""
    for key in merge_group_keys(dataset):
        if key in frame.columns and frame.schema[key] in (pl.String, pl.Categorical):
            return key
    return None


def _bucket_count(rows: int) -> int:
    count = 1
    while rows / count > BUCKET_ROWS:
        count *= 2
    return min(count, 999)


def _with_bucket_ids(frame: pl.DataFrame, column: str, count: int) -> pl.DataFrame:
    """Stable bucket per value (crc32, not Polars' hash, which may change between versions)."""
    values = frame.get_column(column).unique()
    mapping = pl.DataFrame(
        {
            column: values,
            "__bucket__": [zlib.crc32(str(value).encode()) % count for value in values],
        },
        schema={column: frame.schema[column], "__bucket__": pl.Int64},
    )
    return frame.join(mapping, on=column, how="left", nulls_equal=True, maintain_order="left")


def emit_partition(
    merged: pl.DataFrame,
    dataset: str,
    *,
    affected: list[Path],
    existing: list[Path],
    read_dir: Path | None,
    out_dir: Path,
    merged_name: str = MERGED_FILE,
) -> tuple[list[Path], list[Path]]:
    """Write *merged* — the content of the *affected* files — back into the partition.

    Returns ``(written, removed)``. *existing* is every committed file of the
    partition; files outside *affected* are never written. In a bucketed
    partition each row goes to its bucket unless that bucket exists and was
    not loaded, in which case it joins one new appended file instead.
    """
    names = {path.name for path in affected}
    buckets = {parsed: path for path in existing if (parsed := parse_bucket(path.name)) is not None}
    counts = {count for _, count in buckets}
    whole = not existing or merged_name in names or len(affected) == len(existing)
    column = bucket_column(merged, dataset)
    bucketed = column is not None and (bool(buckets) or (whole and merged.height > BUCKET_ROWS))
    written: list[Path] = []
    if not bucketed:
        if whole:
            name = merged_name
        elif affected:
            name = sorted(names)[0]
        else:
            name = f"part-{uuid4().hex[:12]}.parquet"
        target = out_dir / name
        write_parquet_atomic(target, merged, compression="zstd")
        written.append(target)
    else:
        count = max(counts) if counts else _bucket_count(merged.height)
        present = {index for index, size in buckets if size == count}
        loaded = {
            parsed[0]
            for name in names
            if (parsed := parse_bucket(name)) is not None and parsed[1] == count
        }
        spill: list[pl.DataFrame] = []
        tagged = _with_bucket_ids(merged, column, count)
        for (index,), group in tagged.partition_by("__bucket__", as_dict=True).items():
            group = group.drop("__bucket__")
            if index in loaded or index not in present:
                target = out_dir / bucket_name(int(index), count)
                write_parquet_atomic(target, group, compression="zstd")
                written.append(target)
            else:
                spill.append(group)
        if spill:
            target = out_dir / f"part-{uuid4().hex[:12]}.parquet"
            write_parquet_atomic(
                target, pl.concat(spill, how="diagonal_relaxed"), compression="zstd"
            )
            written.append(target)
    removed: list[Path] = []
    for path in affected:
        stale = (
            out_dir / path.relative_to(read_dir) if read_dir is not None else out_dir / path.name
        )
        if stale not in written and stale.exists():
            stale.unlink()
            removed.append(stale)
    return written, removed


def merge_group_keys(dataset: str) -> list[str]:
    """Columns whose equal values must share a file for dedupe to be correct.

    Usually the primary key: storage dedupe never compares rows of different
    keys, and PIT vintages share their business key. ``index_constituents``
    picks one source per index book, so a whole (index, date) book is one group.
    """
    if dataset == "index_constituents":
        return ["index_symbol", "as_of_date"]
    return list(PRIMARY_KEYS.get(dataset, []))


def _row_count(path: Path) -> int:
    return int(pl.scan_parquet(path).select(pl.len()).collect().item())


@dataclass
class PartitionWrite:
    """What one partition write did, for the caller's receipts and logs."""

    rows: int
    written: list[Path] = field(default_factory=list)
    removed: list[Path] = field(default_factory=list)
    changed: bool = False
    change: dict | None = None


def write_partition_rows(
    incoming: pl.DataFrame,
    dataset: str,
    *,
    read_dir: Path | None,
    out_dir: Path,
    load: Callable[[Path], pl.DataFrame],
    dedupe: Callable[[pl.DataFrame], pl.DataFrame],
    finish: Callable[[pl.DataFrame], pl.DataFrame] = lambda frame: frame,
    pit_missing: Callable[[Path], bool] = lambda path: False,
    want_change: bool = False,
    replace_by: list[str] | None = None,
    incoming_first: bool = True,
    merged_name: str = MERGED_FILE,
) -> PartitionWrite:
    """Write *incoming* into one partition, touching only the files it must.

    Every partition used to be read whole, merged and rewritten as one file,
    so the cost of a write was the partition's size, not the change's: forty
    symbols' month of daily bars rewrote 159,192 rows to add 840, and one
    year-partitioned day of ``industry_index`` rewrote ~289k rows for ~1.2k.

    Files in a partition hold disjoint merge groups (see `merge_group_keys`),
    and readers already take every file. So only the key columns of the
    existing files are read; a file is rewritten only when an incoming row
    shares a group with it (or a group is split across files, which repairs a
    legacy fragment), and rows of new groups go into one new file — unless a
    rewrite is happening anyway, which takes them for free. A partition that
    would pass `MAX_PARTITION_FILES` folds its smaller files into one, leaving
    the large merged file alone unless the write touches it.

    *replace_by* names columns whose incoming values are complete: stored rows
    with those values are dropped rather than kept (a recomputed date replaces
    the whole date). *incoming_first* orders incoming rows before stored ones
    for dedupe ties, as compact always has; derived writers put stored first.
    *merged_name* is the dataset's whole-partition file name.

    *read_dir* is the committed partition, *out_dir* its mutable copy with the
    same file names. *load* reads and normalizes one stored file, *dedupe*
    applies storage dedupe, *finish* runs after dedupe on what is written.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    existing = (
        sorted(read_dir.rglob("*.parquet")) if read_dir is not None and read_dir.exists() else []
    )
    keys = merge_group_keys(dataset)
    nested = any(path.parent != read_dir for path in existing)
    if not existing:
        merged = finish(dedupe(incoming))
        written, _ = emit_partition(
            merged,
            dataset,
            affected=[],
            existing=[],
            read_dir=read_dir,
            out_dir=out_dir,
            merged_name=merged_name,
        )
        # Nothing is committed here, so anything else in the mutable copy is
        # an interrupted write's leftover and would be read twice.
        for stale in out_dir.rglob("*.parquet"):
            if stale not in written:
                stale.unlink()
        change = None
        if want_change:
            change = summarize_changes(merged.clear(), merged, dataset)
        return PartitionWrite(rows=merged.height, written=written, changed=True, change=change)

    replace_by = list(replace_by or [])
    route = list(dict.fromkeys([*keys, *replace_by]))
    keyed = bool(keys) and all(column in incoming.columns for column in route)
    affected: list[Path]
    if not keyed or nested:
        # No group to route by, or files below the partition directory that
        # only a whole merge can account for: the old behaviour.
        affected = list(existing)
    else:
        indexed = []
        doubled: set[int] = set()
        for index, path in enumerate(existing):
            frame = pl.read_parquet(path, columns=route)
            # A legacy file holding one key twice is repaired when touched,
            # as every write used to; PIT vintages legitimately repeat it.
            if dataset not in PIT_DATASET_NAMES and frame.select(keys).is_duplicated().any():
                doubled.add(index)
            indexed.append(frame.unique().with_columns(pl.lit(index).alias("__file__")))
        stored = pl.concat(indexed, how="vertical_relaxed")
        shared = stored.group_by(keys).agg(pl.col("__file__").n_unique().alias("__n__"))
        split = shared.filter(pl.col("__n__") > 1).drop("__n__")
        targets = incoming.select(keys).unique()
        hit = (
            pl.concat([targets, split], how="vertical_relaxed").unique()
            if split.height
            else targets
        )
        touched = set(stored.join(hit, on=keys, how="semi")["__file__"].to_list()) | doubled
        if replace_by:
            replaced = incoming.select(replace_by).unique()
            touched |= set(stored.join(replaced, on=replace_by, how="semi")["__file__"].to_list())
        affected = [existing[index] for index in sorted(touched)]
        tails = [path for path in existing if not is_base_file(path.name, merged_name)]
        if len(set(tails) - set(affected)) + 1 > MAX_PARTITION_FILES:
            # Fold the appended files together; the merged file and the
            # buckets are rewritten only where the write itself reaches.
            affected = (
                sorted(set(affected) | set(tails)) if len(tails) < len(existing) else list(existing)
            )

    before = (
        finish(dedupe(pl.concat([load(path) for path in affected], how="diagonal_relaxed")))
        if affected
        else None
    )
    kept = before
    if kept is not None and replace_by and all(c in kept.columns for c in replace_by):
        kept = kept.join(incoming.select(replace_by).unique(), on=replace_by, how="anti")
    if kept is None:
        combined = incoming
    elif incoming_first:
        combined = pl.concat([incoming, kept], how="diagonal_relaxed")
    else:
        combined = pl.concat([kept, incoming], how="diagonal_relaxed")
    merged = finish(dedupe(combined))
    if before is None:
        before = merged.clear()
    unaffected_rows = sum(_row_count(path) for path in existing if path not in affected)
    business = not _business_equal(before, merged)
    if not business and not any(pit_missing(path) for path in affected):
        return PartitionWrite(rows=unaffected_rows + merged.height)

    written, removed = emit_partition(
        merged,
        dataset,
        affected=affected,
        existing=existing,
        read_dir=read_dir,
        out_dir=out_dir,
        merged_name=merged_name,
    )
    change = summarize_changes(before, merged, dataset) if want_change and business else None
    return PartitionWrite(
        rows=unaffected_rows + merged.height,
        written=written,
        removed=removed,
        changed=business,
        change=change,
    )


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
    change_log: list[dict] | None = None,
    staging_files: list[Path] | None = None,
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
    files = staging.list_run_files(dataset, run_id) if staging_files is None else staging_files
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
    if dataset == "trading_status":
        combined = _guard_staged_status(curated_root, combined)
        if combined.is_empty():
            return 0
    if dataset == "valuation_metrics":
        from cnequity.domain.valuation import null_missing_valuations

        combined = null_missing_valuations(combined)
    if dataset == "index_constituents":
        from cnequity.domain.membership import null_missing_index_weights

        combined = null_missing_index_weights(combined)
    if dataset == "corporate_actions":
        # A payment before the ex-date is a source typo, not a fact, so a new
        # row never stores one.  Only staged rows are cleaned: rewriting the
        # committed side here would hide the change from the diff below and a
        # stored typo would never be republished without it.
        combined = clear_invalid_payment_evidence(combined)
    pk = PRIMARY_KEYS.get(dataset, [])
    if pk:
        combined = _dedupe_storage(combined, dataset)

    def _pit(frame: pl.DataFrame) -> pl.DataFrame:
        """Materialize the bitemporal columns so they reach disk.

        They were previously derived on every read instead (19 s of row-wise
        hashing for one `financial_statement_items` scan). Applied to both
        sides of the digest comparison below so adding them is never mistaken
        for a business change.
        """
        return normalize_pit_storage_columns(frame, dataset)

    combined = _pit(combined)

    def _load(path: Path) -> pl.DataFrame:
        return sanitize_dataset_rows(validate_dataframe(pl.read_parquet(path), dataset), dataset)

    def _dedupe(frame: pl.DataFrame) -> pl.DataFrame:
        return _dedupe_storage(frame, dataset) if pk else frame

    def _write(group: pl.DataFrame, read_dir: Path, out_dir: Path, label: str | None) -> int:
        result = write_partition_rows(
            group,
            dataset,
            read_dir=read_dir,
            out_dir=out_dir,
            load=_load,
            dedupe=_dedupe,
            finish=_pit,
            pit_missing=lambda path: _pit_columns_absent(path, dataset),
            want_change=change_log is not None,
        )
        if change_log is not None and result.change is not None:
            change_log.append({"partition": label, **result.change})
        # A rewrite that only adds the PIT columns is not a publication, as
        # before; merging fragments away is, since the file set changed.
        if changed_files is not None and (result.changed or result.removed):
            changed_files.extend(result.written)
        return result.rows

    if partition_col not in combined.columns:
        return _write(combined, read_root, curated.curated_root / dataset, None)

    _PART = "__partition__"
    combined = combined.with_columns(
        _partition_values(combined, partition_col, granularity).alias(_PART)
    )

    total = 0
    for key, group in combined.partition_by(_PART, as_dict=True).items():
        val_str = str(key[0] if isinstance(key, tuple) else key)
        total += _write(
            group.drop(_PART),
            read_root / f"{partition_col}={val_str}",
            curated.partition_path(dataset, partition_col, val_str),
            f"{partition_col}={val_str}",
        )
    return total
