"""Lazy parquet scans with partition pruning for curated/derived lakes.

Pruning is done by inspecting partition directory names and selecting the ones
whose period overlaps the query window, rather than by handing the whole glob to
polars with ``hive_partitioning=True``. That is what lets a dataset partition by
month or year at all: polars types the hive column from the matching file
column, so a ``trade_date=2024`` directory next to a Date column raises
``could not find a 'date/datetime' pattern for '2024'``. See
``domain/partitions.py`` for the period mapping.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from cn_market_lake.domain.datasets import granularity_for_dataset
from cn_market_lake.domain.partitions import (
    Partition,
    parse_partition,
)

# Re-exported for callers that reach for it here; it is dataset registry
# metadata and lives in the domain layer so `storage` can read it without
# importing `query` (which imports `derive`, which imports `storage`).
__all__ = ["granularity_for_dataset"]


def dataset_has_parquet(root: Path) -> bool:
    return root.exists() and any(root.rglob("*.parquet"))


def parquet_glob(root: Path) -> str:
    """Recursive ``*.parquet`` glob in POSIX form for polars / DuckDB.

    ``str(root / "**" / "*.parquet")`` injects backslashes on Windows, which
    break both engines' glob matchers. Forward slashes are accepted everywhere.
    """
    return f"{Path(root).resolve().as_posix()}/**/*.parquet"


def _all_day_partitions(root: Path, partition_col: str | None) -> bool:
    """Whether every partition on disk is a single day.

    Decides hive parsing from the actual layout rather than from the registry:
    a lake mid-migration holds both shapes, and polars cannot parse a
    ``trade_date=2024`` directory as the DATE column beside it. Falling back to
    hive=False costs nothing — the column is in the file either way.
    """
    if partition_col is None:
        return False
    parts = list_partitions(root, partition_col)
    return bool(parts) and all(p.start == p.end for p in parts)


def list_partitions(root: Path, partition_col: str) -> list[Partition]:
    """Partition directories under *root*, sorted by period start.

    The period comes from each directory's own value, not from the dataset's
    configured granularity, so a lake part-migrated between granularities reads
    correctly throughout. Directories that do not parse as a period are skipped:
    they are stray artefacts, not data this dataset owns.
    """
    if not root.exists():
        return []
    prefix = f"{partition_col}="
    out: list[Partition] = []
    for entry in root.iterdir():
        if not entry.is_dir() or not entry.name.startswith(prefix):
            continue
        part = parse_partition(entry.name[len(prefix) :])
        if part is not None:
            out.append(part)
    return sorted(out, key=lambda p: p.start)


def partition_dir(root: Path, partition_col: str, value: str) -> Path:
    return root / f"{partition_col}={value}"


def partition_containing(root: Path, partition_col: str, d: date) -> Partition | None:
    """The existing partition holding *d*, whatever period it spans."""
    return next((p for p in list_partitions(root, partition_col) if p.covers(d)), None)


def list_hive_partition_dates(root: Path, partition_col: str) -> list[date]:
    """Period *start* dates of every partition, ascending.

    For day granularity these are the covered dates themselves. For coarser
    periods, prefer :func:`list_partitions` when the period end matters (e.g.
    reporting coverage); this returns starts so ordering stays meaningful.
    """
    return [p.start for p in list_partitions(root, partition_col)]


def coverage_start_from_partitions(root: Path, partition_col: str) -> date | None:
    parts = list_partitions(root, partition_col)
    return parts[0].start if parts else None


def uses_hive_partitions(root: Path, partition_col: str | None) -> bool:
    if partition_col is None:
        return False
    prefix = f"{partition_col}="
    if not root.exists():
        return False
    return any(entry.is_dir() and entry.name.startswith(prefix) for entry in root.iterdir())


def partition_files_in_range(
    root: Path,
    partition_col: str,
    *,
    start: date | None = None,
    end: date | None = None,
) -> list[Path]:
    """Parquet files in partitions whose period overlaps ``[start, end]``."""
    files: list[Path] = []
    for part in list_partitions(root, partition_col):
        if not part.overlaps(start, end):
            continue
        files.extend(sorted(partition_dir(root, partition_col, part.value).glob("**/*.parquet")))
    return files


def scan_parquet_root(
    root: Path,
    *,
    partition_col: str | None = None,
    start: date | None = None,
    end: date | None = None,
    symbols: list[str] | None = None,
    hive: bool | None = None,
) -> pl.LazyFrame:
    if not dataset_has_parquet(root):
        msg = f"no parquet data under {root}"
        raise FileNotFoundError(msg)

    partitioned = uses_hive_partitions(root, partition_col)
    use_hive = (partitioned and _all_day_partitions(root, partition_col)) if hive is None else hive

    lf: pl.LazyFrame | None = None
    if partitioned and partition_col and (start is not None or end is not None):
        # Directory-level pruning: skip whole periods before touching a footer.
        files = partition_files_in_range(root, partition_col, start=start, end=end)
        if not files:
            # Window is outside the lake's coverage — return an empty frame with
            # the real schema rather than raising, so callers can filter freely.
            return pl.scan_parquet(parquet_glob(root), hive_partitioning=use_hive).filter(
                pl.lit(False)
            )
        lf = pl.scan_parquet([str(f) for f in files], hive_partitioning=use_hive)
    if lf is None:
        lf = pl.scan_parquet(parquet_glob(root), hive_partitioning=use_hive)

    # Still filter on the column: a coarse partition covers days outside the
    # window, and pruning alone would over-return at the period edges.
    if partition_col and (start is not None or end is not None):
        if partition_col in lf.collect_schema().names():
            if start is not None:
                lf = lf.filter(pl.col(partition_col) >= start)
            if end is not None:
                lf = lf.filter(pl.col(partition_col) <= end)
    if symbols and "symbol" in lf.collect_schema().names():
        lf = lf.filter(pl.col("symbol").is_in(symbols))
    return lf


def scan_parquet_files(
    files: list[Path],
    *,
    hive: bool = False,
) -> pl.LazyFrame:
    if not files:
        return pl.LazyFrame()
    return pl.scan_parquet([str(path) for path in files], hive_partitioning=hive)


def collect_parquet_root(
    root: Path,
    *,
    partition_col: str | None = None,
    start: date | None = None,
    end: date | None = None,
    symbols: list[str] | None = None,
    hive: bool | None = None,
) -> pl.DataFrame:
    return scan_parquet_root(
        root,
        partition_col=partition_col,
        start=start,
        end=end,
        symbols=symbols,
        hive=hive,
    ).collect()


def lazy_row_count(lf: pl.LazyFrame) -> int:
    if lf.collect_schema().names() == ():
        return 0
    return int(lf.select(pl.len()).collect().item())


def lazy_mock_row_count(lf: pl.LazyFrame, *, mock_source: str) -> int:
    schema = lf.collect_schema().names()
    if "source" not in schema:
        return 0
    return int(lf.filter(pl.col("source") == mock_source).select(pl.len()).collect().item())


def lazy_n_unique_symbol(lf: pl.LazyFrame) -> int | None:
    if "symbol" not in lf.collect_schema().names():
        return None
    return int(lf.select(pl.col("symbol").n_unique()).collect().item())


def partition_col_for_dataset(dataset: str) -> str | None:
    from cn_market_lake.domain.datasets import PARTITION_COLS

    return PARTITION_COLS.get(dataset)
