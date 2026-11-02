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

import os
import time
from datetime import date
from pathlib import Path

import polars as pl

from cnequity.domain.canonical import dedupe_lazy_by_primary_key
from cnequity.domain.datasets import granularity_for_dataset
from cnequity.domain.partitions import (
    Partition,
    parse_partition,
)
from cnequity.storage.revisions import resolve_committed_root

# Re-exported for callers that reach for it here; it is dataset registry
# metadata and lives in the domain layer so `storage` can read it without
# importing `query` (which imports `derive`, which imports `storage`).
__all__ = ["granularity_for_dataset"]


def _resolved_root(
    root: Path,
    *,
    dataset: str | None = None,
    meta_root: Path | None = None,
    revision: int | str | None = None,
) -> Path:
    """Return one committed generation, retaining legacy-layout fallback."""
    return resolve_committed_root(
        root,
        dataset=dataset,
        meta_root=meta_root,
        revision=revision,
    )


def dataset_has_parquet(
    root: Path,
    *,
    dataset: str | None = None,
    meta_root: Path | None = None,
    revision: int | str | None = None,
    committed: bool = True,
) -> bool:
    resolved = (
        _resolved_root(root, dataset=dataset, meta_root=meta_root, revision=revision)
        if committed
        else Path(root)
    )
    return resolved.exists() and any(resolved.rglob("*.parquet"))


def parquet_glob(root: Path) -> str:
    """Recursive ``*.parquet`` glob in POSIX form for polars / DuckDB.

    ``str(root / "**" / "*.parquet")`` injects backslashes on Windows, which
    break both engines' glob matchers. Forward slashes are accepted everywhere.
    """
    return f"{Path(root).resolve().as_posix()}/**/*.parquet"


def _all_day_partitions(
    root: Path,
    partition_col: str | None,
    *,
    resolve: bool = True,
) -> bool:
    """Whether every partition on disk is a single day.

    Decides hive parsing from the actual layout rather than from the registry:
    a lake mid-migration holds both shapes, and polars cannot parse a
    ``trade_date=2024`` directory as the DATE column beside it. Falling back to
    hive=False costs nothing — the column is in the file either way.
    """
    if partition_col is None:
        return False
    parts = list_partitions(root, partition_col, resolve=resolve)
    return bool(parts) and all(p.start == p.end for p in parts)


def list_partitions(
    root: Path,
    partition_col: str,
    *,
    resolve: bool = True,
) -> list[Partition]:
    """Partition directories under *root*, sorted by period start.

    The period comes from each directory's own value, not from the dataset's
    configured granularity, so a lake part-migrated between granularities reads
    correctly throughout. Directories that do not parse as a period are skipped:
    they are stray artefacts, not data this dataset owns.
    """
    if resolve:
        root = _resolved_root(root)
    try:
        mtime_ns = os.stat(root).st_mtime_ns
    except FileNotFoundError:
        return []
    return list(_cached_partitions(str(root), partition_col, mtime_ns))


# A directory's mtime changes whenever an entry is added, removed or renamed,
# so it keys the listing safely for both committed generations (immutable) and
# legacy mutable roots. Day-partitioned datasets hold ~8k directories; listing
# them once per process instead of several times per read is most of a
# single-symbol load.
#
# Only once the mtime is old enough, though. Linux stamps directories from a
# coarse clock (a few milliseconds), so two partitions created within one tick
# leave the mtime where the first one put it, and a listing cached in between
# would hide the second. Within this window the directory is listed afresh and
# nothing is cached — git's answer to the same "racy" timestamps.
_PARTITION_CACHE: dict[tuple[str, str], tuple[int, tuple[Partition, ...]]] = {}
_PARTITION_CACHE_MAX = 256
_RACY_NS = 2_000_000_000


def _cached_partitions(root: str, partition_col: str, mtime_ns: int) -> tuple[Partition, ...]:
    key = (root, partition_col)
    settled = time.time_ns() - mtime_ns >= _RACY_NS
    hit = _PARTITION_CACHE.get(key)
    if settled and hit is not None and hit[0] == mtime_ns:
        return hit[1]
    prefix = f"{partition_col}="
    out: list[Partition] = []
    with os.scandir(root) as entries:
        for entry in entries:
            if not entry.name.startswith(prefix) or not entry.is_dir():
                continue
            part = parse_partition(entry.name[len(prefix) :])
            if part is not None:
                out.append(part)
    parts = tuple(sorted(out, key=lambda p: p.start))
    if not settled:
        _PARTITION_CACHE.pop(key, None)
        return parts
    if len(_PARTITION_CACHE) >= _PARTITION_CACHE_MAX:
        _PARTITION_CACHE.clear()
    _PARTITION_CACHE[key] = (mtime_ns, parts)
    return parts


def partition_dir(root: Path, partition_col: str, value: str) -> Path:
    return root / f"{partition_col}={value}"


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


def uses_hive_partitions(
    root: Path,
    partition_col: str | None,
    *,
    resolve: bool = True,
) -> bool:
    if partition_col is None:
        return False
    if not root.exists():
        return False
    return bool(list_partitions(root, partition_col, resolve=resolve))


def partition_files_in_range(
    root: Path,
    partition_col: str,
    *,
    start: date | None = None,
    end: date | None = None,
    resolve: bool = True,
) -> list[Path]:
    """Parquet files relevant to ``[start, end]`` in mixed layouts.

    Root-level files have no directory period to prune, so keep them whenever
    partition directories coexist. The column filter applied by
    :func:`scan_parquet_root` removes out-of-range rows afterward. Dropping
    these loose files would make a partially migrated lake return incomplete
    history only for ranged queries, while an unbounded query still appeared
    correct.
    """
    if resolve:
        root = _resolved_root(root)
    files = sorted(root.glob("*.parquet"))
    for part in list_partitions(root, partition_col, resolve=False):
        if not part.overlaps(start, end):
            continue
        files.extend(sorted(partition_dir(root, partition_col, part.value).glob("**/*.parquet")))
    return files


def _scan_parquet_paths(
    files: list[Path],
    *,
    hive: bool,
    missing_columns: str = "raise",
    extra_columns: str = "raise",
    traded_only: bool = False,
) -> pl.LazyFrame:
    """Scan files, preserving columns across legacy/current schema mixes.

    ``pl.scan_parquet([old, new])`` uses one file's schema as the contract and
    raises when another file adds a column (for example legacy daily bars before
    ``volume``). Keep the normal multi-file scan for the overwhelmingly common
    uniform case; only build a diagonal lazy union when footer schemas differ.
    """
    paths = [str(path) for path in files]
    if not paths:
        raise FileNotFoundError("no parquet files to scan")
    try:
        schemas = [pl.read_parquet_schema(path) for path in paths]
    except Exception:
        # Keep construction lazy when a file is corrupt. Dataset audit first
        # identifies and removes unreadable files before collecting the frame;
        # forcing footer reads here would abort that isolation path early.
        return pl.scan_parquet(
            paths,
            hive_partitioning=hive,
            missing_columns=missing_columns,
            extra_columns=extra_columns,
        )
    if all(schema == schemas[0] for schema in schemas[1:]):
        return pl.scan_parquet(paths, hive_partitioning=hive)
    scans: list[pl.LazyFrame] = []
    # Preserve file order (canonical tie-breaking depends on it), while
    # avoiding thousands of optimizer branches for a mostly uniform history.
    group = [paths[0]]
    previous_schema = schemas[0]
    for path, schema in zip(paths[1:], schemas[1:], strict=True):
        if schema != previous_schema:
            scans.append(pl.scan_parquet(group, hive_partitioning=hive))
            group = []
        group.append(path)
        previous_schema = schema
    scans.append(pl.scan_parquet(group, hive_partitioning=hive))
    return pl.concat(scans, how="diagonal_relaxed")


def scan_parquet_root(
    root: Path,
    *,
    partition_col: str | None = None,
    start: date | None = None,
    end: date | None = None,
    symbols: list[str] | None = None,
    hive: bool | None = None,
    traded_only: bool = False,
    dataset: str | None = None,
    meta_root: Path | None = None,
    revision: int | str | None = None,
    committed: bool = True,
) -> pl.LazyFrame:
    # Resolve once at scan construction.  A LazyFrame may be collected after a
    # concurrent compact publishes another revision; retaining this concrete
    # generation path makes the whole query internally consistent.
    logical_dataset = dataset or root.name
    if committed:
        root = _resolved_root(
            root,
            dataset=dataset,
            meta_root=meta_root,
            revision=revision,
        )
    if not dataset_has_parquet(
        root,
        # ``root`` has already been resolved above.  Resolving the concrete
        # generation a second time loses the inferred meta root (its parent is
        # ``meta/revisions/data/<dataset>``) and can incorrectly fall back to
        # the deleted mutable path in direct scanner callers.
        committed=False,
    ):
        msg = f"no parquet data under {root}"
        raise FileNotFoundError(msg)

    partitioned = uses_hive_partitions(root, partition_col, resolve=False)
    use_hive = (
        (partitioned and _all_day_partitions(root, partition_col, resolve=False))
        if hive is None
        else hive
    )

    lf: pl.LazyFrame | None = None
    if partitioned and partition_col and (start is not None or end is not None):
        # Directory-level pruning: skip whole periods before touching a footer.
        files = partition_files_in_range(
            root,
            partition_col,
            start=start,
            end=end,
            resolve=False,
        )
        if not files:
            # Window is outside the lake's coverage — return an empty frame with
            # the real schema rather than raising, so callers can filter freely.
            all_files = partition_files_in_range(root, partition_col, resolve=False)
            if all_files:
                return _scan_parquet_paths(
                    all_files, hive=use_hive, traded_only=traded_only
                ).filter(pl.lit(False))
            return _scan_parquet_paths(
                sorted(root.rglob("*.parquet")), hive=False, traded_only=traded_only
            ).filter(pl.lit(False))
        lf = _scan_parquet_paths(files, hive=use_hive, traded_only=traded_only)
    if lf is None:
        if partitioned and partition_col:
            files = partition_files_in_range(root, partition_col, resolve=False)
            if files:
                lf = _scan_parquet_paths(files, hive=use_hive, traded_only=traded_only)
            else:
                lf = _scan_parquet_paths(
                    sorted(root.rglob("*.parquet")), hive=False, traded_only=traded_only
                )
        else:
            lf = _scan_parquet_paths(
                sorted(root.rglob("*.parquet")), hive=use_hive, traded_only=traded_only
            )

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
    if traded_only:
        # Filtering each file before the union lets an old positive-volume
        # retry survive beside a newer zero-volume canonical row. Canonicalize
        # the daily-bar identity first, then decide whether that final row is
        # a real print. Legacy files without ``volume`` retain row semantics;
        # their diagonal null volume is handled as a traded row below.
        columns = set(lf.collect_schema().names())
        if logical_dataset == "daily_bars" and {"symbol", "trade_date"}.issubset(columns):
            lf = dedupe_lazy_by_primary_key(lf, "daily_bars")
        if "volume" in columns:
            lf = lf.filter((pl.col("volume") > 0) | pl.col("volume").is_null())
    return lf


def scan_parquet_files(
    files: list[Path],
    *,
    hive: bool = False,
    missing_columns: str = "raise",
    extra_columns: str = "raise",
) -> pl.LazyFrame:
    if not files:
        return pl.LazyFrame()
    if missing_columns == "raise" and extra_columns == "raise":
        # Keep the fast path for uniform files while making the default reader
        # safe for a lake that is mid-schema migration.
        return _scan_parquet_paths(files, hive=hive)
    if missing_columns == "insert" and extra_columns == "ignore":
        # A native scan chooses one file's schema and ``extra_columns=ignore``
        # can silently discard a newly-added field when an old file sorts first.
        return _scan_parquet_paths(
            files,
            hive=hive,
            missing_columns=missing_columns,
            extra_columns=extra_columns,
        )
    return pl.scan_parquet(
        [str(path) for path in files],
        hive_partitioning=hive,
        missing_columns=missing_columns,
        extra_columns=extra_columns,
    )


def collect_parquet_root(
    root: Path,
    *,
    partition_col: str | None = None,
    start: date | None = None,
    end: date | None = None,
    symbols: list[str] | None = None,
    hive: bool | None = None,
    traded_only: bool = False,
    dataset: str | None = None,
    meta_root: Path | None = None,
    revision: int | str | None = None,
    committed: bool = True,
) -> pl.DataFrame:
    return scan_parquet_root(
        root,
        partition_col=partition_col,
        start=start,
        end=end,
        symbols=symbols,
        hive=hive,
        traded_only=traded_only,
        dataset=dataset,
        meta_root=meta_root,
        revision=revision,
        committed=committed,
    ).collect()


def lazy_row_count(lf: pl.LazyFrame) -> int:
    if not lf.collect_schema().names():
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
    from cnequity.domain.datasets import PARTITION_COLS

    return PARTITION_COLS.get(dataset)
