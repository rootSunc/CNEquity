"""A dataset's published data as groups of files, from the lake catalog or the generation.

Code that works one partition at a time (coverage answers cached per
partition, read-size estimates, per-day derivations) used to walk the
committed generation's ``<column>=<value>`` directories. The lake catalog
lays files out by its own rules, so that walk does not carry over. This
module gives both layouts one shape:

* a :class:`FileGroup` is a set of files whose rows no other group holds;
* ``low``/``high`` bound the partition column inside the group;
* ``root`` is the directory every group's ``key`` is relative to, so a
  per-group cache can stamp the files the way ``storage.partition_cache`` does.

From the catalog every data file is its own group and its range comes from
the catalog's per-file statistics; from a generation a group is one
partition directory and its range is the directory's period.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from cnequity.domain.datasets import DATASETS
from cnequity.domain.partitions import parse_partition


@dataclass(frozen=True)
class FileGroup:
    key: str
    files: tuple[Path, ...]
    low: date | None
    high: date | None
    bytes: int
    rows: int

    def overlaps(self, start: date | None, end: date | None) -> bool:
        """Whether the group can hold rows in ``[start, end]``; unknown bounds overlap."""
        if start is not None and self.high is not None and self.high < start:
            return False
        if end is not None and self.low is not None and self.low > end:
            return False
        return True


@dataclass(frozen=True)
class Published:
    dataset: str
    root: Path
    groups: tuple[FileGroup, ...]
    from_catalog: bool
    # One row per primary key across the whole dataset.
    unique_keys: bool
    # The catalog table's schema; ``None`` for a generation, whose files say.
    schema: pl.Schema | None = None

    @property
    def files(self) -> list[Path]:
        return [path for group in self.groups for path in group.files]

    def in_range(self, start: date | None, end: date | None) -> list[FileGroup]:
        return [group for group in self.groups if group.overlaps(start, end)]

    def identity(self) -> str:
        """Changes whenever a published row may have: a cache key, not a content digest.

        Catalog files are never rewritten in place, so their names suffice; a
        generation's files are named by partition, so their sizes and times
        are added. A merge that only moves rows still changes it, which costs
        one recomputation and never hides a change.
        """
        entries = []
        for group in self.groups:
            for path in group.files:
                relative = Path(path).relative_to(self.root).as_posix()
                if self.from_catalog:
                    entries.append(relative)
                else:
                    info = Path(path).stat()
                    entries.append(f"{relative}|{info.st_size}|{info.st_mtime_ns}")
        source = "catalog" if self.from_catalog else "generation"
        return hashlib.sha256("\n".join([source, *sorted(entries)]).encode()).hexdigest()

    def scan(self, groups: list[FileGroup] | None = None) -> pl.LazyFrame | None:
        """A lazy scan of the published rows (of *groups*, if given), or ``None`` if empty.

        Catalog files are read against the table's schema: a file can carry the
        catalog's internal row-lineage columns, which are left out, and lack a
        column added after it was written, which reads as null.
        """
        files = [
            path for group in (self.groups if groups is None else groups) for path in group.files
        ]
        if not files:
            return None
        if self.schema is not None:
            return pl.scan_parquet(
                [str(path) for path in files],
                schema=self.schema,
                hive_partitioning=False,
                missing_columns="insert",
                extra_columns="ignore",
            )
        from cnequity.query.parquet_scan import scan_parquet_files

        return scan_parquet_files(files)


def _as_date(value: str | None, *, upper: bool) -> date | None:
    """A column statistic as a date; a period label (``2024Q3``) is its first or last day."""
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        period = parse_partition(str(value))
        if period is None:
            return None
        return period.end if upper else period.start


def _from_catalog(config, dataset: str, generation: Path) -> Published | None:
    from cnequity.query.catalog_read import generation_identity, published_stats

    identity = generation_identity(generation)
    if identity is None:
        return None
    found = published_stats(config, *identity, column=DATASETS[dataset].partition_col)
    if found is None:
        return None
    stats, unique, schema = found
    if not stats:
        return None
    root = Path(stats[0].path).parent
    while root.name.startswith(("year=", "month=")):
        root = root.parent
    groups = tuple(
        FileGroup(
            key=Path(item.path).relative_to(root).as_posix(),
            files=(Path(item.path),),
            low=_as_date(item.low, upper=False),
            high=_as_date(item.high, upper=True),
            bytes=item.bytes,
            rows=item.rows,
        )
        for item in stats
    )
    return Published(dataset, root, groups, True, unique, schema)


def _footer_rows(path: Path) -> int:
    """A file's row count from its footer; an unreadable file counts as empty.

    Sizing is a guard, not a reader: the read that follows reports the broken
    file in its own words.
    """
    try:
        return pq.ParquetFile(path).metadata.num_rows
    except (OSError, ValueError):
        return 0


def _from_generation(dataset: str, root: Path) -> Published:
    """One group per partition directory, or one group for an unpartitioned dataset."""
    by_part: dict[str, list[Path]] = {}
    for path in sorted(root.rglob("*.parquet")) if root.is_dir() else []:
        relative = path.relative_to(root)
        head = relative.parts[0] if len(relative.parts) > 1 and "=" in relative.parts[0] else ""
        by_part.setdefault(head, []).append(path)
    groups = []
    for part, files in sorted(by_part.items()):
        period = parse_partition(part.partition("=")[2]) if part else None
        groups.append(
            FileGroup(
                key=part,
                files=tuple(files),
                low=period.start if period else None,
                high=period.end if period else None,
                bytes=sum(path.stat().st_size for path in files),
                rows=sum(_footer_rows(path) for path in files),
            )
        )
    return Published(dataset, root, tuple(groups), False, False)


def catalog_published(config, dataset: str, read_context=None) -> Published | None:
    """*dataset*'s published files when the lake catalog holds them, else ``None``."""
    from cnequity.storage.read_context import read_root

    return _from_catalog(config, dataset, Path(read_root(config, dataset, read_context)))


def published(config, dataset: str, read_context=None) -> Published:
    """*dataset*'s published files, pinned by *read_context* when one is given."""
    from cnequity.storage.read_context import read_root

    root = Path(read_root(config, dataset, read_context))
    return _from_catalog(config, dataset, root) or _from_generation(dataset, root)


#: Bump when the digest computation changes; every cached answer is then recomputed.
_DIGEST_VERSION = 2


def _digest_columns(dataset: str) -> list[str]:
    """The columns a row hash covers: the registry's, in name order.

    Not the files' columns: catalog files can carry internal columns, and an
    optional column a file predates must hash the same as a stored null.
    """
    from cnequity.domain.schemas import DATASET_SCHEMAS

    return sorted(DATASET_SCHEMAS[dataset])


def _day_rows_sql(scan: str, present: set[str], dataset: str, column: str) -> str:
    values = ", ".join(
        f'"{name}"' if name in present else "NULL" for name in _digest_columns(dataset)
    )
    return (
        f'SELECT CAST("{column}" AS DATE) AS day, count(*), '
        "bit_xor(h)::VARCHAR, sum(h::HUGEINT)::VARCHAR "
        f'FROM (SELECT "{column}", hash({values}) AS h FROM {scan}) GROUP BY 1'
    )


def _digest(count: int, xor: int, total: int) -> str:
    return hashlib.sha256(f"{count}:{xor}:{total}".encode()).hexdigest()


def frame_day_digests(frame: pl.DataFrame, dataset: str) -> dict[date, str]:
    """:func:`day_digests` of rows not yet published, e.g. a derivation's output."""
    import duckdb

    column = DATASETS[dataset].partition_col
    con = duckdb.connect()
    try:
        con.register("frame_rows", frame)
        rows = con.execute(
            _day_rows_sql("frame_rows", set(frame.columns), dataset, column)
        ).fetchall()
    finally:
        con.close()
    return {day: _digest(c, int(x), int(t)) for day, c, x, t in rows if day}


def day_digests(config, dataset: str, read_context=None) -> dict[date, str]:
    """A content digest of *dataset*'s published rows on each day of its partition column.

    A day's digest changes exactly when that day's rows do, whatever files
    hold them: moving rows between files (a merge, the catalog's own layout)
    leaves it unchanged. It is the order-independent combination of one
    hash per row over the registry's columns, so it is computed per file
    group and cached under the group's stamp (``storage.partition_cache``),
    and only groups whose files changed are read again.
    """
    import duckdb

    from cnequity.storage.partition_cache import cached_partition_values

    column = DATASETS[dataset].partition_col
    if column is None:
        raise ValueError(f"{dataset} has no partition column to digest by")
    store = published(config, dataset, read_context)
    if not store.groups:
        return {}

    def compute(files: list[Path]) -> list:
        paths = ", ".join("'" + Path(path).as_posix().replace("'", "''") + "'" for path in files)
        scan = f"read_parquet([{paths}], union_by_name = true, hive_partitioning = false)"
        con = duckdb.connect()
        try:
            present = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {scan}").fetchall()}
            rows = con.execute(_day_rows_sql(scan, present, dataset, column)).fetchall()
        finally:
            con.close()
        return [[day.isoformat(), count, xor, total] for day, count, xor, total in rows if day]

    answers = cached_partition_values(
        config.meta_root,
        store.root,
        f"day_digests-{dataset}",
        {group.key: list(group.files) for group in store.groups},
        compute,
        version=_DIGEST_VERSION * 1000 + _duckdb_major_minor(),
    )
    combined: dict[str, list[int]] = {}
    for parts in answers.values():
        for day, count, xor, total in parts:
            entry = combined.setdefault(day, [0, 0, 0])
            entry[0] += int(count)
            entry[1] ^= int(xor)
            entry[2] += int(total)
    return {date.fromisoformat(day): _digest(*entry) for day, entry in combined.items()}


def period_digest(days: dict[date, str], period_days) -> str | None:
    """One digest for several days (a month of futures bars), or ``None`` if none has rows."""
    found = sorted(f"{day.isoformat()}:{days[day]}" for day in period_days if day in days)
    return hashlib.sha256("\n".join(found).encode()).hexdigest() if found else None


def _duckdb_major_minor() -> int:
    """Row hashes are DuckDB's; a new DuckDB may hash differently."""
    import duckdb

    major, minor, *_ = (int(part) for part in duckdb.__version__.split(".")[:2] + ["0"])
    return major * 100 + minor
