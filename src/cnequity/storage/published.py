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

from dataclasses import dataclass
from datetime import date
from pathlib import Path

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

    @property
    def files(self) -> list[Path]:
        return [path for group in self.groups for path in group.files]

    def in_range(self, start: date | None, end: date | None) -> list[FileGroup]:
        return [group for group in self.groups if group.overlaps(start, end)]


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
    stats, unique = found
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
    return Published(dataset, root, groups, True, unique)


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
