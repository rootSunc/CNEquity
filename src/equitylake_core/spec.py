"""What a market declares about a dataset: the one seam the core reads a market through.

A :class:`DatasetSpec` says what a table holds and how it is kept: its
columns and their SQL types, the key that makes a row unique, the column that
places a row in time, how finely the table is split into partition files, and
the rules a row must meet to be stored at all. The core never names a dataset
or a vendor; it works on whatever the :class:`Registry` it is handed declares.
"""

from __future__ import annotations

import re
from calendar import monthrange
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import date
from types import MappingProxyType
from typing import Literal

Partition = Literal["none", "year", "month", "day"]

PARTITIONS: tuple[Partition, ...] = ("none", "year", "month", "day")

_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
#: ``strftime`` format of a partition's label, by granularity.
_LABEL_FORMAT = {"year": "%Y", "month": "%Y-%m", "day": "%Y-%m-%d"}


class SpecError(ValueError):
    """A dataset declaration contradicts itself."""


@dataclass(frozen=True)
class Rule:
    """A hard error: a row for which *ok* is false is refused, the rest of its batch is kept.

    *ok* is a SQL boolean expression over the row's columns. A row it cannot
    decide (a NULL operand) passes: a missing value is not an error.
    """

    name: str
    ok: str


@dataclass(frozen=True)
class DatasetSpec:
    """One dataset of a market.

    ``columns``
        Column name to SQL type, in the table's column order.
    ``key``
        The columns that identify a row. Writing a row whose key is already
        stored replaces the stored row.
    ``time``
        The date or timestamp column that places a row in time: partitions and
        date-range reads go by it. A timestamp's date is its UTC date.
    ``partition``
        How much time one partition file covers. ``none`` keeps the table in
        one file; pick the coarsest period that keeps a daily write small.
    ``sort``
        Row order inside a partition file (default: ``key``). Lead with the
        column most reads filter on, so a scan can skip row groups.
    ``order``
        Row order of a read (default: ``time`` first, then the rest of
        ``key``), so appending a later period never moves an earlier row.
    ``rules``
        Hard errors, checked on write.
    """

    name: str
    columns: Mapping[str, str]
    key: tuple[str, ...]
    time: str | None = None
    partition: Partition = "none"
    sort: tuple[str, ...] = ()
    order: tuple[str, ...] = ()
    rules: tuple[Rule, ...] = ()
    description: str = ""

    def __post_init__(self) -> None:
        if not _NAME.match(self.name):
            raise SpecError(
                f"{self.name!r}: a dataset name is lower-case letters, digits and "
                "underscores, starting with a letter"
            )
        columns = MappingProxyType({name: kind.strip() for name, kind in self.columns.items()})
        if not columns:
            raise SpecError(f"{self.name}: no columns")
        object.__setattr__(self, "columns", columns)
        key = tuple(self.key)
        if not key:
            raise SpecError(f"{self.name}: no key")
        object.__setattr__(self, "key", key)
        if self.partition not in PARTITIONS:
            raise SpecError(f"{self.name}: unknown partition {self.partition!r}")
        if self.partition != "none" and self.time is None:
            raise SpecError(f"{self.name}: a partitioned dataset needs a time column")
        sort = tuple(self.sort) or key
        order = tuple(self.order) or (
            (self.time, *(column for column in key if column != self.time)) if self.time else key
        )
        object.__setattr__(self, "sort", sort)
        object.__setattr__(self, "order", order)
        object.__setattr__(self, "rules", tuple(self.rules))
        used = {
            "key": key,
            "time": (self.time,) if self.time else (),
            "sort": sort,
            "order": order,
        }
        for role, names in used.items():
            for column in names:
                if column not in columns:
                    raise SpecError(f"{self.name}: {role} column {column!r} is not a column")
        if len(set(key)) != len(key):
            raise SpecError(f"{self.name}: a key column is named twice")
        names = [rule.name for rule in self.rules]
        if len(set(names)) != len(names):
            raise SpecError(f"{self.name}: two rules share a name")

    @property
    def key_spans_partitions(self) -> bool:
        """Whether one key could be stored in two partitions (the key does not fix the time)."""
        return self.partition != "none" and self.time not in self.key

    def label(self, day: date) -> str:
        """The partition a row dated *day* belongs to; ``""`` for an unpartitioned dataset."""
        if self.partition == "none":
            return ""
        return day.strftime(_LABEL_FORMAT[self.partition])

    def label_sql(self) -> str:
        """SQL: the partition label of a row, from its time column."""
        if self.partition == "none":
            return "''"
        return f"strftime(\"{self.time}\", '{_LABEL_FORMAT[self.partition]}')"

    def period(self, label: str) -> tuple[date, date] | None:
        """The first and last day partition *label* covers, or ``None`` if it is not a label."""
        if self.partition == "none":
            return None
        try:
            parts = [int(part) for part in label.split("-")]
            if self.partition == "year" and len(parts) == 1:
                return date(parts[0], 1, 1), date(parts[0], 12, 31)
            if self.partition == "month" and len(parts) == 2:
                year, month = parts
                return date(year, month, 1), date(year, month, monthrange(year, month)[1])
            if self.partition == "day" and len(parts) == 3:
                day = date(*parts)
                return day, day
        except ValueError:
            return None
        return None


class Registry:
    """The datasets of one market, by name."""

    def __init__(self, specs: Iterable[DatasetSpec]):
        self._specs: dict[str, DatasetSpec] = {}
        for spec in specs:
            if spec.name in self._specs:
                raise SpecError(f"dataset {spec.name} is declared twice")
            self._specs[spec.name] = spec

    def __getitem__(self, name: str) -> DatasetSpec:
        try:
            return self._specs[name]
        except KeyError:
            raise KeyError(f"no dataset named {name!r}") from None

    def __contains__(self, name: object) -> bool:
        return name in self._specs

    def __iter__(self) -> Iterator[DatasetSpec]:
        return iter(self._specs.values())

    def __len__(self) -> int:
        return len(self._specs)

    def names(self) -> list[str]:
        return list(self._specs)
