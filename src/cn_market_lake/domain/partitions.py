"""Partition period arithmetic — how a date maps to a partition directory.

Every curated dataset is partitioned by a date column, but the *right* period
per partition is not the same for all of them. Partitioning by day is correct
for ``daily_bars`` (~4,900 rows a day) and pathological for ``trading_calendar``
(one row a day): a Parquet file's footer and column metadata cost roughly a
kilobyte regardless of content, so a one-row-per-day dataset spends 4,220 files
and 16MB to store what fits in a single 50KB file, and every scan pays 4,220
file opens to read it.

``DatasetSpec.partition_granularity`` picks the period; this module owns the
mapping between a date and its partition directory value, in both directions.

Directory values stay unambiguous and lexicographically sortable:
``trade_date=2024-06-03`` (day), ``trade_date=2024-06`` (month),
``trade_date=2024`` (year). Report-period datasets also use self-describing
``report_period=2016Q1`` (calendar quarter).

**Hive partitioning is off for coarse periods, deliberately.** Polars infers the
hive column's type from the matching file column, so a ``trade_date=2024``
directory alongside a Date column raises ``could not find a 'date/datetime'
pattern for '2024'``. The real date column lives in the file regardless, so
nothing is lost by reading it from there; pruning is done by selecting
directories whose period overlaps the query window, which is exact rather than
approximate.
"""

from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date
from typing import Literal

Granularity = Literal["day", "month", "quarter", "year"]

GRANULARITIES: tuple[Granularity, ...] = ("day", "month", "quarter", "year")


@dataclass(frozen=True)
class Partition:
    """One partition directory: its literal value and the dates it covers."""

    value: str
    start: date
    end: date

    def covers(self, d: date) -> bool:
        return self.start <= d <= self.end

    def overlaps(self, start: date | None, end: date | None) -> bool:
        if start is not None and self.end < start:
            return False
        if end is not None and self.start > end:
            return False
        return True


def partition_value(d: date, granularity: Granularity) -> str:
    """Directory value for the partition holding *d*."""
    if granularity == "year":
        return f"{d.year:04d}"
    if granularity == "quarter":
        return f"{d.year:04d}Q{(d.month - 1) // 3 + 1}"
    if granularity == "month":
        return f"{d.year:04d}-{d.month:02d}"
    return d.isoformat()


def _parse_quarter(value: str) -> Partition | None:
    """``2016Q1`` … ``2016Q4`` → calendar-quarter bounds (report_period dirs)."""
    if len(value) != 6 or value[4] not in ("Q", "q"):
        return None
    try:
        year = int(value[:4])
        quarter = int(value[5])
    except ValueError:
        return None
    if quarter not in (1, 2, 3, 4):
        return None
    start_month = 3 * (quarter - 1) + 1
    end_month = start_month + 2
    end_day = monthrange(year, end_month)[1]
    return Partition(value, date(year, start_month, 1), date(year, end_month, end_day))


def parse_partition(value: str) -> Partition | None:
    """Inverse of :func:`partition_value`, inferring the period from its shape.

    Deliberately *not* parameterised by the dataset's configured granularity.
    A partition directory is self-describing — ``2024``, ``2024-06``,
    ``2024-06-03``, and ``2016Q1`` are unambiguous — and reading it that way is
    what lets the registry's granularity change without a migration: a lake
    still holding day directories keeps being read correctly, just with finer
    partitions than new writes will produce. Had this trusted the configured
    granularity instead, flipping a dataset to year would have made every
    existing day directory unparseable and every range query silently return
    nothing.

    Returns None for anything that is not a period, so stray directories are
    skipped rather than given a wrong range.
    """
    quarter = _parse_quarter(value)
    if quarter is not None:
        return quarter
    parts = value.split("-")
    try:
        if len(parts) == 1:
            year = int(parts[0])
            if len(parts[0]) != 4:
                return None
            return Partition(value, date(year, 1, 1), date(year, 12, 31))
        if len(parts) == 2:
            year, month = int(parts[0]), int(parts[1])
            last = monthrange(year, month)[1]
            return Partition(value, date(year, month, 1), date(year, month, last))
        if len(parts) == 3:
            day = date.fromisoformat(value)
            return Partition(value, day, day)
        return None
    except (ValueError, TypeError):
        return None


def granularity_of(part: Partition) -> Granularity:
    """Which period *part* actually spans, as written on disk."""
    if part.start == part.end:
        return "day"
    if part.start.month == 1 and part.end.month == 12:
        return "year"
    # Report-period dirs (``2016Q1``) span a calendar quarter. Checked after
    # year so Q1..Q4 of a year-partitioned dataset cannot be mistaken for one.
    if (
        part.start.day == 1
        and part.start.month in (1, 4, 7, 10)
        and part.end.month == part.start.month + 2
    ):
        return "quarter"
    return "month"


def previous_partition(part: Partition, granularity: Granularity | None = None) -> str:
    """Directory value of the period immediately before *part*."""
    granularity = granularity or granularity_of(part)
    if granularity == "year":
        return partition_value(part.start.replace(year=part.start.year - 1), granularity)
    if granularity == "quarter":
        first = part.start
        prev = (
            date(first.year - 1, 10, 1)
            if first.month == 1
            else date(first.year, first.month - 3, 1)
        )
        return partition_value(prev, granularity)
    if granularity == "month":
        first = part.start
        prev_year = first.year - 1 if first.month == 1 else first.year
        prev_month = 12 if first.month == 1 else first.month - 1
        return partition_value(date(prev_year, prev_month, 1), granularity)
    return partition_value(date.fromordinal(part.start.toordinal() - 1), granularity)


def uses_hive(granularity: Granularity) -> bool:
    """Whether polars may parse this granularity's directory value as the column.

    Only day values round-trip as a date; see the module docstring.
    """
    return granularity == "day"
