"""Reading a dataset: the catalog names the files, Polars scans them.

A read never goes through the catalog for data. The catalog says which files
make up the table at a snapshot; the directory a file sits in says which
partition it is, so a date range drops whole files before any is opened; the
rest is a Parquet scan with the filter pushed down. The result comes back in
the dataset's declared order, which is total, so two reads of one snapshot
return the same rows in the same positions.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import polars as pl

from equitylake_core.catalog import Catalog, quote_identifier
from equitylake_core.spec import DatasetSpec


def _day(value: date | str | None) -> date | None:
    if value is None or (isinstance(value, date) and not isinstance(value, datetime)):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(value)


def empty(catalog: Catalog, spec: DatasetSpec) -> pl.DataFrame:
    """A frame with the dataset's columns and no rows."""
    select = ", ".join(
        f"CAST(NULL AS {kind}) AS {quote_identifier(name)}" for name, kind in spec.columns.items()
    )
    return catalog.relation(f"SELECT {select} WHERE false").pl()


def _in_range(spec: DatasetSpec, path: str, home: Path, start: date | None, end: date | None):
    """Whether the partition file *path* can hold a row dated within the range."""
    parent = Path(path).parent
    period = None if parent == home else spec.period(parent.name)
    if period is None:
        return True  # not a partition directory: only its rows can say
    first, last = period
    return (start is None or last >= start) and (end is None or first <= end)


def load(
    catalog: Catalog,
    spec: DatasetSpec,
    *,
    start: date | str | None = None,
    end: date | str | None = None,
    columns: Sequence[str] | None = None,
    snapshot: int | None = None,
    where: pl.Expr | None = None,
) -> pl.DataFrame:
    """The rows of *spec*'s dataset, in its declared order.

    *start* and *end* bound the time column, both inclusive, as dates.
    *where* keeps the rows a Polars expression over the dataset's columns
    is true for; it is pushed into the scan, so a filter on the leading
    sort column skips most of each file.
    *columns* picks and orders the columns returned. *snapshot* reads the
    lake as it was at that version; it stays readable until its files are
    cleaned up.
    """
    first, last = _day(start), _day(end)
    if (first is not None or last is not None) and spec.time is None:
        raise ValueError(f"{spec.name} has no time column to take a date range on")
    wanted = list(columns) if columns is not None else list(spec.columns)
    unknown = [column for column in wanted if column not in spec.columns]
    if unknown:
        raise ValueError(f"{spec.name} has no column(s) {', '.join(unknown)}")
    schema = empty(catalog, spec)
    if not catalog.has_table(spec.name):
        return schema.select(wanted)
    files = catalog.data_files(spec.name, snapshot)
    if files is None:
        frame = _through_catalog(catalog, spec, snapshot)
    else:
        home = catalog.table_dir(spec.name)
        files = [path for path in files if _in_range(spec, path, home, first, last)]
        if not files:
            return schema.select(wanted)
        frame = pl.scan_parquet(
            files,
            schema=schema.schema,
            hive_partitioning=False,
            missing_columns="insert",
            extra_columns="ignore",
        )
    if spec.time is not None and (first is not None or last is not None):
        moment = pl.col(spec.time)
        if schema.schema[spec.time] == pl.Date:
            low, high = first, None if last is None else last + timedelta(days=1)
        else:
            zone = getattr(schema.schema[spec.time], "time_zone", None)
            tzinfo = timezone.utc if zone else None
            low = None if first is None else datetime.combine(first, time.min, tzinfo)
            high = (
                None
                if last is None
                else datetime.combine(last + timedelta(days=1), time.min, tzinfo)
            )
        if low is not None:
            frame = frame.filter(moment >= low)
        if high is not None:
            frame = frame.filter(moment < high)
    if where is not None:
        frame = frame.filter(where)
    return frame.sort(list(spec.order), maintain_order=True).select(wanted).collect()


def _through_catalog(catalog: Catalog, spec: DatasetSpec, snapshot: int | None) -> pl.LazyFrame:
    """The table as the catalog serves it, for one whose files alone do not hold its rows."""
    at = "" if snapshot is None else f" AT (VERSION => {int(snapshot)})"
    select = ", ".join(quote_identifier(column) for column in spec.columns)
    return catalog.relation(f"SELECT {select} FROM {quote_identifier(spec.name)}{at}").pl().lazy()
