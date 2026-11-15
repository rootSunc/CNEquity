"""The coverage ledger: which cells of a dataset exist, and which were asked for and found empty.

A dataset's time is cut into *cells*: one per session, day, week, month,
quarter or year, and, when the dataset says so, one per entity within each.
A :class:`Coverage` declares that cut. Three things are then known about a
cell, and only the last of them is stored:

* **Expected** comes from the declaration and the market's calendar and
  universe (:mod:`equitylake_core.plan`).
* **Covered** is read off the published rows every time (:func:`stored`), or
  off the dataset's revisions for one that is refreshed rather than extended
  (:func:`refreshed`). Nothing keeps a second copy that could disagree.
* **Asked and empty** cannot be read off rows that do not exist, so it is
  recorded, append-only, in ``_attempts`` (:func:`attempted`).

An empty answer about a period that has barely ended may only mean "not
yet". :func:`standing` says which empty answers still stand at a given time.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Literal

import polars as pl

from equitylake_core.catalog import ALIAS, Catalog, quote_identifier, quote_literal

Grain = Literal["session", "day", "week", "month", "quarter", "year"]
By = Literal["rows", "write"]

GRAINS: tuple[Grain, ...] = ("session", "day", "week", "month", "quarter", "year")

#: One row per cell a write asked a source for and got nothing to store.
ATTEMPTS = "_attempts"
ATTEMPT_COLUMNS = (
    "dataset VARCHAR, period VARCHAR, entity VARCHAR, run VARCHAR, asked_at TIMESTAMPTZ"
)

CELLS_SCHEMA = {"period": pl.String, "entity": pl.String}
_NO_TIME = timedelta(0)


class CoverageError(ValueError):
    """A coverage declaration contradicts itself, or a label is not one of its grain."""


@dataclass(frozen=True)
class Cell:
    """One unit of a dataset that either holds rows or does not."""

    dataset: str
    #: The label of the period, e.g. ``2026-11-13``, ``2026-W46``, ``2026-11``, ``2026Q4``.
    period: str
    #: ``""`` for a dataset that is not covered entity by entity.
    entity: str = ""


@dataclass(frozen=True)
class Coverage:
    """How a dataset's time is cut into cells, and when a cell is owed.

    ``grain``
        The time one cell spans. ``session`` is a day the market's calendar
        names; the others are calendar periods.
    ``by``
        ``rows``: a cell is covered when a row falls in it. ``write``: the
        dataset is refreshed as a whole, so only its latest cell is owed (an
        earlier one cannot be had any more), and that cell is covered when the
        dataset was written, or asked for and found unchanged, on or after
        the cell's first day.
    ``column``
        The date or timestamp column that places a row in a cell (default:
        the dataset's time column). A timestamp's date is its UTC date.
    ``labelled``
        The column holds the cell's label itself (``2026Q3``) instead of a date.
    ``entity``
        The column naming the entity a row is about, for a dataset owed once
        per entity per period; the market's universe says which entities.
    ``start``, ``end``
        The first and the last day a cell is owed for: what the source
        serves, where that has an edge.
    ``lag``
        How long after its last day a cell becomes owed.
    ``settle``
        An empty answer given later than this after a cell's last day is
        final. An earlier one may mean "not yet" and stands only for
        ``recheck`` (the default, zero: it is asked again every time).
    """

    grain: Grain
    by: By = "rows"
    column: str | None = None
    labelled: bool = False
    entity: str | None = None
    start: date | None = None
    end: date | None = None
    lag: timedelta = _NO_TIME
    settle: timedelta = _NO_TIME
    recheck: timedelta = _NO_TIME

    def __post_init__(self) -> None:
        if self.grain not in GRAINS:
            raise CoverageError(f"unknown grain {self.grain!r}")
        if self.by not in ("rows", "write"):
            raise CoverageError(f"unknown coverage basis {self.by!r}")
        if self.by == "write":
            if self.column is not None or self.entity is not None or self.labelled:
                raise CoverageError("coverage by write has no column and no entity")
        for name in ("lag", "settle", "recheck"):
            if getattr(self, name) < _NO_TIME:
                raise CoverageError(f"{name} cannot be negative")


def label(grain: Grain, day: date) -> str:
    """The cell of *grain* that *day* falls in."""
    if grain in ("session", "day"):
        return day.isoformat()
    if grain == "week":
        iso = day.isocalendar()
        return f"{iso.year:04d}-W{iso.week:02d}"
    if grain == "month":
        return f"{day.year:04d}-{day.month:02d}"
    if grain == "quarter":
        return f"{day.year:04d}Q{(day.month + 2) // 3}"
    return f"{day.year:04d}"


def span(grain: Grain, period: str) -> tuple[date, date]:
    """The first and last day of the cell *period* of *grain*."""
    try:
        if grain in ("session", "day"):
            day = date.fromisoformat(period)
            return day, day
        if grain == "week":
            year, week = period.split("-W")
            first = date.fromisocalendar(int(year), int(week), 1)
            return first, first + timedelta(days=6)
        if grain == "month":
            year, month = (int(part) for part in period.split("-"))
            first = date(year, month, 1)
        elif grain == "quarter":
            year, quarter = (int(part) for part in period.split("Q"))
            if not 1 <= quarter <= 4:
                raise ValueError(period)
            first = date(year, 3 * quarter - 2, 1)
        else:
            return date(int(period), 1, 1), date(int(period), 12, 31)
    except ValueError:
        raise CoverageError(f"{period!r} is not a {grain} label") from None
    months = 1 if grain == "month" else 3
    following = first.month - 1 + months
    after = date(first.year + following // 12, following % 12 + 1, 1)
    return first, after - timedelta(days=1)


def periods(grain: Grain, start: date, end: date) -> list[str]:
    """Every calendar cell of *grain* with a day in *start*..*end*, earliest first.

    For ``session`` this is every day; which of them are sessions is the
    calendar's to say.
    """
    found = []
    day = start
    while day <= end:
        name = label(grain, day)
        found.append(name)
        day = span(grain, name)[1] + timedelta(days=1)
    return found


def label_sql(grain: Grain, day: str) -> str:
    """SQL: the cell of *grain* the DATE expression *day* falls in; agrees with :func:`label`."""
    if grain in ("session", "day"):
        return f"strftime({day}, '%Y-%m-%d')"
    if grain == "week":
        return f"strftime({day}, '%G-W%V')"
    if grain == "month":
        return f"strftime({day}, '%Y-%m')"
    if grain == "quarter":
        return f"strftime({day}, '%Y') || 'Q' || CAST(quarter({day}) AS VARCHAR)"
    return f"strftime({day}, '%Y')"


def no_cells() -> pl.DataFrame:
    return pl.DataFrame(schema=CELLS_SCHEMA)


def stored(
    catalog: Catalog,
    table: str,
    coverage: Coverage,
    *,
    column: str | None = None,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    """The cells of *table* that hold at least one published row: ``period``, ``entity``.

    *column* stands in for ``coverage.column`` when the declaration leaves it
    to the dataset's time column. *start* and *end* bound the cells looked
    at; a cell either bound falls in is looked at whole.
    """
    if coverage.by != "rows":
        raise CoverageError("only coverage by rows is read off rows")
    column = coverage.column or column
    if column is None:
        raise CoverageError(f"{table}: no column places a row in a cell")
    if not catalog.has_table(table):
        return no_cells()
    q = quote_identifier
    kind = catalog.column_types(table).get(column)
    if kind is None:
        raise CoverageError(f"{table} has no column {column}")
    first = None if start is None else span(coverage.grain, label(coverage.grain, start))[0]
    last = None if end is None else span(coverage.grain, label(coverage.grain, end))[1]
    if coverage.labelled:
        period = f"CAST({q(column)} AS VARCHAR)"
        where = [f"{q(column)} IS NOT NULL"]
        # Labels of one grain sort as their periods do.
        if first is not None:
            where.append(f"{period} >= {quote_literal(label(coverage.grain, first))}")
        if last is not None:
            where.append(f"{period} <= {quote_literal(label(coverage.grain, last))}")
    else:
        day = f"TRY_CAST({q(column)} AS DATE)"
        period = label_sql(coverage.grain, day)
        # A bound on the stored column itself lets the catalog skip files; a column
        # that only parses to a date has to be read to be bounded.
        bounded = q(column) if kind == "DATE" or kind.startswith("TIMESTAMP") else day
        where = [f"{day} IS NOT NULL"]
        if first is not None:
            where.append(f"{bounded} >= DATE {quote_literal(first.isoformat())}")
        if last is not None:
            after = last + timedelta(days=1)
            where.append(f"{bounded} < DATE {quote_literal(after.isoformat())}")
    known: set[str] = set()
    source = q(table)
    if kind == "DATE" and not coverage.labelled and coverage.entity is None:
        # A file whose smallest and largest date fall in one cell says so in its
        # statistics; only a file spanning several cells has to be opened.
        split = _split_by_statistics(catalog, table, column, coverage.grain, first, last)
        if split is not None:
            known, unsettled = split
            if not unsettled:
                return _cells(known)
            source = "read_parquet([" + ", ".join(quote_literal(path) for path in unsettled) + "])"
    entity = f"COALESCE(CAST({q(coverage.entity)} AS VARCHAR), '')" if coverage.entity else "''"
    found = catalog.relation(
        f"SELECT DISTINCT {period} AS period, {entity} AS entity "
        f"FROM {source} WHERE {' AND '.join(where)}"
    ).pl()
    return pl.concat([found, _cells(known)]).unique() if known else found


def _cells(periods: set[str]) -> pl.DataFrame:
    return pl.DataFrame(
        {"period": sorted(periods), "entity": [""] * len(periods)}, schema=CELLS_SCHEMA
    )


def _split_by_statistics(
    catalog: Catalog, table: str, column: str, grain: Grain, first: date | None, last: date | None
) -> tuple[set[str], list[str]] | None:
    """Cells the file statistics alone prove covered, and the files that must be read.

    ``None`` when the table's files do not hold exactly its rows.
    """
    stats = catalog.file_stats(table, column)
    if stats is None:
        return None
    known: set[str] = set()
    unsettled: list[str] = []
    for item in stats:
        try:
            low, high = date.fromisoformat(item.low), date.fromisoformat(item.high)
        except (TypeError, ValueError):
            unsettled.append(item.path)
            continue
        if (first is not None and high < first) or (last is not None and low > last):
            continue
        name = label(grain, low)
        if name == label(grain, high):
            known.add(name)
        else:
            unsettled.append(item.path)
    return known, unsettled


def refreshed(catalog: Catalog, dataset: str, *, revisions: str) -> datetime | None:
    """When *dataset* was last written, from the record table *revisions*."""
    if not catalog.has_table(revisions):
        return None
    found = catalog.relation(
        "SELECT max(s.snapshot_time) AS at "
        f"FROM {quote_identifier(revisions)} r "
        f"JOIN ducklake_snapshots({quote_literal(ALIAS)}) s ON s.snapshot_id = r.snapshot_id "
        "WHERE r.dataset = ?",
        [dataset],
    ).pl()
    return found.cast({"at": pl.Datetime("us", "UTC")})["at"][0]


def attempted(catalog: Catalog, dataset: str) -> pl.DataFrame:
    """Each cell of *dataset* asked for and found empty, with when it was last asked: ``at``."""
    schema = {**CELLS_SCHEMA, "at": pl.Datetime("us", "UTC")}
    if not catalog.has_table(ATTEMPTS):
        return pl.DataFrame(schema=schema)
    return (
        catalog.relation(
            f"SELECT period, entity, max(asked_at) AS at FROM {ATTEMPTS} WHERE dataset = ? GROUP BY ALL",
            [dataset],
        )
        .pl()
        .cast(schema)
    )


def standing(attempts: pl.DataFrame, coverage: Coverage, now: datetime) -> pl.DataFrame:
    """The cells whose empty answer still stands at *now*: ``period``, ``entity``."""
    if attempts.is_empty():
        return no_cells()
    final = {}
    for period in attempts["period"].unique():
        try:
            last = span(coverage.grain, period)[1]
        except CoverageError:
            continue  # recorded under another grain: it says nothing about these cells
        final[period] = (
            datetime.combine(last + timedelta(days=1), time.min, timezone.utc) + coverage.settle
        )
    if not final:
        return no_cells()
    cutoffs = pl.DataFrame(
        {"period": list(final), "final_from": list(final.values())},
        schema={"period": pl.String, "final_from": pl.Datetime("us", "UTC")},
    )
    return (
        attempts.join(cutoffs, on="period")
        .filter((pl.col("at") >= pl.col("final_from")) | (pl.col("at") > now - coverage.recheck))
        .select("period", "entity")
    )


def prepare(catalog: Catalog) -> None:
    """Make ``_attempts`` and keep its rows in the catalog database; outside a transaction."""
    if not catalog.has_table(ATTEMPTS):
        catalog.execute(f"CREATE TABLE {ATTEMPTS} ({ATTEMPT_COLUMNS})")
    catalog.keep_rows_inline(ATTEMPTS)


def record(catalog: Catalog, rows: pl.DataFrame, *, message: str | None = None) -> int | None:
    """Append *rows* to ``_attempts`` in one snapshot; returns it, or ``None`` for no rows.

    *rows* has the columns ``dataset``, ``period``, ``entity``, ``run`` and
    ``asked_at``. For a writer that publishes its own way: one that writes
    through :class:`~equitylake_core.lake.LakeWrite` says ``empty`` instead,
    and its attempts commit with its rows.
    """
    if rows.is_empty():
        return None
    columns = ["dataset", "period", "entity", "run", "asked_at"]
    frame = rows.select(columns).cast(
        {
            "dataset": pl.String,
            "period": pl.String,
            "entity": pl.String,
            "run": pl.String,
            "asked_at": pl.Datetime("us", "UTC"),
        }
    )
    prepare(catalog)
    catalog.con.abort()
    catalog.con.register("_el_attempts", frame.to_arrow())
    try:
        with catalog.transaction(message=message):
            catalog.execute(
                f"INSERT INTO {ATTEMPTS} ({', '.join(columns)}) "
                f"SELECT {', '.join(columns)} FROM _el_attempts"
            )
    finally:
        catalog.con.unregister("_el_attempts")
    return catalog.snapshot()


def cell_keys(dataset: str, cells: Iterable[str | tuple[str, str] | Cell]) -> list[tuple[str, str]]:
    """Each of *cells* as its period and entity; a bare label is a cell without an entity."""
    keys = []
    for cell in cells:
        if isinstance(cell, Cell):
            if cell.dataset != dataset:
                raise CoverageError(f"a cell of {cell.dataset} was recorded under {dataset}")
            keys.append((cell.period, cell.entity))
        elif isinstance(cell, str):
            keys.append((cell, ""))
        else:
            period, entity = cell
            keys.append((period, entity))
    return keys
