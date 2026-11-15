"""The planner: the cells owed within a scope, minus the covered, minus the known empty.

Filling a new lake, the daily update, a backfill and a repair are the same
question asked over different scopes: which cells in this range are owed and
not there? :func:`plan` answers it and nothing else. It reads the lake and
sends no request; turning the gaps into requests is the source's part.

Every cell in a scope is in exactly one :data:`State`.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Literal

import polars as pl

from equitylake_core import coverage as cov
from equitylake_core.catalog import Catalog
from equitylake_core.coverage import Cell, Coverage
from equitylake_core.market import Calendar, Universe

State = Literal["covered", "empty", "gap", "not_due"]

#: ``covered``: rows are stored. ``empty``: asked, nothing there, and the answer stands.
#: ``gap``: owed and neither. ``not_due``: its period is not over, or not long enough.
STATES: tuple[State, ...] = ("covered", "empty", "gap", "not_due")

Redo = Literal["gaps", "empty", "all"]

_PLAN_SCHEMA = {"period": pl.String, "entity": pl.String, "state": pl.String}


def _end_of(day: date) -> datetime:
    return datetime.combine(day, time.min) + timedelta(days=1)


@dataclass(frozen=True)
class Scope:
    """The part of a lake a run is about.

    ``end`` is the last day whose data the market holds to be final: a cell
    is owed once its last day, plus the dataset's lag, is not after it.
    ``redo`` widens what counts as a gap: ``empty`` asks again for cells
    known to be empty, ``all`` for every owed cell, covered or not.
    """

    start: date
    end: date
    datasets: tuple[str, ...] | None = None
    entities: frozenset[str] | None = None
    redo: Redo = "gaps"

    def __post_init__(self) -> None:
        if self.end < self.start:
            raise ValueError(f"a scope cannot end ({self.end}) before it starts ({self.start})")
        if self.redo not in ("gaps", "empty", "all"):
            raise ValueError(f"unknown redo {self.redo!r}")


@dataclass(frozen=True)
class DatasetPlan:
    dataset: str
    #: One row per cell in scope: ``period``, ``entity``, ``state``; by period, then entity.
    cells: pl.DataFrame

    def counts(self) -> dict[State, int]:
        found = dict(self.cells.group_by("state").len().iter_rows())
        return {state: int(found.get(state, 0)) for state in STATES}

    def gaps(self) -> list[Cell]:
        rows = self.cells.filter(pl.col("state") == "gap")
        return [
            Cell(self.dataset, period, entity)
            for period, entity in rows.select("period", "entity").iter_rows()
        ]


@dataclass(frozen=True)
class Plan:
    scope: Scope
    now: datetime
    datasets: dict[str, DatasetPlan] = field(default_factory=dict)

    def gaps(self) -> list[Cell]:
        return [cell for item in self.datasets.values() for cell in item.gaps()]

    def counts(self) -> dict[str, dict[State, int]]:
        return {name: item.counts() for name, item in self.datasets.items()}


def owed(
    dataset: str,
    coverage: Coverage,
    scope: Scope,
    *,
    calendar: Calendar | None = None,
    universe: Universe | None = None,
) -> pl.DataFrame:
    """The cells of *dataset* in *scope*: ``period``, ``entity``, and whether each is ``due``."""
    schema = {**cov.CELLS_SCHEMA, "due": pl.Boolean}
    start = max(scope.start, coverage.start) if coverage.start else scope.start
    end = min(scope.end, coverage.end) if coverage.end else scope.end
    if start > end:
        return pl.DataFrame(schema=schema)
    if coverage.grain == "session":
        if calendar is None:
            raise ValueError(f"{dataset} is covered by session and no calendar was given")
        names = [day.isoformat() for day in calendar.sessions(start, end)]
    else:
        names = cov.periods(coverage.grain, start, end)
    spans = [cov.span(coverage.grain, name) for name in names]
    cells = pl.DataFrame(
        {
            "period": names,
            "first": [first for first, _ in spans],
            "last": [last for _, last in spans],
        },
        schema={"period": pl.String, "first": pl.Date, "last": pl.Date},
    ).with_columns(
        due=pl.Series([_end_of(last) + coverage.lag <= _end_of(scope.end) for _, last in spans])
    )
    if coverage.entity is None:
        return cells.select("period", pl.lit("").alias("entity"), "due")
    if universe is None:
        raise ValueError(f"{dataset} is covered by {coverage.entity} and no universe was given")
    if cells.is_empty():
        return pl.DataFrame(schema=schema)
    members = universe.members(dataset, cells["first"].min(), cells["last"].max()).select(
        pl.col("entity").cast(pl.String),
        pl.col("first").cast(pl.Date).fill_null(date.min).alias("member_first"),
        pl.col("last").cast(pl.Date).fill_null(date.max).alias("member_last"),
    )
    if scope.entities is not None:
        members = members.filter(pl.col("entity").is_in(sorted(scope.entities)))
    return (
        cells.join_where(
            members,
            pl.col("member_first") <= pl.col("last"),
            pl.col("member_last") >= pl.col("first"),
        )
        .select("period", "entity", "due")
        .unique(subset=["period", "entity"])
    )


def plan_dataset(
    catalog: Catalog,
    dataset: str,
    coverage: Coverage,
    scope: Scope,
    *,
    now: datetime | None = None,
    column: str | None = None,
    calendar: Calendar | None = None,
    universe: Universe | None = None,
    revisions: str = "_revisions",
) -> DatasetPlan:
    """The state of every cell of *dataset* in *scope*.

    *column* places a row in a cell when the declaration does not name one.
    """
    moment = now or datetime.now(timezone.utc)
    cells = owed(dataset, coverage, scope, calendar=calendar, universe=universe)
    if cells.is_empty():
        return DatasetPlan(dataset, pl.DataFrame(schema=_PLAN_SCHEMA))
    attempts = cov.attempted(catalog, dataset)
    if coverage.by == "write":
        return _plan_refresh(catalog, dataset, coverage, scope, cells, attempts, revisions)
    first = cov.span(coverage.grain, cells["period"].min())[0]
    last = cov.span(coverage.grain, cells["period"].max())[1]
    have = cov.stored(catalog, dataset, coverage, column=column, start=first, end=last)
    empty = cov.standing(attempts, coverage, moment)
    keys = ["period", "entity"]
    marked = cells.join(
        have.with_columns(_have=pl.lit(True)), on=keys, how="left", coalesce=True
    ).join(empty.unique().with_columns(_empty=pl.lit(True)), on=keys, how="left", coalesce=True)
    have_it, is_empty, due = (
        pl.col("_have").fill_null(False),
        pl.col("_empty").fill_null(False),
        pl.col("due"),
    )
    if scope.redo == "all":
        state = pl.when(~due).then(pl.lit("not_due")).otherwise(pl.lit("gap"))
    else:
        state = pl.when(have_it).then(pl.lit("covered")).when(~due).then(pl.lit("not_due"))
        if scope.redo == "gaps":
            state = state.when(is_empty).then(pl.lit("empty"))
        state = state.otherwise(pl.lit("gap"))
    return DatasetPlan(
        dataset, marked.select("period", "entity", state.alias("state")).sort("period", "entity")
    )


def _plan_refresh(
    catalog: Catalog,
    dataset: str,
    coverage: Coverage,
    scope: Scope,
    cells: pl.DataFrame,
    attempts: pl.DataFrame,
    revisions: str,
) -> DatasetPlan:
    """The one cell a refreshed dataset owes: the latest that is due.

    A refresh that found nothing new counts as one, so the latest write and
    the latest attempt are the same evidence.
    """
    due = cells.filter(pl.col("due"))
    if due.is_empty():
        return DatasetPlan(dataset, pl.DataFrame(schema=_PLAN_SCHEMA))
    period = due["period"].max()
    opens = datetime.combine(cov.span(coverage.grain, period)[0], time.min, timezone.utc)
    seen = [cov.refreshed(catalog, dataset, revisions=revisions)]
    if not attempts.is_empty():
        seen.append(attempts["at"].max())
    latest = max((moment for moment in seen if moment is not None), default=None)
    fresh = latest is not None and latest >= opens and scope.redo != "all"
    return DatasetPlan(
        dataset,
        pl.DataFrame(
            {"period": [period], "entity": [""], "state": ["covered" if fresh else "gap"]},
            schema=_PLAN_SCHEMA,
        ),
    )


def plan(
    lake,
    scope: Scope,
    *,
    now: datetime | None = None,
    calendar: Calendar | None = None,
    universe: Universe | None = None,
) -> Plan:
    """The state of every cell in *scope*, for each dataset of *lake* that declares coverage."""
    moment = now or datetime.now(timezone.utc)
    wanted: Sequence[str] = scope.datasets if scope.datasets is not None else lake.registry.names()
    found: dict[str, DatasetPlan] = {}
    with lake.catalog() as catalog:
        for name in wanted:
            spec = lake.registry[name]
            if spec.coverage is None:
                if scope.datasets is not None:
                    raise ValueError(f"{name} declares no coverage")
                continue
            found[name] = plan_dataset(
                catalog,
                name,
                spec.coverage,
                scope,
                now=moment,
                column=spec.time,
                calendar=calendar,
                universe=universe,
            )
    return Plan(scope, moment, found)
