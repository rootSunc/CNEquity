"""What the planner would say about this lake, beside what the state files say.

The daily run still decides what to fetch from watermarks, the missing-date
ledger and ``observed_through`` (``meta/state``). This module derives each
dataset's cells from what its spec already declares, asks the core planner
which of them are gaps, and sets the answer beside that state, so the two can
be compared on a real lake before the planner drives anything.

The planner knows a cell was asked about and found empty only from
``_attempts``. :func:`seeds` reads what the state files and the run manifest
already say about that, and :func:`record_seeds` writes it there once.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime

import polars as pl

from cnequity.domain.datasets import (
    DATASETS,
    DatasetSpec,
    history_mode_for,
    is_dataset_enabled,
    removed_from_schedule,
)
from cnequity.file_lock import lake_mutation_lock
from cnequity.quality.freshness import freshness_owed
from cnequity.query.calendar import list_trading_dates
from cnequity.sqlite_readonly import connect_readonly
from cnequity.storage.catalog_bridge import lake_root, revision_snapshots
from cnequity.storage.observation import freshness_mark
from cnequity.storage.state import StateStore
from equitylake_core import coverage as cov
from equitylake_core.catalog import ALIAS, Catalog, quote_literal
from equitylake_core.coverage import Coverage
from equitylake_core.plan import Scope, plan_dataset


def coverage_of(spec: DatasetSpec, today: date, config=None) -> Coverage:
    """The cells *spec*'s dataset owes, from the facts its spec already states.

    A dataset walked by date (it keeps a watermark, or is derived day by day)
    owes a cell per session, or per calendar day for a feed that publishes on
    weekends. One keyed by reporting period owes a cell per quarter. Anything
    else is refreshed as a whole and owes its latest session; so does a live
    snapshot no source can replay, since a day it missed cannot be had any
    more. What the source serves bounds all of them. A spec that names its
    ``cells`` overrides the rule.
    """
    start, end = spec.earliest_available(today), spec.source_retired_date
    if spec.cells == "latest" or (
        spec.cells is None and history_mode_for(spec, config) == "snapshot_only"
    ):
        return Coverage("session", by="write", start=start, end=end)
    if spec.cells is not None:
        return Coverage(spec.cells, column=spec.query_date_col, start=start, end=end)
    by_date = spec.watermark or (spec.layer == "derived" and spec.query_date_col == "trade_date")
    if spec.partition_col is not None and by_date:
        grain = "day" if spec.session_scope == "calendar" else "session"
        return Coverage(grain, column=spec.query_date_col, start=start, end=end)
    if spec.partition_col is not None and spec.partition_granularity == "quarter":
        return Coverage("quarter", column=spec.partition_col, labelled=True, start=start, end=end)
    return Coverage("session", by="write", start=start, end=end)


class LakeCalendar:
    """The exchange sessions the lake's own trading calendar names.

    Read once and widened when a longer range is asked for: every dataset
    asks, and a read of the published calendar costs more than a plan.
    """

    def __init__(self, config):
        self._config = config
        self._span: tuple[date, date] | None = None
        self._days: list[date] = []

    def sessions(self, start: date, end: date) -> Sequence[date]:
        if self._span is None or start < self._span[0] or end > self._span[1]:
            low = start if self._span is None else min(start, self._span[0])
            high = end if self._span is None else max(end, self._span[1])
            self._days = list_trading_dates(self._config, low, high)
            self._span = (low, high)
        first, after = bisect_left(self._days, start), bisect_right(self._days, end)
        return self._days[first:after]


@dataclass(frozen=True)
class Comparison:
    dataset: str
    basis: str
    covered: int
    gaps: tuple[str, ...]
    #: Whether the dataset is supposed to keep up: enabled, scheduled, and its source usable.
    scheduled: bool
    #: The latest of the watermark, the last clean look at the source and the last snapshot.
    mark: date | None
    #: Days the missing-date ledger owes, inside the window.
    owed: tuple[date, ...]
    #: Gaps the state files also hold to be missing: owed, or after the mark.
    agreed: tuple[str, ...]
    #: Gaps the state files count as asked and empty: at or before the mark and not owed.
    asked_empty: tuple[str, ...]
    #: Cells that hold rows though the ledger still owes their day.
    owed_but_stored: tuple[str, ...]


def compare(
    config, start: date, end: date, *, datasets: Iterable[str] | None = None
) -> list[Comparison]:
    """The planner's gaps from *start* to *end* beside the state files', dataset by dataset."""
    calendar, state = LakeCalendar(config), StateStore(config.meta_root)
    scope = Scope(start, end)
    found = []
    with Catalog(lake_root(config), read_only=True) as catalog:
        written = last_written(catalog)
        for name in datasets if datasets is not None else DATASETS:
            spec = DATASETS[name]
            coverage = coverage_of(spec, end, config)
            cells = plan_dataset(
                catalog, name, coverage, scope, calendar=calendar, written=written.get(name)
            ).cells
            states = dict(cells.select("period", "state").iter_rows())
            gaps = tuple(period for period, value in states.items() if value == "gap")
            mark = freshness_mark(config, name, state.get_date(name))
            snapshot = state.get_date(name, field="last_snapshot_date")
            mark = max((day for day in (mark, snapshot) if day is not None), default=None)
            owed = tuple(
                sorted(day for day in state.get_missing_dates(name) if start <= day <= end)
            )
            by_day = coverage.grain in ("session", "day")
            owed_labels = {day.isoformat() for day in owed}
            asked = tuple(
                period
                for period in gaps
                if by_day
                and mark is not None
                and period <= mark.isoformat()
                and period not in owed_labels
            )
            found.append(
                Comparison(
                    dataset=name,
                    basis=f"{coverage.grain}/{coverage.by}",
                    scheduled=is_dataset_enabled(name, config)
                    and not removed_from_schedule(config, name)
                    and freshness_owed(config, name),
                    covered=sum(value == "covered" for value in states.values()),
                    gaps=gaps,
                    mark=mark,
                    owed=owed,
                    agreed=tuple(period for period in gaps if period not in asked),
                    asked_empty=asked,
                    owed_but_stored=tuple(
                        period for period in sorted(owed_labels) if states.get(period) == "covered"
                    ),
                )
            )
    return found


def last_written(catalog: Catalog) -> dict[str, datetime]:
    """When each dataset was last published, by the catalog snapshot of its latest revision.

    The lake keeps its revisions in two tables (see ``revision_snapshots``),
    not in the core's record table, so the planner is told rather than reads.
    """
    newest: dict[str, int] = {}
    for dataset, _revision, snapshot, _unique in revision_snapshots(catalog):
        newest[dataset] = max(snapshot, newest.get(dataset, snapshot))
    if not newest:
        return {}
    times = dict(
        catalog.execute(
            "SELECT snapshot_id, snapshot_time AT TIME ZONE 'UTC' "
            f"FROM ducklake_snapshots({quote_literal(ALIAS)})"
        ).fetchall()
    )
    return {
        dataset: times[snapshot].replace(tzinfo=UTC)
        for dataset, snapshot in newest.items()
        if snapshot in times
    }


def refreshes(manifest_path) -> dict[str, datetime]:
    """When each dataset was last fetched without failing, from the run manifest.

    A refresh that found nothing new publishes nothing, so the manifest is the
    only place it shows.
    """
    try:
        connection = connect_readonly(manifest_path)
    except Exception:  # no manifest yet: no runs to tell of
        return {}
    try:
        rows = connection.execute(
            "SELECT d.dataset, max(COALESCE(r.finished_at, r.started_at)) "
            "FROM dataset_results d JOIN ingestion_runs r USING (run_id) "
            "WHERE d.stage = 'fetch' AND d.status IN ('success', 'warning') GROUP BY d.dataset"
        ).fetchall()
    finally:
        connection.close()
    found = {}
    for dataset, moment in rows:
        if moment:
            stamp = datetime.fromisoformat(moment)
            found[dataset] = stamp if stamp.tzinfo else stamp.replace(tzinfo=UTC)
    return found


@dataclass(frozen=True)
class Seed:
    """What the old ledgers say was asked of one dataset's source and found empty."""

    dataset: str
    #: ``state``: cells the state files hold done that have no rows.
    #: ``refresh``: a refresh the manifest records that published nothing new.
    reason: str
    periods: tuple[str, ...]
    asked_at: datetime


#: A dataset with rows in this share of the cells of its stored span is dense in
#: practice: a cell without rows there is more likely missed than empty.
DENSE_SHARE = 0.95
#: This many empty cells in a row, in a feed that has rows around them, is a
#: stretch that was never fetched rather than a run of empty answers.
LONG_RUN = {"session": 20, "day": 20, "week": 4}


def settled_empty(
    cells: pl.DataFrame,
    *,
    mark: str | None,
    first: str | None,
    owed: set[str],
    long_run: int,
) -> list[str]:
    """The gaps the state files hold done and that read as answers rather than misses.

    From the first covered cell through *mark*, not owed by the missing-date
    ledger. Left a gap, because a gap costs one more request and a wrong
    empty hides a hole for good:

    * anything before the first stored row, where the old ledgers never looked;
    * every gap of a dataset dense in practice (``DENSE_SHARE``);
    * a run of *long_run* or more consecutive gaps.
    """
    if mark is None or first is None:
        return []
    span = cells.filter((pl.col("period") >= first) & (pl.col("period") <= mark))
    if span.is_empty() or (span["state"] == "covered").mean() >= DENSE_SHARE:
        return []
    found, run = [], []
    for period, state in span.select("period", "state").iter_rows():
        if state == "gap":
            run.append(period)
            continue
        if len(run) < long_run:
            found.extend(run)
        run = []
    if len(run) < long_run:
        found.extend(run)
    return [period for period in found if period not in owed]


def _dense(spec: DatasetSpec) -> bool:
    """Whether the dataset promises a row every session, so a session without one is a hole."""
    return spec.coverage_mode == "session_dense" or spec.session_rows


def seeds(config, end: date, *, start: date = date(1990, 1, 1)) -> list[Seed]:
    """What ``meta/state`` and the run manifest say was asked and found empty, up to *end*.

    A dataset that promises a row every session gets nothing: an empty
    session there is a hole the old ledgers missed, not an answer. Cells
    split by entity (``negative_evidence``, ``outstanding_keys``) are not
    read yet.
    """
    calendar, state = LakeCalendar(config), StateStore(config.meta_root)
    refreshed = refreshes(config.manifest_path)
    found: list[Seed] = []
    now = datetime.now(UTC)
    with Catalog(lake_root(config), read_only=True) as catalog:
        written = last_written(catalog)
        for name, spec in DATASETS.items():
            coverage = coverage_of(spec, end, config)
            if coverage.by == "write":
                asked = refreshed.get(name)
                if asked is not None and asked > written.get(
                    name, datetime.min.replace(tzinfo=UTC)
                ):
                    period = cov.label(coverage.grain, asked.date())
                    found.append(Seed(name, "refresh", (period,), asked))
                continue
            if coverage.grain not in ("session", "day", "week") or _dense(spec):
                continue
            cells = plan_dataset(
                catalog, name, coverage, Scope(start, end), calendar=calendar
            ).cells
            covered = cells.filter(pl.col("state") == "covered")["period"]
            mark = freshness_mark(config, name, state.get_date(name))
            snapshot = state.get_date(name, field="last_snapshot_date")
            mark = max((day for day in (mark, snapshot) if day is not None), default=None)
            periods = settled_empty(
                cells,
                mark=None if mark is None else cov.label(coverage.grain, min(mark, end)),
                first=covered.min() if len(covered) else None,
                owed={cov.label(coverage.grain, day) for day in state.get_missing_dates(name)},
                long_run=LONG_RUN[coverage.grain],
            )
            if periods:
                found.append(Seed(name, "state", tuple(periods), now))
    return found


def record_seeds(config, found: Iterable[Seed], *, run: str = "seed:meta-state") -> int | None:
    """Write *found* to the lake's ``_attempts`` in one snapshot, under the lake lock."""
    rows = [
        {
            "dataset": seed.dataset,
            "period": period,
            "entity": "",
            "run": f"{run}:{seed.reason}",
            "asked_at": seed.asked_at,
        }
        for seed in found
        for period in seed.periods
    ]
    if not rows:
        return None
    with lake_mutation_lock(config.meta_root, blocking=True):
        with Catalog(lake_root(config)) as catalog:
            return cov.record(catalog, pl.DataFrame(rows), message=run)
