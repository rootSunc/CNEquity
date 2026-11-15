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
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta

import polars as pl

from cnequity.domain.datasets import (
    DATASETS,
    DatasetSpec,
    history_mode_for,
    is_dataset_enabled,
    removed_from_schedule,
)
from cnequity.domain.symbols import filter_ingest_universe, is_subscription_placeholder
from cnequity.domain.trading_status import independent_suspension_keys
from cnequity.file_lock import lake_mutation_lock
from cnequity.quality.freshness import freshness_owed
from cnequity.query.calendar import list_trading_dates
from cnequity.sqlite_readonly import connect_readonly
from cnequity.storage.catalog_bridge import lake_root, revision_snapshots
from cnequity.storage.fund_no_print import approved_no_print_keys
from cnequity.storage.instrument_catalog import (
    listed_fund_symbols,
    load_curated_instruments,
    load_curated_trading_status,
)
from cnequity.storage.observation import freshness_mark
from cnequity.storage.state import StateStore
from equitylake_core import coverage as cov
from equitylake_core.catalog import ALIAS, Catalog, quote_literal
from equitylake_core.coverage import Coverage
from equitylake_core.plan import Declared, Scope, plan_all, plan_dataset


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
    shared = {
        "start": start,
        "end": end,
        "entity": spec.cells_by,
        "inputs": spec.derived_from,
        "counts": spec.cells_count,
    }
    if spec.cells is not None:
        return Coverage(spec.cells, column=spec.query_date_col, **shared)
    by_date = spec.watermark or (spec.layer == "derived" and spec.query_date_col == "trade_date")
    if spec.partition_col is not None and by_date:
        grain = "day" if spec.session_scope == "calendar" else "session"
        return Coverage(grain, column=spec.query_date_col, **shared)
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


class BarUniverse:
    """The securities owed a daily bar, and the days they are excused one.

    The same scope ``quality.bar_coverage`` checks: listed securities in the
    configured ingest universe (and listed funds when they are ingested),
    from the listing date up to the day before delisting. A session is not
    owed a bar when the security was independently shown suspended or a fund
    was verified not to have printed.
    """

    def __init__(self, config):
        self._config = config
        self._members: pl.DataFrame | None = None

    def members(self, dataset: str, start: date, end: date) -> pl.DataFrame:
        if dataset != "daily_bars":
            raise ValueError(f"no universe is declared for {dataset}")
        if self._members is None:
            self._members = self._load(end)
        return self._members

    def _load(self, end: date) -> pl.DataFrame:
        schema = {"entity": pl.String, "first": pl.Date, "last": pl.Date}
        instruments = load_curated_instruments(self._config)
        if instruments is None or instruments.is_empty():
            return pl.DataFrame(schema=schema)
        instruments = instruments.filter(
            pl.Series(
                [
                    not is_subscription_placeholder(row.get("name"), row.get("symbol"))
                    for row in instruments.select("name", "symbol").iter_rows(named=True)
                ]
            )
        )
        scope = set(
            filter_ingest_universe(instruments["symbol"].to_list(), self._config.ingest_universe)
        )
        if self._config.ingest_listed_funds:
            scope |= set(listed_fund_symbols(instruments, end, start=date(1990, 1, 1)))
        return (
            instruments.filter(
                pl.col("symbol").is_in(sorted(scope)) & pl.col("list_date").is_not_null()
            )
            .select(
                pl.col("symbol").alias("entity"),
                pl.col("list_date").alias("first"),
                (pl.col("delist_date") - timedelta(days=1)).alias("last"),
            )
            .cast(schema)
            .unique()
        )

    def excused(self, dataset: str, start: date, end: date) -> pl.DataFrame:
        keys = {"symbol": pl.String, "trade_date": pl.Date}
        status = load_curated_trading_status(self._config, start=start, end=end)
        halted = (
            independent_suspension_keys(status)
            if status is not None and not status.is_empty()
            else pl.DataFrame(schema=keys)
        )
        quiet = approved_no_print_keys(self._config, start=start, end=end)
        if quiet:
            halted = pl.concat(
                [halted.select(*keys), pl.DataFrame(sorted(quiet), schema=keys, orient="row")]
            )
        return halted.select(
            pl.col("trade_date").cast(pl.String).alias("period"),
            pl.col("symbol").alias("entity"),
        ).unique()


def declared(config, end: date) -> dict[str, Declared]:
    """Every dataset's cells as the planner takes them."""
    return {
        name: Declared(coverage_of(spec, end, config), spec.query_date_col)
        for name, spec in DATASETS.items()
    }


def planned(config, catalog: Catalog, scope: Scope, wanted: Iterable[str] | None = None):
    """The plan of *wanted* (default: every dataset) over *scope*, inputs planned first."""
    return plan_all(
        catalog,
        declared(config, scope.end),
        scope,
        wanted=None if wanted is None else list(wanted),
        calendar=LakeCalendar(config),
        universe=BarUniverse(config),
        written=last_written(catalog),
    )


@dataclass(frozen=True)
class Comparison:
    dataset: str
    basis: str
    covered: int
    #: Cells waiting for an input of a derived dataset.
    blocked: int
    #: Periods with a gap; ``period:entity`` for a dataset owed per entity.
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
    state = StateStore(config.meta_root)
    scope = Scope(start, end)
    found = []
    with Catalog(lake_root(config), read_only=True) as catalog:
        plans = planned(config, catalog, scope, datasets)
        for name, item in plans.items():
            coverage = coverage_of(DATASETS[name], end, config)
            cells = item.cells
            gap_cells = cells.filter(pl.col("state") == "gap")
            if coverage.entity is not None:
                states = {}
                gaps = tuple(
                    f"{p}:{e}" for p, e in gap_cells.select("period", "entity").iter_rows()
                )
            else:
                states = dict(cells.select("period", "state").iter_rows())
                gaps = tuple(gap_cells["period"].to_list())
            mark = freshness_mark(config, name, state.get_date(name))
            snapshot = state.get_date(name, field="last_snapshot_date")
            mark = max((day for day in (mark, snapshot) if day is not None), default=None)
            owed = tuple(
                sorted(day for day in state.get_missing_dates(name) if start <= day <= end)
            )
            by_day = coverage.grain in ("session", "day") and coverage.entity is None
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
                    covered=int((cells["state"] == "covered").sum()),
                    blocked=int((cells["state"] == "blocked").sum()),
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


def refreshes(manifest_path, *, run_id: str | None = None) -> dict[str, datetime]:
    """When each dataset was last fetched without failing, from the run manifest.

    A refresh that found nothing new publishes nothing, so the manifest is the
    only place it shows. With *run_id*, only that run's fetches count.
    """
    try:
        connection = connect_readonly(manifest_path)
    except Exception:  # no manifest yet: no runs to tell of
        return {}
    try:
        rows = connection.execute(
            "SELECT d.dataset, max(COALESCE(r.finished_at, r.started_at)) "
            "FROM dataset_results d JOIN ingestion_runs r USING (run_id) "
            "WHERE d.stage = 'fetch' AND d.status IN ('success', 'warning') "
            + ("AND d.run_id = ? " if run_id else "")
            + "GROUP BY d.dataset",
            [run_id] if run_id else [],
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
    """What the old ledgers say was asked of one dataset's source and how it answered."""

    dataset: str
    #: ``state``: cells the state files hold done that have no rows.
    #: ``refresh``: a refresh the manifest records that published nothing new.
    #: ``short``: days the missing-date ledger owes that hold rows anyway.
    reason: str
    periods: tuple[str, ...]
    asked_at: datetime
    outcome: str = "empty"


#: A dataset with rows in this share of the cells of its stored span is dense in
#: practice: a cell without rows there is more likely missed than empty.
DENSE_SHARE = 0.95
#: This many empty cells in a row, in a feed that has rows around them, is a
#: stretch that was never fetched rather than a run of empty answers.
LONG_RUN = {"session": 20, "day": 20, "week": 4}


def dense_in_practice(cells: pl.DataFrame) -> bool:
    """Whether rows fill ``DENSE_SHARE`` of the cells from the first stored one to the last.

    Judge it on a dataset's whole history: over a few weeks a feed that lost
    a week to an outage looks sparse, and its lost days would read as empty.
    """
    covered = cells.filter(pl.col("state") == "covered")["period"]
    if covered.is_empty():
        return False
    span = cells.filter(pl.col("period").is_between(pl.lit(covered.min()), pl.lit(covered.max())))
    return (span["state"] == "covered").mean() >= DENSE_SHARE


def settled_empty(
    cells: pl.DataFrame,
    *,
    mark: str | None,
    first: str | None,
    owed: set[str],
    long_run: int,
    dense: bool = False,
) -> list[str]:
    """The gaps the state files hold done and that read as answers rather than misses.

    From the first covered cell through *mark*, not owed by the missing-date
    ledger. Left a gap, because a gap costs one more request and a wrong
    empty hides a hole for good:

    * anything before the first stored row, where the old ledgers never looked;
    * every gap of a *dense* dataset (see :func:`dense_in_practice`);
    * a run of *long_run* or more consecutive gaps.
    """
    if mark is None or first is None or dense:
        return []
    span = cells.filter((pl.col("period") >= first) & (pl.col("period") <= mark))
    if span.is_empty():
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


def _answers(
    catalog: Catalog,
    config,
    name: str,
    coverage: Coverage,
    cells: pl.DataFrame,
    *,
    state: StateStore,
    end: date,
    now: datetime,
    asked: datetime | None,
    written: datetime | None,
    history: pl.DataFrame | None = None,
) -> list[Seed]:
    """What the old ledgers say about *name*'s *cells* that ``_attempts`` does not hold yet.

    *history* is the dataset's plan over all its time, when *cells* covers
    only part of it; density is judged on that.
    """
    attempts = cov.attempted(catalog, name)
    if coverage.by == "write":
        known = [written] + attempts["at"].to_list()
        latest = max((moment for moment in known if moment is not None), default=None)
        if asked is not None and (latest is None or asked > latest):
            return [Seed(name, "refresh", (cov.label(coverage.grain, asked.date()),), asked)]
        return []
    if coverage.grain not in ("session", "day", "week") or cells.is_empty():
        return []
    found = []
    owed = {cov.label(coverage.grain, day) for day in state.get_missing_dates(name)}
    covered = set(cells.filter(pl.col("state") == "covered")["period"].to_list())
    short = sorted(owed & covered)
    if short:
        found.append(Seed(name, "short", tuple(short), now, "partial"))
    pending = set(cov.partial(attempts)["period"].to_list()) & set(cells["period"].to_list())
    if pending:
        first = cov.span(coverage.grain, min(pending))[0]
        last = cov.span(coverage.grain, max(pending))[1]
        rowed = set(
            cov.stored(
                catalog,
                name,
                coverage,
                column=DATASETS[name].query_date_col,
                start=first,
                end=last,
            )["period"].to_list()
        )
        whole = sorted((pending & rowed) - owed)
        if whole:
            found.append(Seed(name, "whole", tuple(whole), now, "complete"))
    if _dense(DATASETS[name]):
        return found
    mark = freshness_mark(config, name, state.get_date(name))
    snapshot = state.get_date(name, field="last_snapshot_date")
    mark = max((day for day in (mark, snapshot) if day is not None), default=None)
    periods = settled_empty(
        cells,
        mark=None if mark is None else cov.label(coverage.grain, min(mark, end)),
        first=min(covered) if covered else None,
        owed=owed,
        long_run=LONG_RUN[coverage.grain],
        dense=dense_in_practice(cells if history is None else history),
    )
    if periods:
        found.append(Seed(name, "state", tuple(periods), now))
    return found


#: Earlier than any market data this lake holds: a scope from here is a dataset's whole history.
HISTORY_START = date(1990, 1, 1)


def seeds(config, end: date, *, start: date = HISTORY_START) -> list[Seed]:
    """What ``meta/state`` and the run manifest say was asked and how it was answered, to *end*.

    A dataset that promises a row every session gets no empty answers: an
    empty session there is a hole the old ledgers missed, not an answer. A
    day the missing-date ledger owes but that holds rows was answered in
    part: it is recorded so, and stays owed; once the ledger no longer owes
    it, it is recorded whole. Cells owed per entity are left to the planner:
    their rows say which entities are missing. Running it again adds only
    what changed.
    """
    return _collect(config, Scope(start, end), DATASETS, refreshes(config.manifest_path))


def _collect(config, scope: Scope, names: Iterable[str], asked: dict[str, datetime]) -> list[Seed]:
    state = StateStore(config.meta_root)
    now = datetime.now(UTC)
    found: list[Seed] = []
    wanted = [
        name for name in names if coverage_of(DATASETS[name], scope.end, config).entity is None
    ]
    with Catalog(lake_root(config), read_only=True) as catalog:
        written = last_written(catalog)
        plans = planned(config, catalog, scope, wanted)
        whole = scope.start <= HISTORY_START
        calendar = LakeCalendar(config)
        for name in wanted:
            coverage = coverage_of(DATASETS[name], scope.end, config)
            history = None
            if not whole and coverage.by == "rows":
                # Its inputs do not change which cells hold rows: plan it alone.
                history = plan_dataset(
                    catalog,
                    name,
                    replace(coverage, inputs=()),
                    Scope(HISTORY_START, scope.end),
                    calendar=calendar,
                    column=DATASETS[name].query_date_col,
                ).cells
            found.extend(
                _answers(
                    catalog,
                    config,
                    name,
                    coverage_of(DATASETS[name], scope.end, config),
                    plans[name].cells,
                    state=state,
                    end=scope.end,
                    now=now,
                    asked=asked.get(name),
                    written=written.get(name),
                    history=history,
                )
            )
    return found


#: How far back a run's answers are read off the old ledgers after it publishes.
SYNC_DAYS = 45


def sync_attempts(config, run_id: str, trade_date: date, datasets: Iterable[str]) -> dict:
    """Bring ``_attempts`` up to what the old ledgers say after run *run_id* settled.

    While the daily run still decides from ``meta/state``, this keeps the
    planner's ledger beside it: the same rules as :func:`seeds`, over the
    last ``SYNC_DAYS`` (and a backfill's whole window), for the datasets the
    run touched. Returns what it recorded and, per dataset, the gaps the
    planner sees that the state files hold done.
    """
    from cnequity.run_scope import backfill_of

    window = backfill_of(config)
    start = trade_date - timedelta(days=SYNC_DAYS)
    if window.active and window.start is not None:
        start = min(start, window.start)
    end = max(trade_date, window.end) if window.active and window.end else trade_date
    asked = refreshes(config.manifest_path, run_id=run_id)
    names = sorted((set(datasets) | set(asked)) & set(DATASETS))
    scope = Scope(start, end)
    found = _collect(config, scope, names, asked)
    snapshot = record_seeds(config, found, run=f"run:{run_id}") if found else None
    state = StateStore(config.meta_root)
    disagree: dict[str, list[str]] = {}
    with Catalog(lake_root(config), read_only=True) as catalog:
        plans = planned(config, catalog, scope, [n for n in names if _by_day(config, n, end)])
        for name, item in plans.items():
            mark = freshness_mark(config, name, state.get_date(name))
            owed = {day.isoformat() for day in state.get_missing_dates(name)}
            gaps = [
                period
                for period in item.cells.filter(pl.col("state") == "gap")["period"].to_list()
                if mark is not None and period <= mark.isoformat() and period not in owed
            ]
            if gaps:
                disagree[name] = gaps
    return {
        "snapshot": snapshot,
        "recorded": sum(len(seed.periods) for seed in found),
        "disagreements": disagree,
    }


def _by_day(config, name: str, end: date) -> bool:
    coverage = coverage_of(DATASETS[name], end, config)
    return (
        coverage.by == "rows" and coverage.entity is None and coverage.grain in ("session", "day")
    )


def record_seeds(config, found: Iterable[Seed], *, run: str = "seed:meta-state") -> int | None:
    """Write *found* to the lake's ``_attempts`` in one snapshot, under the lake lock."""
    rows = [
        {
            "dataset": seed.dataset,
            "period": period,
            "entity": "",
            "run": f"{run}:{seed.reason}",
            "asked_at": seed.asked_at,
            "outcome": seed.outcome,
        }
        for seed in found
        for period in seed.periods
    ]
    if not rows:
        return None
    with lake_mutation_lock(config.meta_root, blocking=True):
        with Catalog(lake_root(config)) as catalog:
            return cov.record(catalog, pl.DataFrame(rows), message=run)
