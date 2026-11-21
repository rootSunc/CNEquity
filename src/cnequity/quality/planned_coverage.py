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

import tempfile
from bisect import bisect_left, bisect_right
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import polars as pl

from cnequity.domain.datasets import (
    DATASETS,
    DatasetSpec,
    history_mode_for,
)
from cnequity.domain.market_time import SHANGHAI_TZ
from cnequity.domain.symbols import filter_ingest_universe, is_subscription_placeholder
from cnequity.domain.trading_status import independent_suspension_keys
from cnequity.file_lock import lake_mutation_lock
from cnequity.meta_layout import LAKE_DIR
from cnequity.quality.freshness import freshness_mark
from cnequity.query.calendar import list_trading_dates
from cnequity.sqlite_readonly import connect_readonly
from cnequity.storage.catalog_bridge import lake_root, revision_snapshots
from cnequity.storage.fund_no_print import approved_no_print_keys, quiet_fund_keys
from cnequity.storage.instrument_catalog import (
    listed_fund_symbols,
    load_curated_instruments,
    load_curated_trading_status,
)
from cnequity.storage.state import StateStore
from equitylake_core import coverage as cov
from equitylake_core.catalog import ALIAS, CATALOG_FILE, Catalog, quote_literal
from equitylake_core.coverage import Coverage
from equitylake_core.plan import Declared, Scope, plan_all, plan_dataset

_EMPTY_LAKE: Path | None = None


@contextmanager
def ledger(config) -> Iterator[Catalog]:
    """The lake's catalog, read-only; an empty lake's for a lake that has none yet.

    A lake writes its catalog with its first publish. Until then nothing is
    stored and nothing was asked: every cell is owed, which is what planning
    against an empty catalog says.
    """
    global _EMPTY_LAKE
    root = lake_root(config)
    if not (root / CATALOG_FILE).exists():
        if _EMPTY_LAKE is None or not (_EMPTY_LAKE / CATALOG_FILE).exists():
            made = Path(tempfile.mkdtemp(prefix="cnequity-empty-lake-")) / LAKE_DIR
            Catalog(made, create=True).close()
            _EMPTY_LAKE = made
        root = _EMPTY_LAKE
    with Catalog(root, read_only=True) as catalog:
        yield catalog


def coverage_of(spec: DatasetSpec, today: date, config=None) -> Coverage:
    """The cells *spec*'s dataset owes, from the facts its spec already states.

    A dataset walked by date (it keeps a watermark, or is derived day by day)
    owes a cell per session, or per calendar day for a feed that publishes on
    weekends. One keyed by reporting period owes a cell per quarter. Anything
    else is refreshed as a whole and owes its latest session; so does a live
    snapshot no source can replay, since a day it missed cannot be had any
    more. What the source serves bounds all of them, and so does where this
    lake's history begins and ends (``[datasets.history_from]``,
    ``history_until``). A spec that names its
    ``cells`` overrides the rule.
    """
    start, end = spec.history_bounds(today, config)
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
        "settle": timedelta(days=spec.cells_settle_days),
        "open_day": spec.cells_open_day,
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
    owed a bar when the security was independently shown suspended, or a fund
    was verified not to have printed or holds a zero-volume row for it.
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
                (pl.col("delist_date").cast(pl.Date) - timedelta(days=1)).alias("last"),
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
        quiet = approved_no_print_keys(self._config, start=start, end=end) | quiet_fund_keys(
            self._config, start=start, end=end
        )
        if quiet:
            halted = pl.concat(
                [halted.select(*keys), pl.DataFrame(sorted(quiet), schema=keys, orient="row")]
            )
        return halted.select(
            pl.col("trade_date").cast(pl.String).alias("period"),
            pl.col("symbol").alias("entity"),
        ).unique()


#: The market's time zone: a day, and whether an answer came after it, ends at its midnight.
MARKET_ZONE = SHANGHAI_TZ


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
        zone=MARKET_ZONE,
    )


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
            "WHERE d.stage = 'fetch' AND d.status IN ('success', 'degraded') "
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
            day = asked.astimezone(MARKET_ZONE).date()
            return [Seed(name, "refresh", (cov.label(coverage.grain, day),), asked)]
        return []
    if coverage.grain not in ("session", "day", "week") or cells.is_empty():
        return []
    found = []
    owed = {cov.label(coverage.grain, day) for day in state.get_missing_dates(name)}
    covered = set(cells.filter(pl.col("state") == "covered")["period"].to_list())
    short = sorted(owed & covered)
    if short:
        found.append(Seed(name, "short", tuple(short), now, "partial"))
    # Any part-answered cell, not only those in *cells*: a day that left the
    # sync window while still owed would otherwise stay partial forever.
    pending = set(cov.partial(attempts)["period"].to_list())
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


def _range_days(row: dict) -> list[date]:
    first = date.fromisoformat(str(row["start"]))
    last = date.fromisoformat(str(row.get("end") or row["start"]))
    return [first + timedelta(days=n) for n in range((last - first).days + 1)]


def entity_seeds(config, end: date) -> dict[str, list[tuple[str, str]]]:
    """The (period, entity) cells the old ledgers hold answered, per dataset owed per entity.

    ``meta/state`` asks a security's session again only while it is an
    outstanding key or its day a missing range; every other session without
    a bar (a halt no board reported, a fund that never prints) it counts as
    answered. Those are the cells returned here, once past the dataset's
    settle, so the planner does not owe them; running it again finds none.
    """
    state = StateStore(config.meta_root)
    found: dict[str, list[tuple[str, str]]] = {}
    names = [
        name for name, spec in DATASETS.items() if coverage_of(spec, end, config).entity is not None
    ]
    if not names:
        return found
    with ledger(config) as catalog:
        plans = planned(config, catalog, Scope(HISTORY_START, end), names)
    for name in names:
        coverage = coverage_of(DATASETS[name], end, config)
        settled = (end - coverage.settle - timedelta(days=1)).isoformat()
        owed_days = {
            day.isoformat()
            for row in state.get_payload(name).get("missing_ranges", [])
            for day in _range_days(row)
        }
        owed_keys = {
            (str(row["trade_date"]), str(row["symbol"])) for row in state.get_outstanding_keys(name)
        }
        gaps = plans[name].cells.filter((pl.col("state") == "gap") & (pl.col("period") <= settled))
        cells = [
            (period, entity)
            for period, entity in gaps.select("period", "entity").iter_rows()
            if period not in owed_days and (period, entity) not in owed_keys
        ]
        if cells:
            found[name] = cells
    return found


def _collect(config, scope: Scope, names: Iterable[str], asked: dict[str, datetime]) -> list[Seed]:
    state = StateStore(config.meta_root)
    now = datetime.now(UTC)
    found: list[Seed] = []
    wanted = [
        name for name in names if coverage_of(DATASETS[name], scope.end, config).entity is None
    ]
    with ledger(config) as catalog:
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
                    zone=MARKET_ZONE,
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


def record_refreshes(config, run_id: str) -> int | None:
    """Record the whole-dataset refreshes run *run_id* made, from the run records.

    A dataset refreshed as a whole (``by="write"``) is covered by a refresh
    that published or found nothing new; the run manifest is where that is
    said. Returns the ledger snapshot, or ``None`` when there was nothing new.
    """
    asked = refreshes(config.manifest_path, run_id=run_id)
    found: list[Seed] = []
    with ledger(config) as catalog:
        written = last_written(catalog)
        for name, at in sorted(asked.items()):
            if name not in DATASETS or not refreshed(name):
                continue
            coverage = coverage_of(DATASETS[name], at.date(), config)
            known = [written.get(name), *cov.attempted(catalog, name)["at"].to_list()]
            latest = max((moment for moment in known if moment is not None), default=None)
            if latest is None or at > latest:
                day = at.astimezone(MARKET_ZONE).date()
                found.append(Seed(name, "refresh", (cov.label(coverage.grain, day),), at))
    return record_seeds(config, found, run=f"run:{run_id}") if found else None


def _by_day(config, name: str, end: date) -> bool:
    coverage = coverage_of(DATASETS[name], end, config)
    return (
        coverage.by == "rows" and coverage.entity is None and coverage.grain in ("session", "day")
    )


#: The finding a fetch step reports its answers in; compact records them.
ANSWER_CHECK = "planner_empty_answer"


def refreshed(dataset: str) -> bool:
    """Whether *dataset* is owed by write (refreshed whole), not row by row."""
    return coverage_of(DATASETS[dataset], date.today()).by == "write"


def ledger_view(config, dataset: str) -> dict:
    """*dataset*'s owed-work fields, in the shape gap reports read.

    A day held only in part is its ``partial`` answer in ``_attempts``,
    reported here as a ``missing_ranges`` row (``source_scope_incomplete``);
    keys a fetch could not settle are gaps the planner asks again, not a list.
    """
    short = [day.isoformat() for day in partial_days(config, dataset)]
    return {
        "missing_ranges": [
            {"start": period, "end": period, "last_failure": "source_scope_incomplete"}
            for period in sorted(short)
        ],
        "missing_units": {},
        "outstanding_keys": [],
    }


def partial_days(config, dataset: str) -> set[date]:
    """The days *dataset*'s ledger holds as answered only in part: a clean answer settles them."""
    with ledger(config) as catalog:
        short = cov.partial(cov.attempted(catalog, dataset))["period"].to_list()
    return {date.fromisoformat(period) for period in short if len(period) == 10}


def clean_answer(config, dataset: str, days, asked_at: datetime) -> dict | None:
    """The ``complete`` answer for the *days* fetched cleanly that the ledger held partial."""
    from cnequity.orchestrator.source_gaps import source_gap_dates

    settled = (set(days) & partial_days(config, dataset)) - source_gap_dates(dataset)
    return answer_finding(dataset, settled, asked_at, "complete") if settled else None


def gap_days(config, dataset: str, trade_date: date) -> list[date]:
    """The days *dataset* owes and does not have, over the last ``SYNC_DAYS``.

    For a dataset owed once per session or calendar day and not per entity;
    others are planned elsewhere and give none here.
    """
    coverage = coverage_of(DATASETS[dataset], trade_date, config)
    if not (
        coverage.by == "rows" and coverage.entity is None and coverage.grain in ("session", "day")
    ):
        return []
    scope = Scope(trade_date - timedelta(days=SYNC_DAYS), trade_date)
    with ledger(config) as catalog:
        cells = planned(config, catalog, scope, [dataset])[dataset].cells
    return sorted(
        date.fromisoformat(period)
        for period in cells.filter(pl.col("state") == "gap")["period"].to_list()
    )


def gap_keys(config, dataset: str, trade_date: date) -> list[tuple[str, date]]:
    """The (entity, day) cells *dataset* owes per entity, over the last ``SYNC_DAYS``.

    For a dataset owed a cell per security and session (``daily_bars``);
    others give none here.
    """
    coverage = coverage_of(DATASETS[dataset], trade_date, config)
    if not (coverage.by == "rows" and coverage.entity is not None and coverage.grain == "session"):
        return []
    scope = Scope(trade_date - timedelta(days=SYNC_DAYS), trade_date)
    with ledger(config) as catalog:
        cells = planned(config, catalog, scope, [dataset])[dataset].cells
    gaps = cells.filter(pl.col("state") == "gap")
    return sorted(
        (entity, date.fromisoformat(period))
        for period, entity in gaps.select("period", "entity").iter_rows()
    )


def owed_cells(config, dataset: str, start: date, end: date) -> list[tuple[str, date]]:
    """The (entity, day) cells *dataset* owes per entity in [*start*, *end*]."""
    with ledger(config) as catalog:
        cells = planned(config, catalog, Scope(start, end), [dataset])[dataset].cells
    gaps = cells.filter(pl.col("state") == "gap")
    return sorted(
        (entity, date.fromisoformat(period))
        for period, entity in gaps.select("period", "entity").iter_rows()
    )


def last_answered(config, dataset: str) -> date | None:
    """The latest day the source answered *dataset* cleanly: empty, or with every row.

    Only cells kept per day (not per entity) count: a day a fetch walked and
    the sweeps it ran (``:recheck:``), and refreshes of a whole dataset.
    """
    with ledger(config) as catalog:
        rows = cov.attempted(catalog, dataset)
    days = rows.filter(
        (pl.col("entity") == "")
        & pl.col("outcome").is_in(["empty", "complete"])
        & pl.col("period").str.contains(r"^\d{4}-\d{2}-\d{2}$")
    )["period"]
    return None if days.is_empty() else date.fromisoformat(days.max())


def unanswered_keys(config, dataset: str, start: date, end: date) -> pl.DataFrame:
    """The (symbol, trade_date) cells *dataset* owes in [*start*, *end*] that no fetch answered.

    A cell the source answered empty is a fact about the market (nothing
    printed), even while it is still asked again until it settles; one left
    without any answer is a fetch that failed, and says nothing.
    """
    keys = {"symbol": pl.String, "trade_date": pl.Date}
    with ledger(config) as catalog:
        cells = planned(config, catalog, Scope(start, end), [dataset])[dataset].cells
        answered = cov.attempted(catalog, dataset).filter(pl.col("outcome") == "empty")
    gaps = cells.filter(pl.col("state") == "gap").select("period", "entity")
    return (
        gaps.join(answered.select("period", "entity"), on=["period", "entity"], how="anti")
        .select(
            pl.col("entity").alias("symbol"), pl.col("period").str.to_date().alias("trade_date")
        )
        .cast(keys)
    )


def settled_days(config, dataset: str, start: date, end: date, *, redo: str = "gaps") -> set[date]:
    """The days in [*start*, *end*] *dataset* does not owe: covered, answered empty or not due.

    What a planner-only backfill skips; everything else is asked. *redo* as
    in :class:`Scope`: ``empty`` asks recorded-empty days again, ``all`` every day.
    """
    with ledger(config) as catalog:
        cells = planned(config, catalog, Scope(start, end, redo=redo), [dataset])[dataset].cells
    # A blocked day (its inputs are not in yet) is not settled: the backfill asks.
    settled = cells.filter(pl.col("state").is_in(["covered", "empty", "not_due"]))
    return {date.fromisoformat(period) for period in settled["period"].unique().to_list()}


def answer_finding(
    dataset: str, days: Iterable[date], asked_at: datetime, outcome: str = "empty"
) -> dict:
    """What a fetch step says it asked about and how it was answered, for compact to record.

    *outcome* as in ``_attempts``: ``empty`` (nothing to store), ``partial``
    (rows, but the day is incomplete: it stays owed) or ``complete`` (a clean
    answer, which settles a day an earlier answer left partial).
    """
    periods = sorted({day.isoformat() for day in days})
    return {
        "dataset": dataset,
        "check": ANSWER_CHECK,
        "severity": "info",
        "message": f"{len(periods)} day(s) asked about came back {outcome}",
        "periods": periods,
        "outcome": outcome,
        "asked_at": asked_at.isoformat(),
    }


#: The finding a fetch step reports a clean sweep of its reconcile window in.
RECHECK_CHECK = "planner_recheck"


def recheck_finding(dataset: str, days: Iterable[date], depth: str, asked_at: datetime) -> dict:
    """What a fetch step re-asked and got rows for, sweeping its *depth* window cleanly.

    Recorded as ``complete`` answers, so when the window was last swept at
    each depth is read off ``_attempts`` rather than kept in ``meta/state``.
    """
    periods = sorted({day.isoformat() for day in days})
    return {
        "dataset": dataset,
        "check": RECHECK_CHECK,
        "severity": "info",
        "message": f"{len(periods)} day(s) re-asked in a clean {depth} sweep",
        "periods": periods,
        "depth": depth,
        "asked_at": asked_at.isoformat(),
    }


def last_recheck(config, dataset: str, depth: str) -> date | None:
    """The run day of *dataset*'s last clean *depth* sweep, from ``_attempts``.

    A sweep walks its window up to the day it runs for, so that day is the
    latest one it recorded; a catch-up run for an earlier day dates its sweep
    by that day, not by when it ran.
    """
    with ledger(config) as catalog:
        if not catalog.has_table(cov.ATTEMPTS):
            return None
        rows = catalog.records(cov.ATTEMPTS, "dataset, run, period").filter(
            (pl.col("dataset") == dataset) & pl.col("run").str.ends_with(f":recheck:{depth}")
        )
    if rows.is_empty():
        return None
    return date.fromisoformat(rows["period"].max())


def seed_rechecks(config, datasets: Iterable[str]) -> int:
    """Carry ``meta/state``'s last deep and tail sweep days into ``_attempts``, once.

    Part of the ``attempts-ledger`` migration: without it the first run would
    sweep deep (``daily_bars``: half an hour) only because nothing is on
    record. A dataset already holding a recheck is left alone. Returns the
    rows recorded.
    """
    state = StateStore(config.meta_root)
    rows = []
    for name in datasets:
        for depth in ("deep", "tail"):
            day = state.get_date(name, field=f"{depth}_reconciled_on")
            if day is None or last_recheck(config, name, depth) is not None:
                continue
            asked = datetime.combine(day, time(12), MARKET_ZONE).astimezone(UTC)
            rows += _answer_rows("seed", recheck_finding(name, [day], depth, asked))
    if not rows:
        return 0
    with lake_mutation_lock(config.meta_root, blocking=True):
        with Catalog(lake_root(config)) as catalog:
            cov.record(catalog, pl.DataFrame(rows), message="seed:rechecks")
    return len(rows)


def _answer_rows(run_id: str, item: dict) -> list[dict]:
    if item.get("dataset") not in DATASETS:
        return []
    if item.get("check") == ANSWER_CHECK:
        run, outcome = f"run:{run_id}:step", item.get("outcome", "empty")
    elif item.get("check") == RECHECK_CHECK:
        run, outcome = f"run:{run_id}:recheck:{item.get('depth')}", "complete"
    else:
        return []
    asked = datetime.fromisoformat(item["asked_at"])
    return [
        {
            "dataset": item["dataset"],
            "period": period,
            "entity": "",
            "run": run,
            "asked_at": asked,
            "outcome": outcome,
        }
        for period in item.get("periods") or ()
    ]


def attach_answers(write, run_id: str, findings: Iterable[dict]) -> int:
    """Add the fetch findings that are not on record to this publication."""
    rows = [row for item in findings for row in _answer_rows(run_id, item)]
    if not rows:
        return 0
    catalog = write.catalog
    frame = pl.DataFrame(rows).unique().cast({"asked_at": pl.Datetime("us", "UTC")})
    cov.prepare(catalog)
    keys = ["dataset", "period", "run", "asked_at"]
    seen = catalog.records(cov.ATTEMPTS, ", ".join(keys)).cast(
        {"asked_at": pl.Datetime("us", "UTC")}
    )
    frame = frame.join(seen, on=keys, how="anti")
    write.record_frame(cov.ATTEMPTS, frame)
    return frame.height


def record_answers(config, run_id: str, findings: Iterable[dict]) -> int:
    """Record fetch answers when there is no data publication to include them in."""
    from equitylake_core.write import Write

    findings = list(findings)
    if not any(item.get("check") in {ANSWER_CHECK, RECHECK_CHECK} for item in findings):
        return 0
    with lake_mutation_lock(config.meta_root), Catalog(lake_root(config)) as catalog:
        write = Write(catalog)
        count = attach_answers(write, run_id, findings)
        if count:
            write.commit(message=f"run:{run_id}:answers")
    return count


def record_cell_answers(
    config, dataset: str, cells: Iterable[tuple[str, str]], *, run: str, outcome: str = "empty"
) -> int:
    """Record (period, entity) answers for a dataset owed per entity; returns the rows.

    A fetch that asked a symbol for a session and got nothing back records it
    here, so the planner tells it apart from a fetch that failed (still a gap).
    """
    asked = datetime.now(UTC)
    rows = [
        {
            "dataset": dataset,
            "period": period,
            "entity": entity,
            "run": run,
            "asked_at": asked,
            "outcome": outcome,
        }
        for period, entity in sorted(set(cells))
    ]
    if not rows:
        return 0
    with lake_mutation_lock(config.meta_root, blocking=True):
        with Catalog(lake_root(config)) as catalog:
            cov.record(catalog, pl.DataFrame(rows), message=f"{run}:answers")
    return len(rows)


def answered_cells(config, dataset: str, *, outcome: str = "empty") -> set[tuple[str, str]]:
    """The (period, entity) cells of *dataset* whose latest answer is *outcome*."""
    with ledger(config) as catalog:
        latest = cov.attempted(catalog, dataset)
    return set(latest.filter(pl.col("outcome") == outcome).select("period", "entity").iter_rows())


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
