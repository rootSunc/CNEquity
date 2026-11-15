"""What the planner would say about this lake, beside what the state files say.

Read-only. The daily run still decides what to fetch from watermarks, the
missing-date ledger and ``observed_through`` (``meta/state``). This module
derives each dataset's cells from what its spec already declares, asks the
core planner which of them are gaps, and sets the answer beside that state,
so the two can be compared on a real lake before the planner drives anything.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date

from cnequity.domain.datasets import (
    DATASETS,
    DatasetSpec,
    history_mode_for,
    is_dataset_enabled,
    removed_from_schedule,
)
from cnequity.quality.freshness import freshness_owed
from cnequity.query.calendar import list_trading_dates
from cnequity.storage.catalog_bridge import lake_root
from cnequity.storage.observation import freshness_mark
from cnequity.storage.state import StateStore
from equitylake_core.catalog import Catalog
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
        for name in datasets if datasets is not None else DATASETS:
            spec = DATASETS[name]
            coverage = coverage_of(spec, end, config)
            cells = plan_dataset(catalog, name, coverage, scope, calendar=calendar).cells
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
