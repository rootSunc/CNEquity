"""How often each daily schedule group fetches: every session, or once a week.

``[job.daily.groups.<name>] cadence = "weekly"`` makes a group fetch its
history datasets once a week instead of every session. A history dataset
(``fetch_semantics != "snapshot"``) catches the missed sessions up by date on
the next due run, so fetching it weekly loses nothing and a scheduler that
wakes daily simply skips it in between.

Snapshot datasets keep fetching every session even inside a weekly group: they
only ever describe *today* (fund flow, hot rank, trading status, …), so a day
not fetched is a day lost for good. Their in-group dependencies run with them.

A weekly group is due on the last exchange session of each ISO week, up to
``weekday`` (ISO, 1=Mon…5=Fri, default 5). A Friday holiday therefore moves the
run to Thursday rather than skipping the week.

Freshness follows the cadence: a history dataset owned only by weekly groups is
judged against the group's last due session, not today, so ``cne status``, the
late stale pass and the health gate do not call it stale — and re-fetch it —
on the days it is meant to rest.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from cnequity.config.loader import ScheduleGroup

CADENCES = ("daily", "weekly")
DEFAULT_SCHEDULE_GROUPS = (
    "core",
    "capital",
    "signals",
    "fundamentals",
    "macro_risk",
    "research",
    "actions_sweep",
)


def group_is_runnable(config: Any, group: ScheduleGroup) -> bool:
    """Whether a configured group has any enabled dataset to fetch."""
    from cnequity.domain.datasets import DATASETS, is_dataset_enabled

    datasets = [step for step in group.steps if step in DATASETS]
    return not datasets or any(is_dataset_enabled(name, config) for name in datasets)


def scheduled_group_names(config: Any) -> list[str]:
    """The single ordered group plan used by CLI, shell and launchd."""
    groups = getattr(config, "schedule_groups", None) or {}
    return [name for name, group in groups.items() if group_is_runnable(config, group)]


def _is_session(day: date) -> bool:
    from cnequity.domain.datasets import _is_exchange_session

    return _is_exchange_session(day)


def _is_weekly(group: Any) -> bool:
    return getattr(group, "cadence", "daily") == "weekly"


def _week_anchor(day: date, weekday: int) -> date:
    return day - timedelta(days=day.isoweekday() - 1) + timedelta(days=weekday - 1)


def group_due(group: ScheduleGroup, trade_date: date) -> bool:
    """Whether *group* fetches its history datasets on *trade_date*."""
    if not _is_weekly(group):
        return True
    anchor = _week_anchor(trade_date, int(getattr(group, "weekday", 5) or 5))
    if trade_date > anchor:
        return False
    day = trade_date + timedelta(days=1)
    while day <= anchor:
        if _is_session(day):
            return False
        day += timedelta(days=1)
    return _is_session(trade_date)


def last_due_date(group: ScheduleGroup, anchor: date) -> date:
    """The latest session on or before *anchor* on which *group* was due."""
    if not _is_weekly(group):
        return anchor
    day = anchor
    for _ in range(60):  # a closed fortnight (Spring Festival) is the longest gap
        if group_due(group, day):
            return day
        day -= timedelta(days=1)
    return anchor


def _is_snapshot(step: str) -> bool:
    from cnequity.domain.datasets import DATASETS

    spec = DATASETS.get(step)
    return step == "futures_minute_bars" or (
        spec is not None and spec.fetch_semantics == "snapshot"
    )


def due_steps(group: ScheduleGroup, trade_date: date) -> list[str]:
    """The steps *group* runs on *trade_date*: all when due, else its snapshots.

    A snapshot step keeps its in-group dependencies (``trading_status`` needs
    that session's ``instruments``); ``compact`` stays when anything else does.
    """
    steps = list(group.steps)
    if group_due(group, trade_date):
        return steps
    from cnequity.orchestrator.registry import STEP_REGISTRY

    keep = {step for step in steps if _is_snapshot(step)}
    frontier = list(keep)
    while frontier:
        entry = STEP_REGISTRY.get(frontier.pop())
        for dep in getattr(entry, "depends_on", []) or []:
            if dep in steps and dep not in keep:
                keep.add(dep)
                frontier.append(dep)
    if keep and "compact" in steps:
        keep.add("compact")
    return [step for step in steps if step in keep]


def freshness_anchor(config: Any, dataset: str, anchor: date) -> date:
    """The session *dataset* is expected to be fresh through, given its cadence.

    A history dataset scheduled only in weekly groups is due through the last
    session any of them ran; one in a daily group (or none) through *anchor*.
    """
    if _is_snapshot(dataset):
        return anchor
    groups = [
        group
        for group in (getattr(config, "schedule_groups", None) or {}).values()
        if dataset in getattr(group, "steps", [])
    ]
    if not groups or not all(_is_weekly(group) for group in groups):
        return anchor
    return max(last_due_date(group, anchor) for group in groups)
