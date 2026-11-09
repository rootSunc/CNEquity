"""Whether a dataset in a real lake is due, not just by the registry's rule.

``domain.datasets.stale_between`` is the registry's tolerance rule and stays
pure. Judging a lake also needs two facts only the lake and the schedule hold:

* the last clean look at the source (``storage/observation.py``): an event
  dataset's newest row is its newest event, and a quiet window has none;
* the schedule's cadence (``orchestrator/cadence.py``): a history dataset a
  weekly group fetches is due through that group's last session, not today.
"""

from __future__ import annotations

from cnequity.domain.datasets import (
    daily_freshness_owed,
    removed_from_schedule,
    stale_between,
    tushare_backfill_dataset,
)
from cnequity.orchestrator.cadence import freshness_anchor
from cnequity.storage.key_checks import key_rejected
from cnequity.storage.observation import freshness_mark


def freshness_owed(config, dataset: str) -> bool:
    """``daily_freshness_owed``, and not a Tushare dataset whose key was refused.

    A key the provider rejected cannot update anything, so its datasets'
    watermarks are not shown as stale. The verdict comes from the last key
    check (see ``cnequity.storage.key_checks``).
    """
    if not daily_freshness_owed(config, dataset):
        return False
    if config is not None and tushare_backfill_dataset(dataset):
        return not key_rejected(config, "tushare")
    return True


def is_stale(dataset: str, mark, anchor, config=None) -> bool:
    """Whether *dataset* lags in this lake (see the module docstring)."""
    if mark is None or anchor is None:
        return False
    if not freshness_owed(config, dataset):
        return False
    if config is not None and not removed_from_schedule(config, dataset):
        mark = freshness_mark(config, dataset, mark)
        anchor = freshness_anchor(config, dataset, anchor)
    return stale_between(dataset, mark, anchor, config)


def freshness_state(dataset: str, mark, anchor, config=None) -> str:
    """``"fresh"``, ``"stale"``, ``"n/a"`` or ``"removed"``.

    ``"n/a"`` is a Tushare backfill dataset this lake does not update daily
    (see ``freshness_owed``). ``"removed"`` is ``removed_from_schedule``.
    """
    if removed_from_schedule(config, dataset):
        return "removed"
    if not freshness_owed(config, dataset):
        return "n/a"
    return "stale" if is_stale(dataset, mark, anchor, config) else "fresh"
