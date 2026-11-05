"""How far the source has been asked, separate from how far the data reaches.

An event dataset's newest row is its newest *event*. A quiet window has none,
so the data watermark stops while the lake keeps looking, and freshness judged
from it alone stayed stale for good: `verify` offered a backfill, the backfill
correctly found nothing new, and the next `verify` offered it again.

``observed_through`` records the other fact: every day through this date has
been asked of the source, across the lake's whole scope, and answered without
a failure. It only ever moves when that is proven:

- the window must join what was already covered, because asking about October
  says nothing about July;
- rows that were fetched must reach a published revision first, so a run that
  dies before compaction cannot claim days it never landed. An empty answer has
  nothing to publish and counts at once.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

from cnequity.storage.state import StateStore

logger = logging.getLogger(__name__)

OBSERVED_THROUGH_FIELD = "observed_through"
_STAGED_FIELD = "staged_observed_through"
#: Run-keyed, like ``staged_request_days``; staging cleanup prunes it.
STAGED_OBSERVATION_FIELD = _STAGED_FIELD


def observed_through(config, dataset: str) -> date | None:
    meta_root = getattr(config, "meta_root", None)
    if meta_root is None:
        return None
    try:
        return StateStore(meta_root).get_date(dataset, field=OBSERVED_THROUGH_FIELD)
    except (OSError, ValueError):
        return None


def freshness_mark(config, dataset: str, mark: date | None) -> date | None:
    """The later of the data watermark and the last clean look at the source."""
    seen = observed_through(config, dataset)
    if mark is None or seen is None:
        return mark or seen
    return max(mark, seen)


def _joins(dataset: str, covered: date, start: date) -> bool:
    """Whether a window from *start* leaves no unasked day after *covered*."""
    from cnequity.domain.datasets import _trading_session_lag, staleness_counts_trading_sessions

    if start <= covered + timedelta(days=1):
        return True
    if staleness_counts_trading_sessions(dataset):
        return _trading_session_lag(covered, start - timedelta(days=1)) == 0
    return False


def note_observation(
    config,
    dataset: str,
    run_id: str,
    *,
    start: date | None,
    through: date,
    pending_publish: bool,
) -> bool:
    """Record a clean, whole-scope answer for ``[start, through]``.

    ``start=None`` means the window began at the dataset's history floor.
    With ``pending_publish`` the claim waits for :func:`commit_observation`.
    Returns whether the window counted.
    """
    state = StateStore(config.meta_root)
    covered = freshness_mark(config, dataset, state.get_date(dataset))
    if start is not None and (covered is None or not _joins(dataset, covered, start)):
        logger.info(
            "%s: window %s..%s does not join coverage through %s; observation not recorded",
            dataset,
            start.isoformat(),
            through.isoformat(),
            covered.isoformat() if covered else "nothing",
        )
        return False
    with state.transaction(dataset) as payload:
        if pending_publish:
            staged = payload.setdefault(_STAGED_FIELD, {})
            staged[run_id] = max(staged.get(run_id, through.isoformat()), through.isoformat())
        else:
            _advance(payload, through)
    return True


def commit_observation(config, dataset: str, run_id: str) -> None:
    """Apply a staged observation once its rows reached a revision."""
    with StateStore(config.meta_root).transaction(dataset) as payload:
        staged = payload.get(_STAGED_FIELD) or {}
        through = staged.pop(run_id, None)
        if not staged:
            payload.pop(_STAGED_FIELD, None)
        if through:
            _advance(payload, date.fromisoformat(through))


def _advance(payload: dict, through: date) -> None:
    current = payload.get(OBSERVED_THROUGH_FIELD)
    if current is None or through.isoformat() > str(current):
        payload[OBSERVED_THROUGH_FIELD] = through.isoformat()
