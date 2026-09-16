"""Liveness for work that is slow and quiet.

The pipeline logs what it has finished, not what it is doing, so a step that
fetches for twenty minutes says its first word when it is already over. From
the terminal that is indistinguishable from a hang, and a run that looks hung
gets killed — which is how `cne init` loses the hours it had already banked.

Three deliberately dumb pieces: a reporter for a sweep that walks one item at a
time, a registry of the steps running right now, and a thread that names them
whenever the log has gone quiet for too long. The heartbeat speaks only while
something is actually running; an idle process has nothing to reassure anyone
about.
"""

from __future__ import annotations

import itertools
import logging
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# One minute is longer than any healthy gap measured in a full-market sweep
# (the widest was 105s inside `instruments`, which is exactly the case this
# exists to cover) and short enough that nobody reaches for Ctrl-C first.
HEARTBEAT_INTERVAL_SECONDS = 60.0
_POLL_SECONDS = 5.0


def hms(seconds: float) -> str:
    """Compact duration. A backfill runs for hours; `7245.3s` is not readable."""
    total = int(seconds)
    if total >= 3600:
        return f"{total // 3600}h{(total % 3600) // 60:02d}m"
    if total >= 60:
        return f"{total // 60}m{total % 60:02d}s"
    return f"{total}s"


def sweep_progress(
    log: logging.Logger,
    label: str,
    total: int,
    *,
    every: int = 100,
    unit: str = "symbols",
) -> Callable[[int], None]:
    """Report a long per-item sweep: the first report, every *every*, and the last.

    Whole-market sweeps that walk one symbol at a time — statements, share
    structure, adjustment factors — printed nothing at all between the line
    saying the step began and the line saying it ended, which on a full init is
    a long time to look stopped. This is the shape the TDX xdxr sweep already
    used, factored out so the rest can have it for one call.

    The first report carries no estimate: one item in, the elapsed time is
    still mostly whatever setup ran before the loop, and with a worker pool the
    first items all land together. By the hundredth that has washed out.
    """
    started_at = time.monotonic()
    last = 0

    def report(done: int) -> None:
        # Crossing a multiple of *every*, not landing on one: a sweep that
        # advances a chunk at a time would otherwise report only if its chunk
        # size happened to divide it.
        nonlocal last
        if total <= 0 or done <= last:
            return
        first = last == 0
        due = first or done >= total or done // every > last // every
        last = done
        if not due:
            return
        elapsed = time.monotonic() - started_at
        eta = ""
        if not first:
            eta = f" · ~{hms((elapsed / done) * (total - done))} left"
        log.info(
            "%s %d/%d %s · %s elapsed%s",
            label,
            done,
            total,
            unit,
            hms(elapsed),
            eta,
        )

    return report


_lock = threading.Lock()
# Token-keyed, not name-keyed: a wave can run the same step twice (a retry
# alongside the original), and a name key would let the second scope's exit
# erase the first one's entry.
_active: dict[int, tuple[str, float]] = {}
_tokens = itertools.count(1)
_last_record = time.monotonic()
_heartbeat: threading.Thread | None = None


@contextmanager
def step_scope(name: str) -> Iterator[None]:
    """Count *name* as running for as long as the block runs."""
    token = next(_tokens)
    with _lock:
        _active[token] = (name, time.monotonic())
    try:
        yield
    finally:
        with _lock:
            _active.pop(token, None)


def running_steps() -> list[tuple[str, float]]:
    """Active steps as (name, seconds running), oldest first."""
    now = time.monotonic()
    with _lock:
        active = sorted(_active.values(), key=lambda item: item[1])
    return [(name, now - started) for name, started in active]


class _ActivityFilter(logging.Filter):
    """Records the moment anything was logged, without dropping records."""

    def filter(self, record: logging.LogRecord) -> bool:
        global _last_record
        _last_record = time.monotonic()
        return True


_ACTIVITY = _ActivityFilter()


def start_heartbeat(interval_seconds: float = HEARTBEAT_INTERVAL_SECONDS) -> None:
    """Log a liveness line whenever nothing else has logged for *interval_seconds*.

    Call this after logging is configured, and again after any reconfiguration:
    `logging.basicConfig(force=True)` replaces the root handlers and takes the
    activity filter with them.

    The filter goes on the root *handlers* rather than on a logger, because a
    filter attached to a logger never sees the records its children propagate —
    and every record this pipeline emits comes from a child.
    """
    global _heartbeat
    for handler in logging.getLogger().handlers:
        handler.addFilter(_ACTIVITY)
    with _lock:
        if _heartbeat is not None:
            return
        _heartbeat = threading.Thread(
            target=_beat,
            args=(interval_seconds,),
            name="cne-heartbeat",
            daemon=True,
        )
        thread = _heartbeat
    thread.start()


def _beat_once(interval_seconds: float) -> bool:
    """Emit one liveness line if the log has been quiet that long. Testable."""
    silent_for = time.monotonic() - _last_record
    if silent_for < interval_seconds:
        return False
    active = running_steps()
    if not active:
        return False
    logger.info(
        "still working: %s (no output for %s)",
        ", ".join(f"{name} {hms(elapsed)}" for name, elapsed in active),
        hms(silent_for),
    )
    return True


def _beat(interval_seconds: float) -> None:
    poll = min(_POLL_SECONDS, interval_seconds) or _POLL_SECONDS
    while True:
        time.sleep(poll)
        _beat_once(interval_seconds)
