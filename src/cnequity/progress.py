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

import faulthandler
import itertools
import logging
import os
import signal
import sys
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager

from cnequity.i18n import N_, NP_, Message

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


# The units sweeps count in, for the terminal's translation of progress lines.
# A unit not listed here is shown as the caller wrote it.
PROGRESS_UNITS = (
    NP_("progress unit", "symbols"),
    NP_("progress unit", "windows"),
    NP_("progress unit", "datasets"),
    NP_("progress unit", "boards"),
)


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
            eta = Message(N_(" · ~%s left"), hms((elapsed / done) * (total - done)))
        log.info(
            N_("%s %d/%d %s · %s elapsed%s"),
            label,
            done,
            total,
            Message.word("progress unit", unit),
            hms(elapsed),
            eta,
        )

    return report


def stage_progress(log: logging.Logger, label: str, total: int) -> Callable[..., None]:
    """Report a step that is a short chain of slow calls, one line per call.

    `instruments` is that shape: the TDX security list, the Beijing board,
    listing dates from the exchanges, Baostock and EastMoney, the delisted
    directory. None of them is a per-item sweep, and the step printed nothing
    for 234s on a full-market init. Naming each call as it starts also says
    which source a long gap belongs to.
    """
    started_at = time.monotonic()
    done = 0

    def enter(stage: str, *, held: int | None = None) -> None:
        nonlocal done
        done += 1
        tail = Message(N_(" · %s so far"), held) if held is not None else ""
        log.info(
            N_("%s %d/%d %s · %s elapsed%s"),
            label,
            done,
            total,
            Message(stage),
            hms(time.monotonic() - started_at),
            tail,
        )

    return enter


_lock = threading.Lock()
# Token-keyed, not name-keyed: a wave can run the same step twice (a retry
# alongside the original), and a name key would let the second scope's exit
# erase the first one's entry.
_active: dict[int, tuple[str, float]] = {}
_tokens = itertools.count(1)
_last_record = time.monotonic()
_heartbeat: threading.Thread | None = None
# The thread waits on its own event instead of sleeping, which makes it both
# stoppable and immune to a patched `time.sleep`: a test that fakes sleep to
# capture its arguments would otherwise turn this loop into a full-speed spin,
# and the millions of calls it makes land in that test's capture list. The
# event is per-thread and passed in, so a stop can never signal a generation
# other than the one it took.
_stop: threading.Event | None = None


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


_details: dict[int, Callable[[], object]] = {}
_stack_dumps = False
_hinted = False


def enable_stack_dumps() -> bool:
    """Make ``kill -USR1 <pid>`` write every thread's stack to stderr.

    #56 asked how to see where a quiet run was stuck, and the only answer was
    to install py-spy on the server first. The interpreter can already do it:
    ``faulthandler`` dumps from a C signal handler, so it answers even while
    the main thread is blocked. Pool workers call this too, so a worker's own
    stack is one ``kill`` away. Off on Windows (no SIGUSR1) and when stderr has
    no file descriptor. Returns whether dumps are on.
    """
    global _stack_dumps
    sig = getattr(signal, "SIGUSR1", None)
    if sig is None:
        return False
    try:
        sys.stderr.fileno()
    except (AttributeError, OSError, ValueError):
        return False
    faulthandler.register(sig, file=sys.stderr, all_threads=True, chain=False)
    _stack_dumps = True
    return True


@contextmanager
def detail_scope(describe: Callable[[], object]) -> Iterator[None]:
    """Add *describe*'s answer to every heartbeat while the block runs.

    A step name says *that* something is running, not *where*: #56 sat at
    "still working: daily_bars" for 23 minutes and nobody could tell which of
    105 batches was holding it. A step that knows its own in-flight work
    registers a callable here; it runs on the heartbeat thread, so it must be
    cheap and thread-safe, and returning None (or raising) prints nothing.
    """
    token = next(_tokens)
    with _lock:
        _details[token] = describe
    try:
        yield
    finally:
        with _lock:
            _details.pop(token, None)


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
    global _heartbeat, _stop
    for handler in logging.getLogger().handlers:
        handler.addFilter(_ACTIVITY)
    with _lock:
        if _heartbeat is not None:
            return
        _stop = stop = threading.Event()
        _heartbeat = threading.Thread(
            target=_beat,
            args=(interval_seconds, stop),
            name="cne-heartbeat",
            daemon=True,
        )
        thread = _heartbeat
    thread.start()


def stop_heartbeat(timeout: float = 5.0) -> None:
    """Stop the thread and allow a later `start_heartbeat` to start a new one.

    A daemon thread dies with the process, so nothing in production needed
    this. A test process is one process for thousands of tests, and a thread
    one test started keeps running through every test after it.
    """
    global _heartbeat, _stop
    with _lock:
        thread, _heartbeat = _heartbeat, None
        stop, _stop = _stop, None
    if stop is not None:
        stop.set()
    if thread is not None:
        thread.join(timeout)


def _beat_once(interval_seconds: float) -> bool:
    """Emit one liveness line if the log has been quiet that long. Testable."""
    silent_for = time.monotonic() - _last_record
    if silent_for < interval_seconds:
        return False
    active = running_steps()
    if not active:
        return False
    logger.info(
        N_("still working: %s (no output for %s)"),
        ", ".join(f"{name} {hms(elapsed)}" for name, elapsed in active),
        hms(silent_for),
    )
    global _hinted
    if _stack_dumps and not _hinted:
        _hinted = True
        logger.info(
            N_("To see where it is, run `kill -USR1 %d`: every thread's stack goes to stderr."),
            os.getpid(),
        )
    with _lock:
        describers = list(_details.values())
    for describe in describers:
        try:
            detail = describe()
        except Exception:  # noqa: BLE001 — a heartbeat must never take the run down
            continue
        if detail:
            logger.info("%s", detail)
    return True


def _beat(interval_seconds: float, stop: threading.Event) -> None:
    poll = min(_POLL_SECONDS, interval_seconds) or _POLL_SECONDS
    while not stop.wait(poll):
        _beat_once(interval_seconds)
