"""Cross-platform exclusive file lock (POSIX ``flock`` / Windows ``msvcrt``).

Every concurrency guard in the lake — run lock, watermark writes, rate-limit
state, staging cleanup — needs the same three properties, and both backends
provide them:

* exclusive: one holder at a time, across processes *and* across handles inside
  one process (both backends key the lock to the open handle, not the pid);
* non-blocking acquire fails loudly (:class:`LockUnavailable`) instead of waiting;
* the lock is released when the handle closes, including on process death, so a
  crashed run never strands a lock file.

This module deliberately sits at the package root rather than in ``storage`` or
``domain``: all four call sites live in different layers, and a leaf with no
first-party imports can be used by any of them without inventing a new edge in
the dependency graph.
"""

from __future__ import annotations

import contextlib
import errno
import logging
import os
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import IO

logger = logging.getLogger(__name__)

#: How long a blocking acquire may wait before it reports a stall instead of
#: waiting on. Generous on purpose: the point is to catch a holder that has
#: hung, not to break the queue behind a legitimately long compact. A *crashed*
#: holder never gets here — the kernel releases its lock as the process dies —
#: so anything that reaches this deadline is alive and stuck, which is the case
#: that used to park every later command forever with nothing on screen.
DEFAULT_LOCK_WAIT_SECONDS = 3600.0

#: How long a blocking acquire waits before it says anything. The announcement
#: exists so a long wait does not read as a hang; sub-second queueing is not a
#: wait anyone is wondering about. Several of these locks *are* the per-request
#: rate limiter — `concurrency-ths.lock` is taken and released around every
#: single request — so announcing on first contention put a line in the log for
#: each one: measured at 47 lines in the first 4.5 minutes of
#: `ths-official resource-sectors`, none of them progress. That buries exactly
#: what the progress logging exists to surface, for the same reason httpx is
#: pinned to WARNING.
QUIET_LOCK_WAIT_SECONDS = 3.0

IS_WINDOWS = sys.platform == "win32"

# Windows locks a byte *range*, not a whole file. One byte at offset 0 is enough
# as long as every participant locks the same range, and a range may sit past
# EOF — which is what makes this work on the empty files we use as locks.
_WIN_LOCK_BYTES = 1

# msvcrt has no "wait forever" mode: LK_LOCK retries for ~10s and then raises.
# Poll LK_NBLCK instead so ``blocking=True`` means under Windows what it means
# under flock — wait until the holder is done.
_WIN_RETRY_INTERVAL = 0.05
_POSIX_RETRY_INTERVAL = 0.05

# Contention as reported by msvcrt.locking: EACCES from LK_NBLCK, EDEADLOCK from
# an exhausted LK_LOCK. Anything else (a bad descriptor, say) is a real error and
# must not be retried forever. `EDEADLOCK` is the Windows spelling and `EDEADLK`
# the POSIX one; only one exists per platform, and the tests exercise this branch
# on both, so collect whichever is there.
_WIN_BUSY_ERRNOS = frozenset(
    code
    for code in (
        errno.EACCES,
        errno.EAGAIN,
        getattr(errno, "EDEADLOCK", None),
        getattr(errno, "EDEADLK", None),
    )
    if code is not None
)


# ``run_lock(meta_root, "compact")`` and ``lake_mutation_lock(meta_root)``
# intentionally share one lock file.  A compact step can therefore call a
# revision helper while already holding that file.  ``flock`` does not make a
# second descriptor in the same thread re-entrant, so keep a tiny thread-local
# ownership marker: the specialised lake lock can reuse the outer handle,
# while an accidental nested ``exclusive_lock`` still fails immediately.
_LOCK_STATE = threading.local()


def _lock_key(path: Path | str) -> str:
    return os.path.abspath(os.fspath(path))


def _held_count(path: Path | str) -> int:
    held = getattr(_LOCK_STATE, "held", None)
    if not held:
        return 0
    key = _lock_key(path)
    entry = held.get(key)
    if entry is None:
        return 0
    pid, count = entry
    # A fork copies thread-local state into the child, but not the logical
    # owner of this process-local marker.  Discard inherited entries there;
    # the kernel lock remains the authority across processes.
    if pid != os.getpid():
        held.pop(key, None)
        return 0
    return count


def _mark_held(path: Path | str) -> None:
    held = getattr(_LOCK_STATE, "held", None)
    if held is None:
        held = {}
        _LOCK_STATE.held = held
    key = _lock_key(path)
    _pid, count = held.get(key, (os.getpid(), 0))
    held[key] = (os.getpid(), count + 1)


def _unmark_held(path: Path | str) -> None:
    held = getattr(_LOCK_STATE, "held", None)
    if not held:
        return
    key = _lock_key(path)
    entry = held.get(key)
    if entry is None or entry[0] != os.getpid():
        return
    if entry[1] <= 1:
        held.pop(key, None)
    else:
        held[key] = (entry[0], entry[1] - 1)


class LockUnavailable(RuntimeError):
    """A lock could not be acquired immediately or before its deadline."""


def _acquire_posix(handle: IO, *, blocking: bool, timeout: float | None = None) -> None:
    import fcntl

    if blocking and timeout is None:
        fcntl.flock(handle, fcntl.LOCK_EX)
        return

    deadline = time.monotonic() + max(timeout, 0.0) if blocking and timeout is not None else None
    while True:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except (BlockingIOError, PermissionError) as exc:
            # EWOULDBLOCK on Linux/macOS, EACCES on some other Unixes.
            if not blocking:
                raise LockUnavailable(str(getattr(handle, "name", handle))) from exc
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise LockUnavailable(
                        f"{getattr(handle, 'name', handle)}: timed out acquiring lock"
                    ) from exc
                time.sleep(min(_POSIX_RETRY_INTERVAL, remaining))


def _release_posix(handle: IO) -> None:
    import fcntl

    fcntl.flock(handle, fcntl.LOCK_UN)


def _acquire_windows(handle: IO, *, blocking: bool, timeout: float | None = None) -> None:
    import msvcrt

    deadline = time.monotonic() + max(timeout, 0.0) if blocking and timeout is not None else None
    while True:
        # The locked range starts at the current file position, and "a+" opens
        # at EOF — seek every time so both backends lock the same byte.
        handle.seek(0)
        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, _WIN_LOCK_BYTES)
            return
        except OSError as exc:
            if exc.errno not in _WIN_BUSY_ERRNOS:
                raise
            if not blocking:
                raise LockUnavailable(str(getattr(handle, "name", handle))) from exc
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise LockUnavailable(
                        f"{getattr(handle, 'name', handle)}: timed out acquiring lock"
                    ) from exc
                time.sleep(min(_WIN_RETRY_INTERVAL, remaining))
            else:
                time.sleep(_WIN_RETRY_INTERVAL)


def _release_windows(handle: IO) -> None:
    import msvcrt

    handle.seek(0)
    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, _WIN_LOCK_BYTES)


_acquire = _acquire_windows if IS_WINDOWS else _acquire_posix
_release = _release_windows if IS_WINDOWS else _release_posix


@contextlib.contextmanager
def exclusive_lock(
    path: Path | str,
    *,
    blocking: bool = True,
    timeout: float | None = None,
) -> Iterator[IO]:
    """Hold an exclusive lock on *path* for the duration of the block.

    Creates the lock file (and its parent directory) if needed. With
    ``blocking=False`` a lock already held elsewhere raises
    :class:`LockUnavailable` instead of waiting. With ``timeout`` set, a
    blocking acquire polls until the deadline and then raises the same error.
    """
    path = Path(path)
    if _held_count(path):
        # Do not let an accidental nested generic lock deadlock this thread.
        # ``lake_mutation_lock`` handles the one intentional nested case below.
        raise LockUnavailable(str(path))
    path.parent.mkdir(parents=True, exist_ok=True)
    # "a+" creates without truncating. Truncation matters on Windows, where the
    # locks are mandatory: opening "w" against a file whose byte 0 another
    # process holds fails outright instead of politely queueing.
    with open(path, "a+", encoding="utf-8") as handle:
        if blocking:
            # Try once without waiting, so queuing can be announced. A blocking
            # acquire is otherwise indistinguishable from a hang: no output, no
            # deadline, and the reason (someone else holds this) never said.
            try:
                _acquire(handle, blocking=False)
            except LockUnavailable:
                # Wait out the routine queueing quietly, and only announce what
                # is left — see QUIET_LOCK_WAIT_SECONDS.
                grace = (
                    QUIET_LOCK_WAIT_SECONDS
                    if timeout is None
                    else min(QUIET_LOCK_WAIT_SECONDS, timeout)
                )
                try:
                    _acquire(handle, blocking=True, timeout=grace)
                except LockUnavailable:
                    if timeout is not None and grace >= timeout:
                        raise
                    logger.info(
                        "waiting for %s — another process holds it (up to %s)",
                        path.name,
                        _hms(timeout) if timeout is not None else "indefinitely",
                    )
                    _acquire(
                        handle,
                        blocking=True,
                        timeout=None if timeout is None else timeout - grace,
                    )
        else:
            _acquire(handle, blocking=blocking, timeout=timeout)
        _mark_held(path)
        try:
            yield handle
        finally:
            try:
                _release(handle)
            finally:
                _unmark_held(path)


def is_locked(path: Path | str) -> bool:
    """True when someone currently holds the lock on *path*.

    A missing lock file counts as unlocked and is *not* created by the probe.
    """
    path = Path(path)
    if not path.exists():
        return False
    try:
        with exclusive_lock(path, blocking=False):
            return False
    except LockUnavailable:
        return True
    except OSError:
        # Windows can deny the open itself while another handle holds the file.
        # "Cannot even look at it" is indistinguishable from "held", and the
        # callers (skip this run / keep this lock file) want the same answer.
        return True


def _hms(seconds: float) -> str:
    total = int(seconds)
    if total >= 3600:
        return f"{total // 3600}h{(total % 3600) // 60:02d}m"
    if total >= 60:
        return f"{total // 60}m{total % 60:02d}s"
    return f"{total}s"


@contextlib.contextmanager
def lake_mutation_lock(
    meta_root: Path,
    *,
    blocking: bool = True,
    timeout: float | None = DEFAULT_LOCK_WAIT_SECONDS,
) -> Iterator[IO]:
    """Serialize every curated/derived read-modify-write operation.

    The orchestrator's ``compact`` step already uses this exact lock path via
    ``run_lock``.  Exposing the path from the low-level lock module lets
    maintenance and derive code participate without importing the orchestrator
    package (which would introduce a dependency cycle).
    """
    path = meta_root / "locks" / "compact.lock"
    if _held_count(path):
        # ``run_lock(..., "compact")`` uses the same path.  Reuse that outer
        # process-local lock; other threads/processes still contend in the
        # kernel through ``exclusive_lock``.
        yield None
        return
    with exclusive_lock(path, blocking=blocking, timeout=timeout) as handle:
        yield handle
