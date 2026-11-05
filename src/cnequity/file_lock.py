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
import json
import logging
import os
import shlex
import socket
import sys
import threading
import time
from collections.abc import Callable, Iterator
from datetime import datetime, timezone
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


# ------------------------------------------------------------ holder notes
#
# "waiting for compact.lock — held by another process" was all a queued command
# could say, and on the real lake that was not enough: a 1-hour wait on
# 2026-10-11 timed out and failed an events run, and no log could tell which
# process had held the lock. Locks taken with ``record_holder=True`` keep a
# small JSON note beside the lock file naming the holder, written right after
# acquiring and removed right before releasing — so a holder can only ever
# remove its own note, never its successor's. The note is advisory: the kernel
# lock stays the only authority, and a note that cannot be written or read
# changes nothing about who gets the lock.

#: Appended to the lock file's name. Not ``*.lock``, so the stale-lock sweep
#: in ``staging_cleanup`` never mistakes a note for a lock.
HOLDER_SUFFIX = ".holder.json"

_COMMAND_CHARS = 240

_holder_activity: Callable[[], str | None] | None = None


def set_holder_activity(describe: Callable[[], str | None] | None) -> None:
    """Register what the current thread is doing, for holder notes.

    ``cnequity.progress`` registers the step running on the calling thread.
    This module stays a leaf: it asks through this hook instead of importing.
    """
    global _holder_activity
    _holder_activity = describe


def holder_path(path: Path | str) -> Path:
    path = Path(path)
    return path.with_name(path.name + HOLDER_SUFFIX)


def _command_line() -> str:
    argv = list(sys.argv) or [sys.executable]
    head = Path(argv[0]).name
    if head == "-c":
        head = "python -c"
    elif head == "__main__.py":
        # ``python -m cnequity ...`` puts the package's __main__ path here.
        head = f"python -m {Path(argv[0]).parent.name}"
    command = " ".join([head, *(shlex.quote(str(arg)) for arg in argv[1:])])
    if len(command) > _COMMAND_CHARS:
        command = command[: _COMMAND_CHARS - 1] + "…"
    return command


def _write_holder(path: Path) -> None:
    activity = None
    if _holder_activity is not None:
        try:
            activity = _holder_activity()
        except Exception:  # noqa: BLE001 — a note must never fail the lock
            activity = None
    record = {
        "pid": os.getpid(),
        "host": socket.gethostname(),
        "command": _command_line(),
        "activity": activity,
        "thread": threading.current_thread().name,
        "acquired_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    target = holder_path(path)
    # Written in place, not swapped in: only the lock holder ever writes the
    # note, so there is no writer race, and a reader that catches it half
    # written gets invalid JSON, which ``read_holder`` treats as "no note".
    try:
        target.write_text(json.dumps(record, ensure_ascii=False), encoding="utf-8")
    except OSError:
        logger.debug("could not write holder note %s", target, exc_info=True)


def _clear_holder(path: Path) -> None:
    # Still under the lock: nobody else can have written the note since we
    # acquired, so whatever is there is ours (or a dead holder's) to remove.
    with contextlib.suppress(OSError):
        holder_path(path).unlink(missing_ok=True)


def _pid_alive(pid: int) -> bool:
    if IS_WINDOWS:
        # os.kill(pid, 0) terminates the process on Windows. Without a safe
        # probe, trust the note; the next recording holder overwrites it.
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return True
    return True


def read_holder(path: Path | str) -> dict | None:
    """The holder note for lock *path*, or None when absent, unreadable or stale.

    A note left by a process that died on this host is stale: the kernel
    already released its lock, so the note no longer names the holder.
    """
    try:
        record = json.loads(holder_path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(record, dict) or not isinstance(record.get("pid"), int):
        return None
    if record.get("host") == socket.gethostname() and not _pid_alive(record["pid"]):
        return None
    return record


def describe_holder(path: Path | str) -> str | None:
    """One line naming who holds lock *path*, from its holder note."""
    record = read_holder(path)
    if record is None:
        return None
    pid = record["pid"]
    if pid == os.getpid() and record.get("host") == socket.gethostname():
        who = f"another thread of this process ({record.get('thread') or 'unnamed'})"
    else:
        who = f"pid {pid}"
        host = record.get("host")
        if host and host != socket.gethostname():
            who += f" on {host}"
    parts = [who]
    activity = record.get("activity")
    if activity:
        parts.append(f"step {activity}")
    since = record.get("acquired_at")
    if isinstance(since, str):
        try:
            started = datetime.fromisoformat(since)
            held = (datetime.now(timezone.utc) - started).total_seconds()
            local = started.astimezone().strftime("%Y-%m-%d %H:%M:%S")
            parts.append(f"since {local} ({_hms(max(held, 0.0))})")
        except ValueError:
            pass
    command = record.get("command")
    if command:
        parts.append(f"`{command}`")
    return ", ".join(parts)


def _held_by(path: Path) -> str:
    holder = describe_holder(path)
    if holder:
        return f"held by {holder}"
    return "held by another process or another thread of this run"


@contextlib.contextmanager
def exclusive_lock(
    path: Path | str,
    *,
    blocking: bool = True,
    timeout: float | None = None,
    record_holder: bool = False,
) -> Iterator[IO]:
    """Hold an exclusive lock on *path* for the duration of the block.

    Creates the lock file (and its parent directory) if needed. With
    ``blocking=False`` a lock already held elsewhere raises
    :class:`LockUnavailable` instead of waiting. With ``timeout`` set, a
    blocking acquire polls until the deadline and then raises the same error.
    ``record_holder=True`` keeps a holder note beside the lock while it is
    held (see :func:`describe_holder`); leave it off for per-request locks.
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
                queued_at = time.monotonic()
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
                    # flock conflicts between file handles, not processes:
                    # another thread of this same run queues here too (the
                    # derive's lanes on baostock's one connection), and
                    # blaming "another process" sent people hunting for one.
                    # A holder note, when there is one, names it outright.
                    logger.info(
                        "waiting for %s — %s (up to %s)",
                        path.name,
                        _held_by(path),
                        _hms(timeout) if timeout is not None else "indefinitely",
                    )
                    try:
                        _acquire(
                            handle,
                            blocking=True,
                            timeout=None if timeout is None else timeout - grace,
                        )
                    except LockUnavailable as exc:
                        raise LockUnavailable(
                            f"{path}: timed out acquiring lock after "
                            f"{_hms(time.monotonic() - queued_at)}; {_held_by(path)}"
                        ) from exc
                    # Closes the announced wait, so a log says how long the
                    # queue actually cost instead of leaving it to be inferred.
                    logger.info(
                        "acquired %s after waiting %s",
                        path.name,
                        _hms(time.monotonic() - queued_at),
                    )
        else:
            _acquire(handle, blocking=blocking, timeout=timeout)
        _mark_held(path)
        if record_holder:
            _write_holder(path)
        try:
            yield handle
        finally:
            try:
                if record_holder:
                    _clear_holder(path)
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


#: Relative to a lake ``meta`` directory. Archive walks that directory while
#: holding the lock; the path is shared so the walk can skip the file it cannot
#: re-open. On Windows the lock is mandatory, and a second handle's read fails.
MUTATION_LOCK_RELATIVE = Path("locks") / "compact.lock"


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
    path = meta_root / MUTATION_LOCK_RELATIVE
    if _held_count(path):
        # ``run_lock(..., "compact")`` uses the same path.  Reuse that outer
        # process-local lock; other threads/processes still contend in the
        # kernel through ``exclusive_lock``.
        yield None
        return
    with exclusive_lock(path, blocking=blocking, timeout=timeout, record_holder=True) as handle:
        yield handle
