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
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import IO

IS_WINDOWS = sys.platform == "win32"

# Windows locks a byte *range*, not a whole file. One byte at offset 0 is enough
# as long as every participant locks the same range, and a range may sit past
# EOF — which is what makes this work on the empty files we use as locks.
_WIN_LOCK_BYTES = 1

# msvcrt has no "wait forever" mode: LK_LOCK retries for ~10s and then raises.
# Poll LK_NBLCK instead so ``blocking=True`` means under Windows what it means
# under flock — wait until the holder is done.
_WIN_RETRY_INTERVAL = 0.05

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


class LockUnavailable(RuntimeError):
    """A non-blocking acquire lost the race — someone else holds the lock."""


def _acquire_posix(handle: IO, *, blocking: bool) -> None:
    import fcntl

    if blocking:
        fcntl.flock(handle, fcntl.LOCK_EX)
        return
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (BlockingIOError, PermissionError) as exc:
        # EWOULDBLOCK on Linux/macOS, EACCES on some other Unixes.
        raise LockUnavailable(str(getattr(handle, "name", handle))) from exc


def _release_posix(handle: IO) -> None:
    import fcntl

    fcntl.flock(handle, fcntl.LOCK_UN)


def _acquire_windows(handle: IO, *, blocking: bool) -> None:
    import msvcrt

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
            time.sleep(_WIN_RETRY_INTERVAL)


def _release_windows(handle: IO) -> None:
    import msvcrt

    handle.seek(0)
    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, _WIN_LOCK_BYTES)


_acquire = _acquire_windows if IS_WINDOWS else _acquire_posix
_release = _release_windows if IS_WINDOWS else _release_posix


@contextlib.contextmanager
def exclusive_lock(path: Path | str, *, blocking: bool = True) -> Iterator[IO]:
    """Hold an exclusive lock on *path* for the duration of the block.

    Creates the lock file (and its parent directory) if needed. With
    ``blocking=False`` a lock already held elsewhere raises
    :class:`LockUnavailable` instead of waiting.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # "a+" creates without truncating. Truncation matters on Windows, where the
    # locks are mandatory: opening "w" against a file whose byte 0 another
    # process holds fails outright instead of politely queueing.
    with open(path, "a+", encoding="utf-8") as handle:
        _acquire(handle, blocking=blocking)
        try:
            yield handle
        finally:
            _release(handle)


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


@contextlib.contextmanager
def lake_mutation_lock(meta_root: Path, *, blocking: bool = True) -> Iterator[IO]:
    """Serialize every curated/derived read-modify-write operation.

    The orchestrator's ``compact`` step already uses this exact lock path via
    ``run_lock``.  Exposing the path from the low-level lock module lets
    maintenance and derive code participate without importing the orchestrator
    package (which would introduce a dependency cycle).
    """
    with exclusive_lock(meta_root / "locks" / "compact.lock", blocking=blocking) as handle:
        yield handle
