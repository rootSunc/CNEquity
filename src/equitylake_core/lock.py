"""An exclusive lock on a file, held across processes for the length of a ``with`` block.

The lock belongs to the open handle: the operating system releases it when
the handle closes, also when the process dies, so a crashed writer never
leaves a lake locked.
"""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path


class LockTimeout(TimeoutError):
    """Another process held the lock for longer than the caller would wait."""


def _try_lock(handle) -> bool:
    if os.name == "nt":
        import msvcrt

        try:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        return True
    import fcntl

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return False
    return True


def _unlock(handle) -> None:
    if os.name == "nt":
        import msvcrt

        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def exclusive(path: str | Path, *, timeout: float = 600.0, poll: float = 0.05) -> Iterator[None]:
    """Hold the lock file *path*; wait up to *timeout* seconds for another holder."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout
    with open(target, "a+b") as handle:
        while not _try_lock(handle):
            if time.monotonic() >= deadline:
                raise LockTimeout(f"{target} was held by another process for {timeout:g}s")
            time.sleep(poll)
        try:
            yield
        finally:
            _unlock(handle)


#: The one lock that makes a writer of a lake, in its directory: every writable
#: :class:`~equitylake_core.catalog.Catalog` connection holds it while open, so
#: no path can change a lake's catalog or files without it.
LAKE_LOCK_FILE = "write.lock"
#: How long a writer waits for another one by default.
LAKE_LOCK_TIMEOUT = 3600.0

_HELD = threading.local()


def lake_lock_path(root: str | Path) -> Path:
    return Path(root).expanduser().resolve() / LAKE_LOCK_FILE


def holds_lake_lock(root: str | Path) -> bool:
    """Whether the calling thread holds the lake lock of the lake at *root*."""
    return bool(getattr(_HELD, "counts", {}).get(str(lake_lock_path(root)), 0))


@contextmanager
def lake_lock(
    root: str | Path,
    *,
    timeout: float = LAKE_LOCK_TIMEOUT,
    poll: float = 0.05,
    on_wait: Callable[[Path], None] | None = None,
) -> Iterator[bool]:
    """Hold the lake lock of the lake at *root*; yields whether this call took it.

    Re-entrant for the thread that holds it, and only for that thread: another
    thread of the same process waits like another process does. *on_wait* is
    called once when the lock is busy, before waiting up to *timeout* seconds.
    """
    path = lake_lock_path(root)
    key = str(path)
    counts = getattr(_HELD, "counts", None)
    if counts is None:
        counts = _HELD.counts = {}
    if counts.get(key):
        counts[key] += 1
        try:
            yield False
        finally:
            counts[key] -= 1
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout
    with open(path, "a+b") as handle:
        if not _try_lock(handle):
            if on_wait is not None:
                on_wait(path)
            while not _try_lock(handle):
                if time.monotonic() >= deadline:
                    raise LockTimeout(f"{path} was held by another writer for {timeout:g}s")
                time.sleep(poll)
        counts[key] = 1
        try:
            yield True
        finally:
            counts.pop(key, None)
            _unlock(handle)
