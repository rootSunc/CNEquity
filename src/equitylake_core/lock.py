"""An exclusive lock on a file, held across processes for the length of a ``with`` block.

The lock belongs to the open handle: the operating system releases it when
the handle closes, also when the process dies, so a crashed writer never
leaves a lake locked.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
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
