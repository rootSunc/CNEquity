"""Cross-process file locks. The kernel owns a lock for the life of its handle."""

from __future__ import annotations

import ctypes
import errno
import os
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import BinaryIO


class LockUnavailable(RuntimeError):
    """The lock is held and could not be acquired within the requested wait."""


class LockBusy(LockUnavailable):
    """The lock was busy when tried without waiting."""


class LockTimeout(TimeoutError, LockUnavailable):
    """The lock remained busy until the caller's deadline."""


class LockIOError(OSError):
    """The operating system could not open, acquire, or release a lock."""

    def __init__(self, path: str | Path, error: OSError):
        super().__init__(error.errno, error.strerror or str(error), os.fspath(path))
        self.winerror = getattr(error, "winerror", None)


class _Overlapped(ctypes.Structure):
    _fields_ = [
        ("internal", ctypes.c_size_t),
        ("internal_high", ctypes.c_size_t),
        ("offset", ctypes.c_uint32),
        ("offset_high", ctypes.c_uint32),
        ("event", ctypes.c_void_p),
    ]


@cache
def _windows_api():
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    handle = ctypes.c_void_p
    dword = ctypes.c_uint32
    overlapped = ctypes.POINTER(_Overlapped)
    kernel.LockFileEx.argtypes = [handle, dword, dword, dword, dword, overlapped]
    kernel.UnlockFileEx.argtypes = [handle, dword, dword, dword, overlapped]
    kernel.LockFileEx.restype = ctypes.c_int
    kernel.UnlockFileEx.restype = ctypes.c_int
    return kernel


def _windows_range(handle: BinaryIO, *, unlock: bool = False, shared: bool = False) -> int:
    """Operate on byte zero; return the original Win32 error code on failure."""
    import msvcrt

    api = _windows_api()
    native = msvcrt.get_osfhandle(handle.fileno())
    overlapped = ctypes.byref(_Overlapped())
    if unlock:
        ok = api.UnlockFileEx(native, 0, 1, 0, overlapped)
    else:
        fail_immediately = 1
        exclusive = 2
        flags = fail_immediately if shared else fail_immediately | exclusive
        ok = api.LockFileEx(native, flags, 0, 1, 0, overlapped)
    return 0 if ok else ctypes.get_last_error()


def _try_windows(handle: BinaryIO, *, shared: bool) -> bool:
    error = _windows_range(handle, shared=shared)
    if error == 33:  # ERROR_LOCK_VIOLATION
        return False
    if error:
        raise ctypes.WinError(error)
    return True


def _try_posix(handle: BinaryIO, *, shared: bool) -> bool:
    import fcntl

    mode = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
    try:
        fcntl.flock(handle.fileno(), mode | fcntl.LOCK_NB)
    except OSError as exc:
        if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
            return False
        raise
    return True


_try_lock = _try_windows if os.name == "nt" else _try_posix
_HANDLES_LOCK = threading.Lock()
_HANDLES: set[BinaryIO] = set()
_HELD = threading.local()


def _reset_after_fork() -> None:
    global _HANDLES_LOCK
    # Close the inherited descriptors without unlocking the parent's flock.
    # Lock handles are unbuffered, so there is no inherited buffer lock to take.
    _HANDLES_LOCK = threading.Lock()
    for handle in list(_HANDLES):
        handle.close()
    _HANDLES.clear()
    _HELD.lakes = {}
    _HELD.maintenance = set()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_after_fork)


@contextmanager
def open_lock_file(path: str | Path, *, shared: bool = False) -> Iterator[BinaryIO]:
    target = Path(path)
    try:
        if shared and target.is_file():
            handle = target.open("rb", buffering=0)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            handle = target.open("a+b", buffering=0)
    except OSError as exc:
        raise LockIOError(target, exc) from exc
    with handle:
        with _HANDLES_LOCK:
            _HANDLES.add(handle)
        try:
            yield handle
        finally:
            with _HANDLES_LOCK:
                _HANDLES.discard(handle)


def acquire_handle(
    handle: BinaryIO,
    *,
    blocking: bool = True,
    timeout: float | None = None,
    shared: bool = False,
    poll: float = 0.05,
    on_wait: Callable[[], None] | None = None,
) -> None:
    deadline = None if timeout is None else time.monotonic() + max(0, timeout)
    announced = False
    while True:
        try:
            if _try_lock(handle, shared=shared):
                return
        except OSError as exc:
            raise LockIOError(handle.name, exc) from exc
        if not blocking:
            raise LockBusy(f"{handle.name}: lock is held")
        remaining = poll if deadline is None else deadline - time.monotonic()
        if remaining <= 0:
            raise LockTimeout(f"{handle.name}: timed out acquiring lock ({timeout:g}s)")
        if on_wait is not None and not announced:
            announced = True
            on_wait()
        time.sleep(min(poll, remaining))


def release_handle(handle: BinaryIO) -> None:
    if handle.closed:
        return
    try:
        if os.name == "nt":
            error = _windows_range(handle, unlock=True)
            if error:
                raise ctypes.WinError(error)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except OSError as exc:
        raise LockIOError(handle.name, exc) from exc


@contextmanager
def file_lock(
    path: str | Path, *, timeout: float | None = None, shared: bool = False, poll: float = 0.05
) -> Iterator[None]:
    with open_lock_file(path, shared=shared) as handle:
        acquire_handle(handle, timeout=timeout, shared=shared, poll=poll)
        try:
            yield
        finally:
            release_handle(handle)


def exclusive(path: str | Path, *, timeout: float = 600.0, poll: float = 0.05):
    """Hold a file exclusively for callers of the original Core lock API."""
    return file_lock(path, timeout=timeout, poll=poll)


LAKE_LOCK_FILE = "write.lock"
LAKE_LOCK_TIMEOUT = 3600.0


@dataclass
class _HeldLake:
    stack: ExitStack
    users: int = 1


def lake_lock_path(root: str | Path) -> Path:
    return Path(root).expanduser().resolve() / LAKE_LOCK_FILE


@contextmanager
def lake_operation(root: str | Path, *, timeout: float = LAKE_LOCK_TIMEOUT) -> Iterator[None]:
    """Keep a lake in place for the duration of a managed operation."""
    path = lake_lock_path(root).with_name("maintenance.lock")
    if path in getattr(_HELD, "maintenance", ()):
        yield
    else:
        with file_lock(path, timeout=timeout, shared=True):
            yield


@contextmanager
def lake_maintenance(root: str | Path, *, timeout: float = 0) -> Iterator[None]:
    """Exclude managed readers and writers during operations requiring an idle lake.

    Only the holding thread passes. Other threads of the same process wait like
    other processes, which keeps a dashboard's request threads out of a purge
    it runs in the background; work done under maintenance must not hand lake
    reads or writes to worker threads.
    """
    path = lake_lock_path(root).with_name("maintenance.lock")
    if not hasattr(_HELD, "maintenance"):
        _HELD.maintenance = set()
    active = _HELD.maintenance
    if path in active:
        yield
        return
    with file_lock(path, timeout=timeout):
        active.add(path)
        try:
            yield
        finally:
            active.remove(path)


def lake_files(root: str | Path, *, shared: bool, timeout: float = LAKE_LOCK_TIMEOUT):
    """Guard physical file lifetime, independently of catalog publication."""
    return file_lock(lake_lock_path(root).with_name("readers.lock"), timeout=timeout, shared=shared)


def snapshot_lock(
    root: str | Path, snapshot: int, *, shared: bool, timeout: float = LAKE_LOCK_TIMEOUT
):
    path = lake_lock_path(root).parent / "snapshot-locks" / f"{int(snapshot)}.lock"
    return file_lock(path, timeout=timeout, shared=shared)


@contextmanager
def lake_read(root: str | Path) -> Iterator[None]:
    """Protect managed file readers for the duration of the actual scan or copy."""
    root = Path(root)
    if not root.parent.is_dir():
        yield
        return
    with lake_operation(root):
        if not (root / "catalog.sqlite").is_file():
            yield
        else:
            with lake_files(root, shared=True):
                yield


def holds_lake_lock(root: str | Path) -> bool:
    """Whether the calling thread holds the lake lock of the lake at *root*."""
    return str(lake_lock_path(root)) in getattr(_HELD, "lakes", {})


@contextmanager
def lake_lock(
    root: str | Path,
    *,
    timeout: float = LAKE_LOCK_TIMEOUT,
    poll: float = 0.05,
    on_wait: Callable[[Path], None] | None = None,
) -> Iterator[bool]:
    """Hold the lake lock. Nested calls in the owning thread reuse its handle."""
    path = lake_lock_path(root)
    key = str(path)
    if not hasattr(_HELD, "lakes"):
        _HELD.lakes = {}
    lakes = _HELD.lakes
    held = lakes.get(key)
    took = held is None
    if held is None:
        started = time.monotonic()
        with ExitStack() as stack:
            stack.enter_context(lake_operation(root, timeout=timeout))
            handle = stack.enter_context(open_lock_file(path))
            acquire_handle(
                handle,
                timeout=max(0.0, timeout - (time.monotonic() - started)),
                poll=poll,
                on_wait=None if on_wait is None else lambda: on_wait(path),
            )
            stack.callback(release_handle, handle)
            held = lakes[key] = _HeldLake(stack.pop_all())
    else:
        held.users += 1
    try:
        yield took
    finally:
        held.users -= 1
        if held.users == 0:
            lakes.pop(key, None)
            held.stack.close()
