"""Cross-platform file lock: real semantics on this host, both backends faked.

The backend tests inject a fake ``msvcrt`` / ``fcntl`` into ``sys.modules`` so
the Windows branch is covered from a Unix CI runner and vice versa — the whole
point of the module is that the two agree, and only one is importable per host.
"""

from __future__ import annotations

import errno
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pytest

from cn_market_lake import file_lock
from cn_market_lake.file_lock import LockUnavailable, exclusive_lock, is_locked, lake_mutation_lock

# ---------------------------------------------------------------- real backend


def test_lock_file_and_parent_are_created(tmp_path):
    path = tmp_path / "nested" / "dir" / "run.lock"
    with exclusive_lock(path):
        assert path.exists()


def test_second_non_blocking_acquire_fails(tmp_path):
    path = tmp_path / "run.lock"
    with exclusive_lock(path):
        with pytest.raises(LockUnavailable):
            with exclusive_lock(path, blocking=False):
                pass


def test_lock_is_reusable_after_release(tmp_path):
    path = tmp_path / "run.lock"
    with exclusive_lock(path, blocking=False):
        pass
    with exclusive_lock(path, blocking=False):
        pass


def test_blocking_acquire_waits_for_the_holder(tmp_path):
    path = tmp_path / "run.lock"
    acquired = threading.Event()

    def _waiter() -> None:
        with exclusive_lock(path, blocking=True):
            acquired.set()

    with exclusive_lock(path):
        waiter = threading.Thread(target=_waiter)
        waiter.start()
        # Still held here, so the thread must not have gotten through.
        assert not acquired.wait(timeout=0.2)
    waiter.join(timeout=5)
    assert acquired.is_set()


def test_is_locked_reports_holder_state(tmp_path):
    path = tmp_path / "run.lock"
    assert is_locked(path) is False  # absent
    assert not path.exists()  # ... and the probe did not create it
    with exclusive_lock(path):
        assert is_locked(path) is True
    assert is_locked(path) is False


def test_lake_mutation_lock_shares_compact_lock_with_run_lock(tmp_path):
    """Maintenance must contend with the orchestrator's compact lock."""
    from cn_market_lake.orchestrator.run_lock import RunLockError, run_lock

    with lake_mutation_lock(tmp_path):
        with pytest.raises(RunLockError):
            with run_lock(tmp_path, "compact", blocking=False):
                pass


def _hold_lock(path: str, seconds: float) -> bool:
    """Child process: take the lock, hold it, report that it got it."""
    with exclusive_lock(Path(path)):
        time.sleep(seconds)
    return True


def test_lock_is_exclusive_across_processes(tmp_path):
    path = tmp_path / "run.lock"
    with ProcessPoolExecutor(max_workers=1) as pool:
        future = pool.submit(_hold_lock, str(path), 0.5)
        # Give the child time to actually take it before probing.
        deadline = time.monotonic() + 5
        while not is_locked(path) and time.monotonic() < deadline:
            time.sleep(0.02)
        assert is_locked(path) is True
        with pytest.raises(LockUnavailable):
            with exclusive_lock(path, blocking=False):
                pass
        assert future.result(timeout=10) is True
    assert is_locked(path) is False


# ------------------------------------------------------------- Windows branch


class FakeMsvcrt:
    """Enough of ``msvcrt`` to drive ``_acquire_windows`` off Windows."""

    LK_NBLCK = 2
    LK_UNLCK = 0

    def __init__(self, *, busy: int = 0, error: int = errno.EACCES):
        self.calls: list[tuple[int, int, int]] = []
        self.busy = busy
        self.error = error

    def locking(self, fd: int, mode: int, nbytes: int) -> None:
        self.calls.append((fd, mode, nbytes))
        if mode == self.LK_NBLCK and self.busy > 0:
            self.busy -= 1
            raise OSError(self.error, "lock violation")


@pytest.fixture
def win_handle(tmp_path):
    # "a+" is what exclusive_lock uses; seeded so the position starts past 0.
    path = tmp_path / "run.lock"
    path.write_text("stale", encoding="utf-8")
    with open(path, "a+", encoding="utf-8") as handle:
        yield handle


def _install(monkeypatch, name: str, module) -> None:
    monkeypatch.setitem(sys.modules, name, module)


def test_windows_acquire_locks_one_byte_at_offset_zero(monkeypatch, win_handle):
    fake = FakeMsvcrt()
    _install(monkeypatch, "msvcrt", fake)

    assert win_handle.tell() != 0  # opened at EOF
    file_lock._acquire_windows(win_handle, blocking=False)

    assert win_handle.tell() == 0
    assert fake.calls == [(win_handle.fileno(), FakeMsvcrt.LK_NBLCK, 1)]


def test_windows_release_unlocks_the_same_byte(monkeypatch, win_handle):
    fake = FakeMsvcrt()
    _install(monkeypatch, "msvcrt", fake)

    file_lock._release_windows(win_handle)

    assert win_handle.tell() == 0
    assert fake.calls == [(win_handle.fileno(), FakeMsvcrt.LK_UNLCK, 1)]


def test_windows_non_blocking_contention_raises_lock_unavailable(monkeypatch, win_handle):
    _install(monkeypatch, "msvcrt", FakeMsvcrt(busy=1))

    with pytest.raises(LockUnavailable):
        file_lock._acquire_windows(win_handle, blocking=False)


@pytest.mark.parametrize("code", sorted(file_lock._WIN_BUSY_ERRNOS))
def test_windows_blocking_retries_every_busy_errno(monkeypatch, win_handle, code):
    fake = FakeMsvcrt(busy=3, error=code)
    _install(monkeypatch, "msvcrt", fake)
    sleeps: list[float] = []
    monkeypatch.setattr(file_lock.time, "sleep", sleeps.append)

    file_lock._acquire_windows(win_handle, blocking=True)

    assert len(fake.calls) == 4  # three refusals, then the grant
    assert sleeps == [file_lock._WIN_RETRY_INTERVAL] * 3


def test_windows_unexpected_oserror_is_not_swallowed(monkeypatch, win_handle):
    _install(monkeypatch, "msvcrt", FakeMsvcrt(busy=1, error=errno.EBADF))
    monkeypatch.setattr(file_lock.time, "sleep", lambda _: pytest.fail("must not retry EBADF"))

    with pytest.raises(OSError) as excinfo:
        file_lock._acquire_windows(win_handle, blocking=True)
    assert excinfo.value.errno == errno.EBADF
    assert not isinstance(excinfo.value, LockUnavailable)


# --------------------------------------------------------------- POSIX branch


class FakeFcntl:
    LOCK_EX = 2
    LOCK_NB = 4
    LOCK_UN = 8

    def __init__(self, *, raises: BaseException | None = None):
        self.calls: list[int] = []
        self.raises = raises

    def flock(self, handle, flags: int) -> None:
        self.calls.append(flags)
        if self.raises is not None:
            raise self.raises


def test_posix_blocking_acquire_uses_plain_lock_ex(monkeypatch, win_handle):
    fake = FakeFcntl()
    _install(monkeypatch, "fcntl", fake)

    file_lock._acquire_posix(win_handle, blocking=True)

    assert fake.calls == [FakeFcntl.LOCK_EX]


def test_posix_non_blocking_acquire_adds_lock_nb(monkeypatch, win_handle):
    fake = FakeFcntl()
    _install(monkeypatch, "fcntl", fake)

    file_lock._acquire_posix(win_handle, blocking=False)

    assert fake.calls == [FakeFcntl.LOCK_EX | FakeFcntl.LOCK_NB]


@pytest.mark.parametrize("exc", [BlockingIOError(), PermissionError()])
def test_posix_contention_raises_lock_unavailable(monkeypatch, win_handle, exc):
    _install(monkeypatch, "fcntl", FakeFcntl(raises=exc))

    with pytest.raises(LockUnavailable):
        file_lock._acquire_posix(win_handle, blocking=False)


def test_posix_blocking_acquire_propagates_real_errors(monkeypatch, win_handle):
    _install(monkeypatch, "fcntl", FakeFcntl(raises=OSError(errno.EBADF, "bad fd")))

    with pytest.raises(OSError) as excinfo:
        file_lock._acquire_posix(win_handle, blocking=True)
    assert excinfo.value.errno == errno.EBADF


def test_posix_release_unlocks(monkeypatch, win_handle):
    fake = FakeFcntl()
    _install(monkeypatch, "fcntl", fake)

    file_lock._release_posix(win_handle)

    assert fake.calls == [FakeFcntl.LOCK_UN]


def test_selected_backend_matches_platform():
    if sys.platform == "win32":
        assert file_lock._acquire is file_lock._acquire_windows
        assert file_lock._release is file_lock._release_windows
    else:
        assert file_lock._acquire is file_lock._acquire_posix
        assert file_lock._release is file_lock._release_posix


def test_is_locked_treats_open_oserror_as_held(tmp_path, monkeypatch):
    path = tmp_path / "run.lock"
    path.write_text("", encoding="utf-8")

    def boom(*_a, **_k):
        raise OSError(errno.EACCES, "sharing violation")

    monkeypatch.setattr(file_lock, "exclusive_lock", boom)
    assert is_locked(path) is True


# --- run_lock messages -------------------------------------------------------
# Every scheduled daily group shares one non-blocking lock, so a collision means
# the previous group overran and this one is being skipped for the day. Naming
# the lock alone sent people hunting for a stuck process instead.


def test_daily_group_collision_explains_the_skip(tmp_path):
    import pytest

    from cn_market_lake.orchestrator.run_lock import (
        DAILY_INGESTION_LOCK,
        RunLockError,
        run_lock,
    )

    with run_lock(tmp_path, DAILY_INGESTION_LOCK):
        with pytest.raises(RunLockError) as exc:
            with run_lock(tmp_path, DAILY_INGESTION_LOCK):
                pass
    message = str(exc.value)
    assert "Another daily group is still running" in message
    assert "skipped" in message
    assert "[job.daily.groups]" in message


def test_other_locks_keep_the_generic_message(tmp_path):
    import pytest

    from cn_market_lake.orchestrator.run_lock import RunLockError, run_lock

    with run_lock(tmp_path, "some-run-id"):
        with pytest.raises(RunLockError, match="locked by another process"):
            with run_lock(tmp_path, "some-run-id"):
                pass
