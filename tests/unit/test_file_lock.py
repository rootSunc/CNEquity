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

from cnequity import file_lock
from cnequity.file_lock import LockUnavailable, exclusive_lock, is_locked, lake_mutation_lock

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


def test_posix_blocking_acquire_has_a_total_timeout(monkeypatch, win_handle):
    _install(monkeypatch, "fcntl", FakeFcntl(raises=BlockingIOError()))
    monkeypatch.setattr(file_lock.time, "monotonic", lambda: 1.0)
    monkeypatch.setattr(
        file_lock.time,
        "sleep",
        lambda _: pytest.fail("zero timeout must not sleep"),
    )

    with pytest.raises(LockUnavailable, match="timed out"):
        file_lock._acquire_posix(win_handle, blocking=True, timeout=0)


def test_is_locked_reports_holder_state(tmp_path):
    path = tmp_path / "run.lock"
    assert is_locked(path) is False  # absent
    assert not path.exists()  # ... and the probe did not create it
    with exclusive_lock(path):
        assert is_locked(path) is True
    assert is_locked(path) is False


def test_lake_mutation_lock_shares_compact_lock_with_run_lock(tmp_path):
    """Maintenance must contend with the orchestrator's compact lock."""
    from cnequity.orchestrator.run_lock import RunLockError, run_lock

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


def test_windows_blocking_acquire_has_a_total_timeout(monkeypatch, win_handle):
    _install(monkeypatch, "msvcrt", FakeMsvcrt(busy=1))
    monkeypatch.setattr(file_lock.time, "monotonic", lambda: 1.0)
    monkeypatch.setattr(
        file_lock.time,
        "sleep",
        lambda _: pytest.fail("zero timeout must not sleep"),
    )

    with pytest.raises(LockUnavailable, match="timed out"):
        file_lock._acquire_windows(win_handle, blocking=True, timeout=0)


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

    from cnequity.orchestrator.run_lock import (
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

    from cnequity.orchestrator.run_lock import RunLockError, run_lock

    with run_lock(tmp_path, "some-run-id"):
        with pytest.raises(RunLockError, match="locked by another process"):
            with run_lock(tmp_path, "some-run-id"):
                pass


def test_a_blocking_wait_announces_itself(tmp_path, caplog, monkeypatch):
    """A blocking acquire was indistinguishable from a hang: no output, no
    deadline, and the reason — someone else holds this — never said.

    The grace period is shortened here rather than slept through; what is
    asserted is that a wait outlasting it gets announced."""
    import logging

    from cnequity import file_lock as file_lock_mod

    monkeypatch.setattr(file_lock_mod, "QUIET_LOCK_WAIT_SECONDS", 0.05)
    path = tmp_path / "compact.lock"
    with exclusive_lock(path, blocking=False):
        pass  # create the file so the second acquire has something to open

    holder_done = threading.Event()
    acquired = threading.Event()

    def _hold():
        with exclusive_lock(path, blocking=False):
            acquired.set()
            holder_done.wait(10)

    thread = threading.Thread(target=_hold)
    thread.start()
    try:
        assert acquired.wait(10)
        with caplog.at_level(logging.INFO, logger="cnequity.file_lock"):
            with pytest.raises(LockUnavailable):
                with exclusive_lock(path, blocking=True, timeout=0.5):
                    pass
        assert any("waiting for compact.lock" in r.message for r in caplog.records)
    finally:
        holder_done.set()
        thread.join(10)


def test_an_uncontended_blocking_acquire_says_nothing(tmp_path, caplog):
    """The announcement is for a queue, not for every lock in the lake."""
    import logging

    with caplog.at_level(logging.INFO, logger="cnequity.file_lock"):
        with exclusive_lock(tmp_path / "quiet.lock", blocking=True):
            pass
    assert not [r for r in caplog.records if "waiting for" in r.message]


def test_a_nested_acquire_still_fails_fast_rather_than_queueing(tmp_path):
    """The re-entrancy guard must not turn into a self-inflicted timeout."""
    path = tmp_path / "nested.lock"
    with exclusive_lock(path, blocking=False):
        started = time.monotonic()
        with pytest.raises(LockUnavailable):
            with exclusive_lock(path, blocking=True, timeout=30):
                pass
        assert time.monotonic() - started < 5


def test_the_lake_mutation_lock_wait_is_bounded_by_default():
    """An unbounded wait is the thing being removed; the default must be a
    number, not None."""
    import inspect

    from cnequity.file_lock import DEFAULT_LOCK_WAIT_SECONDS

    signature = inspect.signature(lake_mutation_lock)
    assert signature.parameters["timeout"].default == DEFAULT_LOCK_WAIT_SECONDS
    assert DEFAULT_LOCK_WAIT_SECONDS > 0


def test_a_brief_queue_is_waited_out_in_silence(tmp_path, caplog, monkeypatch):
    """Routine contention is not a wait anyone is wondering about.

    Several of these locks *are* the per-request rate limiter, taken and
    released around every single request, so announcing on first contention put
    one line in the log per request: 47 lines in the first 4.5 minutes of
    `ths-official resource-sectors`, not one of them progress. That buries the
    progress the logging exists to surface.
    """
    import logging

    from cnequity import file_lock as file_lock_mod

    monkeypatch.setattr(file_lock_mod, "QUIET_LOCK_WAIT_SECONDS", 5.0)
    path = tmp_path / "concurrency-ths.lock"
    with exclusive_lock(path, blocking=False):
        pass

    # `is_locked` answers by taking the lock itself. Polling it while the
    # holder does a non-blocking acquire races: the probe wins, the holder
    # dies with LockUnavailable, and this assertion sees an empty stamp.
    # The holder instead keeps the lock until this thread has entered the
    # blocking acquire, then stamps on the way out. A queued acquire cannot
    # finish before that stamp.
    released_at: list[float] = []
    acquired_at: list[float] = []
    holding = threading.Event()
    entered_wait = threading.Event()
    saw_contention: list[bool] = []
    waiter = threading.current_thread()
    real_acquire = file_lock_mod._acquire

    def _acquire(handle, *, blocking, timeout=None):
        if threading.current_thread() is not waiter:
            return real_acquire(handle, blocking=blocking, timeout=timeout)
        if not blocking:
            try:
                return real_acquire(handle, blocking=False, timeout=timeout)
            except LockUnavailable:
                saw_contention.append(True)
                raise
        entered_wait.set()
        return real_acquire(handle, blocking=blocking, timeout=timeout)

    monkeypatch.setattr(file_lock_mod, "_acquire", _acquire)

    def _hold():
        with exclusive_lock(path, blocking=True, timeout=5):
            holding.set()
            if not entered_wait.wait(5):
                return
            time.sleep(0.15)
            released_at.append(time.monotonic())

    thread = threading.Thread(target=_hold)
    thread.start()
    try:
        assert holding.wait(5), "holder never acquired the lock"
        with caplog.at_level(logging.INFO, logger="cnequity.file_lock"):
            with exclusive_lock(path, blocking=True, timeout=30.0):
                acquired_at.append(time.monotonic())
    finally:
        thread.join(10)
    assert released_at and acquired_at
    assert saw_contention, "the acquire must have queued, not walked straight in"
    assert acquired_at[0] >= released_at[0]
    assert not [r for r in caplog.records if "waiting for" in r.message]


# ------------------------------------------------------------ holder notes
#
# A 1-hour wait on the real lake timed out and failed an events run, and no
# log could say which process had held compact.lock. Locks that record their
# holder leave a note beside the lock; waiters and timeouts name it.


def _hold_mutation_lock(meta: str, seconds: float) -> int:
    """Child process: hold the lake lock inside a step, report its pid."""
    import os

    from cnequity.progress import step_scope

    with step_scope("derive_option_greeks"), lake_mutation_lock(Path(meta)):
        time.sleep(seconds)
    return os.getpid()


def _wait_for_note(lock: Path, timeout: float = 10.0) -> None:
    note = file_lock.holder_path(lock)
    deadline = time.monotonic() + timeout
    while not note.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert note.exists(), "holder never wrote its note"


def test_the_lake_lock_keeps_a_holder_note_only_while_held(tmp_path):
    import json
    import os

    from cnequity.progress import step_scope

    lock = tmp_path / file_lock.MUTATION_LOCK_RELATIVE
    note = file_lock.holder_path(lock)
    with step_scope("compact"), lake_mutation_lock(tmp_path):
        record = json.loads(note.read_text(encoding="utf-8"))
        assert record["pid"] == os.getpid()
        assert record["activity"] == "compact"
        assert record["command"]
        assert record["acquired_at"]
    assert not note.exists()
    # The stale-lock sweep globs ``*.lock``; a note must never match it.
    assert not note.match("*.lock")


def test_plain_locks_write_no_note(tmp_path):
    """Per-request rate-limit locks must not pay for a file write each time."""
    lock = tmp_path / "concurrency-ths.lock"
    with exclusive_lock(lock):
        assert not file_lock.holder_path(lock).exists()


def test_a_wait_names_the_thread_and_step_that_hold_the_lock(tmp_path, caplog, monkeypatch):
    import logging

    from cnequity.progress import step_scope

    monkeypatch.setattr(file_lock, "QUIET_LOCK_WAIT_SECONDS", 0.05)
    holding = threading.Event()
    release = threading.Event()

    def _hold():
        with step_scope("derive_industry_index"), lake_mutation_lock(tmp_path):
            holding.set()
            release.wait(10)

    thread = threading.Thread(target=_hold, name="lane-7")
    thread.start()
    try:
        assert holding.wait(10)
        with caplog.at_level(logging.INFO, logger="cnequity.file_lock"):
            with pytest.raises(LockUnavailable) as exc:
                with lake_mutation_lock(tmp_path, timeout=0.3):
                    pass
    finally:
        release.set()
        thread.join(10)
    waiting = [r.getMessage() for r in caplog.records if "waiting for" in r.getMessage()]
    assert waiting, "the wait was not announced"
    assert "another thread of this process (lane-7)" in waiting[0]
    assert "step derive_industry_index" in waiting[0]
    # The timeout error names the holder too, not just the lock path.
    assert "step derive_industry_index" in str(exc.value)
    assert "timed out acquiring lock" in str(exc.value)


def test_a_queued_acquire_reports_how_long_it_waited(tmp_path, caplog, monkeypatch):
    import logging

    monkeypatch.setattr(file_lock, "QUIET_LOCK_WAIT_SECONDS", 0.05)
    holding = threading.Event()

    def _hold():
        with lake_mutation_lock(tmp_path):
            holding.set()
            time.sleep(0.3)

    thread = threading.Thread(target=_hold)
    thread.start()
    assert holding.wait(10)
    with caplog.at_level(logging.INFO, logger="cnequity.file_lock"):
        with lake_mutation_lock(tmp_path, timeout=10):
            pass
    thread.join(10)
    assert any("acquired compact.lock after waiting" in r.getMessage() for r in caplog.records)


def test_another_process_is_named_by_pid_step_and_command(tmp_path):
    lock = tmp_path / file_lock.MUTATION_LOCK_RELATIVE
    with ProcessPoolExecutor(max_workers=1) as pool:
        future = pool.submit(_hold_mutation_lock, str(tmp_path), 1.0)
        _wait_for_note(lock)
        described = file_lock.describe_holder(lock)
        child = future.result(timeout=20)
    assert described is not None
    assert described.startswith(f"pid {child}")
    assert "step derive_option_greeks" in described
    assert "since " in described
    assert "`" in described  # the command line
    assert not file_lock.holder_path(lock).exists()


def test_a_note_left_by_a_dead_process_is_ignored(tmp_path):
    import json
    import socket
    import subprocess

    dead = subprocess.Popen([sys.executable, "-c", "pass"])
    dead.wait(10)
    lock = tmp_path / "compact.lock"
    file_lock.holder_path(lock).write_text(
        json.dumps({"pid": dead.pid, "host": socket.gethostname(), "command": "cne x"}),
        encoding="utf-8",
    )
    assert file_lock.read_holder(lock) is None
    assert file_lock.describe_holder(lock) is None


@pytest.mark.parametrize("content", ["", "{not json", "[]", '{"pid": "12"}'])
def test_an_unreadable_note_is_ignored(tmp_path, content):
    lock = tmp_path / "compact.lock"
    file_lock.holder_path(lock).write_text(content, encoding="utf-8")
    assert file_lock.describe_holder(lock) is None


def test_a_note_that_cannot_be_written_does_not_affect_the_lock(tmp_path, monkeypatch):
    real_write = Path.write_text

    def _fail(self, *args, **kwargs):
        if self.name.endswith(file_lock.HOLDER_SUFFIX):
            raise PermissionError("read-only")
        return real_write(self, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", _fail)
    with lake_mutation_lock(tmp_path):
        assert is_locked(tmp_path / file_lock.MUTATION_LOCK_RELATIVE)
    assert not file_lock.holder_path(tmp_path / file_lock.MUTATION_LOCK_RELATIVE).exists()


def test_a_busy_run_lock_names_its_holder(tmp_path):
    from cnequity.orchestrator.run_lock import RunLockError, run_lock
    from cnequity.progress import step_scope

    holding = threading.Event()
    release = threading.Event()

    def _hold():
        with step_scope("daily_bars"), run_lock(tmp_path, "some-run-id"):
            holding.set()
            release.wait(10)

    thread = threading.Thread(target=_hold, name="retry")
    thread.start()
    try:
        assert holding.wait(10)
        with pytest.raises(RunLockError) as exc:
            with run_lock(tmp_path, "some-run-id"):
                pass
    finally:
        release.set()
        thread.join(10)
    message = str(exc.value)
    assert "locked by another process" in message
    assert "Held by another thread of this process (retry), step daily_bars" in message


def test_a_stalled_run_lock_names_its_holder(tmp_path, monkeypatch):
    from cnequity.orchestrator import run_lock as run_lock_mod

    monkeypatch.setattr(file_lock, "QUIET_LOCK_WAIT_SECONDS", 0.05)
    holding = threading.Event()
    release = threading.Event()

    def _hold():
        from cnequity.progress import step_scope

        with step_scope("ths_official_snapshot"), lake_mutation_lock(tmp_path):
            holding.set()
            release.wait(10)

    thread = threading.Thread(target=_hold, name="snapshot")
    thread.start()
    try:
        assert holding.wait(10)
        with pytest.raises(run_lock_mod.RunLockError) as exc:
            with run_lock_mod.run_lock(tmp_path, "compact", blocking=True, timeout=0.3):
                pass
    finally:
        release.set()
        thread.join(10)
    message = str(exc.value)
    assert "Gave up waiting for the compact lock" in message
    assert "step ths_official_snapshot" in message


@pytest.mark.skipif(sys.platform == "win32", reason="Windows cannot unlink an open lock file")
def test_a_waiter_on_an_unlinked_lock_file_does_not_hold_the_lock(tmp_path):
    """The stale-lock sweep unlinks a free lock file under its own lock.

    A process queued on that file gets the lock on a nameless inode once the
    sweep lets go, while the next opener creates a fresh file and locks that:
    two holders. The waiter must re-open the path instead.
    """
    path = tmp_path / "locks" / "compact.lock"
    waiter_holds = threading.Event()
    release_waiter = threading.Event()

    def waiter():
        with exclusive_lock(path, blocking=True, timeout=10):
            waiter_holds.set()
            release_waiter.wait(10)

    with exclusive_lock(path, blocking=False):
        thread = threading.Thread(target=waiter, daemon=True)
        thread.start()
        time.sleep(0.2)  # the waiter has opened the old file and is queued on it
        path.unlink()
    # The waiter now owns the lock of record; nobody else may take it.
    assert waiter_holds.wait(5)
    with pytest.raises(LockUnavailable):
        with exclusive_lock(path, blocking=False):
            pass
    release_waiter.set()
    thread.join(5)
