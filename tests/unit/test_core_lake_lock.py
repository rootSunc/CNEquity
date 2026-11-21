"""A lake has one writer lock, and every writable catalog connection holds it."""

from __future__ import annotations

import ctypes
import errno
import os
import threading

import pytest

from equitylake_core import lock
from equitylake_core.catalog import Catalog, CatalogError
from equitylake_core.lock import LockIOError, LockTimeout, holds_lake_lock, lake_lock


def test_a_writable_catalog_holds_the_lake_lock_until_it_closes(tmp_path):
    lake = tmp_path / "lake"
    with Catalog(lake, create=True):
        assert holds_lake_lock(lake)
    assert not holds_lake_lock(lake)
    with Catalog(lake, read_only=True):
        assert not holds_lake_lock(lake)


def test_a_failed_open_releases_the_lock(tmp_path):
    lake = tmp_path / "lake"
    with pytest.raises(CatalogError):
        Catalog(lake)
    assert not holds_lake_lock(lake)


def test_native_backend_releases_and_reacquires_an_empty_lock_file(tmp_path):
    path = tmp_path / "empty.lock"
    path.touch()
    with lock.file_lock(path, timeout=0):
        with pytest.raises(LockTimeout), lock.file_lock(path, timeout=0):
            pytest.fail("a second handle acquired the already held byte range")
    with lock.file_lock(path, timeout=0):
        assert path.stat().st_size == 0


def test_a_writer_closing_out_of_order_leaves_the_other_writer_protected(tmp_path):
    lake = tmp_path / "lake"
    first = Catalog(lake, create=True)
    with Catalog(lake):
        first.close()
        assert holds_lake_lock(lake)
        with pytest.raises(LockTimeout):
            with lock.file_lock(lock.lake_lock_path(lake), timeout=0):
                pytest.fail("a writable catalog still owns the lock")
    assert not holds_lake_lock(lake)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires fork")
@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
def test_a_forked_child_must_acquire_its_own_lake_lock(tmp_path):
    with lake_lock(tmp_path):
        pid = os.fork()
        if pid == 0:
            try:
                with lake_lock(tmp_path, timeout=0):
                    os._exit(1)
            except LockTimeout:
                os._exit(0)
            except BaseException:
                os._exit(2)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0


def test_maintenance_excludes_operations_and_can_open_its_own_writer(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    root = tmp_path / "lake"
    with Catalog(root, create=True):
        pass
    with lock.lake_read(root):
        with pytest.raises(LockTimeout), lock.lake_maintenance(root):
            pytest.fail("maintenance must wait for managed reads")
    with lock.lake_maintenance(root), Catalog(root):
        assert holds_lake_lock(root)
        with ThreadPoolExecutor(max_workers=1) as pool:
            writer = pool.submit(Catalog, root, lock_timeout=0)
            with pytest.raises(LockTimeout, match="maintenance.lock"):
                writer.result(timeout=4)


@pytest.mark.parametrize("winerror", [5, 33])
def test_only_windows_lock_violation_is_treated_as_contention(tmp_path, monkeypatch, winerror):
    monkeypatch.setattr(lock, "_windows_range", lambda *args, **kwargs: winerror)
    monkeypatch.setattr(lock, "_try_lock", lock._try_windows)

    def win_error(code):
        error = PermissionError(errno.EACCES, "access denied")
        error.winerror = code
        return error

    monkeypatch.setattr(ctypes, "WinError", win_error, raising=False)
    expected = LockTimeout if winerror == 33 else LockIOError
    with pytest.raises(expected) as error, lock.file_lock(tmp_path / "empty.lock", timeout=0):
        pytest.fail("the Windows mock refused acquisition")
    if isinstance(error.value, LockIOError):
        assert error.value.winerror == winerror
        assert error.value.filename == str(tmp_path / "empty.lock")


def test_the_lock_is_reentrant_only_for_the_thread_that_holds_it(tmp_path):
    lake = tmp_path / "lake"
    with lake_lock(lake) as took:
        assert took
        with lake_lock(lake) as again:
            assert not again
        failed = []

        def other():
            try:
                with lake_lock(lake, timeout=0.2):
                    pass
            except LockTimeout:
                failed.append(True)

        thread = threading.Thread(target=other)
        thread.start()
        thread.join(10)
        assert failed == [True]
    assert not holds_lake_lock(lake)


def test_a_busy_lock_is_announced_once_before_the_wait(tmp_path):
    lake = tmp_path / "lake"
    release = threading.Event()
    taken = threading.Event()

    def hold():
        with lake_lock(lake):
            taken.set()
            release.wait(10)

    thread = threading.Thread(target=hold)
    thread.start()
    assert taken.wait(10)
    waits = []
    try:
        with pytest.raises(LockTimeout):
            with lake_lock(lake, timeout=0.2, on_wait=waits.append):
                pass
    finally:
        release.set()
        thread.join(10)
    assert len(waits) == 1
