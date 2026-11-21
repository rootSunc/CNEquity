"""A lake has one writer lock, and every writable catalog connection holds it."""

from __future__ import annotations

import threading

import pytest

from equitylake_core.catalog import Catalog, CatalogError
from equitylake_core.lock import LockTimeout, holds_lake_lock, lake_lock


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
