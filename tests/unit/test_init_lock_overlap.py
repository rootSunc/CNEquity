"""Init vs. scheduled ingestion: the direction of the exclusion nobody tested.

``refuse_overlap_with_init`` (a daily job refusing while init runs) is covered
in ``test_retry_hardening``. The reverse, ``refuse_init_overlap`` (init refusing
while a daily group or event sweep holds its ingestion lock), has no test, and
neither does the guarantee that a refused init releases its own ``init_job``
lock so the next attempt is not blocked by a ghost.

A thread is enough to play the other holder: ``flock`` conflicts between open
file descriptions (not processes), and ``held_here`` is thread-local, so the
main thread sees the lock as held elsewhere exactly as another process would.
"""

from __future__ import annotations

import threading
from contextlib import ExitStack

import pytest

from cnequity.orchestrator.run_lock import (
    DAILY_INGESTION_LOCK,
    EVENTS_INGESTION_LOCK,
    INIT_JOB_LOCK,
    RunLockError,
    init_job_lock,
    is_run_locked,
    run_lock,
)


class _Holder:
    """Hold run locks on a background thread until released."""

    def __init__(self, meta_root, *names: str):
        self._meta_root = meta_root
        self._names = names
        self._held = threading.Event()
        self._release = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        with ExitStack() as stack:
            for name in self._names:
                stack.enter_context(run_lock(self._meta_root, name))
            self._held.set()
            self._release.wait(30)

    def __enter__(self):
        self._thread.start()
        assert self._held.wait(10), "holder thread never acquired its locks"
        return self

    def __exit__(self, *exc):
        self._release.set()
        self._thread.join(10)


@pytest.mark.parametrize(
    ("held", "wording"),
    [
        ((DAILY_INGESTION_LOCK,), "a daily update"),
        ((EVENTS_INGESTION_LOCK,), "an event sweep"),
        ((DAILY_INGESTION_LOCK, EVENTS_INGESTION_LOCK), "a daily update and an event sweep"),
    ],
)
def test_init_refuses_to_start_beside_scheduled_ingestion(tmp_path, held, wording):
    meta = tmp_path / "meta"
    with _Holder(meta, *held):
        with pytest.raises(RunLockError, match=f"Not starting init while {wording} is running"):
            with init_job_lock(meta):
                pytest.fail("init entered its body while scheduled ingestion held the lake")
        # The refusal must not leave init's own lock behind.
        assert not is_run_locked(meta, INIT_JOB_LOCK)


def test_init_proceeds_once_the_daily_group_finishes(tmp_path):
    meta = tmp_path / "meta"
    with _Holder(meta, DAILY_INGESTION_LOCK):
        with pytest.raises(RunLockError):
            with init_job_lock(meta):
                pass
    entered = False
    with init_job_lock(meta):
        entered = True
    assert entered
    assert not is_run_locked(meta, INIT_JOB_LOCK)


def test_init_job_lock_is_reentrant_for_the_holding_thread(tmp_path):
    """`cne init --resume` takes the lock and then resumes, which takes it again."""
    meta = tmp_path / "meta"
    with init_job_lock(meta):
        with init_job_lock(meta):
            pass
