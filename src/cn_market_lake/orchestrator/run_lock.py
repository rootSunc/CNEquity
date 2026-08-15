"""Exclusive file lock per ingestion run (prevents concurrent retry)."""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from pathlib import Path

from cn_market_lake.file_lock import LockUnavailable, exclusive_lock, is_locked


class RunLockError(RuntimeError):
    """Another process holds the run lock."""


# Shared by every scheduled `daily*` group, so only one can ingest at a time.
DAILY_INGESTION_LOCK = "daily_ingestion"


def lock_path(meta_root: Path, run_id: str) -> Path:
    return meta_root / "locks" / f"{run_id}.lock"


def is_run_locked(meta_root: Path, run_id: str) -> bool:
    """True when another process currently holds ``run_lock`` for *run_id*."""
    return is_locked(lock_path(meta_root, run_id))


@contextlib.contextmanager
def run_lock(meta_root: Path, run_id: str, *, blocking: bool = False) -> Iterator[None]:
    """Exclusive lock scoped to *run_id* (or a global name like ``compact``).

    Non-blocking by default (retry contention should fail loud); pass
    ``blocking=True`` to queue instead — e.g. overlapping runs serializing
    their compact step.
    """
    path = lock_path(meta_root, run_id)
    with contextlib.ExitStack() as stack:
        try:
            stack.enter_context(exclusive_lock(path, blocking=blocking))
        except LockUnavailable as exc:
            raise RunLockError(_lock_busy_message(run_id)) from exc
        yield


def _lock_busy_message(run_id: str) -> str:
    if run_id == DAILY_INGESTION_LOCK:
        # Every scheduled group shares this one lock, so the realistic cause is
        # a previous group still running — not "another operator". Naming the
        # lock alone sent people looking for a stuck process when the answer
        # was that `core` had simply overrun its slot and this group was about
        # to be skipped for the day.
        return (
            "Another daily group is still running (they share one ingestion lock, "
            "and this one does not queue — it is skipped). Usually the previous "
            "group overran its start-time gap: check `cml status`, then widen the "
            "spacing in [job.daily.groups] so the slowest group fits its window."
        )
    return f"Run {run_id} is locked by another process; wait for it to finish before retrying."
