"""Exclusive file lock per ingestion run (prevents concurrent retry)."""

from __future__ import annotations

import contextlib
import time
from collections.abc import Iterator
from pathlib import Path

from cnequity.file_lock import (
    DEFAULT_LOCK_WAIT_SECONDS,
    LockUnavailable,
    exclusive_lock,
    is_locked,
)


class RunLockError(RuntimeError):
    """Another process holds the run lock."""


# Shared by every scheduled `daily*` group, so only one can ingest at a time.
DAILY_INGESTION_LOCK = "daily_ingestion"

# The same contract for the continuous event streams, and deliberately a
# *different* lock: those feeds publish around the clock and must not queue
# behind — or be skipped because of — a heavy evening batch. The two jobs write
# disjoint datasets (enforced in `validate_config`), so letting them overlap
# costs nothing but bandwidth.
EVENTS_INGESTION_LOCK = "events_ingestion"

# Held for the whole of `cne init`, and the reason it is a fixed name rather
# than the run id: the question it answers is "is an init running *now*", asked
# by a different process that does not yet know which run it would be joining.
# A killed init releases it the moment the kernel reaps the process, which is
# what lets a retry tell "someone else is initialising" from "my last attempt
# died" — the two cases that used to produce the same refusal.
INIT_JOB_LOCK = "init_job"


def lock_path(meta_root: Path, run_id: str) -> Path:
    return meta_root / "locks" / f"{run_id}.lock"


def is_run_locked(meta_root: Path, run_id: str) -> bool:
    """True when another process currently holds ``run_lock`` for *run_id*."""
    return is_locked(lock_path(meta_root, run_id))


@contextlib.contextmanager
def run_lock(
    meta_root: Path,
    run_id: str,
    *,
    blocking: bool = False,
    timeout: float | None = DEFAULT_LOCK_WAIT_SECONDS,
) -> Iterator[None]:
    """Exclusive lock scoped to *run_id* (or a global name like ``compact``).

    Non-blocking by default (retry contention should fail loud); pass
    ``blocking=True`` to queue instead — e.g. overlapping runs serializing
    their compact step.

    A blocking wait is bounded. A holder that crashed releases its lock as the
    kernel reaps it, so the only thing that can still be holding one an hour
    later is a process that is alive and stuck — and waiting on that forever,
    silently, parks every command behind it with nothing to look at.
    """
    path = lock_path(meta_root, run_id)
    started = time.monotonic()
    with contextlib.ExitStack() as stack:
        try:
            stack.enter_context(exclusive_lock(path, blocking=blocking, timeout=timeout))
        except LockUnavailable as exc:
            waited = time.monotonic() - started
            if blocking and timeout is not None and waited >= timeout:
                raise RunLockError(_lock_stalled_message(run_id, path, waited)) from exc
            raise RunLockError(_lock_busy_message(run_id)) from exc
        yield


def _lock_stalled_message(run_id: str, path: Path, waited: float) -> str:
    """A bounded wait that ran out says so, and names what to look at."""
    return (
        f"Gave up waiting for the {run_id} lock after {int(waited)}s. "
        f"A crashed holder would have released it immediately, so {path} is held "
        "by a process that is alive and stuck. Find it with "
        f"`lsof {path}` (or `fuser {path}`), then stop it or wait for it; "
        "`cne status` shows what that run was doing."
    )


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
            "group overran its start-time gap: check `cne status`, then widen the "
            "spacing in [job.daily.groups] so the slowest group fits its window."
        )
    if run_id == EVENTS_INGESTION_LOCK:
        return (
            "Another events group is still running (they share one ingestion lock, "
            "and this one does not queue — it is skipped). Event sweeps are cheap "
            "and frequent, so a skipped one costs nothing: the next tick re-reads "
            "the same window. Check `cne status` if they are skipping every time."
        )
    return f"Run {run_id} is locked by another process; wait for it to finish before retrying."
