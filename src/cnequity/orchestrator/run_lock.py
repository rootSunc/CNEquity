"""Exclusive file lock per ingestion run (prevents concurrent retry)."""

from __future__ import annotations

import contextlib
import threading
import time
from collections.abc import Iterator
from pathlib import Path

from cnequity.file_lock import (
    DEFAULT_LOCK_WAIT_SECONDS,
    LockUnavailable,
    _held_count,
    describe_holder,
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


# Which ingestion jobs an init may not overlap, in either direction. Init
# builds the lake's spine — instruments, calendar, the full bar history — and
# a daily group or event sweep started alongside it writes against a lake that
# does not exist yet while doubling the load on the same sources. Daily and
# event jobs still overlap each other (disjoint datasets, see above), and a
# backfill does not hold the daily job off: a skipped day loses its snapshot
# datasets for good, which costs more than sharing the sources for a while.
INIT_EXCLUDES = (DAILY_INGESTION_LOCK, EVENTS_INGESTION_LOCK)


def held_here(meta_root: Path, name: str) -> bool:
    """True when this thread already holds *name*'s lock."""
    return bool(_held_count(lock_path(meta_root, name)))


def held_elsewhere(meta_root: Path, name: str) -> bool:
    """True when *name*'s lock is held, but not by this thread."""
    return not held_here(meta_root, name) and is_run_locked(meta_root, name)


@contextlib.contextmanager
def init_job_lock(meta_root: Path) -> Iterator[None]:
    """Hold ``init_job`` for one init, refusing to start beside daily or event jobs.

    Re-entrant per thread: `cne init --resume` takes it and then resumes, while
    `cne run retry --run-id <init>` resumes directly and used to take nothing,
    so a retried init could run beside a second `cne init` or a daily update.
    """
    if held_here(meta_root, INIT_JOB_LOCK):
        yield
        return
    with run_lock(meta_root, INIT_JOB_LOCK, blocking=False):
        refuse_init_overlap(meta_root)
        yield


def refuse_overlap_with_init(meta_root: Path) -> None:
    """Raise when an init is running; for any ingestion job other than init."""
    if held_elsewhere(meta_root, INIT_JOB_LOCK):
        raise RunLockError(
            "An init is running on this lake. Ingestion waits until it finishes: "
            "`cne status` shows its progress, and `cne init` continues it if it stops."
        )


def refuse_init_overlap(meta_root: Path) -> None:
    """Raise when a daily or event job is running; for `cne init`."""
    busy = [name for name in INIT_EXCLUDES if held_elsewhere(meta_root, name)]
    if busy:
        jobs = " and ".join(
            "a daily update" if name == DAILY_INGESTION_LOCK else "an event sweep" for name in busy
        )
        raise RunLockError(
            f"Not starting init while {jobs} is running on this lake. "
            "Wait for it to finish, then run `cne init` again."
        )


def lock_path(meta_root: Path, run_id: str) -> Path:
    return meta_root / "locks" / f"{run_id}.lock"


def is_run_locked(meta_root: Path, run_id: str) -> bool:
    """True when another process currently holds ``run_lock`` for *run_id*."""
    return is_locked(lock_path(meta_root, run_id))


#: Run locks this process holds, path -> owning thread. Process-wide on
#: purpose: the underlying file lock is, so a registry scoped to one
#: ``JobEngine`` instance would let a second instance ask for a lock this
#: process already holds and be told "another process holds it" — the one
#: explanation that is certainly wrong.
_HELD_RUN_LOCKS: dict[str, int] = {}
_HELD_RUN_LOCKS_GUARD = threading.Lock()


@contextlib.contextmanager
def reentrant_run_lock(meta_root: Path, run_id: str) -> Iterator[None]:
    """Hold *run_id*'s lock, or pass through if this thread already holds it.

    `cne init` holds one run lock across every phase so the orphan reconciler
    at the top of each phase can see that the run is alive. The phases nest
    ordinary ``run_lock`` acquisitions inside that, and re-acquiring a held
    lock would fail rather than queue.
    """
    path = str(lock_path(meta_root, run_id))
    owner = threading.get_ident()
    with _HELD_RUN_LOCKS_GUARD:
        held_by = _HELD_RUN_LOCKS.get(path)
    if held_by == owner:
        yield
        return
    if held_by is not None:
        raise RunLockError(
            f"Run {run_id} is locked by another thread in this process "
            f"(thread {held_by}). This is a bug in the caller, not contention: "
            "run locks are not shared between threads."
        )
    with run_lock(meta_root, run_id, blocking=False):
        with _HELD_RUN_LOCKS_GUARD:
            _HELD_RUN_LOCKS[path] = owner
        try:
            yield
        finally:
            with _HELD_RUN_LOCKS_GUARD:
                _HELD_RUN_LOCKS.pop(path, None)


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
            stack.enter_context(
                exclusive_lock(path, blocking=blocking, timeout=timeout, record_holder=True)
            )
        except LockUnavailable as exc:
            waited = time.monotonic() - started
            if blocking and timeout is not None and waited >= timeout:
                raise RunLockError(_lock_stalled_message(run_id, path, waited)) from exc
            message = _lock_busy_message(run_id)
            holder = describe_holder(path)
            if holder:
                message += f" Held by {holder}."
            raise RunLockError(message) from exc
        yield


def _lock_stalled_message(run_id: str, path: Path, waited: float) -> str:
    """A bounded wait that ran out says so, and names what to look at."""
    holder = describe_holder(path)
    held_by = f"It is held by {holder}. " if holder else ""
    return (
        f"Gave up waiting for the {run_id} lock after {int(waited)}s. {held_by}"
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
