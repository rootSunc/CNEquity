"""Remove compacted or stale staging run directories."""

from __future__ import annotations

import shutil
from contextlib import nullcontext, suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from cnequity.config import Config
from cnequity.domain.datasets import DATASETS
from cnequity.file_lock import (
    IS_WINDOWS,
    LockUnavailable,
    exclusive_lock,
    holder_path,
    is_locked,
)
from cnequity.orchestrator.manifest import Manifest
from cnequity.storage.lifecycle.resources import resource_holds
from cnequity.storage.observation import STAGED_OBSERVATION_FIELD
from cnequity.storage.state import StateStore

# State fields keyed by run id that hold "validated in staging, not yet in a
# revision" evidence. Compact pops a run's entry when it publishes that run's
# staging in full; nothing else did, so a run whose staging was removed
# without a full publish left its entry behind for good.
_RUN_KEYED_STATE_FIELDS = (
    "staged_request_days",
    "staged_units",
    "staged_resume_units",
    STAGED_OBSERVATION_FIELD,
)


@dataclass
class StagingCleanupResult:
    removed_run_ids: list[str]
    orphan_run_ids: list[str]
    bytes_freed: int
    skipped_run_ids: list[str]
    force_removed_run_ids: list[str] = None  # type: ignore[assignment]
    skipped_reasons: dict[str, str] = field(default_factory=dict)
    inventory: list[dict] = field(default_factory=list)
    # dataset -> run ids whose staged-ledger entries were (or, in a dry run,
    # would be) dropped because their staging no longer exists.
    pruned_ledgers: dict[str, list[str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.force_removed_run_ids is None:
            self.force_removed_run_ids = []


def prune_stale_staged_ledgers(
    config: Config,
    *,
    manifest: Manifest | None = None,
    dry_run: bool = False,
) -> dict[str, list[str]]:
    """Drop run-keyed staged-ledger entries whose staging is gone.

    ``staged_request_days`` and its siblings record what a run validated into
    staging, so a full compact of that staging can mark those days covered. An
    entry is only meaningful while the staging it describes exists: once a run
    directory is deleted (``cne run clean``, ``--force``, a lifecycle staging
    purge), no compact can ever publish it, and the entry is dead weight that
    every later state read and write of the dataset re-serialises.

    An entry is kept while its run is still ``running`` — a step may record
    the day before its staging write lands. Dropping an entry never marks a
    day covered: the matching ``missing_ranges`` row stays, so the worst
    outcome of a wrong call here is a refetch, not a false claim of coverage.
    """
    state_dir = config.meta_root / "state"
    if not state_dir.is_dir():
        return {}
    manifest = manifest or Manifest(config.manifest_path)
    store = StateStore(config.meta_root)
    pruned: dict[str, list[str]] = {}
    for path in sorted(state_dir.glob("*.json")):
        dataset = path.stem
        if dataset not in DATASETS:
            continue  # e.g. lake.json, the migration record
        payload = store.get_payload(dataset)
        candidates = {
            run_id
            for name in _RUN_KEYED_STATE_FIELDS
            if isinstance(payload.get(name), dict)
            for run_id in payload[name]
        }
        stale: list[str] = []
        for run_id in sorted(candidates):
            if (config.staging_root / dataset / f"run_id={run_id}").is_dir():
                continue
            run = manifest.get_run(run_id)
            if run is not None and run["status"] == "running":
                continue
            stale.append(run_id)
        if not stale:
            continue
        pruned[dataset] = stale
        if dry_run:
            continue
        with store.transaction(dataset) as current:
            for name in _RUN_KEYED_STATE_FIELDS:
                entries = current.get(name)
                if not isinstance(entries, dict):
                    continue
                for run_id in stale:
                    entries.pop(run_id, None)
                if not entries:
                    current.pop(name, None)
    return pruned


def list_staging_run_ids(staging_root: Path) -> set[str]:
    if not staging_root.exists():
        return set()
    run_ids: set[str] = set()
    for dataset_dir in staging_root.iterdir():
        if not dataset_dir.is_dir():
            continue
        for run_dir in dataset_dir.iterdir():
            if run_dir.is_dir() and run_dir.name.startswith("run_id="):
                run_ids.add(run_dir.name.split("=", 1)[1])
    return run_ids


def staging_run_paths(staging_root: Path, run_id: str) -> list[Path]:
    if not staging_root.exists():
        return []
    paths: list[Path] = []
    for dataset_dir in staging_root.iterdir():
        if not dataset_dir.is_dir():
            continue
        run_dir = dataset_dir / f"run_id={run_id}"
        if run_dir.is_dir():
            paths.append(run_dir)
    return paths


def _dir_size(paths: list[Path]) -> int:
    return sum(f.stat().st_size for path in paths for f in path.rglob("*") if f.is_file())


def _delete_paths(paths: list[Path], *, dry_run: bool) -> int:
    size = _dir_size(paths)
    if not dry_run:
        for path in paths:
            shutil.rmtree(path)
    return size


def _staging_age_days(paths: list[Path], now: datetime) -> float:
    mtime = max(p.stat().st_mtime for p in paths)
    return (now.timestamp() - mtime) / 86400.0


def _has_successful_step(manifest: Manifest, run_id: str, step: str) -> bool:
    return any(
        b["dataset"] == step and b["status"] == "success"
        for b in manifest.get_batches_for_run(run_id)
    )


def run_ready_for_staging_cleanup(manifest: Manifest, run_id: str) -> bool:
    """True when staging is redundant after a recorded compact.

    Terminal runs (success / warning / degraded / failed) with no incomplete
    batches and a successful compact batch have already merged drained datasets
    into curated. Requiring status==success alone stranded large
    failed-but-compacted runs.
    Incomplete batches still block cleanup — compact may have skipped those
    datasets, leaving the only copy in staging.
    """
    run = manifest.get_run(run_id)
    if run is None:
        return False
    if run["status"] not in ("success", "warning", "degraded", "failed"):
        return False
    if manifest.incomplete_batch_count(run_id) > 0:
        return False
    return _has_successful_step(manifest, run_id, "compact")


def _run_age_days(run_row, now: datetime) -> float | None:
    anchor = run_row["finished_at"] or run_row["started_at"]
    if not anchor:
        return None
    return (now - datetime.fromisoformat(anchor)).total_seconds() / 86400.0


def clean_stale_lock_files(
    meta_root: Path,
    *,
    retention_days: int = 7,
    dry_run: bool = False,
) -> int:
    """Delete old run-lock files nobody holds (they accumulate one per run).

    A file is only removed after a non-blocking probe proves it free — a held
    lock (live retry/compact) is always skipped.
    """
    lock_dir = meta_root / "locks"
    if not lock_dir.exists():
        return 0
    cutoff = datetime.now(timezone.utc).timestamp() - retention_days * 86400
    removed = 0
    for path in lock_dir.glob("*.lock"):
        if path.stat().st_mtime > cutoff:
            continue
        if dry_run or IS_WINDOWS:
            if is_locked(path):
                continue
            if not dry_run:
                try:
                    # Windows refuses to delete an open file, so the lock
                    # cannot be held across the unlink there. A holder that
                    # reappears in this gap turns the unlink into an OSError.
                    path.unlink(missing_ok=True)
                except OSError:
                    continue
                with suppress(OSError):
                    holder_path(path).unlink(missing_ok=True)
        else:
            # POSIX unlinks under the lock: probing and then unlinking let a
            # process take the lock in between and keep it on the deleted
            # file while the next opener locked a fresh one — two holders of
            # the lake lock. A waiter queued on this file re-opens the path
            # once it gets the lock (see ``exclusive_lock``).
            try:
                with exclusive_lock(path, blocking=False):
                    path.unlink(missing_ok=True)
                    # A holder that crashed leaves its note behind; it goes
                    # with its lock.
                    with suppress(OSError):
                        holder_path(path).unlink(missing_ok=True)
            except (LockUnavailable, OSError):
                continue
        removed += 1
    return removed


def clean_staging(
    config: Config,
    *,
    dry_run: bool = False,
    orphan_retention_days: int = 7,
    force: bool = False,
) -> StagingCleanupResult:
    """Remove staging that is safe to delete.

    Safe means: the run is terminal and compact merged its staging into curated,
    or the staging belongs to no manifest run (orphan) and is old enough.

    Staging of failed/incomplete runs is resumable state — its successful
    batches may exist *only* in staging (compact was gated off). It is never
    deleted unless ``force=True``, and then the run's success batches are
    demoted to failed in the manifest so a later ``cne run retry`` refetches them
    instead of silently losing their rows.
    """
    with resource_holds(config.meta_root, dry_run=dry_run) as holds:
        return _clean_staging(
            config,
            dry_run=dry_run,
            orphan_retention_days=orphan_retention_days,
            force=force,
            holds=holds,
        )


def _clean_staging(
    config: Config, *, dry_run: bool, orphan_retention_days: int, force: bool, holds: set[str]
) -> StagingCleanupResult:
    manifest = Manifest(config.manifest_path)
    staging_root = config.staging_root
    now = datetime.now(timezone.utc)

    staging_run_ids = list_staging_run_ids(staging_root)

    removed: list[str] = []
    orphans: list[str] = []
    force_removed: list[str] = []
    skipped: list[str] = []
    bytes_freed = 0
    skipped_reasons: dict[str, str] = {}
    inventory: list[dict] = []

    for run_id in sorted(staging_run_ids):
        paths = staging_run_paths(staging_root, run_id)
        if not paths:
            continue
        run = manifest.get_run(run_id)
        entry = {
            "run_id": run_id,
            "status": run["status"] if run else "orphan",
            "bytes": _dir_size(paths),
            "incomplete_batches": manifest.incomplete_batch_count(run_id) if run else 0,
            "age_days": _run_age_days(run, now) if run else _staging_age_days(paths, now),
        }
        inventory.append(entry)
        lock_path = config.meta_root / "locks" / f"{run_id}.lock"
        try:
            # The executor holds the run lock until deletion finishes. Merely
            # probing then deleting lets a retry start in between those actions.
            if dry_run and is_locked(lock_path):
                raise LockUnavailable(str(lock_path))
            with nullcontext() if dry_run else exclusive_lock(lock_path, blocking=False):
                run = manifest.get_run(run_id)
                entry["status"] = run["status"] if run else "orphan"
                entry["incomplete_batches"] = manifest.incomplete_batch_count(run_id) if run else 0
                reason = None
                if f"staging/{run_id}" in holds:
                    reason = "lifecycle_hold"
                elif run is None:
                    if _staging_age_days(paths, now) < orphan_retention_days:
                        reason = "young_orphan"
                    else:
                        orphans.append(run_id)
                        entry["decision"] = "orphan_candidate"
                elif run_ready_for_staging_cleanup(manifest, run_id):
                    removed.append(run_id)
                    entry["decision"] = "compacted_candidate"
                elif force and run is not None and run["status"] != "running":
                    if not dry_run:
                        manifest.demote_success_batches(
                            run_id,
                            reason="staging evicted by cne run clean --force; refetch on retry",
                        )
                    force_removed.append(run_id)
                    entry["decision"] = "force_candidate"
                else:
                    reason = (
                        "running"
                        if run is not None and run["status"] == "running"
                        else "incomplete_batches"
                        if entry["incomplete_batches"]
                        else "not_compacted"
                    )
                if reason:
                    skipped.append(run_id)
                    skipped_reasons[run_id] = reason
                    entry["decision"] = reason
                else:
                    bytes_freed += _delete_paths(paths, dry_run=dry_run)
        except LockUnavailable:
            skipped.append(run_id)
            skipped_reasons[run_id] = "active_run_lock"
            entry["decision"] = "active_run_lock"

    clean_stale_lock_files(
        config.meta_root,
        retention_days=orphan_retention_days,
        dry_run=dry_run,
    )
    pruned_ledgers = prune_stale_staged_ledgers(config, manifest=manifest, dry_run=dry_run)

    return StagingCleanupResult(
        removed_run_ids=removed,
        orphan_run_ids=orphans,
        bytes_freed=bytes_freed,
        skipped_run_ids=skipped,
        force_removed_run_ids=force_removed,
        skipped_reasons=skipped_reasons,
        inventory=inventory,
        pruned_ledgers=pruned_ledgers,
    )


DEFAULT_LOG_RETENTION_DAYS = 30


@dataclass(frozen=True)
class LogCleanupResult:
    removed: list[str]
    kept: int
    bytes_freed: int


def clean_run_logs(
    data_root: Path,
    *,
    retention_days: int = DEFAULT_LOG_RETENTION_DAYS,
    dry_run: bool = False,
    now: datetime | None = None,
) -> LogCleanupResult:
    """Delete ``logs/cne-*.log`` older than *retention_days*.

    `attach_log_file` writes one timestamped file per invocation, and nothing
    removed them: a daily pipeline leaves a file per group per day, plus one per
    retry and backfill, for as long as the lake exists. They are pure
    diagnostics, so age is the only thing worth keeping them by.

    Only files this CLI names are considered — anything else under ``logs/``
    belongs to whoever put it there (launchd redirects its own stdout here).
    """
    with resource_holds(Path(data_root) / "meta", dry_run=dry_run) as holds:
        return _clean_run_logs(
            data_root, retention_days=retention_days, dry_run=dry_run, now=now, holds=holds
        )


def _clean_run_logs(
    data_root: Path, *, retention_days: int, dry_run: bool, now: datetime | None, holds: set[str]
) -> LogCleanupResult:
    log_dir = Path(data_root) / "logs"
    if retention_days <= 0 or not log_dir.is_dir():
        return LogCleanupResult(removed=[], kept=0, bytes_freed=0)

    moment = now or datetime.now()
    cutoff = moment.timestamp() - retention_days * 86400
    removed: list[str] = []
    freed = 0
    kept = 0
    for path in sorted(log_dir.glob("cne-*.log")):
        try:
            stat = path.stat()
        except OSError:
            continue
        if stat.st_mtime >= cutoff or f"log/{path.name}" in holds:
            kept += 1
            continue
        if not dry_run:
            path.unlink()
        removed.append(path.name)
        freed += stat.st_size
    return LogCleanupResult(removed=removed, kept=kept, bytes_freed=freed)
