"""Remove compacted or stale staging run directories."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from cn_market_lake.config import Config
from cn_market_lake.file_lock import is_locked
from cn_market_lake.orchestrator.manifest import Manifest


@dataclass
class StagingCleanupResult:
    removed_run_ids: list[str]
    orphan_run_ids: list[str]
    bytes_freed: int
    skipped_run_ids: list[str]
    force_removed_run_ids: list[str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.force_removed_run_ids is None:
            self.force_removed_run_ids = []


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

    Terminal runs (success / warning / failed) with no incomplete batches and a
    successful compact batch have already merged drained datasets into curated.
    Requiring status==success alone stranded large failed-but-compacted runs.
    Incomplete batches still block cleanup — compact may have skipped those
    datasets, leaving the only copy in staging.
    """
    run = manifest.get_run(run_id)
    if run is None:
        return False
    if run["status"] not in ("success", "warning", "failed"):
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
        if is_locked(path):
            continue
        if not dry_run:
            try:
                # Deliberately unlinked *after* the probe closed its handle:
                # Windows refuses to delete an open file, so holding the lock
                # across the unlink — which is what the flock version did —
                # cannot work there. A holder that reappears in this gap turns
                # the unlink into an OSError, and skipping is the right answer.
                path.unlink(missing_ok=True)
            except OSError:
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

    Safe means: the run succeeded and compact merged its staging into curated,
    or the staging belongs to no manifest run (orphan) and is old enough.

    Staging of failed/incomplete runs is resumable state — its successful
    batches may exist *only* in staging (compact was gated off). It is never
    deleted unless ``force=True``, and then the run's success batches are
    demoted to failed in the manifest so a later ``cml retry`` refetches them
    instead of silently losing their rows.
    """
    manifest = Manifest(config.manifest_path)
    staging_root = config.staging_root
    now = datetime.now(timezone.utc)

    known_run_ids = {row["run_id"] for row in manifest.list_runs()}
    staging_run_ids = list_staging_run_ids(staging_root)

    removed: list[str] = []
    orphans: list[str] = []
    force_removed: list[str] = []
    skipped: list[str] = []
    bytes_freed = 0

    for run_id in sorted(staging_run_ids):
        paths = staging_run_paths(staging_root, run_id)
        if not paths:
            continue

        if run_id not in known_run_ids:
            if _staging_age_days(paths, now) < orphan_retention_days:
                skipped.append(run_id)
                continue
            orphans.append(run_id)
            bytes_freed += _delete_paths(paths, dry_run=dry_run)
            continue

        if run_ready_for_staging_cleanup(manifest, run_id):
            removed.append(run_id)
            bytes_freed += _delete_paths(paths, dry_run=dry_run)
            continue

        run = manifest.get_run(run_id)
        if force and run is not None and run["status"] != "running":
            if not dry_run:
                manifest.demote_success_batches(
                    run_id,
                    reason="staging evicted by cml clean --force; refetch on retry",
                )
            force_removed.append(run_id)
            bytes_freed += _delete_paths(paths, dry_run=dry_run)
            continue
        skipped.append(run_id)

    clean_stale_lock_files(
        config.meta_root,
        retention_days=orphan_retention_days,
        dry_run=dry_run,
    )

    return StagingCleanupResult(
        removed_run_ids=removed,
        orphan_run_ids=orphans,
        bytes_freed=bytes_freed,
        skipped_run_ids=skipped,
        force_removed_run_ids=force_removed,
    )
