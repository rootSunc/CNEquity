"""Backup-source snapshot storage (never write to curated)."""

from __future__ import annotations

import json
import logging
import shutil
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.domain.schemas import validate_dataframe
from cnequity.storage.atomic import write_json_atomic, write_parquet_atomic

logger = logging.getLogger(__name__)

DEFAULT_SNAPSHOT_RETENTION_DAYS = 14
_SNAPSHOT_METADATA = "_snapshot.json"


@dataclass
class SnapshotCleanupResult:
    removed_run_dirs: list[str] = field(default_factory=list)
    kept_run_dirs: list[str] = field(default_factory=list)
    bytes_freed: int = 0


class SnapshotStore:
    """Write/read backup-source captures under ``meta/source_snapshots/``."""

    def __init__(self, meta_root: Path):
        self.root = meta_root / "source_snapshots"

    def write(
        self,
        dataset: str,
        df: pl.DataFrame,
        *,
        source: str,
        data_version: str,
        run_id: str,
        batch_id: str = "backup",
        trade_date: date | None = None,
    ) -> Path | None:
        if df.is_empty():
            return None
        df = validate_dataframe(df, dataset)
        out_dir = (
            self.root
            / dataset
            / f"source={source}"
            / f"data_version={data_version}"
            / f"run_id={run_id}"
        )
        if trade_date is not None:
            out_dir = out_dir / f"trade_date={trade_date.isoformat()}"
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"part-{batch_id}.parquet"
        # A source snapshot is evidence used by later audits. Publishing a
        # half-written parquet here can make the next diff fail or, worse,
        # read a file whose footer was never fully flushed.
        write_parquet_atomic(path, df, compression="zstd")

        # Filesystem mtimes are not durable ordering: archive/restore tools and
        # ordinary copies commonly rewrite them. Keep the logical write time
        # beside the run so read_latest and retention make the same decision
        # after a restore. The parquet is published first, so a crash before
        # this metadata lands leaves the legacy mtime fallback usable.
        run_dir = self._run_dir(dataset, source=source, data_version=data_version, run_id=run_id)
        metadata_path = run_dir / _SNAPSHOT_METADATA
        now = datetime.now(timezone.utc)
        created_at = now.isoformat()
        if metadata_path.exists():
            try:
                previous = json.loads(metadata_path.read_text(encoding="utf-8"))
                created_at = str(previous.get("created_at") or created_at)
            except (OSError, TypeError, ValueError):
                pass
        write_json_atomic(
            metadata_path,
            {
                "schema_version": 1,
                "dataset": dataset,
                "source": source,
                "data_version": data_version,
                "run_id": run_id,
                "created_at": created_at,
                "updated_at": now.isoformat(),
            },
            ensure_ascii=False,
            indent=2,
        )
        return path

    def _run_dir(
        self,
        dataset: str,
        *,
        source: str,
        data_version: str,
        run_id: str,
    ) -> Path:
        return (
            self.root
            / dataset
            / f"source={source}"
            / f"data_version={data_version}"
            / f"run_id={run_id}"
        )

    @staticmethod
    def _run_timestamp(run_dir: Path) -> float:
        """Return persisted logical time, with mtime fallback for old runs."""
        metadata = run_dir / _SNAPSHOT_METADATA
        try:
            payload = json.loads(metadata.read_text(encoding="utf-8"))
            raw = payload.get("updated_at") or payload.get("created_at")
            if raw:
                return datetime.fromisoformat(str(raw)).timestamp()
        except (OSError, TypeError, ValueError, OverflowError):
            pass
        try:
            return run_dir.stat().st_mtime
        except OSError:
            return float("-inf")

    def list_files(
        self,
        dataset: str,
        *,
        source: str | None = None,
        data_version: str | None = None,
        run_id: str | None = None,
    ) -> list[Path]:
        base = self.root / dataset
        if not base.exists():
            return []
        pattern = "**/*.parquet"
        if source:
            base = base / f"source={source}"
        if not base.exists():
            return []
        files = sorted(base.glob(pattern))
        if data_version:
            version_component = f"data_version={data_version}"
            files = [f for f in files if version_component in f.parts]
        if run_id:
            run_component = f"run_id={run_id}"
            files = [f for f in files if run_component in f.parts]
        return files

    def _latest_snapshot_key(self, dataset: str, *, source: str) -> tuple[str, str] | None:
        """Return ``(data_version, run_id)`` for the newest snapshot run."""
        base = self.root / dataset
        if not base.exists():
            return None
        run_dirs: list[Path] = []
        for source_dir in base.glob(f"source={source}"):
            if not source_dir.is_dir():
                continue
            for ver_dir in source_dir.iterdir():
                if not ver_dir.is_dir() or not ver_dir.name.startswith("data_version="):
                    continue
                for run_dir in ver_dir.iterdir():
                    if run_dir.is_dir() and run_dir.name.startswith("run_id="):
                        run_dirs.append(run_dir)
        if not run_dirs:
            return None
        run_dirs.sort(key=lambda p: (self._run_timestamp(p), p.parent.name, p.name))
        newest = run_dirs[-1]
        return (
            newest.parent.name.split("=", 1)[1],
            newest.name.split("=", 1)[1],
        )

    def _latest_run_id(self, dataset: str, *, source: str) -> str | None:
        """Newest ``run_id=`` directory under ``dataset/source=…``.

        New snapshots sort by their persisted logical write time. Runs created
        before that metadata existed continue to sort by directory mtime.
        """
        latest = self._latest_snapshot_key(dataset, source=source)
        return latest[1] if latest is not None else None

    def read_latest(self, dataset: str, *, source: str) -> pl.DataFrame:
        """Load only the newest run_id's parquet parts (not the full history)."""
        latest = self._latest_snapshot_key(dataset, source=source)
        if latest is None:
            return pl.DataFrame()
        data_version, run_id = latest
        files = self.list_files(
            dataset,
            source=source,
            data_version=data_version,
            run_id=run_id,
        )
        if not files:
            return pl.DataFrame()
        out = pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")
        # A run may contain overlapping batches (for example a retry or a
        # multi-day backup window). Snapshot consumers compare by PK, so
        # leaving duplicates here can multiply joins and make a clean source
        # diff look like many disagreements.
        return dedupe_by_primary_key(out, dataset)


def _dir_size(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def clean_source_snapshots(
    meta_root: Path,
    *,
    retention_days: int = DEFAULT_SNAPSHOT_RETENTION_DAYS,
    dry_run: bool = False,
    now: datetime | None = None,
) -> SnapshotCleanupResult:
    """Delete ``run_id=`` snapshot dirs older than *retention_days*.

    Always keeps the newest run_id per ``(dataset, source, data_version)`` so
    ``read_latest`` / source_diff still have a peer even after long idle gaps.
    """
    from cnequity.storage.lifecycle.resources import resource_holds

    with resource_holds(meta_root, dry_run=dry_run) as holds:
        return _clean_source_snapshots(
            meta_root, retention_days=retention_days, dry_run=dry_run, now=now, holds=holds
        )


def _clean_source_snapshots(
    meta_root: Path, *, retention_days: int, dry_run: bool, now: datetime | None, holds: set[str]
) -> SnapshotCleanupResult:
    root = meta_root / "source_snapshots"
    result = SnapshotCleanupResult()
    if not root.exists() or retention_days < 0:
        return result

    anchor = now or datetime.now(timezone.utc)
    cutoff = anchor.timestamp() - retention_days * 86400

    for dataset_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        for source_dir in sorted(p for p in dataset_dir.iterdir() if p.is_dir()):
            if not source_dir.name.startswith("source="):
                continue
            for ver_dir in sorted(p for p in source_dir.iterdir() if p.is_dir()):
                if not ver_dir.name.startswith("data_version="):
                    continue
                run_dirs = sorted(
                    (p for p in ver_dir.iterdir() if p.is_dir() and p.name.startswith("run_id=")),
                    key=lambda p: (SnapshotStore._run_timestamp(p), p.name),
                )
                if not run_dirs:
                    continue
                newest = run_dirs[-1]
                for run_dir in run_dirs:
                    rel = run_dir.relative_to(root).as_posix()
                    if run_dir == newest or f"source_snapshot/{rel}" in holds:
                        result.kept_run_dirs.append(rel)
                        continue
                    if SnapshotStore._run_timestamp(run_dir) >= cutoff:
                        result.kept_run_dirs.append(rel)
                        continue
                    size = _dir_size(run_dir)
                    result.removed_run_dirs.append(rel)
                    result.bytes_freed += size
                    if not dry_run:
                        shutil.rmtree(run_dir)
                        logger.info(
                            "removed stale source_snapshot %s (%.1f MiB)",
                            rel,
                            size / (1024 * 1024),
                        )
    return result
