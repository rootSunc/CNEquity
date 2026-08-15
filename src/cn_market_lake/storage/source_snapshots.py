"""Backup-source snapshot storage (ADR-0003 — never write to curated)."""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from cn_market_lake.domain.schemas import validate_dataframe

logger = logging.getLogger(__name__)

DEFAULT_SNAPSHOT_RETENTION_DAYS = 14


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
        df.write_parquet(path, compression="zstd")
        return path

    def list_files(
        self,
        dataset: str,
        *,
        source: str | None = None,
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
        if run_id:
            files = [f for f in files if f"run_id={run_id}" in str(f)]
        return files

    def _latest_run_id(self, dataset: str, *, source: str) -> str | None:
        """Newest ``run_id=`` directory under ``dataset/source=…`` (by mtime, then name)."""
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
        run_dirs.sort(key=lambda p: (p.stat().st_mtime, p.name))
        return run_dirs[-1].name.split("=", 1)[1]

    def read_latest(self, dataset: str, *, source: str) -> pl.DataFrame:
        """Load only the newest run_id's parquet parts (not the full history)."""
        run_id = self._latest_run_id(dataset, source=source)
        if run_id is None:
            return pl.DataFrame()
        files = self.list_files(dataset, source=source, run_id=run_id)
        if not files:
            return pl.DataFrame()
        return pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")


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
                    key=lambda p: (p.stat().st_mtime, p.name),
                )
                if not run_dirs:
                    continue
                newest = run_dirs[-1]
                for run_dir in run_dirs:
                    rel = str(run_dir.relative_to(root))
                    if run_dir == newest:
                        result.kept_run_dirs.append(rel)
                        continue
                    if run_dir.stat().st_mtime >= cutoff:
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
