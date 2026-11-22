"""How much room the disk holding a lake has left, and what that changes.

Two thresholds, ``[storage.free_space]``:

* below ``warn_gb``, catalog retention keeps replaced revisions for one day
  instead of ``keep_days``, wherever it runs (the daily run, backfill,
  ``cne run maintain``), and ``cne run maintain`` reports a problem;
* below ``stop_gb``, nothing new is fetched or published: a writer that ran
  the disk full would leave the catalog database or a checkout half written.

Either set to 0 is off. Current revisions and held ones are never affected.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

#: ``keep_days`` while free space is below ``warn_gb``.
LOW_SPACE_KEEP_DAYS = 1

_GB = 1_000_000_000


@dataclass(frozen=True)
class FreeSpace:
    free_bytes: int
    total_bytes: int
    warn_bytes: int
    stop_bytes: int

    @property
    def low(self) -> bool:
        return bool(self.warn_bytes) and self.free_bytes < self.warn_bytes

    @property
    def critical(self) -> bool:
        return bool(self.stop_bytes) and self.free_bytes < self.stop_bytes

    @property
    def level(self) -> str:
        return "critical" if self.critical else "low" if self.low else "ok"

    def to_dict(self) -> dict:
        return {
            "level": self.level,
            "free_bytes": self.free_bytes,
            "total_bytes": self.total_bytes,
            "warn_bytes": self.warn_bytes,
            "stop_bytes": self.stop_bytes,
        }


def _disk_usage(path: Path):
    return shutil.disk_usage(path)


def free_space(config) -> FreeSpace:
    """Free space on the disk holding *config*'s data root (or its nearest existing parent).

    A config with no data root (a caller's stand-in) has nothing to measure: no
    threshold applies.
    """
    data_root = getattr(config, "data_root", None)
    if data_root is None:
        return FreeSpace(0, 0, 0, 0)
    root = Path(data_root).expanduser().absolute()
    while not root.exists() and root != root.parent:
        root = root.parent
    usage = _disk_usage(root)
    warn = getattr(config, "free_space_warn_gb", 0) or 0
    stop = getattr(config, "free_space_stop_gb", 0) or 0
    return FreeSpace(usage.free, usage.total, int(warn * _GB), int(stop * _GB))


def require_free_space(config) -> None:
    """Refuse to start writing when free space is below ``stop_gb``."""
    space = free_space(config)
    if space.critical:
        from cnequity.storage.revisions import DiskSpaceError

        raise DiskSpaceError(
            f"only {space.free_bytes / _GB:.1f} GB free on the lake's disk, below "
            f"[storage.free_space].stop_gb ({space.stop_bytes / _GB:g} GB); nothing was "
            "fetched or written. `cne storage usage` shows where the space goes and "
            "`cne storage catalog-retention --apply` deletes expired lake files"
        )


def effective_keep_days(config, keep_days: int) -> int:
    """*keep_days*, or one day while free space is below ``warn_gb`` (0 means no limit)."""
    if not free_space(config).low:
        return keep_days
    return LOW_SPACE_KEEP_DAYS if keep_days == 0 else min(keep_days, LOW_SPACE_KEEP_DAYS)
