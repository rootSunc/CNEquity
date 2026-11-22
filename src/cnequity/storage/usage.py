"""Where a lake's disk space goes, and how much deleting each part would free.

File sizes overstate what a deletion frees. A lake snapshot and a rehearsal
copy are clones of the lake's files (``file_copy`` uses APFS ``clonefile``, or
a ``FICLONE`` reflink on Linux file systems that support it)
and share their blocks; a hard-linked file frees nothing until its last link
goes. Adding up ``st_size`` (what ``du`` does, near enough) reported 99 GB for
two rehearsal copies that freed under 7 GB.

So each part of the lake is reported three ways:

* ``logical_bytes``: the files' sizes.
* ``allocated_bytes``: the blocks they occupy, each inode counted once.
* ``freeable_bytes``: blocks no other file shares, on files whose every link
  lies in the same part. On macOS this is the file system's own count of a
  file's unshared bytes (``ATTR_CMNEXT_PRIVATESIZE``); elsewhere clones cannot
  be seen, and a file's allocated bytes count. Blocks two files of the same
  part share count in neither, so on macOS this is a lower bound: what
  deleting the part frees at least. On a reflink file system (btrfs, XFS) it
  can overstate a part that shares blocks with another.

The lake's data files are also split by what the catalog says of them: in the
current snapshot, kept only for older revisions, expired and waiting out the
grace period before retention deletes them, or registered nowhere.
"""

from __future__ import annotations

import ctypes
import os
import shutil
import stat
import struct
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

#: Parts of the data root reported one level deeper, by their child directories.
_SPLIT = ("meta",)


class _AttrList(ctypes.Structure):
    _fields_ = [
        ("bitmapcount", ctypes.c_ushort),
        ("reserved", ctypes.c_uint16),
        ("commonattr", ctypes.c_uint32),
        ("volattr", ctypes.c_uint32),
        ("dirattr", ctypes.c_uint32),
        ("fileattr", ctypes.c_uint32),
        ("forkattr", ctypes.c_uint32),
    ]


_ATTR_CMNEXT_PRIVATESIZE = 0x00000008
_FSOPT_NOFOLLOW = 0x1
_FSOPT_ATTR_CMN_EXTENDED = 0x20


def _private_size_reader() -> Callable[[str], int | None] | None:
    """macOS's count of a file's bytes no clone shares; ``None`` where there is none."""
    if sys.platform != "darwin":
        return None
    try:
        getattrlist = ctypes.CDLL(None, use_errno=True).getattrlist
    except (OSError, AttributeError):
        return None

    def read(path: str) -> int | None:
        request = _AttrList(5, 0, 0, 0, 0, 0, _ATTR_CMNEXT_PRIVATESIZE)
        buffer = ctypes.create_string_buffer(16)
        options = _FSOPT_NOFOLLOW | _FSOPT_ATTR_CMN_EXTENDED
        if getattrlist(os.fsencode(path), ctypes.byref(request), buffer, 16, options) != 0:
            return None
        _length, size = struct.unpack_from("<Iq", buffer.raw, 0)
        return size

    return read


@dataclass
class Part:
    files: int = 0
    logical_bytes: int = 0
    #: inode -> [links seen in this part, links the file has, allocated, freeable]
    _inodes: dict[tuple[int, int], list[int]] = field(default_factory=dict, repr=False)

    def add(self, info: os.stat_result, private: int | None) -> None:
        self.files += 1
        self.logical_bytes += info.st_size
        key = (info.st_dev, info.st_ino)
        seen = self._inodes.get(key)
        if seen is not None:
            seen[0] += 1
            return
        allocated = info.st_blocks * 512
        free = allocated if private is None else private
        self._inodes[key] = [1, info.st_nlink, allocated, free]

    def merge(self, other: Part) -> None:
        self.files += other.files
        self.logical_bytes += other.logical_bytes
        for key, entry in other._inodes.items():
            mine = self._inodes.get(key)
            if mine is None:
                self._inodes[key] = list(entry)
            else:
                mine[0] += entry[0]

    def settle(self) -> dict[str, int]:
        return {
            "files": self.files,
            "logical_bytes": self.logical_bytes,
            "allocated_bytes": sum(e[2] for e in self._inodes.values()),
            "freeable_bytes": sum(e[3] for e in self._inodes.values() if e[0] >= e[1]),
        }


def _lake_file_states(config) -> tuple[dict[str, str], dict[str, Any]] | None:
    """Absolute path -> catalog state of each data file it knows, and the expiry details.

    States: ``current`` (in the latest snapshot), ``history`` (only in older
    kept snapshots), ``expired`` (scheduled for deletion after the grace).
    ``None`` without a catalog whose layout this code reads.
    """
    from cnequity.storage.catalog_bridge import FILE_GRACE_HOURS, lake_root
    from equitylake_core.catalog import CATALOG_FILE, KNOWN_METADATA_FORMATS, METADATA, Catalog

    root = lake_root(config)
    if not (root / CATALOG_FILE).is_file():
        return None
    with Catalog(root, read_only=True) as catalog:
        if catalog.metadata_format() not in KNOWN_METADATA_FORMATS:
            return None
        data_path = str(
            catalog.execute(
                f"SELECT value FROM {METADATA}.ducklake_metadata WHERE key = 'data_path'"
            ).fetchone()[0]
        )

        def under(base: str, path: str, relative) -> str:
            return os.path.join(base, path) if relative else path

        states: dict[str, str] = {}
        for schema_path, s_rel, table_path, t_rel, path, f_rel, end in catalog.execute(
            f"SELECT s.path, s.path_is_relative, t.path, t.path_is_relative, f.path, "
            f"f.path_is_relative, f.end_snapshot FROM {METADATA}.ducklake_data_file f "
            f"JOIN {METADATA}.ducklake_table t ON t.table_id = f.table_id "
            f"AND t.end_snapshot IS NULL "
            f"JOIN {METADATA}.ducklake_schema s ON s.schema_id = t.schema_id "
            "AND s.end_snapshot IS NULL"
        ).fetchall():
            table_dir = under(under(data_path, schema_path, s_rel), table_path, t_rel)
            states[os.path.normpath(under(table_dir, path, f_rel))] = (
                "current" if end is None else "history"
            )
        first = last = None
        for path, relative, start in catalog.execute(
            f"SELECT path, path_is_relative, CAST(schedule_start AS VARCHAR) "
            f"FROM {METADATA}.ducklake_files_scheduled_for_deletion"
        ).fetchall():
            states[os.path.normpath(under(data_path, path, relative))] = "expired"
            first = start if first is None or start < first else first
            last = start if last is None or start > last else last
    # Retention deletes an expired file once the grace has passed since it expired.
    return states, {"expired_between": [first, last], "grace_hours": FILE_GRACE_HOURS}


def _units(data_root: Path) -> tuple[list[tuple[Path, str, bool]], list[tuple[Path, str]]]:
    """What to walk: ``(directory, part, recurse)`` units, and loose files with their part.

    A part's subdirectories are walked as units of their own, so one large
    part (a raw archive of a million files) spreads over the threads.
    """
    parts: list[tuple[Path, str]] = []
    loose: list[tuple[Path, str]] = []
    for entry in sorted(data_root.iterdir()):
        if entry.is_symlink():
            continue
        if entry.is_dir() and entry.name in _SPLIT:
            for child in sorted(entry.iterdir()):
                if child.is_symlink():
                    continue
                if child.is_dir():
                    parts.append((child, f"{entry.name}/{child.name}"))
                else:
                    loose.append((child, f"{entry.name}/(files)"))
        elif entry.is_dir():
            parts.append((entry, entry.name))
        else:
            loose.append((entry, "(files)"))
    units: list[tuple[Path, str, bool]] = []
    for directory, name in parts:
        units.append((directory, name, False))
        try:
            children = sorted(directory.iterdir())
        except OSError:
            continue
        units.extend(
            (child, name, True) for child in children if child.is_dir() and not child.is_symlink()
        )
    return units, loose


def _walk(
    root: Path,
    recurse: bool,
    private: Callable[[str], int | None] | None,
    lake_data: Path | None,
    states: dict[str, str],
) -> tuple[Part, dict[str, Part]]:
    """Measure the files under *root*, or with *recurse* False only those directly in it."""
    part, lake = Part(), {}
    in_lake = lake_data is not None and (
        root.is_relative_to(lake_data) or lake_data.is_relative_to(root)
    )
    pending = [str(root)]
    while pending:
        directory = pending.pop()
        try:
            with os.scandir(directory) as entries:
                listed = list(entries)
        except OSError:
            continue
        for entry in listed:
            try:
                info = entry.stat(follow_symlinks=False)
            except OSError:
                continue
            if stat.S_ISDIR(info.st_mode):
                if recurse:
                    pending.append(entry.path)
                continue
            if not stat.S_ISREG(info.st_mode):
                continue
            size = private(entry.path) if private is not None else None
            part.add(info, size)
            if in_lake and entry.path.startswith(str(lake_data) + os.sep):
                state = states.get(os.path.normpath(entry.path), "unregistered")
                lake.setdefault(state, Part()).add(info, size)
    return part, lake


def storage_usage(config, *, workers: int = 8) -> dict[str, Any]:
    """Measure the data root of *config*; reads only.

    Each part is walked on its own thread: the time goes on system calls (a
    raw archive holds over a million small files), which run in parallel.
    """
    from concurrent.futures import ThreadPoolExecutor

    from cnequity.storage.catalog_bridge import lake_root

    data_root = Path(config.data_root).resolve()
    private = _private_size_reader()
    catalog = _lake_file_states(config)
    states, expiry = catalog if catalog is not None else ({}, {})
    lake_data = (lake_root(config) / "data").resolve() if catalog is not None else None

    units, loose = _units(data_root)
    parts: dict[str, Part] = {}
    lake: dict[str, Part] = {}
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        walked = pool.map(
            lambda unit: (unit[1], _walk(unit[0], unit[2], private, lake_data, states)), units
        )
        for name, (part, states_seen) in walked:
            parts.setdefault(name, Part()).merge(part)
            for state, seen in states_seen.items():
                lake.setdefault(state, Part()).merge(seen)
    for path, name in loose:
        info = path.lstat()
        if stat.S_ISREG(info.st_mode):
            parts.setdefault(name, Part()).add(info, private(str(path)) if private else None)

    usage = shutil.disk_usage(data_root)
    settled = {name: part.settle() for name, part in parts.items() if part.files}
    report: dict[str, Any] = {
        "data_root": str(data_root),
        "clone_aware": private is not None,
        "disk": {"total_bytes": usage.total, "free_bytes": usage.free},
        "total": {
            key: sum(p[key] for p in settled.values())
            for key in ("files", "logical_bytes", "allocated_bytes")
        },
        "parts": dict(sorted(settled.items(), key=lambda kv: -kv[1]["allocated_bytes"])),
    }
    if catalog is not None:
        report["lake_files"] = {
            state: lake[state].settle() if state in lake else Part().settle()
            for state in ("current", "history", "expired", "unregistered")
        } | expiry
    return report
