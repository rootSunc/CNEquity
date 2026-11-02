"""Per-partition answers that are recomputed only when a partition's files change.

Every compact asked the same whole-dataset questions again — which sessions
does ``daily_bars`` cover — by decoding every row (19M, deduped by key, ~6 s),
although a run changes a handful of partitions. A partition's answer depends
only on its own files when the dataset's key includes the partition column, and
committed generations are immutable and hard-linked: an untouched partition
keeps the names, sizes and mtimes of its files from one generation to the next.
So the answer is cached under that stamp and recomputed only where it moved.

The cache lives under ``meta/stats``, a runtime path that lifecycle reference
fingerprints already ignore. Losing it costs one full recomputation, nothing else.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

from cnequity.storage.atomic import write_json_atomic

_VERSION = 1


def _stamp(partition_dir: Path, files: list[Path]) -> list[list]:
    out = []
    for path in sorted(files):
        info = path.stat()
        out.append([path.relative_to(partition_dir).as_posix(), info.st_size, info.st_mtime_ns])
    return out


def cached_partition_values(
    meta_root: Path,
    root: Path,
    name: str,
    partitions: dict[str, list[Path]],
    compute: Callable[[list[Path]], list],
    *,
    version: int = 1,
) -> dict[str, list]:
    """``compute(files)`` for every partition, reusing answers whose files did not change.

    *partitions* maps a partition directory name (relative to *root*) to its
    files; *name* identifies the question, *version* invalidates every answer
    when the computation itself changes.
    """
    path = Path(meta_root) / "stats" / "partition_cache" / f"{name}.json"
    try:
        stored = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        stored = {}
    if stored.get("format") != _VERSION or stored.get("version") != version:
        stored = {}
    cached: dict = stored.get("partitions", {})
    out: dict[str, list] = {}
    fresh: dict[str, dict] = {}
    dirty = False
    for part, files in partitions.items():
        stamp = _stamp(root / part if part else root, files)
        entry = cached.get(part)
        if entry is not None and entry.get("stamp") == stamp:
            value = entry["value"]
        else:
            value = compute(files)
            dirty = True
        out[part] = value
        fresh[part] = {"stamp": stamp, "value": value}
    if dirty or set(cached) != set(fresh):
        path.parent.mkdir(parents=True, exist_ok=True)
        write_json_atomic(
            path, {"format": _VERSION, "version": version, "partitions": fresh}, indent=None
        )
    return out
