"""Compressed JSON files with a stable logical name.

A caller names a file by its plain ``.json`` path. What is stored beside that
path is ``<name>.json.zst`` when a zstd binding is available (stdlib
``compression.zstd`` from Python 3.14, else the ``zstandard`` package) and
``<name>.json.gz`` otherwise, so every supported Python can read and write it.
Readers accept all three spellings, so files written before compression stay
readable and are replaced by the next write.

Large, repetitive JSON ledgers (the CNINFO pagination checkpoints reached
239 MB) compress about 19x with zstd and are rewritten after every page, so
the saving is in write I/O as much as in space.

This module imports nothing from the package except the Windows-safe replace,
so any layer may use it.
"""

from __future__ import annotations

import gzip
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from cnequity.file_replace import replace_with_retry

_ZSTD_LEVEL = 3
_GZIP_LEVEL = 6


def _zstd():
    """The zstd ``(compress, decompress)`` pair, or None when none is installed."""
    try:
        from compression import zstd  # type: ignore[import-not-found]

        return (
            lambda data: zstd.compress(data, level=_ZSTD_LEVEL),
            zstd.decompress,
        )
    except ImportError:
        pass
    try:
        import zstandard  # type: ignore[import-not-found]
    except ImportError:
        return None
    return (
        lambda data: zstandard.ZstdCompressor(level=_ZSTD_LEVEL).compress(data),
        lambda data: zstandard.ZstdDecompressor().decompress(data),
    )


def variants(path: str | Path) -> tuple[Path, Path, Path]:
    """Every file that may hold *path*'s content, preferred first."""
    path = Path(path)
    return (
        path.with_name(path.name + ".zst"),
        path.with_name(path.name + ".gz"),
        path,
    )


def stored_path(path: str | Path) -> Path | None:
    """The file currently holding *path*'s content, or None."""
    for candidate in variants(path):
        if candidate.exists():
            return candidate
    return None


def exists(path: str | Path) -> bool:
    return stored_path(path) is not None


def read_json(path: str | Path) -> Any:
    """Load *path*'s JSON from whichever variant exists.

    Raises ``FileNotFoundError`` when none does, and ``ValueError`` when the
    stored bytes are not valid (compressed) JSON.
    """
    stored = stored_path(path)
    if stored is None:
        raise FileNotFoundError(str(path))
    data = stored.read_bytes()
    if stored.name.endswith(".zst"):
        codec = _zstd()
        if codec is None:
            raise ValueError(f"{stored} is zstd-compressed but no zstd library is installed")
        try:
            data = codec[1](data)
        except Exception as exc:  # noqa: BLE001 — bindings raise their own types
            raise ValueError(f"{stored}: {exc}") from exc
    elif stored.name.endswith(".gz"):
        try:
            data = gzip.decompress(data)
        except OSError as exc:
            raise ValueError(f"{stored}: {exc}") from exc
    return json.loads(data.decode("utf-8"))


def write_json(path: str | Path, payload: Any, **dump_options: Any) -> Path:
    """Atomically store *payload* for logical *path*, compressed; return the file.

    The new file is synced and swapped in before any other variant is removed,
    so an interrupted write leaves the previous content readable.
    """
    path = Path(path)
    data = (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str, **dump_options) + "\n"
    ).encode("utf-8")
    codec = _zstd()
    if codec is not None:
        target, blob = path.with_name(path.name + ".zst"), codec[0](data)
    else:
        target = path.with_name(path.name + ".gz")
        blob = gzip.compress(data, compresslevel=_GZIP_LEVEL, mtime=0)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=str(target.parent))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(blob)
            handle.flush()
            os.fsync(handle.fileno())
        replace_with_retry(tmp_name, target)
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
    for other in variants(path):
        if other != target:
            try:
                other.unlink()
            except FileNotFoundError:
                pass
    return target


def remove(path: str | Path) -> None:
    """Delete every variant of *path*."""
    for candidate in variants(path):
        try:
            candidate.unlink()
        except FileNotFoundError:
            pass
