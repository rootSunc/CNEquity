"""Atomic writes for curated/derived data and quality metadata."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

import polars as pl

from cnequity.file_replace import (  # noqa: F401 - re-exported for callers
    IS_WINDOWS,
    LINK_UNSUPPORTED_ERRNOS,
    FileInUseError,
    fsync_dir,
    publish_once,
    replace_with_retry,
)
from cnequity.i18n import N_, Message


class RollbackIncompleteError(RuntimeError):
    """A swap failed and putting the original back failed too; data sits in a backup."""


def fsync_file(path: Path | str) -> None:
    """Flush one file's data and metadata to stable storage.

    Windows rejects fsync on a read-only descriptor, while a copy that
    inherited a read-only mode cannot be reopened for writing on POSIX.
    """
    if IS_WINDOWS:
        with open(path, "r+b") as handle:
            os.fsync(handle.fileno())
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def swap_with_backup(staged: Path, target: Path, backup: Path) -> None:
    """Replace *target* with *staged*, keeping the old tree at *backup*.

    The caller removes *backup* after success. If promoting *staged* fails and
    the original cannot be moved back either, the original stays at *backup*
    and the error names it, so no data is silently lost.
    """
    replace_with_retry(target, backup)
    try:
        replace_with_retry(staged, target)
    except BaseException:
        try:
            replace_with_retry(backup, target)
        except OSError as restore_exc:
            raise RollbackIncompleteError(
                Message(
                    N_(
                        "Replacing %s failed and the original could not be restored; it is kept "
                        "intact at %s. Close the program holding it, then rename it back to %s "
                        "to recover"
                    ),
                    target,
                    backup,
                    target.name,
                )
            ) from restore_exc
        raise


def write_parquet_atomic(path: Path, df: pl.DataFrame, **kwargs) -> Path:
    """Write *df* to *path* via a unique same-directory temp file.

    A unique temporary name matters when two independent workers refresh the
    same cache or derived artifact: a fixed ``target.tmp`` lets them overwrite
    each other's in-progress footer before either ``os.replace`` runs.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    os.close(fd)
    tmp = Path(tmp_name)
    try:
        df.write_parquet(tmp, **kwargs)
        fsync_file(tmp)
        replace_with_retry(tmp, path)
        return path
    except BaseException:
        if tmp.exists():
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
        raise


def write_json_atomic(path: Path, payload: Any, **kwargs: Any) -> Path:
    """Serialize JSON to a unique temp file, then atomically replace *path*."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, **kwargs)
            handle.flush()
            os.fsync(handle.fileno())
        replace_with_retry(tmp, path)
        return path
    except BaseException:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise
