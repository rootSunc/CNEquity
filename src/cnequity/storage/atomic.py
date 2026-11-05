"""Atomic writes for curated/derived data and quality metadata."""

from __future__ import annotations

import errno
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import polars as pl

from cnequity.i18n import N_, Message

# Windows refuses ``os.replace`` while another handle (DuckDB, Explorer preview,
# AV) holds the destination. A short backoff covers the common "query just
# closed" race; persistent locks surface as FileInUseError with a hint.
_REPLACE_ATTEMPTS = 5
_REPLACE_BACKOFF_SEC = 0.05
IS_WINDOWS = sys.platform == "win32"


class FileInUseError(PermissionError):
    """Windows kept denying a replace because another program holds the path."""


class RollbackIncompleteError(RuntimeError):
    """A swap failed and putting the original back failed too; data sits in a backup."""


# Filesystems that cannot sync a directory say so with one of these.
_DIR_SYNC_UNSUPPORTED = {errno.EINVAL, errno.ENOTSUP, errno.EOPNOTSUPP, errno.EBADF}


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


def fsync_dir(path: Path | str) -> None:
    """Make the entries created or renamed in *path* durable.

    Windows cannot open a directory for this and orders renames itself.
    """
    if IS_WINDOWS:
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    except OSError as exc:
        if exc.errno not in _DIR_SYNC_UNSUPPORTED:
            raise
    finally:
        os.close(fd)


def replace_with_retry(tmp: Path | str, path: Path | str) -> None:
    """``os.replace`` that rides out transient Windows sharing denials.

    The parent directory is synced afterwards, so the rename survives a
    crash rather than only the renamed file's contents.
    """
    for attempt in range(_REPLACE_ATTEMPTS):
        try:
            os.replace(tmp, path)
        except PermissionError as exc:
            if attempt + 1 < _REPLACE_ATTEMPTS:
                time.sleep(_REPLACE_BACKOFF_SEC * (2**attempt))
                continue
            if not IS_WINDOWS:
                raise
            raise FileInUseError(
                exc.errno,
                Message(
                    N_(
                        "%s is in use by another program and still cannot be replaced after %d "
                        "attempts. Common causes: Jupyter/DuckDB/polars is reading this data, "
                        "File Explorer has the folder open, or antivirus/sync software is "
                        "scanning it. Close the program holding it and run again"
                    ),
                    path,
                    _REPLACE_ATTEMPTS,
                ),
                str(tmp),
                None,
                str(path),
            ) from exc
        else:
            fsync_dir(Path(path).parent)
            return


# exFAT/FAT volumes and some network shares refuse hard links outright.
LINK_UNSUPPORTED_ERRNOS = frozenset(
    {errno.EXDEV, errno.EPERM, errno.EMLINK, errno.ENOTSUP, errno.EOPNOTSUPP, errno.ENOSYS}
)


def publish_once(temporary: Path, path: Path) -> bool:
    """Move a fully written *temporary* to *path* unless *path* already exists.

    Returns False when *path* was already present. A hard link gives the
    no-clobber guarantee atomically. Where links are unsupported, an existence
    check plus a rename stands in; the window between them is safe only for
    content-addressed paths, where a racing writer lands the same bytes.
    *temporary* is consumed either way.
    """
    try:
        os.link(temporary, path)
    except FileExistsError:
        Path(temporary).unlink(missing_ok=True)
        return False
    except OSError as exc:
        if exc.errno not in LINK_UNSUPPORTED_ERRNOS:
            raise
        if os.path.lexists(path):
            Path(temporary).unlink(missing_ok=True)
            return False
        replace_with_retry(temporary, path)
        return True
    Path(temporary).unlink(missing_ok=True)
    fsync_dir(Path(path).parent)
    return True


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
    except Exception:
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
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise
