"""Windows-safe file replacement with no lake or dataframe dependencies.

Every layer that writes files needs these, adapters included, so they live
below ``storage``; ``storage.atomic`` re-exports them.
"""

from __future__ import annotations

import errno
import os
import sys
import time
from pathlib import Path

from cnequity.i18n import N_, Message

# Windows refuses ``os.replace`` while another handle (DuckDB, Explorer preview,
# AV) holds the destination. A short backoff covers the common "query just
# closed" race; persistent locks surface as FileInUseError with a hint.
_REPLACE_ATTEMPTS = 5
_REPLACE_BACKOFF_SEC = 0.05
IS_WINDOWS = sys.platform == "win32"


class FileInUseError(PermissionError):
    """Windows kept denying a replace because another program holds the path."""


# Filesystems that cannot sync a directory say so with one of these.
_DIR_SYNC_UNSUPPORTED = {errno.EINVAL, errno.ENOTSUP, errno.EOPNOTSUPP, errno.EBADF}


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


def replace_with_retry(tmp: Path | str, path: Path | str, *, sync_dir: bool = True) -> None:
    """``os.replace`` that rides out transient Windows sharing denials.

    The parent directory is synced afterwards, so the rename survives a
    crash rather than only the renamed file's contents. ``sync_dir=False``
    skips that for scratch state rewritten on every request, where losing the
    last rename to a crash costs nothing.
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
            if sync_dir:
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
