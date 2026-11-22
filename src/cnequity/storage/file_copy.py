"""Independent file copies, using filesystem copy-on-write when available."""

from __future__ import annotations

import ctypes
import errno
import os
import shutil
import sys
from functools import lru_cache
from pathlib import Path

from cnequity.storage.atomic import fsync_file


@lru_cache(maxsize=1)
def _clonefile():
    if sys.platform != "darwin":
        return None
    try:
        function = ctypes.CDLL(None, use_errno=True).clonefile
    except AttributeError:
        return None
    function.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int]
    function.restype = ctypes.c_int
    return function


#: Linux ``FICLONE`` ioctl (``_IOW(0x94, 9, int)``). btrfs, XFS with reflink
#: and bcachefs share the source's blocks copy-on-write; ext4 refuses it.
_FICLONE = 0x40049409
_UNSUPPORTED = {
    errno.EXDEV,
    errno.ENOTSUP,
    errno.EOPNOTSUPP,
    errno.EINVAL,
    errno.ENOSYS,
    errno.ENOTTY,
}


def _ficlone(src: str | Path, dst: str | Path) -> bool:
    """Reflink *src* to a new *dst* on Linux; False when the filesystem cannot."""
    if not sys.platform.startswith("linux"):
        return False
    import fcntl

    with open(src, "rb") as source:
        fd = os.open(dst, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            fcntl.ioctl(fd, _FICLONE, source.fileno())
        except OSError as exc:
            os.close(fd)
            os.unlink(dst)
            if exc.errno in _UNSUPPORTED:
                return False
            raise
        os.close(fd)
    return True


def copy2_isolated(src: str | Path, dst: str | Path) -> str | Path:
    """Copy to a fresh path with independent writes and copy2 metadata.

    APFS clones and Linux reflinks have separate inodes and share storage until either file is
    written. Hard links would let a mutable writer corrupt retained revisions
    and are never used. Unsupported filesystems fall back to a byte copy; on
    Windows that is ``CopyFile2`` (Python 3.12+), which itself block-clones on
    ReFS and Dev Drive volumes from Windows 11 24H2.
    The copy is synced before returning: a revision pointer published after
    it must not survive a crash that the copy did not.
    """
    clone = _clonefile()
    if clone is not None:
        if clone(os.fsencode(src), os.fsencode(dst), 0) == 0:
            shutil.copystat(src, dst)
            fsync_file(dst)
            return dst
        error = ctypes.get_errno()
        if error not in _UNSUPPORTED:
            raise OSError(error, os.strerror(error), os.fspath(dst))
    elif _ficlone(src, dst):
        shutil.copystat(src, dst)
        fsync_file(dst)
        return dst
    copied = shutil.copy2(src, dst)
    fsync_file(copied)
    return copied
