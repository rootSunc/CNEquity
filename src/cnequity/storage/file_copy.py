"""Independent file copies, using filesystem copy-on-write when available."""

from __future__ import annotations

import ctypes
import errno
import os
import shutil
import sys
from functools import lru_cache
from pathlib import Path


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


def copy2_isolated(src: str | Path, dst: str | Path) -> str | Path:
    """Copy to a fresh path with independent writes and copy2 metadata.

    APFS clones have separate inodes and share storage until either file is
    written. Hard links would let a mutable writer corrupt retained revisions
    and are never used. Unsupported filesystems fall back to a byte copy.
    """
    clone = _clonefile()
    if clone is not None:
        if clone(os.fsencode(src), os.fsencode(dst), 0) == 0:
            shutil.copystat(src, dst)
            return dst
        error = ctypes.get_errno()
        if error not in {errno.EXDEV, errno.ENOTSUP, errno.EOPNOTSUPP, errno.EINVAL, errno.ENOSYS}:
            raise OSError(error, os.strerror(error), os.fspath(dst))
    return shutil.copy2(src, dst)
