import ctypes
import errno
import os
import sys

import pytest

from cnequity.storage import file_copy


def test_native_copy_preserves_bytes_metadata_and_write_isolation(tmp_path):
    src, dst = tmp_path / "source", tmp_path / "copy"
    src.write_bytes(b"original" * 1024)
    os.chmod(src, 0o640)
    os.utime(src, ns=(1_700_000_000_000_000_000,) * 2)
    file_copy.copy2_isolated(src, dst)
    assert dst.read_bytes() == src.read_bytes()
    assert dst.stat().st_ino != src.stat().st_ino
    assert dst.stat().st_mode == src.stat().st_mode
    assert dst.stat().st_mtime_ns == src.stat().st_mtime_ns
    with dst.open("r+b") as stream:
        stream.write(b"changed!")
    assert src.read_bytes().startswith(b"original")
    with src.open("r+b") as stream:
        stream.write(b"source!!")
    assert dst.read_bytes().startswith(b"changed!")


@pytest.mark.parametrize("error", [errno.EXDEV, errno.ENOTSUP, errno.ENOSYS])
def test_unsupported_clone_falls_back_to_independent_copy(tmp_path, monkeypatch, error):
    def unavailable(*args):
        ctypes.set_errno(error)
        return -1

    monkeypatch.setattr(file_copy, "_clonefile", lambda: unavailable)
    src, dst = tmp_path / "source", tmp_path / "copy"
    src.write_bytes(b"content")
    file_copy.copy2_isolated(src, dst)
    assert src.stat().st_ino != dst.stat().st_ino
    assert dst.read_bytes() == b"content"


def test_permission_error_does_not_silently_fallback(tmp_path, monkeypatch):
    def denied(*args):
        ctypes.set_errno(errno.EACCES)
        return -1

    monkeypatch.setattr(file_copy, "_clonefile", lambda: denied)
    monkeypatch.setattr(file_copy.shutil, "copy2", lambda *a: pytest.fail("must preserve error"))
    with pytest.raises(PermissionError):
        file_copy.copy2_isolated(tmp_path / "source", tmp_path / "copy")


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS clonefile API")
def test_macos_exposes_clonefile():
    assert file_copy._clonefile() is not None
