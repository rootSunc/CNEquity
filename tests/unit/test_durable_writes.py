"""A published revision pointer must not outlive the files it points at.

``current.json`` is the commit marker. A crash after it is renamed into place
but before the generation's copies and directory entries reach the disk leaves
a pointer to files that are not there.
"""

import os
import stat
import sys
from datetime import date
from pathlib import Path

import pytest

from cnequity import file_replace
from cnequity.storage import file_copy, revisions
from cnequity.storage.atomic import fsync_dir, fsync_file, replace_with_retry
from cnequity.storage.revisions import RevisionStore
from cnequity.storage.state import StateStore


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX read-only descriptors")
def test_a_read_only_copy_can_still_be_synced(tmp_path):
    path = tmp_path / "part.parquet"
    path.write_bytes(b"x")
    path.chmod(stat.S_IRUSR)
    fsync_file(path)
    fsync_dir(tmp_path)


def test_a_replace_syncs_the_directory_it_renamed_into(tmp_path, monkeypatch):
    synced: list[Path] = []
    monkeypatch.setattr(file_replace, "fsync_dir", lambda path: synced.append(Path(path)))
    tmp = tmp_path / ".pointer.tmp"
    tmp.write_text("{}")

    replace_with_retry(tmp, tmp_path / "current.json")

    assert synced == [tmp_path]


def test_state_writes_reach_the_disk_before_the_rename(tmp_path, monkeypatch):
    events: list[str] = []
    real_fsync = os.fsync
    monkeypatch.setattr(
        "cnequity.storage.state.os.fsync", lambda fd: (events.append("fsync"), real_fsync(fd))
    )
    monkeypatch.setattr(
        "cnequity.storage.state.replace_with_retry",
        lambda tmp, path: (events.append("replace"), os.replace(tmp, path)),
    )

    StateStore(tmp_path).set_date("daily_bars", date(2026, 10, 9))

    assert events == ["fsync", "replace"]


def test_the_pointer_is_published_after_the_generation_is_durable(tmp_path, monkeypatch):
    curated = tmp_path / "curated"
    part = curated / "daily_bars" / "trade_date=2024-06-18" / "part-merged.parquet"
    part.parent.mkdir(parents=True)
    part.write_bytes(b"first")
    store = RevisionStore(tmp_path / "meta", curated)
    store.ensure_current("daily_bars")

    events: list[tuple[str, Path]] = []
    real_file, real_dir, real_json = (
        file_copy.fsync_file,
        revisions.fsync_dir,
        revisions.write_json_atomic,
    )

    def record_file(path):
        events.append(("file", Path(path)))
        real_file(path)

    def record_dir(path):
        events.append(("dir", Path(path)))
        real_dir(path)

    def record_json(path, payload, **kwargs):
        events.append(("json", Path(path)))
        return real_json(path, payload, **kwargs)

    monkeypatch.setattr(file_copy, "fsync_file", record_file)
    monkeypatch.setattr(revisions, "fsync_dir", record_dir)
    monkeypatch.setattr(revisions, "write_json_atomic", record_json)

    receipt = store.commit(
        "daily_bars",
        run_id="run-1",
        changed_files=[part],
        schema_version=2,
        contract_fingerprint="contract-sha",
    )

    assert receipt is not None
    pointer = store.pointer_path("daily_bars")
    published = events.index(("json", pointer))
    copied = [i for i, (kind, path) in enumerate(events) if kind == "file"]
    partition_dirs = [
        i
        for i, (kind, path) in enumerate(events)
        if kind == "dir" and path.name == "trade_date=2024-06-18"
    ]
    assert copied and max(copied) < published
    assert partition_dirs and max(partition_dirs) < published
