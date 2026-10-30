"""A publish that does not fit fails clearly and leaves the previous generation."""

from __future__ import annotations

import errno
import json
import os
import shutil
from pathlib import Path

import pytest

from cnequity import i18n
from cnequity.cli._root import _expected_failure
from cnequity.config import Config
from cnequity.storage.lifecycle import LifecycleStore
from cnequity.storage.revisions import (
    _RECEIPT_ALLOWANCE_BYTES,
    DiskSpaceError,
    RevisionStore,
)


@pytest.fixture(autouse=True)
def _english_messages(monkeypatch):
    monkeypatch.setenv("CNE_LANG", "en")
    i18n.reset()
    yield
    i18n.reset()


def _file(root: Path, day: str, value: bytes) -> Path:
    path = root / "daily_bars" / f"trade_date={day}" / "part-merged.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value)
    return path


def _commit(store: RevisionStore, run_id: str, files: list[Path]):
    receipt = store.commit(
        "daily_bars",
        run_id=run_id,
        changed_files=files,
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert receipt is not None
    return receipt


def _names(cfg: Config) -> set[str]:
    root = cfg.meta_root / "revisions" / "data" / "daily_bars"
    return {path.name for path in root.iterdir()} if root.is_dir() else set()


def test_publish_refuses_before_writing_when_free_space_is_short(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake")
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    path = _file(cfg.curated_root, "2024-06-18", b"day")
    real = shutil.disk_usage
    monkeypatch.setattr(
        shutil,
        "disk_usage",
        lambda target: real(target)._replace(free=0),
    )

    with pytest.raises(DiskSpaceError, match="daily_bars") as raised:
        _commit(store, "run-1", [path])

    assert "unfinished generation was not kept" in str(raised.value)
    assert store.current_pointer("daily_bars") is None
    assert _names(cfg) == set()
    assert _expected_failure(raised.value) == str(raised.value)


def test_hard_linked_files_do_not_count_against_free_space(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake")
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    original = _file(cfg.curated_root, "2024-06-18", b"x" * (2 * 1024 * 1024))
    _commit(store, "run-1", [original])
    added = _file(cfg.curated_root, "2024-06-19", b"new")
    real = shutil.disk_usage
    monkeypatch.setattr(
        shutil,
        "disk_usage",
        lambda target: real(target)._replace(free=_RECEIPT_ALLOWANCE_BYTES + 100),
    )

    receipt = _commit(store, "run-2", [added])

    carried = (
        cfg.meta_root
        / "revisions"
        / "data"
        / "daily_bars"
        / receipt.revision_id
        / "trade_date=2024-06-18"
        / "part-merged.parquet"
    )
    assert carried.stat().st_nlink > 1
    assert carried.read_bytes() == b"x" * (2 * 1024 * 1024)


def test_enospc_during_the_copy_removes_the_unfinished_generation(tmp_path, monkeypatch):
    from cnequity.storage import revisions

    cfg = Config(data_root=tmp_path / "lake")
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    original = _file(cfg.curated_root, "2024-06-18", b"day")
    first = _commit(store, "run-1", [original])
    added = _file(cfg.curated_root, "2024-06-19", b"new")
    before = _names(cfg)

    def fail(src, dst):
        raise OSError(errno.ENOSPC, "No space left on device", os.fspath(dst))

    monkeypatch.setattr(revisions, "copy2_isolated", fail)
    with pytest.raises(DiskSpaceError, match="ran out of disk space"):
        _commit(store, "run-2", [added])

    assert _names(cfg) == before
    assert store.current_pointer("daily_bars")["revision_id"] == first.revision_id
    parent = cfg.meta_root / "revisions" / "data" / "daily_bars"
    assert [path for path in parent.iterdir() if path.name.startswith(".")] == []
    current = store.current_root("daily_bars")
    assert current is not None
    assert (current / "trade_date=2024-06-18" / "part-merged.parquet").read_bytes() == b"day"


def test_enospc_while_writing_the_receipt_removes_the_unpublished_generation(tmp_path, monkeypatch):
    from cnequity.storage import revisions

    cfg = Config(data_root=tmp_path / "lake")
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    original = _file(cfg.curated_root, "2024-06-18", b"day")
    _commit(store, "run-1", [original])
    added = _file(cfg.curated_root, "2024-06-19", b"new")
    before = _names(cfg)
    real = revisions.write_json_atomic

    def fail_receipt(path, payload, **kwargs):
        if path.name != "current.json" and path.parent.name == "daily_bars":
            raise OSError(errno.ENOSPC, "No space left on device", os.fspath(path))
        return real(path, payload, **kwargs)

    monkeypatch.setattr(revisions, "write_json_atomic", fail_receipt)
    with pytest.raises(DiskSpaceError, match="ran out of disk space"):
        _commit(store, "run-2", [added])

    assert _names(cfg) == before
    assert store.current_root("daily_bars") is not None


def test_inspect_reports_bytes_a_generation_does_not_share(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    original = _file(cfg.curated_root, "2024-06-18", b"shared")
    first = _commit(store, "run-1", [original])
    added = _file(cfg.curated_root, "2024-06-19", b"only-here")
    second = _commit(store, "run-2", [added])
    report = LifecycleStore(cfg.meta_root).inspect()
    by_id = {obj["revision_id"]: obj for obj in report["objects"]}

    current = by_id[second.revision_id]
    assert current["exclusive_bytes"] == len(b"only-here")
    assert current["logical_bytes"] > current["exclusive_bytes"]
    # The carried file is hard-linked, so deleting the previous generation
    # frees nothing until the newer link is gone too. The legacy baseline is a
    # separate copy and still counts.
    assert by_id[first.revision_id]["exclusive_bytes"] == 0
    # The unreceipted baseline has no file manifest, so its logical size is
    # zero. The bytes are still on disk and exclusive counts them.
    assert by_id["legacy"]["exclusive_bytes"] == len(b"shared")


def test_old_receipt_with_inlined_coverage_still_loads(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    path = _file(cfg.curated_root, "2024-06-18", b"day")
    receipt = _commit(store, "run-1", [path])
    receipt_path = (
        cfg.meta_root
        / "revisions"
        / "daily_bars"
        / f"{receipt.revision:08d}-{receipt.revision_id}.json"
    )
    payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    payload["metadata"]["coverage"] = {
        "status": "partial",
        "gaps": {"outstanding_keys": [{"symbol": "000001.SZ", "trade_date": "2024-06-18"}]},
    }
    receipt_path.write_text(json.dumps(payload), encoding="utf-8")

    loaded = store.latest("daily_bars")
    assert loaded is not None
    assert loaded.metadata["coverage"]["gaps"]["outstanding_keys"][0]["symbol"] == "000001.SZ"
