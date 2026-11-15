"""A publish that does not fit fails clearly and leaves the previous generation."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tests.legacy_lake import commit_generation

from cnequity import i18n
from cnequity.config import Config
from cnequity.storage.lifecycle import LifecycleStore
from cnequity.storage.revisions import (
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
    receipt = commit_generation(
        store,
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
