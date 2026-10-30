"""Configured generation retention deletes old directories and keeps receipts."""

from __future__ import annotations

from pathlib import Path

import pytest

from cnequity.cli._shared import attach_revision_retention
from cnequity.config import Config, load_config
from cnequity.config.bootstrap import path_for_toml
from cnequity.storage.lifecycle import LifecycleError, LifecycleStore, reference_fingerprint
from cnequity.storage.revisions import (
    RevisionConsistencyError,
    RevisionStore,
    apply_configured_revision_retention,
    retain_revision_generations,
)


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


def _lake(tmp_path: Path) -> tuple[Config, RevisionStore, list]:
    cfg = Config(data_root=tmp_path / "lake")
    curated = cfg.curated_root
    store = RevisionStore(cfg.meta_root, curated)
    days = ["2024-06-18", "2024-06-19", "2024-06-20", "2024-06-21"]
    files = [_file(curated, day, day.encode()) for day in days]
    receipts = []
    for index, path in enumerate(files):
        receipts.append(_commit(store, f"run-{index}", [path]))
    # Rewrite one intermediate partition so its old bytes belong only to the
    # generations that still point at them.
    files[1].write_bytes(b"rewritten")
    receipts.append(_commit(store, "run-rewrite", [files[1]]))
    return cfg, store, receipts


def _generation_names(cfg: Config) -> set[str]:
    root = cfg.meta_root / "revisions" / "data" / "daily_bars"
    return {path.name for path in root.iterdir()}


def test_retention_keeps_the_newest_generations_and_their_shared_bytes(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    first = _file(cfg.curated_root, "2024-06-18", b"day-18")
    added = _file(cfg.curated_root, "2024-06-19", b"day-19")
    oldest = _commit(store, "r0", [first])
    replaced = _commit(store, "r1", [added])
    added.write_bytes(b"day-19b")
    _commit(store, "r2", [added])
    newer = _file(cfg.curated_root, "2024-06-20", b"day-20")
    latest = _commit(store, "r3", [newer])
    carried = (
        cfg.meta_root
        / "revisions"
        / "data"
        / "daily_bars"
        / oldest.revision_id
        / "trade_date=2024-06-18"
        / "part-merged.parquet"
    )
    shared = (
        cfg.meta_root
        / "revisions"
        / "data"
        / "daily_bars"
        / latest.revision_id
        / "trade_date=2024-06-18"
        / "part-merged.parquet"
    )
    assert carried.stat().st_ino == shared.stat().st_ino
    cfg.revision_keep_generations = 2

    report = apply_configured_revision_retention(cfg)

    assert report is not None and report["status"] == "ok"
    assert report["removed"] == 2
    # The replaced partition lived only in the removed generation.
    assert report["freed_bytes"] == len(b"day-19")
    assert replaced.revision_id not in _generation_names(cfg)
    assert latest.revision_id in _generation_names(cfg)
    assert shared.read_bytes() == b"day-18"
    current = store.current_root("daily_bars")
    assert current is not None
    assert (current / "trade_date=2024-06-19" / "part-merged.parquet").read_bytes() == b"day-19b"
    assert len(list((cfg.meta_root / "revisions" / "daily_bars").glob("*.json"))) == 5
    with pytest.raises(RevisionConsistencyError, match="generation is missing"):
        store.current_root("daily_bars", revision=oldest.revision)


def test_retention_keeps_a_held_generation_and_the_current_pointer(tmp_path):
    cfg, store, receipts = _lake(tmp_path)
    lifecycle = LifecycleStore(cfg.meta_root)
    lifecycle.import_manifest(
        {
            "schema_version": 1,
            "meta_root": str(lifecycle.meta),
            "holds": {},
            "reference_roots": [],
            "reference_fingerprint": reference_fingerprint([], meta_root=lifecycle.meta),
        }
    )
    lifecycle.hold(f"revision/daily_bars/{receipts[0].revision_id}", "pinned")

    removed = retain_revision_generations(cfg.meta_root, keep=1)

    names = _generation_names(cfg)
    assert receipts[0].revision_id in names
    assert receipts[-1].revision_id in names
    assert receipts[1].revision_id not in names
    assert sum(len(item.removed_revision_ids) for item in removed) == 3


def test_stale_reference_inventory_deletes_nothing(tmp_path):
    cfg, _store, _receipts = _lake(tmp_path)
    notes = tmp_path / "notes"
    notes.mkdir()
    evidence = notes / "pin.md"
    evidence.write_text("pinned\n", encoding="utf-8")
    lifecycle = LifecycleStore(cfg.meta_root)
    roots = [{"path": str(notes.resolve())}]
    lifecycle.import_manifest(
        {
            "schema_version": 1,
            "meta_root": str(lifecycle.meta),
            "holds": {},
            "reference_roots": roots,
            "reference_fingerprint": reference_fingerprint(roots, meta_root=lifecycle.meta),
        }
    )
    evidence.write_text("changed\n", encoding="utf-8")
    before = _generation_names(cfg)

    with pytest.raises(LifecycleError, match="Reference inventory changed"):
        retain_revision_generations(cfg.meta_root, keep=1)

    assert _generation_names(cfg) == before
    cfg.revision_keep_generations = 1
    report = apply_configured_revision_retention(cfg)
    assert report is not None and report["status"] == "failed"
    assert _generation_names(cfg) == before


def test_unfinished_purge_blocks_retention(tmp_path):
    cfg, _store, _receipts = _lake(tmp_path)
    journal = cfg.meta_root / "lifecycle" / "purges"
    journal.mkdir(parents=True)
    (journal / "abc.json").write_text('{"status": "prepared"}', encoding="utf-8")
    before = _generation_names(cfg)

    with pytest.raises(LifecycleError, match="unfinished purge"):
        retain_revision_generations(cfg.meta_root, keep=1)

    assert _generation_names(cfg) == before


def test_default_keeps_five_generations(tmp_path):
    cfg, store, receipts = _lake(tmp_path)
    extra = _file(cfg.curated_root, "2024-06-22", b"day-22")
    receipts.append(_commit(store, "run-extra", [extra]))
    assert cfg.revision_keep_generations == 5
    # The first commit also keeps the unreceipted legacy baseline, which retention must not drop.
    assert len(_generation_names(cfg)) == 7

    report = apply_configured_revision_retention(cfg)

    assert report is not None and report["status"] == "ok"
    assert report["keep"] == 5
    assert report["removed"] == 1
    names = _generation_names(cfg)
    assert "legacy" in names
    assert receipts[0].revision_id not in names
    assert receipts[-1].revision_id in names
    assert len(names) == 6
    assert store.current_root("daily_bars", revision=receipts[-1].revision_id).is_dir()


def test_zero_keeps_every_generation(tmp_path, capsys):
    cfg, _store, _receipts = _lake(tmp_path)
    cfg.revision_keep_generations = 0
    before = _generation_names(cfg)
    payload: dict = {}

    attach_revision_retention(cfg, payload)

    assert payload == {}
    assert capsys.readouterr().err == ""
    assert apply_configured_revision_retention(cfg) is None
    assert _generation_names(cfg) == before


def test_keep_generations_loads_from_config_and_rejects_a_boolean(tmp_path):
    path = tmp_path / "cnequity.toml"
    path.write_text(
        "\n".join(
            [
                'extends = "builtin"',
                "[data]",
                f'root = "{path_for_toml(tmp_path / "lake")}"',
                "[storage.retention]",
                "keep_generations = 2",
                "",
            ]
        ),
        encoding="utf-8",
    )
    assert load_config(path).revision_keep_generations == 2
    path.write_text(
        f'extends = "builtin"\n[data]\nroot = "{path_for_toml(tmp_path / "lake")}"\n',
        encoding="utf-8",
    )
    assert load_config(path).revision_keep_generations == 5
    path.write_text(
        "\n".join(
            [
                'extends = "builtin"',
                "[data]",
                f'root = "{path_for_toml(tmp_path / "lake")}"',
                "[storage.retention]",
                "keep_generations = true",
                "",
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="keep_generations"):
        load_config(path)
