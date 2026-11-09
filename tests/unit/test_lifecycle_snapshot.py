"""Portable retention evidence survives full snapshots without granting purge."""

import json
from datetime import date

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.lifecycle import LifecycleError, LifecycleStore, digest, reference_fingerprint
from cnequity.storage.lifecycle.snapshot import (
    DEPENDENCIES_PATH,
    export_dependencies,
    read_dependencies,
)
from cnequity.storage.snapshots import SnapshotStore


def imported_store(cfg, evidence):
    evidence.mkdir(exist_ok=True)
    roots = [{"path": str(evidence)}]
    store = LifecycleStore(cfg.meta_root)
    store.import_manifest(
        {
            "schema_version": 1,
            "meta_root": str(store.meta),
            "holds": {
                "revision/daily_bars/previous": [{"reason": "historical verification"}],
            },
            "reference_roots": roots,
            "reference_fingerprint": reference_fingerprint(roots),
        }
    )
    return store


def test_snapshot_restores_protection_without_deletion_authority(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    source = cfg.curated_root / "daily_bars/part.parquet"
    source.parent.mkdir(parents=True)
    pl.DataFrame(
        {"symbol": ["600000.SH"], "trade_date": [date(2024, 1, 2)], "close": [10.0]}
    ).write_parquet(source)
    lifecycle = imported_store(cfg, tmp_path / "evidence")
    snapshots = SnapshotStore(cfg)
    manifest = json.loads(snapshots.create("protected", ["daily_bars"]).read_text())
    assert manifest["lifecycle_dependencies"]["requires_rebinding"]
    assert snapshots.verify("protected").passed
    target = snapshots.restore("protected", tmp_path / "restored")
    restored = LifecycleStore(target / "meta")
    dependencies = read_dependencies(restored.meta)
    assert dependencies["protection"]["holds"] == lifecycle.registry()["holds"]
    assert not restored.registry_path.exists()
    with pytest.raises(LifecycleError, match="Import"):
        restored.plan(selected_ids=["revision/daily_bars/any"])
    assert export_dependencies(restored) == dependencies
    roots = [{"path": str(tmp_path / "evidence")}]
    restored.import_manifest(
        {
            "schema_version": 1,
            "meta_root": str(restored.meta),
            "holds": {},
            "reference_roots": roots,
            "reference_fingerprint": reference_fingerprint(roots),
        }
    )
    assert restored.registry()["holds"] == lifecycle.registry()["holds"]
    assert restored.registry()["lake_id"] != lifecycle.registry()["lake_id"]
    assert "pending" not in restored.registry()
    with pytest.raises(ValueError, match="full snapshot"):
        snapshots._lake_index(cfg.data_root, ["daily_bars"])


def test_tampered_dependencies_cannot_be_reimported(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    store = imported_store(cfg, tmp_path / "evidence")
    value = export_dependencies(store)
    restored = LifecycleStore(tmp_path / "restored/meta")
    value["protection"]["holds"] = {}
    write_json_atomic(restored.meta / DEPENDENCIES_PATH, value)
    with pytest.raises(LifecycleError, match="Invalid"):
        read_dependencies(restored.meta)


def test_digest_valid_but_missing_case_hold_rejected(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    store = imported_store(cfg, tmp_path / "evidence")
    value = export_dependencies(store)
    value["protection"]["cases"] = [{"case_id": "test", "roots": ["revision/daily_bars/missing"]}]
    value["protection_digest"] = digest(value["protection"])
    write_json_atomic(store.meta / DEPENDENCIES_PATH, value)
    with pytest.raises(LifecycleError, match="lack protection"):
        read_dependencies(store.meta)


def test_deleting_a_snapshot_releases_only_the_references_it_contributed(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    source = cfg.curated_root / "daily_bars/part.parquet"
    source.parent.mkdir(parents=True)
    pl.DataFrame(
        {"symbol": ["600000.SH"], "trade_date": [date(2024, 1, 2)], "close": [10.0]}
    ).write_parquet(source)
    lifecycle = imported_store(cfg, tmp_path / "evidence")
    snapshots = SnapshotStore(cfg)
    path = snapshots.create("rollback", ["daily_bars"]).parent
    rid = "a" * 32
    note = tmp_path / "evidence" / "note.md"
    note.write_text(f"{rid}\n", encoding="utf-8")
    inside = {"kind": "reference", "source": str(path / "manifest.json"), "revision_id": rid}
    outside = {"kind": "reference", "source": str(note), "revision_id": rid}
    lifecycle.import_manifest(
        {
            "schema_version": 1,
            "meta_root": str(lifecycle.meta),
            "holds": {
                "revision/daily_bars/aaa": [inside, outside],
                "revision/daily_bars/bbb": [{**inside, "revision_id": "b" * 32}],
            },
            "reference_roots": [{"path": str(tmp_path / "evidence")}],
            "reference_fingerprint": reference_fingerprint([{"path": str(tmp_path / "evidence")}]),
        }
    )

    result = snapshots.delete("rollback")

    assert not path.exists() and not list(path.parent.glob(".deleting-*"))
    holds = lifecycle.registry()["holds"]
    assert holds["revision/daily_bars/aaa"] == [outside], "a citation elsewhere still protects"
    assert "revision/daily_bars/bbb" not in holds
    assert holds["revision/daily_bars/previous"], "manual holds are never released here"
    assert {r["object_id"] for r in result["released"]} == {
        "revision/daily_bars/aaa",
        "revision/daily_bars/bbb",
    }
    with pytest.raises(FileNotFoundError):
        snapshots.delete("rollback")


def test_snapshot_delete_cli_names_a_missing_snapshot(tmp_path):
    from click.testing import CliRunner

    from cnequity.cli.main import cli

    cfg = Config(data_root=tmp_path / "lake")
    config = tmp_path / "cne.toml"
    # as_posix keeps the path a valid TOML string on Windows, where the
    # native form's backslashes are escape sequences.
    config.write_text(f'data_root = "{cfg.data_root.as_posix()}"\n', encoding="utf-8")
    missing = CliRunner().invoke(cli, ["snapshot", "delete", "nope", "--config", str(config)])
    assert missing.exit_code != 0 and "no snapshot named" in missing.output
