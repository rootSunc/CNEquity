"""Portable retention evidence survives full snapshots without granting purge."""

import json
from datetime import date

import pytest

from cnequity.config import Config
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.lifecycle import LifecycleError, LifecycleStore, digest
from cnequity.storage.lifecycle.snapshot import (
    DEPENDENCIES_PATH,
    export_dependencies,
    read_dependencies,
)
from cnequity.storage.snapshots import SnapshotStore


@pytest.fixture(autouse=True)
def _catalog():
    from cnequity.query import catalog_read
    from cnequity.storage import revision_records
    from equitylake_core.extensions import ExtensionUnavailableError, ensure_extensions

    try:
        import duckdb

        ensure_extensions(duckdb.connect())
    except ExtensionUnavailableError as exc:
        pytest.skip(str(exc))
    yield
    catalog_read.close_all()
    revision_records.forget()


def _seed(cfg) -> None:
    from tests.catalog_seed import publish_rows
    from tests.unit.test_lake_write import _bars

    publish_rows(cfg, "daily_bars", _bars(date(2024, 1, 2), {"600000.SH": 10.0}))


def imported_store(cfg, evidence):
    evidence.mkdir(exist_ok=True)
    store = LifecycleStore(cfg.meta_root)
    store.import_manifest(
        {
            "schema_version": 1,
            "meta_root": str(store.meta),
            "holds": {
                "revision/daily_bars/previous": [{"reason": "historical verification"}],
            },
        }
    )
    return store


def test_snapshot_restores_protection_without_deletion_authority(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _seed(cfg)
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
    assert export_dependencies(restored) == dependencies
    restored.import_manifest({"schema_version": 1, "meta_root": str(restored.meta), "holds": {}})
    assert restored.registry()["holds"] == lifecycle.registry()["holds"]
    assert restored.registry()["lake_id"] != lifecycle.registry()["lake_id"]
    assert "pending" not in restored.registry()


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


def test_deleting_a_snapshot_leaves_every_hold(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _seed(cfg)
    lifecycle = imported_store(cfg, tmp_path / "evidence")
    snapshots = SnapshotStore(cfg)
    path = snapshots.create("rollback", ["daily_bars"]).parent
    before = lifecycle.registry()["holds"]

    assert snapshots.delete("rollback") == {"snapshot": "rollback", "path": str(path.absolute())}

    assert not path.exists() and not list(path.parent.glob(".deleting-*"))
    assert lifecycle.registry()["holds"] == before
    with pytest.raises(FileNotFoundError):
        snapshots.delete("rollback")


def test_restoring_an_old_snapshot_carries_manual_holds_but_not_references(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    store = imported_store(cfg, tmp_path / "evidence")
    value = export_dependencies(store)
    reference = {"kind": "reference", "source": "/old/note.md", "revision_id": "a" * 32}
    value["protection"]["holds"]["revision/daily_bars/cited"] = [reference]
    value["protection"]["reference_roots"] = [{"path": "/old"}]
    value["protection_digest"] = digest(value["protection"])
    restored = LifecycleStore(tmp_path / "restored/meta")
    write_json_atomic(restored.meta / DEPENDENCIES_PATH, value)
    restored.import_manifest({"schema_version": 1, "meta_root": str(restored.meta), "holds": {}})
    assert set(restored.registry()["holds"]) == {"revision/daily_bars/previous"}


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
