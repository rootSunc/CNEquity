import json
import shutil
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pytest
from tests.catalog_seed import publish_rows
from tests.unit.test_lake_write import _bars

from cnequity.config import Config
from cnequity.quality.verify import covered_through
from cnequity.storage.snapshots import FORMAT_VERSION, SnapshotStore
from cnequity.storage.state import StateStore


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


def _write_bars(cfg: Config, close: float = 10.0, day: date = date(2024, 6, 18)):
    """Publish one bar into the lake's catalog; returns the revision."""
    return publish_rows(cfg, "daily_bars", _bars(day, {"600000.SH": close}), run_id=f"r{close}")


def _load(cfg: Config, **kwargs) -> pl.DataFrame:
    from cnequity.query import catalog_read, load

    catalog_read.close_all()
    return load("daily_bars", config=cfg, **kwargs)


def _lake_file(cfg: Config) -> Path:
    return next((cfg.data_root / "lake" / "data").rglob("*.parquet"))


def test_snapshot_create_verify_and_restore(tmp_path):
    from cnequity.storage.migrations import owed

    cfg = Config(data_root=tmp_path / "lake")
    _write_bars(cfg)
    store = SnapshotStore(cfg)
    manifest_path = store.create("research-2024", ["daily_bars"])
    manifest = json.loads(manifest_path.read_text())
    assert manifest["format_version"] == FORMAT_VERSION
    assert manifest["revisions"]["daily_bars"]["revision"] == 1
    snapshot = store.path("research-2024")
    # Each dataset once, and no generation, pointer or receipt beside it.
    assert [p.relative_to(snapshot).as_posix() for p in snapshot.rglob("*.parquet")] == [
        "data/curated/daily_bars/trade_date=2024-06/part-merged.parquet"
    ]
    assert not (snapshot / "meta" / "revisions").exists()
    verification = store.verify("research-2024")
    assert verification.passed and verification.verified_files == 2

    restored = store.restore("research-2024", tmp_path / "restored")
    restored_cfg = replace(cfg, data_root=restored)
    assert _load(restored_cfg)["close"].to_list() == [10.0]
    assert not (restored / "curated").exists() and not (restored / ".restore").exists()
    assert (restored / "meta" / "restored-snapshot.json").exists()
    assert covered_through(restored_cfg, "daily_bars") == covered_through(cfg, "daily_bars")
    # A lake on its catalog that owes nothing it did not owe before.
    assert json.loads((restored / "meta" / "storage.json").read_text())["publish"] == "catalog"
    storage = {"lake-catalog", "revision-records", "drop-generations"}
    assert [migration.id for migration in owed(restored_cfg)] == [
        migration.id for migration in owed(cfg) if migration.id not in storage
    ]


def test_a_restored_lake_keeps_each_revision_and_reads_it_pinned(tmp_path):
    from cnequity.storage.revisions import RevisionStore

    cfg = Config(data_root=tmp_path / "lake")
    _write_bars(cfg, 10.0)
    second = _write_bars(cfg, 12.0)
    assert second.revision == 2
    store = SnapshotStore(cfg, tmp_path / "snapshots")
    store.create("pinned", ["daily_bars"])
    restored_cfg = replace(cfg, data_root=tmp_path / "restored")
    store.restore("pinned", restored_cfg.data_root)

    pointer = RevisionStore(restored_cfg.meta_root, restored_cfg.curated_root).current_pointer(
        "daily_bars"
    )
    assert (pointer["revision"], pointer["revision_id"]) == (2, second.revision_id)
    by_id = _load(restored_cfg, revision_map={"daily_bars": second.revision_id})
    assert by_id["close"].to_list() == [12.0]
    # The restored lake takes new writes after the restored revision.
    assert _write_bars(restored_cfg, 13.0).revision == 3


def test_snapshot_accepts_a_calendar_whose_watermark_runs_ahead_of_today(tmp_path):
    """trading_calendar is written a year ahead; its rows back the watermark."""
    from cnequity.domain.schemas import DATASET_SCHEMAS

    cfg = Config(data_root=tmp_path / "lake")
    day = date(2099, 1, 4)
    schema = DATASET_SCHEMAS["trading_calendar"]
    frame = pl.DataFrame(
        {
            "trade_date": [day],
            "is_trading": [True],
            "source": ["test"],
            "data_version": ["v1"],
            "fetched_at": [datetime(2024, 6, 18, tzinfo=timezone.utc)],
        }
    )
    frame = frame.with_columns(
        pl.lit(None, dtype=dtype).alias(name)
        for name, dtype in schema.items()
        if name not in frame.columns
    ).select(list(schema))
    publish_rows(cfg, "trading_calendar", frame)
    StateStore(cfg.meta_root).set_date("trading_calendar", day)
    store = SnapshotStore(cfg, tmp_path / "snapshots")

    store.create("calendar", ["trading_calendar"])

    assert store.verify("calendar").passed


def test_snapshot_detects_tampering_and_refuses_restore(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _write_bars(cfg)
    store = SnapshotStore(cfg)
    store.create("tampered", ["daily_bars"])
    stored = next((store.path("tampered") / "data").rglob("*.parquet"))
    stored.write_bytes(b"damaged")
    assert not store.verify("tampered").passed
    with pytest.raises(ValueError, match="verification failed"):
        store.restore("tampered", tmp_path / "restored")
    assert not (tmp_path / "restored").exists()


def test_snapshot_verify_and_export_require_exact_manifest_files(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _write_bars(cfg)
    store = SnapshotStore(cfg, tmp_path / "snapshots")
    store.create("exact", ["daily_bars"])
    snapshot = store.path("exact")

    rogue = snapshot / "rogue.secret"
    rogue.write_text("must not be released", encoding="utf-8")
    verification = store.verify("exact")
    assert not verification.passed
    assert "rogue.secret" in verification.mismatched
    with pytest.raises(ValueError, match="verification failed"):
        store.export_archive("exact", tmp_path / "rogue.tar", compression="none")

    rogue.unlink()
    payload = next((snapshot / "data").rglob("*.parquet"))
    external = tmp_path / "outside.parquet"
    external.write_bytes(payload.read_bytes())
    payload.unlink()
    payload.symlink_to(external)
    verification = store.verify("exact")
    assert not verification.passed
    assert payload.relative_to(snapshot).as_posix() in verification.mismatched
    with pytest.raises(ValueError, match="verification failed"):
        store.export_archive("exact", tmp_path / "symlink.tar", compression="none")


def test_snapshot_verify_and_export_reject_duplicate_manifest_path(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _write_bars(cfg)
    store = SnapshotStore(cfg, tmp_path / "snapshots")
    store.create("duplicate", ["daily_bars"])
    manifest_path = store.path("duplicate") / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"].append(dict(manifest["files"][0]))
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    verification = store.verify("duplicate")
    assert not verification.passed
    assert verification.mismatched.count(manifest["files"][0]["path"]) == 1
    with pytest.raises(ValueError, match="verification failed"):
        store.export_archive("duplicate", tmp_path / "duplicate.tar", compression="none")


def test_restore_refuses_active_or_nonempty_target(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _write_bars(cfg)
    store = SnapshotStore(cfg)
    store.create("safe", ["daily_bars"])
    with pytest.raises(ValueError, match="active data root"):
        store.restore("safe", cfg.data_root)
    target = tmp_path / "occupied"
    target.mkdir()
    (target / "keep.txt").write_text("user data")
    with pytest.raises(FileExistsError, match="not empty"):
        store.restore("safe", target)
    assert (target / "keep.txt").read_text() == "user data"


def test_snapshot_rejects_symlinked_lake_and_snapshot_roots(tmp_path):
    real_lake = tmp_path / "real-lake"
    real_cfg = Config(data_root=real_lake)
    _write_bars(real_cfg)

    linked_lake = tmp_path / "linked-lake"
    linked_lake.symlink_to(real_lake, target_is_directory=True)
    linked_store = SnapshotStore(Config(data_root=linked_lake), tmp_path / "snapshots")
    with pytest.raises(ValueError, match="symlink"):
        linked_store.create("data-root-link", ["daily_bars"])
    assert not linked_store.path("data-root-link").exists()

    normal_store = SnapshotStore(real_cfg, tmp_path / "normal-snapshots")
    normal_store.create("normal", ["daily_bars"])
    linked_snapshots = tmp_path / "linked-snapshots"
    linked_snapshots.symlink_to(normal_store.root, target_is_directory=True)
    linked_store = SnapshotStore(real_cfg, linked_snapshots)
    with pytest.raises(ValueError, match="symlink"):
        linked_store.verify("normal")
    with pytest.raises(ValueError, match="symlink"):
        linked_store.export_archive("normal", tmp_path / "linked.tar", compression="none")


def test_restore_rejects_existing_and_dangling_symlink_targets(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _write_bars(cfg)
    store = SnapshotStore(cfg)
    store.create("restore-safe", ["daily_bars"])

    external = tmp_path / "external-target"
    external.mkdir()
    linked_target = tmp_path / "linked-target"
    linked_target.symlink_to(external, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        store.restore("restore-safe", linked_target)
    assert not any(external.iterdir())

    dangling_destination = tmp_path / "not-created"
    dangling_target = tmp_path / "dangling-target"
    dangling_target.symlink_to(dangling_destination, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        store.restore("restore-safe", dangling_target)
    assert not dangling_destination.exists()


def test_snapshot_rejects_unknown_dataset_and_duplicate_name(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _write_bars(cfg)
    store = SnapshotStore(cfg)
    with pytest.raises(ValueError, match="unknown dataset"):
        store.create("bad", ["not-a-dataset"])
    store.create("once", ["daily_bars"])
    with pytest.raises(FileExistsError, match="already exists"):
        store.create("once", ["daily_bars"])


def test_a_lake_not_yet_on_its_catalog_is_not_snapshotted(tmp_path):
    from tests.legacy_lake import publish_generation

    from cnequity.storage.lake_record import LakeMigrationRequired

    cfg = Config(data_root=tmp_path / "lake")
    publish_generation(cfg, "daily_bars", _bars(date(2024, 6, 18), {"600000.SH": 10.0}))
    store = SnapshotStore(cfg, tmp_path / "snapshots")
    with pytest.raises(LakeMigrationRequired, match="cne upgrade"):
        store.create("old", ["daily_bars"])
    assert not store.path("old").exists()


def test_snapshot_rejects_traversal_in_manifest(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _write_bars(cfg)
    store = SnapshotStore(cfg)
    manifest_path = store.create("unsafe", ["daily_bars"])
    manifest = json.loads(manifest_path.read_text())
    manifest["files"][0]["path"] = "../../outside.parquet"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="unsafe snapshot path"):
        store.verify("unsafe")
    with pytest.raises(ValueError, match="unsafe snapshot path"):
        store.restore("unsafe", tmp_path / "restored")


def test_snapshot_verify_rejects_contract_and_fake_watermark_edits(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _write_bars(cfg)
    StateStore(cfg.meta_root).set_date("daily_bars", date(2024, 6, 18))
    store = SnapshotStore(cfg, tmp_path / "snapshots")
    store.create("semantic", ["daily_bars"])
    manifest_path = store.path("semantic") / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    manifest["contract_fingerprint"] = "forged-global-contract"
    manifest["dataset_states"]["daily_bars"]["last_success_trade_date"] = "2099-01-01"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    verification = store.verify("semantic")
    assert not verification.passed
    assert "contract_fingerprint" in verification.mismatched
    assert "state:daily_bars:last_success_trade_date" in verification.mismatched
    with pytest.raises(ValueError, match="verification failed"):
        store.restore("semantic", tmp_path / "restored")


def test_snapshot_verify_rejects_dataset_contract_and_revision_tampering(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _write_bars(cfg)
    store = SnapshotStore(cfg, tmp_path / "snapshots")
    store.create("revision-semantic", ["daily_bars"])
    manifest_path = store.path("revision-semantic") / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["contracts"]["daily_bars"]["fingerprint"] = "forged-dataset-contract"
    manifest["revisions"]["daily_bars"]["revision_id"] = "f" * 32
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    verification = store.verify("revision-semantic")
    assert not verification.passed
    assert any(item.startswith("contract:daily_bars") for item in verification.mismatched)
    assert "state:daily_bars:revision_id" in verification.mismatched
    with pytest.raises(ValueError, match="verification failed"):
        store.restore("revision-semantic", tmp_path / "restored")

    del manifest["revisions"]
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    assert "revisions" in store.verify("revision-semantic").mismatched


def test_a_snapshot_takes_the_revision_from_the_catalog_not_a_stale_state_copy(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    revision = _write_bars(cfg)
    state = StateStore(cfg.meta_root)
    payload = state.get_payload("daily_bars")
    # A copy an older release left in the state file, one revision off.
    payload["revision"] = revision.revision + 1
    state._write_payload(state._path("daily_bars"), payload)

    store = SnapshotStore(cfg, tmp_path / "snapshots")
    store.create("catalog", ["daily_bars"])
    manifest = json.loads((store.path("catalog") / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["dataset_states"]["daily_bars"]["revision"] == revision.revision
    assert store.verify("catalog").passed


def test_snapshot_files_are_copies_the_lake_can_retire(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _write_bars(cfg)
    store = SnapshotStore(cfg, tmp_path / "snapshots")
    store.create("copied", ["daily_bars"])

    copies = list((store.path("copied") / "data").rglob("*.parquet"))
    assert copies and all(path.stat().st_nlink == 1 for path in copies)
    # Retention removing the lake's file leaves the snapshot whole.
    lake_file = _lake_file(cfg)
    lake_file.chmod(0o644)
    lake_file.unlink()
    assert store.verify("copied").passed


def test_snapshot_refuses_a_symlinked_lake_file(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _write_bars(cfg)
    lake_file = _lake_file(cfg)
    external = tmp_path / "outside.parquet"
    external.write_bytes(lake_file.read_bytes())
    lake_file.chmod(0o644)
    lake_file.unlink()
    lake_file.symlink_to(external)

    store = SnapshotStore(cfg, tmp_path / "snapshots")
    with pytest.raises(ValueError, match="unsupported symlink"):
        store.create("symlinked", ["daily_bars"])
    assert not store.path("symlinked").exists()


def test_restore_rejects_targets_inside_source_snapshot_package(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _write_bars(cfg)
    store = SnapshotStore(cfg, tmp_path / "snapshots")
    store.create("nested-target", ["daily_bars"])
    snapshot = store.path("nested-target")

    with pytest.raises(ValueError, match="inside the source snapshot/package"):
        store.restore("nested-target", snapshot / "nested")
    with pytest.raises(ValueError, match="inside the source snapshot/package"):
        store.restore("nested-target", store.root / "outside-snapshot")


def test_snapshot_copies_files_without_holding_the_lake_lock(tmp_path, monkeypatch):
    """A slow copy (a raw archive on a network drive) must not make every run wait."""
    import threading

    from cnequity.file_lock import lake_mutation_lock
    from cnequity.storage import snapshots

    cfg = Config(data_root=tmp_path / "lake")
    _write_bars(cfg)
    StateStore(cfg.meta_root).set_date("daily_bars", date(2024, 6, 18))
    copied = []
    copy2 = shutil.copy2

    def take_the_lock(source):
        with lake_mutation_lock(cfg.meta_root, blocking=False):
            copied.append(source)

    def copy_while_another_run_takes_the_lock(source, target):
        if str(source).endswith(".parquet"):
            # Another thread stands in for another run: the lock is re-entrant
            # only within the thread that holds it.
            other = threading.Thread(target=take_the_lock, args=(source,))
            other.start()
            other.join()
        return copy2(source, target)

    monkeypatch.setattr(snapshots.shutil, "copy2", copy_while_another_run_takes_the_lock)
    store = SnapshotStore(cfg)
    store.create("unlocked", ["daily_bars"])

    assert copied
    assert store.verify("unlocked").passed


def test_snapshot_fails_when_a_listed_file_changes_before_it_is_copied(tmp_path, monkeypatch):
    from cnequity.storage import snapshots

    cfg = Config(data_root=tmp_path / "lake")
    _write_bars(cfg)
    store = SnapshotStore(cfg)
    copy2 = shutil.copy2

    def rewrite_then_copy(source, target):
        if str(source).endswith(".parquet"):
            Path(source).chmod(0o644)
            Path(source).write_bytes(b"rewritten by another writer")
        return copy2(source, target)

    monkeypatch.setattr(snapshots.shutil, "copy2", rewrite_then_copy)
    with pytest.raises(RuntimeError, match="changed during snapshot"):
        store.create("torn", ["daily_bars"])
    assert not store.path("torn").exists()
    assert not list(store.root.glob(".torn-*"))


def _legacy_snapshot(store: SnapshotStore, name: str, lake: Config, dataset: str) -> None:
    """A snapshot as releases before 0.25.0 made it: the tree and any revision files."""
    from cnequity.domain.contracts import contract_fingerprint, dataset_contract
    from cnequity.storage.revisions import sha256_file

    snapshot = store.path(name)
    files = []

    def add(source: Path, relative: str, layer: str) -> None:
        stored = snapshot / relative
        stored.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, stored)
        files.append(
            {
                "dataset": dataset,
                "layer": layer,
                "path": relative,
                "size_bytes": stored.stat().st_size,
                "sha256": sha256_file(stored),
            }
        )

    tree = lake.curated_root / dataset
    for path in sorted(tree.rglob("*.parquet")):
        add(path, f"data/curated/{dataset}/{path.relative_to(tree).as_posix()}", "curated")
    for path in sorted((lake.meta_root / "revisions").rglob("*")):
        if path.is_file():
            add(path, f"meta/{path.relative_to(lake.meta_root).as_posix()}", "meta")
    state_path = lake.meta_root / "state" / f"{dataset}.json"
    state = json.loads(state_path.read_text()) if state_path.is_file() else {}
    manifest = {
        "format": "cnequity.lake-snapshot",
        "format_version": 1,
        "name": name,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "datasets": [dataset],
        "dataset_states": {dataset: state},
        "contracts": {
            dataset: {
                "schema_version": dataset_contract(dataset)["schema_version"],
                "fingerprint": contract_fingerprint(dataset),
            }
        },
        "contract_fingerprint": contract_fingerprint(),
        "contract_fingerprints": {dataset: contract_fingerprint(dataset)},
        "lineage": {},
        "runtime_state": {},
        "research_inputs": None,
        "lifecycle_dependencies": None,
        "files": files,
    }
    (snapshot / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


def test_a_snapshot_made_before_0_25_restores_onto_a_catalog(tmp_path):
    from tests.legacy_lake import publish_generation

    from cnequity.storage.lake_record import read_lake

    old = Config(data_root=tmp_path / "old-lake")
    publish_generation(old, "daily_bars", _bars(date(2024, 6, 18), {"600000.SH": 10.0}))
    store = SnapshotStore(old, tmp_path / "snapshots")
    _legacy_snapshot(store, "before", old, "daily_bars")
    assert store.verify("before").passed

    restored = replace(old, data_root=tmp_path / "restored")
    store.restore("before", restored.data_root)

    assert _load(restored)["close"].to_list() == [10.0]
    done = read_lake(restored)["migrations"]
    assert {"lake-catalog", "revision-records", "drop-generations"} <= set(done)
    # The generation layout is gone: the catalog holds the rows.
    assert not any((restored.data_root / "curated").rglob("*.parquet"))
    assert not (restored.meta_root / "revisions" / "data" / "daily_bars").exists()
    assert _write_bars(restored, 11.0).revision == 2


def test_pre_0_25_snapshot_restores_data_without_revision_pointers(tmp_path):
    from polars.testing import assert_frame_equal

    from cnequity.storage.lake_record import read_lake

    old = Config(data_root=tmp_path / "old-lake")
    frame = _bars(date(2024, 6, 18), {"600000.SH": 10.0})
    folder = old.curated_root / "daily_bars" / "trade_date=2024-06"
    folder.mkdir(parents=True)
    frame.write_parquet(folder / "part-sample.parquet")
    store = SnapshotStore(old, tmp_path / "snapshots")
    _legacy_snapshot(store, "sample-024", old, "daily_bars")
    assert store.verify("sample-024").passed

    restored = replace(old, data_root=tmp_path / "restored")
    store.restore("sample-024", restored.data_root)

    assert_frame_equal(_load(restored).select(frame.columns), frame)
    assert "legacy-datasets" in read_lake(restored)["migrations"]
    assert not any(restored.curated_root.rglob("*.parquet"))
    assert _write_bars(restored, 11.0).revision == 2


def test_pre_0_25_snapshot_restores_when_unrelated_dataset_contract_changed(tmp_path):
    """A format_version=1 snapshot verifies and restores if its own datasets' contracts match."""
    from tests.legacy_lake import publish_generation

    old = Config(data_root=tmp_path / "old-lake")
    publish_generation(old, "daily_bars", _bars(date(2024, 6, 18), {"600000.SH": 10.0}))
    store = SnapshotStore(old, tmp_path / "snapshots")
    _legacy_snapshot(store, "from-024", old, "daily_bars")

    # Simulate an unrelated dataset changing the global registry fingerprint between 0.24 and 0.25.
    manifest_path = store.path("from-024") / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["contract_fingerprint"] = "0.24.0-global-registry-fingerprint"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    verification = store.verify("from-024")
    assert verification.passed, verification.mismatched

    restored = replace(old, data_root=tmp_path / "restored-from-024")
    store.restore("from-024", restored.data_root)
    assert _load(restored)["close"].to_list() == [10.0]


def test_v2_snapshot_create_and_restore_supports_migrated_revision_zero_dataset(tmp_path):
    """Upgraded lakes holding a revision=0 legacy dataset can still snapshot and restore."""
    from cnequity.domain.schemas import DATASET_SCHEMAS
    from cnequity.storage.catalog_migration import migrate_to_catalog

    lake = Config(data_root=tmp_path / "upgraded")
    inst_dir = lake.meta_root / "revisions" / "data" / "instruments" / "legacy"
    inst_dir.mkdir(parents=True)
    inst_schema = DATASET_SCHEMAS["instruments"]
    inst_row = {name: None for name in inst_schema}
    inst_row.update(
        symbol="600519.SH",
        name="贵州茅台",
        exchange="SH",
        asset_type="stock",
        list_date=date(2001, 8, 27),
        status="listed",
        source="test",
        data_version="v1",
        fetched_at=datetime(2024, 6, 18, 8, 0, tzinfo=timezone.utc),
    )
    pl.DataFrame([inst_row], schema=inst_schema).write_parquet(inst_dir / "part-merged.parquet")
    (lake.meta_root / "revisions" / "instruments").mkdir(parents=True)
    (lake.meta_root / "revisions" / "instruments" / "current.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "dataset": "instruments",
                "revision": 0,
                "revision_id": "legacy",
                "generation_path": "revisions/data/instruments/legacy",
                "receipt": None,
            }
        ),
        encoding="utf-8",
    )
    migrate_to_catalog(lake, apply=True)

    store = SnapshotStore(lake, tmp_path / "snapshots")
    store.create("with-legacy", ["instruments"])
    assert store.verify("with-legacy").passed

    restored = replace(lake, data_root=tmp_path / "restored-legacy")
    store.restore("with-legacy", restored.data_root)
    from cnequity.query import load

    assert load("instruments", config=restored)["symbol"].to_list() == ["600519.SH"]
