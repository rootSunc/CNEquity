"""Purge fault injection and maintenance checks against tiny offline lakes."""

from __future__ import annotations

import json
import sqlite3

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.storage.lifecycle import LifecycleError, LifecycleStore, digest
from cnequity.storage.revisions import RevisionConsistencyError, RevisionStore


@pytest.fixture
def lake(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    writer = RevisionStore(cfg.meta_root, cfg.curated_root)
    source = cfg.curated_root / "daily_bars/part.parquet"
    source.parent.mkdir(parents=True)
    for i in range(5):
        pl.DataFrame({"close": [float(i)]}).write_parquet(source)
        writer.commit(
            "daily_bars",
            run_id=str(i),
            changed_files=[source],
            schema_version=1,
            contract_fingerprint="test",
        )
    store = LifecycleStore(cfg.meta_root)
    store.import_manifest(
        {
            "schema_version": 1,
            "meta_root": str(store.meta),
            "holds": {},
            "reference_roots": [],
            "reference_fingerprint": digest([]),
        }
    )
    return cfg, store, writer


def matured(store, monkeypatch=None, keep: int = 2):
    """A deletion plan of every generation *keep* leaves unprotected."""
    ids = [o["object_id"] for o in store.inspect(keep=keep)["objects"] if not o["blocked_reasons"]]
    return store.plan(selected_ids=ids, keep=keep)


def test_a_deletion_plan_names_its_versions(lake):
    cfg, store, writer = lake
    with pytest.raises(LifecycleError, match="without duplicates"):
        store.plan(selected_ids=[])
    current = next(o["object_id"] for o in store.inspect()["objects"] if o["current"])
    with pytest.raises(LifecycleError, match="current version"):
        store.plan(selected_ids=[current])
    plan = matured(store)
    assert len(plan["purge_ids"]) == 3
    assert plan["explicit_selection"] is True


def test_purge_keeps_current_receipts_and_is_idempotent(lake, monkeypatch):
    cfg, store, writer = lake
    plan = matured(store, monkeypatch)
    current = writer.current_root("daily_bars")
    receipt_paths = list((cfg.meta_root / "revisions/daily_bars").glob("*.json"))
    receipts = {p: p.read_bytes() for p in receipt_paths}
    with pytest.raises(LifecycleError, match="maintenance"):
        store.purge(plan["plan_id"])
    result = store.purge(plan["plan_id"], maintenance=True)
    assert result["status"] == "complete"
    assert len(result["items"]) == 3
    assert result["logical_bytes_deleted"] == plan["logical_bytes_to_purge"] > 0
    assert writer.current_root("daily_bars") == current
    assert current.exists()
    assert {p: p.read_bytes() for p in receipt_paths} == receipts
    assert store.purge(plan["plan_id"], maintenance=True) == result
    old_id = plan["purge_ids"][0].rsplit("/", 1)[-1]
    with pytest.raises(RevisionConsistencyError, match="missing"):
        writer.current_root("daily_bars", revision=old_id)


@pytest.mark.parametrize("change", ["hold", "publish", "extra_file", "modified_file"])
def test_purge_rejects_changed_plan_before_deleting(lake, monkeypatch, change):
    cfg, store, writer = lake
    plan = matured(store, monkeypatch)
    obj = next(o for o in plan["objects"] if o["object_id"] in plan["purge_ids"])
    root = cfg.meta_root / obj["path"]
    if change == "hold":
        store.hold(obj["object_id"], "new dependency")
    elif change == "publish":
        source = cfg.curated_root / "daily_bars/part.parquet"
        pl.DataFrame({"close": [999.0]}).write_parquet(source)
        writer.commit(
            "daily_bars",
            run_id="new",
            changed_files=[source],
            schema_version=1,
            contract_fingerprint="test",
        )
    elif change == "extra_file":
        (root / "extra.txt").write_text("unique data")
    else:
        (root / "part.parquet").write_bytes(b"corrupted")
    with pytest.raises(LifecycleError):
        store.purge(plan["plan_id"], maintenance=True)
    assert all((cfg.meta_root / o["path"]).exists() for o in plan["objects"])


def test_sha_validation_rejects_corruption_before_plan(lake, monkeypatch):
    cfg, store, _ = lake
    plan = matured(store, monkeypatch)
    obj = next(o for o in plan["objects"] if o["object_id"] in plan["purge_ids"])
    path = cfg.meta_root / obj["path"] / "part.parquet"
    raw = path.read_bytes()
    path.write_bytes(bytes([raw[0] ^ 1]) + raw[1:])
    # Re-marking changed bytes can start a new period, but cannot bypass receipt SHA.
    with pytest.raises(LifecycleError, match="content differs"):
        matured(store, monkeypatch)


def test_running_ingestion_and_invalid_manifest_block_purge(lake, monkeypatch):
    cfg, store, _ = lake
    plan = matured(store, monkeypatch)
    manifest = cfg.meta_root / "manifest.db"
    with sqlite3.connect(manifest) as connection:
        connection.execute("CREATE TABLE ingestion_runs (run_id TEXT, status TEXT)")
        connection.execute("INSERT INTO ingestion_runs VALUES ('busy', 'running')")
    with pytest.raises(LifecycleError, match="running"):
        store.purge(plan["plan_id"], maintenance=True)
    manifest.write_bytes(b"broken sqlite")
    with pytest.raises(LifecycleError, match="verify"):
        store.purge(plan["plan_id"], maintenance=True)


def test_publication_lock_blocks_purge(lake, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor

    from cnequity.file_lock import LockUnavailable, lake_mutation_lock

    cfg, store, _ = lake
    plan = matured(store, monkeypatch)
    with lake_mutation_lock(cfg.meta_root), ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(store.purge, plan["plan_id"], maintenance=True)
        with pytest.raises(LockUnavailable):
            future.result(timeout=5)
    assert all((cfg.meta_root / o["path"]).exists() for o in plan["objects"])


def test_partial_failure_resumes_without_expanding_scope(lake, monkeypatch):
    cfg, store, writer = lake
    plan = matured(store, monkeypatch)
    import cnequity.storage.lifecycle.purge as purge

    real = purge.shutil.rmtree

    def fail_after_one_file(path):
        next(path.rglob("*.parquet")).unlink()
        raise OSError("injected partial deletion")

    with monkeypatch.context() as patch:
        patch.setattr(purge.shutil, "rmtree", fail_after_one_file)
        with pytest.raises(OSError, match="injected"):
            store.purge(plan["plan_id"], maintenance=True)
    journal = json.loads((store.root / "purges" / f"{plan['plan_id']}.json").read_text())
    assert journal["status"] == "error" and journal["logical_bytes_deleted"] == 0
    with pytest.raises(LifecycleError, match="unfinished"):
        store.plan(selected_ids=plan["purge_ids"])
    assert purge.shutil.rmtree == real
    result = store.purge(plan["plan_id"], maintenance=True)
    assert result["status"] == "complete"
    assert set(result["items"]) == set(plan["purge_ids"])
    assert writer.current_root("daily_bars").exists()


def test_new_hold_blocks_retry_after_partial_failure(lake, monkeypatch):
    cfg, store, _ = lake
    plan = matured(store, monkeypatch)
    import cnequity.storage.lifecycle.purge as purge

    with monkeypatch.context() as patch:
        patch.setattr(purge.shutil, "rmtree", lambda path: (_ for _ in ()).throw(OSError("stop")))
        with pytest.raises(OSError):
            store.purge(plan["plan_id"], maintenance=True)
    # Import supports missing/quarantined objects; no byte dependency is lost.
    value = store.registry()
    value["holds"] = {plan["purge_ids"][0]: [{"reason": "discovered dependency"}]}
    store.import_manifest(value)
    with pytest.raises(LifecycleError, match="stale"):
        store.purge(plan["plan_id"], maintenance=True)
    assert list((store.root / "trash").rglob("*.parquet"))


def test_crash_after_rename_is_resumable(lake, monkeypatch):
    import cnequity.storage.lifecycle.purge as purge

    _, store, _ = lake
    plan = matured(store, monkeypatch)
    real = purge.replace_with_retry

    def crash_after_rename(source, target):
        real(source, target)
        raise KeyboardInterrupt("simulated process interruption")

    with monkeypatch.context() as patch:
        patch.setattr(purge, "replace_with_retry", crash_after_rename)
        with pytest.raises(KeyboardInterrupt):
            store.purge(plan["plan_id"], maintenance=True)
    result = store.purge(plan["plan_id"], maintenance=True)
    assert result["status"] == "complete"


def test_path_reuse_after_partial_deletion_is_rejected(lake, monkeypatch):
    cfg, store, _ = lake
    plan = matured(store, monkeypatch)
    import cnequity.storage.lifecycle.purge as purge

    with monkeypatch.context() as patch:
        patch.setattr(purge.shutil, "rmtree", lambda path: (_ for _ in ()).throw(OSError("stop")))
        with pytest.raises(OSError):
            store.purge(plan["plan_id"], maintenance=True)
    obj = next(o for o in plan["objects"] if o["object_id"] == plan["purge_ids"][0])
    reused = cfg.meta_root / obj["path"]
    reused.mkdir()
    (reused / "new-input.txt").write_text("do not delete")
    with pytest.raises(LifecycleError, match="Conflicting"):
        store.purge(plan["plan_id"], maintenance=True)
    assert (reused / "new-input.txt").read_text() == "do not delete"


def test_symlink_generation_and_extra_directory_are_never_deleted(lake, monkeypatch):
    cfg, store, _ = lake
    plan = matured(store, monkeypatch)
    obj = next(o for o in plan["objects"] if o["object_id"] in plan["purge_ids"])
    root = cfg.meta_root / obj["path"]
    (root / "unknown").mkdir()
    with pytest.raises(LifecycleError):
        matured(store, monkeypatch)
    assert root.exists()


def test_requires_bytes_closure_and_provenance_are_distinct(lake):
    _, store, _ = lake
    ids = [o["object_id"] for o in store.inventory() if o["revision"] > 0]
    a, b, c = ids[:3]
    case = {
        "case_id": "repair-1",
        "roots": [a],
        "dependencies": [
            {"from": a, "to": b, "kind": "requires_bytes"},
            {"from": b, "to": a, "kind": "requires_bytes"},
            {"from": b, "to": c, "kind": "provenance"},
        ],
    }
    store.import_manifest({**store.registry(), "cases": [case]})
    value = store.registry()
    assert a in value["holds"] and b in value["holds"] and c not in value["holds"]
    candidates = [
        o["object_id"] for o in store.inspect(keep=1)["objects"] if not o["blocked_reasons"]
    ]
    assert a not in candidates
    with pytest.raises(LifecycleError, match="immutable"):
        store.import_manifest({**value, "cases": [{**case, "roots": [c]}]})
    value["holds"].pop(b)
    store._save(value)
    with pytest.raises(LifecycleError, match="missing"):
        store.registry()


def test_case_registration_checks_evidence_and_holds(lake, monkeypatch, tmp_path):
    from cnequity.storage.revisions import sha256_file

    _, store, _ = lake
    plan = matured(store, monkeypatch)
    oid = plan["purge_ids"][0]
    evidence = tmp_path / "published.json"
    evidence.write_text("{}")
    case = {
        "case_id": "repair-proof",
        "roots": [oid],
        "dependencies": [],
        "evidence": [{"path": str(evidence), "sha256": sha256_file(evidence)}],
    }
    store.register_case(case)
    assert oid in store.registry()["holds"]
    store.register_case(case)
    assert len(store.registry()["cases"]) == 1
    evidence.write_text('{"changed": true}')
    with pytest.raises(LifecycleError, match="evidence"):
        store.register_case(case)


@pytest.mark.usefixtures("chinese_output")
def test_cli_cannot_bypass_web_confirmation(lake, monkeypatch):
    from click.testing import CliRunner

    from cnequity.cli.main import cli

    cfg, store, _ = lake
    plan = matured(store, monkeypatch)
    monkeypatch.setattr("cnequity.cli.storage_cmds._cfg", lambda _: cfg)
    runner = CliRunner()
    for argv in (
        ["storage", "apply", plan["plan_id"], "--phase", "purge"],
        ["storage", "apply", plan["plan_id"], "--phase", "purge", "--maintenance-window"],
    ):
        denied = runner.invoke(cli, argv)
        assert denied.exit_code == 1 and "存储页" in denied.output, denied.output
    assert all((cfg.meta_root / o["path"]).exists() for o in plan["objects"])
