from __future__ import annotations

import hashlib
import json
import shutil
import tarfile
from datetime import date
from pathlib import Path

import polars as pl
import pytest
from click.testing import CliRunner

from cnequity.cli.main import cli
from cnequity.config import Config
from cnequity.config.bootstrap import path_for_toml
from cnequity.quality.dataset_checks import audit_curated_dataset
from cnequity.query.reader import load
from cnequity.storage.snapshots import SnapshotStore
from cnequity.storage.state import StateStore


def _write_bar(root: Path, day: date, close: float, *, name: str = "part.parquet") -> None:
    part = root / "curated" / "daily_bars" / f"trade_date={day.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["600000.SH"], "trade_date": [day], "close": [close]}).write_parquet(
        part / name
    )


def _state(root: Path, day: date) -> None:
    StateStore(root / "meta").set_date("daily_bars", day)


def _write_queryable_bar(root: Path, day: date, close: float) -> None:
    part = root / "curated" / "daily_bars" / f"trade_date={day.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600000.SH"],
            "trade_date": [day],
            "open": [close - 0.5],
            "high": [close + 0.5],
            "low": [close - 1.0],
            "close": [close],
            "volume": [1000],
            "amount": [close * 1000],
            "source": ["tdx_protocol"],
            "data_version": ["v2"],
            "fetched_at": [f"{day.isoformat()}T00:00:00+00:00"],
        }
    ).write_parquet(part / "part.parquet")


def _file_hashes(root: Path) -> dict[str, str]:
    """Hash lake payload files while excluding the process lock inode."""
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file() and Path("meta") / "locks" not in path.relative_to(root).parents
    }


def test_delta_add_replace_delete_and_apply_is_idempotent(tmp_path):
    baseline = tmp_path / "baseline"
    target = tmp_path / "target"
    day_one = date(2026, 8, 28)
    day_two = date(2026, 8, 29)
    _write_bar(baseline, day_one, 10.0)
    _state(baseline, day_one)
    _write_bar(target, day_one, 11.0)
    _write_bar(target, day_two, 12.0)
    _state(target, day_two)

    # A second file exists only in the baseline, exercising the delete path.
    _write_bar(baseline, date(2026, 8, 27), 9.0, name="to-delete.parquet")

    cfg = Config(data_root=target)
    store = SnapshotStore(cfg, tmp_path / "packages")
    manifest_path = store.create_delta("daily-update", baseline, target, ["daily_bars"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert {item["operation"] for item in manifest["changes"]} == {
        "add",
        "replace",
        "delete",
    }
    assert manifest["contract_fingerprint"]
    assert all(item["sha256"] for item in manifest["changes"] if item["operation"] != "delete")
    assert store.verify_delta("daily-update").passed

    store.apply_delta("daily-update", baseline)
    assert store._lake_index(baseline, ["daily_bars"]) == store._lake_index(target, ["daily_bars"])
    # Retry after a successful transfer is a no-op, not a base-mismatch error.
    store.apply_delta("daily-update", baseline)


def test_delta_refuses_tampered_base_and_package(tmp_path):
    baseline = tmp_path / "baseline"
    target = tmp_path / "target"
    day = date(2026, 8, 28)
    _write_bar(baseline, day, 10.0)
    _write_bar(target, day, 11.0)
    store = SnapshotStore(Config(data_root=target), tmp_path / "packages")
    store.create_delta("safe", baseline, target, ["daily_bars"])

    _write_bar(baseline, day, 99.0)
    with pytest.raises(ValueError, match="base mismatch"):
        store.apply_delta("safe", baseline)

    # Recreate the baseline and tamper with the package payload. Verification
    # must fail before any target mutation is attempted.
    _write_bar(baseline, day, 10.0)
    payload = next((store.delta_path("safe") / "data").rglob("*.parquet"))
    payload.write_bytes(b"tampered")
    result = store.verify_delta("safe")
    assert not result.passed
    with pytest.raises(ValueError, match="verification failed"):
        store.apply_delta("safe", baseline)
    assert pl.read_parquet(baseline / "curated/daily_bars/trade_date=2026-08-28/part.parquet")[
        "close"
    ].to_list() == [10.0]


def test_delta_rejects_dangling_symlink_parent_before_external_write(tmp_path):
    """A missing add target must not make a dangling parent link writable."""

    baseline = tmp_path / "baseline"
    target = tmp_path / "target"
    day = date(2026, 8, 28)
    _write_bar(baseline, day, 10.0)
    _write_bar(target, day, 10.0)
    # The state file is an intentional add in this delta.  Keeping the
    # baseline's state parent absent lets the test exercise mkdir/create
    # handling rather than a pre-existing regular destination.
    _state(target, day)

    store = SnapshotStore(Config(data_root=target), tmp_path / "packages")
    store.create_delta("dangling-parent", baseline, target, ["daily_bars"])
    assert any(
        item["path"] == "meta/state/daily_bars.json"
        for item in json.loads(
            store.delta_path("dangling-parent").joinpath("manifest.json").read_text()
        )["changes"]
    )

    external = tmp_path / "external"
    external.mkdir()
    meta = baseline / "meta"
    meta.mkdir()
    (meta / "state").symlink_to(external, target_is_directory=True)

    with pytest.raises(ValueError, match="symlink"):
        store.apply_delta("dangling-parent", baseline)
    assert not (external / "daily_bars.json").exists()
    assert not (meta / "state" / "daily_bars.json").exists()


def test_snapshot_restores_adjustment_cache_and_rebuilds_old_cache_shape(tmp_path):
    source = tmp_path / "source"
    _write_bar(source, date(2026, 8, 28), 10.0)
    derived = source / "derived" / "adj_factors" / "trade_date=2026-08-28"
    derived.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600000.SH"],
            "trade_date": [date(2026, 8, 28)],
            "adjust_type": ["hfq"],
            "factor": [0.5],
        }
    ).write_parquet(derived / "part.parquet")

    cfg = Config(data_root=source)
    store = SnapshotStore(cfg, tmp_path / "snapshots")
    store.create("warm", ["daily_bars", "adj_factors"])
    restored = store.restore("warm", tmp_path / "restored")
    cache = restored / "meta/adj_factors_cache/600000_SH_hfq.parquet"
    assert cache.is_file()
    assert pl.read_parquet(cache)["factor"].to_list() == [0.5]

    # Simulate a v1 snapshot made before cache inclusion.  The runtime state
    # says the source is rebuildable, and restore should create the cache from
    # the aligned derived table without network access.
    old_manifest = json.loads((store.path("warm") / "manifest.json").read_text())
    old_manifest["runtime_state"]["adj_factors_cache"] = {
        "mode": "rebuildable",
        "files": 0,
        "rebuild_from": "derived/adj_factors",
    }
    for item in old_manifest["files"]:
        if item["path"].startswith("meta/adj_factors_cache/"):
            item["path"] = item["path"]
    # Remove the cache records from the old package so no cache is copied.
    old_manifest["files"] = [
        item
        for item in old_manifest["files"]
        if not item["path"].startswith("meta/adj_factors_cache/")
    ]
    (store.path("warm") / "manifest.json").write_text(json.dumps(old_manifest), encoding="utf-8")
    rebuilt = store.restore("warm", tmp_path / "已重算")
    rebuilt_cache = rebuilt / "meta/adj_factors_cache/600000_SH_hfq.parquet"
    assert rebuilt_cache.is_file()
    assert pl.read_parquet(rebuilt_cache)["factor"].to_list() == [0.5]


def test_revision_delta_checks_revision_precondition(tmp_path):
    # Revision mode is intentionally offline: receipts are enough to package
    # the current bytes, and the integer revision protects the destination.
    target = tmp_path / "target"
    baseline = tmp_path / "baseline"
    day = date(2026, 8, 28)
    _write_bar(target, day, 10.0)
    _write_bar(baseline, day, 10.0)
    _state(target, day)
    _state(baseline, day)
    baseline_state = StateStore(baseline / "meta")
    baseline_state._write_payload(
        baseline_state._path("daily_bars"),
        {**baseline_state.get_payload("daily_bars"), "revision": 1},
    )

    from cnequity.storage.revisions import RevisionStore

    path = target / "curated/daily_bars/trade_date=2026-08-28/part.parquet"
    RevisionStore(target / "meta", target / "curated").commit(
        "daily_bars",
        run_id="r1",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    _write_bar(target, day, 11.0)
    revision_store = RevisionStore(target / "meta", target / "curated")
    revision_store.commit(
        "daily_bars",
        run_id="r2",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    StateStore(target / "meta")._write_payload(
        StateStore(target / "meta")._path("daily_bars"),
        {**StateStore(target / "meta").get_payload("daily_bars"), "revision": 2},
    )

    store = SnapshotStore(Config(data_root=target), tmp_path / "packages")
    store.create_delta_from_revision("revision-update", 1, ["daily_bars"])
    assert store.verify_delta("revision-update").passed
    store.apply_delta("revision-update", baseline)
    updated = baseline / "curated/daily_bars/trade_date=2026-08-28/part.parquet"
    assert pl.read_parquet(updated)["close"].to_list() == [11.0]


def test_revision_delta_from_rev2_snapshot_publishes_usable_rev3_chain(tmp_path):
    source = tmp_path / "source"
    path = source / "curated/daily_bars/trade_date=2026-08-28/part.parquet"
    _write_queryable_bar(source, date(2026, 8, 28), 10.0)
    from cnequity.storage.revisions import RevisionStore

    revisions = RevisionStore(source / "meta", source / "curated")
    first = revisions.commit(
        "daily_bars",
        run_id="r1",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert first is not None
    _write_queryable_bar(source, date(2026, 8, 28), 11.0)
    second = revisions.commit(
        "daily_bars",
        run_id="r2",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert second is not None and second.revision == 2

    snapshots = SnapshotStore(Config(data_root=source), tmp_path / "snapshots")
    snapshots.create("rev2", ["daily_bars"])
    baseline = snapshots.restore("rev2", tmp_path / "baseline")

    _write_queryable_bar(source, date(2026, 8, 28), 12.0)
    third = revisions.commit(
        "daily_bars",
        run_id="r3",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert third is not None and third.revision == 3

    update = SnapshotStore(Config(data_root=source), tmp_path / "delta-packages")
    update.create_delta_from_revision("rev3", 2, ["daily_bars"])
    assert update.verify_delta("rev3").passed
    update.apply_delta("rev3", baseline)
    assert load("daily_bars", config=Config(data_root=baseline), revision=2)["close"].to_list() == [
        11.0
    ]
    assert load("daily_bars", config=Config(data_root=baseline), revision=3)["close"].to_list() == [
        12.0
    ]


def test_delta_defers_deleting_old_cow_generation_until_pointer_switch(tmp_path):
    baseline = tmp_path / "baseline"
    target = tmp_path / "target"
    baseline_path = baseline / "curated/daily_bars/trade_date=2026-08-28/part.parquet"
    target_path = target / "curated/daily_bars/trade_date=2026-08-28/part.parquet"
    _write_queryable_bar(baseline, date(2026, 8, 28), 10.0)
    _write_queryable_bar(target, date(2026, 8, 28), 10.0)
    from cnequity.storage.revisions import RevisionStore

    baseline_revisions = RevisionStore(baseline / "meta", baseline / "curated")
    target_revisions = RevisionStore(target / "meta", target / "curated")
    baseline_first = baseline_revisions.commit(
        "daily_bars",
        run_id="r1",
        changed_files=[baseline_path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    target_first = target_revisions.commit(
        "daily_bars",
        run_id="r1",
        changed_files=[target_path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert baseline_first is not None and target_first is not None
    _write_queryable_bar(baseline, date(2026, 8, 28), 11.0)
    _write_queryable_bar(target, date(2026, 8, 28), 11.0)
    assert (
        baseline_revisions.commit(
            "daily_bars",
            run_id="r2",
            changed_files=[baseline_path],
            schema_version=1,
            contract_fingerprint="contract",
        )
        is not None
    )
    target_second = target_revisions.commit(
        "daily_bars",
        run_id="r2",
        changed_files=[target_path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert target_second is not None
    old_generation = target_revisions.generation_root("daily_bars", target_first.revision_id)
    old_receipt = (
        target_revisions.root
        / "daily_bars"
        / (f"{target_first.revision:08d}-{target_first.revision_id}.json")
    )
    # The target has already garbage-collected its old revision; the delta must
    # remove those paths from the baseline only after publishing its pointer.
    shutil.rmtree(old_generation)
    old_receipt.unlink()

    store = SnapshotStore(Config(data_root=target), tmp_path / "packages")
    store.create_delta("pruned-cow", baseline, target, ["daily_bars"])
    changes = json.loads(store.delta_path("pruned-cow").joinpath("manifest.json").read_text())[
        "changes"
    ]
    assert any(
        item["operation"] == "delete" and "revisions/data" in item["path"] for item in changes
    )
    store.apply_delta("pruned-cow", baseline)
    assert load("daily_bars", config=Config(data_root=baseline))["close"].to_list() == [11.0]
    assert store._lake_index(baseline, ["daily_bars"]) == store._lake_index(target, ["daily_bars"])


@pytest.mark.parametrize("orphan_kind", ["generation", "receipt"])
def test_revision_delta_rollback_restores_preexisting_protected_orphan_bytes(
    tmp_path, monkeypatch, orphan_kind
):
    """A failed rev3 publication must restore bytes already at protected paths."""
    from cnequity.storage.revisions import RevisionStore

    source = tmp_path / "source"
    source_cfg = Config(data_root=source)
    day = date(2026, 8, 28)
    mutable = source / "curated/daily_bars/trade_date=2026-08-28/part.parquet"
    _write_queryable_bar(source, day, 10.0)
    revisions = RevisionStore(source_cfg.meta_root, source_cfg.curated_root)
    first = revisions.commit(
        "daily_bars",
        run_id="r1",
        changed_files=[mutable],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert first is not None and first.revision == 1
    _write_queryable_bar(source, day, 11.0)
    second = revisions.commit(
        "daily_bars",
        run_id="r2",
        changed_files=[mutable],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert second is not None and second.revision == 2

    packages = tmp_path / "packages"
    store = SnapshotStore(source_cfg, packages)
    store.create("rev2", ["daily_bars"])
    baseline = store.restore("rev2", tmp_path / "baseline")

    _write_queryable_bar(source, day, 12.0)
    third = revisions.commit(
        "daily_bars",
        run_id="r3",
        changed_files=[mutable],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert third is not None and third.revision == 3
    store.create_delta_from_revision("rev3", 2, ["daily_bars"], target_data_root=source)

    generation_file = Path(*Path(third.generation_files[0].path).parts[1:])
    if orphan_kind == "generation":
        orphan = baseline / "meta" / third.generation_path / generation_file
        orphan.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(
            revisions.generation_root("daily_bars", third.revision_id) / generation_file,
            orphan,
        )
        pl.read_parquet(orphan).with_columns(pl.lit(99.0).alias("close")).write_parquet(orphan)
    else:
        receipt_relative = Path("revisions/daily_bars") / (
            f"{third.revision:08d}-{third.revision_id}.json"
        )
        orphan = baseline / "meta" / receipt_relative
        orphan.parent.mkdir(parents=True, exist_ok=True)
        orphan_payload = {
            "dataset": "daily_bars",
            "revision": third.revision,
            "revision_id": "orphan",
            "generation_path": second.generation_path,
            "orphan_marker": "99",
        }
        orphan.write_text(json.dumps(orphan_payload, sort_keys=True), encoding="utf-8")

    before_hashes = _file_hashes(baseline)
    before_pointer = json.loads(
        (baseline / "meta/revisions/daily_bars/current.json").read_text(encoding="utf-8")
    )

    import cnequity.storage.snapshots as snapshot_module

    original_digest = snapshot_module._index_digest
    digest_calls = 0

    def fail_post_fingerprint(index):
        nonlocal digest_calls
        digest_calls += 1
        if digest_calls == 1:
            return original_digest(index)
        return "injected-post-fingerprint-mismatch"

    monkeypatch.setattr(snapshot_module, "_index_digest", fail_post_fingerprint)
    with pytest.raises(ValueError, match="post-apply fingerprint mismatch"):
        store.apply_delta("rev3", baseline)

    assert digest_calls >= 2
    assert _file_hashes(baseline) == before_hashes
    assert (
        json.loads(
            (baseline / "meta/revisions/daily_bars/current.json").read_text(encoding="utf-8")
        )
        == before_pointer
    )
    assert load("daily_bars", config=Config(data_root=baseline))["close"].to_list() == [11.0]
    assert not list((baseline / "meta/applied-deltas").glob("*.json"))
    if orphan_kind == "generation":
        assert pl.read_parquet(orphan)["close"].to_list() == [99.0]
    else:
        assert json.loads(orphan.read_text(encoding="utf-8"))["orphan_marker"] == "99"


def test_restore_then_isolated_incremental_delta_has_no_network_dependency(tmp_path):
    source = tmp_path / "source"
    _write_bar(source, date(2026, 8, 28), 10.0)
    _state(source, date(2026, 8, 28))
    snapshot_store = SnapshotStore(Config(data_root=source), tmp_path / "packages")
    snapshot_store.create("baseline", ["daily_bars"])
    restored = snapshot_store.restore("baseline", tmp_path / "restored")

    # The next run is built in a separate work directory.  It can be produced
    # by an offline fixture or by a real daily run; no source adapter is called
    # by this test.  Only the new partition and watermark differ.
    next_lake = tmp_path / "next"
    shutil.copytree(restored, next_lake)
    _write_bar(next_lake, date(2026, 8, 29), 11.0)
    _state(next_lake, date(2026, 8, 29))

    update_store = SnapshotStore(Config(data_root=next_lake), tmp_path / "packages")
    update_store.create_delta("after-restore", restored, next_lake, ["daily_bars"])
    assert update_store.verify_delta("after-restore").passed
    update_store.apply_delta("after-restore", restored)
    assert update_store._lake_index(restored, ["daily_bars"]) == update_store._lake_index(
        next_lake, ["daily_bars"]
    )


def test_recovery_drill_restore_query_audit_and_rejects_corrupt_archive(tmp_path):
    """Exercise archive recovery and the next fixture-backed daily update."""

    day_one = date(2026, 8, 28)
    day_two = date(2026, 8, 29)
    source = tmp_path / "source"
    _write_queryable_bar(source, day_one, 10.0)
    _state(source, day_one)
    packages = tmp_path / "packages"
    store = SnapshotStore(Config(data_root=source), packages)
    store.create("baseline", ["daily_bars"])
    archive = store.export_archive("baseline", tmp_path / "baseline.tar.gz")

    damaged = tmp_path / "damaged.tar.gz"
    damaged.write_bytes(archive.read_bytes()[:-32])
    with pytest.raises((OSError, RuntimeError, ValueError, EOFError, tarfile.ReadError)):
        store.import_archive(damaged, name="damaged")
    assert not store.path("damaged").exists()

    store.import_archive(archive, name="baseline-imported")
    assert store.verify("baseline-imported").passed
    restored = store.restore("baseline-imported", tmp_path / "restored")

    queried = load(
        "daily_bars",
        config=Config(data_root=restored),
        start=day_one,
        end=day_one,
    )
    assert queried.select("symbol", "close").to_dicts() == [{"symbol": "600000.SH", "close": 10.0}]
    findings = audit_curated_dataset(
        "daily_bars",
        "trade_date",
        restored / "curated" / "daily_bars",
        day_one,
        full=True,
    )
    assert not [item for item in findings if item.get("severity") == "error"]

    next_lake = tmp_path / "next"
    shutil.copytree(restored, next_lake)
    _write_queryable_bar(next_lake, day_two, 11.0)
    _state(next_lake, day_two)
    update_store = SnapshotStore(Config(data_root=next_lake), packages)
    update_store.create_delta("after-recovery", restored, next_lake, ["daily_bars"])
    assert update_store.verify_delta("after-recovery").passed
    update_store.apply_delta("after-recovery", restored)

    recovered = load(
        "daily_bars",
        config=Config(data_root=restored),
        start=day_two,
        end=day_two,
    )
    assert recovered.select("symbol", "close").to_dicts() == [
        {"symbol": "600000.SH", "close": 11.0}
    ]
    post_findings = audit_curated_dataset(
        "daily_bars",
        "trade_date",
        restored / "curated" / "daily_bars",
        day_two,
        full=True,
    )
    assert not [item for item in post_findings if item.get("severity") == "error"]


def test_delta_cli_create_verify_apply(tmp_path):
    baseline = tmp_path / "baseline"
    target = tmp_path / "target"
    _write_bar(baseline, date(2026, 8, 28), 10.0)
    _write_bar(target, date(2026, 8, 28), 11.0)
    config_path = tmp_path / "cnequity.toml"
    config_path.write_text(f'[data]\nroot = "{path_for_toml(target)}"\n', encoding="utf-8")
    package_root = tmp_path / "packages"
    runner = CliRunner()
    common = ["--config", str(config_path), "--snapshot-root", str(package_root)]
    created = runner.invoke(
        cli,
        [
            "snapshot",
            "delta",
            "create",
            "cli-update",
            "--from",
            str(baseline),
            "--dataset",
            "daily_bars",
            *common,
        ],
    )
    assert created.exit_code == 0, created.output
    verified = runner.invoke(cli, ["snapshot", "delta", "verify", "cli-update", *common])
    assert verified.exit_code == 0, verified.output
    applied = runner.invoke(
        cli,
        ["snapshot", "delta", "apply", "cli-update", str(baseline), *common],
    )
    assert applied.exit_code == 0, applied.output
