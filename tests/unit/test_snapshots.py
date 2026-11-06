import json
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.storage.snapshots import SnapshotStore
from cnequity.storage.state import StateStore


def _write_bars(cfg: Config) -> None:
    path = cfg.curated_root / "daily_bars" / "trade_date=2024-06-18"
    path.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600000.SH"],
            "trade_date": [date(2024, 6, 18)],
            "close": [10.0],
            "fetched_at": [datetime(2024, 6, 18, tzinfo=timezone.utc)],
        }
    ).write_parquet(path / "part-merged.parquet")


def test_snapshot_create_verify_and_restore(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _write_bars(cfg)
    StateStore(cfg.meta_root).set_date("daily_bars", date(2024, 6, 18))
    store = SnapshotStore(cfg)
    manifest = store.create("research-2024", ["daily_bars"])
    assert manifest.exists()
    verification = store.verify("research-2024")
    assert verification.passed and verification.verified_files == 2

    restored = store.restore("research-2024", tmp_path / "restored")
    file = restored / "curated" / "daily_bars" / "trade_date=2024-06-18" / "part-merged.parquet"
    assert file.exists()
    assert pl.read_parquet(file)["close"].to_list() == [10.0]
    assert (restored / "meta" / "restored-snapshot.json").exists()
    assert StateStore(restored / "meta").get_date("daily_bars") == date(2024, 6, 18)


def test_snapshot_accepts_a_calendar_whose_watermark_runs_ahead_of_today(tmp_path):
    """trading_calendar is written a year ahead; its rows back the watermark."""
    cfg = Config(data_root=tmp_path / "lake")
    day = date(2099, 1, 4)
    path = cfg.curated_root / "trading_calendar" / "trade_date=2099"
    path.mkdir(parents=True)
    pl.DataFrame(
        {
            "trade_date": [day],
            "is_trading": [True],
            "fetched_at": [datetime(2024, 6, 18, tzinfo=timezone.utc)],
        }
    ).write_parquet(path / "part-merged.parquet")
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

    real_dataset = real_cfg.curated_root / "daily_bars"
    external_dataset = tmp_path / "external-dataset"
    real_dataset.rename(external_dataset)
    real_dataset.symlink_to(external_dataset, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        SnapshotStore(real_cfg, tmp_path / "dataset-snapshots").create(
            "dataset-root-link", ["daily_bars"]
        )

    normal_store = SnapshotStore(real_cfg, tmp_path / "normal-snapshots")
    # Restore the old layout for the control snapshot after exercising the
    # source-root rejection above.
    real_dataset.unlink()
    external_dataset.rename(real_dataset)
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
    assert not (external / "curated").exists()

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


def test_snapshot_copies_and_restores_referenced_revision_receipt(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _write_bars(cfg)
    receipt_relative = Path("revisions/daily_bars/00000001-test.json")
    receipt = cfg.meta_root / receipt_relative
    receipt.parent.mkdir(parents=True)
    receipt.write_text(
        json.dumps(
            {
                "dataset": "daily_bars",
                "revision": 1,
                "revision_id": "test",
                "committed_at": "2026-08-29T00:00:00+00:00",
                "run_id": "run-test",
                "schema_version": 1,
                "contract_fingerprint": "contract-test",
                "content_digest": "content-test",
                "changed_partitions": [],
                "files": [],
                "metadata": {},
            }
        ),
        encoding="utf-8",
    )
    state = StateStore(cfg.meta_root)
    state._write_payload(
        state._path("daily_bars"),
        {
            "revision": 1,
            "revision_id": "test",
            "revision_receipt": receipt_relative.as_posix(),
        },
    )

    store = SnapshotStore(cfg)
    store.create("with-revision", ["daily_bars"])
    restored = store.restore("with-revision", tmp_path / "restored")

    restored_receipt = restored / "meta" / receipt_relative
    assert restored_receipt.is_file()
    assert json.loads(restored_receipt.read_text())["revision_id"] == "test"


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


def test_snapshot_verify_rejects_dataset_contract_and_pointer_state_tampering(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _write_bars(cfg)
    from cnequity.storage.revisions import RevisionStore

    data_file = cfg.curated_root / "daily_bars" / "trade_date=2024-06-18" / "part-merged.parquet"
    revision = RevisionStore(cfg.meta_root, cfg.curated_root).commit(
        "daily_bars",
        run_id="r1",
        changed_files=[data_file],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert revision is not None
    store = SnapshotStore(cfg, tmp_path / "snapshots")
    store.create("pointer-semantic", ["daily_bars"])
    manifest_path = store.path("pointer-semantic") / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["contracts"]["daily_bars"]["fingerprint"] = "forged-dataset-contract"
    manifest["dataset_states"]["daily_bars"]["revision"] = revision.revision + 1
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    verification = store.verify("pointer-semantic")
    assert not verification.passed
    assert any(item.startswith("contract:daily_bars") for item in verification.mismatched)
    assert "state:daily_bars:pointer" in verification.mismatched
    with pytest.raises(ValueError, match="verification failed"):
        store.restore("pointer-semantic", tmp_path / "restored")


def test_snapshot_create_refuses_state_pointer_mismatch(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _write_bars(cfg)
    from cnequity.storage.revisions import RevisionStore

    data_file = cfg.curated_root / "daily_bars" / "trade_date=2024-06-18" / "part-merged.parquet"
    revision = RevisionStore(cfg.meta_root, cfg.curated_root).commit(
        "daily_bars",
        run_id="r1",
        changed_files=[data_file],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert revision is not None
    state = StateStore(cfg.meta_root)
    payload = state.get_payload("daily_bars")
    payload["revision"] = revision.revision + 1
    state._write_payload(state._path("daily_bars"), payload)

    store = SnapshotStore(cfg, tmp_path / "snapshots")
    with pytest.raises(ValueError, match="consistency check"):
        store.create("inconsistent", ["daily_bars"])
    assert not store.path("inconsistent").exists()


def _commit_two_bar_generations(cfg: Config) -> Path:
    """Publish two generations; the unchanged day is hard-linked between them."""
    from cnequity.storage.revisions import RevisionStore

    _write_bars(cfg)
    second_dir = cfg.curated_root / "daily_bars" / "trade_date=2024-06-19"
    second_dir.mkdir(parents=True)
    second = second_dir / "part-merged.parquet"

    def write_second(close: float) -> None:
        pl.DataFrame(
            {
                "symbol": ["600000.SH"],
                "trade_date": [date(2024, 6, 19)],
                "close": [close],
                "fetched_at": [datetime(2024, 6, 19, tzinfo=timezone.utc)],
            }
        ).write_parquet(second)

    write_second(11.0)
    first = cfg.curated_root / "daily_bars" / "trade_date=2024-06-18" / "part-merged.parquet"
    revisions = RevisionStore(cfg.meta_root, cfg.curated_root)
    for run_id, changed in (("r1", [first, second]), ("r2", [second])):
        if run_id == "r2":
            write_second(12.0)
        assert (
            revisions.commit(
                "daily_bars",
                run_id=run_id,
                changed_files=changed,
                schema_version=1,
                contract_fingerprint="contract",
            )
            is not None
        )
    StateStore(cfg.meta_root).set_date("daily_bars", date(2024, 6, 19))
    current = revisions.current_root("daily_bars")
    assert current is not None
    return current / "trade_date=2024-06-18" / "part-merged.parquet"


def test_snapshot_copies_hard_linked_generation_files(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    linked = _commit_two_bar_generations(cfg)
    assert linked.stat().st_nlink > 1

    store = SnapshotStore(cfg, tmp_path / "snapshots")
    store.create("linked", ["daily_bars"])

    assert store.verify("linked").passed
    copies = list((store.path("linked")).rglob("trade_date=2024-06-18/*.parquet"))
    assert copies and all(path.stat().st_nlink == 1 for path in copies)
    restored = store.restore("linked", tmp_path / "restored")
    file = restored / "curated" / "daily_bars" / "trade_date=2024-06-19" / "part-merged.parquet"
    assert pl.read_parquet(file)["close"].to_list() == [12.0]


def test_snapshot_still_refuses_symlink_in_generation(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    linked = _commit_two_bar_generations(cfg)
    external = tmp_path / "outside.parquet"
    external.write_bytes(linked.read_bytes())
    linked.unlink()
    linked.symlink_to(external)

    store = SnapshotStore(cfg, tmp_path / "snapshots")
    with pytest.raises(ValueError, match="unsupported link or special file"):
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
