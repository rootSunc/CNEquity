import shutil
import uuid
from datetime import date
from pathlib import Path

import polars as pl
import pytest
from tests.legacy_lake import commit_generation

from cnequity.config import Config
from cnequity.query import dataset_revisions, dataset_state
from cnequity.storage.parquet import StagingWriter, compact_dataset
from cnequity.storage.revisions import RevisionStore


def _curated_file(root: Path, value: bytes = b"first") -> Path:
    path = root / "daily_bars" / "trade_date=2024-06-18" / "part-merged.parquet"
    path.parent.mkdir(parents=True)
    path.write_bytes(value)
    return path


def test_revision_commit_publishes_receipt_and_state(tmp_path):
    curated = tmp_path / "curated"
    path = _curated_file(curated)
    store = RevisionStore(tmp_path / "meta", curated)

    receipt = commit_generation(
        store,
        "daily_bars",
        run_id="run-1",
        changed_files=[path],
        schema_version=2,
        contract_fingerprint="contract-sha",
    )

    assert receipt is not None
    assert receipt.revision == 1
    assert receipt.changed_partitions == ("daily_bars/trade_date=2024-06-18",)
    assert receipt.files[0].path == "daily_bars/trade_date=2024-06-18/part-merged.parquet"
    assert receipt.files[0].size_bytes == len(b"first")
    state = store.state.get_payload("daily_bars")
    assert state["revision"] == 1
    assert state["revision_receipt"] == (
        f"revisions/daily_bars/{receipt.revision:08d}-{receipt.revision_id}.json"
    )
    assert state["revision_id"] == receipt.revision_id
    assert state["content_digest"] == receipt.content_digest
    assert store.latest("daily_bars") == receipt


def test_revision_increments_when_an_old_partition_changes(tmp_path):
    curated = tmp_path / "curated"
    path = _curated_file(curated)
    store = RevisionStore(tmp_path / "meta", curated)
    first = commit_generation(
        store,
        "daily_bars",
        run_id="run-1",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="contract-sha",
    )
    path.write_bytes(b"historical repair")
    second = commit_generation(
        store,
        "daily_bars",
        run_id="run-2",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="contract-sha",
    )

    assert first is not None and second is not None
    assert second.revision == first.revision + 1
    assert second.content_digest != first.content_digest


def test_compact_reports_only_physical_dataset_changes(tmp_path):
    staging = tmp_path / "staging"
    curated = tmp_path / "curated"
    writer = StagingWriter(staging)
    row = pl.DataFrame(
        {
            "symbol": ["600000.SH"],
            "trade_date": [date(2024, 6, 18)],
            "open": [10.0],
            "high": [10.5],
            "low": [9.8],
            "close": [10.2],
            "volume": [100.0],
            "amount": [1020.0],
            "source": ["test"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-18T10:00:00+00:00"],
        }
    )
    writer.write_batch("daily_bars", "run-1", "batch-1", row)
    first_changes: list[Path] = []
    compact_dataset(
        staging, curated, "daily_bars", "run-1", changed_files=first_changes, out_root=curated
    )
    assert len(first_changes) == 1

    writer.write_batch("daily_bars", "run-2", "batch-1", row)
    second_changes: list[Path] = []
    compact_dataset(
        staging, curated, "daily_bars", "run-2", changed_files=second_changes, out_root=curated
    )
    assert second_changes == []


def test_compact_ignores_fetch_timestamp_churn_but_keeps_business_changes(tmp_path):
    staging = tmp_path / "staging"
    curated = tmp_path / "curated"
    writer = StagingWriter(staging)
    base = {
        "symbol": ["600000.SH"],
        "trade_date": [date(2024, 6, 18)],
        "open": [10.0],
        "high": [10.5],
        "low": [9.8],
        "close": [10.2],
        "volume": [100.0],
        "amount": [1020.0],
        "source": ["test"],
        "data_version": ["v1"],
    }

    first = pl.DataFrame({**base, "fetched_at": ["2024-06-18T10:00:00+00:00"]})
    writer.write_batch("daily_bars", "run-1", "batch-1", first)
    first_changes: list[Path] = []
    compact_dataset(
        staging, curated, "daily_bars", "run-1", changed_files=first_changes, out_root=curated
    )
    assert len(first_changes) == 1
    path = first_changes[0]
    before = path.read_bytes()
    revisions = RevisionStore(tmp_path / "meta", curated)
    first_revision = commit_generation(
        revisions,
        "daily_bars",
        run_id="run-1",
        changed_files=first_changes,
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert first_revision is not None and first_revision.revision == 1

    # Reconciliation stamps a fresh observation time, but it is not a new
    # business row. The canonical partition and revision input stay untouched.
    same_business = pl.DataFrame({**base, "fetched_at": ["2024-06-18T11:00:00+00:00"]})
    writer.write_batch("daily_bars", "run-2", "batch-1", same_business)
    second_changes: list[Path] = []
    compact_dataset(
        staging, curated, "daily_bars", "run-2", changed_files=second_changes, out_root=curated
    )
    assert second_changes == []
    assert path.read_bytes() == before
    assert (
        commit_generation(
            revisions,
            "daily_bars",
            run_id="run-2",
            changed_files=second_changes,
            schema_version=1,
            contract_fingerprint="contract",
        )
        is None
    )
    assert revisions.state.get_revision("daily_bars") == 1

    changed_business = same_business.with_columns(pl.lit(10.3).alias("close"))
    writer.write_batch("daily_bars", "run-3", "batch-1", changed_business)
    third_changes: list[Path] = []
    compact_dataset(
        staging, curated, "daily_bars", "run-3", changed_files=third_changes, out_root=curated
    )
    assert third_changes == [path]
    assert pl.read_parquet(path)["close"].item() == 10.3
    third_revision = commit_generation(
        revisions,
        "daily_bars",
        run_id="run-3",
        changed_files=third_changes,
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert third_revision is not None and third_revision.revision == 2


def test_public_dataset_state_reads_committed_identity(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    path = _curated_file(cfg.curated_root)
    receipt = commit_generation(
        RevisionStore(cfg.meta_root, cfg.curated_root),
        "daily_bars",
        run_id="run-1",
        changed_files=[path],
        schema_version=3,
        contract_fingerprint="contract-sha",
    )
    assert receipt is not None

    state = dataset_state("daily_bars", config=cfg)
    assert state.revision == 1
    assert state.revision_id == receipt.revision_id
    assert state.schema_version == 3
    assert state.contract_fingerprint == "contract-sha"
    assert state.changed_partitions == ("daily_bars/trade_date=2024-06-18",)


def test_revision_store_rejects_user_meta_symlink_but_allows_macos_var_alias(tmp_path):
    real_meta = tmp_path / "real-meta"
    real_meta.mkdir()
    linked_meta = tmp_path / "linked-meta"
    linked_meta.symlink_to(real_meta, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        RevisionStore(linked_meta, tmp_path / "curated")

    # macOS exposes /var as a root-owned alias to /private/var.  It is a
    # trusted system boundary, not a user-controlled lake link; allow a
    # temporary child while still rejecting the configured child itself when
    # it is a user symlink.
    alias_root = Path("/var/tmp") / f"cnequity-revision-{uuid.uuid4().hex}"
    try:
        store = RevisionStore(alias_root / "meta", alias_root / "curated")
        assert store.meta_root == alias_root / "meta"
    finally:
        shutil.rmtree(alias_root, ignore_errors=True)


def test_revision_queries_do_not_initialize_an_empty_lake(tmp_path):
    from cnequity.storage.revisions import committed_revision, resolve_committed_root

    lake = tmp_path / "absent_lake"
    logical = lake / "curated" / "futures_bars"
    assert resolve_committed_root(logical) == logical
    assert committed_revision(logical) is None
    assert not lake.exists()


def test_public_dataset_revisions_lists_changes_after_a_revision(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    path = _curated_file(cfg.curated_root)
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    first = commit_generation(
        store,
        "daily_bars",
        run_id="run-1",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="contract-a",
    )
    later = cfg.curated_root / "daily_bars" / "trade_date=2026-10-28" / "part-merged.parquet"
    later.parent.mkdir(parents=True)
    later.write_bytes(b"later")
    second = commit_generation(
        store,
        "daily_bars",
        run_id="run-2",
        changed_files=[later],
        schema_version=1,
        contract_fingerprint="contract-b",
    )
    assert first is not None and second is not None

    changes = dataset_revisions("daily_bars", after=first.revision_id, config=cfg)
    assert [c.revision_id for c in changes] == [second.revision_id]
    assert changes[0].changed_partitions == ("daily_bars/trade_date=2026-10-28",)
    assert changes[0].contract_fingerprint == "contract-b"
    assert dataset_revisions("daily_bars", after=second.revision_id, config=cfg) == ()

    with pytest.raises(ValueError, match="not in the receipt history"):
        dataset_revisions("daily_bars", after="unknown", config=cfg)

    later.write_bytes(b"later again")
    third = commit_generation(
        store,
        "daily_bars",
        run_id="run-3",
        changed_files=[later],
        schema_version=1,
        contract_fingerprint="contract-b",
    )
    assert third is not None
    (cfg.meta_root / "revisions" / "daily_bars" / f"{2:08d}-{second.revision_id}.json").unlink()
    with pytest.raises(ValueError, match="the revision before 3 of daily_bars is missing"):
        dataset_revisions("daily_bars", after=first.revision_id, config=cfg)


def _commit(store, run_id, files):
    return commit_generation(
        store,
        "daily_bars",
        run_id=run_id,
        changed_files=files,
        schema_version=1,
        contract_fingerprint="contract-sha",
    )


def test_history_reads_receipts_written_before_the_base_link(tmp_path):
    import json

    cfg = Config(data_root=tmp_path / "data")
    path = _curated_file(cfg.curated_root)
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    first = _commit(store, "run-1", [path])
    later = []
    for value in (b"two", b"three"):
        path.write_bytes(value)
        later.append(_commit(store, f"run-{value.decode()}", [path]))
    # Receipts from before 2026-09-26 carry no base_revision_id.
    for receipt in (cfg.meta_root / "revisions" / "daily_bars").glob("[0-9]*-*.json"):
        payload = json.loads(receipt.read_text())
        payload["metadata"].pop("base_revision_id", None)
        receipt.write_text(json.dumps(payload))

    changes = dataset_revisions("daily_bars", after=first.revision_id, config=cfg)

    assert [c.revision_id for c in changes] == [r.revision_id for r in later]
