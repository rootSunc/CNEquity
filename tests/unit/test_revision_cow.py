import json
import shutil
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.file_lock import is_locked
from cnequity.query.parquet_scan import scan_parquet_root
from cnequity.query.reader import load, scan
from cnequity.query.universe import coverage_end_date, coverage_start_date
from cnequity.steps.common import load_bar_universe
from cnequity.storage.raw_archive import RawArchiveError, RawPayloadArchive
from cnequity.storage.revisions import RevisionConsistencyError, RevisionStore
from cnequity.storage.snapshots import SnapshotStore


def _bars(path: Path, day: date, close: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600000.SH"],
            "trade_date": [day],
            "open": [close],
            "high": [close],
            "low": [close],
            "close": [close],
            "volume": [100.0],
            "amount": [1000.0],
            "source": ["test"],
            "data_version": ["v1"],
            "fetched_at": ["2026-01-01T00:00:00+00:00"],
        }
    ).write_parquet(path)


def _revision_lake(root: Path, close: float) -> tuple[Config, RevisionStore, Path]:
    config = Config(data_root=root)
    path = root / "curated/daily_bars/trade_date=2026-01-01/part.parquet"
    _bars(path, date(2026, 1, 1), close)
    store = RevisionStore(config.meta_root, config.curated_root)
    receipt = store.commit(
        "daily_bars",
        run_id=f"r-{close}",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert receipt is not None
    return config, store, path


@pytest.mark.skipif(not Path("/tmp").is_symlink(), reason="needs the macOS /tmp alias")
def test_commit_accepts_lake_under_system_tmp_alias():
    root = Path(tempfile.mkdtemp(dir="/tmp")) / "lake"
    try:
        _commit_twice_through(root)
    finally:
        shutil.rmtree(root.parent, ignore_errors=True)


def _commit_twice_through(root: Path) -> None:
    cfg, store, path = _revision_lake(root, 10.0)
    _bars(path, date(2026, 1, 1), 11.0)
    receipt = store.commit(
        "daily_bars",
        run_id="r-again",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert receipt is not None
    assert load("daily_bars", config=cfg)["close"].to_list() == [11.0]


@pytest.mark.parametrize("changed_dataset", ["instruments", "trading_status"])
def test_revision_map_pins_universe_dependencies(tmp_path, changed_dataset):
    cfg, store, _ = _revision_lake(tmp_path, 10.0)
    day = date(2026, 1, 1)
    frames = {
        "instruments": pl.DataFrame(
            {"symbol": ["600000.SH"], "list_date": [date(2000, 1, 1)], "delist_date": [None]},
            schema_overrides={"delist_date": pl.Date},
        ),
        "trading_status": pl.DataFrame(
            {
                "symbol": ["600000.SH"],
                "trade_date": [day],
                "is_trading": [True],
                "status": ["normal"],
                "risk_warning": [False],
            }
        ),
    }
    for dataset, frame in frames.items():
        path = cfg.curated_root / dataset / "part.parquet"
        path.parent.mkdir(parents=True)
        frame.write_parquet(path)
        store.commit(
            dataset,
            run_id="initial",
            changed_files=[path],
            schema_version=1,
            contract_fingerprint="test",
        )
    pinned = {"daily_bars": 1, "instruments": 1, "trading_status": 1}
    assert load("daily_bars", config=cfg, universe="all_a", revision_map=pinned).height == 1
    frame = frames[changed_dataset].with_columns(
        pl.lit(date(2025, 12, 31)).alias("delist_date")
        if changed_dataset == "instruments"
        else pl.lit(True).alias("risk_warning")
    )
    path = cfg.curated_root / changed_dataset / "part.parquet"
    frame.write_parquet(path)
    store.commit(
        changed_dataset,
        run_id="correction",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="test",
    )
    assert load("daily_bars", config=cfg, universe="all_a").height == 0
    assert load("daily_bars", config=cfg, universe="all_a", revision_map=pinned).height == 1


def test_explicit_revision_cannot_silently_read_unversioned_lake(tmp_path):
    _bars(tmp_path / "curated/daily_bars/part.parquet", date(2026, 1, 1), 10.0)
    with pytest.raises(RevisionConsistencyError, match="no retained revision"):
        load("daily_bars", data_root=tmp_path, revision=1)


def test_cow_pointer_keeps_query_on_one_generation_after_failed_publish(tmp_path, monkeypatch):
    data = tmp_path / "data"
    cfg = Config(data_root=data)
    path = data / "curated/daily_bars/trade_date=2026-01-01/part.parquet"
    _bars(path, date(2026, 1, 1), 10.0)
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    first = store.commit(
        "daily_bars",
        run_id="r1",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert first is not None
    old_root = store.current_root("daily_bars")
    assert old_root is not None

    _bars(path, date(2026, 1, 1), 11.0)
    second_pointer = store.pointer_path("daily_bars")

    def fail_pointer(target, payload, **kwargs):
        if Path(target) == second_pointer:
            raise OSError("injected pointer failure")
        from cnequity.storage.atomic import write_json_atomic

        return write_json_atomic(target, payload, **kwargs)

    monkeypatch.setattr("cnequity.storage.revisions.write_json_atomic", fail_pointer)
    with pytest.raises(OSError, match="injected pointer failure"):
        store.commit(
            "daily_bars",
            run_id="r2",
            changed_files=[path],
            schema_version=1,
            contract_fingerprint="contract",
        )
    assert store.current_root("daily_bars") == old_root
    assert load("daily_bars", config=cfg)["close"].to_list() == [10.0]


def test_snapshot_create_holds_lake_lock_while_commit_waits(tmp_path, monkeypatch):
    data = tmp_path / "data"
    cfg, revisions, path = _revision_lake(data, 10.0)
    _bars(path, date(2026, 1, 1), 11.0)
    snapshots = SnapshotStore(cfg, tmp_path / "snapshots")
    entered = threading.Event()
    release = threading.Event()
    original_source_root = snapshots._source_root

    def blocked_source_root(dataset: str):
        assert is_locked(cfg.meta_root / "locks" / "compact.lock")
        entered.set()
        assert release.wait(timeout=5)
        return original_source_root(dataset)

    monkeypatch.setattr(snapshots, "_source_root", blocked_source_root)
    with ThreadPoolExecutor(max_workers=2) as executor:
        snapshot_future = executor.submit(snapshots.create, "during-commit", ["daily_bars"])
        assert entered.wait(timeout=5)
        commit_future = executor.submit(
            revisions.commit,
            "daily_bars",
            run_id="r2",
            changed_files=[path],
            schema_version=1,
            contract_fingerprint="contract",
        )
        time.sleep(0.05)
        assert not commit_future.done()
        release.set()
        snapshot_future.result(timeout=10)
        second = commit_future.result(timeout=10)
    assert second is not None and second.revision == 2
    restored = snapshots.restore("during-commit", tmp_path / "restored")
    assert load("daily_bars", config=Config(data_root=restored))["close"].to_list() == [10.0]


def test_delta_pointer_switch_keeps_lock_free_readers_on_complete_generations(
    tmp_path, monkeypatch
):
    baseline_cfg, _, _ = _revision_lake(tmp_path / "baseline", 10.0)
    target_cfg, target_revisions, target_path = _revision_lake(tmp_path / "target", 10.0)
    _bars(target_path, date(2026, 1, 1), 11.0)
    second = target_revisions.commit(
        "daily_bars",
        run_id="r2",
        changed_files=[target_path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert second is not None
    snapshots = SnapshotStore(target_cfg, tmp_path / "snapshots")
    snapshots.create_delta(
        "reader-safe", baseline_cfg.data_root, target_cfg.data_root, ["daily_bars"]
    )

    import cnequity.storage.snapshots as snapshot_module

    original_copy2 = snapshot_module.shutil.copy2
    copy_started = threading.Event()

    def slow_copy2(source, destination, *args, **kwargs):
        copy_started.set()
        time.sleep(0.003)
        return original_copy2(source, destination, *args, **kwargs)

    monkeypatch.setattr(snapshot_module.shutil, "copy2", slow_copy2)
    stop = threading.Event()
    consistency_errors: list[RevisionConsistencyError] = []
    observed: list[float] = []

    def read_while_applying() -> None:
        while not stop.is_set():
            frame = load("daily_bars", config=baseline_cfg)
            if not frame.is_empty():
                observed.extend(frame["close"].to_list())
                assert set(observed) <= {10.0, 11.0}

    def collect_consistency_errors() -> None:
        try:
            read_while_applying()
        except RevisionConsistencyError as exc:
            consistency_errors.append(exc)

    with ThreadPoolExecutor(max_workers=2) as executor:
        reader_future = executor.submit(collect_consistency_errors)
        apply_future = executor.submit(snapshots.apply_delta, "reader-safe", baseline_cfg.data_root)
        assert copy_started.wait(timeout=5)
        apply_future.result(timeout=15)
        stop.set()
        reader_future.result(timeout=5)

    assert not consistency_errors
    assert observed
    assert load("daily_bars", config=baseline_cfg)["close"].to_list() == [11.0]


def test_delta_receipt_failure_rolls_back_pointer_and_generation(tmp_path, monkeypatch):
    baseline_cfg, _, _ = _revision_lake(tmp_path / "baseline", 10.0)
    target_cfg, target_revisions, target_path = _revision_lake(tmp_path / "target", 10.0)
    _bars(target_path, date(2026, 1, 1), 11.0)
    second = target_revisions.commit(
        "daily_bars",
        run_id="r2",
        changed_files=[target_path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert second is not None
    snapshots = SnapshotStore(target_cfg, tmp_path / "snapshots")
    snapshots.create_delta("rollback", baseline_cfg.data_root, target_cfg.data_root, ["daily_bars"])
    before_index = snapshots._lake_index(baseline_cfg.data_root, ["daily_bars"])
    before_pointer = json.loads(
        (baseline_cfg.meta_root / "revisions/daily_bars/current.json").read_text(encoding="utf-8")
    )

    import cnequity.storage.snapshots as snapshot_module

    original_write = snapshot_module.write_json_atomic

    def fail_application_receipt(path, *args, **kwargs):
        if "applied-deltas" in str(path):
            raise OSError("injected application receipt failure")
        return original_write(path, *args, **kwargs)

    monkeypatch.setattr(snapshot_module, "write_json_atomic", fail_application_receipt)
    with pytest.raises(OSError, match="application receipt"):
        snapshots.apply_delta("rollback", baseline_cfg.data_root)
    assert snapshots._lake_index(baseline_cfg.data_root, ["daily_bars"]) == before_index
    assert (
        json.loads(
            (baseline_cfg.meta_root / "revisions/daily_bars/current.json").read_text(
                encoding="utf-8"
            )
        )
        == before_pointer
    )
    assert load("daily_bars", config=baseline_cfg)["close"].to_list() == [10.0]


def test_snapshot_roundtrip_preserves_current_pointer_generation(tmp_path):
    data = tmp_path / "data"
    cfg = Config(data_root=data)
    path = data / "curated/daily_bars/trade_date=2026-01-01/part.parquet"
    _bars(path, date(2026, 1, 1), 10.0)
    RevisionStore(cfg.meta_root, cfg.curated_root).commit(
        "daily_bars",
        run_id="r1",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    snapshots = SnapshotStore(cfg, tmp_path / "snapshots")
    snapshots.create("baseline", ["daily_bars"])
    restored = snapshots.restore("baseline", tmp_path / "restored")
    restored_cfg = Config(data_root=restored)
    assert load("daily_bars", config=restored_cfg)["close"].to_list() == [10.0]


def test_corrupt_pointer_fails_closed_instead_of_falling_back(tmp_path):
    data = tmp_path / "data"
    cfg = Config(data_root=data)
    path = data / "curated/daily_bars/trade_date=2026-01-01/part.parquet"
    _bars(path, date(2026, 1, 1), 10.0)
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    store.ensure_current("daily_bars")
    store.pointer_path("daily_bars").write_text("{}", encoding="utf-8")
    with pytest.raises(RevisionConsistencyError):
        scan_parquet_root(data / "curated/daily_bars", partition_col="trade_date")


def test_pointer_only_reads_cover_reader_scan_and_universe_paths(tmp_path):
    data = tmp_path / "data"
    cfg = Config(data_root=data)
    day = date(2026, 1, 1)
    path = data / "curated/daily_bars/trade_date=2026-01-01/part.parquet"
    _bars(path, day, 10.0)

    instruments = data / "curated/instruments/part.parquet"
    instruments.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600000.SH"],
            "name": ["fixture"],
            "exchange": ["SH"],
            "asset_type": ["stock"],
            "list_date": [date(2020, 1, 1)],
            "delist_date": [None],
            "prev_symbol": [None],
            "source": ["fixture"],
            "data_version": ["v1"],
            "fetched_at": ["2026-01-01T00:00:00+00:00"],
        }
    ).write_parquet(instruments)

    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    receipt = store.commit(
        "daily_bars",
        run_id="r1",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert receipt is not None
    shutil.rmtree(data / "curated/daily_bars")

    loaded = load("daily_bars", config=cfg)
    assert loaded.select("symbol", "close").to_dicts() == [{"symbol": "600000.SH", "close": 10.0}]
    assert scan("daily_bars", config=cfg).collect().height == 1
    assert load_bar_universe(cfg) == {"600000.SH"}
    assert coverage_start_date(cfg, "daily_bars") == day
    assert coverage_end_date(cfg, "daily_bars") == day
    assert load("daily_bars", config=cfg, universe="all_a").height == 1


def test_derived_revision_cow_isolates_fixed_curated_and_derived_reads(tmp_path):
    data = tmp_path / "data"
    cfg = Config(data_root=data)
    day = date(2026, 1, 1)
    bars_path = data / "curated/daily_bars/trade_date=2026-01-01/part.parquet"
    factors_path = data / "derived/adj_factors/trade_date=2026-01-01/part.parquet"
    _bars(bars_path, day, 10.0)
    factors_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600000.SH"],
            "trade_date": [day],
            "adjust_type": ["hfq"],
            "factor": [0.5],
            "source": ["derived"],
            "data_version": ["v1"],
            "fetched_at": ["2026-01-01T00:00:00+00:00"],
        }
    ).write_parquet(factors_path)

    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    first_bar = store.commit(
        "daily_bars",
        run_id="r1",
        changed_files=[bars_path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    first_factor = store.commit(
        "adj_factors",
        run_id="r1",
        changed_files=[factors_path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert first_bar is not None and first_factor is not None

    _bars(bars_path, day, 11.0)
    pl.DataFrame(
        {
            "symbol": ["600000.SH"],
            "trade_date": [day],
            "adjust_type": ["hfq"],
            "factor": [0.75],
            "source": ["derived"],
            "data_version": ["v1"],
            "fetched_at": ["2026-01-02T00:00:00+00:00"],
        }
    ).write_parquet(factors_path)
    second_bar = store.commit(
        "daily_bars",
        run_id="r2",
        changed_files=[bars_path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    second_factor = store.commit(
        "adj_factors",
        run_id="r2",
        changed_files=[factors_path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert second_bar is not None and second_factor is not None

    assert load("daily_bars", config=cfg, revision=first_bar.revision)["close"].to_list() == [10.0]
    assert load("daily_bars", config=cfg, revision=second_bar.revision)["close"].to_list() == [11.0]
    assert load("adj_factors", config=cfg, revision=first_factor.revision)["factor"].to_list() == [
        0.5
    ]
    assert load("adj_factors", config=cfg, revision=second_factor.revision)["factor"].to_list() == [
        0.75
    ]


def test_adjusted_pinned_read_uses_per_dataset_revision_map(tmp_path):
    data = tmp_path / "data"
    cfg = Config(data_root=data)
    day = date(2026, 1, 1)
    bars_path = data / "curated/daily_bars/trade_date=2026-01-01/part.parquet"
    factors_path = data / "derived/adj_factors/trade_date=2026-01-01/part.parquet"
    _bars(bars_path, day, 10.0)
    factors_path.parent.mkdir(parents=True, exist_ok=True)
    factor_frame = pl.DataFrame(
        {
            "symbol": ["600000.SH"],
            "trade_date": [day],
            "adjust_type": ["hfq"],
            "factor": [0.5],
            "source": ["derived"],
            "data_version": ["v1"],
            "fetched_at": ["2026-01-01T00:00:00+00:00"],
        }
    )
    factor_frame.write_parquet(factors_path)
    revisions = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    bar_revision = revisions.commit(
        "daily_bars",
        run_id="bars",
        changed_files=[bars_path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    factor_one = revisions.commit(
        "adj_factors",
        run_id="factors-1",
        changed_files=[factors_path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert bar_revision is not None and factor_one is not None

    factor_frame.with_columns(pl.lit(0.75).alias("factor")).write_parquet(factors_path)
    factor_two = revisions.commit(
        "adj_factors",
        run_id="factors-2",
        changed_files=[factors_path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert factor_two is not None

    # The scalar is a daily-bars selection.  The factor read must stay on its
    # current revision (0.75), rather than interpreting daily revision 1 as a
    # factor revision and returning 0.5.
    scalar = load("daily_bars", config=cfg, adjust="hfq", revision=bar_revision.revision)
    assert scalar["adj_close"].to_list() == [7.5]
    explicit = load(
        "daily_bars",
        config=cfg,
        adjust="hfq",
        revision={"daily_bars": bar_revision.revision, "adj_factors": factor_one.revision},
    )
    assert explicit["adj_close"].to_list() == [5.0]


def test_revision_commits_serialize_complete_derived_generations(tmp_path):
    data = tmp_path / "data"
    cfg = Config(data_root=data)
    factors = []
    for index, value in enumerate((0.5, 0.75), start=1):
        path = data / f"derived/adj_factors/trade_date=2026-01-0{index}/part.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "symbol": ["600000.SH"],
                "trade_date": [date(2026, 1, index)],
                "adjust_type": ["hfq"],
                "factor": [value],
                "source": ["derived"],
                "data_version": ["v1"],
                "fetched_at": [f"2026-01-0{index}T00:00:00+00:00"],
            }
        ).write_parquet(path)
        factors.append(path)

    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    first = store.commit(
        "adj_factors",
        run_id="r0",
        changed_files=[factors[0]],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert first is not None

    def commit(path: Path) -> int:
        revision = store.commit(
            "adj_factors",
            run_id=path.stem,
            changed_files=[path],
            schema_version=1,
            contract_fingerprint="contract",
        )
        assert revision is not None
        return revision.revision

    with ThreadPoolExecutor(max_workers=2) as executor:
        revisions = sorted(executor.map(commit, [factors[0], factors[1]]))
    assert revisions == [2, 3]
    current = store.current_root("adj_factors")
    assert current is not None
    assert len(list(current.rglob("*.parquet"))) == 2


def test_raw_archive_redacts_secrets_and_replays_without_network(tmp_path):
    archive = RawPayloadArchive(tmp_path / "meta")
    record = archive.archive(
        "financial_statement_items",
        {"value": 1},
        source="eastmoney",
        request_params={"token": "secret", "filter": {"proxy": "http://proxy"}},
        url="https://example.test/data?token=secret&date=2026-01-01",
    )
    assert record is not None
    metadata = (tmp_path / "meta" / record.metadata_path).read_text(encoding="utf-8")
    assert "secret" not in metadata
    assert "http://proxy" not in metadata
    assert archive.replay(record, lambda payload: payload.decode("utf-8")) == '{"value": 1}'


def test_raw_archive_rejects_same_size_payload_tampering(tmp_path):
    archive = RawPayloadArchive(tmp_path / "meta")
    record = archive.archive(
        "financial_statement_items",
        {"value": 1},
        source="eastmoney",
    )
    assert record is not None
    payload_path = tmp_path / "meta" / record.payload_path
    original = bytearray(payload_path.read_bytes())
    original[len(original) // 2] ^= 0x01
    assert len(original) == record.compressed_bytes
    payload_path.write_bytes(original)

    with pytest.raises(RawArchiveError):
        archive.read(record)
    with pytest.raises(RawArchiveError):
        archive.records("financial_statement_items")
    with pytest.raises(RawArchiveError):
        archive.archive("financial_statement_items", {"value": 1}, source="eastmoney")


def test_retention_keeps_the_pointer_and_the_newest_generations(tmp_path):
    """The preview deletes nothing; retention deletes exactly what it previewed."""
    from cnequity.storage.revisions import (
        preview_revision_retention,
        retain_revision_generations,
    )

    cfg = Config(data_root=tmp_path / "data")
    path = cfg.curated_root / "daily_bars/trade_date=2026-01-01/part.parquet"
    store = RevisionStore(cfg.meta_root, cfg.curated_root)

    for index in range(6):
        _bars(path, date(2026, 1, 1), 10.0 + index)
        assert store.commit(
            "daily_bars",
            run_id=f"r-{index}",
            changed_files=[path],
            schema_version=1,
            contract_fingerprint="contract",
        )

    generations = cfg.meta_root / "revisions" / "data" / "daily_bars"
    receipts = cfg.meta_root / "revisions" / "daily_bars"
    before = {p.name for p in generations.iterdir()}
    assert len(before) >= 6
    receipts_before = {p.name for p in receipts.glob("*.json")}
    current_before = store.current_root("daily_bars")

    preview = preview_revision_retention(cfg.meta_root, keep=2)
    planned = {rid for item in preview for rid in item.candidate_revision_ids}
    assert planned, "there is something to delete"
    assert {p.name for p in generations.iterdir()} == before, "a preview must not delete"

    removed = retain_revision_generations(cfg.meta_root, keep=2)

    assert {rid for item in removed for rid in item.removed_revision_ids} == planned
    assert {p.name for p in generations.iterdir()} == before - planned
    # The generation every reader resolves to survived, and still reads.
    assert current_before is not None and current_before.is_dir()
    assert store.current_root("daily_bars") == current_before
    assert load("daily_bars", config=cfg)["close"].to_list() == [15.0]
    # Lineage is untouched: receipts are the record, generations are the bytes.
    assert {p.name for p in receipts.glob("*.json")} == receipts_before


def test_retention_keeps_what_the_pointer_resolves_to_even_at_keep_one(tmp_path):
    """`keep=1` is the most aggressive setting and must still leave a readable lake."""
    from cnequity.storage.revisions import retain_revision_generations

    cfg, store, _path = _revision_lake(tmp_path, 10.0)
    generations = cfg.meta_root / "revisions" / "data" / "daily_bars"
    current = store.current_root("daily_bars")

    retain_revision_generations(cfg.meta_root, keep=1)

    assert current is not None and current.is_dir()
    assert store.current_root("daily_bars") == current
    assert load("daily_bars", config=cfg)["close"].to_list() == [10.0]
    assert list(generations.iterdir()), "at least the pointed-to generation remains"


def test_commit_excludes_abandoned_mutable_partition(tmp_path):
    cfg, store, first = _revision_lake(tmp_path, 10.0)
    _bars(first, date(2026, 1, 1), 99.0)
    second = cfg.curated_root / "daily_bars/trade_date=2026-01-02/part.parquet"
    _bars(second, date(2026, 1, 2), 20.0)
    store.commit(
        "daily_bars",
        run_id="next",
        changed_files=[second],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert load("daily_bars", config=cfg).sort("trade_date")["close"].to_list() == [10.0, 20.0]


def test_dataset_state_uses_pointer_after_state_write_interruption(tmp_path):
    from cnequity.query.state import dataset_state

    cfg, store, path = _revision_lake(tmp_path, 10.0)
    state_path = cfg.meta_root / "state/daily_bars.json"
    old_state = state_path.read_bytes()
    _bars(path, date(2026, 1, 1), 20.0)
    receipt = store.commit(
        "daily_bars",
        run_id="next",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    state_path.write_bytes(old_state)
    assert dataset_state("daily_bars", config=cfg).revision_id == receipt.revision_id
    assert load("daily_bars", config=cfg)["close"].to_list() == [20.0]


def test_commit_keeps_undeclared_files_of_a_declared_partition(tmp_path):
    cfg, store, first = _revision_lake(tmp_path, 10.0)
    sibling = first.parent / "part-other.parquet"
    _bars(sibling, date(2026, 1, 1), 11.0)
    pl.read_parquet(sibling).with_columns(pl.lit("600001.SH").alias("symbol")).write_parquet(
        sibling
    )
    store.commit(
        "daily_bars",
        run_id="sibling",
        changed_files=[sibling],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert sorted(load("daily_bars", config=cfg)["close"].to_list()) == [10.0, 11.0]


def test_commit_propagates_a_partition_removed_by_the_writer(tmp_path):
    cfg, store, first = _revision_lake(tmp_path, 10.0)
    second = cfg.curated_root / "daily_bars/trade_date=2026-01-02/part.parquet"
    _bars(second, date(2026, 1, 2), 20.0)
    store.commit(
        "daily_bars",
        run_id="add",
        changed_files=[second],
        schema_version=1,
        contract_fingerprint="contract",
    )
    shutil.rmtree(first.parent)
    _bars(second, date(2026, 1, 2), 21.0)
    store.commit(
        "daily_bars",
        run_id="rebuild",
        changed_files=[second],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert load("daily_bars", config=cfg)["close"].to_list() == [21.0]


def test_commit_rejects_a_root_file_in_a_partitioned_dataset(tmp_path):
    cfg, store, first = _revision_lake(tmp_path, 10.0)
    stray = cfg.curated_root / "daily_bars/merged.parquet"
    _bars(stray, date(2026, 1, 1), 99.0)
    with pytest.raises(RevisionConsistencyError, match="partition layout"):
        store.commit(
            "daily_bars",
            run_id="stray",
            changed_files=[stray],
            schema_version=1,
            contract_fingerprint="contract",
        )
    assert load("daily_bars", config=cfg)["close"].to_list() == [10.0]


def test_layout_violations_flags_root_files_beside_partitions_and_foreign_keys():
    from cnequity.storage.revisions import layout_violations

    problems = layout_violations(
        "daily_bars",
        [
            Path("merged.parquet"),
            Path("symbol=600000.SH/part.parquet"),
            Path("trade_date=2026-01/part.parquet"),
            Path("trade_date=2026-01-02/part.parquet"),
        ],
    )
    assert len(problems) == 2
    assert any("root file beside" in problem for problem in problems)
    assert any("not a trade_date partition" in problem for problem in problems)
    # A root-only legacy layout or a legacy granularity is left to the audit.
    assert layout_violations("daily_bars", [Path("merged.parquet")]) == []
    assert layout_violations("instruments", [Path("part-merged.parquet")]) == []
