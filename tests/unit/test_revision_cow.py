import shutil
from datetime import date
from pathlib import Path

import polars as pl
import pytest
from tests.legacy_lake import commit_generation

from cnequity.config import Config
from cnequity.query.parquet_scan import scan_parquet_root
from cnequity.query.reader import load, scan
from cnequity.query.universe import coverage_end_date, coverage_start_date
from cnequity.steps.common import load_bar_universe
from cnequity.storage.raw_archive import RawArchiveError, RawPayloadArchive
from cnequity.storage.revisions import RevisionConsistencyError, RevisionStore


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
    receipt = commit_generation(
        store,
        "daily_bars",
        run_id=f"r-{close}",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert receipt is not None
    return config, store, path


def _commit_twice_through(root: Path) -> None:
    cfg, store, path = _revision_lake(root, 10.0)
    _bars(path, date(2026, 1, 1), 11.0)
    receipt = commit_generation(
        store,
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
        commit_generation(
            store,
            dataset,
            run_id="initial",
            changed_files=[path],
            schema_version=1,
            contract_fingerprint="test",
        )
    pinned = {"daily_bars": 1, "instruments": 1, "trading_status": 1}
    assert load("daily_bars", config=cfg, profile="legacy_all_a", revision_map=pinned).height == 1
    frame = frames[changed_dataset].with_columns(
        pl.lit(date(2025, 12, 31)).alias("delist_date")
        if changed_dataset == "instruments"
        else pl.lit(True).alias("risk_warning")
    )
    path = cfg.curated_root / changed_dataset / "part.parquet"
    frame.write_parquet(path)
    commit_generation(
        store,
        changed_dataset,
        run_id="correction",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="test",
    )
    assert load("daily_bars", config=cfg, profile="legacy_all_a").height == 0
    assert load("daily_bars", config=cfg, profile="legacy_all_a", revision_map=pinned).height == 1


def test_explicit_revision_cannot_silently_read_unversioned_lake(tmp_path):
    _bars(tmp_path / "curated/daily_bars/part.parquet", date(2026, 1, 1), 10.0)
    with pytest.raises(RevisionConsistencyError, match="no retained revision"):
        load("daily_bars", data_root=tmp_path, revision=1)


def test_corrupt_pointer_fails_closed_instead_of_falling_back(tmp_path):
    data = tmp_path / "data"
    cfg = Config(data_root=data)
    path = data / "curated/daily_bars/trade_date=2026-01-01/part.parquet"
    _bars(path, date(2026, 1, 1), 10.0)
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    store.pointer_path("daily_bars").parent.mkdir(parents=True)
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
    receipt = commit_generation(
        store,
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
    assert load("daily_bars", config=cfg, profile="legacy_all_a").height == 1


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
    first_bar = commit_generation(
        store,
        "daily_bars",
        run_id="r1",
        changed_files=[bars_path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    first_factor = commit_generation(
        store,
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
    second_bar = commit_generation(
        store,
        "daily_bars",
        run_id="r2",
        changed_files=[bars_path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    second_factor = commit_generation(
        store,
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
    bar_revision = commit_generation(
        revisions,
        "daily_bars",
        run_id="bars",
        changed_files=[bars_path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    factor_one = commit_generation(
        revisions,
        "adj_factors",
        run_id="factors-1",
        changed_files=[factors_path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert bar_revision is not None and factor_one is not None

    factor_frame.with_columns(pl.lit(0.75).alias("factor")).write_parquet(factors_path)
    factor_two = commit_generation(
        revisions,
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
        assert commit_generation(
            store,
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


def test_dataset_state_uses_pointer_after_state_write_interruption(tmp_path):
    from cnequity.query.state import dataset_state

    cfg, store, path = _revision_lake(tmp_path, 10.0)
    state_path = cfg.meta_root / "state/daily_bars.json"
    old_state = state_path.read_bytes()
    _bars(path, date(2026, 1, 1), 20.0)
    receipt = commit_generation(
        store,
        "daily_bars",
        run_id="next",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    state_path.write_bytes(old_state)
    assert dataset_state("daily_bars", config=cfg).revision_id == receipt.revision_id
    assert load("daily_bars", config=cfg)["close"].to_list() == [20.0]
