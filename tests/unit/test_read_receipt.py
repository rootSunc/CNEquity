"""The public read receipt must describe the generation actually scanned."""

from datetime import date, datetime, timezone

import polars as pl
import pytest
from tests.catalog_seed import commit_tree

from cnequity.config import Config
from cnequity.orchestrator.manifest import Manifest
from cnequity.query import dataset_attempt, dataset_state, load_with_receipt
from cnequity.query.receipt import ReadReceiptError
from cnequity.storage.revisions import RevisionStore


def _bars(path, close: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600000.SH"],
            "trade_date": [date(2024, 6, 27)],
            "open": [close],
            "high": [close],
            "low": [close],
            "close": [close],
            "volume": [1000],
            "amount": [close * 1000],
            "source": ["test_exchange"],
            "data_version": ["v1"],
            "fetched_at": [datetime(2024, 6, 28, tzinfo=timezone.utc)],
        }
    ).write_parquet(path)


def test_dataset_state_is_read_only_when_state_is_missing(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    state = dataset_state("daily_bars", config=cfg)
    assert state.revision_id is None
    assert not cfg.meta_root.exists()


def test_dataset_attempt_reads_failure_without_mutating_last_good_revision(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    assert dataset_attempt("etf_profiles", config=cfg) is None
    assert not cfg.meta_root.exists()
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily:research")
    manifest.record_dataset_result(
        run_id,
        "etf_profiles",
        "fetch",
        "failed",
        criticality="research",
        error_code="ConnectError",
        error_message="official index source unavailable",
    )
    manifest.finish_run(run_id, "degraded")
    attempt = dataset_attempt("etf_profiles", config=cfg)
    assert attempt is not None
    assert attempt["run_id"] == run_id
    assert attempt["status"] == "failed"
    assert attempt["error_message"] == "official index source unavailable"
    assert dataset_state("etf_profiles", config=cfg).revision_id is None
    later = manifest.start_run("daily:research")
    manifest.record_dataset_result(
        later, "etf_profiles", "fetch", "success", criticality="research"
    )
    manifest.record_dataset_result(
        later,
        "etf_profiles",
        "stage",
        "failed",
        criticality="research",
        error_code="ValidationError",
        error_message="new directory could not be staged",
    )
    manifest.finish_run(later, "degraded")
    attempt = dataset_attempt("etf_profiles", config=cfg)
    assert attempt is not None
    assert attempt["run_id"] == later and attempt["stage"] == "stage"
    assert attempt["error_message"] == "new directory could not be staged"


def test_legacy_read_reports_observed_rows_without_claiming_coverage(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _bars(cfg.curated_root / "daily_bars" / "part.parquet", 10.0)

    result = load_with_receipt("daily_bars", start="2024-06-01", end="2024-06-30", config=cfg)

    assert result.frame["close"].to_list() == [10.0]
    assert result.receipt["replayable"] is False
    assert result.receipt["unpinned_datasets"] == ["daily_bars"]
    assert result.receipt["coverage"] == {
        "kind": "observed_rows_only",
        "requested_start": "2024-06-01",
        "requested_end": "2024-06-30",
        "date_column": "trade_date",
        "observed_start": "2024-06-27",
        "observed_end": "2024-06-27",
        "row_count": 1,
        "completeness": "not_assessed",
    }
    assert result.receipt["provenance"]["returned_row_sources"] == {"test_exchange": 1}
    assert result.receipt["pit"]["mode"] == "not_applicable"


def test_receipt_pins_and_replays_old_generation_after_repair(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    path = cfg.curated_root / "daily_bars" / "part.parquet"
    _bars(path, 10.0)
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    first = commit_tree(
        store,
        "daily_bars",
        run_id="before-repair",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="test-contract",
    )
    assert first is not None

    original = load_with_receipt("daily_bars", config=cfg, require_replayable=True)
    assert original.receipt["dependencies"]["daily_bars"]["revision_id"] == first.revision_id
    assert original.receipt["replayable"] is True

    _bars(path, 11.0)
    second = commit_tree(
        store,
        "daily_bars",
        run_id="repair",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="test-contract",
    )
    assert second is not None
    replay = load_with_receipt(
        "daily_bars",
        config=cfg,
        revision_map={"daily_bars": first.revision_id},
        require_replayable=True,
    )
    current = load_with_receipt("daily_bars", config=cfg, require_replayable=True)

    assert replay.frame["close"].to_list() == [10.0]
    assert current.frame["close"].to_list() == [11.0]
    assert replay.receipt["dependencies"]["daily_bars"]["revision_id"] == first.revision_id
    assert current.receipt["dependencies"]["daily_bars"]["revision_id"] == second.revision_id
    assert replay.receipt["receipt_id"] == original.receipt["receipt_id"]


def test_replayable_read_rejects_unversioned_adjustment_dependency(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    path = cfg.curated_root / "daily_bars" / "part.parquet"
    _bars(path, 10.0)
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    assert (
        commit_tree(
            store,
            "daily_bars",
            run_id="bars-only",
            changed_files=[path],
            schema_version=1,
            contract_fingerprint="test-contract",
        )
        is not None
    )

    with pytest.raises(ReadReceiptError, match="adj_factors"):
        load_with_receipt("daily_bars", adjust="hfq", config=cfg, require_replayable=True)
