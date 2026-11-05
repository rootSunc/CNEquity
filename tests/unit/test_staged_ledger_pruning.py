"""Run-keyed staged-ledger entries must not outlive the staging they describe."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.orchestrator.manifest import Manifest
from cnequity.storage import StagingWriter
from cnequity.storage.staging_cleanup import clean_staging, prune_stale_staged_ledgers
from cnequity.storage.state import StateStore

DAY = date(2024, 6, 28)


def _bar_row(symbol: str) -> dict:
    return {
        "symbol": symbol,
        "trade_date": DAY,
        "open": 10.0,
        "high": 11.0,
        "low": 9.0,
        "close": 10.5,
        "volume": 1000,
        "amount": 10_500.0,
        "source": "mock",
        "data_version": "v1",
        "fetched_at": f"{DAY.isoformat()}T00:00:00+00:00",
    }


def _compacted_run(manifest: Manifest) -> str:
    """A terminal run whose compact succeeded, so cleanup may remove its staging."""
    run_id = manifest.start_run("daily", {"trade_date": DAY.isoformat()})
    manifest.start_batch(run_id, "batch-0", "daily_bars", "daily_bars")
    manifest.finish_batch(run_id, "batch-0", "success", rows_written=1)
    manifest.start_batch(run_id, "compact-0", "compact", "compact")
    manifest.finish_batch(run_id, "compact-0", "success", rows_written=1)
    manifest.finish_run(run_id, "warning")
    return run_id


def test_cleanup_drops_entries_of_removed_staging(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    manifest = Manifest(cfg.manifest_path)
    run_id = _compacted_run(manifest)
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "batch-0", pl.DataFrame([_bar_row("000001.SZ")])
    )
    state = StateStore(cfg.meta_root)
    state.record_missing_dates("daily_bars", [DAY], reason="test_gap")
    # A partial compact publishes some of the run's staging, so compact never
    # pops this run's entry; cleanup is the only thing that can retire it.
    state.mark_staged_request_days("daily_bars", run_id, [DAY])
    state.mark_staged_units("daily_bars", run_id, ["000001.SZ"])

    result = clean_staging(cfg)

    assert run_id in result.removed_run_ids
    assert result.pruned_ledgers == {"daily_bars": [run_id]}
    payload = state.get_payload("daily_bars")
    assert "staged_request_days" not in payload
    assert "staged_units" not in payload
    # Pruning never claims coverage: the day is still owed.
    assert DAY in state.get_missing_dates("daily_bars")


def test_dry_run_reports_but_keeps_entries(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    state = StateStore(cfg.meta_root)
    state.mark_staged_request_days("daily_bars", "gone-run", [DAY])

    assert prune_stale_staged_ledgers(cfg, dry_run=True) == {"daily_bars": ["gone-run"]}
    assert "gone-run" in state.get_payload("daily_bars")["staged_request_days"]


def test_entries_with_live_staging_or_a_running_run_are_kept(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    manifest = Manifest(cfg.manifest_path)
    with_staging = manifest.start_run("daily", {})
    manifest.finish_run(with_staging, "failed")
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", with_staging, "batch-0", pl.DataFrame([_bar_row("000001.SZ")])
    )
    still_running = manifest.start_run("daily", {})  # no staging written yet
    state = StateStore(cfg.meta_root)
    for run_id in (with_staging, still_running, "abandoned"):
        state.mark_staged_request_days("daily_bars", run_id, [DAY])

    assert prune_stale_staged_ledgers(cfg) == {"daily_bars": ["abandoned"]}
    kept = state.get_payload("daily_bars")["staged_request_days"]
    assert set(kept) == {with_staging, still_running}


def test_non_dataset_state_files_are_ignored(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    state_dir = cfg.meta_root / "state"
    state_dir.mkdir(parents=True)
    (state_dir / "lake.json").write_text('{"staged_request_days": {"x": []}}', encoding="utf-8")

    assert prune_stale_staged_ledgers(cfg) == {}
    assert "staged_request_days" in (state_dir / "lake.json").read_text(encoding="utf-8")
