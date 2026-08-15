from datetime import date

from cn_market_lake.config import Config
from cn_market_lake.orchestrator.engine import JobEngine
from cn_market_lake.orchestrator.manifest import Manifest
from cn_market_lake.orchestrator.registry import StepEntry
from cn_market_lake.storage.layout import init_data_layout


def _batch(manifest: Manifest, run_id: str, batch_id: str, status: str) -> None:
    manifest.start_batch(run_id, batch_id, task_id="trading_status", dataset="trading_status")
    manifest.finish_batch(run_id, batch_id, status)


def test_warning_batch_is_retryable_and_not_success(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run("backfill")
    monkeypatch.setattr(
        "cn_market_lake.orchestrator.engine.get_step",
        lambda name: StepEntry(
            fn=lambda *args: {"status": "warning", "rows_read": 3, "rows_written": 2},
            group="test",
            requires_workers=False,
        ),
    )

    result = engine._run_step("trading_status", date(2024, 6, 28), run_id, {})

    assert result["status"] == "warning"
    retryable = engine.manifest.get_retryable_batches(run_id)
    assert len(retryable) == 1
    assert retryable[0]["status"] == "warning"
    assert engine.manifest.incomplete_batch_count(run_id) == 1
    assert engine.manifest.incomplete_batch_counts_by_dataset(run_id) == {"trading_status": 1}


def test_partial_failure_fields_are_promoted_to_warning(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run("ticks")
    monkeypatch.setattr(
        "cn_market_lake.orchestrator.engine.get_step",
        lambda name: StepEntry(
            fn=lambda *args: {
                "rows_read": 10,
                "rows_written": 8,
                "failed_symbol_days": 2,
            },
            group="test",
            requires_workers=False,
        ),
    )

    result = engine._run_step("trade_ticks", date(2024, 6, 28), run_id, {})

    assert result["status"] == "warning"
    assert engine.manifest.incomplete_batch_counts_by_dataset(run_id) == {"trade_ticks": 1}


def test_one_successful_retry_supersedes_all_prior_attempts(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    engine = JobEngine(cfg)
    run_id = manifest.start_run("backfill")
    _batch(manifest, run_id, "failed-1", "failed")
    _batch(manifest, run_id, "warning-2", "warning")
    calls: list[str] = []

    def get_step(name: str) -> StepEntry:
        def run(*args):
            calls.append(name)
            return {"rows_read": 1, "rows_written": 1}

        return StepEntry(fn=run, group="test", requires_workers=False)

    monkeypatch.setattr("cn_market_lake.orchestrator.engine.get_step", get_step)

    result = engine._retry_run_locked(run_id, date(2024, 6, 28), auto_finalize=False)

    assert result["status"] == "success"
    assert calls == ["trading_status"]
    assert manifest.get_batch(run_id, "failed-1")["status"] == "superseded"
    assert manifest.get_batch(run_id, "warning-2")["status"] == "superseded"
    assert manifest.incomplete_batch_count(run_id) == 0


def test_warning_retry_does_not_supersede_prior_failure(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    engine = JobEngine(cfg)
    run_id = manifest.start_run("backfill")
    _batch(manifest, run_id, "failed-1", "failed")
    monkeypatch.setattr(
        "cn_market_lake.orchestrator.engine.get_step",
        lambda name: StepEntry(
            fn=lambda *args: {"status": "warning", "rows_read": 1, "rows_written": 1},
            group="test",
            requires_workers=False,
        ),
    )

    result = engine._retry_run_locked(run_id, date(2024, 6, 28), auto_finalize=False)

    assert result["status"] == "failed"
    assert manifest.get_batch(run_id, "failed-1")["status"] == "failed"
    assert len(manifest.get_retryable_batches(run_id)) == 2


def test_init_retry_restores_the_failed_steps_backfill_mode(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    engine = JobEngine(cfg)
    run_id = manifest.start_run(
        "init",
        {
            "backfill": False,
            "phases": ["phase3_index_and_status"],
            "trade_date": "2024-06-28",
        },
    )
    _batch(manifest, run_id, "failed-status", "failed")
    manifest.start_batch(run_id, "index-ok", "index_bars", "index_bars")
    manifest.finish_batch(run_id, "index-ok", "success")
    observed: list[bool] = []

    def get_step(name: str) -> StepEntry:
        def run(config, *args):
            observed.append(bool(config._backfill))
            return {"rows_read": 1, "rows_written": 1}

        return StepEntry(fn=run, group="test", requires_workers=False)

    monkeypatch.setattr("cn_market_lake.orchestrator.engine.get_step", get_step)

    result = engine._retry_run_locked(run_id, date(2024, 6, 28), auto_finalize=False)

    assert result["status"] == "success"
    assert observed == [True]
