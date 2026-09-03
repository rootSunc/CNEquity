from datetime import date

import cnequity.steps  # noqa: F401
from cnequity.config import Config, ScheduleGroup, WaveConfig
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.manifest import Manifest
from cnequity.orchestrator.registry import register_step
from cnequity.storage.layout import init_data_layout


def test_run_job_persists_expected_steps_in_metadata(tmp_path):
    """Verify run_job persists expected_steps in metadata_json on start."""
    cfg = Config(data_root=tmp_path / "data", tdx_allow_mock=True)
    init_data_layout(cfg)
    engine = JobEngine(cfg)

    # Use a lightweight step that does not call network
    run_date = date(2024, 6, 28)
    waves = [WaveConfig(name="test_wave", parallel=False, steps=["trading_calendar"])]
    result = engine.run_job("daily:test", trade_date=run_date, waves=waves)

    run_id = result["run_id"]
    manifest = Manifest(cfg.manifest_path)
    meta = manifest.get_run_metadata(run_id)

    assert "expected_steps" in meta
    assert meta["expected_steps"] == ["trading_calendar"]


def test_retry_executes_missing_steps_after_mid_dag_crash(tmp_path, monkeypatch):
    """Verify that when a DAG crashes mid-way, retry executes the steps that never started."""
    cfg = Config(data_root=tmp_path / "data", tdx_allow_mock=True)
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)

    run_date = "2024-06-28"
    # Metadata has 2 steps, but step 2 never recorded a batch due to mid-run crash
    run_id = manifest.start_run(
        "daily:custom",
        {
            "trade_date": run_date,
            "expected_steps": ["trading_calendar", "mock_downstream_step"],
        },
    )

    # Step 1 ran and failed
    manifest.start_batch(
        run_id,
        "batch_step1",
        "trading_calendar",
        "trading_calendar",
        window_start=run_date,
        window_end=run_date,
    )
    manifest.finish_batch(run_id, "batch_step1", "failed", error_message="interrupted")
    manifest.finish_run(run_id, "failed")

    # Register mock downstream step
    step2_called = []

    def _mock_step(config, td, run_id_, context):
        step2_called.append(run_id_)
        return {"status": "success", "rows_written": 10}

    register_step("mock_downstream_step", group="core")(_mock_step)

    engine = JobEngine(cfg)
    result = engine.run_job("retry", date(2024, 6, 28), run_id=run_id)

    assert result["status"] == "success"
    assert len(step2_called) == 1

    # Verify both steps have recorded batches now
    batches = manifest.get_batches_for_run(run_id)
    step_names = {b["dataset"] for b in batches}
    assert "trading_calendar" in step_names
    assert "mock_downstream_step" in step_names
    assert manifest.incomplete_batch_count(run_id) == 0

    run = manifest.get_run(run_id)
    assert run["status"] == "success"


def test_retry_falls_back_to_toml_for_legacy_run(tmp_path):
    """Verify that a legacy run without expected_steps in metadata falls back to config.schedule_groups."""
    cfg = Config(data_root=tmp_path / "data", tdx_allow_mock=True)
    cfg.schedule_groups = {
        "core": ScheduleGroup(
            at="16:00",
            steps=["trading_calendar", "mock_legacy_fallback_step"],
            parallel=False,
        )
    }
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)

    run_date = "2024-06-28"
    # Legacy run without expected_steps
    run_id = manifest.start_run(
        "daily:core",
        {"trade_date": run_date},
    )
    # Step 1 succeeded, step 2 never recorded
    manifest.start_batch(
        run_id,
        "batch_cal",
        "trading_calendar",
        "trading_calendar",
        window_start=run_date,
        window_end=run_date,
    )
    manifest.finish_batch(run_id, "batch_cal", "success")
    manifest.finish_run(run_id, "failed")

    mock_called = []

    def _mock_legacy_step(config, td, run_id_, context):
        mock_called.append(run_id_)
        return {"status": "success", "rows_written": 5}

    register_step("mock_legacy_fallback_step", group="core")(_mock_legacy_step)

    engine = JobEngine(cfg)
    # Missing steps should be detected via TOML fallback
    missing = engine._missing_run_steps(run_id)
    assert missing == ["mock_legacy_fallback_step"]

    result = engine.run_job("retry", date(2024, 6, 28), run_id=run_id)
    assert result["status"] == "success"
    assert len(mock_called) == 1

    batches = manifest.get_batches_for_run(run_id)
    assert any(b["dataset"] == "mock_legacy_fallback_step" for b in batches)


def test_retry_graceful_fallback_on_unknown_group(tmp_path, caplog):
    """Verify that a legacy run referencing a deleted/unknown group logs warning and does not crash."""
    cfg = Config(data_root=tmp_path / "data", tdx_allow_mock=True)
    cfg.schedule_groups = {}  # Empty groups
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)

    run_id = manifest.start_run(
        "daily:non_existent_group",
        {"trade_date": "2024-06-28"},
    )
    manifest.start_batch(
        run_id,
        "b1",
        "trading_calendar",
        "trading_calendar",
        window_start="2024-06-28",
        window_end="2024-06-28",
    )
    manifest.finish_batch(run_id, "b1", "success")
    manifest.finish_run(run_id, "failed")

    engine = JobEngine(cfg)
    missing = engine._missing_run_steps(run_id)
    # Should safely return empty list without raising KeyError
    assert missing == []


def test_retry_blocked_from_success_if_missing_step_fails(tmp_path):
    """Verify that if a missing step fails during retry, the run is NOT marked success."""
    cfg = Config(data_root=tmp_path / "data", tdx_allow_mock=True)
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)

    run_id = manifest.start_run(
        "daily:broken",
        {
            "trade_date": "2024-06-28",
            "expected_steps": ["mock_failing_missing_step"],
        },
    )
    manifest.finish_run(run_id, "failed")

    def _failing_step(config, td, run_id_, context):
        raise RuntimeError("upstream network timeout")

    register_step("mock_failing_missing_step", group="core")(_failing_step)

    engine = JobEngine(cfg)
    result = engine.run_job("retry", date(2024, 6, 28), run_id=run_id)

    assert result["status"] == "failed"
    run = manifest.get_run(run_id)
    assert run["status"] == "failed"
