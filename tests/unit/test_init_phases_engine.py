from datetime import date

import pytest

import cn_market_lake.steps  # noqa: F401
from cn_market_lake.config import Config
from cn_market_lake.config.bootstrap import path_for_toml
from cn_market_lake.orchestrator.engine import JobEngine
from cn_market_lake.orchestrator.init_phases import (
    missing_steps,
    phases_never_started,
)
from cn_market_lake.orchestrator.manifest import Manifest
from cn_market_lake.storage.layout import init_data_layout


def _minimal_init_phases() -> list[str]:
    return [
        "phase1_reference",
        "phase3_index_and_status",
        "phase4_finalize",
    ]


@pytest.fixture
def cfg(tmp_path):
    return Config(
        data_root=tmp_path / "data",
        init_phases=_minimal_init_phases(),
        tdx_allow_mock=True,
    )


def test_init_phase_failure_stops_without_keep_going(cfg, monkeypatch):
    init_data_layout(cfg)
    calls: list[str] = []

    from cn_market_lake.orchestrator import engine as eng_mod
    from cn_market_lake.orchestrator.registry import StepEntry

    def _get_step(name: str):
        def _fn(config, trade_date, run_id, context):
            calls.append(name)
            if name == "instruments":
                raise RuntimeError("simulated instruments failure")
            return {"rows_read": 1, "rows_written": 1}

        return StepEntry(fn=_fn, group="test", requires_workers=False)

    monkeypatch.setattr(eng_mod, "get_step", _get_step)

    engine = JobEngine(cfg)
    result = engine.run_init_phases(date(2024, 6, 28))
    assert result["status"] == "failed"
    assert "instruments" in calls
    assert "index_bars" not in calls
    run = engine.manifest.get_run(result["run_id"])
    assert run["status"] == "failed"


def test_init_manifest_final_status_reflects_failed_phase(cfg, monkeypatch):
    init_data_layout(cfg)
    from cn_market_lake.orchestrator import engine as eng_mod
    from cn_market_lake.orchestrator.registry import StepEntry

    def _get_step(name: str):
        def _fn(config, trade_date, run_id, context):
            if name == "trading_calendar":
                raise RuntimeError("calendar failed")
            return {"rows_read": 1, "rows_written": 1}

        return StepEntry(fn=_fn, group="test", requires_workers=False)

    monkeypatch.setattr(eng_mod, "get_step", _get_step)

    engine = JobEngine(cfg)
    result = engine.run_init_phases(date(2024, 6, 28), keep_going=True)
    assert result["status"] == "failed"
    assert engine.manifest.get_run(result["run_id"])["status"] == "failed"


def test_retry_runs_missing_init_steps(cfg):
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    phases = _minimal_init_phases()
    run_id = manifest.start_run("init", {"phases": phases, "trade_date": "2024-06-28"})
    manifest.start_batch(run_id, "b1", "instruments", "instruments")
    manifest.finish_batch(run_id, "b1", "success", rows_written=1)
    manifest.start_batch(run_id, "b2", "trading_calendar", "trading_calendar")
    manifest.finish_batch(run_id, "b2", "success", rows_written=1)
    manifest.finish_run(run_id, "success")

    batches = manifest.get_batches_for_run(run_id)
    assert missing_steps(phases, batches) == [
        "index_bars",
        "trading_status",
        "compact",
        "derive_adj_factors",
        "derive_industry_index",
        "audit",
    ]
    assert phases_never_started(phases, batches) == ["phase3_index_and_status", "phase4_finalize"]


def test_init_blocks_new_run_when_incomplete_exists(cfg, monkeypatch):
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run(
        "init",
        {"phases": _minimal_init_phases(), "trade_date": "2024-06-28"},
    )
    manifest.finish_run(run_id, "failed")

    monkeypatch.setattr(
        JobEngine,
        "run_init_phases",
        lambda self, **kwargs: pytest.fail("should not start new init"),
    )

    from click.testing import CliRunner

    from cn_market_lake.cli.main import cli

    cfg_path = cfg.data_root.parent / "cn-market-lake.toml"
    cfg_path.write_text(
        f'[data]\nroot = "{path_for_toml(cfg.data_root)}"\n[job.init.phases]\nnames = {json_phases()}'
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--config", str(cfg_path)])
    assert result.exit_code != 0
    assert "--resume" in result.output or "resume" in result.output.lower()


def json_phases():
    import json

    return json.dumps(_minimal_init_phases())


def test_resume_init_finds_latest_incomplete(cfg, monkeypatch):
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run(
        "init",
        {"phases": _minimal_init_phases(), "trade_date": "2024-06-28"},
    )
    manifest.finish_run(run_id, "failed")

    seen: dict[str, str] = {}

    engine = JobEngine(cfg)

    def fake_resume(self, trade_date=None, *, run_id=None, keep_going=False):
        seen["run_id"] = run_id or self.manifest.latest_incomplete_init_run()["run_id"]
        return {"run_id": seen["run_id"], "status": "success", "resumed": True, "phases": []}

    monkeypatch.setattr(JobEngine, "resume_init", fake_resume)

    result = engine.run_init_phases(resume=True)
    assert result["resumed"] is True
    assert seen["run_id"] == run_id


def test_reference_and_index_phases_backfill_history():
    """index_bars and trading_calendar must backfill 2016+ during init, like daily_bars."""
    from cn_market_lake.orchestrator.init_phases import (
        DEFAULT_INIT_PHASES,
        phase_backfill,
        step_backfill,
    )

    assert phase_backfill("phase1_reference") is True
    assert phase_backfill("phase3_index_and_status") is True
    phases = DEFAULT_INIT_PHASES
    assert step_backfill("trading_calendar", phases) is True
    assert step_backfill("index_bars", phases) is True
    # instruments/trading_status are date-insensitive but flagged consistently
    assert step_backfill("index_bars", ["phase1_reference"]) is False
