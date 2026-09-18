from datetime import date
from pathlib import Path

import pytest

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.config.bootstrap import path_for_toml
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.init_phases import (
    missing_steps,
    phases_never_started,
)
from cnequity.orchestrator.manifest import Manifest
from cnequity.storage.layout import init_data_layout


def _minimal_init_phases() -> list[str]:
    return [
        "phase1_reference",
        "phase3_index_and_status",
        "phase4_finalize",
    ]


def _write_config(tmp_path) -> Path:
    """A config file on disk: the CLI paths under test resolve one."""
    path = tmp_path / "cnequity.toml"
    path.write_text(
        f"""
[data]
root = "{path_for_toml(tmp_path / "data")}"

[tdx_protocol]
allow_mock = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return path


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

    from cnequity.orchestrator import engine as eng_mod
    from cnequity.orchestrator.registry import StepEntry

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
    from cnequity.orchestrator import engine as eng_mod
    from cnequity.orchestrator.registry import StepEntry

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


def test_init_never_starts_a_second_run_over_an_incomplete_one(cfg, monkeypatch):
    """It resumes that run instead — what it must never do is start over."""
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run(
        "init",
        {"phases": _minimal_init_phases(), "trade_date": "2024-06-28"},
    )
    manifest.finish_run(run_id, "failed")

    seen: dict = {}

    def _capture(self, trade_date=None, *, resume=False, resume_run_id=None, keep_going=False):
        if not resume:
            pytest.fail("should not start a new init over an incomplete one")
        seen["run_id"] = resume_run_id
        return {"run_id": resume_run_id, "status": "success", "phases": []}

    monkeypatch.setattr(JobEngine, "run_init_phases", _capture)

    from click.testing import CliRunner

    from cnequity.cli.main import cli

    cfg_path = cfg.data_root.parent / "cnequity.toml"
    cfg_path.write_text(
        f'[data]\nroot = "{path_for_toml(cfg.data_root)}"\n[job.init.phases]\nnames = {json_phases()}'
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--config", str(cfg_path)])
    assert result.exit_code == 0, result.output
    assert seen["run_id"] == run_id


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
    from cnequity.orchestrator.init_phases import (
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


def test_a_killed_init_is_resumed_not_refused(tmp_path, monkeypatch):
    """Looked hung, got killed, started again — the one command the operator
    had left used to answer "no" to the one thing they were trying to do."""
    from click.testing import CliRunner

    from cnequity.cli import setup_cmds
    from cnequity.cli._root import cli

    cfg_path = _write_config(tmp_path)
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run(
        "init", {"phases": ["phase1_reference"], "trade_date": "2026-09-16"}
    )
    # Left as `running`, exactly as a SIGKILLed process leaves it.

    seen: dict = {}

    def _capture(self, trade_date=None, *, resume=False, resume_run_id=None, keep_going=False):
        seen.update(resume=resume, resume_run_id=resume_run_id)
        return {"run_id": resume_run_id, "status": "success", "phases": []}

    monkeypatch.setattr(setup_cmds.JobEngine, "run_init_phases", _capture)

    result = CliRunner().invoke(cli, ["init", "--config", str(cfg_path)])

    assert result.exit_code == 0, result.output
    assert "直接续跑而不是从头再来" in result.output
    assert seen == {"resume": True, "resume_run_id": run_id}


def test_a_live_init_is_still_refused(tmp_path, monkeypatch):
    """The refusal is right for a peer that is actually running."""
    import subprocess
    import sys
    import time

    from click.testing import CliRunner

    from cnequity.cli import setup_cmds
    from cnequity.cli._root import cli
    from cnequity.orchestrator.run_lock import INIT_JOB_LOCK, lock_path

    cfg_path = _write_config(tmp_path)
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    Manifest(cfg.manifest_path).start_run(
        "init", {"phases": ["phase1_reference"], "trade_date": "2026-09-16"}
    )

    def _never(*args, **kwargs):
        raise AssertionError("must not join a run another process owns")

    monkeypatch.setattr(setup_cmds.JobEngine, "run_init_phases", _never)

    # A real second process: the in-process guard raises a different error, and
    # what matters is what another `cne init` sees.
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import time\n"
            "from pathlib import Path\n"
            "from cnequity.orchestrator.run_lock import run_lock\n"
            f"with run_lock(Path({str(cfg.meta_root)!r}), {INIT_JOB_LOCK!r}, blocking=False):\n"
            "    print('held', flush=True)\n"
            "    time.sleep(30)\n",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout.readline().strip() == "held"
        deadline = time.monotonic() + 10
        while not lock_path(cfg.meta_root, INIT_JOB_LOCK).exists():
            assert time.monotonic() < deadline
            time.sleep(0.05)
        result = CliRunner().invoke(cli, ["init", "--config", str(cfg_path)])
    finally:
        holder.terminate()
        holder.wait(30)

    assert result.exit_code != 0
    assert "已经有一个 init 在跑" in result.output
