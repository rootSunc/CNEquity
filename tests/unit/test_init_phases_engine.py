from datetime import date
from pathlib import Path

import pytest

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.config.bootstrap import path_for_toml
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.init_phases import (
    missing_steps,
    pending_phases,
)
from cnequity.orchestrator.manifest import Manifest
from cnequity.run_scope import backfill_of
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


def test_init_persists_new_instrument_dates_without_changing_step_context(cfg, monkeypatch):
    init_data_layout(cfg)
    from cnequity.orchestrator import engine as eng_mod
    from cnequity.orchestrator.registry import StepEntry

    # Exercise a consumer in the same phase, where step context is shared.
    cfg.init_phases = ["phase1_reference"]
    monkeypatch.setitem(eng_mod.INIT_PHASE_STEPS, "phase1_reference", ["instruments", "daily_bars"])
    listed = date(2024, 6, 28)
    new = [{"symbol": "000001.SZ", "list_date": listed, "delist_date": None}]
    observed = []

    def _get_step(name):
        def _fn(config, trade_date, run_id, context):
            if name == "instruments":
                return {"rows_read": 1, "context_updates": {"new_instruments": new}}
            if context.get("new_instruments"):
                observed.append(context["new_instruments"][0]["list_date"])
            return {"rows_read": 1, "rows_written": 1}

        return StepEntry(fn=_fn, group="test", requires_workers=False)

    monkeypatch.setattr(eng_mod, "get_step", _get_step)
    engine = JobEngine(cfg)
    result = engine.run_init_phases(listed)

    assert result["status"] == "success"
    assert observed and all(value is listed for value in observed)
    manifest = Manifest(engine.manifest.db_path)
    assert manifest.get_run(result["run_id"])["status"] == "success"
    steps = [
        step
        for phase in manifest.get_run_metadata(result["run_id"])["phase_results"]
        for step in phase["results"]
    ]
    instruments = next(
        step for step in steps if step.get("context_updates", {}).get("new_instruments")
    )
    assert instruments["context_updates"]["new_instruments"][0]["list_date"] == "2024-06-28"
    assert new[0]["list_date"] is listed


def test_init_parent_run_stays_locked_between_phases(cfg, monkeypatch):
    """A long first phase must not make the parent look orphaned to phase two."""
    init_data_layout(cfg)
    cfg.batch_stale_seconds = 0

    from cnequity.orchestrator import engine as eng_mod
    from cnequity.orchestrator.registry import StepEntry

    observed_statuses: list[str] = []

    def _get_step(name: str):
        def _fn(config, trade_date, run_id, context):
            observed_statuses.append(engine.manifest.get_run(run_id)["status"])
            return {"rows_read": 1, "rows_written": 1}

        return StepEntry(fn=_fn, group="test", requires_workers=False)

    monkeypatch.setattr(eng_mod, "get_step", _get_step)

    engine = JobEngine(cfg)
    result = engine.run_init_phases(date(2024, 6, 28))

    assert result["status"] == "success"
    assert observed_statuses
    assert set(observed_statuses) == {"running"}


def test_interrupt_closes_active_init_batch_and_can_resume_immediately(cfg, monkeypatch):
    init_data_layout(cfg)
    cfg.init_phases = ["phase2a_corporate_actions"]

    from cnequity.orchestrator import engine as eng_mod
    from cnequity.orchestrator.registry import StepEntry

    attempts = 0

    def _get_step(name: str):
        def _fn(config, trade_date, run_id, context):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise KeyboardInterrupt
            return {"rows_read": 1, "rows_written": 1}

        return StepEntry(fn=_fn, group="test", requires_workers=False)

    monkeypatch.setattr(eng_mod, "get_step", _get_step)
    engine = JobEngine(cfg)

    with pytest.raises(KeyboardInterrupt):
        engine.run_init_phases(date(2024, 6, 28))

    interrupted = engine.manifest.latest_incomplete_init_run()
    assert interrupted is not None
    assert interrupted["status"] == "failed"
    batches = engine.manifest.get_batches_for_run(interrupted["run_id"])
    assert [batch["status"] for batch in batches] == ["failed"]
    assert "interrupted" in batches[0]["error_message"]

    resumed = engine.run_init_phases(
        date(2024, 6, 28), resume=True, resume_run_id=interrupted["run_id"]
    )
    assert resumed["status"] == "success"
    assert attempts == 2


def _failed_init(cfg, phases, step):
    init_data_layout(cfg)
    cfg.init_phases = phases
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("init", {"phases": phases, "trade_date": "2024-06-28"})
    manifest.start_batch(run_id, f"failed-{step}", step, step)
    manifest.finish_batch(run_id, f"failed-{step}", "failed", error_message="interrupted")
    manifest.finish_run(run_id, "failed", error_message="interrupted")
    return run_id


def _steps_failing(monkeypatch, failing: str) -> list[str]:
    from cnequity.orchestrator import engine as eng_mod
    from cnequity.orchestrator.registry import StepEntry

    calls: list[str] = []

    def _get_step(name: str):
        def _fn(config, trade_date, current_run_id, context):
            calls.append(name)
            if name == failing:
                raise RuntimeError("still unavailable")
            return {"rows_read": 1, "rows_written": 1}

        return StepEntry(fn=_fn, group="test", requires_workers=False)

    monkeypatch.setattr(eng_mod, "get_step", _get_step)
    return calls


def test_resume_does_not_advance_past_a_still_failing_blocking_phase(cfg, monkeypatch):
    run_id = _failed_init(
        cfg, ["phase1_reference", "phase3_index_and_status", "phase4_finalize"], "instruments"
    )
    calls = _steps_failing(monkeypatch, "instruments")
    result = JobEngine(cfg).run_init_phases(date(2024, 6, 28), resume=True, resume_run_id=run_id)

    assert result["status"] == "failed"
    assert result["lake_usable"] is False
    assert "index_bars" not in calls
    assert "compact" not in calls


def test_resume_carries_on_past_a_still_failing_non_blocking_phase(cfg, monkeypatch):
    run_id = _failed_init(
        cfg,
        ["phase2a_corporate_actions", "phase3_index_and_status", "phase4_finalize"],
        "corporate_actions",
    )
    calls = _steps_failing(monkeypatch, "corporate_actions")
    result = JobEngine(cfg).run_init_phases(date(2024, 6, 28), resume=True, resume_run_id=run_id)

    assert calls[0] == "corporate_actions"
    assert {"index_bars", "trading_status", "compact", "audit"} <= set(calls)
    assert result["lake_usable"] is True
    assert result["owed_steps"] == ["corporate_actions"]
    # The owed step keeps the init open, so the next `cne init` resumes it.
    assert JobEngine(cfg).manifest.latest_incomplete_init_run()["run_id"] == run_id


def test_a_first_build_publishes_when_a_non_blocking_phase_fails(cfg, monkeypatch):
    from cnequity.orchestrator.init_phases import DEFAULT_INIT_PHASES

    init_data_layout(cfg)
    cfg.init_phases = list(DEFAULT_INIT_PHASES)
    calls = _steps_failing(monkeypatch, "corporate_actions")
    engine = JobEngine(cfg)
    result = engine.run_init_phases(date(2024, 6, 28))

    assert {"daily_bars", "index_bars", "compact", "derive_adj_factors"} <= set(calls)
    # The ledger stays truthful about the owed step; the lake is usable anyway.
    assert result["status"] == "failed"
    assert result["lake_usable"] is True
    assert result["owed_steps"] == ["corporate_actions"]
    assert engine.manifest.latest_incomplete_init_run()["run_id"] == result["run_id"]


def test_a_first_build_still_stops_when_daily_bars_fail(cfg, monkeypatch):
    from cnequity.orchestrator.init_phases import DEFAULT_INIT_PHASES

    init_data_layout(cfg)
    cfg.init_phases = list(DEFAULT_INIT_PHASES)
    calls = _steps_failing(monkeypatch, "daily_bars")
    result = JobEngine(cfg).run_init_phases(date(2024, 6, 28))

    assert result["status"] == "failed"
    assert result["lake_usable"] is False
    assert "compact" not in calls


def test_resume_does_not_finish_the_parent_before_remaining_phases(cfg, monkeypatch):
    init_data_layout(cfg)
    from cnequity.orchestrator import engine as eng_mod
    from cnequity.orchestrator.registry import StepEntry

    cfg.init_phases = ["phase1_reference", "phase3_index_and_status"]
    monkeypatch.setitem(eng_mod.INIT_PHASE_STEPS, "phase1_reference", ["instruments"])
    monkeypatch.setitem(eng_mod.INIT_PHASE_STEPS, "phase3_index_and_status", ["trading_status"])
    engine = JobEngine(cfg)
    manifest = engine.manifest
    run_id = manifest.start_run("init", {"phases": cfg.init_phases, "trade_date": "2024-06-28"})
    manifest.start_batch(run_id, "done", "instruments", "instruments")
    manifest.finish_batch(run_id, "done", "success")
    manifest.finish_run(run_id, "failed", error_message="interrupted between phases")
    events = []
    original_finish = manifest.finish_run

    def finish(rid, status, *args, **kwargs):
        events.append(("finish", status))
        return original_finish(rid, status, *args, **kwargs)

    def get_step(name):
        def execute(*args):
            row = manifest.get_run(run_id)
            assert row["status"] == "running"
            assert row["finished_at"] is None
            events.append(("step", name))
            return {"rows_read": 1, "rows_written": 1}

        return StepEntry(fn=execute, group="test", requires_workers=False)

    monkeypatch.setattr(manifest, "finish_run", finish)
    monkeypatch.setattr(eng_mod, "get_step", get_step)
    result = engine.resume_init(date(2024, 6, 28), run_id=run_id)
    assert result["status"] == "success"
    assert events == [("step", "trading_status"), ("finish", "success")]


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
    assert pending_phases(phases, batches) == ["phase3_index_and_status", "phase4_finalize"]


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
    assert step_backfill("trading_status", phases) is False
    # Index backfill remains independent of the status snapshot.
    assert step_backfill("index_bars", ["phase1_reference"]) is False


@pytest.mark.usefixtures("chinese_output")
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
        seen.update(
            resume=resume,
            resume_run_id=resume_run_id,
            backfill_start=backfill_of(self.config).start,
        )
        return {"run_id": resume_run_id, "status": "success", "phases": []}

    monkeypatch.setattr(setup_cmds.JobEngine, "run_init_phases", _capture)

    result = CliRunner().invoke(cli, ["init", "--config", str(cfg_path)])

    assert result.exit_code == 0, result.output
    assert "直接续跑而不是从头再来" in result.output
    assert seen == {"resume": True, "resume_run_id": run_id, "backfill_start": None}


@pytest.mark.usefixtures("chinese_output")
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


def test_retry_does_not_advance_an_init_past_an_unresolved_phase(cfg, monkeypatch):
    """`cne init` has refused to cross an unresolved phase boundary since it
    learned to resume. `cne run retry --run-id <init run>` reached the same
    run through a different door and used only the dependency graph, which is
    a weaker claim: nothing declares that index_bars needs corporate_actions,
    so retry advanced past a phase resume would have stopped at."""
    init_data_layout(cfg)
    cfg.init_phases = ["phase2a_corporate_actions", "phase3_index_and_status"]

    from cnequity.orchestrator import engine as eng_mod
    from cnequity.orchestrator.registry import StepEntry

    ran: list[str] = []

    def _get_step(name: str):
        def _fn(config, trade_date, run_id, context):
            ran.append(name)
            return {"rows_read": 1, "rows_written": 1}

        return StepEntry(fn=_fn, group="test", requires_workers=False)

    monkeypatch.setattr(eng_mod, "get_step", _get_step)

    engine = JobEngine(cfg)
    manifest = engine.manifest
    run_id = manifest.start_run(
        "init",
        {"trade_date": "2024-06-28", "phases": cfg.init_phases},
    )
    # Phase 2a started and is still owed; phase 3 never started at all.
    manifest.queue_batches(
        run_id,
        "corporate_actions",
        "corporate_actions",
        [("ca-0", ["600519.SH"], "2024-06-28", "2024-06-28")],
    )

    missing = engine._missing_init_steps(run_id)
    assert "index_bars" not in missing, "a later phase must wait for an unresolved earlier one"

    # The run is still not successful, and the gate says why using the whole
    # remainder rather than the phase-ordered slice.
    _, unresolved = engine._gate_on_missing_steps(run_id, "success")
    assert "index_bars" in unresolved


def test_a_fully_successful_phase_does_not_block_the_next_one(cfg, monkeypatch):
    init_data_layout(cfg)
    cfg.init_phases = ["phase2a_corporate_actions", "phase3_index_and_status"]

    engine = JobEngine(cfg)
    manifest = engine.manifest
    run_id = manifest.start_run(
        "init",
        {"trade_date": "2024-06-28", "phases": cfg.init_phases},
    )
    manifest.start_batch(run_id, "ca-0", "corporate_actions", "corporate_actions")
    manifest.finish_batch(run_id, "ca-0", "success", rows_read=1, rows_written=1)

    assert "index_bars" in engine._missing_init_steps(run_id)


def test_a_step_a_later_run_completed_no_longer_leaves_init_open(cfg):
    """An init from 2026-07-06 never recorded `derive_industry_index`; the daily
    schedule has derived it every session since, yet `cne status` failed the
    health gate on it daily from 2026-09-19. Resuming keeps the strict view."""
    from cnequity.orchestrator.init_phases import expected_steps

    init_data_layout(cfg)
    manifest = Manifest(cfg.manifest_path)
    phases = ["phase4_finalize"]
    init_id = manifest.start_run("init", {"phases": phases, "trade_date": "2024-06-28"})
    for step in expected_steps(phases):
        if step == "derive_industry_index":
            continue
        manifest.start_batch(init_id, f"b-{step}", task_id=step, dataset=step)
        manifest.finish_batch(init_id, f"b-{step}", "success")
    manifest.finish_run(init_id, "success")

    assert manifest.latest_incomplete_init_run(discharged_by_later_runs=True) is not None

    daily = manifest.start_run("daily:core")
    manifest.start_batch(
        daily, "b-idx", task_id="derive_industry_index", dataset="derive_industry_index"
    )
    manifest.finish_batch(daily, "b-idx", "success")

    assert manifest.latest_incomplete_init_run(discharged_by_later_runs=True) is None
    assert manifest.latest_incomplete_init_run()["run_id"] == init_id


def test_a_first_build_fetches_bars_before_the_corporate_action_sweep(cfg, monkeypatch):
    """The xdxr sweep covers every instrument whatever the window and held a
    first build's bars back by ~44 minutes; only phase4 reads its rows."""
    from cnequity.orchestrator.init_phases import DEFAULT_INIT_PHASES

    init_data_layout(cfg)
    cfg.init_phases = list(DEFAULT_INIT_PHASES)
    calls = _steps_failing(monkeypatch, "nothing")
    result = JobEngine(cfg).run_init_phases(date(2024, 6, 28))

    assert result["status"] == "success"
    assert calls.index("daily_bars") < calls.index("corporate_actions")
    assert calls.index("corporate_actions") < calls.index("derive_adj_factors")


def test_an_unedited_legacy_phase_list_gets_the_new_order():
    from cnequity.orchestrator.init_phases import (
        DEFAULT_INIT_PHASES,
        LEGACY_DEFAULT_INIT_PHASES,
        configured_init_phases,
    )

    assert configured_init_phases(list(LEGACY_DEFAULT_INIT_PHASES)) == DEFAULT_INIT_PHASES
    assert configured_init_phases([]) == DEFAULT_INIT_PHASES
    # Anything else is the operator's own choice.
    custom = ["phase1_reference", "phase2a_corporate_actions", "phase2c_daily_bars_backfill"]
    assert configured_init_phases(custom) == custom


def test_a_recorded_init_keeps_its_own_phase_order(cfg):
    """A run started before the reorder resumes in the order it recorded."""
    from cnequity.orchestrator.init_phases import LEGACY_DEFAULT_INIT_PHASES

    init_data_layout(cfg)
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run(
        "init", {"phases": list(LEGACY_DEFAULT_INIT_PHASES), "trade_date": "2024-06-28"}
    )
    assert engine._init_phases_list(run_id) == list(LEGACY_DEFAULT_INIT_PHASES)


def test_retry_starts_bars_the_phase_order_runs_before_corporate_actions(cfg):
    """daily_bars declares corporate_actions for daily-wave ordering. In an init
    that schedules the sweep later, honoring that edge on retry would leave
    bars waiting for a phase that only starts after them."""
    from cnequity.orchestrator.init_phases import (
        DEFAULT_INIT_PHASES,
        LEGACY_DEFAULT_INIT_PHASES,
        expected_steps,
    )

    init_data_layout(cfg)
    engine = JobEngine(cfg)
    manifest = engine.manifest
    phases = list(DEFAULT_INIT_PHASES)
    run_id = manifest.start_run("init", {"phases": phases, "trade_date": "2024-06-28"})
    for step in ("instruments", "trading_calendar"):
        manifest.start_batch(run_id, f"b-{step}", step, step)
        manifest.finish_batch(run_id, f"b-{step}", "success", rows_read=1, rows_written=1)

    assert engine._missing_init_steps(run_id) == ["daily_bars"]
    ready, blocked = engine._split_runnable_missing(
        run_id, ["daily_bars"], in_run=set(expected_steps(phases)), init_phases=phases
    )
    assert (ready, blocked) == (["daily_bars"], [])

    # The legacy order still runs the sweep first, so the edge still gates.
    legacy = list(LEGACY_DEFAULT_INIT_PHASES)
    ready, blocked = engine._split_runnable_missing(
        run_id, ["daily_bars"], in_run=set(expected_steps(legacy)), init_phases=legacy
    )
    assert (ready, blocked) == ([], ["daily_bars"])


@pytest.mark.parametrize(
    "failing", ["derive_adj_factors", "derive_industry_index", "audit", "trading_status_derive"]
)
def test_a_derive_or_audit_failure_leaves_the_published_lake_usable(cfg, monkeypatch, failing):
    """compact has published the bars by then; failing the build over a derive
    skipped phase 5 and reported queryable bars as an unusable lake."""
    from cnequity.orchestrator.init_phases import DEFAULT_INIT_PHASES

    init_data_layout(cfg)
    cfg.init_phases = list(DEFAULT_INIT_PHASES)
    calls = _steps_failing(monkeypatch, failing)
    engine = JobEngine(cfg)
    result = engine.run_init_phases(date(2024, 6, 28))

    assert "trading_status_derive" in calls
    assert result["lake_usable"] is True
    assert result["owed_steps"] == [failing]
    assert engine.manifest.latest_incomplete_init_run()["run_id"] == result["run_id"]


def test_a_compact_failure_still_stops_the_build(cfg, monkeypatch):
    from cnequity.orchestrator.init_phases import DEFAULT_INIT_PHASES

    init_data_layout(cfg)
    cfg.init_phases = list(DEFAULT_INIT_PHASES)
    calls = _steps_failing(monkeypatch, "compact")
    result = JobEngine(cfg).run_init_phases(date(2024, 6, 28))

    assert result["lake_usable"] is False
    assert "trading_status_derive" not in calls


def test_resume_redoes_an_owed_derive_and_finishes(cfg, monkeypatch):
    from cnequity.orchestrator.init_phases import DEFAULT_INIT_PHASES

    init_data_layout(cfg)
    cfg.init_phases = list(DEFAULT_INIT_PHASES)
    _steps_failing(monkeypatch, "derive_adj_factors")
    engine = JobEngine(cfg)
    first = engine.run_init_phases(date(2024, 6, 28))
    assert first["owed_steps"] == ["derive_adj_factors"]

    calls = _steps_failing(monkeypatch, "nothing")
    resumed = engine.run_init_phases(date(2024, 6, 28), resume=True, resume_run_id=first["run_id"])

    assert "derive_adj_factors" in calls
    assert resumed["owed_steps"] == []
    assert resumed["status"] == "success"


def test_the_factor_prefetch_spans_bars_and_actions_and_stops_before_finalize(cfg, monkeypatch):
    from cnequity.derive import adj_factors
    from cnequity.orchestrator.init_phases import DEFAULT_INIT_PHASES

    init_data_layout(cfg)
    cfg.init_phases = list(DEFAULT_INIT_PHASES)
    calls = _steps_failing(monkeypatch, "nothing")

    class Recorder:
        def __init__(self, config, trade_date):
            pass

        def start(self):
            calls.append("prefetch:start")
            return self

        def stop(self, timeout=60.0):
            calls.append("prefetch:stop")

    monkeypatch.setattr(adj_factors, "BackgroundFactorPrefetch", Recorder)
    engine = JobEngine(cfg)
    result = engine.run_init_phases(date(2024, 6, 28))

    start, stop = calls.index("prefetch:start"), calls.index("prefetch:stop")
    assert calls.count("prefetch:start") == calls.count("prefetch:stop") == 1
    assert start < calls.index("daily_bars") < calls.index("corporate_actions") < stop
    assert stop < calls.index("derive_adj_factors")
    assert result["status"] != "failed"
    assert engine.manifest.latest_incomplete_init_run() is None


def test_an_init_that_stops_early_also_stops_the_prefetch(cfg, monkeypatch):
    from cnequity.derive import adj_factors
    from cnequity.orchestrator.init_phases import DEFAULT_INIT_PHASES

    init_data_layout(cfg)
    cfg.init_phases = list(DEFAULT_INIT_PHASES)
    calls = _steps_failing(monkeypatch, "daily_bars")
    stopped = []

    class Recorder:
        def __init__(self, config, trade_date):
            pass

        def start(self):
            return self

        def stop(self, timeout=60.0):
            stopped.append(True)

    monkeypatch.setattr(adj_factors, "BackgroundFactorPrefetch", Recorder)
    result = JobEngine(cfg).run_init_phases(date(2024, 6, 28))
    assert result["status"] == "failed"
    assert "compact" not in calls
    assert stopped == [True]
