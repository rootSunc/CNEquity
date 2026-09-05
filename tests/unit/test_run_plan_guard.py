"""A run's plan outlives the process that was executing it.

Batch receipts describe the steps that produced one. A daily job killed
mid-DAG (OOM, `kill -9`) leaves nothing at all behind for the steps it never
reached, so the ledger reads as a clean run: `cne retry` repaired whatever had
failed and closed the day as `success` with the rest of it silently missing.
These tests pin the plan to the run and make a truncated run say so.
"""

from datetime import date

import cnequity.steps  # noqa: F401 — registers the steps used below
from cnequity.config import Config, WaveConfig
from cnequity.orchestrator.engine import JobEngine
from cnequity.storage.layout import init_data_layout

TRADE_DATE = date(2024, 6, 28)
PLAN = ["instruments", "announcement_index", "compact"]


def _engine(tmp_path) -> JobEngine:
    cfg = Config(data_root=tmp_path / "data", tdx_allow_mock=True)
    init_data_layout(cfg)
    return JobEngine(cfg)


def _crashed_run(engine: JobEngine, *, plan=PLAN, reached=("instruments",)) -> str:
    """A run that recorded its plan, then died after *reached*."""
    manifest = engine.manifest
    run_id = manifest.start_run(
        "daily:capital",
        {"trade_date": TRADE_DATE.isoformat(), "backfill": False, "planned_steps": list(plan)},
    )
    for step in reached:
        manifest.start_batch(run_id, f"b-{step}", step, step)
        manifest.finish_batch(run_id, f"b-{step}", "failed", error_message="killed: OOM")
    manifest.finish_run(run_id, "failed")
    return run_id


def _stub_steps(engine: JobEngine, monkeypatch, *, failing: set[str] | None = None) -> list[str]:
    """Record each step and land a batch for it, without touching a source."""
    failing = failing or set()
    ran: list[str] = []

    def fake_run_step(name, trade_date, run_id, context, *, retry_of=None):
        ran.append(name)
        status = "failed" if name in failing else "success"
        batch_id = retry_of[0] if retry_of else f"b-{name}"
        engine.manifest.start_batch(run_id, batch_id, name, name)
        engine.manifest.finish_batch(
            run_id,
            batch_id,
            status,
            error_message="still broken" if status == "failed" else None,
        )
        return {"step": name, "status": status, "rows_read": 0, "rows_written": 0}

    monkeypatch.setattr(engine, "_run_step", fake_run_step)
    return ran


def test_run_job_pins_its_plan_to_the_run(tmp_path, monkeypatch):
    engine = _engine(tmp_path)
    _stub_steps(engine, monkeypatch)

    result = engine.run_job(
        "daily:capital",
        TRADE_DATE,
        waves=[WaveConfig(name="group:capital", parallel=False, steps=PLAN)],
    )

    metadata = engine.manifest.get_run_metadata(result["run_id"])
    assert metadata["planned_steps"] == PLAN


def test_retry_runs_the_steps_the_crash_never_reached(tmp_path, monkeypatch):
    engine = _engine(tmp_path)
    run_id = _crashed_run(engine)
    ran = _stub_steps(engine, monkeypatch)

    result = engine.run_job("retry", TRADE_DATE, run_id=run_id, retry_failed_only=True)

    # The failed batch is retried and the two steps that never started run too.
    assert ran[0] == "instruments"
    assert {"announcement_index", "compact"} <= set(ran)
    assert result["missing_steps"] == ["announcement_index", "compact"]
    assert result["missing_steps_unresolved"] == []
    assert result["status"] == "success"
    assert engine.manifest.get_run(run_id)["status"] == "success"


def test_retry_refuses_success_while_a_planned_step_is_missing(tmp_path, monkeypatch):
    """The upstream stays broken, so its dependent must not run — nor pass."""
    engine = _engine(tmp_path)
    run_id = _crashed_run(engine)
    ran = _stub_steps(engine, monkeypatch, failing={"instruments"})

    result = engine.run_job("retry", TRADE_DATE, run_id=run_id, retry_failed_only=True)

    assert "announcement_index" not in ran  # its input in this run never landed
    assert result["status"] == "failed"
    assert "announcement_index" in result["missing_steps_unresolved"]
    run = engine.manifest.get_run(run_id)
    assert run["status"] == "failed"
    assert "planned steps never ran" in (run["error_message"] or "")


def test_a_run_with_a_clean_ledger_but_a_truncated_plan_is_not_a_success(tmp_path, monkeypatch):
    """Nothing failed — the process simply died before the rest of the plan."""
    engine = _engine(tmp_path)
    manifest = engine.manifest
    run_id = manifest.start_run(
        "daily:capital",
        {"trade_date": TRADE_DATE.isoformat(), "backfill": False, "planned_steps": list(PLAN)},
    )
    manifest.start_batch(run_id, "b-instruments", "instruments", "instruments")
    manifest.finish_batch(run_id, "b-instruments", "success")
    manifest.finish_run(run_id, "failed")
    _stub_steps(engine, monkeypatch, failing={"announcement_index"})

    result = engine.run_job("retry", TRADE_DATE, run_id=run_id, retry_failed_only=True)

    assert result["status"] == "failed"


def test_a_run_started_before_plans_were_recorded_keeps_its_old_behaviour(tmp_path, monkeypatch):
    """No plan means no claim about what is missing — do not invent one."""
    engine = _engine(tmp_path)
    manifest = engine.manifest
    run_id = manifest.start_run("daily:capital", {"trade_date": TRADE_DATE.isoformat()})
    manifest.start_batch(run_id, "b-instruments", "instruments", "instruments")
    manifest.finish_batch(run_id, "b-instruments", "failed", error_message="boom")
    manifest.finish_run(run_id, "failed")
    ran = _stub_steps(engine, monkeypatch)

    result = engine.run_job("retry", TRADE_DATE, run_id=run_id, retry_failed_only=True)

    assert "announcement_index" not in ran
    assert result["status"] == "success"


def test_a_missing_step_is_not_blocked_by_a_derive_it_cannot_name(tmp_path, monkeypatch):
    """`derive_industry_index` books its batch under `industry_index`.

    Readiness is judged from the ledger, so matching only one of a batch's two
    names would leave `audit` permanently "blocked" by an upstream that had in
    fact succeeded.
    """
    engine = _engine(tmp_path)
    manifest = engine.manifest
    plan = ["compact", "derive_adj_factors", "derive_industry_index", "audit"]
    run_id = manifest.start_run(
        "daily:core",
        {"trade_date": TRADE_DATE.isoformat(), "backfill": False, "planned_steps": plan},
    )
    for step, dataset in (
        ("compact", "compact"),
        ("derive_adj_factors", "adj_factors"),
        ("derive_industry_index", "industry_index"),
    ):
        manifest.start_batch(run_id, f"b-{step}", step, dataset)
        manifest.finish_batch(run_id, f"b-{step}", "success")
    manifest.finish_run(run_id, "failed")
    ran = _stub_steps(engine, monkeypatch)

    result = engine.run_job("retry", TRADE_DATE, run_id=run_id, retry_failed_only=True)

    assert "audit" in ran
    assert result["status"] == "success"
