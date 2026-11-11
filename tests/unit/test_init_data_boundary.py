"""Init owns current status and scoped delisted recovery, not historical ST."""

from datetime import date

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.domain.schemas import with_provenance
from cnequity.orchestrator.engine import JobEngine
from cnequity.run_scope import backfill_of, set_backfill
from cnequity.steps import reference
from cnequity.storage import StagingWriter


def _snapshot(monkeypatch):
    seen = []

    def fetch(config, dataset, trade_date, fetch_fn, **kwargs):
        assert kwargs["snapshot_only"] is True
        seen.append((backfill_of(config).active, dataset, trade_date))
        return pl.DataFrame(), []

    def reject_history(*args, **kwargs):
        pytest.fail("init must not start a historical ST sweep")

    monkeypatch.setattr(reference, "fetch_incremental_daily", fetch)
    monkeypatch.setattr(reference, "load_symbols", lambda config: ["600001.SH"])
    monkeypatch.setattr(reference, "_backfill_trading_status_st", reject_history)
    monkeypatch.setattr(reference, "_drop_unlisted_codes", lambda c, df, d, ctx: (df, [], None))
    monkeypatch.setattr(reference, "snapshot_trading_status_exchange", lambda *a, **k: 0)
    return seen


def test_init_status_uses_snapshot_without_mutating_parallel_index_config(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, active=True)
    seen = _snapshot(monkeypatch)
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run("init", {})

    result = engine.run_step("trading_status", date(2026, 9, 30), run_id)

    assert result["status"] == "success"
    assert seen == [(False, "trading_status", date(2026, 9, 30))]
    assert backfill_of(cfg).active is True


def test_explicit_backfill_still_uses_historical_st(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, active=True)
    monkeypatch.setattr(
        reference, "_backfill_trading_status_st", lambda *a, **k: {"completed_symbols": 5560}
    )
    assert reference.step_trading_status(cfg, date(2026, 9, 30), "backfill", {}) == {
        "completed_symbols": 5560
    }


def test_legacy_init_st_warning_is_superseded_and_staging_preserved(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, active=True)
    seen = _snapshot(monkeypatch)
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run("init", {})
    engine.manifest.start_batch(
        run_id, "old-st", dataset="trading_status", task_id="trading_status"
    )
    engine.manifest.finish_batch(run_id, "old-st", "warning")
    writer = StagingWriter(cfg.staging_root)
    frame = with_provenance(
        pl.DataFrame(
            {
                "symbol": ["600001.SH"],
                "trade_date": [date(2026, 9, 30)],
                "is_trading": [True],
                "status": ["normal"],
                "risk_warning": [False],
            }
        ),
        source="baostock",
        data_version="v1",
    )
    writer.write_batch("trading_status", run_id, "old-rows", frame)
    staged = writer.list_run_files("trading_status", run_id)[0]
    before = staged.read_bytes()

    result = engine._run_step("trading_status", date(2026, 9, 30), run_id, {}, retry_of=["old-st"])

    assert result["status"] == "success"
    assert seen[0][0] is False
    assert engine.manifest.get_batch(run_id, "old-st")["status"] == "superseded"
    assert staged.read_bytes() == before


@pytest.mark.parametrize("job", ["daily", "backfill"])
def test_automatic_recovery_is_specific_to_init(tmp_path, monkeypatch, job):
    cfg = Config(data_root=tmp_path / "data")
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run(job, {})
    from cnequity.orchestrator.registry import StepEntry

    monkeypatch.setattr(
        "cnequity.orchestrator.engine.get_step",
        lambda name: StepEntry(fn=lambda *a: {"rows_written": 0}, group="core"),
    )
    monkeypatch.setattr(
        engine, "_recover_init_delisted_bars", lambda *a: pytest.fail("unexpected auto recovery")
    )
    assert engine.run_step("daily_bars", date(2026, 9, 30), run_id)["status"] == "success"


def test_init_recovery_failure_stops_daily_fetch(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run("init", {})
    from cnequity.orchestrator.registry import StepEntry

    monkeypatch.setattr(
        "cnequity.orchestrator.engine.get_step",
        lambda name: StepEntry(
            fn=lambda *a: pytest.fail("daily fetch ran after failure"), group="core"
        ),
    )

    def fail(*args):
        raise RuntimeError("unresolved delisted target")

    monkeypatch.setattr(engine, "_recover_init_delisted_bars", fail)
    result = engine.run_step("daily_bars", date(2026, 9, 30), run_id)
    assert result["status"] == "failed"
    assert "unresolved delisted target" in result["error"]


def test_default_init_completes_with_current_status_and_index_history(tmp_path, monkeypatch):
    from cnequity.orchestrator import engine as module
    from cnequity.orchestrator.init_phases import DEFAULT_INIT_PHASES
    from cnequity.orchestrator.registry import StepEntry

    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, start=date(2023, 9, 30))
    engine = JobEngine(cfg)
    seen = _snapshot(monkeypatch)
    index_flags = []

    def get_step(name):
        if name == "trading_status":
            fn = reference.step_trading_status
        else:

            def fn(config, trade_date, run_id, context):
                if name == "index_bars":
                    index_flags.append(backfill_of(config).active)
                return {"rows_written": 0}

        return StepEntry(fn=fn, group="core", requires_workers=False)

    monkeypatch.setattr(module, "get_step", get_step)
    result = engine.run_init_phases(date(2026, 9, 30))
    assert result["status"] == "success"
    assert [phase["phase"] for phase in result["phases"]] == DEFAULT_INIT_PHASES
    assert index_flags == [True]
    assert seen == [(False, "trading_status", date(2026, 9, 30))]


@pytest.mark.parametrize("is_init, severity", [(True, "info"), (False, "warning")])
def test_missing_historical_st_is_optional_only_for_init(tmp_path, monkeypatch, is_init, severity):
    from cnequity.quality import audit

    cfg = Config(data_root=tmp_path / "data")
    monkeypatch.setattr(audit, "trading_status_coverage_start", lambda c: date(2026, 9, 30))
    monkeypatch.setattr(
        audit, "st_evidence_coverage_report", lambda *a: {"verified": False, "reason": "missing"}
    )
    findings = audit._collect_lake_findings(cfg, date(2026, 9, 30), {"_init": is_init})
    finding = next(f for f in findings if f["check"] == "trading_status_coverage_start")
    assert finding["severity"] == severity
    assert finding["st_evidence_verified"] is False
