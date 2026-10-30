"""Execution closure never substitutes for scope or publication evidence."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import date

import polars as pl
import pytest
from click.testing import CliRunner

from cnequity.cli.main import cli
from cnequity.config import Config
from cnequity.domain.schemas import with_provenance
from cnequity.orchestrator.compact_gate import compact_allowed, publication_files
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.init_phases import init_run_complete
from cnequity.orchestrator.manifest import Manifest
from cnequity.orchestrator.outcomes import (
    InputUnavailableError,
    SourceUnavailableError,
    error_kind,
    execution_exit_code,
    step_outcome,
)
from cnequity.orchestrator.registry import StepEntry
from cnequity.storage.parquet import StagingWriter
from cnequity.storage.revisions import RevisionStore
from cnequity.storage.state import StateStore

DAY = date(2024, 6, 28)


def _bars(symbol="600519.SH", day=DAY):
    return with_provenance(
        pl.DataFrame(
            {
                "symbol": [symbol],
                "trade_date": [day],
                "open": [10.0],
                "high": [10.0],
                "low": [10.0],
                "close": [10.0],
                "volume": [100],
                "amount": [1000.0],
            }
        ),
        source="tdx_protocol",
        data_version="v2",
    )


def _config(tmp_path):
    return Config(data_root=tmp_path / "data", publication_gate="off", audit_gate="off")


@pytest.mark.parametrize(
    ("status", "out", "execution", "coverage"),
    [
        ("warning", {"rows_written": 1}, "completed", "partial"),
        ("success", {}, "completed", "unknown"),
        ("success", {"coverage_complete": True}, "completed", "complete"),
        ("skipped", {"reason_code": "input_unavailable"}, "skipped", "partial"),
        ("failed", {"reason_code": "source_unavailable"}, "completed", "partial"),
        ("failed", {}, "failed", "unknown"),
        ("blocked", {}, "failed", "unknown"),
    ],
)
def test_result_axes_do_not_infer_coverage_from_success(status, out, execution, coverage):
    result = step_outcome(status, out)
    assert result.execution_status == execution
    assert result.coverage_status == coverage
    assert result.result_schema_version == 2


def test_unknown_errors_are_not_source_outages():
    assert error_kind(RuntimeError("network timeout in my buggy computation")) == "execution_error"
    assert error_kind(OSError("disk full")) == "storage_failure"
    assert step_outcome("failed", error=InputUnavailableError()).execution_status == "skipped"
    assert execution_exit_code("degraded") == 0
    assert execution_exit_code("failed") == 1


def test_automatic_retry_uses_typed_reason_before_legacy_text():
    from cnequity.orchestrator.engine import _is_transient_retry_error

    assert _is_transient_retry_error("opaque source refusal", "source_transient")
    assert not _is_transient_retry_error("timeout in buggy computation", "execution_error")
    assert not _is_transient_retry_error("timeout", "storage_failure")
    assert _is_transient_retry_error("legacy connection timeout")


def test_capability_warning_without_usable_results_finishes_degraded(tmp_path):
    manifest = Manifest(tmp_path / "manifest.db")
    run = manifest.start_run("backfill")
    manifest.record_dataset_result(
        run, "trading_status", "fetch", "warning", reason_code="capability_limit"
    )
    manifest.finish_run(run, "warning")
    result = manifest.get_run(run)
    assert result["status"] == "degraded"
    assert result["execution_status"] == "completed"
    assert result["coverage_status"] == "partial"
    assert manifest.aggregate_run_status(run)["usable_result"] is False


def test_source_cooling_defers_unswept_days_without_failed_request_counts(tmp_path):
    from cnequity.domain.http_policy import SourceCoolingDown
    from cnequity.steps.common import walk_day_backfill

    cfg = _config(tmp_path)
    cfg._backfill_start = date(2024, 6, 24)
    cfg._backfill_end = DAY
    calls = []

    def fetch(day):
        calls.append(day)
        if len(calls) > 1:
            raise SourceCoolingDown("source cooling down")
        return pl.DataFrame({"trade_date": [day], "metric_id": ["advance_ratio"], "value": [0.5]})

    result = walk_day_backfill(cfg, DAY, "cooling", "market_breadth", fetch, source="derived")
    assert calls == [date(2024, 6, 24), date(2024, 6, 25)]
    assert result["status"] == "warning"
    assert result["failed_days"] == 0
    assert result["days_deferred"] == 4
    assert result["days_fetched"] == 1
    gaps = StateStore(cfg.meta_root).get_payload("market_breadth")["missing_ranges"]
    assert [row["start"] for row in gaps] == [f"2024-06-{day}" for day in (25, 26, 27, 28)]
    assert all(row["last_failure"] == "source_cooling_down" for row in gaps)


@pytest.mark.parametrize(
    ("status", "rows", "reason", "expected"),
    [
        ("warning", 1, "source_unavailable", "degraded"),
        ("failed", 0, "source_unavailable", "degraded"),
        ("failed", 1, "execution_error", "failed"),
        ("skipped", 0, "input_unavailable", "degraded"),
    ],
)
def test_source_limits_and_execution_errors_apply_to_research_too(
    tmp_path, status, rows, reason, expected
):
    manifest = Manifest(tmp_path / "manifest.db")
    run = manifest.start_run("backfill")
    manifest.record_dataset_result(
        run,
        "adj_factors",
        "derive",
        status,
        criticality="research",
        rows_written=rows,
        reason_code=reason,
    )
    manifest.finish_run(run, "success")
    row = manifest.get_run(run)
    assert row["status"] == expected
    assert row["execution_status"] == ("failed" if expected == "failed" else "completed")


def test_verified_no_data_is_a_usable_result(tmp_path):
    manifest = Manifest(tmp_path / "manifest.db")
    run = manifest.start_run("backfill")
    manifest.record_dataset_result(
        run,
        "corporate_actions",
        "fetch",
        "success",
        rows_written=0,
        usable_result=True,
        coverage_status="complete",
    )
    manifest.record_dataset_result(
        run,
        "adj_factors",
        "derive",
        "skipped",
        reason_code="input_unavailable",
    )
    assert manifest.aggregate_run_status(run)["status"] == "degraded"


def test_old_receipts_migrate_without_inventing_completion(tmp_path):
    path = tmp_path / "manifest.db"
    metadata = '{"checkpoint":"keep","phases":["phase3_index_and_status"]}'
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.executescript("""
            CREATE TABLE ingestion_runs (
                run_id TEXT PRIMARY KEY, job_name TEXT, status TEXT, started_at TEXT,
                finished_at TEXT, rows_read INTEGER, rows_written INTEGER,
                error_message TEXT, metadata_json TEXT
            );
            CREATE TABLE dataset_results (
                run_id TEXT, dataset TEXT, stage TEXT, status TEXT
            );
        """)
        conn.execute(
            "INSERT INTO ingestion_runs VALUES ('old', 'init', 'warning', ?, NULL, 0, 0, NULL, ?)",
            ("2024-06-28T00:00:00+00:00", metadata),
        )
        conn.execute(
            "INSERT INTO dataset_results VALUES ('old','trading_status','fetch','warning')"
        )
    for _ in range(2):
        manifest = Manifest(path)
        receipt = manifest.get_dataset_results("old")[0]
        assert receipt["result_schema_version"] == 1
        assert receipt["execution_status"] is None
        assert receipt["coverage_status"] == "unknown"
        assert manifest.get_run("old")["metadata_json"] == metadata
        assert len(manifest.get_dataset_results("old")) == 1


def test_partial_publication_keeps_missing_keys_and_retry_attempts(tmp_path):
    cfg = _config(tmp_path)
    engine = JobEngine(cfg)
    run = engine.manifest.start_run("backfill")
    engine.manifest.start_batch(run, "source", "daily_bars", "daily_bars")
    engine.manifest.finish_batch(
        run,
        "source",
        "failed",
        execution_status="completed",
        reason_code="source_unavailable",
    )
    engine.manifest.record_dataset_result(
        run,
        "daily_bars",
        "fetch",
        "warning",
        rows_written=1,
        reason_code="source_unavailable",
    )
    state = StateStore(cfg.meta_root)
    state.record_outstanding_keys(
        "daily_bars", [("000001.SZ", DAY)], run_id=run, reason="source_unavailable"
    )
    writer = StagingWriter(cfg.staging_root)
    sealed = writer.write_batch("daily_bars", run, "valid", _bars())
    unsealed = sealed.with_name("part-unknown.parquet")
    unsealed.write_bytes(sealed.read_bytes())
    assert publication_files(engine.manifest, run, "daily_bars") == [sealed]
    result = engine.run_step("compact", DAY, run)
    receipt = engine.manifest.get_dataset_result(run, "daily_bars", "publish_revision")
    assert result["status"] == "warning"
    assert receipt["publication_status"] == "partial"
    assert receipt["revision_id"]
    assert engine.manifest.get_batch(run, "source")["status"] == "failed"
    assert len(state.get_outstanding_keys("daily_bars")) == 1
    root = RevisionStore(cfg.meta_root, cfg.curated_root).current_root("daily_bars")
    assert pl.read_parquet(next(root.rglob("*.parquet")))["symbol"].to_list() == ["600519.SH"]
    assert state.get_payload("daily_bars")["complete_through"] < DAY.isoformat()


def test_active_and_unsealed_legacy_units_stay_blocked(tmp_path):
    cfg = _config(tmp_path)
    manifest = Manifest(cfg.manifest_path)
    run = manifest.start_run("backfill")
    manifest.start_batch(run, "active", "daily_bars", "daily_bars")
    path = StagingWriter(cfg.staging_root).write_batch("daily_bars", run, "valid", _bars())
    assert compact_allowed(manifest, run, "daily_bars")[0] is False
    manifest.finish_batch(run, "active", "failed")
    path.with_suffix(".sealed.json").unlink()
    assert compact_allowed(manifest, run, "daily_bars")[0] is False


def test_corrupt_sealed_bytes_never_replace_current(tmp_path):
    cfg = _config(tmp_path)
    engine = JobEngine(cfg)
    run = engine.manifest.start_run("backfill")
    writer = StagingWriter(cfg.staging_root)
    path = writer.write_batch("daily_bars", run, "valid", _bars())
    path.write_bytes(b"corrupt")
    result = engine.run_step("compact", DAY, run)
    assert result["status"] == "failed"
    assert result["execution_status"] == "failed"
    assert RevisionStore(cfg.meta_root, cfg.curated_root).current_pointer("daily_bars") is None


def test_published_repair_clears_only_matching_keys(tmp_path):
    cfg = _config(tmp_path)
    engine = JobEngine(cfg)
    run = engine.manifest.start_run("backfill")
    state = StateStore(cfg.meta_root)
    state.record_outstanding_keys(
        "daily_bars",
        [("600519.SH", DAY), ("000001.SZ", DAY)],
        run_id=run,
        reason="source_unavailable",
    )
    StagingWriter(cfg.staging_root).write_batch("daily_bars", run, "valid", _bars())
    engine.run_step("compact", DAY, run)
    assert [(r["symbol"], r["trade_date"]) for r in state.get_outstanding_keys("daily_bars")] == [
        ("000001.SZ", DAY.isoformat())
    ]
    pointer = RevisionStore(cfg.meta_root, cfg.curated_root).current_pointer("daily_bars")
    receipt = json.loads((cfg.meta_root / pointer["receipt"]).read_text())
    coverage = receipt["metadata"]["coverage"]
    assert coverage["resolved_outstanding_keys"] == 1
    assert coverage["gaps_source"] == "dataset_state"
    assert coverage["gaps"]["counts"]["outstanding_keys"] == 1
    assert [r["symbol"] for r in coverage["gaps"]["sample"]["outstanding_keys"]] == ["000001.SZ"]


def test_receipt_bounds_the_gap_index_it_records(tmp_path):
    cfg = _config(tmp_path)
    engine = JobEngine(cfg)
    run = engine.manifest.start_run("backfill")
    keys = [(f"{index:06d}.SZ", DAY) for index in range(1, 501)]
    StateStore(cfg.meta_root).record_outstanding_keys(
        "daily_bars", keys, run_id=run, reason="source_unavailable"
    )
    StagingWriter(cfg.staging_root).write_batch("daily_bars", run, "valid", _bars())
    engine.run_step("compact", DAY, run)
    pointer = RevisionStore(cfg.meta_root, cfg.curated_root).current_pointer("daily_bars")
    gaps = json.loads((cfg.meta_root / pointer["receipt"]).read_text())["metadata"]["coverage"][
        "gaps"
    ]
    assert gaps["counts"]["outstanding_keys"] == 500
    assert len(gaps["sample"]["outstanding_keys"]) == 20
    assert gaps["truncated"] is True


def test_source_limited_init_closes_and_reaches_later_phases(tmp_path, monkeypatch):
    cfg = _config(tmp_path)
    cfg.init_phases = ["phase3_index_and_status", "phase5_derive_and_publish"]
    engine = JobEngine(cfg)
    seen = []

    def entry(name):
        def run(*args):
            seen.append(name)
            if name in {"index_bars", "trading_status"}:
                raise SourceUnavailableError("source unavailable")
            if name == "trading_status_derive":
                raise InputUnavailableError("daily_bars absent")
            return {"rows_written": 0}

        return StepEntry(fn=run, group="core")

    monkeypatch.setattr("cnequity.orchestrator.engine.get_step", entry)
    result = engine.run_init_phases(DAY)
    assert "trading_status_derive" in seen
    assert result["status"] == "degraded"
    assert result["execution_status"] == "completed"
    assert result["usable_result"] is False
    assert {item["dataset"] for item in result["fallback"]} == {"index_bars", "trading_status"}
    assert init_run_complete(cfg.init_phases, engine._init_execution_batches(result["run_id"]))
    assert engine.manifest.latest_incomplete_init_run() is None


def test_useful_partial_init_is_terminal_and_reports_degradation(tmp_path, monkeypatch):
    cfg = _config(tmp_path)
    cfg.init_phases = ["phase3_index_and_status", "phase5_derive_and_publish"]
    engine = JobEngine(cfg)

    def entry(name):
        def run(*args):
            if name == "trading_status":
                return {"status": "warning", "rows_written": 1, "coverage_complete": False}
            return {"rows_written": 1}

        return StepEntry(fn=run, group="core")

    monkeypatch.setattr("cnequity.orchestrator.engine.get_step", entry)
    result = engine.run_init_phases(DAY)
    assert result["status"] == "degraded"
    assert result["execution_status"] == "completed"
    assert result["coverage_status"] == "partial"
    assert engine.manifest.latest_incomplete_init_run() is None


def test_status_reports_failed_run_without_failing_the_read(tmp_path):
    cfg = _config(tmp_path)
    run = Manifest(cfg.manifest_path).start_run("backfill")
    Manifest(cfg.manifest_path).finish_run(run, "failed")
    config = tmp_path / "cnequity.toml"
    config.write_text(f'[data]\nroot = "{cfg.data_root.as_posix()}"\n')
    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--run", "latest", "--config", str(config)])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["status"] == "failed"
    gated = runner.invoke(cli, ["status", "--gate", "--run", "latest", "--config", str(config)])
    assert gated.exit_code == 1, gated.output


def test_single_derive_with_no_input_finishes_with_a_skip_receipt(tmp_path):
    cfg = _config(tmp_path)
    config = tmp_path / "cnequity.toml"
    config.write_text(f'[data]\nroot = "{cfg.data_root.as_posix()}"\n')
    result = CliRunner().invoke(cli, ["derive", "trading_status", "--config", str(config)])
    assert result.exit_code == 0, result.output
    summary = json.loads(result.stdout)
    assert summary["execution_status"] == "completed"
    assert summary["coverage_status"] == "partial"
    assert summary["fallback"][0]["retry_command"].startswith("cne derive trading_status")
    receipt = Manifest(cfg.manifest_path).get_dataset_result(
        summary["run_id"], "trading_status_derive", "fetch"
    )
    assert receipt["execution_status"] == "skipped"
    assert receipt["reason_code"] == "input_unavailable"


def _factor_frame():
    return with_provenance(
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [DAY],
                "adjust_type": ["hfq"],
                "factor": [1.0],
            }
        ),
        source="sina",
        data_version="v1",
    )


def test_derived_publication_records_partial_input_identity(tmp_path):
    from cnequity.steps.finalize import _capture_derive_inputs, _publish_derived_revision

    cfg = _config(tmp_path)
    root = cfg.curated_root / "daily_bars"
    root.mkdir(parents=True)
    _bars().write_parquet(root / "part.parquet")
    StateStore(cfg.meta_root).record_outstanding_keys(
        "daily_bars", [("000001.SZ", DAY)], run_id="source", reason="source_unavailable"
    )
    inputs = _capture_derive_inputs(cfg, "adj_factors")
    target = cfg.derived_root / "adj_factors"
    target.mkdir(parents=True)
    _factor_frame().write_parquet(target / "part.parquet")
    result = _publish_derived_revision(
        cfg, "adj_factors", "derive", DAY, {}, input_revisions=inputs
    )
    assert result["coverage_status"] == "partial"
    assert result["publication_status"] == "partial"
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    pointer = store.current_pointer("adj_factors")
    receipt = json.loads((cfg.meta_root / pointer["receipt"]).read_text())
    assert receipt["metadata"]["input_revisions"] == inputs


def test_changed_input_revision_cannot_publish_derived_candidate(tmp_path):
    from cnequity.steps.finalize import _capture_derive_inputs, _publish_derived_revision

    cfg = _config(tmp_path)
    root = cfg.curated_root / "daily_bars"
    root.mkdir(parents=True)
    path = root / "part.parquet"
    _bars().write_parquet(path)
    inputs = _capture_derive_inputs(cfg, "adj_factors")
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    _bars("000001.SZ").write_parquet(path)
    from cnequity.domain.contracts import contract_fingerprint, dataset_contract

    contract = dataset_contract("daily_bars")
    store.commit(
        "daily_bars",
        run_id="new-input",
        changed_files=[path],
        schema_version=contract["schema_version"],
        contract_fingerprint=contract_fingerprint(contract),
    )
    target = cfg.derived_root / "adj_factors"
    target.mkdir(parents=True)
    _factor_frame().write_parquet(target / "part.parquet")
    with pytest.raises(InputUnavailableError, match="changed during calculation"):
        _publish_derived_revision(cfg, "adj_factors", "derive", DAY, {}, input_revisions=inputs)
    assert store.current_pointer("adj_factors") is None


def test_storage_failure_stops_later_writes(tmp_path, monkeypatch):
    from cnequity.config.loader import WaveConfig

    cfg = _config(tmp_path)
    engine = JobEngine(cfg)
    seen = []

    def entry(name):
        def run(*args):
            seen.append(name)
            if name == "instruments":
                raise OSError("disk full")
            return {"rows_written": 1}

        return StepEntry(fn=run, group="core")

    monkeypatch.setattr("cnequity.orchestrator.engine.get_step", entry)
    result = engine.run_job(
        "backfill",
        DAY,
        waves=[
            WaveConfig(name="write", parallel=False, steps=["instruments", "trading_calendar"]),
            WaveConfig(name="publish", parallel=False, steps=["compact"]),
        ],
    )
    assert seen == ["instruments"]
    assert result["status"] == "failed"
    assert result["results"][0]["reason_code"] == "storage_failure"


def test_publication_excludes_coverage_only_errors_but_keeps_integrity(tmp_path, monkeypatch):
    from cnequity.quality.publication import _errors

    findings = [
        {"severity": "error", "check": "daily_bars_calendar_missing_day"},
        {"severity": "error", "check": "duplicate_primary_key"},
    ]
    monkeypatch.setattr("cnequity.quality.audit._collect_lake_findings", lambda *a, **kw: findings)
    monkeypatch.setattr("cnequity.quality.audit._profile_adjusted_findings", lambda cfg, rows: rows)
    monkeypatch.setattr("cnequity.quality.source_diff.run_source_diffs", lambda *a, **kw: [])
    assert _errors(_config(tmp_path), DAY) == [findings[1]]


def test_unknown_missing_bar_key_is_not_a_suspension_fact(tmp_path):
    from cnequity.derive.trading_status_history import _suspended_pairs, derive_suspension_history

    cfg = _config(tmp_path)
    days = [date(2024, 6, d) for d in (26, 27, 28)]
    for dataset in ("daily_bars", "instruments", "trading_calendar"):
        (cfg.curated_root / dataset).mkdir(parents=True)
    bars = pl.concat(
        [_bars(symbol, day) for symbol in ("600519.SH", "000001.SZ") for day in (days[0], days[-1])]
    )
    bars.write_parquet(cfg.curated_root / "daily_bars" / "part.parquet")
    pl.DataFrame({"trade_date": days, "is_trading": [True] * 3}).write_parquet(
        cfg.curated_root / "trading_calendar" / "part.parquet"
    )
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "list_date": [date(2010, 1, 1)] * 2,
            "delist_date": [None] * 2,
        },
        schema_overrides={"delist_date": pl.Date},
    ).write_parquet(cfg.curated_root / "instruments" / "part.parquet")
    StateStore(cfg.meta_root).record_outstanding_keys(
        "daily_bars", [("600519.SH", days[1])], run_id="fetch", reason="source_unavailable"
    )
    assert _suspended_pairs(cfg).rows() == [("000001.SZ", days[1])]
    with pytest.raises(InputUnavailableError, match="no daily bars in the requested window"):
        derive_suspension_history(cfg, "derive", start=days[1], end=days[1])


def test_monthly_repair_does_not_count_other_scopes_as_attempted(tmp_path):
    from cnequity.cli.backfill_cmds import _settle_outstanding

    cfg = _config(tmp_path)
    root = cfg.curated_root / "daily_bars"
    root.mkdir(parents=True)
    _bars().write_parquet(root / "part.parquet")
    state = StateStore(cfg.meta_root)
    state.record_outstanding_keys(
        "daily_bars",
        [("000001.SZ", DAY), ("000001.SZ", date(2024, 5, 28))],
        run_id="fetch",
        reason="source_unavailable",
    )
    result = _settle_outstanding(
        cfg, "daily_bars", attempted_scope=({"000001.SZ"}, "2024-06-01", "2024-06-30")
    )
    assert result["still_owed"] == 2
    rows = {row["trade_date"]: row for row in state.get_outstanding_keys("daily_bars")}
    assert rows[DAY.isoformat()]["attempts"] == 1
    assert rows["2024-05-28"].get("attempts", 0) == 0
