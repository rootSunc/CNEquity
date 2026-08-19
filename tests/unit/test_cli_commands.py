"""Offline CLI smoke tests — thin wrappers around engine / derive / cleanup."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest
from click.testing import CliRunner

from cnequity.cli.main import _recover_compactable_backfill_staging, cli
from cnequity.config import Config
from cnequity.config.bootstrap import path_for_toml
from cnequity.derive.adj_factors import AdjFactorsResult
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.manifest import Manifest
from cnequity.orchestrator.run_lock import RunLockError
from cnequity.storage import StagingWriter
from cnequity.storage.source_snapshots import SnapshotCleanupResult
from cnequity.storage.staging_cleanup import StagingCleanupResult


def _write_config(tmp_path, *, extra: str = "") -> str:
    cfg_path = tmp_path / "cnequity.toml"
    cfg_path.write_text(
        f"""
[data]
root = "{path_for_toml(tmp_path / "data")}"

[orchestrator]
workers = 1

[tdx_protocol]
allow_mock = true

[job.init.phases]
names = ["phase1_reference"]

[[job.daily.waves]]
name = "finalize"
parallel = false
steps = ["compact"]
{extra}
"""
    )
    return str(cfg_path)


@pytest.fixture
def cfg_path(tmp_path):
    return _write_config(tmp_path)


def test_config_validate_ok(cfg_path):
    result = CliRunner().invoke(cli, ["config", "validate", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "Configuration OK" in result.output


def test_config_validate_reports_errors(tmp_path):
    cfg_path = tmp_path / "bad.toml"
    cfg_path.write_text(
        f"""
[data]
root = "{path_for_toml(tmp_path / "data")}"

[orchestrator]
workers = 0
"""
    )
    result = CliRunner().invoke(cli, ["config", "validate", "--config", str(cfg_path)])
    assert result.exit_code == 1
    assert "ERROR:" in result.output


def test_backfill_recovers_terminal_staging_before_new_run(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("backfill")
    batch_id = "st-failed"
    manifest.start_batch(
        run_id,
        batch_id,
        task_id="trading_status",
        dataset="trading_status",
        blocks_compaction=False,
    )
    manifest.finish_batch(run_id, batch_id, "failed", error_message="interrupted")
    manifest.finish_run(run_id, "failed", error_message="interrupted")
    StagingWriter(cfg.staging_root).write_batch(
        "trading_status",
        run_id,
        "batch-00000",
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [date(2024, 6, 28)],
                "is_trading": [True],
                "status": ["normal"],
                "source": ["baostock"],
                "data_version": ["v1"],
                "fetched_at": [datetime.now(timezone.utc)],
            }
        ),
    )

    class FakeEngine:
        def __init__(self):
            self.config = cfg
            self.manifest = manifest
            self.calls = []

        def run_step(self, name, trade_date, recovered_run_id):
            self.calls.append((name, trade_date, recovered_run_id))
            return {"status": "success"}

    engine = FakeEngine()
    assert _recover_compactable_backfill_staging(engine, "trading_status") == [run_id]
    assert engine.calls[0][0] == "compact"
    assert engine.calls[0][2] == run_id


def test_run_daily_failure_exits_nonzero(cfg_path, monkeypatch):
    monkeypatch.setattr(
        JobEngine,
        "run_job",
        lambda self, *a, **k: {"run_id": "d-fail", "status": "failed", "results": []},
    )
    result = CliRunner().invoke(cli, ["run", "daily", "--config", cfg_path])
    assert result.exit_code == 1
    assert "d-fail" in result.output


def test_run_daily_run_lock_error(cfg_path, monkeypatch):
    def boom(self, *a, **k):
        raise RunLockError("daily lock held")

    monkeypatch.setattr(JobEngine, "run_job", boom)
    result = CliRunner().invoke(cli, ["run", "daily", "--config", cfg_path])
    assert result.exit_code != 0
    assert "daily lock held" in result.output


def test_run_daily_unknown_group(cfg_path):
    result = CliRunner().invoke(cli, ["run", "daily", "--config", cfg_path, "--group", "nope"])
    assert result.exit_code != 0
    assert "Unknown group" in result.output


def test_status_no_runs(cfg_path):
    result = CliRunner().invoke(cli, ["status", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "No runs yet." in result.output


def test_status_latest_run(cfg_path, monkeypatch):
    class FakeManifest:
        def __init__(self, *a, **k):
            pass

        def latest_run(self):
            return {"run_id": "r1"}

        def run_summary(self, run_id):
            return {"run_id": run_id, "status": "success", "job_name": "daily"}

        def count_stale_running_runs(self, **kwargs):
            return 0

    monkeypatch.setattr("cnequity.cli.main.Manifest", FakeManifest)
    result = CliRunner().invoke(cli, ["status", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["run_id"] == "r1"
    assert payload["status"] == "success"


def test_status_datasets_all_fresh(cfg_path, monkeypatch):
    monkeypatch.setattr(
        "cnequity.cli.main._last_trading_day",
        lambda cfg, today: date(2024, 6, 28),
    )
    monkeypatch.setattr(
        "cnequity.query.reader.list_datasets",
        lambda config=None: pl.DataFrame(
            {
                "dataset": ["daily_bars"],
                "has_data": [True],
                "watermarked": [True],
                "watermark": [date(2024, 6, 28)],
                "coverage_end": [date(2024, 6, 28)],
            }
        ),
    )
    monkeypatch.setattr("cnequity.domain.datasets.is_stale", lambda *a, **k: False)
    result = CliRunner().invoke(cli, ["status", "--datasets", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "last trading day: 2024-06-28" in result.output


def test_status_datasets_ignores_disabled_optional_capture(cfg_path, monkeypatch):
    monkeypatch.setattr(
        "cnequity.cli.main._last_trading_day",
        lambda cfg, today: date(2024, 6, 28),
    )
    monkeypatch.setattr(
        "cnequity.query.reader.list_datasets",
        lambda config=None: pl.DataFrame(
            {
                "dataset": ["trade_ticks"],
                "has_data": [True],
                "watermarked": [True],
                "watermark": [date(2024, 6, 1)],
                "coverage_end": [date(2024, 6, 1)],
            }
        ),
    )
    result = CliRunner().invoke(cli, ["status", "--datasets", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "n/a" in result.output
    assert "STALE datasets" not in result.output


def test_retry_unknown_run(cfg_path, monkeypatch):
    class FakeManifest:
        def get_run(self, run_id):
            return None

    class FakeEngine:
        def __init__(self, cfg):
            self.manifest = FakeManifest()

    monkeypatch.setattr("cnequity.cli.main.JobEngine", FakeEngine)
    result = CliRunner().invoke(cli, ["retry", "--config", cfg_path, "--run-id", "missing"])
    assert result.exit_code != 0
    assert "Unknown run_id" in result.output


def test_retry_init_run(cfg_path, monkeypatch):
    class FakeManifest:
        def get_run(self, run_id):
            return {"run_id": run_id, "job_name": "init"}

    class FakeEngine:
        def __init__(self, cfg):
            self.manifest = FakeManifest()

        def resume_init(self, **kwargs):
            return {"run_id": kwargs["run_id"], "status": "success"}

        def run_job(self, *a, **k):
            raise AssertionError("non-init path should not run")

    monkeypatch.setattr("cnequity.cli.main.JobEngine", FakeEngine)
    result = CliRunner().invoke(cli, ["retry", "--config", cfg_path, "--run-id", "init-1"])
    assert result.exit_code == 0, result.output
    assert "init-1" in result.output


def test_retry_failed_job(cfg_path, monkeypatch):
    class FakeManifest:
        def get_run(self, run_id):
            return {"run_id": run_id, "job_name": "daily"}

    class FakeEngine:
        def __init__(self, cfg):
            self.manifest = FakeManifest()

        def run_job(self, job_name, **kwargs):
            assert job_name == "retry"
            assert kwargs.get("retry_failed_only") is True
            return {"run_id": kwargs["run_id"], "status": "failed"}

    monkeypatch.setattr("cnequity.cli.main.JobEngine", FakeEngine)
    result = CliRunner().invoke(cli, ["retry", "--config", cfg_path, "--run-id", "d1"])
    assert result.exit_code == 1


def test_retry_failed_groups_retries_latest_failed_per_group(cfg_path, monkeypatch):
    class FakeManifest:
        def list_runs(self):
            # Newest first per group; only a group's latest run matters.
            return [
                {"run_id": "core-new", "job_name": "daily:core", "status": "success"},
                {"run_id": "core-old", "job_name": "daily:core", "status": "failed"},
                {"run_id": "research-new", "job_name": "daily:research", "status": "failed"},
                {"run_id": "capital-new", "job_name": "daily:capital", "status": "success"},
                {"run_id": "capital-old", "job_name": "daily:capital", "status": "failed"},
            ]

        def get_run(self, run_id):
            return {"run_id": run_id, "job_name": "daily:research"}

    class FakeEngine:
        def __init__(self, cfg):
            self.manifest = FakeManifest()
            self.calls = []

        def run_job(self, job_name, **kwargs):
            assert job_name == "retry"
            assert kwargs.get("retry_failed_only") is True
            self.calls.append(kwargs["run_id"])
            return {"run_id": kwargs["run_id"], "status": "success"}

    monkeypatch.setattr("cnequity.cli.main.JobEngine", lambda cfg: FakeEngine(cfg))
    result = CliRunner().invoke(cli, ["retry", "--config", cfg_path, "--failed-groups"])
    assert result.exit_code == 0, result.output
    # core's latest succeeded, capital's latest succeeded, only research failed.
    assert result.output.count("retrying failed daily group run") == 1
    assert "research-new" in result.output


def test_retry_failed_groups_none(cfg_path, monkeypatch):
    class FakeManifest:
        def list_runs(self):
            return [{"run_id": "core-new", "job_name": "daily:core", "status": "success"}]

    class FakeEngine:
        def __init__(self, cfg):
            self.manifest = FakeManifest()

    monkeypatch.setattr("cnequity.cli.main.JobEngine", lambda cfg: FakeEngine(cfg))
    result = CliRunner().invoke(cli, ["retry", "--config", cfg_path, "--failed-groups"])
    assert result.exit_code == 0, result.output
    assert "no failed daily group run to retry" in result.output


def test_derive_adj_factors(cfg_path, monkeypatch):
    monkeypatch.setattr(
        "cnequity.cli.main.compute_adj_factors",
        lambda cfg, full=False: AdjFactorsResult(rows=12, task_count=12, failed=[], findings=[]),
    )
    result = CliRunner().invoke(cli, ["derive", "adj_factors", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "Derived adj_factors: 12 rows" in result.output


def test_derive_industry_index(cfg_path, monkeypatch):
    monkeypatch.setattr(
        "cnequity.derive.industry_index.derive_industry_index",
        lambda cfg, full=False, start=None, end=None: {"rows": 3, "note": "ok"},
    )
    result = CliRunner().invoke(cli, ["derive", "industry_index", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert '"rows": 3' in result.output


def test_derive_unknown_target(cfg_path):
    result = CliRunner().invoke(cli, ["derive", "not_a_thing", "--config", cfg_path])
    assert result.exit_code != 0
    assert "Unknown derive target" in result.output


def test_clean_dry_run(cfg_path, monkeypatch):
    monkeypatch.setattr(
        "cnequity.cli.main.clean_staging",
        lambda cfg, **kwargs: StagingCleanupResult(
            removed_run_ids=["r1"],
            orphan_run_ids=[],
            bytes_freed=100,
            skipped_run_ids=[],
        ),
    )
    monkeypatch.setattr(
        "cnequity.cli.main.clean_source_snapshots",
        lambda meta_root, **kwargs: SnapshotCleanupResult(
            removed_run_dirs=[], kept_run_dirs=["s1"], bytes_freed=20
        ),
    )
    result = CliRunner().invoke(cli, ["clean", "--dry-run", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["dry_run"] is True
    assert payload["removed_run_ids"] == ["r1"]
    assert payload["bytes_freed"] == 120


def test_catalog_lists_datasets(tmp_path, cfg_path):
    from cnequity.config import load_config

    cfg = load_config(cfg_path)
    part = cfg.curated_root / "daily_bars" / "trade_date=2024-06-28"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 28)],
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [1],
            "amount": [1.0],
            "source": ["mock"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-28T00:00:00+00:00"],
        }
    ).write_parquet(part / "part-000.parquet")

    result = CliRunner().invoke(cli, ["catalog", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    entries = json.loads(result.output)
    assert any(e["dataset"] == "daily_bars" and e["rows"] == 1 for e in entries)


def test_query_on_demand(cfg_path, monkeypatch):
    class FakeSvc:
        def __init__(self, cfg):
            pass

        def fetch(self, dataset, symbol):
            return {"dataset": dataset, "symbol": symbol, "rows": 0}

    monkeypatch.setattr("cnequity.cli.main.OnDemandService", FakeSvc)
    result = CliRunner().invoke(
        cli,
        ["query", "--config", cfg_path, "--dataset", "daily_bars", "--symbol", "600519.SH"],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["symbol"] == "600519.SH"


def test_query_sql(cfg_path, monkeypatch, tmp_path):
    db = tmp_path / "lake.duckdb"
    db.touch()

    class FakeCon:
        def execute(self, sql):
            return SimpleNamespace(pl=lambda: pl.DataFrame({"n": [1]}))

        def close(self):
            pass

    monkeypatch.setattr("cnequity.cli.main.ensure_duckdb_views", lambda cfg: db)
    monkeypatch.setattr("duckdb.connect", lambda *a, **k: FakeCon())
    result = CliRunner().invoke(cli, ["query", "--config", cfg_path, "--sql", "SELECT 1 AS n"])
    assert result.exit_code == 0, result.output
    assert "1" in result.output


def test_audit_with_run_id(cfg_path, monkeypatch):
    monkeypatch.setattr("cnequity.cli.main.run_audit", lambda cfg, rid, d: 2)
    result = CliRunner().invoke(cli, ["audit", "--config", cfg_path, "--run-id", "r1"])
    assert result.exit_code == 0, result.output
    assert "2 findings" in result.output


def test_servers_connection_failure(cfg_path, monkeypatch):
    """`cne servers test` now delegates to the tdx probe, which asserts that
    real bars came back rather than that a socket opened. Still exits 1, and
    the reason it reports is the vendor's, not "connection failed"."""
    import cnequity.adapters.tdx_protocol.client as tdx_client

    monkeypatch.setattr(
        tdx_client,
        "_quotes_client",
        lambda cfg: (_ for _ in ()).throw(RuntimeError("unreachable")),
    )
    result = CliRunner().invoke(cli, ["servers", "test", "--config", cfg_path])
    assert result.exit_code == 1
    assert "unreachable" in result.output
    assert "cne sources --only tdx_protocol" in result.output


def test_compact_uses_latest_run(cfg_path, monkeypatch):
    class FakeManifest:
        def __init__(self, *a, **k):
            pass

        def latest_run(self):
            return {"run_id": "latest-1"}

    class FakeEngine:
        def __init__(self, cfg):
            pass

        def run_step(self, name, trade_date, run_id):
            assert name == "compact"
            assert run_id == "latest-1"
            return {"rows_written": 9}

    monkeypatch.setattr("cnequity.cli.main.Manifest", FakeManifest)
    monkeypatch.setattr("cnequity.cli.main.JobEngine", FakeEngine)
    result = CliRunner().invoke(cli, ["compact", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "latest-1" in result.output


def test_compact_no_runs(cfg_path, monkeypatch):
    class FakeManifest:
        def __init__(self, *a, **k):
            pass

        def latest_run(self):
            return None

    monkeypatch.setattr("cnequity.cli.main.Manifest", FakeManifest)
    result = CliRunner().invoke(cli, ["compact", "--config", cfg_path])
    assert result.exit_code != 0
    assert "No runs found" in result.output


def test_repartition_lists_candidates(cfg_path, monkeypatch):
    monkeypatch.setattr(
        "cnequity.storage.repartition.repartition_candidates",
        lambda cfg: ["index_bars"],
    )
    result = CliRunner().invoke(cli, ["repartition", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "index_bars" in result.output


def test_repartition_dataset_dry_run(cfg_path, monkeypatch):
    from cnequity.storage.repartition import RepartitionResult

    monkeypatch.setattr(
        "cnequity.storage.repartition.repartition_candidates",
        lambda cfg: ["index_bars"],
    )
    monkeypatch.setattr(
        "cnequity.storage.repartition.repartition_dataset",
        lambda cfg, name, dry_run=False: RepartitionResult(
            dataset=name,
            changed=True,
            rows=10,
            files_before=5,
            files_after=1,
            partitions_before=5,
            partitions_after=1,
            bytes_before=1_000_000,
            bytes_after=200_000,
        ),
    )
    result = CliRunner().invoke(
        cli, ["repartition", "index_bars", "--dry-run", "--config", cfg_path]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["dry_run"] is True
    assert payload["results"][0]["dataset"] == "index_bars"


def test_audit_full_healthy(cfg_path, monkeypatch):
    monkeypatch.setattr(
        "cnequity.quality.audit.lake_health",
        lambda cfg, d, **kwargs: {
            "last_trading_day": "2024-06-28",
            "findings_by_severity": {"error": 0, "warning": 0, "info": 1},
            "empty_datasets": [],
            "stale_datasets": [],
            "error_findings": [],
            "warning_findings": [],
            "historical_universe_validity": {
                "window": {"start": "2020-01-01", "end": "2024-06-28"},
                "universe_ready": True,
                "blockers": [],
            },
            "healthy": True,
        },
    )
    result = CliRunner().invoke(cli, ["audit", "--full", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "HEALTHY" in result.output


def test_audit_full_unhealthy(cfg_path, monkeypatch):
    monkeypatch.setattr(
        "cnequity.quality.audit.lake_health",
        lambda cfg, d, **kwargs: {
            "last_trading_day": "2024-06-28",
            "findings_by_severity": {"error": 1, "warning": 0, "info": 0},
            "empty_datasets": ["news_headlines"],
            "stale_datasets": ["fund_flow"],
            "error_findings": [{"dataset": "daily_bars", "message": "pk dup"}],
            "warning_findings": [],
            "historical_universe_validity": {
                "window": {"start": "2020-01-01", "end": "2024-06-28"},
                "universe_ready": False,
                "blockers": [{"message": "ST history starts too late"}],
            },
            "healthy": False,
        },
    )
    result = CliRunner().invoke(cli, ["audit", "--full", "--config", cfg_path])
    assert result.exit_code == 1
    assert "UNHEALTHY" in result.output


def test_audit_full_research_window_is_a_strict_independent_gate(cfg_path, monkeypatch):
    monkeypatch.setattr(
        "cnequity.quality.audit.lake_health",
        lambda cfg, d, **kwargs: {
            "last_trading_day": "2024-06-28",
            "findings_by_severity": {},
            "empty_datasets": [],
            "stale_datasets": [],
            "error_findings": [],
            "warning_findings": [],
            "historical_universe_validity": {
                "window": {"start": "2020-01-01", "end": "2024-06-28"},
                "universe_ready": False,
                "blockers": [{"message": "delisted coverage is unverified"}],
            },
            "healthy": True,
        },
    )

    result = CliRunner().invoke(
        cli,
        ["audit", "--full", "--config", cfg_path, "--research-start", "2020-01-01"],
    )

    assert result.exit_code == 1
    assert "historical all-A" in result.output
    assert "BLOCKED" in result.output
    assert "HEALTHY" in result.output


def test_derive_trading_status_and_orphans(cfg_path, monkeypatch):
    monkeypatch.setattr(
        "cnequity.derive.trading_status_history.derive_suspension_history",
        lambda cfg, start=None, end=None: 7,
    )
    result = CliRunner().invoke(
        cli,
        [
            "derive",
            "trading_status",
            "--config",
            cfg_path,
            "--start",
            "2020-01-01",
            "--end",
            "2020-01-31",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "7 rows" in result.output

    monkeypatch.setattr(
        "cnequity.storage.valuation_orphans.purge_valuation_orphan_symbols",
        lambda cfg: {"purged_symbols": 2},
    )
    result = CliRunner().invoke(cli, ["derive", "valuation_orphans", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "purged_symbols" in result.output


def test_delisted_discover_and_status(cfg_path, monkeypatch):
    monkeypatch.setattr(
        "cnequity.steps.delisted.discover_delisted",
        lambda cfg, limit=None: SimpleNamespace(
            probed=10,
            delisted=2,
            never_issued=5,
            failed=["x"],
            remaining=100,
            complete=False,
        ),
    )
    result = CliRunner().invoke(
        cli, ["delisted", "discover", "--config", cfg_path, "--limit", "10"]
    )
    assert result.exit_code == 0, result.output
    assert '"probed": 10' in result.output

    monkeypatch.setattr(
        "cnequity.steps.delisted.classify_catalog",
        lambda cfg: ({"600001.SH": date(2020, 1, 2)}, set()),
    )
    monkeypatch.setattr("cnequity.steps.delisted.pending_codes", lambda cfg: ["000002"])
    monkeypatch.setattr(
        "cnequity.steps.delisted.delisted_symbols_in_window",
        lambda cfg, start: ["600001.SH"],
    )
    result = CliRunner().invoke(cli, ["delisted", "status", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["delisted"] == 1
    assert payload["pending_probe"] == 1


def test_delisted_coverage_uses_exit_status_as_a_strict_gate(cfg_path, monkeypatch):
    report = {
        "window": {"start": "2020-01-01", "end": "2024-12-31"},
        "verified": True,
    }
    monkeypatch.setattr(
        "cnequity.steps.delisted.delisted_coverage_report",
        lambda cfg, start, end, sample=15: report,
    )

    result = CliRunner().invoke(
        cli,
        [
            "delisted",
            "coverage",
            "--config",
            cfg_path,
            "--start",
            "2020-01-01",
            "--end",
            "2024-12-31",
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["verified"] is True

    report["verified"] = False
    result = CliRunner().invoke(
        cli, ["delisted", "coverage", "--config", cfg_path, "--start", "2020-01-01"]
    )
    assert result.exit_code == 1
    assert json.loads(result.output)["verified"] is False


def test_delisted_reconcile_is_dry_run_unless_apply_is_explicit(cfg_path, monkeypatch):
    seen = []
    report = {"read_only": True, "counts": {"safe_correction": 1}}
    monkeypatch.setattr(
        "cnequity.steps.delisted.delisted_catalog_reconciliation_report",
        lambda cfg, sample=15: seen.append("dry-run") or report,
    )
    monkeypatch.setattr(
        "cnequity.steps.delisted.reconcile_delisted_catalog",
        lambda cfg, sample=15: seen.append("apply") or {**report, "read_only": False},
    )

    result = CliRunner().invoke(cli, ["delisted", "reconcile", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert seen == ["dry-run"]

    result = CliRunner().invoke(cli, ["delisted", "reconcile", "--config", cfg_path, "--apply"])
    assert result.exit_code == 0, result.output
    assert seen == ["dry-run", "apply"]


def test_delisted_repair(cfg_path, monkeypatch):
    class FakeManifest:
        def start_run(self, *a, **k):
            return "repair-1"

        def finish_run(self, *a, **k):
            return None

    class FakeEngine:
        def __init__(self, cfg):
            self.manifest = FakeManifest()

        def run_step(self, name, trade_date, run_id):
            return {"rows_written": 1}

    monkeypatch.setattr("cnequity.cli.main.JobEngine", FakeEngine)
    monkeypatch.setattr(
        "cnequity.steps.delisted.repair_delisted_instruments",
        lambda cfg, run_id, start=None: {"rows_written": 3, "updated": 3},
    )
    monkeypatch.setattr(
        "cnequity.steps.delisted.purge_subscription_placeholders",
        lambda cfg: 1,
    )
    monkeypatch.setattr("cnequity.cli.main.ensure_duckdb_views", lambda cfg: Path("/tmp/x"))
    result = CliRunner().invoke(cli, ["delisted", "repair", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "repair-1" in result.output


def test_delisted_repair_surfaces_incomplete_bars(cfg_path, monkeypatch):
    class FakeManifest:
        def __init__(self):
            self.finished = None

        def start_run(self, *a, **k):
            return "repair-warning"

        def finish_run(self, *args, **kwargs):
            self.finished = (args, kwargs)

    manifest = FakeManifest()

    class FakeEngine:
        def __init__(self, cfg):
            self.manifest = manifest

        def run_step(self, name, trade_date, run_id):
            return {"rows_written": 1, "status": "success"}

    monkeypatch.setattr("cnequity.cli.main.JobEngine", FakeEngine)
    monkeypatch.setattr(
        "cnequity.steps.delisted.repair_delisted_instruments",
        lambda cfg, run_id, start=None: {
            "rows_written": 1,
            "still_need_bars": ["600071.SH"],
            "status": "warning",
        },
    )
    monkeypatch.setattr("cnequity.steps.delisted.purge_subscription_placeholders", lambda cfg: 0)
    monkeypatch.setattr("cnequity.cli.main.ensure_duckdb_views", lambda cfg: Path("/tmp/x"))

    result = CliRunner().invoke(cli, ["delisted", "repair", "--config", cfg_path])

    assert result.exit_code != 0, result.output
    assert '"status": "warning"' in result.output
    assert manifest.finished[0][1] == "warning"


def test_delisted_backfill(cfg_path, monkeypatch):
    class FakeManifest:
        finish_kwargs = None

        def start_run(self, *a, **k):
            return "bf-1"

        def finish_run(self, *a, **k):
            self.finish_kwargs = k

    class FakeEngine:
        def __init__(self, cfg):
            self.manifest = FakeManifest()

        def run_step(self, name, trade_date, run_id):
            return {"rows_written": 4}

    monkeypatch.setattr("cnequity.cli.main.JobEngine", FakeEngine)
    monkeypatch.setattr(
        "cnequity.steps.delisted.backfill_delisted_bars",
        lambda cfg, run_id, since: {"rows_read": 8, "rows_written": 8, "symbols": 2},
    )
    result = CliRunner().invoke(
        cli, ["delisted", "backfill", "--config", cfg_path, "--since", "2020-01-01"]
    )
    assert result.exit_code == 0, result.output
    assert "bf-1" in result.output


def test_backfill_snapshot_dataset_rejected(cfg_path):
    result = CliRunner().invoke(cli, ["backfill", "fund_flow", "--config", cfg_path])
    assert result.exit_code != 0
    assert "snapshot" in result.output.lower() or "not supported" in result.output.lower()


def test_backfill_trading_status_uses_dedicated_history_path(cfg_path, monkeypatch):
    finished = {}

    class FakeManifest:
        def finish_run(self, run_id, status, **kwargs):
            finished["status"] = status

    class FakeEngine:
        def __init__(self, cfg):
            self.manifest = FakeManifest()

        def run_job(self, *args, **kwargs):
            return {
                "run_id": "status-bf",
                "status": "success",
                "rows_read": 1,
                "rows_written": 1,
            }

        def run_step(self, name, trade_date, run_id):
            assert name == "compact"
            return {"rows_written": 1}

    monkeypatch.setattr("cnequity.cli.main.JobEngine", FakeEngine)
    result = CliRunner().invoke(cli, ["backfill", "trading_status", "--config", cfg_path])

    assert result.exit_code == 0, result.output
    assert finished["status"] == "success"
    assert "status-bf" in result.output


def test_backfill_sector_bars_force_and_retry_mutex(cfg_path):
    result = CliRunner().invoke(
        cli,
        ["backfill", "sector_bars", "--config", cfg_path, "--retry-failed", "--force"],
    )
    assert result.exit_code != 0
    assert "either" in result.output.lower() or "not both" in result.output.lower()


def test_backfill_success_compacts(cfg_path, monkeypatch):
    finished = {}

    class FakeManifest:
        def finish_run(self, run_id, status, **k):
            finished["run_id"] = run_id
            finished["status"] = status

    class FakeEngine:
        def __init__(self, cfg):
            self.cfg = cfg
            self.manifest = FakeManifest()

        def run_job(self, *a, **k):
            assert getattr(self.cfg, "_backfill_start", None) == date(2024, 1, 1)
            assert getattr(self.cfg, "_backfill_workers", None) == 2
            return {
                "run_id": "bf-ok",
                "status": "success",
                "rows_read": 1,
                "rows_written": 1,
            }

        def run_step(self, name, trade_date, run_id):
            assert name == "compact"
            return {"rows_written": 1}

    monkeypatch.setattr("cnequity.cli.main.JobEngine", FakeEngine)
    result = CliRunner().invoke(
        cli,
        [
            "backfill",
            "margin_trading",
            "--config",
            cfg_path,
            "--start",
            "2024-01-01",
            "--workers",
            "2",
        ],
    )
    assert result.exit_code == 0, result.output
    assert finished["status"] == "success"
    assert "bf-ok" in result.output


def test_catchup_already_fresh_core_only(cfg_path, monkeypatch):
    monkeypatch.setattr("cnequity.steps.common.is_trading_day", lambda cfg, d: True)
    monkeypatch.setattr(
        "cnequity.cli.main._dataset_watermark",
        lambda cfg, name: date(2024, 6, 28),
    )
    monkeypatch.setattr(
        "cnequity.cli.main._gate_fresh_for_catchup",
        lambda cfg, td, core_only=False: {
            "daily_bars": True,
            "adj_factors": True,
            "market_breadth": True,
            "core": True,
            "all": True,
        },
    )

    class FakeEngine:
        def __init__(self, cfg):
            self.cfg = cfg

        def run_job(self, *a, **k):
            raise AssertionError("should skip when already fresh")

    monkeypatch.setattr("cnequity.cli.main.JobEngine", FakeEngine)
    from cnequity.cli import main as cli_main
    from cnequity.config import ScheduleGroup

    real_cfg = cli_main._cfg

    def _cfg_with_core(path):
        c = real_cfg(path)
        c.schedule_groups["core"] = ScheduleGroup(at="09:00", steps=["daily_bars"])
        return c

    monkeypatch.setattr(cli_main, "_cfg", _cfg_with_core)
    result = CliRunner().invoke(
        cli,
        [
            "run",
            "catchup",
            "--config",
            cfg_path,
            "--trade-date",
            "2024-06-28",
            "--core-only",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "skipped_already_fresh" in result.output
