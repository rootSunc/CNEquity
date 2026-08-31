"""Offline CLI smoke tests — thin wrappers around engine / derive / cleanup."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from types import SimpleNamespace

import polars as pl
import pytest
from click.testing import CliRunner

from cnequity.cli.backfill_cmds import _recover_compactable_backfill_staging
from cnequity.cli.main import cli
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


def test_backfill_reconciles_orphaned_running_staging_before_recovery(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("backfill")
    manifest.start_batch(
        run_id,
        "st-running",
        task_id="trading_status",
        dataset="trading_status",
        blocks_compaction=False,
    )
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
    # Simulate a process that disappeared before it could finish the run.
    old = "2000-01-01T00:00:00+00:00"
    with manifest._connect() as conn:
        conn.execute("UPDATE ingestion_runs SET started_at = ? WHERE run_id = ?", (old, run_id))
        conn.execute(
            "UPDATE ingestion_batches SET started_at = ?, heartbeat_at = ? WHERE run_id = ?",
            (old, old, run_id),
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
    assert engine.calls[0][2] == run_id
    assert manifest.get_run(run_id)["status"] == "failed"


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

    monkeypatch.setattr("cnequity.cli.quality_cmds.Manifest", FakeManifest)
    result = CliRunner().invoke(cli, ["status", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["run_id"] == "r1"
    assert payload["status"] == "success"


def test_status_run_degraded_exits_two_and_lists_dataset_stages(tmp_path, cfg_path):
    cfg = Config(data_root=tmp_path / "data")
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily")
    manifest.record_dataset_result(
        run_id, "daily_bars", "publish_revision", "success", criticality="core"
    )
    manifest.record_dataset_result(
        run_id,
        "adj_factors",
        "derive",
        "failed",
        criticality="research",
        error_code="source_down",
    )
    manifest.finish_run(run_id, "degraded")

    result = CliRunner().invoke(cli, ["status", "--run", "latest", "--config", cfg_path])

    assert result.exit_code == 2, result.output
    payload = json.loads(result.output)
    assert payload["dataset_status"] == "degraded"
    assert any(
        item["dataset"] == "adj_factors"
        and item["stage"] == "derive"
        and item["status"] == "failed"
        for item in payload["dataset_results"]
    )


def test_status_run_core_failure_exits_one(tmp_path, cfg_path):
    cfg = Config(data_root=tmp_path / "data")
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily")
    manifest.record_dataset_result(
        run_id,
        "daily_bars",
        "compact",
        "failed",
        criticality="core",
        error_code="compact_failed",
    )
    manifest.finish_run(run_id, "failed")

    result = CliRunner().invoke(cli, ["status", "--run", run_id, "--config", cfg_path])

    assert result.exit_code == 1, result.output
    payload = json.loads(result.output)
    assert payload["dataset_status"] == "failed"


def test_status_datasets_all_fresh(cfg_path, monkeypatch):
    monkeypatch.setattr(
        "cnequity.cli.quality_cmds._last_trading_day",
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
        "cnequity.cli.quality_cmds._last_trading_day",
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

    monkeypatch.setattr("cnequity.cli.run_cmds.JobEngine", FakeEngine)
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

    monkeypatch.setattr("cnequity.cli.run_cmds.JobEngine", FakeEngine)
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

    monkeypatch.setattr("cnequity.cli.run_cmds.JobEngine", FakeEngine)
    result = CliRunner().invoke(cli, ["retry", "--config", cfg_path, "--run-id", "d1"])
    assert result.exit_code == 1


def test_retry_failed_groups_retries_only_latest_failed_per_group(cfg_path, monkeypatch):
    class FakeManifest:
        def list_runs(self):
            return [
                {"run_id": "core-new", "job_name": "daily:core", "status": "success"},
                {"run_id": "core-old", "job_name": "daily:core", "status": "failed"},
                {
                    "run_id": "research-new",
                    "job_name": "daily:research",
                    "status": "failed",
                },
                {"run_id": "capital-new", "job_name": "daily:capital", "status": "success"},
                {"run_id": "capital-old", "job_name": "daily:capital", "status": "failed"},
            ]

    class FakeEngine:
        def __init__(self, cfg):
            self.manifest = FakeManifest()

    class Proc:
        returncode = 0

    calls: list[list[str]] = []
    monkeypatch.setattr("cnequity.cli.run_cmds.JobEngine", FakeEngine)
    monkeypatch.setattr(
        "cnequity.cli.run_cmds.subprocess.run",
        lambda argv, **kwargs: calls.append(argv) or Proc(),
    )

    result = CliRunner().invoke(cli, ["retry", "--config", cfg_path, "--failed-groups"])

    assert result.exit_code == 0, result.output
    assert result.output.count("Retrying failed daily group run") == 1
    assert "research-new" in result.output
    assert len(calls) == 1
    assert calls[0][-1] == "research-new"


def test_retry_failed_groups_reports_child_failure(cfg_path, monkeypatch):
    class FakeManifest:
        def list_runs(self):
            return [{"run_id": "core-new", "job_name": "daily:core", "status": "failed"}]

    class FakeEngine:
        def __init__(self, cfg):
            self.manifest = FakeManifest()

    class Proc:
        returncode = 1

    monkeypatch.setattr("cnequity.cli.run_cmds.JobEngine", FakeEngine)
    monkeypatch.setattr("cnequity.cli.run_cmds.subprocess.run", lambda *args, **kwargs: Proc())

    result = CliRunner().invoke(cli, ["retry", "--config", cfg_path, "--failed-groups"])

    assert result.exit_code == 1


def test_retry_requires_exactly_one_scope(cfg_path):
    result = CliRunner().invoke(cli, ["retry", "--config", cfg_path])
    assert result.exit_code != 0
    assert "provide --run-id or --failed-groups" in result.output


def test_derive_adj_factors(cfg_path, monkeypatch):
    monkeypatch.setattr(
        "cnequity.cli.maintain_cmds.compute_adj_factors",
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
        "cnequity.cli.maintain_cmds.clean_staging",
        lambda cfg, **kwargs: StagingCleanupResult(
            removed_run_ids=["r1"],
            orphan_run_ids=[],
            bytes_freed=100,
            skipped_run_ids=[],
        ),
    )
    monkeypatch.setattr(
        "cnequity.cli.maintain_cmds.clean_source_snapshots",
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


def test_stats_show_scans_curated_when_no_stats_exist(tmp_path, cfg_path):
    """The former `cne catalog`. A lake that has never run `stats rebuild` must
    still be able to answer what is in it, without a build step first."""
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

    result = CliRunner().invoke(cli, ["stats", "show", "--json", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    entries = json.loads(result.output)
    assert any(e["dataset"] == "daily_bars" and e["rows"] == 1 for e in entries)


def test_stats_show_fallback_refuses_the_views_it_cannot_serve(cfg_path):
    """The scan has no per-partition or per-source detail. Silently returning the
    dataset roll-up for `--by-source` would answer a different question."""
    result = CliRunner().invoke(cli, ["stats", "show", "--by-source", "--config", cfg_path])

    assert result.exit_code != 0
    assert "cne stats rebuild" in result.output


def test_query_on_demand(cfg_path, monkeypatch):
    class FakeSvc:
        def __init__(self, cfg):
            pass

        def fetch(self, dataset, symbol, **kwargs):
            return {"dataset": dataset, "symbol": symbol, "rows": 0, **kwargs}

    monkeypatch.setattr("cnequity.cli.consume_cmds.OnDemandService", FakeSvc)
    result = CliRunner().invoke(
        cli,
        [
            "query",
            "--config",
            cfg_path,
            "--dataset",
            "daily_bars",
            "--symbol",
            "600519.SH",
            "--refresh",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["symbol"] == "600519.SH"
    assert payload["refresh"] is True


@pytest.mark.parametrize(
    "args",
    [
        ["--dataset", "stock_news"],
        ["--symbol", "600519.SH"],
        ["--refresh"],
    ],
)
def test_query_rejects_partial_on_demand_options(cfg_path, args):
    result = CliRunner().invoke(cli, ["query", "--config", cfg_path, *args])

    assert result.exit_code != 0
    assert "requires" in result.output or "together" in result.output


def test_query_sql(cfg_path, monkeypatch, tmp_path):
    db = tmp_path / "lake.duckdb"
    db.touch()

    class FakeCon:
        def execute(self, sql):
            return SimpleNamespace(pl=lambda: pl.DataFrame({"n": [1]}))

        def close(self):
            pass

    monkeypatch.setattr("cnequity.cli.consume_cmds.ensure_duckdb_views", lambda cfg: db)
    monkeypatch.setattr("duckdb.connect", lambda *a, **k: FakeCon())
    result = CliRunner().invoke(cli, ["query", "--config", cfg_path, "--sql", "SELECT 1 AS n"])
    assert result.exit_code == 0, result.output
    assert "1" in result.output


def test_audit_with_run_id(cfg_path, monkeypatch):
    monkeypatch.setattr("cnequity.cli.quality_cmds.run_audit", lambda cfg, rid, d: 2)
    result = CliRunner().invoke(cli, ["audit", "--config", cfg_path, "--run-id", "r1"])
    assert result.exit_code == 0, result.output
    assert "2 findings" in result.output


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

    monkeypatch.setattr("cnequity.cli.maintain_cmds.Manifest", FakeManifest)
    monkeypatch.setattr("cnequity.cli.maintain_cmds.JobEngine", FakeEngine)
    result = CliRunner().invoke(cli, ["compact", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "latest-1" in result.output


def test_compact_no_runs(cfg_path, monkeypatch):
    class FakeManifest:
        def __init__(self, *a, **k):
            pass

        def latest_run(self):
            return None

    monkeypatch.setattr("cnequity.cli.maintain_cmds.Manifest", FakeManifest)
    result = CliRunner().invoke(cli, ["compact", "--config", cfg_path])
    assert result.exit_code != 0
    assert "No runs found" in result.output


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
                "blockers": [
                    {
                        "message": "delisted coverage is unverified",
                        "remediation": "run cne delisted coverage",
                    }
                ],
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
    assert "remediation: run cne delisted coverage" in result.output
    assert "HEALTHY (operational; research BLOCKED)" in result.output


def test_audit_full_can_select_scoped_research_universe(cfg_path, monkeypatch):
    observed: dict[str, object] = {}

    def _health(_cfg, _trade_date, **kwargs):
        observed.update(kwargs)
        return {
            "last_trading_day": "2024-06-28",
            "findings_by_severity": {"error": 0, "warning": 0, "info": 0},
            "empty_datasets": [],
            "stale_datasets": [],
            "error_findings": [],
            "warning_findings": [],
            "historical_universe_validity": {
                "universe": "all_a_sh_sz",
                "window": {"start": "2020-01-01", "end": "2024-06-28"},
                "universe_ready": True,
                "blockers": [],
            },
            "healthy": True,
        }

    monkeypatch.setattr("cnequity.quality.audit.lake_health", _health)
    result = CliRunner().invoke(
        cli,
        [
            "audit",
            "--full",
            "--config",
            cfg_path,
            "--research-universe",
            "all_a_sh_sz",
        ],
    )

    assert result.exit_code == 0, result.output
    assert observed["research_universe"] == "all_a_sh_sz"
    assert "historical all_a_sh_sz" in result.output


def test_derive_trading_status_and_orphans(cfg_path, monkeypatch):
    seen: dict = {}

    def fake_derive(cfg, run_id, *, start=None, end=None, batch_id="derive-0"):
        seen.update(run_id=run_id, start=start, end=end)
        return 7

    monkeypatch.setattr(
        "cnequity.derive.trading_status_history.derive_suspension_history", fake_derive
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
    # The window is honoured, and the rows are staged against a real run so
    # the compact that follows can publish them.
    assert seen["start"] == date(2020, 1, 1)
    assert seen["end"] == date(2020, 1, 31)
    assert seen["run_id"]
    summary = json.loads(result.output)
    assert summary["rows_staged"] == 7
    assert summary["run_id"] == seen["run_id"]
    assert "compact" in summary

    monkeypatch.setattr(
        "cnequity.storage.valuation_orphans.purge_valuation_orphan_symbols",
        lambda cfg: {"purged_symbols": 2},
    )
    result = CliRunner().invoke(cli, ["derive", "valuation_orphans", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "purged_symbols" in result.output


def test_delisted_status_summarises_the_catalogue(cfg_path, monkeypatch):
    # The sweep that fills this catalogue is `scripts/delisted_ops.py discover`;
    # reading it stayed here because reading is the routine half.
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

    monkeypatch.setattr("cnequity.cli.delisted_cmds.JobEngine", FakeEngine)
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

    monkeypatch.setattr("cnequity.cli.backfill_cmds.JobEngine", FakeEngine)
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

    monkeypatch.setattr("cnequity.cli.backfill_cmds.JobEngine", FakeEngine)
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


def test_backfill_workers_rejects_unsupported_date_walk(cfg_path):
    result = CliRunner().invoke(
        cli,
        ["backfill", "daily_bars", "--config", cfg_path, "--workers", "2"],
    )

    assert result.exit_code != 0
    assert "supported only for margin_trading" in result.output


def test_backfill_workers_help_describes_shared_limiter(cfg_path):
    result = CliRunner().invoke(cli, ["backfill", "--help"])

    assert result.exit_code == 0
    assert "shared source" in result.output
    assert "bypassing" not in result.output


def test_run_group_help_lists_ticks(cfg_path):
    result = CliRunner().invoke(cli, ["run", "daily", "--help"])

    assert result.exit_code == 0
    assert "ticks" in result.output


def test_run_rejects_invalid_source_concurrency_as_click_error(tmp_path):
    cfg_path = tmp_path / "invalid-source.toml"
    cfg_path.write_text(
        f'''
[data]
root = "{path_for_toml(tmp_path / "data")}"

[orchestrator]
workers = 1
source_concurrency = {{ sina = 0 }}

[tdx_protocol]
allow_mock = true

[[job.daily.waves]]
name = "w"
parallel = false
steps = ["compact"]
''',
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        cli,
        ["run", "daily", "--config", str(cfg_path), "--trade-date", "2026-08-28"],
    )

    assert result.exit_code == 1
    assert "source concurrency" in result.output
    assert "Traceback" not in result.output
