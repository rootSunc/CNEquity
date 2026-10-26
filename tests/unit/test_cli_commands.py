"""Offline CLI smoke tests — thin wrappers around engine / derive / cleanup."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
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


@pytest.mark.usefixtures("chinese_output")
def test_config_validate_ok(cfg_path):
    result = CliRunner().invoke(cli, ["config", "validate", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "配置检查通过" in result.output


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
    manifest.finish_batch(run_id, batch_id, "success")
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


def test_backfill_does_not_repeat_compact_for_blocked_staging(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("backfill")
    manifest.start_batch(
        run_id,
        "blocking-failure",
        task_id="trading_status",
        dataset="trading_status",
        blocks_compaction=True,
    )
    manifest.finish_batch(run_id, "blocking-failure", "failed", error_message="source conflict")
    manifest.finish_run(run_id, "failed", error_message="source conflict")
    StagingWriter(cfg.staging_root).write_batch(
        "trading_status",
        run_id,
        "batch-00000",
        pl.DataFrame(
            {
                "symbol": ["600025.SH"],
                "trade_date": [date(2020, 6, 19)],
                "is_trading": [True],
                "status": ["normal"],
                "source": ["baostock"],
                "data_version": ["v1"],
                "fetched_at": [datetime.now(timezone.utc)],
            }
        ),
    )

    for seal in cfg.staging_root.rglob("*.sealed.json"):
        seal.unlink()  # Unknown legacy staging remains blocked.

    class FakeEngine:
        def __init__(self):
            self.config = cfg
            self.manifest = manifest

        def run_step(self, *args):
            raise AssertionError("blocked staging must not reach compact")

    assert _recover_compactable_backfill_staging(FakeEngine(), "trading_status") == []
    assert manifest.get_run(run_id)["status"] == "failed"
    assert StagingWriter(cfg.staging_root).list_run_files("trading_status", run_id)


def test_backfill_recovers_staging_from_a_degraded_run(tmp_path):
    """`degraded` is terminal, and it is the tier a partial sweep lands in.

    The recovery filter used to name the terminal spellings it knew, so the
    runs most likely to have left rows in staging were the ones it skipped.
    """
    cfg = Config(data_root=tmp_path / "data")
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("backfill")
    batch_id = "st-degraded"
    manifest.start_batch(
        run_id,
        batch_id,
        task_id="trading_status",
        dataset="trading_status",
        blocks_compaction=False,
    )
    manifest.finish_batch(run_id, batch_id, "success")
    manifest.start_batch(run_id, "unrelated-failure", task_id="daily_bars", dataset="daily_bars")
    manifest.finish_batch(run_id, "unrelated-failure", "failed", error_message="source refused")
    manifest.finish_run(run_id, "degraded", error_message="one or more steps failed")
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


@pytest.mark.parametrize(
    ("args", "hint"),
    [
        (["init", "--since", "2026-10-09"], "Run `cne init` again to resume"),
        (["run", "daily", "--group", "core"], "run the same command again to continue"),
        (["backfill", "daily_bars", "--start", "2026-10-01"], "run the same command again"),
    ],
)
def test_ctrl_c_says_what_survived_and_how_to_continue(tmp_path, monkeypatch, args, hint):
    """Click's bare "Aborted!" told nobody that finished batches are kept."""
    cfg = _write_config(
        tmp_path, extra='\n[job.daily.groups.core]\nat = "16:00"\nsteps = ["compact"]\n'
    )

    def interrupted(*_a, **_k):
        raise KeyboardInterrupt

    monkeypatch.setattr(JobEngine, "run_job", interrupted)
    monkeypatch.setattr(JobEngine, "run_init_phases", interrupted)
    result = CliRunner().invoke(cli, [*args, "--config", cfg])
    assert result.exit_code == 1
    assert hint in result.output
    assert "Aborted!" in result.output


def test_ctrl_c_on_a_read_only_command_adds_no_resume_hint(cfg_path, monkeypatch):
    def interrupted(*_a, **_k):
        raise KeyboardInterrupt

    monkeypatch.setattr("cnequity.cli.quality_cmds.Manifest.latest_run", interrupted)
    result = CliRunner().invoke(cli, ["status", "--config", cfg_path])
    assert result.exit_code == 1
    assert "Stopped." not in result.output


@pytest.mark.usefixtures("chinese_output")
def test_run_daily_unknown_group(cfg_path):
    result = CliRunner().invoke(cli, ["run", "daily", "--config", cfg_path, "--group", "nope"])
    assert result.exit_code != 0
    assert "未知调度组" in result.output


@pytest.mark.usefixtures("chinese_output")
def test_run_daily_unknown_group_names_the_configured_ones(tmp_path):
    """`Unknown group: core` alone leaves the reader guessing what is valid."""
    cfg = _write_config(
        tmp_path,
        extra='\n[job.daily.groups.capital]\nat = "17:00"\nsteps = ["compact"]\n',
    )
    result = CliRunner().invoke(cli, ["run", "daily", "--config", cfg, "--group", "core"])
    assert result.exit_code != 0
    assert "配置里有：capital" in result.output


@pytest.mark.usefixtures("chinese_output")
def test_run_daily_refuses_a_config_with_no_waves(tmp_path):
    """An empty plan is not a successful day.

    A config without `[[job.daily.waves]]` — the one `cne init --profile demo`
    writes — reported `planned_steps: []`, `status: success` and exit 0 while
    fetching nothing, which is exactly what a scheduler cannot act on.
    """
    cfg = tmp_path / "no-waves.toml"
    cfg.write_text(
        f'[data]\nroot = "{path_for_toml(tmp_path / "data")}"\n\n[orchestrator]\nworkers = 1\n',
        encoding="utf-8",
    )
    result = CliRunner().invoke(cli, ["run", "daily", "--config", str(cfg)])
    assert result.exit_code != 0
    assert "没有 [[job.daily.waves]]" in result.output
    assert "cne config create" in result.output


def _demo_config(tmp_path):
    cfg = tmp_path / "demo.toml"
    cfg.write_text(
        f'[data]\nroot = "{path_for_toml(tmp_path / "data")}"\nprofile = "demo"\n\n'
        "[orchestrator]\nworkers = 1\n",
        encoding="utf-8",
    )
    return str(cfg)


@pytest.mark.parametrize("extra", [[], ["--stale-only"]])
def test_run_daily_on_a_demo_lake_explains_there_is_no_schedule(tmp_path, extra):
    result = CliRunner().invoke(cli, ["run", "daily", "--config", _demo_config(tmp_path), *extra])
    assert result.exit_code != 0
    assert "demo lake" in result.output
    assert "cne init" in result.output
    assert "cne config create" not in result.output


@pytest.mark.parametrize("action", ["diff", "upgrade"])
def test_config_diff_and_upgrade_leave_a_demo_config_alone(tmp_path, action):
    path = _demo_config(tmp_path)
    before = open(path, encoding="utf-8").read()
    result = CliRunner().invoke(cli, ["config", action, "--config", path])
    assert result.exit_code == 0, result.output
    assert "demo lake" in result.output
    assert open(path, encoding="utf-8").read() == before


@pytest.mark.usefixtures("chinese_output")
def test_status_no_runs(cfg_path):
    result = CliRunner().invoke(cli, ["status", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "还没有任何 run。" in result.output


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
        run_id, "daily_bars", "publish_revision", "success", criticality="core", rows_written=1
    )
    manifest.record_dataset_result(
        run_id,
        "adj_factors",
        "derive",
        "warning",
        criticality="research",
        error_code="source_down",
    )
    manifest.finish_run(run_id, "degraded")

    result = CliRunner().invoke(cli, ["status", "--gate", "--run", "latest", "--config", cfg_path])

    assert result.exit_code == 2, result.output
    payload = json.loads(result.output)
    assert payload["dataset_status"] == "degraded"
    assert any(
        item["dataset"] == "adj_factors"
        and item["stage"] == "derive"
        and item["status"] == "warning"
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

    result = CliRunner().invoke(cli, ["status", "--gate", "--run", run_id, "--config", cfg_path])

    assert result.exit_code == 1, result.output
    payload = json.loads(result.output)
    assert payload["dataset_status"] == "failed"


@pytest.mark.usefixtures("chinese_output")
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
    result = CliRunner().invoke(cli, ["status", "--gate", "--datasets", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "最后交易日：2024-06-28" in result.output


def test_status_datasets_keeps_long_dataset_names_on_one_line(cfg_path, monkeypatch):
    names = ["earnings_disclosure_schedule", "financial_statement_items"]
    monkeypatch.setattr(
        "cnequity.cli.quality_cmds._last_trading_day",
        lambda cfg, today: date(2024, 6, 28),
    )
    monkeypatch.setattr(
        "cnequity.query.reader.list_datasets",
        lambda config=None: pl.DataFrame(
            {
                "dataset": names,
                "layer": ["curated"] * 2,
                "has_data": [False, False],
                "watermarked": [False, False],
                "watermark": pl.Series([None, None], dtype=pl.Date),
                "coverage_start": pl.Series([None, None], dtype=pl.Date),
                "coverage_end": pl.Series([None, None], dtype=pl.Date),
            }
        ),
    )
    monkeypatch.setattr("cnequity.domain.datasets.is_stale", lambda *a, **k: False)
    result = CliRunner().invoke(cli, ["status", "--datasets", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    for name in names:
        assert name in result.output


@pytest.mark.usefixtures("chinese_output")
def test_status_datasets_fails_when_an_init_is_incomplete(cfg_path, monkeypatch):
    cfg = Config(data_root=Path(cfg_path).parent / "data")
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run(
        "init",
        {"phases": ["phase1_reference"], "trade_date": "2024-06-28"},
    )
    manifest.start_batch(run_id, "instruments-ok", "instruments", "instruments")
    manifest.finish_batch(run_id, "instruments-ok", "success")
    manifest.finish_run(run_id, "failed", error_message="interrupted")

    monkeypatch.setattr(
        "cnequity.cli.quality_cmds._last_trading_day",
        lambda config, today: date(2024, 6, 28),
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

    result = CliRunner().invoke(cli, ["status", "--gate", "--datasets", "--config", cfg_path])

    assert result.exit_code == 1, result.output
    assert "init 尚未完成" in result.output
    assert run_id in result.output
    assert "cne init" in result.output


@pytest.mark.usefixtures("chinese_output")
def test_status_datasets_rejects_a_fresh_but_narrow_daily_bar_tip(cfg_path, monkeypatch):
    cfg = Config(data_root=Path(cfg_path).parent / "data")
    instruments_root = cfg.curated_root / "instruments"
    bars_root = cfg.curated_root / "daily_bars" / "trade_date=2024-06-28"
    instruments_root.mkdir(parents=True, exist_ok=True)
    bars_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ", "600000.SH", "920001.BJ"],
            "list_date": [
                date(2001, 8, 27),
                date(1991, 4, 3),
                date(1999, 11, 10),
                date(2021, 11, 15),
            ],
            "delist_date": [None, None, None, None],
        }
    ).write_parquet(instruments_root / "part-0.parquet")
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 28)],
        }
    ).write_parquet(bars_root / "part-0.parquet")

    monkeypatch.setattr(
        "cnequity.cli.quality_cmds._last_trading_day",
        lambda config, today: date(2024, 6, 28),
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

    result = CliRunner().invoke(cli, ["status", "--gate", "--datasets", "--config", cfg_path])

    assert result.exit_code == 1, result.output
    assert "取数截面（沪深京全市场 A 股）：INCOMPLETE" in result.output
    assert "1/4" in result.output
    assert "日期 fresh 不代表标的覆盖完整" in result.output


def test_a_code_the_probe_found_unlisted_is_not_owed_a_bar(tmp_path):
    """001246.SZ / 301660.SZ / 301716.SZ held codes on 2026-09-24 without a
    listing; the daily_bars probe proved it, and the proof has to agree."""
    from cnequity.cli.quality_cmds import _tip_scope
    from cnequity.steps.common import record_negative_evidence

    cfg = Config(data_root=tmp_path / "data")
    tip = date(2024, 6, 28)
    symbols = ["600000.SH", "000001.SZ", "001246.SZ", "920001.BJ"]
    (cfg.curated_root / "instruments").mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": symbols,
            "list_date": [date(2000, 1, 1), date(2000, 1, 1), None, date(2000, 1, 1)],
            "delist_date": [None] * len(symbols),
        }
    ).write_parquet(cfg.curated_root / "instruments" / "part-0.parquet")
    for dataset in ("daily_bars", "trading_status"):
        part = cfg.curated_root / dataset / f"trade_date={tip.isoformat()}"
        part.mkdir(parents=True)
        frame = {"symbol": ["600000.SH", "000001.SZ", "920001.BJ"], "trade_date": [tip] * 3}
        if dataset == "trading_status":
            frame["is_trading"] = [True] * 3
        pl.DataFrame(frame).write_parquet(part / "part-0.parquet")
    record_negative_evidence(
        cfg, "daily_bars", ["001246.SZ"], tip, tip, reason="not_yet_listed", source="probe"
    )

    for dataset in ("daily_bars", "trading_status"):
        catalog = pl.DataFrame(
            {
                "dataset": [dataset],
                "has_data": [True],
                "watermarked": [True],
                "watermark": [tip],
                "coverage_end": [tip],
            }
        )
        scope = _tip_scope(cfg, catalog, dataset)
        assert scope is not None and scope["state"] == "complete", (dataset, scope)
        assert scope["covered"] == 4


@pytest.mark.parametrize(
    ("suspended_evidence", "expected_state"),
    [(False, "incomplete"), (True, "complete")],
)
def test_daily_bar_tip_requires_every_symbol_or_explicit_suspension(
    tmp_path, suspended_evidence, expected_state
):
    from cnequity.cli.quality_cmds import _tip_scope

    cfg = Config(data_root=tmp_path / "data")
    tip = date(2024, 6, 28)
    symbols = [
        "600000.SH",
        "600001.SH",
        "600002.SH",
        "600003.SH",
        "000001.SZ",
        "000002.SZ",
        "000003.SZ",
        "000004.SZ",
        "920001.BJ",
        "920002.BJ",
    ]
    instruments_root = cfg.curated_root / "instruments"
    bars_root = cfg.curated_root / "daily_bars" / f"trade_date={tip.isoformat()}"
    instruments_root.mkdir(parents=True)
    bars_root.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": symbols,
            "list_date": [date(2000, 1, 1)] * len(symbols),
            "delist_date": [None] * len(symbols),
        }
    ).write_parquet(instruments_root / "part-0.parquet")
    pl.DataFrame({"symbol": symbols[:-1], "trade_date": [tip] * (len(symbols) - 1)}).write_parquet(
        bars_root / "part-0.parquet"
    )
    if suspended_evidence:
        status_root = cfg.curated_root / "trading_status" / f"trade_date={tip.isoformat()}"
        status_root.mkdir(parents=True)
        pl.DataFrame(
            {"symbol": [symbols[-1]], "trade_date": [tip], "is_trading": [False]}
        ).write_parquet(status_root / "part-0.parquet")

    catalog = pl.DataFrame(
        {
            "dataset": ["daily_bars"],
            "has_data": [True],
            "watermarked": [True],
            "watermark": [tip],
            "coverage_end": [tip],
        }
    )

    scope = _tip_scope(cfg, catalog)

    assert scope is not None
    assert scope["state"] == expected_state
    assert scope["covered"] == (10 if suspended_evidence else 9)
    assert scope["expected"] == 10


@pytest.mark.usefixtures("chinese_output")
def test_daily_bar_tip_cannot_prove_all_a_from_an_instrument_list_without_bj(tmp_path):
    from cnequity.cli.quality_cmds import _tip_scope

    cfg = Config(data_root=tmp_path / "data")
    tip = date(2024, 6, 28)
    instruments_root = cfg.curated_root / "instruments"
    bars_root = cfg.curated_root / "daily_bars" / f"trade_date={tip.isoformat()}"
    instruments_root.mkdir(parents=True)
    bars_root.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "list_date": [date(2001, 8, 27), date(1991, 4, 3)],
            "delist_date": [None, None],
        }
    ).write_parquet(instruments_root / "part-0.parquet")
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "trade_date": [tip, tip],
        }
    ).write_parquet(bars_root / "part-0.parquet")
    catalog = pl.DataFrame(
        {
            "dataset": ["daily_bars"],
            "has_data": [True],
            "watermarked": [True],
            "watermark": [tip],
            "coverage_end": [tip],
        }
    )

    scope = _tip_scope(cfg, catalog)

    assert scope is not None
    assert scope["state"] == "unverified"
    assert "BJ" in scope["message"]
    assert "truncated security table" in scope["message"]


@pytest.mark.usefixtures("chinese_output")
def test_daily_bar_tip_rejects_an_instrument_table_smaller_than_its_success_receipt(tmp_path):
    from cnequity.cli.quality_cmds import _tip_scope

    cfg = Config(data_root=tmp_path / "data")
    tip = date(2024, 6, 28)
    symbols = ["600519.SH", "000001.SZ", "920001.BJ"]
    instruments_root = cfg.curated_root / "instruments"
    bars_root = cfg.curated_root / "daily_bars" / f"trade_date={tip.isoformat()}"
    instruments_root.mkdir(parents=True)
    bars_root.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": symbols,
            "list_date": [date(2000, 1, 1)] * 3,
            "delist_date": [None] * 3,
        }
    ).write_parquet(instruments_root / "part-0.parquet")
    pl.DataFrame({"symbol": symbols, "trade_date": [tip] * 3}).write_parquet(
        bars_root / "part-0.parquet"
    )
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily:test")
    manifest.start_batch(run_id, "instruments", "instruments", "instruments")
    manifest.finish_batch(run_id, "instruments", "success", rows_read=5000, rows_written=5000)
    catalog = pl.DataFrame(
        {
            "dataset": ["daily_bars"],
            "has_data": [True],
            "watermarked": [True],
            "watermark": [tip],
            "coverage_end": [tip],
        }
    )

    scope = _tip_scope(cfg, catalog)

    assert scope is not None
    assert scope["state"] == "unverified"
    assert "has only 3 symbols" in scope["message"]
    assert "the 5000 the latest successful batch wrote" in scope["message"]
    assert "may be truncated" in scope["message"]


@pytest.mark.usefixtures("chinese_output")
def test_status_datasets_treats_sample_dates_as_non_gating(tmp_path, monkeypatch):
    cfg_path = tmp_path / "sample.toml"
    cfg_path.write_text(
        f'[data]\nroot = "{path_for_toml(tmp_path / "sample-data")}"\nprofile = "sample"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "cnequity.cli.quality_cmds._last_trading_day",
        lambda config, today: date(2026, 9, 18),
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

    result = CliRunner().invoke(cli, ["status", "--gate", "--datasets", "--config", str(cfg_path)])

    assert result.exit_code == 0, result.output
    assert "sample" in result.output
    assert "STALE" not in result.output
    assert "合成数据" in result.output


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
    result = CliRunner().invoke(cli, ["status", "--gate", "--datasets", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "n/a" in result.output
    assert "STALE datasets" not in result.output


def _wide_inventory(config=None):
    """`list_datasets` as it really is: freshness plus governance metadata."""
    return pl.DataFrame(
        {
            "dataset": ["daily_bars"],
            "layer": ["curated"],
            "has_data": [True],
            "watermarked": [True],
            "watermark": [date(2024, 6, 28)],
            "coverage_start": [date(2024, 5, 20)],
            "coverage_end": [date(2024, 6, 28)],
            "revision_id": ["rev-000000000000"],
            "contract_fingerprint": ["b7d1e4c9a2f30518cc7e6b4419aa02d5"],
        }
    )


def test_status_datasets_shows_freshness_not_the_whole_inventory(cfg_path, monkeypatch):
    """Twenty columns squeezed into a terminal shredded every value.

    The flag is the freshness probe the runbooks reach for, so it prints what
    it promises; the governance columns have `--all-columns`.
    """
    monkeypatch.setattr(
        "cnequity.cli.quality_cmds._last_trading_day",
        lambda cfg, today: date(2024, 6, 28),
    )
    monkeypatch.setattr("cnequity.query.reader.list_datasets", _wide_inventory)
    monkeypatch.setattr("cnequity.domain.datasets.is_stale", lambda *a, **k: False)

    result = CliRunner().invoke(cli, ["status", "--gate", "--datasets", "--config", cfg_path])

    assert result.exit_code == 0, result.output
    # 9 inventory columns + freshness, narrowed to the 7 that describe it.
    assert "shape: (1, 7)" in result.output
    assert "contract_fingerprint" not in result.output
    assert "revision_id" not in result.output


def test_status_datasets_all_columns_keeps_the_full_inventory(cfg_path, monkeypatch):
    monkeypatch.setattr(
        "cnequity.cli.quality_cmds._last_trading_day",
        lambda cfg, today: date(2024, 6, 28),
    )
    monkeypatch.setattr("cnequity.query.reader.list_datasets", _wide_inventory)
    monkeypatch.setattr("cnequity.domain.datasets.is_stale", lambda *a, **k: False)

    result = CliRunner().invoke(
        cli, ["status", "--gate", "--datasets", "--all-columns", "--config", cfg_path]
    )

    assert result.exit_code == 0, result.output
    # Every column is still there; at this width polars shreds the headers,
    # which is exactly why it is no longer the default.
    assert "shape: (1, 10)" in result.output


@pytest.mark.usefixtures("chinese_output")
def test_status_all_columns_without_datasets_is_a_usage_error(cfg_path):
    result = CliRunner().invoke(cli, ["status", "--all-columns", "--config", cfg_path])

    assert result.exit_code != 0
    assert "--all-columns 只能配合 --datasets 使用" in result.output


@pytest.mark.usefixtures("chinese_output")
def test_retry_unknown_run(cfg_path, monkeypatch):
    class FakeManifest:
        def get_run(self, run_id):
            return None

    class FakeEngine:
        def __init__(self, cfg):
            self.manifest = FakeManifest()

    monkeypatch.setattr("cnequity.cli.run_cmds.JobEngine", FakeEngine)
    result = CliRunner().invoke(cli, ["run", "retry", "--config", cfg_path, "--run-id", "missing"])
    assert result.exit_code != 0
    assert "未知 run_id" in result.output


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
    result = CliRunner().invoke(cli, ["run", "retry", "--config", cfg_path, "--run-id", "init-1"])
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
    result = CliRunner().invoke(cli, ["run", "retry", "--config", cfg_path, "--run-id", "d1"])
    assert result.exit_code == 1


@pytest.mark.usefixtures("chinese_output")
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

        def _reconcile_orphans(self):
            # Selection reconciles first now: a run killed mid-flight is still
            # recorded as `running`, and this command keeps only `failed` rows.
            return {"runs_closed": 0, "batches_closed": 0, "skipped_locked": 0}

    class Proc:
        returncode = 0

    calls: list[list[str]] = []
    monkeypatch.setattr("cnequity.cli.run_cmds.JobEngine", FakeEngine)
    monkeypatch.setattr(
        "cnequity.cli.run_cmds.subprocess.run",
        lambda argv, **kwargs: calls.append(argv) or Proc(),
    )

    result = CliRunner().invoke(cli, ["run", "retry", "--config", cfg_path, "--failed-groups"])

    assert result.exit_code == 0, result.output
    assert result.output.count("重试失败的 daily 调度组 run") == 1
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

    result = CliRunner().invoke(cli, ["run", "retry", "--config", cfg_path, "--failed-groups"])

    assert result.exit_code == 1


@pytest.mark.usefixtures("chinese_output")
def test_retry_requires_exactly_one_scope(cfg_path):
    result = CliRunner().invoke(cli, ["run", "retry", "--config", cfg_path])
    assert result.exit_code != 0
    assert "请给出 --run-id 或 --failed-groups" in result.output


@pytest.mark.usefixtures("chinese_output")
def test_derive_adj_factors(cfg_path, monkeypatch):
    monkeypatch.setattr(
        "cnequity.query.parquet_scan.dataset_has_parquet", lambda _root, **kwargs: True
    )
    monkeypatch.setattr(
        "cnequity.cli.maintain_cmds.compute_adj_factors",
        lambda cfg, full=False: AdjFactorsResult(rows=12, task_count=12, failed=[], findings=[]),
    )
    result = CliRunner().invoke(cli, ["derive", "adj_factors", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "已派生 adj_factors：12 行" in result.output


def test_derive_industry_index(cfg_path, monkeypatch):
    monkeypatch.setattr(
        "cnequity.query.parquet_scan.dataset_has_parquet", lambda _root, **kwargs: True
    )
    monkeypatch.setattr(
        "cnequity.derive.industry_index.derive_industry_index",
        lambda cfg, full=False, start=None, end=None: {"rows": 3, "note": "ok"},
    )
    result = CliRunner().invoke(cli, ["derive", "industry_index", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert '"rows": 3' in result.output


@pytest.mark.usefixtures("chinese_output")
def test_derive_unknown_target(cfg_path):
    result = CliRunner().invoke(cli, ["derive", "not_a_thing", "--config", cfg_path])
    assert result.exit_code != 0
    assert "未知的 derive 目标" in result.output


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
    result = CliRunner().invoke(cli, ["run", "clean", "--dry-run", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    # stdout alone: the log-file notice shares `result.output` but not stdout.
    payload = json.loads(result.stdout)
    assert payload["dry_run"] is True
    assert payload["removed_run_ids"] == []
    assert payload["candidate_run_ids"] == ["r1"]
    assert payload["bytes_freed"] == 0
    assert payload["logical_bytes_selected"] == 120


def test_clean_with_changed_references_refuses_without_traceback(cfg_path, tmp_path, caplog):
    from cnequity.config import load_config
    from cnequity.storage.lifecycle import LifecycleStore, reference_fingerprint

    cfg = load_config(cfg_path)
    evidence = tmp_path / "references"
    evidence.mkdir()
    roots = [{"path": str(evidence)}]
    store = LifecycleStore(cfg.meta_root)
    store.import_manifest(
        {
            "schema_version": 1,
            "meta_root": str(store.meta),
            "holds": {},
            "reference_roots": roots,
            "reference_fingerprint": reference_fingerprint(roots),
        }
    )
    (evidence / "new.json").write_text("{}")
    registry_before = store.registry_path.read_bytes()
    staged = cfg.staging_root / "daily_bars/run_id=kept/input.parquet"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"must survive the refusal")

    result = CliRunner().invoke(cli, ["run", "clean", "--config", cfg_path])

    assert result.exit_code == 1
    assert "References changed; refresh the lifecycle import before cleanup" in result.output
    assert "Traceback" not in result.output
    records = [record for record in caplog.records if record.name == "cnequity.cli"]
    assert len(records) == 1
    assert records[0].exc_info is None
    assert staged.read_bytes() == b"must survive the refusal"
    assert store.registry_path.read_bytes() == registry_before


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


@pytest.mark.usefixtures("chinese_output")
def test_stats_show_builds_the_tables_a_detail_view_needs(cfg_path):
    """The scan has no per-source detail, so `--by-source` builds the tables
    itself rather than sending the reader to `cne stats rebuild` first."""
    from cnequity.config import load_config
    from cnequity.storage.stats import load_summary

    result = CliRunner().invoke(cli, ["stats", "show", "--by-source", "--config", cfg_path])

    assert result.exit_code == 0, result.output
    assert "尚未生成" in result.output
    assert load_summary(load_config(cfg_path)) is not None


@pytest.mark.usefixtures("chinese_output")
def test_stats_show_refreshes_tables_a_later_run_made_stale(cfg_path):
    from cnequity.config import load_config
    from cnequity.storage.stats import rebuild_stats, stats_freshness

    cfg = load_config(cfg_path)
    rebuild_stats(cfg)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily:core", {})
    manifest.record_dataset_result(run_id, "daily_bars", "fetch", "success", rows_written=1)
    manifest.finish_run(run_id, "success", rows_written=1)
    assert stats_freshness(cfg).stale

    result = CliRunner().invoke(cli, ["stats", "show", "--config", cfg_path])

    assert result.exit_code == 0, result.output
    assert "过期" in result.output and "STALE" not in result.output
    assert not stats_freshness(cfg).stale


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


@pytest.mark.usefixtures("chinese_output")
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
    assert "必须一起给" in result.output or "需要同时给" in result.output


def test_query_sql(cfg_path, monkeypatch):
    class FakeCon:
        def sql(self, sql):
            return SimpleNamespace(to_arrow_table=lambda: pl.DataFrame({"n": [1]}).to_arrow())

        def close(self):
            pass

    monkeypatch.setattr("cnequity.cli.consume_cmds.views_connection", lambda cfg: FakeCon())
    result = CliRunner().invoke(cli, ["query", "--config", cfg_path, "--sql", "SELECT 1 AS n"])
    assert result.exit_code == 0, result.output
    assert "1" in result.output


def _stub_audit(monkeypatch, *, errors: int = 0, warnings: int = 0, total: int = 2):
    def run_audit(cfg, rid, day, context=None):
        if context is not None:
            context["audit_by_severity"] = {"error": errors, "warning": warnings}
            context["audit_error_findings"] = [
                {"dataset": "daily_bars", "message": "boom"} for _ in range(errors)
            ]
        return total

    monkeypatch.setattr("cnequity.cli.quality_cmds.run_audit", run_audit)


@pytest.mark.usefixtures("chinese_output")
def test_audit_with_run_id(cfg_path, monkeypatch):
    _stub_audit(monkeypatch)
    result = CliRunner().invoke(cli, ["audit", "--config", cfg_path, "--run-id", "r1"])
    assert result.exit_code == 0, result.output
    assert "2 条 findings" in result.output


def test_audit_multiple_run_ids_are_collected_in_one_pass(cfg_path, monkeypatch):
    calls = []

    def audit(cfg, rid, day, context):
        calls.append((rid, context["related_run_ids"]))
        context["audit_by_severity"] = {"error": 1}
        context["audit_error_findings"] = [{"message": "early group error"}]
        return 1

    monkeypatch.setattr("cnequity.cli.quality_cmds.run_audit", audit)
    result = CliRunner().invoke(
        cli, ["audit", "--config", cfg_path, "--run-id", "core", "--run-id", "research"]
    )
    assert result.exit_code == 1
    assert calls == [("research", ["core", "research"])]
    assert "early group error" in result.output


@pytest.mark.usefixtures("chinese_output")
def test_per_run_audit_exits_non_zero_on_error_findings(cfg_path, monkeypatch):
    """It is used as a gate, so recording an error and reporting success is a
    gate that never fires."""
    _stub_audit(monkeypatch, errors=1, warnings=3)
    result = CliRunner().invoke(cli, ["audit", "--config", cfg_path, "--run-id", "r1"])
    assert result.exit_code == 1, result.output
    assert "1 error，3 warning" in result.output
    # Name what failed; a bare count sends the operator back to the JSON.
    assert "boom" in result.output


def test_per_run_audit_warnings_alone_do_not_fail(cfg_path, monkeypatch):
    _stub_audit(monkeypatch, errors=0, warnings=5)
    result = CliRunner().invoke(cli, ["audit", "--config", cfg_path, "--run-id", "r1"])
    assert result.exit_code == 0, result.output


def _staged_run(cfg_path, run_id: str, *, job: str, status: str, compacted: bool = False):
    cfg = Config(data_root=Path(cfg_path).parent / "data")
    manifest = Manifest(cfg.manifest_path)
    manifest.start_run(job, {}, run_id=run_id)
    manifest.start_batch(run_id, f"{run_id}-b", "instruments", "instruments")
    manifest.finish_batch(run_id, f"{run_id}-b", "success")
    if compacted:
        manifest.start_batch(run_id, f"{run_id}-c", "compact", "compact")
        manifest.finish_batch(run_id, f"{run_id}-c", "success")
    if status != "running":
        manifest.finish_run(run_id, status)
    run_dir = cfg.staging_root / "instruments" / f"run_id={run_id}"
    run_dir.mkdir(parents=True)
    pl.DataFrame({"symbol": ["600000.SH"]}).write_parquet(run_dir / "b0.parquet")


def test_bare_compact_publishes_every_unpublished_finished_run(cfg_path, monkeypatch):
    """No more hunting stranded runs and compacting them one --run-id at a time."""
    _staged_run(cfg_path, "daily-failed", job="daily:core", status="failed")
    _staged_run(cfg_path, "backfill-done", job="backfill", status="warning")
    _staged_run(cfg_path, "already", job="daily:core", status="success", compacted=True)
    _staged_run(cfg_path, "init-run", job="init", status="failed")
    seen: list[str] = []

    def fake_step(self, name, trade_date, run_id, *a, **k):
        assert name == "compact"
        seen.append(run_id)
        return {"status": "success", "rows_written": 1}

    monkeypatch.setattr(JobEngine, "run_step", fake_step)
    monkeypatch.setattr(Manifest, "reconcile_orphaned_runs", lambda self, **k: {})
    result = CliRunner().invoke(cli, ["run", "compact", "--config", cfg_path])

    assert result.exit_code == 0, result.output
    assert seen == ["daily-failed", "backfill-done"], (
        "oldest first; init and published runs left alone"
    )


@pytest.mark.usefixtures("chinese_output")
def test_bare_compact_with_nothing_pending(cfg_path):
    result = CliRunner().invoke(cli, ["run", "compact", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "没有待发布的 staging" in result.output


def test_compact_with_run_id_still_targets_one_run(cfg_path, monkeypatch):
    seen: list[str] = []
    monkeypatch.setattr(
        JobEngine,
        "run_step",
        lambda self, name, td, run_id, *a, **k: seen.append(run_id) or {"rows_written": 9},
    )
    result = CliRunner().invoke(cli, ["run", "compact", "--run-id", "r-1", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert seen == ["r-1"]


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


@pytest.mark.usefixtures("chinese_output")
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
    assert "历史 all-A" in result.output
    assert "BLOCKED" in result.output
    assert "修复建议：run cne delisted coverage" in result.output
    assert "HEALTHY（运维层面；研究层面 BLOCKED）" in result.output


@pytest.mark.usefixtures("chinese_output")
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
    assert "历史 all_a_sh_sz" in result.output


def test_derive_trading_status_and_orphans(cfg_path, monkeypatch):
    seen: dict = {}

    def fake_derive(cfg, run_id, *, start=None, end=None, batch_id="derive-0"):
        seen.update(run_id=run_id, start=start, end=end)
        return 7

    monkeypatch.setattr(
        "cnequity.query.parquet_scan.dataset_has_parquet", lambda _root, **kwargs: True
    )
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
    summary = json.loads(result.stdout)
    assert summary["rows_staged"] == 7
    assert summary["run_id"] == seen["run_id"]
    assert "compact" in summary

    # The one-off target is gone; a valuation_metrics backfill drops orphans itself.
    result = CliRunner().invoke(cli, ["derive", "valuation_orphans", "--config", cfg_path])
    assert result.exit_code != 0


def test_derive_trading_status_failure_is_persisted_and_exits_nonzero(cfg_path, monkeypatch):
    from cnequity.config import load_config

    def fail_derive(*args, **kwargs):
        raise RuntimeError("cannot derive suspension history")

    monkeypatch.setattr(
        "cnequity.query.parquet_scan.dataset_has_parquet", lambda _root, **kwargs: True
    )
    monkeypatch.setattr(
        "cnequity.derive.trading_status_history.derive_suspension_history", fail_derive
    )

    result = CliRunner().invoke(cli, ["derive", "trading_status", "--config", cfg_path])

    assert result.exit_code == 1, result.output
    summary = json.loads(result.stdout)
    assert summary["status"] == "failed"
    persisted = Manifest(load_config(cfg_path).manifest_path).get_run(summary["run_id"])
    assert persisted is not None
    assert persisted["status"] == "failed"


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


def test_delisted_profile_plan_is_offline(cfg_path, monkeypatch):
    monkeypatch.setattr(
        "cnequity.cli.backfill_cmds._run_delisted_profile",
        lambda *_args: pytest.fail("plan must not run a fetch"),
    )
    result = CliRunner().invoke(
        cli,
        [
            "backfill",
            "daily_bars",
            "--profile",
            "delisted",
            "--start",
            "2020-01-01",
            "--plan",
            "--config",
            cfg_path,
        ],
    )
    assert result.exit_code == 0, result.output
    assert '"profile": "delisted"' in result.output


def test_delisted_profile_runs_shared_backfill_path(cfg_path, monkeypatch):
    calls = []

    def fake_run(cfg, since):
        calls.append(since)
        return {"run_id": "bf-1", "status": "success", "rows_written": 8}

    monkeypatch.setattr("cnequity.cli.backfill_cmds._run_delisted_profile", fake_run)
    result = CliRunner().invoke(
        cli,
        [
            "backfill",
            "daily_bars",
            "--profile",
            "delisted",
            "--start",
            "2020-01-01",
            "--config",
            cfg_path,
        ],
    )
    assert result.exit_code == 0, result.output
    assert calls == [date(2020, 1, 1)]
    assert '"run_id": "bf-1"' in result.output


@pytest.mark.usefixtures("chinese_output")
def test_shfe_annual_plan_is_offline_and_requires_partial_fields(cfg_path, tmp_path, monkeypatch):
    archive = tmp_path / "annual.zip"
    archive.write_bytes(b"local-plan-only")
    monkeypatch.setattr(
        "cnequity.cli.backfill_cmds._run_shfe_annual_archive",
        lambda *_args, **_kwargs: pytest.fail("plan must not import"),
    )
    base = [
        "import",
        "shfe-archive",
        str(archive),
        "--dataset",
        "futures_bars",
        "--year",
        "2026",
        "--config",
        cfg_path,
    ]
    # A preview by default: the plan, never the import.
    rejected = CliRunner().invoke(cli, base)
    assert rejected.exit_code != 0
    assert "--accept-partial-fields" in rejected.output
    planned = CliRunner().invoke(cli, [*base, "--accept-partial-fields"])
    assert planned.exit_code == 0, planned.output
    assert '"network_requests": 0' in planned.output
    naive = CliRunner().invoke(
        cli,
        [
            *base,
            "--accept-partial-fields",
            "--downloaded-at",
            "2026-09-27T09:00:00",
        ],
    )
    assert naive.exit_code != 0
    assert "必须带时区" in naive.output


def test_backfill_explains_a_watermark_it_pulled_back(cfg_path, monkeypatch):
    from cnequity.config import load_config
    from cnequity.storage.state import StateStore

    state = StateStore(load_config(cfg_path).meta_root)
    state.set_date("margin_trading", date(2024, 3, 29))

    class FakeEngine:
        def __init__(self, cfg):
            self.manifest = SimpleNamespace(finish_run=lambda *a, **k: None)

        def run_job(self, *a, **k):
            # An older window left a hole, so finalize pulled the watermark back.
            state.set_date("margin_trading", date(2024, 1, 5))
            return {"run_id": "bf", "status": "success", "rows_read": 1, "rows_written": 1}

        def run_step(self, name, trade_date, run_id):
            return {"rows_written": 1}

    monkeypatch.setattr("cnequity.cli.backfill_cmds.JobEngine", FakeEngine)
    result = CliRunner().invoke(
        cli,
        ["backfill", "margin_trading", "--config", cfg_path, "--start", "2024-01-01"],
    )
    assert result.exit_code == 0, result.output
    assert "2024-03-29" in result.output and "2024-01-05" in result.output
    assert "cne backfill margin_trading --start 2024-01-06" in result.output


def test_backfill_snapshot_dataset_reports_capability_and_fallback(cfg_path):
    result = CliRunner().invoke(cli, ["backfill", "fund_flow", "--config", cfg_path])
    # A snapshot-only dataset cannot serve history: nothing usable, so the backfill fails.
    assert result.exit_code == 1, result.output
    assert "fetched nothing usable" in result.stderr
    payload = json.loads(result.stdout)
    assert payload["execution_status"] == "completed"
    assert payload["status"] == "degraded"
    assert payload["fallback"][0]["reason_codes"] == ["capability_limit"]
    assert payload["fallback"][0]["history_mode"] == "snapshot_only"
    assert payload["fallback"][0]["retry_command"] is None
    assert not list(Path(cfg_path).parent.joinpath("data").rglob("*.parquet"))


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


@pytest.mark.parametrize("extra", [[], ["--plan"]])
def test_backfill_trading_status_bj_reports_capability_limit(cfg_path, extra):
    result = CliRunner().invoke(
        cli,
        ["backfill", "trading_status", "--config", cfg_path, "--symbols", "920201.BJ", *extra],
    )
    # A plan writes nothing by design; a run the source cannot serve fetched nothing usable.
    assert result.exit_code == (0 if extra else 1), result.output
    payload = json.loads(result.stdout)
    if extra:
        assert payload["writes"] is False
        assert payload["symbols"] == ["920201.BJ"]
        assert not Path(cfg_path).parent.joinpath("data").exists()
    else:
        assert payload["execution_status"] == "completed"
        assert payload["status"] == "degraded"
        assert payload["results"][0]["unsupported_symbols"] == 1
        assert payload["fallback"][0]["reason_codes"] == ["capability_limit"]
        assert not list(Path(cfg_path).parent.joinpath("data").rglob("*.parquet"))


@pytest.mark.usefixtures("chinese_output")
def test_backfill_refetch_is_refused_where_it_has_no_meaning(cfg_path):
    result = CliRunner().invoke(cli, ["backfill", "daily_bars", "--config", cfg_path, "--refetch"])
    assert result.exit_code != 0
    assert "--refetch" in result.output


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


@pytest.mark.usefixtures("chinese_output")
def test_backfill_workers_rejects_unsupported_date_walk(cfg_path):
    result = CliRunner().invoke(
        cli,
        ["backfill", "daily_bars", "--config", cfg_path, "--workers", "2"],
    )

    assert result.exit_code != 0
    assert "只支持 margin_trading" in result.output


@pytest.mark.usefixtures("chinese_output")
def test_backfill_workers_help_describes_shared_limiter(cfg_path):
    result = CliRunner().invoke(cli, ["backfill", "--help"])

    assert result.exit_code == 0
    assert "共享的源限流器" in result.output
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


def test_bad_sql_is_an_error_not_a_traceback(cfg_path):
    """A SQL typo is the most ordinary thing that happens to this command.

    DuckDB's own message names the line, column and near miss; only the Python
    traceback wrapped around it needed to go.
    """
    result = CliRunner().invoke(
        cli, ["query", "--config", cfg_path, "--sql", "SELECT * FROM nosuchtable"]
    )

    assert result.exit_code != 0
    assert "Traceback" not in result.output
    assert "nosuchtable" in result.output


@pytest.mark.usefixtures("chinese_output")
@pytest.mark.parametrize(
    "argv",
    [
        ["backfill", "daily_bars", "--start", "notadate"],
        ["backfill", "daily_bars", "--end", "2026-13-45"],
        ["run", "daily", "--trade-date", "notadate"],
        ["derive", "adj_factors", "--start", "notadate"],
    ],
)
def test_a_mistyped_date_is_a_usage_error_not_a_traceback(cfg_path, argv):
    """`date.fromisoformat` raises a bare ValueError, which used to reach the
    operator as a Python traceback from thirteen separate call sites."""
    result = CliRunner().invoke(cli, [*argv, "--config", cfg_path])

    assert result.exit_code != 0
    assert "Traceback" not in result.output
    assert "不是 ISO 日期（YYYY-MM-DD）" in result.output


def test_status_datasets_exits_1_when_something_is_stale(cfg_path, monkeypatch):
    """`scripts/scheduler/daily_pipeline.sh` uses this exit code as its stale probe.

    On a clean day it costs one directory walk and skips the retry sleep; if
    the non-zero exit ever stopped happening the pipeline would silently stop
    re-fetching what it missed.
    """
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
                "watermark": [date(2024, 6, 1)],
                "coverage_end": [date(2024, 6, 1)],
            }
        ),
    )
    monkeypatch.setattr("cnequity.domain.datasets.is_stale", lambda *a, **k: True)

    result = CliRunner().invoke(cli, ["status", "--gate", "--datasets", "--config", cfg_path])

    assert result.exit_code == 1, result.output
    assert "STALE" in result.output


def _stale_lake(monkeypatch, datasets: list[str]):
    monkeypatch.setattr(
        "cnequity.cli.quality_cmds._last_trading_day",
        lambda cfg, today: date(2024, 6, 28),
    )
    monkeypatch.setattr(
        "cnequity.query.reader.list_datasets",
        lambda config=None: pl.DataFrame(
            {
                "dataset": datasets,
                "has_data": [True] * len(datasets),
                "watermarked": [True] * len(datasets),
                "watermark": [date(2024, 6, 1)] * len(datasets),
                "coverage_end": [date(2024, 6, 1)] * len(datasets),
            }
        ),
    )
    monkeypatch.setattr("cnequity.domain.datasets.is_stale", lambda *a, **k: True)


@pytest.mark.usefixtures("chinese_output")
def test_freshness_gate_can_be_scoped_to_the_groups_this_host_runs(tmp_path, monkeypatch):
    """A host scheduling `core` alone has twenty-odd datasets no job fetches, so
    they are permanently stale by construction.

    Unscoped, the gate failed every single day — 21 to 25 stale on
    2026-09-12/13/14 — and the daily notification became something to dismiss,
    with three genuinely UNHEALTHY days sitting unread inside the noise. The
    command already worked out which schedule group owns each stale dataset and
    printed "a schedule gap, not a failure"; it just failed anyway.
    """
    scoped = _write_config(
        tmp_path,
        extra=(
            '\n[job.daily.groups.core]\nat = "16:00"\nsteps = ["daily_bars"]\n'
            '\n[job.daily.groups.capital]\nat = "16:30"\nsteps = ["valuation_metrics"]\n'
        ),
    )
    _stale_lake(monkeypatch, ["valuation_metrics"])

    result = CliRunner().invoke(
        cli, ["status", "--gate", "--datasets", "--groups", "core", "--config", scoped]
    )

    assert result.exit_code == 0, result.output
    # Still reported — scoping the gate must not hide the gap.
    assert "STALE" in result.output
    assert "1 个属于这台机器不跑的组" in result.output


def test_a_scoped_gate_still_fails_on_its_own_group(cfg_path, monkeypatch):
    """Scoping is not silencing: a dataset the host does schedule still fails."""
    _stale_lake(monkeypatch, ["daily_bars"])

    result = CliRunner().invoke(
        cli, ["status", "--gate", "--datasets", "--groups", "core", "--config", cfg_path]
    )

    assert result.exit_code == 1, result.output


def test_an_unscoped_gate_still_fails_on_everything(cfg_path, monkeypatch):
    """The default is unchanged: without --groups, any stale dataset fails."""
    _stale_lake(monkeypatch, ["valuation_metrics"])

    result = CliRunner().invoke(cli, ["status", "--gate", "--datasets", "--config", cfg_path])

    assert result.exit_code == 1, result.output


@pytest.mark.usefixtures("chinese_output")
@pytest.mark.parametrize("errors, expected", [(0, 0), (1, 1)])
def test_full_quality_gate_does_not_conflate_optional_freshness(
    cfg_path, monkeypatch, errors, expected
):
    monkeypatch.setattr(
        "cnequity.quality.audit.lake_health",
        lambda *args, **kwargs: {
            "last_trading_day": "2026-09-15",
            "findings_by_severity": {"error": errors},
            "empty_datasets": [],
            "stale_datasets": ["minute_bars"],
            "error_findings": [],
            "warning_findings": [],
            "healthy": False,
            "historical_universe_validity": {
                "window": {"start": "2020-01-01", "end": "2026-09-15"},
                "universe_ready": True,
                "blockers": [],
            },
        },
    )
    result = CliRunner().invoke(cli, ["audit", "--full", "--quality-only", "--config", cfg_path])
    assert result.exit_code == expected, result.output
    assert "STALE 数据集：minute_bars" in result.output
    assert "Quality gate:" in result.output


@pytest.mark.usefixtures("chinese_output")
def test_quality_only_cannot_silently_change_a_per_run_audit(cfg_path):
    result = CliRunner().invoke(cli, ["audit", "--quality-only", "--config", cfg_path])
    assert result.exit_code != 0
    assert "需要配合 --full" in result.output


def test_group_ownership_includes_derived_outputs(tmp_path):
    from cnequity.cli.quality_cmds import stale_datasets_by_group
    from cnequity.config import Config, ScheduleGroup

    cfg = Config(
        data_root=tmp_path,
        schedule_groups={
            "core": ScheduleGroup(
                at="16:00", steps=["daily_bars", "derive_adj_factors", "derive_industry_index"]
            )
        },
    )
    assert stale_datasets_by_group(cfg, ["adj_factors", "industry_index"]) == {
        "core": ["adj_factors", "industry_index"]
    }


def test_cli_derive_publishes_new_revision_and_preserves_old_reader(cfg_path, monkeypatch):
    monkeypatch.setattr(
        "cnequity.query.parquet_scan.dataset_has_parquet", lambda _root, **kwargs: True
    )
    from cnequity.config import load_config
    from cnequity.domain.schemas import ADJ_FACTORS_SCHEMA
    from cnequity.query.parquet_scan import scan_parquet_root
    from cnequity.storage.revisions import RevisionStore

    cfg = load_config(cfg_path)
    root = cfg.derived_root / "adj_factors"
    path = root / "trade_date=2026-09-15" / "part-0.parquet"
    path.parent.mkdir(parents=True)
    frame = pl.DataFrame(
        [
            {
                "symbol": "920001.BJ",
                "trade_date": date(2026, 9, 15),
                "adjust_type": "hfq",
                "factor": 1.0,
                "source": "sina",
                "data_version": "v1",
                "fetched_at": None,
            }
        ],
        schema=ADJ_FACTORS_SCHEMA,
    )
    frame.write_parquet(path)
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    before = store.ensure_current("adj_factors")

    def compute(config, full=False):
        frame.with_columns(pl.lit(2.0).alias("factor")).write_parquet(path)
        return AdjFactorsResult(1, 1, [], [])

    monkeypatch.setattr("cnequity.cli.maintain_cmds.compute_adj_factors", compute)
    result = CliRunner().invoke(cli, ["derive", "adj_factors", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert store.current_root("adj_factors") != before
    assert scan_parquet_root(root).collect()["factor"].to_list() == [2.0]
    assert pl.read_parquet(before / "trade_date=2026-09-15" / "part-0.parquet")[
        "factor"
    ].to_list() == [1.0]


def test_cli_derive_source_limit_finishes_with_fallback(cfg_path, monkeypatch):
    monkeypatch.setattr(
        "cnequity.query.parquet_scan.dataset_has_parquet", lambda _root, **kwargs: True
    )
    monkeypatch.setattr(
        "cnequity.cli.maintain_cmds.compute_adj_factors",
        lambda cfg, full=False: AdjFactorsResult(0, 1, ["920001.BJ:hfq"], []),
    )
    result = CliRunner().invoke(cli, ["derive", "adj_factors", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output[result.output.index("{") :])
    assert payload["status"] == "degraded"
    assert payload["coverage_status"] == "partial"
    assert payload["fallback"][0]["retry_command"].startswith("cne derive adj_factors")


def test_run_daily_all_groups_runs_each_group_and_survives_one_failure(tmp_path, monkeypatch):
    """The whole day in one command, for a lake installed from PyPI.

    `scripts/scheduler/daily_pipeline.sh` is the only thing that ran all six groups and
    it is not installed by the package, so the single command people reached
    for covered a third of the lake in silence.
    """
    cfg = _write_config(
        tmp_path,
        extra=(
            '\n[job.daily.groups.core]\nat = "16:00"\nsteps = ["compact"]\n'
            '\n[job.daily.groups.capital]\nat = "17:00"\nsteps = ["compact"]\n'
            '\n[job.daily.groups.research]\nat = "18:00"\nsteps = ["compact"]\n'
        ),
    )
    seen: list[str] = []

    def fake_run_job(self, job_name, *a, **k):
        seen.append(job_name)
        if job_name == "daily:capital":
            raise RuntimeError("eastmoney refused the window")
        return {"run_id": f"r-{job_name}", "status": "success", "results": []}

    monkeypatch.setattr(JobEngine, "run_job", fake_run_job)
    result = CliRunner().invoke(cli, ["run", "daily", "--all-groups", "--config", cfg])

    assert seen == ["daily:core", "daily:capital", "daily:research"], "config order, all of them"
    assert result.exit_code == 1, "a failed group must reach the scheduler"
    assert "eastmoney refused the window" in result.output
    payload = json.loads(result.output[result.output.index("{") :])
    assert [g["status"] for g in payload["groups"]] == ["success", "failed", "success"]


@pytest.mark.parametrize(
    ("statuses", "exit_code"),
    [
        (["failed", "degraded", "success"], 1),
        (["degraded", "failed", "success"], 1),
        (["exception", "degraded", "success"], 1),
        (["degraded", "exception", "success"], 1),
        (["degraded", "success", "success"], 0),
        (["success", "warning", "success"], 0),
    ],
)
def test_all_groups_core_failure_takes_precedence_over_degradation(
    tmp_path, monkeypatch, statuses, exit_code
):
    cfg = _write_config(
        tmp_path,
        extra="".join(
            f'\n[job.daily.groups.{name}]\nat = "17:00"\nsteps = ["compact"]\n'
            for name in ("core", "capital", "research")
        ),
    )
    seen = []

    def fake_run_job(self, job_name, **kwargs):
        status = statuses[len(seen)]
        seen.append(job_name)
        if status == "exception":
            raise RuntimeError("group could not start")
        return {"run_id": job_name, "status": status}

    monkeypatch.setattr(JobEngine, "run_job", fake_run_job)
    result = CliRunner().invoke(cli, ["run", "daily", "--all-groups", "--config", cfg])
    assert result.exit_code == exit_code, result.output
    assert len(seen) == 3


@pytest.mark.parametrize(
    ("day", "backfill", "history_due"),
    [("2026-09-23", False, False), ("2026-09-24", False, True), ("2026-09-23", True, True)],
)
def test_all_groups_honors_weekly_history_but_keeps_snapshots(
    tmp_path, monkeypatch, day, backfill, history_due
):
    cfg = _write_config(
        tmp_path,
        extra=(
            '\n[job.daily.groups.signals]\nat = "17:00"\ncadence = "weekly"\n'
            'steps = ["dragon_tiger", "block_trades", "compact"]\n'
            '\n[job.daily.groups.capital]\nat = "17:00"\ncadence = "weekly"\n'
            'steps = ["fund_flow", "margin_trading", "compact"]\n'
        ),
    )
    seen = {}

    def fake_run_job(self, job_name, **kwargs):
        seen[job_name] = kwargs["waves"][0].steps
        return {"run_id": job_name, "status": "success", "results": []}

    monkeypatch.setattr(JobEngine, "run_job", fake_run_job)
    args = ["run", "daily", "--all-groups", "--config", cfg, "--trade-date", day]
    result = CliRunner().invoke(cli, [*args, *(["--backfill"] if backfill else [])])
    assert result.exit_code == 0, result.output
    if history_due:
        assert seen["daily:signals"] == ["dragon_tiger", "block_trades", "compact"]
        assert seen["daily:capital"] == ["fund_flow", "margin_trading", "compact"]
    else:
        assert "daily:signals" not in seen
        assert seen["daily:capital"] == ["fund_flow", "compact"]
        assert "skipped_not_scheduled" in result.output


_DAY_GROUPS = (
    '\n[job.daily.groups.core]\nat = "16:00"\nsteps = ["compact"]\n'
    '\n[job.daily.groups.capital]\nat = "17:00"\nsteps = ["compact"]\n'
    '\n[job.events.groups.disclosures]\nsteps = ["compact"]\n'
)


def _record_jobs(monkeypatch, *, events_error: Exception | None = None) -> list[str]:
    seen: list[str] = []

    def fake_run_job(self, job_name, *a, **k):
        seen.append(job_name)
        if job_name == "events" and events_error is not None:
            raise events_error
        return {"run_id": f"r-{job_name}", "status": "success", "results": []}

    monkeypatch.setattr(JobEngine, "run_job", fake_run_job)
    return seen


def test_bare_run_daily_runs_every_group_then_the_event_stream(tmp_path, monkeypatch):
    """One command for the whole day: no second cron line for `run events`."""
    cfg = _write_config(tmp_path, extra=_DAY_GROUPS)
    seen = _record_jobs(monkeypatch)
    result = CliRunner().invoke(cli, ["run", "daily", "--config", cfg])
    assert result.exit_code == 0, result.output
    assert seen == ["daily:core", "daily:capital", "daily:audit", "events"]
    payload = json.loads(result.output[result.output.index("{") :])
    assert payload["audit"]["status"] == "success", "one lake audit, after every group"
    assert payload["events"]["status"] == "success"


def _pin_monday(monkeypatch, *, final: bool) -> date:
    """Monday 2026-10-19 in Beijing, before or after the 15:05 settlement."""
    monday = date(2026, 10, 19)
    settled = (lambda _day: True) if final else (lambda day: day != monday)
    monkeypatch.setattr("cnequity.cli.run_cmds.shanghai_today", lambda: monday)
    monkeypatch.setattr("cnequity.cli.run_cmds.is_session_final", settled)
    monkeypatch.setattr("cnequity.cli.quality_cmds.is_session_final", settled)
    return monday


def _record_trade_dates(monkeypatch) -> dict[str, object]:
    seen: dict[str, object] = {}

    def fake_run_job(self, job_name, *a, trade_date=None, **k):
        seen[job_name] = trade_date
        return {"run_id": f"r-{job_name}", "status": "success", "results": []}

    monkeypatch.setattr(JobEngine, "run_job", fake_run_job)
    return seen


def test_run_daily_before_the_close_updates_through_the_last_settled_session(tmp_path, monkeypatch):
    """At 01:00 on a Monday the groups catch up to Friday, not to Monday.

    Targeting Monday made `daily_bars` refuse the unfinished session with a
    traceback, and stamped snapshot feeds read before the open with Monday's
    date. The event stream follows the calendar and keeps today.
    """
    cfg = _write_config(tmp_path, extra=_DAY_GROUPS)
    _pin_monday(monkeypatch, final=False)
    seen = _record_trade_dates(monkeypatch)
    result = CliRunner().invoke(cli, ["run", "daily", "--config", cfg])
    assert result.exit_code == 0, result.output
    friday = date(2026, 10, 16)
    assert seen["daily:core"] == friday
    assert seen["daily:capital"] == friday
    assert seen["daily:audit"] == friday
    assert seen["events"] is None
    assert "not final until 15:05" in result.output


@pytest.mark.parametrize("flags", [["--group", "core"], ["--core-only"]])
def test_single_group_runs_also_wait_for_the_settled_session(tmp_path, monkeypatch, flags):
    cfg = _write_config(
        tmp_path,
        extra=_DAY_GROUPS + '\n[[job.daily.waves]]\nname = "w"\nsteps = ["compact"]\n',
    )
    _pin_monday(monkeypatch, final=False)
    seen = _record_trade_dates(monkeypatch)
    result = CliRunner().invoke(cli, ["run", "daily", "--config", cfg, *flags])
    assert result.exit_code == 0, result.output
    assert list(seen.values()) == [date(2026, 10, 16)]


@pytest.mark.parametrize(
    ("final", "flags", "expected"),
    [
        (True, [], None),
        (False, ["--trade-date", "2026-10-19"], date(2026, 10, 19)),
    ],
)
def test_settled_session_leaves_final_and_explicit_dates_alone(
    tmp_path, monkeypatch, final, flags, expected
):
    cfg = _write_config(tmp_path, extra=_DAY_GROUPS)
    _pin_monday(monkeypatch, final=final)
    seen = _record_trade_dates(monkeypatch)
    result = CliRunner().invoke(cli, ["run", "daily", "--no-events", "--config", cfg, *flags])
    assert result.exit_code == 0, result.output
    assert seen["daily:core"] == expected
    assert "not final until 15:05" not in result.output


@pytest.mark.parametrize(
    ("flags", "expected"),
    [
        (["--no-events"], ["daily:core", "daily:capital", "daily:audit"]),
        (["--all-groups"], ["daily:core", "daily:capital"]),
        (["--backfill"], ["daily:core", "daily:capital", "daily:audit"]),
        (["--core-only"], ["daily"]),
        (["--group", "capital"], ["daily:capital"]),
    ],
)
def test_run_daily_narrowing_flags(tmp_path, monkeypatch, flags, expected):
    cfg = _write_config(tmp_path, extra=_DAY_GROUPS)
    seen = _record_jobs(monkeypatch)
    result = CliRunner().invoke(cli, ["run", "daily", "--config", cfg, *flags])
    assert result.exit_code == 0, result.output
    assert seen == expected


def test_bare_run_daily_points_a_stale_config_at_upgrade(tmp_path, monkeypatch):
    cfg = _write_config(tmp_path, extra=_DAY_GROUPS)
    _record_jobs(monkeypatch)
    result = CliRunner().invoke(cli, ["run", "daily", "--config", cfg])
    assert result.exit_code == 0, result.output
    assert "cne config upgrade" in result.output


def test_run_daily_skips_the_audit_when_no_group_ran(tmp_path, monkeypatch):
    """A weekend: every group skips, so there is nothing new to audit."""
    cfg = _write_config(tmp_path, extra=_DAY_GROUPS)
    seen: list[str] = []

    def fake_run_job(self, job_name, *a, **k):
        seen.append(job_name)
        status = "success" if job_name == "events" else "skipped_non_trading_day"
        return {"run_id": f"r-{job_name}", "status": status, "results": []}

    monkeypatch.setattr(JobEngine, "run_job", fake_run_job)
    result = CliRunner().invoke(cli, ["run", "daily", "--config", cfg])
    assert result.exit_code == 0, result.output
    assert seen == ["daily:core", "daily:capital", "events"]


def test_run_daily_leaves_a_running_event_stream_alone(tmp_path, monkeypatch):
    """An existing `cne run events` cron holding its lock is not a failed day."""
    cfg = _write_config(tmp_path, extra=_DAY_GROUPS)
    _record_jobs(monkeypatch, events_error=RunLockError("events lock held"))
    result = CliRunner().invoke(cli, ["run", "daily", "--config", cfg])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output[result.output.index("{") :])
    assert payload["events"]["status"] == "skipped_locked"


def test_run_daily_reports_a_failed_event_stream(tmp_path, monkeypatch):
    cfg = _write_config(tmp_path, extra=_DAY_GROUPS)
    _record_jobs(monkeypatch, events_error=RuntimeError("cninfo down"))
    result = CliRunner().invoke(cli, ["run", "daily", "--config", cfg])
    assert result.exit_code == 1
    assert "cninfo down" in result.output


@pytest.mark.parametrize(
    "flags", [["--core-only", "--group", "core"], ["--no-events", "--all-groups"]]
)
def test_run_daily_rejects_contradictory_scope_flags(tmp_path, flags):
    cfg = _write_config(tmp_path, extra=_DAY_GROUPS)
    result = CliRunner().invoke(cli, ["run", "daily", "--config", cfg, *flags])
    assert result.exit_code != 0


def test_run_daily_all_groups_skips_groups_whose_datasets_are_off(tmp_path, monkeypatch):
    """`intraday` and `ticks` are configured but opt-in, and off by default."""
    cfg = _write_config(
        tmp_path,
        extra=(
            '\n[job.daily.groups.core]\nat = "16:00"\nsteps = ["compact"]\n'
            '\n[job.daily.groups.intraday]\nat = "18:45"\n'
            'steps = ["minute_bars", "compact"]\n'
        ),
    )
    seen: list[str] = []

    def fake_run_job(self, job_name, *a, **k):
        seen.append(job_name)
        return {"run_id": f"r-{job_name}", "status": "success", "results": []}

    monkeypatch.setattr(JobEngine, "run_job", fake_run_job)
    result = CliRunner().invoke(cli, ["run", "daily", "--all-groups", "--config", cfg])

    assert result.exit_code == 0, result.output
    assert seen == ["daily:core"]
    assert "skipped" in result.output


def _tip_scope_lake(tmp_path, *, missing: int = 1):
    """A lake whose tip partition is missing *missing* of ten active symbols."""
    cfg = Config(data_root=tmp_path / "data")
    tip = date(2024, 6, 28)
    symbols = [f"60000{i}.SH" for i in range(4)] + [f"00000{i}.SZ" for i in range(4)]
    symbols += ["920001.BJ", "920002.BJ"]
    instruments_root = cfg.curated_root / "instruments"
    bars_root = cfg.curated_root / "daily_bars" / f"trade_date={tip.isoformat()}"
    instruments_root.mkdir(parents=True)
    bars_root.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": symbols,
            "list_date": [date(2000, 1, 1)] * len(symbols),
            "delist_date": [None] * len(symbols),
        }
    ).write_parquet(instruments_root / "part-0.parquet")
    observed = symbols[: len(symbols) - missing]
    pl.DataFrame({"symbol": observed, "trade_date": [tip] * len(observed)}).write_parquet(
        bars_root / "part-0.parquet"
    )
    catalog = pl.DataFrame(
        {
            "dataset": ["daily_bars"],
            "has_data": [True],
            "watermarked": [True],
            "watermark": [tip],
            "coverage_end": [tip],
        }
    )
    return cfg, tip, symbols, catalog


def test_a_key_the_ingest_side_already_tolerated_is_not_a_second_failure(tmp_path):
    """`daily_bars` carries an explicit unresolved-key budget — refusing a
    checkpoint over 14 of 5,500 threw away two hours of `cne init` — and every
    key it lets through is written to the outstanding ledger and reported
    separately. Failing the status gate for those same keys would be the two
    halves of one system contradicting each other, with the operator between.
    """
    from cnequity.cli.quality_cmds import _tip_scope
    from cnequity.storage.state import StateStore

    cfg, tip, symbols, catalog = _tip_scope_lake(tmp_path)
    cfg.meta_root.mkdir(parents=True, exist_ok=True)

    before = _tip_scope(cfg, catalog)
    assert before["state"] == "incomplete"
    assert before["missing"] == [symbols[-1]]

    StateStore(cfg.meta_root).record_outstanding_keys(
        "daily_bars",
        [(symbols[-1], tip)],
        run_id="run-1",
        reason="unresolved_tip",
    )

    after = _tip_scope(cfg, catalog)
    assert after["state"] == "complete"
    assert after["missing"] == []
    assert after["owed"] == 1


def test_an_owed_key_for_a_different_day_does_not_excuse_this_one(tmp_path):
    from cnequity.cli.quality_cmds import _tip_scope
    from cnequity.storage.state import StateStore

    cfg, tip, symbols, catalog = _tip_scope_lake(tmp_path)
    cfg.meta_root.mkdir(parents=True, exist_ok=True)
    StateStore(cfg.meta_root).record_outstanding_keys(
        "daily_bars",
        [(symbols[-1], date(2024, 6, 27))],
        run_id="run-1",
        reason="unresolved_tip",
    )

    assert _tip_scope(cfg, catalog)["state"] == "incomplete"


def test_scope_gate_respects_the_groups_flag_that_owns_daily_bars(cfg_path, monkeypatch):
    """`--groups` promises to gate only on what this host is scheduled to
    fetch. A cross-section hole in daily_bars is a claim about whoever owns
    daily_bars, so it answers to the same promise."""
    from cnequity.cli import quality_cmds as qc

    monkeypatch.setattr(
        qc,
        "_tip_scope",
        lambda cfg, catalog, dataset="daily_bars": (
            {
                "dataset": dataset,
                "state": "incomplete",
                "date": date(2024, 6, 28),
                "covered": 1,
                "expected": 4,
                "ratio": 0.25,
                "missing": ["000001.SZ"],
                "owed": 0,
            }
            if dataset == "daily_bars"
            else None
        ),
    )
    monkeypatch.setattr(qc, "stale_datasets_by_group", lambda cfg, names: {"core": list(names)})
    monkeypatch.setattr(qc, "_last_trading_day", lambda config, today: date(2024, 6, 28))
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

    runner = CliRunner()
    gated = runner.invoke(
        cli, ["status", "--gate", "--datasets", "--groups", "core", "--config", cfg_path]
    )
    assert gated.exit_code == 1, gated.output

    exempt = runner.invoke(
        cli, ["status", "--gate", "--datasets", "--groups", "capital", "--config", cfg_path]
    )
    assert exempt.exit_code == 0, exempt.output


@pytest.mark.usefixtures("chinese_output")
def test_unprovable_coverage_exits_two_not_one(cfg_path, monkeypatch):
    """ "5,207 securities have no bar" is a repair; "instruments is missing, so
    I cannot check" is a setup problem. A caller that reads both as the same
    red light learns to ignore both."""
    from cnequity.cli import quality_cmds as qc

    monkeypatch.setattr(
        qc,
        "_tip_scope",
        lambda cfg, catalog, dataset="daily_bars": (
            {
                "dataset": dataset,
                "state": "unverified",
                "date": date(2024, 6, 28),
                "message": "instruments 缺失",
            }
            if dataset == "daily_bars"
            else None
        ),
    )
    monkeypatch.setattr(qc, "_last_trading_day", lambda config, today: date(2024, 6, 28))
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

    result = CliRunner().invoke(cli, ["status", "--gate", "--datasets", "--config", cfg_path])
    assert result.exit_code == 2, result.output
    assert "UNVERIFIED" in result.output
    assert "证明不了" in result.output


def test_no_scope_skips_the_cross_section_read_entirely(cfg_path, monkeypatch):
    """The check reads the tip partition, instruments and trading_status. That
    is not free, and `cne status` used to be a metadata-only command."""
    from cnequity.cli import quality_cmds as qc

    calls: list[int] = []

    def _scope(cfg, catalog, dataset="daily_bars"):
        calls.append(dataset)
        return {
            "dataset": dataset,
            "state": "incomplete",
            "date": date(2024, 6, 28),
            "covered": 0,
            "expected": 4,
            "ratio": 0.0,
            "missing": ["600519.SH"],
            "owed": 0,
        }

    monkeypatch.setattr(qc, "_tip_scope", _scope)
    monkeypatch.setattr(qc, "_last_trading_day", lambda config, today: date(2024, 6, 28))
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

    result = CliRunner().invoke(
        cli, ["status", "--gate", "--datasets", "--no-scope", "--config", cfg_path]
    )
    assert result.exit_code == 0, result.output
    assert not calls
    assert "取数截面" not in result.output


def test_trading_status_is_proved_against_the_same_instrument_list(tmp_path):
    """The cross-section proof is not a `daily_bars` special case: any dataset
    whose expected tip keys are "every instrument active that session" answers
    the same question, and freshness is equally one-dimensional for all of
    them."""
    from cnequity.cli.quality_cmds import INSTRUMENT_SCOPED_DATASETS, _tip_scope

    assert "trading_status" in INSTRUMENT_SCOPED_DATASETS
    # index_bars is keyed by an index universe, not by instruments; holding it
    # to this list would invent a gap rather than find one.
    assert "index_bars" not in INSTRUMENT_SCOPED_DATASETS

    cfg = Config(data_root=tmp_path / "data")
    tip = date(2024, 6, 28)
    symbols = ["600519.SH", "000001.SZ", "920001.BJ"]
    instruments_root = cfg.curated_root / "instruments"
    status_root = cfg.curated_root / "trading_status" / f"trade_date={tip.isoformat()}"
    instruments_root.mkdir(parents=True)
    status_root.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": symbols,
            "list_date": [date(2000, 1, 1)] * 3,
            "delist_date": [None] * 3,
        }
    ).write_parquet(instruments_root / "part-0.parquet")
    # One security has no status row at all on the tip session.
    pl.DataFrame(
        {
            "symbol": symbols[:2],
            "trade_date": [tip, tip],
            "is_trading": [True, True],
        }
    ).write_parquet(status_root / "part-0.parquet")

    catalog = pl.DataFrame(
        {
            "dataset": ["trading_status"],
            "has_data": [True],
            "watermarked": [True],
            "watermark": [tip],
            "coverage_end": [tip],
        }
    )

    report = _tip_scope(cfg, catalog, "trading_status")

    assert report is not None
    assert report["dataset"] == "trading_status"
    assert report["state"] == "incomplete"
    assert report["missing"] == ["920001.BJ"]


def test_every_scope_report_names_its_dataset(tmp_path):
    """The caller prints and gates per dataset, so a report that cannot say
    which dataset it describes is unusable — and the early `unverified` exits
    are exactly the ones that used to omit it."""
    from cnequity.cli.quality_cmds import _tip_scope

    cfg = Config(data_root=tmp_path / "data")
    bars_root = cfg.curated_root / "daily_bars" / "trade_date=2024-06-28"
    bars_root.mkdir(parents=True)
    pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [date(2024, 6, 28)]}).write_parquet(
        bars_root / "part-0.parquet"
    )
    catalog = pl.DataFrame(
        {
            "dataset": ["daily_bars"],
            "has_data": [True],
            "watermarked": [True],
            "watermark": [date(2024, 6, 28)],
            "coverage_end": [date(2024, 6, 28)],
        }
    )

    # No instruments table: the proof cannot be made, and must say so about a
    # named dataset.
    report = _tip_scope(cfg, catalog, "daily_bars")
    assert report["state"] == "unverified"
    assert report["dataset"] == "daily_bars"


@pytest.mark.parametrize(
    ("args", "needle"),
    [
        (["decision-data", "stock-terms"], "corporate_actions"),
        (["derive", "industry_index"], "industry_members"),
    ],
)
def test_a_missing_input_dataset_is_an_error_not_a_traceback(tmp_path, args, needle):
    from cnequity.config import load_config
    from cnequity.storage.layout import init_data_layout

    cfg_path = _demo_config(tmp_path)
    init_data_layout(load_config(cfg_path))

    result = CliRunner().invoke(cli, [*args, "--config", cfg_path])

    assert result.exit_code == 1
    assert needle in result.output
    assert "cne backfill" in result.output
    assert "Traceback" not in result.output
    assert not isinstance(result.exception, (ValueError, RuntimeError))


def test_comma_option_joins_repeats_and_stays_none_when_absent():
    import click

    from cnequity.cli._shared import comma_option

    @click.command()
    @comma_option("--only", default=None)
    def probe(only):
        click.echo(repr(only))

    runner = CliRunner()
    assert runner.invoke(probe, []).output.strip() == "None"
    assert runner.invoke(probe, ["--only", "a,b"]).output.strip() == "'a,b'"
    assert runner.invoke(probe, ["--only", "a", "--only", "b,c"]).output.strip() == "'a,b,c'"


@pytest.mark.parametrize("dataset", ["adj_factors", "valuation_metrics"])
def test_backfill_symbols_on_an_unscoped_dataset_is_a_clear_error(tmp_path, dataset):
    """`code, _, exchange = ...` shadowed gettext, so this path raised UnboundLocalError."""
    result = CliRunner().invoke(
        cli,
        [
            "backfill",
            dataset,
            "--symbols",
            "600519.SH",
            "--plan",
            "--config",
            _demo_config(tmp_path),
        ],
    )

    assert result.exit_code == 1
    assert "does not support overriding the scope by symbol" in result.output
    assert not isinstance(result.exception, UnboundLocalError)


def test_backfill_futures_minute_rejects_a_bad_contract_cleanly(tmp_path):
    result = CliRunner().invoke(
        cli,
        [
            "backfill",
            "futures_minute_bars",
            "--symbols",
            "NOPE.SHF",
            "--config",
            _demo_config(tmp_path),
        ],
    )

    assert result.exit_code == 1
    assert not isinstance(result.exception, UnboundLocalError)


def test_corporate_action_gaps_without_evidence_names_the_command_to_run(tmp_path):
    from cnequity.config import load_config
    from cnequity.storage.layout import init_data_layout

    cfg_path = _demo_config(tmp_path)
    init_data_layout(load_config(cfg_path))

    result = CliRunner().invoke(cli, ["repair", "corporate-action-gaps", "--config", cfg_path])

    assert result.exit_code == 1
    assert "cne derive adj_factor_source" in result.output
    assert isinstance(result.exception, SystemExit)


@pytest.mark.parametrize(
    ("target", "needle"),
    [("sector_code_map", "sector_bars")],
)
def test_derive_with_missing_input_data_is_an_error_not_a_traceback(tmp_path, target, needle):
    from cnequity.config import load_config
    from cnequity.storage.layout import init_data_layout

    cfg_path = _demo_config(tmp_path)
    init_data_layout(load_config(cfg_path))

    result = CliRunner().invoke(cli, ["derive", target, "--config", cfg_path])

    assert result.exit_code == 1
    assert needle in result.output
    assert isinstance(result.exception, SystemExit)


def test_retry_of_a_succeeded_run_says_there_is_nothing_to_retry(cfg_path, monkeypatch):
    class FakeManifest:
        def get_run(self, run_id):
            return {"run_id": run_id, "job_name": "daily", "status": "success"}

    class FakeEngine:
        def __init__(self, cfg):
            self.manifest = FakeManifest()

        def run_job(self, *a, **k):
            raise AssertionError("a succeeded run must not be re-run")

    monkeypatch.setattr("cnequity.cli.run_cmds.JobEngine", FakeEngine)
    result = CliRunner().invoke(cli, ["run", "retry", "--config", cfg_path, "--run-id", "d1"])
    assert result.exit_code == 0, result.output
    assert "already succeeded" in result.output


def test_a_broken_config_names_the_line_but_not_its_text(tmp_path):
    """The position is what a person needs; the text may hold a token."""
    path = tmp_path / "broken.toml"
    path.write_text('[sources.tushare]\ntoken = "s3cret\n', encoding="utf-8")
    result = CliRunner().invoke(cli, ["status", "--config", str(path)])
    assert result.exit_code == 1
    assert "TOMLDecodeError at line 2, column" in result.output
    assert "s3cret" not in result.output
