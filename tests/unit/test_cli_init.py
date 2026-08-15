from datetime import date
from pathlib import Path

from click.testing import CliRunner

from cn_market_lake.cli.main import cli
from cn_market_lake.config import load_config
from cn_market_lake.config.bootstrap import path_for_toml
from cn_market_lake.orchestrator.engine import JobEngine
from cn_market_lake.storage.layout import init_data_layout


def _write_config(tmp_path) -> str:
    cfg_path = tmp_path / "cn-market-lake.toml"
    cfg_path.write_text(
        f"""
[data]
root = "{path_for_toml(tmp_path / "data")}"

[tdx_protocol]
allow_mock = true

[job.init.phases]
names = ["phase1_reference"]
"""
    )
    return str(cfg_path)


def test_init_layout_only_skips_phases(tmp_path, monkeypatch):
    cfg_path = _write_config(tmp_path)
    called = {"phases": False}

    def fake_run_init_phases(self, trade_date=None, **kwargs):
        called["phases"] = True
        return {"run_id": "x", "status": "success", "phases": []}

    monkeypatch.setattr(JobEngine, "run_init_phases", fake_run_init_phases)

    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--config", cfg_path, "--layout-only"])

    assert result.exit_code == 0
    assert called["phases"] is False
    assert "Initialized layout" in result.output
    cfg = load_config(cfg_path)
    init_data_layout(cfg)
    assert cfg.curated_root.exists()


def test_init_runs_phases_by_default(tmp_path, monkeypatch):
    cfg_path = _write_config(tmp_path)
    seen: dict[str, date | None] = {"trade_date": "unset"}

    def fake_run_init_phases(self, trade_date=None, **kwargs):
        seen["trade_date"] = trade_date
        return {
            "run_id": "init-run",
            "status": "success",
            "phases": [{"phase": "phase1_reference", "status": "success"}],
        }

    monkeypatch.setattr(JobEngine, "run_init_phases", fake_run_init_phases)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["init", "--config", cfg_path, "--trade-date", "2024-06-28"],
    )

    assert result.exit_code == 0
    assert seen["trade_date"] == date(2024, 6, 28)
    assert "init-run" in result.output


def test_run_daily_passes_trade_date(tmp_path, monkeypatch):
    cfg_path = _write_config(tmp_path)
    # Minimal schedule group so --group core resolves.
    Path(cfg_path).write_text(
        Path(cfg_path).read_text()
        + """
[job.daily.groups.core]
at = "16:00"
steps = ["compact"]
"""
    )
    seen: dict = {}

    def fake_run_job(self, job_name, trade_date=None, **kwargs):
        seen["job_name"] = job_name
        seen["trade_date"] = trade_date
        return {"run_id": "d1", "status": "success", "results": []}

    monkeypatch.setattr(JobEngine, "run_job", fake_run_job)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["run", "daily", "--config", cfg_path, "--group", "core", "--trade-date", "2026-07-17"],
    )
    assert result.exit_code == 0, result.output
    assert seen["job_name"] == "daily:core"
    assert seen["trade_date"] == date(2026, 7, 17)


def test_run_catchup_core_then_breadth(tmp_path, monkeypatch):
    cfg_path = _write_config(tmp_path)
    Path(cfg_path).write_text(
        Path(cfg_path).read_text()
        + """
[job.daily.groups.core]
at = "16:00"
steps = ["compact"]
"""
    )
    calls: list[tuple] = []

    def fake_run_job(self, job_name, trade_date=None, **kwargs):
        calls.append(
            (job_name, trade_date, [s for w in kwargs.get("waves") or [] for s in w.steps])
        )
        return {"run_id": f"r-{len(calls)}", "status": "success", "results": []}

    monkeypatch.setattr(JobEngine, "run_job", fake_run_job)
    monkeypatch.setattr(
        "cn_market_lake.steps.common.is_trading_day",
        lambda cfg, d: True,
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["run", "catchup", "--config", cfg_path, "--trade-date", "2026-07-17"],
    )
    assert result.exit_code == 0, result.output
    assert calls[0][0] == "daily:core"
    assert calls[0][1] == date(2026, 7, 17)
    assert calls[1][0] == "daily:market_breadth"
    assert "market_breadth" in calls[1][2] and "compact" in calls[1][2]


def test_run_catchup_skips_when_gate_already_fresh(tmp_path, monkeypatch):
    cfg_path = _write_config(tmp_path)
    Path(cfg_path).write_text(
        Path(cfg_path).read_text()
        + """
[job.daily.groups.core]
at = "16:00"
steps = ["compact"]
"""
    )
    calls: list[str] = []

    def fake_run_job(self, job_name, trade_date=None, **kwargs):
        calls.append(job_name)
        return {"run_id": "x", "status": "success", "results": []}

    monkeypatch.setattr(JobEngine, "run_job", fake_run_job)
    monkeypatch.setattr(
        "cn_market_lake.steps.common.is_trading_day",
        lambda cfg, d: True,
    )
    monkeypatch.setattr(
        "cn_market_lake.cli.main._dataset_watermark",
        lambda cfg, name: date(2026, 7, 17),
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["run", "catchup", "--config", cfg_path, "--trade-date", "2026-07-17"],
    )
    assert result.exit_code == 0, result.output
    assert calls == []
    assert "skipped_already_fresh" in result.output


def test_run_catchup_extra_group_failure_still_ok(tmp_path, monkeypatch):
    cfg_path = _write_config(tmp_path)
    Path(cfg_path).write_text(
        Path(cfg_path).read_text()
        + """
[job.daily.groups.core]
at = "16:00"
steps = ["compact"]

[job.daily.groups.capital]
at = "16:30"
steps = ["compact"]
"""
    )
    calls: list[str] = []

    def fake_run_job(self, job_name, trade_date=None, **kwargs):
        calls.append(job_name)
        status = "failed" if job_name == "daily:capital" else "success"
        return {"run_id": f"r-{len(calls)}", "status": status, "results": []}

    monkeypatch.setattr(JobEngine, "run_job", fake_run_job)
    monkeypatch.setattr(
        "cn_market_lake.steps.common.is_trading_day",
        lambda cfg, d: True,
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "run",
            "catchup",
            "--config",
            cfg_path,
            "--trade-date",
            "2026-07-17",
            "--extra-group",
            "capital",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "daily:capital" in calls
    assert '"status": "failed"' in result.output


def test_init_exits_nonzero_when_phase_fails(tmp_path, monkeypatch):
    cfg_path = _write_config(tmp_path)

    def fake_run_init_phases(self, trade_date=None, **kwargs):
        return {
            "run_id": "init-run",
            "status": "failed",
            "phases": [{"phase": "phase1_reference", "status": "failed"}],
        }

    monkeypatch.setattr(JobEngine, "run_init_phases", fake_run_init_phases)

    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--config", cfg_path])

    assert result.exit_code == 1
