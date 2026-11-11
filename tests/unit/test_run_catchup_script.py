"""`scripts/run_catchup.py` — the former `cne run catchup`.

It moved out of the CLI because it is composition, not capability: every step is
`cne run daily` against one schedule group, plus opinions about which groups to
run and which failures are advisory, and those opinions belong in a script.

Moving it must not have cost it its behaviour, so these are the four cases that
guarded it as a command, driving the script's `main()` instead.
"""

from __future__ import annotations

import importlib.util
import json
from datetime import date
from pathlib import Path

import pytest

from cnequity.config.bootstrap import path_for_toml
from cnequity.orchestrator.engine import JobEngine

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "run_catchup.py"


@pytest.fixture
def catchup():
    """Load the script as a module so its internals can be monkeypatched."""
    spec = importlib.util.spec_from_file_location("run_catchup_script", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_config(tmp_path, *extra_groups: str) -> str:
    cfg_path = tmp_path / "cnequity.toml"
    cfg_path.write_text(
        f"""
[data]
root = "{path_for_toml(tmp_path / "data")}"

[tdx_protocol]
allow_mock = true

[job.init.phases]
names = ["phase1_reference"]

[job.daily.groups.core]
at = "16:00"
steps = ["compact"]
"""
        + "".join(
            f'\n[job.daily.groups.{name}]\nat = "16:30"\nsteps = ["compact"]\n'
            for name in extra_groups
        )
    )
    return str(cfg_path)


def _always_trading(monkeypatch):
    monkeypatch.setattr("cnequity.steps.common.is_trading_day", lambda cfg, d: True)


def test_core_runs_then_market_breadth(tmp_path, monkeypatch, catchup, capsys):
    cfg_path = _write_config(tmp_path)
    calls: list[tuple] = []

    def fake_run_job(self, job_name, trade_date=None, **kwargs):
        calls.append(
            (job_name, trade_date, [s for w in kwargs.get("waves") or [] for s in w.steps])
        )
        return {"run_id": f"r-{len(calls)}", "status": "success", "results": []}

    monkeypatch.setattr(JobEngine, "run_job", fake_run_job)
    _always_trading(monkeypatch)

    assert catchup.main(["--config", cfg_path, "--trade-date", "2026-07-17"]) == 0

    assert calls[0][0] == "daily:core"
    assert calls[0][1] == date(2026, 7, 17)
    assert calls[1][0] == "daily:market_breadth"
    assert "market_breadth" in calls[1][2] and "compact" in calls[1][2]


def test_a_fresh_gate_runs_nothing(tmp_path, monkeypatch, catchup, capsys):
    cfg_path = _write_config(tmp_path)
    calls: list[str] = []

    def fake_run_job(self, job_name, trade_date=None, **kwargs):
        calls.append(job_name)
        return {"run_id": "x", "status": "success", "results": []}

    monkeypatch.setattr(JobEngine, "run_job", fake_run_job)
    _always_trading(monkeypatch)
    monkeypatch.setattr(catchup, "dataset_watermark", lambda cfg, name: date(2026, 7, 17))

    assert catchup.main(["--config", cfg_path, "--trade-date", "2026-07-17"]) == 0

    assert calls == []
    assert "skipped_already_fresh" in capsys.readouterr().out


def test_core_only_skips_market_breadth_when_already_fresh(tmp_path, monkeypatch, catchup, capsys):
    """`--core-only` must not reach the engine at all once bars and adj are at
    the target date — the engine here fails loudly if it is called."""
    cfg_path = _write_config(tmp_path)

    class FakeEngine:
        def __init__(self, cfg):
            self.cfg = cfg

        def run_job(self, *a, **k):
            raise AssertionError("should skip when already fresh")

    _always_trading(monkeypatch)
    monkeypatch.setattr(catchup, "JobEngine", FakeEngine)
    monkeypatch.setattr(catchup, "dataset_watermark", lambda cfg, name: date(2026, 7, 17))

    exit_code = catchup.main(["--config", cfg_path, "--trade-date", "2026-07-17", "--core-only"])

    assert exit_code == 0
    assert "skipped_already_fresh" in capsys.readouterr().out


def test_an_extra_group_failure_stays_advisory(tmp_path, monkeypatch, catchup, capsys):
    """The gate decides the exit code. An extra group is best-effort by
    definition — usually an EastMoney-heavy group on an overseas egress."""
    cfg_path = _write_config(tmp_path, "capital")

    def fake_run_job(self, job_name, trade_date=None, **kwargs):
        status = "failed" if job_name == "daily:capital" else "success"
        return {"run_id": job_name, "status": status, "results": []}

    monkeypatch.setattr(JobEngine, "run_job", fake_run_job)
    _always_trading(monkeypatch)

    exit_code = catchup.main(
        [
            "--config",
            cfg_path,
            "--trade-date",
            "2026-07-17",
            "--extra-group",
            "capital",
        ]
    )

    out = capsys.readouterr().out
    # Two JSON documents are printed: the plan, then the results. The last one
    # is the summary, and it is what has to record the failure.
    summary = json.loads(out[out.rindex('{\n  "core"') :])

    assert exit_code == 0
    assert summary["core"]["status"] == "success"
    assert summary["capital"]["status"] == "failed"


def test_a_core_failure_fails_the_run(tmp_path, monkeypatch, catchup, capsys):
    cfg_path = _write_config(tmp_path)

    def fake_run_job(self, job_name, trade_date=None, **kwargs):
        return {"run_id": job_name, "status": "failed", "results": []}

    monkeypatch.setattr(JobEngine, "run_job", fake_run_job)
    _always_trading(monkeypatch)

    assert catchup.main(["--config", cfg_path, "--trade-date", "2026-07-17"]) == 1


def test_a_non_trading_date_is_refused(tmp_path, monkeypatch, catchup, capsys):
    cfg_path = _write_config(tmp_path)
    monkeypatch.setattr("cnequity.steps.common.is_trading_day", lambda cfg, d: False)

    assert catchup.main(["--config", cfg_path, "--trade-date", "2026-07-18"]) == 1
    assert "not a trading day" in capsys.readouterr().err
