"""`cne backfill` input errors that the code comments say once cost real work.

``backfill_cmds.backfill`` carries two guards, each with a comment describing
the regression it fixed, and neither has a test:

* transposed ``--start``/``--end`` "used to cost a full network sweep: the walk
  had no days in it, the step raised, the engine logged the traceback, and the
  command still printed status=success with rows_written=0";
* ``parse_date_option`` exists because ``date.fromisoformat`` at thirteen call
  sites "let that reach the operator as a Python traceback — for something as
  ordinary as a typo in ``--start``".

Both must refuse before an engine exists, so these tests make constructing one
an error that would surface as an unhandled exception.
"""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from cnequity.cli import backfill_cmds
from cnequity.cli.main import cli
from cnequity.config.bootstrap import path_for_toml


def _write_config(tmp_path) -> str:
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
""",
        encoding="utf-8",
    )
    return str(cfg_path)


@pytest.fixture
def engines(monkeypatch):
    """Record any attempt to build an engine; the guards must run first."""
    built: list[object] = []

    class _NoEngine:
        def __init__(self, cfg, *args, **kwargs):
            built.append(cfg)
            raise AssertionError("backfill built an engine for a request it should refuse")

    monkeypatch.setattr(backfill_cmds, "JobEngine", _NoEngine)
    return built


@pytest.mark.parametrize("dataset", ["market_breadth", "daily_bars", "margin_trading"])
def test_transposed_dates_are_refused_before_any_work(tmp_path, engines, dataset):
    result = CliRunner().invoke(
        cli,
        [
            "backfill",
            dataset,
            "--start",
            "2026-06-10",
            "--end",
            "2026-06-01",
            "--config",
            _write_config(tmp_path),
        ],
    )

    assert engines == []
    assert result.exit_code == 1, result.output
    assert "--start must be on or before --end" in result.output
    assert "Traceback" not in result.output
    assert '"status": "success"' not in result.output
    assert not (tmp_path / "data" / "staging").exists() or not any(
        (tmp_path / "data" / "staging").rglob("*.parquet")
    )


@pytest.mark.parametrize(
    ("flag", "value"),
    [("--start", "2026-13-01"), ("--end", "yesterday"), ("--start", "2026/06/01")],
)
def test_a_malformed_date_is_a_usage_error_not_a_traceback(tmp_path, engines, flag, value):
    other = "--end" if flag == "--start" else "--start"
    result = CliRunner().invoke(
        cli,
        [
            "backfill",
            "market_breadth",
            flag,
            value,
            other,
            "2026-06-01",
            "--config",
            _write_config(tmp_path),
        ],
    )

    assert engines == []
    assert result.exit_code == 2, result.output  # Click's usage-error code
    assert "is not an ISO date" in result.output
    assert value in result.output
    assert flag in result.output
    assert "Traceback" not in result.output
