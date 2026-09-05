"""`scripts/delisted_ops.py` — the four `cne delisted` subcommands that moved out.

Rebuilding the delisted universe is a one-off project: an hours-long resumable
sweep, corrections applied once, then a gate you check afterwards. `status` and
`backfill` stayed in the CLI because they have a routine shape; these four did
not. The behaviour has to survive the move, so these are the cases that guarded
them as commands.

The step functions are imported inside each subcommand, so patching
`cnequity.steps.delisted.*` still reaches them; `JobEngine` and
`ensure_duckdb_views` are module-level and are patched on the script.
"""

from __future__ import annotations

import importlib.util
import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from cnequity.config.bootstrap import path_for_toml

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "delisted_ops.py"


@pytest.fixture
def ops():
    spec = importlib.util.spec_from_file_location("delisted_ops_script", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def cfg_path(tmp_path):
    path = tmp_path / "cnequity.toml"
    path.write_text(
        f'[data]\nroot = "{path_for_toml(tmp_path / "data")}"\n\n'
        "[tdx_protocol]\nallow_mock = true\n",
        encoding="utf-8",
    )
    return str(path)


def test_discover_reports_the_sweep_without_concluding_from_failures(
    ops, cfg_path, monkeypatch, capsys
):
    monkeypatch.setattr(
        "cnequity.steps.delisted.discover_delisted",
        lambda cfg, limit=None: SimpleNamespace(
            probed=10, delisted=2, never_issued=5, failed=["x"], remaining=100, complete=False
        ),
    )

    assert ops.main(["--config", cfg_path, "discover", "--limit", "10"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["probed"] == 10
    # A failed probe is counted separately and leaves the sweep incomplete —
    # it must never be filed as "this code never existed".
    assert payload["failed"] == 1
    assert payload["complete"] is False


def test_coverage_exit_status_is_the_gate(ops, cfg_path, monkeypatch, capsys):
    report = {"window": {"start": "2020-01-01", "end": "2024-12-31"}, "verified": True}
    monkeypatch.setattr(
        "cnequity.steps.delisted.delisted_coverage_report",
        lambda cfg, start, end, sample=15, **kwargs: report,
    )

    ok = ops.main(
        ["--config", cfg_path, "coverage", "--start", "2020-01-01", "--end", "2024-12-31"]
    )
    assert ok == 0
    assert json.loads(capsys.readouterr().out)["verified"] is True

    report["verified"] = False
    assert ops.main(["--config", cfg_path, "coverage", "--start", "2020-01-01"]) == 1
    assert json.loads(capsys.readouterr().out)["verified"] is False


def test_coverage_passes_the_research_universe_through(ops, cfg_path, monkeypatch, capsys):
    observed: dict[str, object] = {}

    def _coverage(_cfg, start, end, *, sample, universe):
        observed.update(start=start, end=end, sample=sample, universe=universe)
        return {"verified": True, "universe": universe}

    monkeypatch.setattr("cnequity.steps.delisted.delisted_coverage_report", _coverage)

    exit_code = ops.main(
        ["--config", cfg_path, "coverage", "--start", "2020-01-01", "--universe", "all_a_sh_sz"]
    )

    assert exit_code == 0
    assert observed["universe"] == "all_a_sh_sz"
    assert observed["start"] == date(2020, 1, 1)


def test_coverage_refuses_a_backwards_window(ops, cfg_path, capsys):
    exit_code = ops.main(
        ["--config", cfg_path, "coverage", "--start", "2024-01-01", "--end", "2020-01-01"]
    )

    assert exit_code == 1
    assert "on or before" in capsys.readouterr().err


def test_reconcile_is_dry_run_unless_apply_is_explicit(ops, cfg_path, monkeypatch, capsys):
    seen: list[str] = []
    report = {"read_only": True, "counts": {"safe_correction": 1}}
    monkeypatch.setattr(
        "cnequity.steps.delisted.delisted_catalog_reconciliation_report",
        lambda cfg, sample=15: seen.append("dry-run") or report,
    )
    monkeypatch.setattr(
        "cnequity.steps.delisted.reconcile_delisted_catalog",
        lambda cfg, sample=15: seen.append("apply") or {**report, "read_only": False},
    )

    assert ops.main(["--config", cfg_path, "reconcile"]) == 0
    assert seen == ["dry-run"]

    assert ops.main(["--config", cfg_path, "reconcile", "--apply"]) == 0
    assert seen == ["dry-run", "apply"]


def test_reconcile_reports_a_refusal_rather_than_raising(ops, cfg_path, monkeypatch, capsys):
    """`--apply` refuses during an active ingestion; that is a message, not a
    traceback."""

    def _boom(cfg, sample=15):
        raise RuntimeError("refusing: ingestion run daily:core is active")

    monkeypatch.setattr("cnequity.steps.delisted.reconcile_delisted_catalog", _boom)

    assert ops.main(["--config", cfg_path, "reconcile", "--apply"]) == 1
    assert "is active" in capsys.readouterr().err


def _fake_engine(monkeypatch, ops, manifest):
    class FakeEngine:
        def __init__(self, cfg):
            self.manifest = manifest

        def run_step(self, name, trade_date, run_id):
            return {"rows_written": 1, "status": "success"}

    monkeypatch.setattr(ops, "JobEngine", FakeEngine)
    monkeypatch.setattr(ops, "ensure_duckdb_views", lambda cfg: Path("/tmp/x"))


def test_repair_records_a_run_and_rebuilds_the_views(ops, cfg_path, monkeypatch, capsys):
    class FakeManifest:
        def start_run(self, *a, **k):
            return "repair-1"

        def finish_run(self, *a, **k):
            return None

    _fake_engine(monkeypatch, ops, FakeManifest())
    monkeypatch.setattr(
        "cnequity.steps.delisted.repair_delisted_instruments",
        lambda cfg, run_id, start=None: {"rows_written": 3, "updated": 3},
    )
    monkeypatch.setattr("cnequity.steps.delisted.purge_subscription_placeholders", lambda cfg: 1)

    assert ops.main(["--config", cfg_path, "repair"]) == 0
    assert "repair-1" in capsys.readouterr().out


def test_repair_surfaces_incomplete_bars(ops, cfg_path, monkeypatch, capsys):
    """A partial repair must not read as success: it leaves `universe=all_a`
    still selecting some dead names."""

    class FakeManifest:
        def __init__(self):
            self.finished = None

        def start_run(self, *a, **k):
            return "repair-warning"

        def finish_run(self, *args, **kwargs):
            self.finished = (args, kwargs)

    manifest = FakeManifest()
    _fake_engine(monkeypatch, ops, manifest)
    monkeypatch.setattr(
        "cnequity.steps.delisted.repair_delisted_instruments",
        lambda cfg, run_id, start=None: {
            "rows_written": 1,
            "still_need_bars": ["600071.SH"],
            "status": "warning",
        },
    )
    monkeypatch.setattr("cnequity.steps.delisted.purge_subscription_placeholders", lambda cfg: 0)

    assert ops.main(["--config", cfg_path, "repair"]) == 1

    captured = capsys.readouterr()
    assert '"status": "warning"' in captured.out
    assert manifest.finished[0][1] == "warning"


def test_repair_does_not_let_a_degraded_step_read_as_success(ops, cfg_path, monkeypatch, capsys):
    """The merge listed only the spellings seen at the time.

    That is how `cne backfill` came to report success for a sweep whose every
    slice had failed — `degraded` fell past the elif onto the success branch.
    """

    class FakeManifest:
        def __init__(self):
            self.finished = None

        def start_run(self, *a, **k):
            return "repair-degraded"

        def finish_run(self, *args, **kwargs):
            self.finished = (args, kwargs)

    manifest = FakeManifest()
    _fake_engine(monkeypatch, ops, manifest)
    monkeypatch.setattr(
        "cnequity.steps.delisted.repair_delisted_instruments",
        lambda cfg, run_id, start=None: {
            "rows_written": 1,
            "still_need_bars": ["600071.SH"],
            "status": "degraded",
        },
    )
    monkeypatch.setattr("cnequity.steps.delisted.purge_subscription_placeholders", lambda cfg: 0)

    assert ops.main(["--config", cfg_path, "repair"]) == 1
    assert manifest.finished[0][1] == "warning"
