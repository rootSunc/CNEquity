"""`cml init --profile quick` — the shallow first backfill, and what keeps it honest."""

from __future__ import annotations

from datetime import date

import pytest
from click.testing import CliRunner

from cn_market_lake.cli.main import QUICK_PROFILE_YEARS, _init_history_start, cli
from cn_market_lake.config import Config
from cn_market_lake.config.bootstrap import path_for_toml
from cn_market_lake.orchestrator.engine import JobEngine
from cn_market_lake.orchestrator.worker_pool import _window_backfill
from cn_market_lake.steps.common import BACKFILL_START


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


# --- window selection ------------------------------------------------------


def test_full_profile_leaves_each_step_its_own_default():
    """None, not BACKFILL_START: index_constituents and industry_members have
    their own floors, and pinning them all to 2016 would widen those sweeps."""
    assert _init_history_start("full", None, date(2026, 8, 2)) is None


def test_quick_profile_is_three_calendar_years():
    today = date(2026, 8, 2)
    assert _init_history_start("quick", None, today) == today.replace(
        year=today.year - QUICK_PROFILE_YEARS
    )


def test_quick_profile_survives_a_leap_day():
    start = _init_history_start("quick", None, date(2028, 2, 29))
    assert start == date(2025, 3, 1)


def test_since_overrides_the_profile():
    assert _init_history_start("quick", "2019-01-01", date(2026, 8, 2)) == date(2019, 1, 1)


# --- the CLI wiring --------------------------------------------------------


def _capture_backfill_start(tmp_path, monkeypatch, args: list[str]):
    seen: dict = {}

    def fake_run_init_phases(self, trade_date=None, **kwargs):
        seen["start"] = getattr(self.config, "_backfill_start", None)
        return {"run_id": "init-run", "status": "success", "phases": []}

    monkeypatch.setattr(JobEngine, "run_init_phases", fake_run_init_phases)
    result = CliRunner().invoke(cli, ["init", "--config", _write_config(tmp_path), *args])
    assert result.exit_code == 0, result.output
    return seen.get("start"), result.output


def test_quick_profile_reaches_the_engine(tmp_path, monkeypatch):
    start, output = _capture_backfill_start(
        tmp_path, monkeypatch, ["--profile", "quick", "--trade-date", "2026-08-02"]
    )
    assert start == date(2023, 8, 2)
    assert "2023-08-02" in output


def test_default_init_is_the_shallow_window(tmp_path, monkeypatch):
    """A bare `cml init` takes the quick window — a usable lake on the first run.

    Measured per 10 symbols on one connection: 3 years ~4.8s against ~15.1s for
    everything from 2001. Going shallower than that buys little (1 year ~3.9s,
    the per-symbol round trip dominating once the window is short) and costs
    the multi-year windows factor work needs, so `quick` is the floor, not 1y.
    """
    # Derived, not hardcoded: a bare `cml init` anchors on today, so a literal
    # date here passes only on the day it was written and fails at the next
    # midnight for reasons that have nothing to do with the behaviour under test.
    today = date.today()
    try:
        expected = today.replace(year=today.year - QUICK_PROFILE_YEARS)
    except ValueError:  # Feb 29 — same fallback the CLI uses
        expected = date(today.year - QUICK_PROFILE_YEARS, 3, 1)

    start, output = _capture_backfill_start(tmp_path, monkeypatch, [])
    assert start == expected
    assert "History window" in output
    assert "cml backfill daily_bars" in output, "must say how to deepen"


def test_full_profile_still_takes_everything(tmp_path, monkeypatch):
    start, output = _capture_backfill_start(tmp_path, monkeypatch, ["--profile", "full"])
    assert start is None
    assert "History window" not in output


def test_quick_profile_prints_how_to_deepen(tmp_path, monkeypatch):
    _, output = _capture_backfill_start(tmp_path, monkeypatch, ["--profile", "quick"])
    assert "cml backfill daily_bars" in output


# --- the two traps a shallower window opens --------------------------------


def test_shallow_window_stays_strict_about_pagination_failures(tmp_path):
    """A backfill raises on a mid-pagination failure instead of keeping the pages
    that arrived. That used to be inferred from `start == 2016-01-01`, which a
    quick init does not match — inferring it would make the shallow path the
    lenient one, and lose a symbol's older years without saying so."""
    cfg = Config(data_root=tmp_path / "data")
    cfg._backfill = True
    assert _window_backfill(cfg, date(2023, 8, 2)) is True


def test_daily_window_is_still_lenient(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    assert _window_backfill(cfg, date(2026, 8, 2)) is False
    # The historical sentinel keeps working for callers that never set the flag.
    assert _window_backfill(cfg, BACKFILL_START) is True


def test_resume_reuses_the_original_window(tmp_path, monkeypatch):
    """A resume runs from a fresh process days later. Without the window on the
    run record it would default to full depth and fetch years the operator
    deliberately skipped."""
    from cn_market_lake.orchestrator.manifest import Manifest

    cfg = Config(data_root=tmp_path / "data")
    cfg.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run(
        "init",
        {
            "phases": ["phase1_reference"],
            "trade_date": "2026-08-02",
            "history_start": "2023-08-02",
        },
    )

    engine = JobEngine(cfg)
    monkeypatch.setattr(
        JobEngine, "_retry_run", lambda self, *a, **k: {"rows_read": 0, "rows_written": 0}
    )
    monkeypatch.setattr(
        JobEngine, "run_job", lambda self, *a, **k: {"status": "success", "results": []}
    )
    monkeypatch.setattr(JobEngine, "_finalize_init_run", lambda self, *a, **k: "success")

    engine.resume_init(date(2026, 8, 5), run_id=run_id)
    assert engine.config._backfill_start == date(2023, 8, 2)


def test_resume_does_not_override_an_explicit_window(tmp_path, monkeypatch):
    from cn_market_lake.orchestrator.manifest import Manifest

    cfg = Config(data_root=tmp_path / "data")
    cfg.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    run_id = Manifest(cfg.manifest_path).start_run(
        "init", {"phases": ["phase1_reference"], "history_start": "2023-08-02"}
    )
    cfg._backfill_start = date(2016, 1, 1)

    engine = JobEngine(cfg)
    monkeypatch.setattr(
        JobEngine, "_retry_run", lambda self, *a, **k: {"rows_read": 0, "rows_written": 0}
    )
    monkeypatch.setattr(
        JobEngine, "run_job", lambda self, *a, **k: {"status": "success", "results": []}
    )
    monkeypatch.setattr(JobEngine, "_finalize_init_run", lambda self, *a, **k: "success")

    engine.resume_init(date(2026, 8, 5), run_id=run_id)
    assert engine.config._backfill_start == date(2016, 1, 1)


def test_init_records_the_window_on_the_run(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    cfg.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    cfg._backfill_start = date(2023, 8, 2)
    engine = JobEngine(cfg)

    captured: dict = {}

    def fake_execute(self, run_id, trade_date, phases, **kwargs):
        captured["meta"] = self.manifest.get_run_metadata(run_id)
        return {"run_id": run_id, "status": "success", "phases": []}

    monkeypatch.setattr(JobEngine, "_execute_init_phases", fake_execute)
    engine.run_init_phases(trade_date=date(2026, 8, 2))
    assert captured["meta"]["history_start"] == "2023-08-02"


@pytest.mark.parametrize("profile", ["full", "quick"])
def test_every_profile_keeps_the_full_cross_section(profile, tmp_path, monkeypatch):
    """Shallower, never narrower. A symbol filter here would bake in the exact
    survivorship bias `cml delisted backfill` exists to repair."""
    start, _ = _capture_backfill_start(tmp_path, monkeypatch, ["--profile", profile])
    cfg = Config(data_root=tmp_path / "data")
    if start:
        cfg._backfill_start = start
    assert getattr(cfg, "_scope_symbols", None) is None
    assert getattr(cfg, "_backfill_symbols", None) is None
