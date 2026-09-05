"""The 7x24 event streams run as their own job.

Disclosures and news are published on weekends and holidays, but they used to
be scheduled inside `daily` schedule groups: the trading-day gate skipped the
whole job on the days they were still publishing, and every group shared one
ingestion lock with the evening batch, so a frequent event sweep could only run
in the gaps the heavy groups left. `events` is a separate family — no calendar
gate, its own lock — and config validation keeps it honest.
"""

from __future__ import annotations

from datetime import date

import pytest

import cnequity.steps  # noqa: F401 — registers the steps used below
from cnequity.config import Config, WaveConfig
from cnequity.config.loader import ScheduleGroup, validate_config
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.run_lock import (
    DAILY_INGESTION_LOCK,
    EVENTS_INGESTION_LOCK,
    RunLockError,
    run_lock,
)
from cnequity.storage.layout import init_data_layout

SUNDAY = date(2024, 6, 30)
DISCLOSURES = WaveConfig(
    name="events:disclosures", parallel=True, steps=["announcement_index", "compact"]
)


def _engine(tmp_path) -> JobEngine:
    cfg = Config(data_root=tmp_path / "data", tdx_allow_mock=True)
    init_data_layout(cfg)
    return JobEngine(cfg)


def _stub_steps(engine: JobEngine, monkeypatch) -> list[str]:
    ran: list[str] = []

    def fake_run_step(name, trade_date, run_id, context, *, retry_of=None):
        ran.append(name)
        engine.manifest.start_batch(run_id, f"b-{name}", name, name)
        engine.manifest.finish_batch(run_id, f"b-{name}", "success")
        return {"step": name, "status": "success", "rows_read": 0, "rows_written": 0}

    monkeypatch.setattr(engine, "_run_step", fake_run_step)
    return ran


def test_an_events_job_ingests_on_a_day_the_market_was_closed(tmp_path, monkeypatch):
    engine = _engine(tmp_path)
    ran = _stub_steps(engine, monkeypatch)

    result = engine.run_job("events:disclosures", SUNDAY, waves=[DISCLOSURES])

    assert result["status"] == "success"
    assert ran == ["announcement_index", "compact"]


def test_a_daily_job_is_still_gated_on_the_trading_calendar(tmp_path, monkeypatch):
    engine = _engine(tmp_path)
    ran = _stub_steps(engine, monkeypatch)

    result = engine.run_job(
        "daily:capital",
        SUNDAY,
        waves=[WaveConfig(name="group:capital", parallel=True, steps=["fund_flow"])],
    )

    assert result["status"] == "skipped_non_trading_day"
    assert ran == []


def test_the_evening_batch_does_not_block_an_event_sweep(tmp_path, monkeypatch):
    engine = _engine(tmp_path)
    _stub_steps(engine, monkeypatch)

    with run_lock(engine.config.meta_root, DAILY_INGESTION_LOCK):
        result = engine.run_job("events:disclosures", SUNDAY, waves=[DISCLOSURES])

    assert result["status"] == "success"


def test_two_event_sweeps_do_not_overlap_each_other(tmp_path, monkeypatch):
    engine = _engine(tmp_path)
    _stub_steps(engine, monkeypatch)

    with run_lock(engine.config.meta_root, EVENTS_INGESTION_LOCK):
        with pytest.raises(RunLockError, match="Another events group is still running"):
            engine.run_job("events:disclosures", SUNDAY, waves=[DISCLOSURES])


def test_a_session_dataset_cannot_be_scheduled_as_an_event_stream(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    cfg.daily_waves = [WaveConfig(name="reference", parallel=True, steps=["instruments"])]
    cfg.events_groups = {"bad": ScheduleGroup(at="20:00", steps=["daily_bars", "compact"])}

    errors = [e for e in validate_config(cfg) if "events group" in e]

    assert len(errors) == 1
    assert "daily_bars" in errors[0] and "trading-session dataset" in errors[0]


def test_a_feed_cannot_be_scheduled_in_both_jobs(tmp_path):
    """Different locks, so both jobs could ingest it into the same run window."""
    cfg = Config(data_root=tmp_path / "data")
    cfg.daily_waves = [WaveConfig(name="reference", parallel=True, steps=["instruments"])]
    cfg.schedule_groups = {
        "capital": ScheduleGroup(at="17:00", steps=["announcement_index", "compact"])
    }
    cfg.events_groups = {
        "disclosures": ScheduleGroup(at="20:00", steps=["announcement_index", "compact"])
    }

    errors = [e for e in validate_config(cfg) if "events group" in e]

    assert len(errors) == 1
    assert "also scheduled in the daily job" in errors[0]


def test_the_shipped_config_schedules_every_calendar_feed_as_an_event_stream(tmp_path):
    import sys
    from pathlib import Path
    from unittest.mock import patch

    from cnequity.config import load_config
    from cnequity.domain.datasets import calendar_scope_datasets

    example = Path(__file__).resolve().parents[2] / "configs" / "cnequity.example.toml"
    with patch.object(sys, "platform", "linux"):
        cfg = load_config(example)

    scheduled = {step for group in cfg.events_groups.values() for step in group.steps}
    daily = {step for wave in cfg.daily_waves for step in wave.steps}
    daily |= {step for group in cfg.schedule_groups.values() for step in group.steps}

    assert calendar_scope_datasets() <= scheduled
    assert calendar_scope_datasets() & daily == set()


# --- CLI ---------------------------------------------------------------------


@pytest.fixture
def events_config(tmp_path):
    from cnequity.config import load_config
    from cnequity.config.bootstrap import path_for_toml

    cfg_path = tmp_path / "events.toml"
    cfg_path.write_text(
        f"""
[data]
root = "{path_for_toml(tmp_path / "data")}"

[orchestrator]
workers = 1

[tdx_protocol]
allow_mock = true

[[job.daily.waves]]
name = "reference"
parallel = true
steps = ["instruments"]

[job.events.groups.disclosures]
at = "20:00"
steps = ["announcement_index", "compact"]
""",
        encoding="utf-8",
    )
    load_config(cfg_path)
    return cfg_path


def test_run_events_says_so_when_nothing_is_configured(config):
    from click.testing import CliRunner

    from cnequity.cli.main import cli

    result = CliRunner().invoke(cli, ["run", "events", "--config", str(config.config_path)])

    assert result.exit_code != 0
    assert "[job.events.groups]" in result.output


def test_run_events_names_the_groups_it_knows(events_config):
    from click.testing import CliRunner

    from cnequity.cli.main import cli

    result = CliRunner().invoke(
        cli, ["run", "events", "--group", "nope", "--config", str(events_config)]
    )

    assert result.exit_code != 0
    assert "Unknown events group: nope" in result.output
    assert "disclosures" in result.output


def test_run_events_ingests_on_a_sunday_end_to_end(events_config, monkeypatch):
    """The command the scheduler fires, on a day `cne run daily` refuses."""
    from click.testing import CliRunner

    from cnequity.cli.main import cli

    ran: list[str] = []

    def fake_run_step(self, name, trade_date, run_id, context, *, retry_of=None):
        ran.append(name)
        self.manifest.start_batch(run_id, f"b-{name}", name, name)
        self.manifest.finish_batch(run_id, f"b-{name}", "success")
        return {"step": name, "status": "success", "rows_read": 0, "rows_written": 0}

    monkeypatch.setattr(JobEngine, "_run_step", fake_run_step)
    result = CliRunner().invoke(
        cli,
        [
            "run",
            "events",
            "--trade-date",
            SUNDAY.isoformat(),
            "--config",
            str(events_config),
        ],
    )

    assert result.exit_code == 0, result.output
    assert '"status": "success"' in result.output
    assert ran == ["announcement_index", "compact"]
