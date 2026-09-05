"""`cne run daily --stale-only` — the same-day second attempt.

The gap it closes: a ``snapshot`` dataset fetches only the run day, so an outage
during the one scheduled window loses that day for good. valuation_metrics lost
2026-07-30 and 07-31 to a push2 clist outage; per-host retries already existed
and were exhausted, and no later run could have replayed a snapshot.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest
from click.testing import CliRunner

from cnequity.cli.main import cli
from cnequity.cli.run_cmds import stale_fetch_steps

ANCHOR = date(2026, 7, 31)
BEHIND = date(2026, 7, 29)
FETCHED = datetime(2026, 7, 31, 12, 0, tzinfo=timezone.utc)


def _write(config, dataset: str, day: date) -> None:
    part = config.curated_root / dataset / f"trade_date={day}"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [day],
            "source": ["tdx_protocol"],
            "data_version": ["v1"],
            "fetched_at": [FETCHED],
        }
    ).write_parquet(part / "part-0.parquet")


def _watermark(config, dataset: str, day: date) -> None:
    from cnequity.storage.state import StateStore

    StateStore(config.meta_root).set_date(dataset, day)


def _snapshot_capture(config, dataset: str, day: date) -> None:
    from cnequity.storage.state import StateStore

    StateStore(config.meta_root).set_date(dataset, day, field="last_snapshot_date")


@pytest.fixture
def lake(config):
    _write(config, "daily_bars", ANCHOR)
    _watermark(config, "daily_bars", ANCHOR)
    _write(config, "valuation_metrics", BEHIND)
    _watermark(config, "valuation_metrics", BEHIND)
    return config


def test_only_datasets_behind_the_anchor_are_selected(lake):
    steps = stale_fetch_steps(lake, ANCHOR)
    assert "valuation_metrics" in steps
    assert "daily_bars" not in steps


def test_a_feed_the_events_job_owns_is_left_to_that_job(lake):
    """Two jobs, two locks: the daily stale pass must not re-fetch an event feed."""
    from cnequity.config.loader import ScheduleGroup

    part = lake.curated_root / "announcement_index" / f"announce_date={BEHIND}"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "announcement_id": ["A-1"],
            "symbol": ["600519.SH"],
            "announce_date": [BEHIND],
            "source": ["cninfo"],
            "data_version": ["v1"],
            "fetched_at": [FETCHED],
        }
    ).write_parquet(part / "part-0.parquet")
    _watermark(lake, "announcement_index", BEHIND)

    assert "announcement_index" in stale_fetch_steps(lake, ANCHOR)

    lake.events_groups = {
        "disclosures": ScheduleGroup(at="20:00", steps=["announcement_index", "compact"])
    }
    assert "announcement_index" not in stale_fetch_steps(lake, ANCHOR)


def test_derived_datasets_are_left_to_cne_derive(lake):
    """Re-fetching is not what a computed dataset needs."""
    _write(lake, "adj_factors", BEHIND)
    _watermark(lake, "adj_factors", BEHIND)
    assert "adj_factors" not in stale_fetch_steps(lake, ANCHOR)


def test_datasets_with_no_data_are_not_called_stale(lake):
    """An empty opt-in dataset is not behind; it was never started."""
    assert "minute_bars" not in stale_fetch_steps(lake, ANCHOR)


def test_per_dataset_tolerance_is_respected(lake):
    """margin_trading is T+1 tolerant; a one-day lag is not stale."""
    _write(lake, "margin_trading", date(2026, 7, 30))
    _watermark(lake, "margin_trading", date(2026, 7, 30))
    assert "margin_trading" not in stale_fetch_steps(lake, ANCHOR)


def test_nothing_stale_is_a_clean_no_op(config, monkeypatch):
    """Safe on a timer: it must not create a run when there is nothing to do."""
    _write(config, "daily_bars", ANCHOR)
    _watermark(config, "daily_bars", ANCHOR)
    # Watermarkless rolling snapshots are considered stale until a capture
    # marker proves that their finite window was observed.
    _snapshot_capture(config, "economic_calendar", ANCHOR)
    _snapshot_capture(config, "share_unlock_schedule", ANCHOR)
    monkeypatch.setattr("cnequity.cli.run_cmds._last_trading_day", lambda cfg, today: ANCHOR)

    result = CliRunner().invoke(
        cli, ["run", "daily", "--stale-only", "--config", str(config.config_path)]
    )
    assert result.exit_code == 0
    assert "nothing stale" in result.output


def test_stale_only_refuses_to_be_combined_with_group(config):
    result = CliRunner().invoke(
        cli,
        ["run", "daily", "--stale-only", "--group", "core", "--config", str(config.config_path)],
    )
    assert result.exit_code != 0
    assert "--group" in result.output
