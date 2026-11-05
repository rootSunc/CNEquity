"""Per-group fetch cadence: weekly groups rest their history datasets, never snapshots."""

from __future__ import annotations

from datetime import date

import pytest

import cnequity.steps  # noqa: F401 — registers steps (dependencies for due_steps)
from cnequity.config import Config, load_config
from cnequity.config.bootstrap import path_for_toml
from cnequity.config.loader import ScheduleGroup, validate_config
from cnequity.orchestrator.cadence import due_steps, freshness_anchor, group_due, last_due_date
from cnequity.quality.freshness import is_stale

_WEEKLY = ScheduleGroup(at="17:30", steps=[], cadence="weekly")


@pytest.mark.parametrize(
    ("day", "due"),
    [
        (date(2026, 9, 17), False),  # ordinary Thursday
        (date(2026, 9, 18), True),  # ordinary Friday
        (date(2026, 9, 24), True),  # Thursday before the 9-25 Mid-Autumn close
        (date(2026, 9, 30), True),  # Wednesday before the National Day closes
        (date(2026, 10, 8), False),
        (date(2026, 10, 9), True),
        (date(2026, 9, 19), False),  # Saturday
    ],
)
def test_a_weekly_group_is_due_on_the_last_session_of_its_week(day, due):
    assert group_due(_WEEKLY, day) is due


def test_a_daily_group_is_always_due():
    assert group_due(ScheduleGroup(at="16:00", steps=[]), date(2026, 9, 17))


def test_weekday_moves_the_due_session_earlier():
    wednesday = ScheduleGroup(at="17:30", steps=[], cadence="weekly", weekday=3)
    assert group_due(wednesday, date(2026, 9, 16))
    assert not group_due(wednesday, date(2026, 9, 18))


def test_last_due_date_walks_back_to_the_previous_run():
    assert last_due_date(_WEEKLY, date(2026, 9, 23)) == date(2026, 9, 18)
    assert last_due_date(_WEEKLY, date(2026, 9, 24)) == date(2026, 9, 24)


def test_off_days_keep_only_snapshots_and_their_dependencies():
    core = ScheduleGroup(
        at="16:00",
        steps=[
            "instruments",
            "trading_calendar",
            "trading_status",
            "corporate_actions",
            "daily_bars",
            "compact",
            "derive_adj_factors",
        ],
        cadence="weekly",
    )
    # trading_status is a snapshot and needs that session's instruments.
    assert due_steps(core, date(2026, 9, 17)) == ["instruments", "trading_status", "compact"]
    assert due_steps(core, date(2026, 9, 18)) == core.steps


def test_an_all_history_group_runs_nothing_on_its_off_days():
    signals = ScheduleGroup(
        at="17:00", steps=["dragon_tiger", "block_trades", "compact"], cadence="weekly"
    )
    assert due_steps(signals, date(2026, 9, 17)) == []


def _cfg(tmp_path, groups: dict[str, ScheduleGroup]) -> Config:
    cfg = Config(data_root=tmp_path / "data")
    cfg.schedule_groups = groups
    return cfg


def test_a_weekly_history_dataset_is_not_stale_between_runs(tmp_path):
    cfg = _cfg(
        tmp_path, {"signals": ScheduleGroup(at="17:00", steps=["dragon_tiger"], cadence="weekly")}
    )
    # Fetched through Friday 9-18; on Wednesday 9-23 it is due through 9-18 only.
    assert freshness_anchor(cfg, "dragon_tiger", date(2026, 9, 23)) == date(2026, 9, 18)
    assert not is_stale("dragon_tiger", date(2026, 9, 18), date(2026, 9, 23), cfg)
    # Without the cadence it would be three sessions behind.
    assert is_stale("dragon_tiger", date(2026, 9, 18), date(2026, 9, 23))
    # A missed weekly run is still stale the week after.
    assert is_stale("dragon_tiger", date(2026, 9, 11), date(2026, 9, 23), cfg)


def test_a_dataset_also_in_a_daily_group_keeps_the_daily_anchor(tmp_path):
    cfg = _cfg(
        tmp_path,
        {
            "weekly": ScheduleGroup(at="17:00", steps=["dragon_tiger"], cadence="weekly"),
            "daily": ScheduleGroup(at="16:00", steps=["dragon_tiger"]),
        },
    )
    assert freshness_anchor(cfg, "dragon_tiger", date(2026, 9, 23)) == date(2026, 9, 23)


def test_a_snapshot_in_a_weekly_group_keeps_the_daily_anchor(tmp_path):
    cfg = _cfg(
        tmp_path, {"capital": ScheduleGroup(at="16:30", steps=["fund_flow"], cadence="weekly")}
    )
    assert freshness_anchor(cfg, "fund_flow", date(2026, 9, 23)) == date(2026, 9, 23)


def test_config_reads_and_validates_cadence(tmp_path):
    path = tmp_path / "cnequity.toml"
    path.write_text(
        f"""[data]
root = "{path_for_toml(tmp_path / "data")}"
[job.daily.groups.fundamentals]
at = "17:30"
cadence = "weekly"
weekday = 4
steps = ["financial_statement_items", "compact"]
[job.daily.groups.signals]
at = "17:00"
steps = ["dragon_tiger", "compact"]
""",
        encoding="utf-8",
    )
    cfg = load_config(path)
    assert (
        cfg.schedule_groups["fundamentals"].cadence,
        cfg.schedule_groups["fundamentals"].weekday,
    ) == ("weekly", 4)
    assert cfg.schedule_groups["signals"].cadence == "daily"
    assert not [e for e in validate_config(cfg) if "cadence" in e or "weekday" in e]

    cfg.schedule_groups["signals"].cadence = "monthly"
    cfg.schedule_groups["fundamentals"].weekday = 9
    errors = validate_config(cfg)
    assert any("signals.cadence" in e for e in errors)
    assert any("fundamentals.weekday" in e for e in errors)


def test_rolling_derivative_minutes_capture_every_session_even_in_weekly_group(tmp_path):
    group = ScheduleGroup(
        at="18:30", steps=["futures_bars", "futures_minute_bars", "compact"], cadence="weekly"
    )
    day = date(2026, 9, 17)
    assert due_steps(group, day) == ["futures_minute_bars", "compact"]
    assert (
        freshness_anchor(_cfg(tmp_path, {"derivatives": group}), "futures_minute_bars", day) == day
    )
