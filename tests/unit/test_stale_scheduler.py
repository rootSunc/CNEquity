"""Deadline/cost ordering and honest repairability for scheduled retries."""

from datetime import date

import polars as pl

import cnequity.steps  # noqa: F401
from cnequity.cli.run_cmds import _repairable_gaps, stale_fetch_plan
from cnequity.domain.datasets import DATASETS
from cnequity.quality.verify import Gap, _backfillable


def test_snapshot_only_stale_work_is_scheduled_before_expensive_bars(monkeypatch, tmp_path):
    cfg = type("Cfg", (), {"data_root": tmp_path, "minute_bars_enabled": False})()
    rows = pl.DataFrame(
        {
            "dataset": ["daily_bars", "fund_flow"],
            "layer": ["curated", "curated"],
            "date_col": ["trade_date", "trade_date"],
            "fetch_semantics": ["by_date", "snapshot"],
            "history_mode": ["by_date", "snapshot_only"],
            "backfill_source": [None, None],
            "pit_quality": ["strict", "snapshot_only"],
            "pit_storage_columns": [[], []],
            "history_horizon_days": [None, None],
            "pit": [False, False],
            "has_data": [True, True],
            "coverage_start": [date(2024, 1, 1), date(2024, 1, 1)],
            "coverage_end": [date(2024, 1, 1), date(2024, 1, 1)],
            "watermarked": [True, True],
            "watermark": [date(2024, 1, 1), date(2024, 1, 1)],
            "revision": [None, None],
            "revision_id": [None, None],
            "schema_version": [None, None],
            "contract_fingerprint": [None, None],
        }
    )
    monkeypatch.setattr("cnequity.query.reader.list_datasets", lambda config=None: rows)

    plan = stale_fetch_plan(cfg, date(2024, 1, 5))

    assert [item["dataset"] for item in plan] == ["fund_flow", "daily_bars"]
    assert plan[0]["deadline"] == "same_day"
    assert plan[0]["history_mode"] == "snapshot_only"
    assert plan[1]["estimated_cost"] > plan[0]["estimated_cost"]


def test_watermarkless_live_snapshots_recover_missing_same_day_capture(monkeypatch, tmp_path):
    """A missed rolling window remains schedulable, including unlock events."""
    cfg = type("Cfg", (), {"data_root": tmp_path, "minute_bars_enabled": False})()
    rows = pl.DataFrame(
        {
            "dataset": ["economic_calendar", "share_unlock_schedule"],
            "layer": ["curated", "curated"],
            "date_col": ["event_date", "unlock_date"],
            "fetch_semantics": ["snapshot", "snapshot"],
            "history_mode": ["snapshot_only", "snapshot_with_backfill"],
            "backfill_source": [None, "eastmoney"],
            "pit_quality": ["snapshot_only", "strict"],
            "pit_storage_columns": [[], []],
            "history_horizon_days": [None, None],
            "pit": [False, False],
            "has_data": [False, True],
            "coverage_start": [None, date(2024, 1, 1)],
            "coverage_end": [None, date(2024, 1, 1)],
            "snapshot_date": [None, date(2024, 1, 1)],
            "watermarked": [False, False],
            "watermark": [None, None],
            "revision": [None, None],
            "revision_id": [None, None],
            "schema_version": [None, None],
            "contract_fingerprint": [None, None],
        }
    )
    monkeypatch.setattr("cnequity.query.reader.list_datasets", lambda config=None: rows)

    plan = stale_fetch_plan(cfg, date(2024, 1, 5))

    assert {item["dataset"] for item in plan} == {
        "economic_calendar",
        "share_unlock_schedule",
    }
    assert all(item["deadline"] == "same_day" for item in plan)
    assert all(item["priority"] == 0 for item in plan)


def test_watermarkless_live_snapshot_retries_yesterday_marker_on_same_day(monkeypatch, tmp_path):
    cfg = type("Cfg", (), {"data_root": tmp_path, "minute_bars_enabled": False})()
    rows = pl.DataFrame(
        {
            "dataset": ["share_unlock_schedule"],
            "layer": ["curated"],
            "date_col": ["unlock_date"],
            "fetch_semantics": ["snapshot"],
            "history_mode": ["snapshot_with_backfill"],
            "backfill_source": ["eastmoney"],
            "pit_quality": ["strict"],
            "pit_storage_columns": [[]],
            "history_horizon_days": [None],
            "pit": [False],
            "has_data": [True],
            "coverage_start": [date(2024, 1, 1)],
            "coverage_end": [date(2024, 1, 4)],
            "snapshot_date": [date(2024, 1, 4)],
            "watermarked": [False],
            "watermark": [None],
            "revision": [None],
            "revision_id": [None],
            "schema_version": [None],
            "contract_fingerprint": [None],
        }
    )
    monkeypatch.setattr("cnequity.query.reader.list_datasets", lambda config=None: rows)

    plan = stale_fetch_plan(cfg, date(2024, 1, 5))

    assert [item["dataset"] for item in plan] == ["share_unlock_schedule"]
    assert plan[0]["deadline"] == "same_day"


def test_snapshot_only_is_never_marked_repairable():
    assert _backfillable(DATASETS["fund_flow"]) is False
    assert _backfillable(DATASETS["valuation_metrics"]) is True


def test_scheduled_gap_repair_filters_snapshot_only(monkeypatch):
    gaps = [
        Gap("fund_flow", "interior", "snapshot", True),
        Gap("valuation_metrics", "interior", "history", True),
        Gap("daily_bars", "interior", "unrepairable", False),
    ]
    monkeypatch.setattr("cnequity.quality.verify.verify_lake", lambda *args, **kwargs: gaps)

    selected = _repairable_gaps(object(), date(2024, 1, 5))

    assert [gap.dataset for gap in selected] == ["valuation_metrics"]


def test_stale_plan_cannot_escape_explicit_host_groups(monkeypatch, tmp_path):
    from cnequity.config import Config, ScheduleGroup

    cfg = Config(
        data_root=tmp_path,
        schedule_groups={
            "core": ScheduleGroup(at="16:00", steps=["daily_bars", "compact"]),
            "capital": ScheduleGroup(at="16:30", steps=["fund_flow", "compact"]),
        },
    )
    rows = pl.DataFrame(
        {
            "dataset": ["daily_bars", "fund_flow"],
            "has_data": [True, True],
            "watermarked": [True, True],
            "watermark": [date(2024, 1, 1)] * 2,
            "coverage_end": [date(2024, 1, 1)] * 2,
        }
    )
    monkeypatch.setattr("cnequity.query.reader.list_datasets", lambda **kwargs: rows)
    assert [p["dataset"] for p in stale_fetch_plan(cfg, date(2024, 1, 5), groups={"core"})] == [
        "daily_bars"
    ]
    import click
    import pytest

    with pytest.raises(click.ClickException, match="Unknown stale groups"):
        stale_fetch_plan(cfg, date(2024, 1, 5), groups={"typo"})
