"""Calendar coverage at both ends.

Back: the calendar's start followed daily_bars alone (2001) while index_bars
reached 1990-12-19, so 2,538 index_bars dates sat before the calendar existed
and the audit reported them as bars on non-trading days.

Forward: trading_calendar is written a year ahead of every run. Past the bundled
holiday table the fallback only strips weekends, so 春节 comes back marked as a
session — a year of trading days that are not, in silence.
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.quality.cross_checks import trading_calendar_horizon_findings


def _meta():
    return {"source": "t", "data_version": "v1", "fetched_at": None}


def _calendar(tmp_path, last_trading_day: date) -> Config:
    cfg = Config(data_root=tmp_path / "lake")
    root = cfg.curated_root / "trading_calendar" / "trade_date=2026"
    root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [
            {"trade_date": date(2026, 1, 5), "is_trading": True, **_meta()},
            {"trade_date": last_trading_day, "is_trading": True, **_meta()},
        ]
    ).write_parquet(root / "part-0.parquet")
    return cfg


def test_silent_while_inside_the_holiday_table(tmp_path):
    from cnequity.adapters.calendar.exchange_calendar import calendar_seed_end

    inside = calendar_seed_end()
    assert trading_calendar_horizon_findings(_calendar(tmp_path, inside), date(2026, 8, 7)) == []


def test_sessions_after_the_last_holiday_are_not_beyond_the_table(tmp_path):
    """The last closure of the table is 2027-10-07, but the seed covers the
    rest of that year: the weekdays after it are real sessions."""
    from cnequity.domain.holidays_cn import CLOSED_DATES

    after_last_closure = date.fromisoformat(max(CLOSED_DATES)) + timedelta(days=11)
    cfg = _calendar(tmp_path, after_last_closure)
    assert trading_calendar_horizon_findings(cfg, date(2026, 8, 7)) == []


def test_fires_once_the_calendar_outruns_the_table(tmp_path):
    from cnequity.adapters.calendar.exchange_calendar import calendar_seed_end

    table_end = calendar_seed_end()
    beyond = table_end + timedelta(days=90)
    findings = trading_calendar_horizon_findings(_calendar(tmp_path, beyond), date(2026, 8, 7))
    assert len(findings) == 1
    f = findings[0]
    assert f["check"] == "trading_calendar_beyond_holiday_table"
    assert f["days_beyond"] == 90
    assert "holidays_cn.py" in f["message"], "must name what to refresh"


def test_holidays_past_the_table_really_are_marked_as_sessions():
    """The failure the check exists to catch, demonstrated rather than asserted
    in prose: 2028-01-26 is 春节 and the builder calls it a trading day."""
    from cnequity.adapters.calendar.exchange_calendar import (
        build_trading_calendar,
        ensure_seed_csv,
    )
    from cnequity.domain.holidays_cn import CLOSED_DATES

    spring_festival = date(2028, 1, 26)
    assert spring_festival > date.fromisoformat(max(CLOSED_DATES))
    with pytest.raises(RuntimeError, match="holiday seed ends"):
        build_trading_calendar(spring_festival, spring_festival, seed_path=ensure_seed_csv())


def test_earliest_bar_date_considers_index_bars_too(tmp_path):
    """The calendar start must follow whichever bar dataset reaches furthest."""
    from cnequity.steps.reference import _earliest_bar_date

    cfg = Config(data_root=tmp_path / "lake")
    for dataset, day in (("daily_bars", date(2001, 1, 2)), ("index_bars", date(1990, 12, 19))):
        root = cfg.curated_root / dataset / f"trade_date={day.year}"
        root.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"trade_date": [day]}).write_parquet(root / "part-0.parquet")

    assert _earliest_bar_date(cfg) == date(1990, 12, 19)


def test_earliest_bar_date_ignores_daily_placeholder_only_rows(tmp_path):
    from cnequity.steps.reference import _earliest_bar_date

    cfg = Config(data_root=tmp_path / "lake")
    daily = cfg.curated_root / "daily_bars" / "trade_date=1980"
    index = cfg.curated_root / "index_bars" / "trade_date=1990"
    daily.mkdir(parents=True, exist_ok=True)
    index.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"trade_date": [date(1980, 1, 2)], "volume": [0]}).write_parquet(
        daily / "part-0.parquet"
    )
    pl.DataFrame({"trade_date": [date(1990, 12, 19)]}).write_parquet(index / "part-0.parquet")

    assert _earliest_bar_date(cfg) == date(1990, 12, 19)


def test_the_calendar_step_stops_at_the_holiday_table_instead_of_failing(tmp_path, monkeypatch):
    """A year ahead crossed the table's end on 2027-01-01; from that day the
    step raised, and phase 1 of every `cne init` with it."""
    import cnequity.steps.reference as reference
    from cnequity.adapters.calendar.exchange_calendar import calendar_seed_end
    from cnequity.storage.layout import init_data_layout

    cfg = Config(data_root=tmp_path / "lake")
    init_data_layout(cfg)
    written: dict = {}

    def _capture(config, run_id, dataset, df):
        written["df"] = df
        return {}

    monkeypatch.setattr(reference, "write_simple", _capture)
    reference.step_trading_calendar(cfg, date(2027, 3, 1), "run-1", {})

    assert written["df"]["trade_date"].max() == calendar_seed_end()
