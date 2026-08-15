from datetime import date

import polars as pl

from cn_market_lake.adapters.calendar.exchange_calendar import (
    build_trading_calendar,
    calendar_forward_coverage_days,
    calendar_seed_end,
)
from cn_market_lake.adapters.calendar.holidays_cn import EXTRA_TRADING_DATES


def test_cny_holiday_not_trading():
    cal = build_trading_calendar(date(2024, 2, 8), date(2024, 2, 18))
    row = cal.filter(pl.col("trade_date") == date(2024, 2, 12))
    assert row.height == 1
    assert row["is_trading"][0] is False


def test_regular_weekday_is_trading():
    cal = build_trading_calendar(date(2024, 6, 24), date(2024, 6, 28))
    row = cal.filter(pl.col("trade_date") == date(2024, 6, 27))
    assert row["is_trading"][0] is True


def test_weekend_not_trading():
    cal = build_trading_calendar(date(2024, 6, 22), date(2024, 6, 23))
    assert cal.filter(pl.col("is_trading")).height == 0


def test_no_weekend_makeup_sessions():
    # SSE/SZSE never open on 调休 make-up weekends, even though offices do.
    assert EXTRA_TRADING_DATES == frozenset()
    # Sample former false-positives across years: Saturdays/Sundays adjacent
    # to holidays that were wrongly flagged as trading days.
    for d in (date(2016, 10, 8), date(2019, 2, 3), date(2024, 5, 11), date(2024, 9, 29)):
        cal = build_trading_calendar(d, d)
        assert cal["is_trading"][0] is False, d


def test_new_year_holiday_not_trading():
    # 元旦 closures were systematically missing from CLOSED_DATES for 2019+.
    # 2016-01-01 (a Friday) was also missing and regressed curated data.
    for d in (
        date(2016, 1, 1),
        date(2019, 1, 1),
        date(2020, 1, 1),
        date(2021, 1, 1),
        date(2022, 1, 3),
        date(2023, 1, 2),
        date(2024, 1, 1),
        date(2025, 1, 1),
        date(2026, 1, 1),
        date(2027, 1, 1),
    ):
        cal = build_trading_calendar(d, d)
        assert cal["is_trading"][0] is False, d


def test_seed_is_authoritative_over_index_bars(tmp_path):
    # A spurious index_bars row on a seed holiday must not flip it to trading.
    holiday = date(2027, 1, 1)
    bars_dir = tmp_path / "index_bars" / f"trade_date={holiday.isoformat()}"
    bars_dir.mkdir(parents=True)
    pl.DataFrame({"trade_date": [holiday], "symbol": ["000001"]}).write_parquet(
        bars_dir / "part.parquet"
    )
    cal = build_trading_calendar(holiday, holiday, curated_root=tmp_path)
    assert cal["is_trading"][0] is False


def test_calendar_forward_coverage_days():
    assert calendar_seed_end() == date(2027, 12, 31)
    assert calendar_forward_coverage_days(date(2027, 10, 2)) == 90
    assert calendar_forward_coverage_days(date(2027, 10, 3)) == 89
    assert calendar_forward_coverage_days(date(2024, 6, 28)) > 90
