from datetime import date, datetime, timedelta, timezone

import polars as pl
import pytest

from cnequity.adapters.calendar.exchange_calendar import (
    build_trading_calendar,
    calendar_forward_coverage_days,
    calendar_seed_end,
    load_seed_calendar,
)
from cnequity.domain import holidays_cn
from cnequity.domain.holidays_cn import (
    CLOSED_DATES,
    EXTRA_TRADING_DATES,
    HOLIDAY_SCHEDULES,
    HOLIDAY_TABLE_END,
    HOLIDAY_TABLE_START,
    HolidaySchedule,
    exchange_may_open,
)


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

    assert build_trading_calendar(date(2001, 1, 1), date(2001, 1, 1))["is_trading"][0] is False


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


def test_daily_placeholder_does_not_create_pre_seed_session(tmp_path):
    root = tmp_path / "curated" / "daily_bars"
    root.mkdir(parents=True)
    days = [date(2010, 2, 10), date(2010, 2, 17), date(2010, 2, 22)]
    pl.DataFrame(
        {
            "trade_date": days,
            "symbol": ["600519.SH"] * 3,
            "volume": [100, 0, 100],
        }
    ).write_parquet(root / "part-000.parquet")

    cal = build_trading_calendar(
        date(2010, 2, 10), date(2010, 2, 24), curated_root=tmp_path / "curated"
    )
    assert date(2010, 2, 17) not in set(cal.filter(pl.col("is_trading"))["trade_date"])


def test_daily_retry_uses_canonical_row_before_creating_pre_seed_session(tmp_path):
    root = tmp_path / "curated" / "daily_bars"
    root.mkdir(parents=True)
    first = date(2010, 2, 10)
    day = date(2010, 2, 17)
    pl.DataFrame(
        {
            "trade_date": [first, day, day],
            "symbol": ["600519.SH"] * 3,
            "volume": [100, 100, 0],
            "fetched_at": [
                datetime(2010, 2, 10, 7, tzinfo=timezone.utc),
                datetime(2010, 2, 17, 7, tzinfo=timezone.utc),
                datetime(2010, 2, 17, 8, tzinfo=timezone.utc),
            ],
        }
    ).write_parquet(root / "part-retry.parquet")

    cal = build_trading_calendar(day, day, curated_root=tmp_path / "curated")

    assert cal["is_trading"][0] is False


def test_weekend_index_bar_does_not_create_pre_seed_session(tmp_path):
    root = tmp_path / "curated" / "index_bars"
    root.mkdir(parents=True)
    saturday = date(2010, 2, 13)
    pl.DataFrame({"trade_date": [saturday], "symbol": ["000001.SH"]}).write_parquet(
        root / "part-000.parquet"
    )

    cal = build_trading_calendar(saturday, saturday, curated_root=tmp_path / "curated")

    assert cal["is_trading"][0] is False


def test_known_historical_holiday_overrides_bar_evidence(tmp_path):
    root = tmp_path / "curated" / "daily_bars"
    root.mkdir(parents=True)
    holiday = date(2001, 1, 1)
    pl.DataFrame({"trade_date": [holiday], "symbol": ["000529.SZ"], "volume": [100]}).write_parquet(
        root / "part-000.parquet"
    )

    cal = build_trading_calendar(holiday, holiday, curated_root=tmp_path / "curated")

    assert cal["is_trading"][0] is False


def test_corrupt_bar_file_does_not_abort_calendar_derivation(tmp_path):
    root = tmp_path / "curated" / "index_bars"
    root.mkdir(parents=True)
    valid_date = date(2010, 2, 10)
    pl.DataFrame({"trade_date": [valid_date], "symbol": ["000001"]}).write_parquet(
        root / "valid.parquet"
    )
    (root / "broken.parquet").write_bytes(b"not a parquet file")

    cal = build_trading_calendar(valid_date, valid_date, curated_root=tmp_path / "curated")

    assert cal["is_trading"][0] is True


def test_calendar_forward_coverage_days():
    assert calendar_seed_end() == date(2027, 12, 31)
    assert calendar_forward_coverage_days(date(2027, 10, 2)) == 90
    assert calendar_forward_coverage_days(date(2027, 10, 3)) == 89
    assert calendar_forward_coverage_days(date(2024, 6, 28)) > 90


def test_2027_follows_the_state_council_notice():
    """国办发明电〔2026〕10号 replaced a guessed 2027 that missed five closures
    and closed two sessions."""
    closures = [date(2027, 2, 5), date(2027, 5, 4), date(2027, 5, 5), date(2027, 6, 9)]
    closures.append(date(2027, 9, 15))
    sessions = [date(2027, 6, 14), date(2027, 9, 20)]
    cal = build_trading_calendar(date(2027, 1, 1), date(2027, 12, 31))
    is_trading = dict(cal.select("trade_date", "is_trading").iter_rows())
    for day in closures + sessions:
        assert day.weekday() < 5, day  # a weekend would pass for the wrong reason
    assert not any(is_trading[day] for day in closures)
    assert all(is_trading[day] for day in sessions)
    assert len(HOLIDAY_SCHEDULES[2027].closed) == 18
    assert sum(is_trading.values()) == 243


def test_the_seed_is_the_holiday_table():
    """No second copy: the seed covers exactly the scheduled years, by the same rule."""
    seed = load_seed_calendar()
    assert seed["trade_date"].min() == HOLIDAY_TABLE_START == date(min(HOLIDAY_SCHEDULES), 1, 1)
    assert seed["trade_date"].max() == HOLIDAY_TABLE_END == date(max(HOLIDAY_SCHEDULES), 12, 31)
    assert seed.height == (HOLIDAY_TABLE_END - HOLIDAY_TABLE_START).days + 1
    assert all(exchange_may_open(day) == flag for day, flag in seed.iter_rows())
    assert max(CLOSED_DATES) <= HOLIDAY_TABLE_END.isoformat()


def test_a_pinned_seed_file_overrides_the_table(tmp_path):
    seed_path = tmp_path / "trading_calendar.csv"
    seed_path.write_text("trade_date,is_trading\n2024-06-27,false\n", encoding="utf-8")
    cal = build_trading_calendar(date(2024, 6, 27), date(2024, 6, 27), seed_path=seed_path)
    assert cal["is_trading"][0] is False


@pytest.mark.parametrize(
    ("schedules", "message"),
    [
        (
            {2016: HolidaySchedule("x", frozenset()), 2018: HolidaySchedule("x", frozenset())},
            "consecutive",
        ),
        ({2016: HolidaySchedule("", frozenset())}, "no source"),
        ({2016: HolidaySchedule("x", frozenset({"2016-01-02"}))}, "not a weekday"),
        ({2016: HolidaySchedule("x", frozenset({"2017-01-02"}))}, "not a weekday"),
    ],
)
def test_holiday_schedules_reject_unsourced_or_misplaced_years(monkeypatch, schedules, message):
    monkeypatch.setattr(holidays_cn, "HOLIDAY_SCHEDULES", schedules)
    with pytest.raises(ValueError, match=message):
        holidays_cn._check_schedules()


def test_the_year_after_the_table_is_refused_not_guessed():
    first_unscheduled = HOLIDAY_TABLE_END + timedelta(days=1)
    while first_unscheduled.weekday() >= 5:
        first_unscheduled += timedelta(days=1)
    with pytest.raises(RuntimeError, match="official holiday schedule"):
        build_trading_calendar(first_unscheduled, first_unscheduled)
