from datetime import date, datetime, timezone

import polars as pl

from cnequity.adapters.calendar.exchange_calendar import (
    build_trading_calendar,
    calendar_forward_coverage_days,
    calendar_seed_end,
)
from cnequity.domain.holidays_cn import EXTRA_TRADING_DATES


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
