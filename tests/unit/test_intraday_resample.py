from datetime import date, datetime, timedelta

import polars as pl
import pytest

from cnequity.query import resample_trade_bars


def bars(start=datetime(2026, 9, 15, 9, 31), count=5):
    return pl.DataFrame(
        {
            "symbol": ["603869.SH"] * count,
            "trade_date": [date(2026, 9, 15)] * count,
            "bar_time": [start + timedelta(minutes=i) for i in range(count)],
            "frequency": ["1m"] * count,
            **{k: [10.0] * count for k in ["open", "high", "low", "close"]},
            "volume": [100] * count,
            "amount": [1000.0] * count,
        }
    )


def test_untraded_opening_carry_does_not_change_trade_ohlc():
    frame = bars().with_columns(
        pl.Series("volume", [0, 100, 0, 100, 0]),
        pl.Series("amount", [0.0, 1000.0, 0.0, 1000.0, 0.0]),
        *[pl.Series(k, [20.0, 10.0, 20.0, 10.0, 20.0]) for k in ["open", "high", "low", "close"]],
    )
    result = resample_trade_bars(frame)
    assert result.select("open", "high", "low", "close").row(0) == (10.0, 10.0, 10.0, 10.0)
    assert result.select("volume", "amount").row(0) == (200, 2000.0)
    assert result["bar_time"][0] == datetime(2026, 9, 15, 9, 35)


def test_positive_amount_retains_trade_when_volume_rounded_to_zero():
    frame = bars().with_columns(
        pl.lit(0).alias("volume"),
        pl.Series("amount", [100.0, 0.0, 0.0, 0.0, 0.0]),
        *[pl.Series(k, [10.0, 20.0, 20.0, 20.0, 20.0]) for k in ["open", "high", "low", "close"]],
    )
    result = resample_trade_bars(frame)
    assert result["amount"][0] == 100
    assert result.select("open", "high", "low", "close").row(0) == (10.0, 10.0, 10.0, 10.0)


def test_all_untraded_interval_keeps_quote_and_zero_quantities():
    result = resample_trade_bars(
        bars().with_columns(pl.lit(0).alias("volume"), pl.lit(0.0).alias("amount"))
    )
    assert result.select("open", "high", "low", "close", "volume", "amount").row(0) == (
        10.0,
        10.0,
        10.0,
        10.0,
        0,
        0.0,
    )


def test_hourly_intervals_anchor_to_each_half_session():
    frame = pl.concat([bars(count=120), bars(datetime(2026, 9, 15, 13, 1), 120)])
    result = resample_trade_bars(frame, "60m")
    assert [(d.hour, d.minute) for d in result["bar_time"]] == [
        (10, 30),
        (11, 30),
        (14, 0),
        (15, 0),
    ]
    assert result["volume"].sum() == 24000


def test_missing_and_duplicate_minutes_are_not_silently_filled():
    with pytest.raises(ValueError, match="incomplete"):
        resample_trade_bars(bars().slice(1))
    with pytest.raises(ValueError, match="duplicate"):
        resample_trade_bars(pl.concat([bars(), bars().head(1)]))


def test_lunch_and_mismatched_dates_are_rejected():
    with pytest.raises(ValueError, match="session"):
        resample_trade_bars(bars(datetime(2026, 9, 15, 12, 1)))
    with pytest.raises(ValueError, match="trade date"):
        resample_trade_bars(bars().with_columns(pl.lit(date(2026, 9, 14)).alias("trade_date")))
