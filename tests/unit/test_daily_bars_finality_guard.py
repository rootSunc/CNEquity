"""Current-day daily bars must not be staged before the TDX bar is final."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from cn_market_lake.steps.bars import (
    _reject_unfinished_daily_bar_window,
    step_daily_bars,
)

TRADING_DAY = date(2026, 8, 10)


def test_rejects_current_trading_day_before_shanghai_cutoff(config):
    # 06:59 UTC is 14:59 in Shanghai, regardless of the host's local timezone.
    now = datetime(2026, 8, 10, 6, 59, tzinfo=timezone.utc)

    with pytest.raises(RuntimeError, match="not final until 15:05 Asia/Shanghai"):
        _reject_unfinished_daily_bar_window(config, TRADING_DAY, now=now)


def test_rejects_future_end_while_shanghai_session_is_open(config):
    # A host in UTC+14 may already report the next local calendar date while
    # Shanghai is still on the current trading day.
    now = datetime(2026, 8, 10, 1, 0, tzinfo=timezone.utc)

    with pytest.raises(RuntimeError, match="not final until 15:05 Asia/Shanghai"):
        _reject_unfinished_daily_bar_window(config, date(2026, 8, 11), now=now)


def test_allows_current_trading_day_at_shanghai_cutoff(config):
    now = datetime(2026, 8, 10, 7, 5, tzinfo=timezone.utc)

    _reject_unfinished_daily_bar_window(config, TRADING_DAY, now=now)


def test_allows_historical_window_during_a_later_session(config):
    now = datetime(2026, 8, 11, 6, 0, tzinfo=timezone.utc)

    _reject_unfinished_daily_bar_window(config, TRADING_DAY, now=now)


def test_allows_current_non_trading_day_before_cutoff(config):
    sunday = date(2026, 8, 9)
    now = datetime(2026, 8, 9, 6, 0, tzinfo=timezone.utc)

    _reject_unfinished_daily_bar_window(config, sunday, now=now)


def test_rejects_naive_clock(config):
    with pytest.raises(ValueError, match="timezone-aware"):
        _reject_unfinished_daily_bar_window(
            config,
            TRADING_DAY,
            now=datetime(2026, 8, 10, 14, 59),
        )


@pytest.mark.parametrize(
    "context",
    [
        {},
        {
            "_retry_batch_specs": [
                ("retry-0", ["600519.SH"], TRADING_DAY, TRADING_DAY),
            ]
        },
    ],
)
def test_step_applies_guard_before_loading_or_fetching(config, monkeypatch, context):
    events: list[str] = []

    def stop_before_fetch(*args, **kwargs):
        events.append("guard")
        raise RuntimeError("stop before fetch")

    monkeypatch.setattr(
        "cn_market_lake.steps.bars._reject_unfinished_daily_bar_window",
        stop_before_fetch,
    )
    monkeypatch.setattr(
        "cn_market_lake.steps.bars.load_symbols",
        lambda cfg: events.append("symbols") or ["600519.SH"],
    )
    monkeypatch.setattr(
        "cn_market_lake.steps.bars.fetch_daily_bars_parallel",
        lambda *args, **kwargs: events.append("fetch"),
    )

    with pytest.raises(RuntimeError, match="stop before fetch"):
        step_daily_bars(config, TRADING_DAY, "run-18", context)

    assert events == ["guard"]
