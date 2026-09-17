"""Current-day daily bars must not be staged before the TDX bar is final."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from cnequity.steps.bars import (
    _backfill_window,
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
        "cnequity.steps.bars._reject_unfinished_daily_bar_window",
        stop_before_fetch,
    )
    monkeypatch.setattr(
        "cnequity.steps.bars.load_symbols",
        lambda cfg: events.append("symbols") or ["600519.SH"],
    )
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_daily_bars_parallel",
        lambda *args, **kwargs: events.append("fetch"),
    )

    with pytest.raises(RuntimeError, match="stop before fetch"):
        step_daily_bars(config, TRADING_DAY, "run-18", context)

    assert events == ["guard"]


# `_backfill_window` reads the clock itself, so these drive it through the same
# UTC instants the guard tests use: 06:59 UTC is 14:59 in Shanghai, 07:05 is 15:05.
_MID_SESSION = datetime(2026, 8, 10, 6, 59, tzinfo=timezone.utc)
_AFTER_CLOSE = datetime(2026, 8, 10, 7, 5, tzinfo=timezone.utc)


def _freeze(monkeypatch, instant):
    from cnequity.domain.market_time import shanghai_now

    monkeypatch.setattr("cnequity.steps.bars.shanghai_now", lambda now=None: shanghai_now(instant))


def test_an_unspecified_backfill_end_stops_at_the_last_settled_session(config, monkeypatch):
    """`cne init` during a session used to fail the whole phase on this.

    phase2c asks for "all the history there is", which resolved to today —
    whose bar is still forming — so the guard refused in 2.8ms after 37 minutes
    of reference and corporate-action work, and phases 3 and 4 never ran.
    """
    _freeze(monkeypatch, _MID_SESSION)

    _start, end = _backfill_window(config, TRADING_DAY)

    assert end == TRADING_DAY - timedelta(days=1)
    _reject_unfinished_daily_bar_window(config, end, now=_MID_SESSION)


def test_an_explicit_backfill_end_is_passed_through_untouched(config, monkeypatch):
    """Repairing today's truncated bar is a real request, and before the close
    it has to fail loudly rather than quietly fetch a different day."""
    _freeze(monkeypatch, _MID_SESSION)
    config._backfill_end = TRADING_DAY

    _start, end = _backfill_window(config, TRADING_DAY)

    assert end == TRADING_DAY
    with pytest.raises(RuntimeError, match="not final until"):
        _reject_unfinished_daily_bar_window(config, end, now=_MID_SESSION)


def test_after_the_close_the_window_reaches_today(config, monkeypatch):
    _freeze(monkeypatch, _AFTER_CLOSE)

    _start, end = _backfill_window(config, TRADING_DAY)

    assert end == TRADING_DAY


def test_a_small_residue_is_tolerated_rather_than_failing_the_run(config):
    """14 symbols out of 5,500 must not discard two hours of `cne init`.

    The gate exists so a snapshot missing a real slice of the market is not
    stamped complete. A vendor's transient outage costing 0.25% is not that,
    and refusing there took phases 3 and 4 down with it.
    """
    from cnequity.steps.bars import _unresolved_budget

    config.daily_bars_unresolved_tolerance = 0.01
    assert _unresolved_budget(config, 5500) == 55
    assert 14 <= _unresolved_budget(config, 5500), "a 0.25% residue is within budget"


def test_a_real_slice_of_the_market_still_refuses(config):
    from cnequity.steps.bars import _unresolved_budget

    config.daily_bars_unresolved_tolerance = 0.01
    assert 300 > _unresolved_budget(config, 5500), "5% is not a residue"


def test_a_tiny_universe_tolerates_nothing(config):
    """A fraction alone would let one symbol of five through as 'small'."""
    from cnequity.steps.bars import _unresolved_budget

    config.daily_bars_unresolved_tolerance = 0.01
    assert _unresolved_budget(config, 5) == 0


def test_the_tolerance_can_be_switched_off(config):
    """Fail-closed stays available for anyone who wants the old contract."""
    from cnequity.steps.bars import _unresolved_budget

    config.daily_bars_unresolved_tolerance = 0.0
    assert _unresolved_budget(config, 5500) == 0


def test_an_interior_gap_within_tolerance_is_owed_rather_than_fatal(config, monkeypatch, tmp_path):
    """The gate that actually killed `cne init`: 5,037 keys of ~4.1M — 0.12%.

    The unknown-symbol tolerance did not cover this one; the interior gap is
    counted in symbol×session keys, so it needs the key count as its
    denominator rather than the symbol count.
    """
    from cnequity.steps.bars import _unresolved_budget

    config.daily_bars_unresolved_tolerance = 0.01
    expected_keys = 750 * 5500  # three years of a whole-market sweep
    assert 5037 <= _unresolved_budget(config, expected_keys)


def test_an_interior_gap_beyond_tolerance_still_refuses(config):
    from cnequity.steps.bars import _unresolved_budget

    config.daily_bars_unresolved_tolerance = 0.01
    assert 100_000 > _unresolved_budget(config, 750 * 5500)
