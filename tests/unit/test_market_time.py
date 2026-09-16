from datetime import date, datetime, timezone

import pytest

from cnequity.domain.market_time import is_session_final, shanghai_now, shanghai_today


def test_exchange_clock_is_independent_of_host_timezone():
    instant = datetime(2026, 8, 10, 1, 0, tzinfo=timezone.utc)

    assert shanghai_now(instant).isoformat() == "2026-08-10T09:00:00+08:00"
    assert shanghai_today(instant) == date(2026, 8, 10)


def test_market_clock_rejects_naive_timestamp():
    with pytest.raises(ValueError, match="timezone-aware"):
        shanghai_today(datetime(2026, 8, 10, 9, 0))


def test_current_session_is_provisional_before_settlement_buffer():
    day = date(2026, 8, 17)

    assert is_session_final(day, datetime(2026, 8, 17, 6, 59, tzinfo=timezone.utc)) is False
    assert is_session_final(day, datetime(2026, 8, 17, 7, 5, tzinfo=timezone.utc)) is True
    assert (
        is_session_final(day - date.resolution, datetime(2026, 8, 17, 6, 59, tzinfo=timezone.utc))
        is True
    )


@pytest.mark.parametrize("month,shanghai_hour", [(9, 4), (12, 5)])
def test_helsinki_2300_catchup_anchors_previous_chinese_session(monkeypatch, month, shanghai_hour):
    from zoneinfo import ZoneInfo

    from cnequity.cli import quality_cmds

    local = datetime(2026, month, 16, 23, tzinfo=ZoneInfo("Europe/Helsinki"))
    assert shanghai_now(local).hour == shanghai_hour
    assert shanghai_today(local) == date(2026, month, 17)
    monkeypatch.setattr(quality_cmds, "is_session_final", lambda day: is_session_final(day, local))
    monkeypatch.setattr("cnequity.steps.common.is_trading_day", lambda cfg, day: day.weekday() < 5)
    assert quality_cmds._last_trading_day(None, shanghai_today(local)) == date(2026, month, 16)
