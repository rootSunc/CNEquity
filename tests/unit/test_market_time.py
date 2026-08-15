from datetime import date, datetime, timezone

import pytest

from cn_market_lake.domain.market_time import shanghai_now, shanghai_today


def test_exchange_clock_is_independent_of_host_timezone():
    instant = datetime(2026, 8, 10, 1, 0, tzinfo=timezone.utc)

    assert shanghai_now(instant).isoformat() == "2026-08-10T09:00:00+08:00"
    assert shanghai_today(instant) == date(2026, 8, 10)


def test_market_clock_rejects_naive_timestamp():
    with pytest.raises(ValueError, match="timezone-aware"):
        shanghai_today(datetime(2026, 8, 10, 9, 0))
