from datetime import date, datetime, time

import pytest

from cn_market_lake.adapters.tdx_protocol._wire import MAX_TICK_PAGE
from cn_market_lake.adapters.tdx_protocol.trade_ticks import (
    AFTER_HOURS,
    DIRECTIONS,
    MAX_SESSION_PAGES,
    TdxTradeTicksError,
    fetch_trade_ticks,
    in_session,
    price_divisor,
)

DAY = date(2026, 7, 31)


def _tick(hour: int, minute: int, **over) -> dict:
    """One wire row as the transaction parser emits it."""
    row = {
        "hour": hour,
        "minute": minute,
        "time": f"{hour:02d}:{minute:02d}",
        "price_raw": 135060,
        "vol": 3,
        "direction": 0,
    }
    row.update(over)
    return row


def _session(count: int, start: tuple[int, int] = (9, 30), end: tuple[int, int] = (11, 30)):
    """*count* rows walking from *start* to *end*, then repeating the last minute.

    Repeats are what the real wire does — a busy minute holds up to 20 records —
    and the walk never goes backwards, which is the invariant the adapter checks.
    """
    out = []
    minute = start[0] * 60 + start[1]
    last = end[0] * 60 + end[1]
    for _ in range(count):
        out.append(_tick(minute // 60, minute % 60))
        minute = min(minute + 1, last)
    return out


class FakeClient:
    """Serves prepared pages, newest block first, as the wire does."""

    def __init__(self, pages: list[list[dict]]):
        self.pages = pages
        self.calls: list[dict] = []

    def ticks_history(self, code, on_date, market, start, offset):
        self.calls.append(
            {"code": code, "date": on_date, "market": market, "start": start, "offset": offset}
        )
        index = len(self.calls) - 1
        return self.pages[index] if index < len(self.pages) else []


@pytest.mark.parametrize(
    ("clock", "expected"),
    [
        ((9, 25), True),  # opening call auction — one record a session
        ((9, 24), False),
        ((9, 26), False),  # nothing between the auction and the open
        ((9, 30), True),  # a trade at 09:30 is real, unlike a 09:30 bar label
        ((11, 30), True),
        ((12, 15), False),  # lunch
        ((13, 0), True),  # also real, unlike the bar path where 13:01 is first
        ((15, 0), True),
        ((15, 2), False),  # between the close and after-hours trading
        ((15, 5), True),  # after-hours fixed price opens
        ((15, 30), True),
        ((15, 31), False),
    ],
)
def test_session_windows(clock, expected):
    assert in_session(time(*clock)) is expected


def test_direction_codes_cover_what_the_wire_emits():
    assert DIRECTIONS == {0: "buy", 1: "sell", 2: "neutral", 5: "after_hours"}


@pytest.mark.parametrize(
    ("symbol", "divisor"),
    [
        ("600519.SH", 100),
        ("688981.SH", 100),  # STAR board is still a stock
        ("000001.SZ", 100),
        ("300750.SZ", 100),
        ("510300.SH", 1000),  # fund — the 10x trap
        ("159915.SZ", 1000),
    ],
)
def test_price_divisor_follows_the_instrument(symbol, divisor):
    assert price_divisor(symbol) == divisor


def test_price_divisor_refuses_to_guess():
    with pytest.raises(TdxTradeTicksError, match="no known price coefficient"):
        price_divisor("777777.SH")


def test_price_divides_exactly():
    # 135060 * 0.01 is 1350.6000000000001; the division is the same double as
    # the literal, which is what the daily reconciliation compares against.
    client = FakeClient([[_tick(10, 0, price_raw=135060)]])
    (row,) = fetch_trade_ticks(client, "600519.SH", DAY)
    assert row["price"] == 1350.6


def test_fetch_assembles_pages_oldest_first():
    # start=0 is the tail of the session; start=2000 the part before it.
    tail = _session(MAX_TICK_PAGE, start=(13, 0), end=(15, 0))
    head = _session(300, start=(10, 0), end=(11, 30))
    client = FakeClient([tail, head])

    rows = fetch_trade_ticks(client, "600519.SH", DAY)

    assert [c["start"] for c in client.calls] == [0, MAX_TICK_PAGE]
    assert len(rows) == MAX_TICK_PAGE + 300
    # The later-fetched page holds the earlier part of the day.
    assert rows[0]["trade_time"].hour == 10
    assert rows[-1]["trade_time"].hour == 15
    assert [r["tick_seq"] for r in rows[:3]] == [0, 1, 2]
    assert rows[-1]["tick_seq"] == len(rows) - 1


def test_fetch_converts_lots_to_shares_and_scales_price():
    client = FakeClient([[_tick(10, 0, price_raw=135060, vol=3)]])
    (row,) = fetch_trade_ticks(client, "600519.SH", DAY)
    assert row["price"] == pytest.approx(1350.60)
    assert row["volume"] == 300
    assert row["direction"] == "buy"
    assert row["trade_time"] == datetime(2026, 7, 31, 10, 0)


def test_fetch_scales_a_fund_price_by_a_thousand():
    client = FakeClient([[_tick(10, 0, price_raw=3368)]])
    (row,) = fetch_trade_ticks(client, "159915.SZ", DAY)
    assert row["price"] == pytest.approx(3.368)


def test_after_hours_rows_keep_their_own_label():
    client = FakeClient([[_tick(10, 0, direction=1), _tick(15, 20, direction=5)]])
    rows = fetch_trade_ticks(client, "600519.SH", DAY)
    assert [r["direction"] for r in rows] == ["sell", AFTER_HOURS]


def test_unknown_direction_is_kept_and_logged(caplog):
    client = FakeClient([[_tick(10, 0, direction=7)]])
    (row,) = fetch_trade_ticks(client, "600519.SH", DAY)
    assert row["direction"] == "unknown"
    assert row["volume"] == 300  # the rest of the record is still good
    assert "unknown direction code" in caplog.text


def test_fetch_maps_market_from_the_suffix():
    client = FakeClient([[_tick(10, 0)]])
    fetch_trade_ticks(client, "600519.SH", DAY)
    assert client.calls[0]["market"] == 1
    assert client.calls[0]["code"] == "600519"

    client = FakeClient([[_tick(10, 0)]])
    fetch_trade_ticks(client, "000001.SZ", DAY)
    assert client.calls[0]["market"] == 0


def test_fetch_refuses_beijing_symbols():
    client = FakeClient([[]])
    with pytest.raises(TdxTradeTicksError, match="no Beijing-exchange"):
        fetch_trade_ticks(client, "920003.BJ", DAY)
    assert client.calls == []


def test_fetch_refuses_an_unknown_exchange():
    with pytest.raises(TdxTradeTicksError, match="unsupported exchange suffix"):
        fetch_trade_ticks(FakeClient([]), "600519.XX", DAY)


def test_a_session_with_no_trades_is_empty_not_an_error():
    assert fetch_trade_ticks(FakeClient([[]]), "600519.SH", DAY) == []


def test_a_short_page_ends_the_walk():
    client = FakeClient([_session(10)])
    fetch_trade_ticks(client, "600519.SH", DAY)
    assert len(client.calls) == 1


def test_a_failing_page_never_returns_a_partial_session():
    class Broken:
        def __init__(self):
            self.calls = 0

        def ticks_history(self, code, on_date, market, start, offset):
            self.calls += 1
            if self.calls == 1:
                return _session(MAX_TICK_PAGE)
            raise ConnectionError("boom")

    # The first page succeeded, but returning it alone would number every row
    # from the wrong anchor.
    with pytest.raises(TdxTradeTicksError, match="page failed at start=2000"):
        fetch_trade_ticks(Broken(), "600519.SH", DAY)


def test_a_session_that_never_ends_fails_rather_than_truncating():
    client = FakeClient([_session(MAX_TICK_PAGE) for _ in range(MAX_SESSION_PAGES + 1)])
    with pytest.raises(TdxTradeTicksError, match="never reached the session's start"):
        fetch_trade_ticks(client, "600519.SH", DAY)
    assert len(client.calls) == MAX_SESSION_PAGES


def test_off_session_records_fail_instead_of_being_dropped():
    client = FakeClient([[_tick(10, 0), _tick(12, 15), _tick(14, 0)]])
    with pytest.raises(TdxTradeTicksError, match="outside trading hours"):
        fetch_trade_ticks(client, "600519.SH", DAY)


def test_out_of_order_records_fail_because_tick_seq_would_lie():
    client = FakeClient([[_tick(14, 0), _tick(10, 0)]])
    with pytest.raises(TdxTradeTicksError, match="not in time order"):
        fetch_trade_ticks(client, "600519.SH", DAY)


def test_on_page_fires_once_per_page_kept():
    seen = []
    client = FakeClient(
        [_session(MAX_TICK_PAGE, start=(13, 0), end=(15, 0)), _session(5, start=(9, 30))]
    )
    fetch_trade_ticks(client, "600519.SH", DAY, on_page=lambda: seen.append(1))
    assert len(seen) == 2


def test_tick_seq_is_dense_over_the_whole_session():
    client = FakeClient(
        [_session(MAX_TICK_PAGE, start=(13, 0), end=(15, 0)), _session(120, start=(9, 30))]
    )
    rows = fetch_trade_ticks(client, "600519.SH", DAY)
    assert [r["tick_seq"] for r in rows] == list(range(len(rows)))
