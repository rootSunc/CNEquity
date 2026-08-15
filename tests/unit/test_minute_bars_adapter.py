from datetime import date, datetime, time, timedelta

import pytest

from cn_market_lake.adapters.tdx_protocol.minute_bars import (
    FREQUENCIES,
    TdxMinuteBarsError,
    _parse_stamp,
    bars_per_session,
    category_for,
    fetch_minute_bars_paginated,
    in_session,
    pages_for_window,
)


def _bar(stamp: datetime, **over):
    """One wire row as the TDX parser emits it (components plus a string)."""
    row = {
        "year": stamp.year,
        "month": stamp.month,
        "day": stamp.day,
        "hour": stamp.hour,
        "minute": stamp.minute,
        "datetime": stamp.strftime("%Y-%m-%d %H:%M"),
        "open": 10.0,
        "high": 10.5,
        "low": 9.5,
        "close": 10.2,
        "vol": 1000,
        "volume": 1000,
        "amount": 10200.0,
    }
    row.update(over)
    return row


def _session(day: date, count: int = 240) -> list[dict]:
    """The first *count* closing-minute labels of a session, in order."""
    stamps = []
    minute = datetime(day.year, day.month, day.day, 9, 31)
    while len(stamps) < count:
        if in_session(minute):
            stamps.append(minute)
        minute += timedelta(minutes=1)
    return [_bar(s) for s in stamps]


class FakeClient:
    def __init__(self, pages: list[list[dict]]):
        self.pages = pages
        self.calls: list[dict] = []

    def bars(self, symbol, frequency, market, start, offset):
        self.calls.append(
            {"symbol": symbol, "frequency": frequency, "market": market, "start": start}
        )
        index = len(self.calls) - 1
        return self.pages[index] if index < len(self.pages) else []


def test_category_and_session_size_per_frequency():
    assert category_for("1m") == 8
    assert category_for("5m") == 0
    assert bars_per_session("1m") == 240
    assert bars_per_session("5m") == 48
    with pytest.raises(ValueError, match="unsupported intraday frequency"):
        category_for("2m")


def test_frequencies_cover_a_full_session():
    # 4 trading hours: every frequency's bar count must multiply back to 240
    # minutes, which is what makes bars_per_session usable as a gap yardstick.
    minutes = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "60m": 60}
    for label, per_bar in minutes.items():
        assert label in FREQUENCIES
        assert bars_per_session(label) * per_bar == 240


@pytest.mark.parametrize(
    ("clock", "expected"),
    [
        ((9, 31), True),  # first bar of the session
        ((9, 30), False),  # the opening auction is inside the 09:31 bar
        ((11, 30), True),  # last bar before lunch
        ((12, 15), False),  # lunch break
        ((13, 0), False),  # 13:01 is the first afternoon label, not 13:00
        ((13, 1), True),
        ((15, 0), True),  # closing auction
        ((15, 1), False),
    ],
)
def test_in_session_boundaries(clock, expected):
    assert in_session(datetime(2026, 7, 31, *clock)) is expected


def test_fetch_keeps_only_in_window_session_bars():
    day = date(2026, 7, 31)
    page = [
        _bar(datetime(2026, 7, 30, 14, 59)),  # before the window
        _bar(datetime(2026, 7, 31, 9, 31)),
        _bar(datetime(2026, 7, 31, 12, 15)),  # lunch — a decode error
        _bar(datetime(2026, 7, 31, 15, 0)),
    ]
    rows = fetch_minute_bars_paginated(FakeClient([page]), "600519.SH", day, day)
    assert [r["bar_time"] for r in rows] == [
        datetime(2026, 7, 31, 9, 31),
        datetime(2026, 7, 31, 15, 0),
    ]
    assert {r["frequency"] for r in rows} == {"1m"}
    assert {r["trade_date"] for r in rows} == {day}


def test_fetch_maps_market_and_category_from_symbol():
    day = date(2026, 7, 31)
    client = FakeClient([[_bar(datetime(2026, 7, 31, 9, 31))]])
    fetch_minute_bars_paginated(client, "000001.SZ", day, day, frequency="5m")
    assert client.calls[0]["market"] == 0
    assert client.calls[0]["frequency"] == 0
    assert client.calls[0]["symbol"] == "000001"


def test_fetch_refuses_beijing_symbols():
    # TDX has no BJ route and there is no intraday fallback vendor, so an empty
    # list would read as "did not trade" rather than "cannot be served".
    with pytest.raises(TdxMinuteBarsError, match="Beijing"):
        fetch_minute_bars_paginated(
            FakeClient([]), "920819.BJ", date(2026, 7, 31), date(2026, 7, 31)
        )


def test_fetch_pages_until_reaching_window_start():
    full = _session(date(2026, 7, 31))
    older = _session(date(2026, 7, 29))
    # 800-row pages force a second request; the second reaches before `start`.
    page1 = (full + full + full + full)[:800]
    page2 = (older + older + older + older)[:800]
    client = FakeClient([page1, page2])
    rows = fetch_minute_bars_paginated(client, "600519.SH", date(2026, 7, 30), date(2026, 7, 31))
    assert len(client.calls) == 2
    assert client.calls[1]["start"] == 800
    # Page 2 is entirely older than the window, so nothing from it survives.
    assert {r["trade_date"] for r in rows} == {date(2026, 7, 31)}


def test_fetch_respects_max_pages():
    page = _session(date(2026, 7, 31)) * 4
    client = FakeClient([page[:800], page[:800], page[:800]])
    fetch_minute_bars_paginated(
        client, "600519.SH", date(2020, 1, 1), date(2026, 7, 31), max_pages=2
    )
    assert len(client.calls) == 2


def test_first_page_failure_always_raises():
    class Broken:
        def bars(self, **kwargs):
            raise RuntimeError("connection reset")

    with pytest.raises(TdxMinuteBarsError, match="start=0"):
        fetch_minute_bars_paginated(Broken(), "600519.SH", date(2026, 7, 31), date(2026, 7, 31))


def test_lunch_boundary_padding_bar_is_dropped():
    """The source pads inactive instruments with a 13:00 bar.

    Observed on 162107.SZ (a barely-traded LOF): a 13:00-labelled bar on days
    it did not trade, zero volume, close carried forward. 13:01 is the first
    real afternoon label, so 13:00 is padding — keeping it would put a phantom
    bar in every gap check.
    """
    page = [
        _bar(datetime(2026, 7, 31, 11, 30)),
        _bar(datetime(2026, 7, 31, 13, 0), vol=0, volume=0, amount=0.0, close=1.0),
        _bar(datetime(2026, 7, 31, 13, 1)),
    ]
    rows = fetch_minute_bars_paginated(
        FakeClient([page]), "162107.SZ", date(2026, 7, 31), date(2026, 7, 31)
    )
    assert [r["bar_time"].time() for r in rows] == [time(11, 30), time(13, 1)]


def test_no_trade_minute_stores_exact_zeros():
    # The wire decoder maps a raw 0 volume to 2**-127, not to 0.0. Left alone
    # that denormal lands in `amount` and quietly breaks the lake's no-trade
    # convention (volume=0, amount=0).
    denormal = 2.0**-127
    page = [_bar(datetime(2026, 7, 31, 14, 59), vol=denormal, volume=denormal, amount=denormal)]
    rows = fetch_minute_bars_paginated(
        FakeClient([page]), "600519.SH", date(2026, 7, 31), date(2026, 7, 31)
    )
    assert rows[0]["volume"] == 0
    assert rows[0]["amount"] == 0.0


def test_real_quantities_survive_the_zero_snap():
    page = [_bar(datetime(2026, 7, 31, 9, 31), vol=67700, volume=67700, amount=91_450_000.0)]
    rows = fetch_minute_bars_paginated(
        FakeClient([page]), "600519.SH", date(2026, 7, 31), date(2026, 7, 31)
    )
    assert rows[0]["volume"] == 67700
    assert rows[0]["amount"] == 91_450_000.0


def test_duplicate_bars_are_deduped_by_primary_key():
    stamp = datetime(2026, 7, 31, 9, 31)
    page = [_bar(stamp, close=10.0), _bar(stamp, close=11.0)]
    rows = fetch_minute_bars_paginated(
        FakeClient([page]), "600519.SH", date(2026, 7, 31), date(2026, 7, 31)
    )
    assert len(rows) == 1


def test_pages_for_window_covers_the_whole_window():
    # 240 bars a day against 800-bar pages: 3 days fit in one page but never
    # align to it, so the bound always carries a spare page.
    assert pages_for_window("1m", 3) == 2
    assert pages_for_window("1m", 95) == 30
    assert pages_for_window("5m", 491) == 31
    assert pages_for_window("1m", 1) == 2


class RecordingQuotes:
    """A fake TDX client that records which thread used it."""

    instances: list["RecordingQuotes"] = []

    def __init__(self):
        self.symbols: list[str] = []
        self.threads: set = set()
        RecordingQuotes.instances.append(self)

    def bars(self, symbol, frequency, market, start, offset):
        import threading

        self.symbols.append(symbol)
        self.threads.add(threading.get_ident())
        if start:  # single page per symbol
            return []
        return [_bar(datetime(2026, 7, 31, 9, 31)), _bar(datetime(2026, 7, 31, 9, 32))]


def _patch_client(monkeypatch):
    from cn_market_lake.adapters.tdx_protocol import client as client_mod

    RecordingQuotes.instances = []
    monkeypatch.setattr(client_mod, "_quotes_client", lambda config: RecordingQuotes())
    monkeypatch.setattr(client_mod, "_close_quotes_client", lambda c: None)
    return client_mod


def test_threaded_fetch_uses_one_client_per_lane(monkeypatch):
    client_mod = _patch_client(monkeypatch)
    symbols = [f"60000{i}.SH" for i in range(8)]

    df, failed = client_mod.fetch_minute_bars(
        symbols, date(2026, 7, 31), date(2026, 7, 31), workers=4
    )

    # One connection per lane, never shared: the wire client is not thread-safe.
    assert len(RecordingQuotes.instances) == 4
    for inst in RecordingQuotes.instances:
        assert len(inst.threads) == 1
    assert df.height == 16
    assert failed == []
    # Every symbol fetched exactly once across all lanes.
    fetched = sorted(s for inst in RecordingQuotes.instances for s in inst.symbols)
    assert fetched == sorted(s.split(".")[0] for s in symbols)


def test_threaded_fetch_deals_symbols_round_robin(monkeypatch):
    client_mod = _patch_client(monkeypatch)
    symbols = [f"60000{i}.SH" for i in range(8)]
    client_mod.fetch_minute_bars(symbols, date(2026, 7, 31), date(2026, 7, 31), workers=4)

    # Round-robin, not contiguous blocks: a lane must not be able to draw a run
    # of illiquid names and finish long after the others.
    lanes = [inst.symbols for inst in RecordingQuotes.instances]
    assert sorted(lanes) == [
        ["600000", "600004"],
        ["600001", "600005"],
        ["600002", "600006"],
        ["600003", "600007"],
    ]


def test_worker_count_never_exceeds_the_symbol_count(monkeypatch):
    client_mod = _patch_client(monkeypatch)
    client_mod.fetch_minute_bars(["600519.SH"], date(2026, 7, 31), date(2026, 7, 31), workers=8)
    # One symbol must not open eight connections.
    assert len(RecordingQuotes.instances) == 1


def test_single_worker_keeps_the_serial_path(monkeypatch):
    client_mod = _patch_client(monkeypatch)
    symbols = [f"60000{i}.SH" for i in range(4)]
    df, _ = client_mod.fetch_minute_bars(symbols, date(2026, 7, 31), date(2026, 7, 31), workers=1)
    assert len(RecordingQuotes.instances) == 1
    assert df.height == 8


def test_threaded_fetch_records_per_symbol_failures(monkeypatch):
    client_mod = _patch_client(monkeypatch)
    # A BJ symbol cannot be served at all; it must come back as a failure
    # rather than killing its lane or the batch.
    symbols = ["600000.SH", "920819.BJ", "600002.SH", "600003.SH"]
    df, failed = client_mod.fetch_minute_bars(
        symbols, date(2026, 7, 31), date(2026, 7, 31), workers=2
    )
    assert failed == ["920819.BJ"]
    assert df.height == 6


def test_parse_stamp_falls_back_to_the_datetime_string():
    # Missing the decoded int components entirely; only the formatted string
    # the parser also emits is usable.
    row = {"datetime": "2026-07-31 09:31"}
    assert _parse_stamp(row) == datetime(2026, 7, 31, 9, 31)


def test_parse_stamp_returns_none_when_nothing_parses():
    assert _parse_stamp({}) is None
    assert _parse_stamp({"datetime": ""}) is None
    assert _parse_stamp({"datetime": "not-a-timestamp"}) is None


def test_off_session_only_page_yields_no_rows():
    # Every bar on the page lands at lunch; nothing survives the session
    # filter, so the sweep must report an empty result, not crash on it.
    page = [_bar(datetime(2026, 7, 31, 12, 15)), _bar(datetime(2026, 7, 31, 12, 20))]
    rows = fetch_minute_bars_paginated(
        FakeClient([page]), "600519.SH", date(2026, 7, 31), date(2026, 7, 31)
    )
    assert rows == []


def test_fetch_stops_on_an_empty_but_not_none_page():
    # A full page followed by an explicitly empty (not exception-raising) page:
    # the sweep must stop cleanly rather than treat `[]` as a failure.
    full_page = (_session(date(2026, 7, 31)) * 4)[:800]
    client = FakeClient([full_page, []])
    rows = fetch_minute_bars_paginated(client, "600519.SH", date(2026, 7, 30), date(2026, 7, 31))
    assert len(client.calls) == 2
    assert len(rows) > 0


def test_on_page_fires_once_per_page_transition():
    full_page = (_session(date(2026, 7, 31)) * 4)[:800]
    short_page = _session(date(2026, 7, 30), count=10)
    client = FakeClient([full_page, short_page])
    calls: list[int] = []
    fetch_minute_bars_paginated(
        client, "600519.SH", date(2026, 7, 30), date(2026, 7, 31), on_page=lambda: calls.append(1)
    )
    # Two pages were fetched, so the "moving to the next page" callback fires
    # exactly once — never on the page that ends the sweep.
    assert len(calls) == 1


class _RaisesAfterFirstPage:
    """A page failure partway through a sweep — not the first page."""

    def __init__(self, first_page: list[dict]):
        self.first_page = first_page
        self.calls = 0

    def bars(self, symbol, frequency, market, start, offset):
        self.calls += 1
        if self.calls == 1:
            return self.first_page
        raise ConnectionError("host reset")


def test_midsweep_failure_keeps_rows_already_collected_when_not_backfill():
    full_page = (_session(date(2026, 7, 31)) * 4)[:800]
    client = _RaisesAfterFirstPage(full_page)
    # backfill=False (the daily-run default): a later page's failure must not
    # discard what the sweep already has, and must not raise past the caller.
    rows = fetch_minute_bars_paginated(
        client, "600519.SH", date(2020, 1, 1), date(2026, 7, 31), backfill=False
    )
    assert client.calls == 2
    assert len(rows) > 0


def test_midsweep_failure_raises_when_backfill():
    # A backfill needs a complete series or an honest failure, not a silently
    # truncated one — same contract as the daily bars pagination.
    full_page = (_session(date(2026, 7, 31)) * 4)[:800]
    client = _RaisesAfterFirstPage(full_page)
    with pytest.raises(TdxMinuteBarsError):
        fetch_minute_bars_paginated(
            client, "600519.SH", date(2020, 1, 1), date(2026, 7, 31), backfill=True
        )


def test_single_worker_calls_heartbeat_before_each_symbol(monkeypatch):
    client_mod = _patch_client(monkeypatch)
    symbols = [f"60000{i}.SH" for i in range(3)]
    beats: list[int] = []
    client_mod.fetch_minute_bars(
        symbols,
        date(2026, 7, 31),
        date(2026, 7, 31),
        workers=1,
        on_heartbeat=lambda: beats.append(1),
    )
    assert len(beats) == len(symbols)


def test_single_worker_records_a_raised_symbol_without_failing_the_batch(monkeypatch):
    from cn_market_lake.adapters.tdx_protocol import client as client_mod

    class Flaky:
        def bars(self, symbol, frequency, market, start, offset):
            if symbol == "000001" and start == 0:
                raise RuntimeError("bad symbol")
            return []

    monkeypatch.setattr(client_mod, "_quotes_client", lambda config: Flaky())
    monkeypatch.setattr(client_mod, "_close_quotes_client", lambda c: None)

    df, failed = client_mod.fetch_minute_bars(
        ["600519.SH", "000001.SZ"], date(2026, 7, 31), date(2026, 7, 31), workers=1
    )
    assert failed == ["000001.SZ"]
    assert df.is_empty()


def test_wire_client_unavailable_raises_a_named_error(monkeypatch):
    from cn_market_lake.adapters.tdx_protocol import client as client_mod

    monkeypatch.setattr(client_mod, "_CONNECT_RETRY_BACKOFF_SEC", 0)

    def _boom(config):
        raise ImportError("no wire module")

    monkeypatch.setattr(client_mod, "_quotes_client", _boom)
    with pytest.raises(client_mod.TdxSourceError, match="unavailable"):
        client_mod.fetch_minute_bars(["600519.SH"], date(2026, 7, 31), date(2026, 7, 31))


def test_general_fetch_failure_resets_server_cache_and_raises(monkeypatch):
    from cn_market_lake.adapters.tdx_protocol import client as client_mod

    monkeypatch.setattr(client_mod, "_CONNECT_RETRY_BACKOFF_SEC", 0)
    reset_calls = []
    monkeypatch.setattr(client_mod, "reset_tdx_server_cache", lambda: reset_calls.append(1))

    def _boom(config):
        raise RuntimeError("host unreachable")

    monkeypatch.setattr(client_mod, "_quotes_client", _boom)
    with pytest.raises(client_mod.TdxSourceError, match="TDX fetch failed"):
        client_mod.fetch_minute_bars(["600519.SH"], date(2026, 7, 31), date(2026, 7, 31))
    # Once between the two connect attempts, once more at the outer handler —
    # a dead server must not stay cached for the next batch to retry against.
    assert reset_calls == [1, 1]


def test_threaded_fetch_calls_heartbeat_once_per_symbol(monkeypatch):
    client_mod = _patch_client(monkeypatch)
    symbols = [f"60000{i}.SH" for i in range(6)]
    beats: list[int] = []
    client_mod.fetch_minute_bars(
        symbols,
        date(2026, 7, 31),
        date(2026, 7, 31),
        workers=3,
        on_heartbeat=lambda: beats.append(1),
    )
    # Lanes share one lock around the callback; the total must still be exact
    # even though multiple threads call it concurrently.
    assert len(beats) == len(symbols)


def test_unparseable_row_is_skipped_without_breaking_the_page():
    # A row neither the fast int-component path nor the string fallback can
    # parse (garbage, or a wire decode glitch) must be dropped silently,
    # not crash the rest of a perfectly good page.
    page = [
        _bar(datetime(2026, 7, 31, 9, 31)),
        {"datetime": "garbage", "open": 1, "high": 1, "low": 1, "close": 1, "vol": 1, "amount": 1},
        _bar(datetime(2026, 7, 31, 9, 32)),
    ]
    rows = fetch_minute_bars_paginated(
        FakeClient([page]), "600519.SH", date(2026, 7, 31), date(2026, 7, 31)
    )
    assert [r["bar_time"].time() for r in rows] == [time(9, 31), time(9, 32)]


def test_connect_with_retry_succeeds_after_one_failure(monkeypatch):
    from cn_market_lake.adapters.tdx_protocol import client as client_mod

    monkeypatch.setattr(client_mod, "_CONNECT_RETRY_BACKOFF_SEC", 0)
    reset_calls = []
    monkeypatch.setattr(client_mod, "reset_tdx_server_cache", lambda: reset_calls.append(1))

    attempts = []

    def flaky(config):
        attempts.append(1)
        if len(attempts) == 1:
            raise TimeoutError("timed out")
        return "a-client"

    monkeypatch.setattr(client_mod, "_quotes_client", flaky)
    assert client_mod._connect_with_retry(None) == "a-client"
    assert len(attempts) == 2
    # Re-probes rather than retrying the same server straight into the same timeout.
    assert reset_calls == [1]


def test_connect_with_retry_raises_the_last_error_once_exhausted(monkeypatch):
    from cn_market_lake.adapters.tdx_protocol import client as client_mod

    monkeypatch.setattr(client_mod, "_CONNECT_RETRY_BACKOFF_SEC", 0)
    monkeypatch.setattr(client_mod, "reset_tdx_server_cache", lambda: None)

    def always_fails(config):
        raise TimeoutError("timed out")

    monkeypatch.setattr(client_mod, "_quotes_client", always_fails)
    with pytest.raises(TimeoutError, match="timed out"):
        client_mod._connect_with_retry(None)
