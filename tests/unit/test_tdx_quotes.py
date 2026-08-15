"""Offline unit tests for the Quotes facade over the vendored wire client."""

from __future__ import annotations

import pytest

from cn_market_lake.adapters.tdx_protocol import quotes as q


class _FakeWire:
    def __init__(self):
        self.closed = False
        self.calls: list[tuple] = []
        self.bars_rows = [{"vol": 10, "close": 1.0}]
        self.index_rows = [{"vol": 2, "close": 3.0}]
        self.xdxr_rows = [{"category": 1}]
        self.count = 0
        self.list_pages: dict[int, list[dict]] = {}

    def connect(self, host, port, time_out=10):
        self.calls.append(("connect", host, port, time_out))

    def close(self):
        self.closed = True
        self.calls.append(("close",))

    def get_security_bars(self, frequency, market, symbol, start, offset):
        self.calls.append(("bars", frequency, market, symbol, start, offset))
        return list(self.bars_rows)

    def get_index_bars(self, frequency, market, symbol, start, offset):
        self.calls.append(("index", frequency, market, symbol, start, offset))
        return list(self.index_rows)

    def get_xdxr_info(self, market, symbol):
        self.calls.append(("xdxr", market, symbol))
        if self.xdxr_rows is None:
            return None
        return list(self.xdxr_rows)

    def get_security_count(self, market):
        self.calls.append(("count", market))
        return self.count

    def get_security_list(self, market, start):
        self.calls.append(("list", market, start))
        return list(self.list_pages.get(start, []))


@pytest.fixture
def fake_wire(monkeypatch):
    wire = _FakeWire()

    def fake_client(*, multithread=False, heartbeat=False):
        wire.calls.append(("ctor", multithread, heartbeat))
        return wire

    monkeypatch.setattr(q, "TdxWireClient", fake_client)
    return wire


def test_market_for_stock_and_index():
    assert q.market_for_stock("600519") == q.MARKET_SH
    assert q.market_for_stock("000001") == q.MARKET_SZ
    assert q.market_for_index("000001") == q.MARKET_SH
    assert q.market_for_index("399001") == q.MARKET_SZ
    assert q.market_for_index("880001") == q.MARKET_SH


def test_with_volume_aliases_and_empty():
    assert q._with_volume(None) == []
    assert q._with_volume([]) == []
    rows = [{"vol": 5, "close": 1.0}]
    assert q._with_volume(rows)[0]["volume"] == 5
    # do not overwrite an existing volume key
    kept = [{"vol": 1, "volume": 9}]
    assert q._with_volume(kept)[0]["volume"] == 9


def test_factory_connects_and_close_is_idempotent(fake_wire):
    client = q.Quotes.factory(server=("1.2.3.4", 7709), timeout=7, heartbeat=True)
    assert ("connect", "1.2.3.4", 7709, 7) in fake_wire.calls
    assert client.server == ("1.2.3.4", 7709)
    client.close()
    assert fake_wire.closed
    # close must swallow wire errors
    fake_wire.close = lambda: (_ for _ in ()).throw(RuntimeError("boom"))
    client.close()


def test_context_manager_closes(fake_wire):
    with q.Quotes.factory(server=("9.9.9.9", 7709)) as client:
        assert client.server[0] == "9.9.9.9"
    assert fake_wire.closed


def test_bars_honours_explicit_market_and_caps_page(fake_wire):
    client = q.Quotes(fake_wire, ("h", 1))
    rows = client.bars("000001", market=1, start=3, offset=5000)
    assert rows[0]["volume"] == 10
    assert ("bars", q.CATEGORY_DAILY, 1, "000001", 3, q.MAX_PAGE) in fake_wire.calls


def test_bars_falls_back_to_prefix_market(fake_wire):
    client = q.Quotes(fake_wire, ("h", 1))
    client.bars("600519")
    assert any(c[:4] == ("bars", q.CATEGORY_DAILY, q.MARKET_SH, "600519") for c in fake_wire.calls)


def test_index_uses_index_market_map(fake_wire):
    client = q.Quotes(fake_wire, ("h", 1))
    rows = client.index("000001", start=0, offset=10)
    assert rows[0]["volume"] == 2
    assert ("index", q.CATEGORY_DAILY, q.MARKET_SH, "000001", 0, 10) in fake_wire.calls


def test_xdxr_and_empty_wire_response(fake_wire):
    client = q.Quotes(fake_wire, ("h", 1))
    assert client.xdxr("600519") == [{"category": 1}]
    fake_wire.xdxr_rows = None  # type: ignore[assignment]
    assert client.xdxr("000001", market=0) == []


def test_stocks_pages_until_empty(fake_wire):
    fake_wire.count = 2500
    fake_wire.list_pages = {
        0: [{"code": "000001"}],
        1000: [{"code": "000002"}],
        2000: [],
    }
    client = q.Quotes(fake_wire, ("h", 1))
    out = client.stocks(q.MARKET_SZ)
    assert [r["code"] for r in out] == ["000001", "000002"]
    assert ("count", q.MARKET_SZ) in fake_wire.calls


def test_stocks_rejects_unknown_market(fake_wire):
    client = q.Quotes(fake_wire, ("h", 1))
    with pytest.raises(ValueError, match="unsupported TDX market"):
        client.stocks(9)


def test_stocks_zero_count(fake_wire):
    fake_wire.count = 0
    client = q.Quotes(fake_wire, ("h", 1))
    assert client.stocks(q.MARKET_SH) == []
