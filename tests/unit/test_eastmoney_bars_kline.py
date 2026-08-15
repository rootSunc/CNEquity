"""EastMoney per-symbol historical kline fetch (adapters/eastmoney/bars.py)."""

from __future__ import annotations

from datetime import date

from cn_market_lake.adapters.eastmoney.bars import _secid, fetch_daily_bars


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _Client:
    def __init__(self, klines_by_symbol=None, *, raise_for=None):
        self.klines_by_symbol = klines_by_symbol or {}
        self.raise_for = raise_for or set()
        self.closed = False
        self.calls: list[dict] = []

    def get(self, url, params=None):
        self.calls.append(params or {})
        secid = (params or {}).get("secid")
        if secid in self.raise_for:
            raise RuntimeError("kline fetch failed")
        klines = self.klines_by_symbol.get(secid, [])
        return _Response({"data": {"klines": klines}})

    def close(self):
        self.closed = True


def test_secid_maps_exchange_to_market_code():
    assert _secid("600519.SH") == "1.600519"
    assert _secid("000001.SZ") == "0.000001"
    assert _secid("920001.BJ") == "2.920001"


def test_fetch_daily_bars_parses_kline_rows():
    klines = ["20240628,10.0,10.5,11.0,9.0,1000,10500.0"]
    client = _Client({"1.600519": klines})
    df = fetch_daily_bars(["600519.SH"], date(2024, 6, 1), date(2024, 6, 28), client=client)
    assert client.closed is False
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["symbol"] == "600519.SH"
    assert row["trade_date"] == date(2024, 6, 28)
    assert row["open"] == 10.0
    assert row["close"] == 10.5
    assert row["high"] == 11.0
    assert row["low"] == 9.0
    assert row["volume"] == 100_000  # 1000 手 → 股
    assert row["amount"] == 10500.0


def test_fetch_daily_bars_skips_short_and_unparsable_lines():
    klines = [
        "too,short",
        "not-a-date,10.0,10.5,11.0,9.0,1000,10500.0",
        "20240628,10.0,10.5,11.0,9.0,1000,10500.0",
    ]
    client = _Client({"0.000001": klines})
    df = fetch_daily_bars(["000001.SZ"], date(2024, 6, 1), date(2024, 6, 28), client=client)
    assert df.height == 1
    assert df.row(0, named=True)["trade_date"] == date(2024, 6, 28)


def test_fetch_daily_bars_continues_after_symbol_failure():
    client = _Client(
        {"1.600519": ["20240628,10.0,10.5,11.0,9.0,1000,10500.0"]},
        raise_for={"0.000001"},
    )
    df = fetch_daily_bars(
        ["000001.SZ", "600519.SH"], date(2024, 6, 1), date(2024, 6, 28), client=client
    )
    assert df.height == 1
    assert df.row(0, named=True)["symbol"] == "600519.SH"


def test_fetch_daily_bars_empty_when_no_rows():
    client = _Client({})
    df = fetch_daily_bars(["600519.SH"], date(2024, 6, 1), date(2024, 6, 28), client=client)
    assert df.is_empty()


def test_fetch_daily_bars_owns_and_closes_default_client(monkeypatch):
    created: list[_Client] = []

    def _factory(**kwargs):
        client = _Client({})
        created.append(client)
        return client

    monkeypatch.setattr("cn_market_lake.adapters.eastmoney.bars.EastMoneyClient", _factory)
    df = fetch_daily_bars(["600519.SH"], date(2024, 6, 1), date(2024, 6, 28))
    assert df.is_empty()
    assert created[0].closed is True
