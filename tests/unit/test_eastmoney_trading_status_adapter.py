"""EastMoney ST / suspension trading status adapter (full offline coverage)."""

from __future__ import annotations

from datetime import date

import cn_market_lake.adapters.eastmoney.trading_status as ts


class _FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeClient:
    def __init__(self, payload: dict | None = None, *, get_raises: Exception | None = None):
        self.payload = payload or {}
        self.get_raises = get_raises
        self.closed = False

    def get(self, url):
        if self.get_raises is not None:
            raise self.get_raises
        return _FakeResponse(self.payload)

    def close(self):
        self.closed = True


def test_exchange_from_code_all_branches():
    assert ts._exchange_from_code("600519") == "SH"
    assert ts._exchange_from_code("688111") == "SH"
    assert ts._exchange_from_code("920001") == "BJ"
    assert ts._exchange_from_code("000001") == "SZ"


def test_fetch_st_symbols_skips_non_all_a_items(monkeypatch):
    monkeypatch.setattr(
        ts,
        "fetch_clist_pages",
        lambda client, *, fields, fs, page_size: [
            {"f12": "810001", "f13": 1, "f14": "无效代码"},
            {"f12": "600519", "f13": 1, "f14": "贵州茅台"},
        ],
    )
    out = ts._fetch_st_symbols(client=object())  # type: ignore[arg-type]
    assert out == {"600519.SH"}


def test_fetch_suspended_symbols_parses_result_rows():
    payload = {
        "result": {
            "data": [
                {"SECURITY_CODE": "600519", "TRADE_MARKET": "SH"},
                {"SECURITY_CODE": "810001", "TRADE_MARKET": "SH"},  # excluded, not all_a
            ]
        }
    }
    client = _FakeClient(payload)
    out = ts._fetch_suspended_symbols(client, date(2024, 6, 28))
    assert out == {"600519.SH"}


def test_fetch_suspended_symbols_empty_on_transport_failure():
    client = _FakeClient(get_raises=RuntimeError("network down"))
    out = ts._fetch_suspended_symbols(client, date(2024, 6, 28))
    assert out == set()


def test_fetch_suspended_symbols_empty_result_key():
    client = _FakeClient({})
    out = ts._fetch_suspended_symbols(client, date(2024, 6, 28))
    assert out == set()


def test_fetch_trading_status_eastmoney_labels_suspended_st_and_normal(monkeypatch):
    monkeypatch.setattr(ts, "_fetch_st_symbols", lambda client: {"000002.SZ"})
    monkeypatch.setattr(ts, "_fetch_suspended_symbols", lambda client, trade_date: {"600519.SH"})
    trade_date = date(2024, 6, 28)
    client = _FakeClient()
    df = ts.fetch_trading_status_eastmoney(
        ["600519.SH", "000002.SZ", "000001.SZ"], trade_date, client=client
    )
    assert client.closed is False  # caller-owned client must survive
    rows = {r["symbol"]: r for r in df.iter_rows(named=True)}
    assert rows["600519.SH"]["status"] == "suspended"
    assert rows["600519.SH"]["is_trading"] is False
    assert rows["000002.SZ"]["status"] == "st"
    assert rows["000002.SZ"]["is_trading"] is True
    assert rows["000001.SZ"]["status"] == "normal"
    assert rows["000001.SZ"]["is_trading"] is True


def test_fetch_trading_status_eastmoney_takes_no_extra_st_source():
    """The AkShare ST union is gone; EastMoney's own list is the daily feed.

    `ak.stock_zh_a_st_em` queried the same push2 clist board with the same
    `fs` filter as `_fetch_st_symbols`, so unioning it added a retry, not a
    second opinion (issue #3). Guard the signature so it does not creep back.
    """
    import inspect

    params = inspect.signature(ts.fetch_trading_status_eastmoney).parameters
    assert "extra_st_symbols" not in params


def test_fetch_trading_status_eastmoney_owns_and_closes_default_client(monkeypatch):
    created: list[_FakeClient] = []

    def _factory(**kwargs):
        client = _FakeClient()
        created.append(client)
        return client

    monkeypatch.setattr(ts, "EastMoneyClient", _factory)
    monkeypatch.setattr(ts, "_fetch_st_symbols", lambda client: set())
    monkeypatch.setattr(ts, "_fetch_suspended_symbols", lambda client, trade_date: set())
    df = ts.fetch_trading_status_eastmoney(["000001.SZ"], date(2024, 6, 28))
    assert df.row(0, named=True)["status"] == "normal"
    assert created[0].closed is True
