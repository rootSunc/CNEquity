"""EastMoney ST / suspension trading status adapter (full offline coverage)."""

from __future__ import annotations

from datetime import date

import pytest

import cnequity.adapters.eastmoney.trading_status as ts
from cnequity.adapters.eastmoney import datacenter


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
        self.urls: list[str] = []

    def get(self, url):
        self.urls.append(url)
        if self.get_raises is not None:
            raise self.get_raises
        return _FakeResponse(self.payload)

    def close(self):
        self.closed = True


def test_exchange_from_code_all_branches():
    assert ts._exchange_from_code("600519") == "SH"
    assert ts._exchange_from_code("688111") == "SH"
    assert ts._exchange_from_code("920001") == "BJ"
    assert ts._exchange_from_code("830001") == "BJ"
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


def _row(code: str, start: str, end: str | None) -> dict:
    return {
        "SECURITY_CODE": code,
        "SUSPEND_START_DATE": start,
        "SUSPEND_END_TIME": end,
    }


def test_suspension_covers_new_columns():
    d = date(2026, 8, 18)
    assert ts._suspension_covers(_row("600984", "2026-08-11 00:00:00", None), d) is True
    assert ts._suspension_covers(_row("600984", "2026-08-11 00:00:00", "null"), d) is True
    assert (
        ts._suspension_covers(_row("600984", "2026-08-11 00:00:00", "2026-08-18 09:30:00"), d)
        is True
    )
    assert (
        ts._suspension_covers(_row("600984", "2026-08-11 00:00:00", "2026-08-17 09:30:00"), d)
        is False
    )
    assert ts._suspension_covers(_row("600984", "2026-08-20 00:00:00", None), d) is False


def test_fetch_suspended_symbols_uses_new_contract_filter(monkeypatch):
    from urllib.parse import unquote

    payload = {"result": {"data": [_row("600984", "2026-08-11 00:00:00", None)]}}
    client = _FakeClient(payload)
    out = ts._fetch_suspended_symbols(client, date(2026, 8, 18))

    assert out == {"600984.SH"}
    assert len(client.urls) == len(ts._SUSPEND_MARKETS)
    for url, market in zip(client.urls, ts._SUSPEND_MARKETS, strict=False):
        decoded = unquote(url)
        assert "DATETIME='2026-08-18'" in decoded
        assert f'MARKET="{market}"' in decoded


def test_fetch_suspended_symbols_dedupes_codes_across_markets(monkeypatch):
    payload = {
        "result": {
            "data": [
                _row("600984", "2026-08-11 00:00:00", None),
                _row("600984", "2026-08-11 00:00:00", None),
                _row("810001", "2026-08-11 00:00:00", None),  # not all_a → excluded
            ]
        }
    }
    client = _FakeClient(payload)
    out = ts._fetch_suspended_symbols(client, date(2026, 8, 18))
    assert out == {"600984.SH"}


def test_fetch_suspended_symbols_raises_when_all_batches_empty():
    empty_batch = {"success": False, "message": "返回数据为空", "code": 9201}
    client = _FakeClient(empty_batch)
    with pytest.raises(RuntimeError, match="empty across all markets"):
        ts._fetch_suspended_symbols(client, date(2026, 8, 18))


def test_fetch_suspended_symbols_paginates_a_single_market():
    page1 = {
        "success": True,
        "result": {
            "pages": 2,
            "count": 501,
            "data": [_row("600984", "2026-08-11 00:00:00", None)] * 500,
        },
    }
    page2 = {
        "success": True,
        "result": {
            "pages": 2,
            "count": 501,
            "data": [_row("000001", "2026-08-18 00:00:00", None)],
        },
    }
    empty_batch = {"success": False, "message": "返回数据为空", "code": 9201}

    class _PagedClient:
        def __init__(self):
            self.responses = [page1, page2] + [empty_batch] * (len(ts._SUSPEND_MARKETS) - 1)
            self.urls: list[str] = []

        def get(self, url):
            self.urls.append(url)
            return _FakeResponse(self.responses.pop(0))

    client = _PagedClient()
    out = ts._fetch_suspended_symbols(client, date(2026, 8, 18))

    assert out == {"600984.SH", "000001.SZ"}
    assert len(client.urls) == len(ts._SUSPEND_MARKETS) + 1  # one extra page on market 1
    assert "pageNumber=2" in client.urls[1]


def test_fetch_suspended_symbols_rejects_rows_outside_requested_interval():
    client = _FakeClient(
        {
            "result": {
                "data": [
                    _row("600519", "2026-06-01 00:00:00", "2026-06-27 00:00:00"),
                ]
            }
        }
    )

    with pytest.raises(RuntimeError, match="2026-06-28"):
        ts._fetch_suspended_symbols(client, date(2026, 6, 28))


def test_fetch_suspended_symbols_raises_on_malformed_response():
    client = _FakeClient({})
    with pytest.raises(RuntimeError, match="without a result object"):
        ts._fetch_suspended_symbols(client, date(2026, 6, 28))


def test_fetch_suspended_symbols_rejects_non_object_rows():
    client = _FakeClient({"result": {"data": [None]}})
    with pytest.raises(RuntimeError, match="non-object row"):
        ts._fetch_suspended_symbols(client, date(2026, 6, 28))


def test_fetch_suspended_symbols_raises_on_transport_failure(monkeypatch):
    monkeypatch.setattr(datacenter.time, "sleep", lambda _seconds: None)
    client = _FakeClient(get_raises=RuntimeError("network down"))
    with pytest.raises(RuntimeError, match="network down"):
        ts._fetch_suspended_symbols(client, date(2024, 6, 28))


def test_fetch_st_symbols_propagates_transport_failure(monkeypatch):
    def _boom(*_args, **_kwargs):
        raise RuntimeError("ST list unavailable")

    monkeypatch.setattr(ts, "fetch_clist_pages", _boom)
    with pytest.raises(RuntimeError, match="ST list unavailable"):
        ts._fetch_st_symbols(client=object())  # type: ignore[arg-type]


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


def test_fetch_trading_status_eastmoney_dedupes_input_symbols(monkeypatch):
    monkeypatch.setattr(ts, "_fetch_st_symbols", lambda client: set())
    monkeypatch.setattr(ts, "_fetch_suspended_symbols", lambda client, trade_date: set())
    df = ts.fetch_trading_status_eastmoney(
        ["000001.SZ", "000001.SZ"], date(2024, 6, 28), client=_FakeClient()
    )
    assert df.height == 1


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


def test_fetch_trading_status_eastmoney_closes_owned_client_on_failure(monkeypatch):
    created: list[_FakeClient] = []

    def _factory(**kwargs):
        client = _FakeClient()
        created.append(client)
        return client

    monkeypatch.setattr(ts, "EastMoneyClient", _factory)

    def _boom(_client):
        raise RuntimeError("ST list unavailable")

    monkeypatch.setattr(ts, "_fetch_st_symbols", _boom)
    with pytest.raises(RuntimeError, match="ST list unavailable"):
        ts.fetch_trading_status_eastmoney(["000001.SZ"], date(2024, 6, 28))
    assert created[0].closed is True
