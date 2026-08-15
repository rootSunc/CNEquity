"""Offline coverage for Sina daily kline adapter."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest

from cn_market_lake.adapters.sina import bars as sina


def test_parse_payload_null_and_bad_json():
    assert sina._parse_payload("") is None
    assert sina._parse_payload("null") is None
    assert sina._parse_payload('[{"day":"2024-01-02"}]') == [{"day": "2024-01-02"}]
    with pytest.raises(sina.SinaBarsError):
        sina._parse_payload("not-json")


def test_symbol_exists_and_fetch_filters(monkeypatch):
    rows = [
        {
            "day": "2024-01-02",
            "open": "10",
            "high": "11",
            "low": "9",
            "close": "10.5",
            "volume": "1500",
        },
        {"day": "bad", "open": "1", "high": "1", "low": "1", "close": "1", "volume": "100"},
        {
            "day": "2024-01-03",
            "open": "10.5",
            "high": "12",
            "low": "10",
            "close": "11",
            "volume": "2500",
        },
    ]
    monkeypatch.setattr(sina, "_request", lambda symbol, datalen, client: rows)
    assert sina.symbol_exists("600519.SH") == date(2024, 1, 3)

    monkeypatch.setattr(sina, "_request", lambda symbol, datalen, client: None)
    assert sina.symbol_exists("999999.SZ") is None

    monkeypatch.setattr(sina, "_request", lambda symbol, datalen, client: rows)
    df = sina.fetch_daily_bars_sina(
        "600519.SH",
        start=date(2024, 1, 3),
        end=date(2024, 1, 3),
    )
    assert df.height == 1
    assert df["volume"][0] == 2500  # 股 passes through — Sina already reports shares
    assert df["amount"][0] is None


def test_fetch_unknown_symbol_empty_schema(monkeypatch):
    monkeypatch.setattr(sina, "_request", lambda symbol, datalen, client: None)
    df = sina.fetch_daily_bars_sina("000000.SH")
    assert df.is_empty()
    assert "symbol" in df.schema


def test_synthetic_post_delisting_terminal_copy_is_rejected(monkeypatch):
    rows = [
        {
            "day": "2020-07-31",
            "open": "0.31",
            "high": "0.31",
            "low": "0.29",
            "close": "0.30",
            "volume": "14132651",
        },
        {
            "day": "2021-06-27",
            "open": "0.310",
            "high": "0.310",
            "low": "0.290",
            "close": "0.300",
            "volume": "14132651",
        },
    ]
    monkeypatch.setattr(sina, "_request", lambda symbol, datalen, client: rows)

    assert sina.symbol_exists("300028.SZ") == date(2020, 7, 31)
    assert sina.fetch_daily_bars_sina("300028.SZ")["trade_date"].to_list() == [date(2020, 7, 31)]


def test_symbol_probe_uses_last_positive_volume_bar(monkeypatch):
    rows = [
        {"day": "2026-06-08", "volume": "100"},
        {"day": "2026-06-09", "volume": "0"},
    ]
    seen = {}

    def request(symbol, datalen, client):
        seen["datalen"] = datalen
        return rows

    monkeypatch.setattr(sina, "_request", request)

    assert sina.symbol_exists("688287.SH") == date(2026, 6, 8)
    assert seen["datalen"] > 1


def test_request_uses_client(monkeypatch):
    class Resp:
        text = '[{"day":"2024-01-02","open":"1","high":"1","low":"1","close":"1","volume":"100"}]'

        def raise_for_status(self):
            return None

    seen = {}

    def get(url, params=None, headers=None):
        seen["params"] = params
        return Resp()

    client = SimpleNamespace(get=get, close=lambda: None)
    monkeypatch.setattr(sina, "to_sina_symbol", lambda s: "sh600519")
    out = sina._request("600519.SH", 1, client)
    assert out is not None
    assert seen["params"]["symbol"] == "sh600519"
