"""The TDX adapter must fail loudly instead of fabricating data.

Mock rows are only allowed behind allow_mock=True and must be labeled
source="mock" so audit can reject them downstream.
"""

from datetime import date
from threading import Event

import polars as pl
import pytest

from cnequity.adapters.tdx_protocol import client as tdx
from cnequity.config import Config
from cnequity.domain.schemas import MOCK_SOURCE, with_provenance

START = date(2024, 6, 24)
END = date(2024, 6, 28)


@pytest.fixture(autouse=True)
def _no_tdx_client(monkeypatch):
    def _boom(_config=None):
        raise RuntimeError("simulated TDX outage")

    monkeypatch.setattr(tdx, "_quotes_client", _boom)


def test_instruments_raises_on_partial_market_failure(monkeypatch):
    import pandas as pd

    class _FakeClient:
        def stocks(self, *, market: int):
            if market == 0:
                raise RuntimeError("SZ timeout")
            return pd.DataFrame({"code": ["600519"], "name": ["贵州茅台"]})

    monkeypatch.setattr(tdx, "_quotes_client", lambda _config=None: _FakeClient())

    with pytest.raises(tdx.TdxSourceError, match="market fetch failed"):
        tdx.fetch_instruments(allow_mock=False)


def test_instruments_raises_without_allow_mock():
    with pytest.raises(tdx.TdxSourceError, match="instruments"):
        tdx.fetch_instruments()


def test_daily_bars_raises_without_allow_mock():
    with pytest.raises(tdx.TdxSourceError, match="daily_bars"):
        tdx.fetch_daily_bars(["600519.SH"], START, END)


def test_trading_calendar_uses_seed_without_mock():
    cal = tdx.fetch_trading_calendar(START, END, allow_mock=False)
    assert cal.height == (END - START).days + 1
    assert "is_trading" in cal.columns
    assert cal.filter(pl.col("trade_date") == date(2024, 6, 28))["is_trading"][0] is True


def test_corporate_actions_raises_without_allow_mock_on_backfill_path():
    with pytest.raises(tdx.TdxSourceError, match="corporate_actions"):
        tdx.fetch_corporate_actions(date(2024, 6, 28), primary_only=True)


def test_corporate_actions_default_backfill_reaches_research_floor(monkeypatch, tmp_path):
    row = pl.DataFrame(
        {
            "symbol": ["600849.SH"],
            "ex_date": [date(2005, 8, 19)],
            "action_type": ["cash_dividend"],
            "cash_dividend": [0.08],
            "bonus_ratio": [0.0],
            "transfer_ratio": [0.0],
            "allotment_ratio": [None],
            "allotment_price": [None],
        },
        schema_overrides={"allotment_ratio": pl.Float64, "allotment_price": pl.Float64},
    )
    monkeypatch.setattr(tdx, "fetch_corporate_actions_tdx", lambda *args, **kwargs: row)

    out = tdx.fetch_corporate_actions(
        date(2005, 12, 31),
        symbols=["600849.SH"],
        backfill=True,
        primary_only=True,
        config=Config(data_root=tmp_path / "data"),
    )

    assert out.select("ex_date").to_series().to_list() == [date(2005, 8, 19)]


def test_trading_status_raises_without_allow_mock(monkeypatch):
    def _boom(*_a, **_kw):
        raise RuntimeError("simulated EastMoney outage")

    monkeypatch.setattr(
        "cnequity.adapters.tdx_protocol.client.fetch_trading_status_eastmoney",
        _boom,
    )
    with pytest.raises(tdx.TdxSourceError, match="trading_status"):
        tdx.fetch_trading_status(["600519.SH"], END)


def test_mock_rows_are_labeled_and_survive_normalization():
    df = tdx.fetch_daily_bars(["600519.SH"], START, END, allow_mock=True)
    assert df.height > 0
    assert set(df["source"].unique().to_list()) == {MOCK_SOURCE}

    normalized = tdx.normalize_with_source(df, "tdx_protocol")
    assert set(normalized["source"].unique().to_list()) == {MOCK_SOURCE}


def test_real_rows_get_real_source_label():
    df = with_provenance(
        tdx._mock_bars(["600519.SH"], START, END).drop("source"),
        source="tdx_protocol",
        data_version="v1",
    )
    assert set(df["source"].unique().to_list()) == {"tdx_protocol"}


def test_daily_bars_dedupes_duplicate_symbol_input(monkeypatch):
    row = {"symbol": "600519.SH", "trade_date": START, "close": 1.0}
    monkeypatch.setattr(tdx, "_quotes_client", lambda _config=None: object())
    monkeypatch.setattr(tdx, "_close_quotes_client", lambda _client: None)
    monkeypatch.setattr(tdx, "fetch_bars_paginated", lambda *_a, **_k: [row])

    out = tdx.fetch_daily_bars(["600519.SH", "600519.SH"], START, END)
    assert out.height == 1


def test_daily_bars_keeps_other_symbols_when_one_symbol_page_fails(monkeypatch):
    row = {
        "symbol": "600519.SH",
        "trade_date": START,
        "open": 1.0,
        "high": 1.0,
        "low": 1.0,
        "close": 1.0,
        "volume": 100,
        "amount": 100.0,
    }
    monkeypatch.setattr(tdx, "_quotes_client", lambda _config=None: object())
    monkeypatch.setattr(tdx, "_close_quotes_client", lambda _client: None)

    def _fetch(_client, symbol, *_args, **_kwargs):
        if symbol == "000001.SZ":
            raise RuntimeError("one symbol timed out")
        return [row]

    monkeypatch.setattr(tdx, "fetch_bars_paginated", _fetch)

    out = tdx.fetch_daily_bars(["000001.SZ", "600519.SH"], START, END)

    assert out["symbol"].to_list() == ["600519.SH"]


def test_daily_bars_abandons_a_hung_symbol_request(monkeypatch):
    row = {
        "symbol": "600519.SH",
        "trade_date": START,
        "open": 1.0,
        "high": 1.0,
        "low": 1.0,
        "close": 1.0,
        "volume": 100,
        "amount": 100.0,
    }
    release = Event()
    monkeypatch.setattr(tdx, "_quotes_client", lambda _config=None: object())
    monkeypatch.setattr(tdx, "_close_quotes_client", lambda _client: None)
    monkeypatch.setattr(tdx, "_TDX_SYMBOL_REQUEST_TIMEOUT_SECONDS", 0.01)

    def _fetch(_client, symbol, *_args, **_kwargs):
        if symbol == "000001.SZ":
            release.wait(1.0)
            return []
        return [row]

    monkeypatch.setattr(tdx, "fetch_bars_paginated", _fetch)

    out = tdx.fetch_daily_bars(["600519.SH", "000001.SZ"], START, END)
    release.set()

    assert out["symbol"].to_list() == ["600519.SH"]


def test_provenance_cannot_be_stamped_without_naming_the_vendor():
    """The default was `"tdx_protocol"`, and it answered for callers that had
    nothing to do with TDX.

    That is how 58,672 `trading_status` rows came to name a vendor serving no
    status feed: the step called this with no source, and the default filled
    it in. The forwarder that produced those rows still lives in this very
    module — `fetch_trading_status` goes straight to EastMoney — so the next
    caller is one keyword away from repeating it. Requiring the argument is
    what makes the mistake impossible rather than merely fixed once.
    """
    import inspect

    signature = inspect.signature(tdx.normalize_with_source)

    assert signature.parameters["source"].default is inspect.Parameter.empty
    with pytest.raises(TypeError):
        tdx.normalize_with_source(pl.DataFrame({"symbol": ["600519.SH"]}))
