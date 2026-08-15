"""The TDX adapter must fail loudly instead of fabricating data.

Mock rows are only allowed behind allow_mock=True and must be labeled
source="mock" so audit can reject them downstream.
"""

from datetime import date

import polars as pl
import pytest

from cn_market_lake.adapters.tdx_protocol import client as tdx
from cn_market_lake.domain.schemas import MOCK_SOURCE, with_provenance

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


def test_trading_status_raises_without_allow_mock(monkeypatch):
    def _boom(*_a, **_kw):
        raise RuntimeError("simulated EastMoney outage")

    monkeypatch.setattr(
        "cn_market_lake.adapters.tdx_protocol.client.fetch_trading_status_eastmoney",
        _boom,
    )
    with pytest.raises(tdx.TdxSourceError, match="trading_status"):
        tdx.fetch_trading_status(["600519.SH"], END)


def test_mock_rows_are_labeled_and_survive_normalization():
    df = tdx.fetch_daily_bars(["600519.SH"], START, END, allow_mock=True)
    assert df.height > 0
    assert set(df["source"].unique().to_list()) == {MOCK_SOURCE}

    normalized = tdx.normalize_with_source(df)
    assert set(normalized["source"].unique().to_list()) == {MOCK_SOURCE}


def test_real_rows_get_real_source_label():
    df = with_provenance(
        tdx._mock_bars(["600519.SH"], START, END).drop("source"),
        source="tdx_protocol",
        data_version="v1",
    )
    assert set(df["source"].unique().to_list()) == {"tdx_protocol"}
