"""Unit tests for Sina offshore commodity bars (COMEX gold)."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl

from cn_market_lake.adapters.sina.global_futures import (
    OFFSHORE_CONTRACTS,
    fetch_offshore_commodity_bars_range,
)
from cn_market_lake.domain.schemas import validate_dataframe, with_provenance


def test_offshore_contracts_unique():
    syms = [c[0] for c in OFFSHORE_CONTRACTS]
    assert len(syms) == len(set(syms))
    assert ("GC0.CMX", "GC", "COMEX黄金", "CMX") in OFFSHORE_CONTRACTS


def test_fetch_offshore_parses_sina_payload():
    payload = [
        {
            "date": "2026-07-20",
            "open": "4022.700",
            "high": "4046.000",
            "low": "3986.500",
            "close": "4011.800",
            "volume": "0",
            "position": "12",
            "settlement": "0",
        },
        {
            "date": "2026-07-21",
            "open": "4011.800",
            "high": "4088.400",
            "low": "4003.300",
            "close": "4077.700",
            "volume": "0",
            "position": "0",
            "settlement": "0",
        },
        {
            "date": "2019-01-02",
            "open": "1",
            "high": "1",
            "low": "1",
            "close": "1",
            "volume": "0",
            "position": "0",
            "settlement": "0",
        },
    ]
    fake = MagicMock()
    fake.get.return_value = MagicMock(
        raise_for_status=MagicMock(),
        json=MagicMock(return_value=payload),
    )
    with patch("cn_market_lake.adapters.sina.global_futures.httpx.Client", return_value=fake):
        fake.__enter__ = MagicMock(return_value=fake)
        fake.__exit__ = MagicMock(return_value=None)
        # Client is constructed as context? Our code doesn't use context manager —
        # it constructs Client() and calls .get / .close
        client = MagicMock()
        client.get.return_value = MagicMock(
            raise_for_status=MagicMock(),
            json=MagicMock(return_value=payload),
        )
        df = fetch_offshore_commodity_bars_range(
            date(2026, 7, 20),
            date(2026, 7, 21),
            client=client,
        )
    assert df.height == 2
    assert set(df["symbol"].to_list()) == {"GC0.CMX"}
    assert df.filter(pl.col("trade_date") == date(2026, 7, 21))["close"][0] == 4077.7
    assert df["source"].unique().to_list() == ["sina"]
    validated = validate_dataframe(
        with_provenance(df, source="eastmoney", data_version="v1"),
        "commodity_bars",
    )
    assert validated["source"].unique().to_list() == ["sina"]


def test_fetch_offshore_empty_on_bad_payload():
    client = MagicMock()
    client.get.return_value = MagicMock(
        raise_for_status=MagicMock(),
        json=MagicMock(return_value={"error": True}),
    )
    df = fetch_offshore_commodity_bars_range(date(2026, 7, 21), date(2026, 7, 21), client=client)
    assert df.is_empty()
