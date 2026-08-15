"""TDX sector index bars adapter."""

from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from cn_market_lake.adapters.tdx_protocol.sector_bars import (
    _rows_with_change_pct,
    fetch_sector_index_bars,
    fetch_sector_index_bars_batch,
)
from cn_market_lake.config import Config


def test_rows_with_change_pct():
    rows = [
        {"trade_date": date(2026, 7, 9), "close": 100.0},
        {"trade_date": date(2026, 7, 10), "close": 110.0},
    ]
    out = _rows_with_change_pct(rows)
    assert out[1]["change_pct"] == pytest.approx(10.0)


def test_fetch_sector_index_bars_batch(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    routing = pl.DataFrame(
        [
            {
                "sector_code": "BK1343",
                "sector_name": "物业管理",
                "board_type": "industry",
                "ohlc_source": "tdx_protocol",
                "tdx_code": "881423",
            },
            {
                "sector_code": "BK1169",
                "sector_name": "Kimi概念",
                "board_type": "concept",
                "ohlc_source": "eastmoney",
                "tdx_code": None,
            },
        ]
    )
    fake_bars = [
        {
            "sector_code": "BK1343",
            "sector_name": "物业管理",
            "board_type": "industry",
            "trade_date": date(2026, 7, 14),
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": 1,
            "amount": 1.0,
            "change_pct": 0.0,
        }
    ]

    with patch(
        "cn_market_lake.adapters.tdx_protocol.sector_bars.fetch_sector_index_bars",
        return_value=fake_bars,
    ):
        df, failed, succeeded = fetch_sector_index_bars_batch(
            routing,
            date(2026, 7, 1),
            date(2026, 7, 14),
            config=cfg,
        )
    assert df.height == 1
    assert succeeded == ["BK1343"]
    assert failed == []


def test_fetch_sector_index_bars_pagination(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    fake = [
        {
            "trade_date": date(2026, 7, 14),
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.5,
            "volume": 1000,
            "amount": 5000.0,
        }
    ]

    with (
        patch(
            "cn_market_lake.adapters.tdx_protocol.sector_bars.fetch_bars_paginated",
            return_value=fake,
        ),
        patch(
            "cn_market_lake.adapters.tdx_protocol.sector_bars._quotes_client",
            return_value=MagicMock(),
        ),
        patch(
            "cn_market_lake.adapters.tdx_protocol.sector_bars.close_quotes_client",
        ),
    ):
        rows = fetch_sector_index_bars(
            sector_code="BK1343",
            sector_name="物业管理",
            board_type="industry",
            tdx_code="881423",
            start=date(2026, 7, 14),
            end=date(2026, 7, 14),
            config=cfg,
        )
    assert len(rows) == 1
    assert rows[0]["close"] == 10.5
    assert rows[0]["sector_code"] == "BK1343"
