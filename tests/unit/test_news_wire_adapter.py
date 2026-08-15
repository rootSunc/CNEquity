"""Tests for flash news wire adapter."""

from __future__ import annotations

from datetime import date

import polars as pl

from cn_market_lake.adapters.eastmoney.news_wire import fetch_flash_news_wire


def test_fetch_flash_news_wire_adds_wire_fields(monkeypatch):
    base = pl.DataFrame(
        [
            {
                "news_id": "n1",
                "publish_date": date(2026, 7, 14),
                "publish_time": "10:00:00",
                "title": "测试快讯",
                "summary": None,
                "related_symbols": "600519.SH",
                "channel": "fast_news",
            }
        ]
    )

    monkeypatch.setattr(
        "cn_market_lake.adapters.eastmoney.news_wire.fetch_news_headlines",
        lambda _d, page_size=200: base,
    )
    df = fetch_flash_news_wire(date(2026, 7, 14))
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["wire_id"] == "eastmoney:n1"
    assert row["wire_source"] == "eastmoney"
    assert len(row["item_hash"]) == 16
