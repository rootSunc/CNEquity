"""Tests for flash news wire adapter."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.adapters.eastmoney.news_wire import flash_rows_from_news


def test_flash_rows_add_wire_fields():
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

    df = flash_rows_from_news(base, date(2026, 7, 14))
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["wire_id"] == "eastmoney:n1"
    assert row["wire_source"] == "eastmoney"
    assert len(row["item_hash"]) == 16


def test_flash_rows_dedupe_duplicate_source_rows():
    base = pl.DataFrame(
        [
            {
                "news_id": "n1",
                "publish_date": date(2026, 7, 14),
                "publish_time": "10:00:00",
                "title": "旧标题",
                "summary": None,
                "related_symbols": "600519.SH",
                "channel": "fast_news",
            },
            {
                "news_id": "n1",
                "publish_date": date(2026, 7, 14),
                "publish_time": "10:01:00",
                "title": "新标题",
                "summary": None,
                "related_symbols": "600519.SH",
                "channel": "fast_news",
            },
        ]
    )
    df = flash_rows_from_news(base, date(2026, 7, 14))
    assert df.height == 1
    assert df["title"][0] == "新标题"
