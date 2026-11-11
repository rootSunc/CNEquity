"""Multi-source flash news wire (MVP: EastMoney fast news)."""

from __future__ import annotations

import hashlib
import logging
from datetime import date

import polars as pl

from cnequity.domain.schemas import frame_from_rows

logger = logging.getLogger(__name__)


def _item_hash(title: str, wire_source: str, published_at: str) -> str:
    raw = f"{wire_source}|{published_at}|{title}".encode()
    return hashlib.sha256(raw).hexdigest()[:16]


def flash_rows_from_news(base: pl.DataFrame, trade_date: date | None = None) -> pl.DataFrame:
    """``flash_news_wire`` rows from ``news_headlines``-shaped rows.

    Shared by the per-day fetch above and the shared incremental news feed
    (``steps/news_feed.py``), so both datasets come from one request.
    """
    if base.is_empty():
        return base

    rows: list[dict] = []
    for row in base.iter_rows(named=True):
        title = str(row.get("title") or "").strip()
        news_id = str(row.get("news_id") or "").strip()
        if not title or not news_id:
            continue
        wire_source = "eastmoney"
        pub_date = row.get("publish_date") or trade_date
        pub_time = str(row.get("publish_time") or "00:00:00")
        published_at = f"{pub_date}T{pub_time}"
        item = {
            "wire_id": f"{wire_source}:{news_id}",
            "wire_source": wire_source,
            "item_hash": _item_hash(title, wire_source, published_at),
            "publish_date": pub_date,
            "publish_time": pub_time,
            "title": title,
            "summary": row.get("summary"),
            "related_symbols": row.get("related_symbols"),
            "importance": None,
            "channel": row.get("channel") or "fast_news",
        }
        for column in ("source", "data_version", "fetched_at"):
            if column in row:
                item[column] = row[column]
        rows.append(item)
    if not rows:
        return pl.DataFrame()
    return frame_from_rows(rows, "flash_news_wire").unique(
        subset=["wire_id", "wire_source"], keep="last"
    )
