"""Multi-source flash news wire (MVP: EastMoney fast news)."""

from __future__ import annotations

import hashlib
import logging
from datetime import date

import polars as pl

from cnequity.adapters.eastmoney.rotation import fetch_news_headlines
from cnequity.domain.schemas import frame_from_rows

logger = logging.getLogger(__name__)


def _item_hash(title: str, wire_source: str, published_at: str) -> str:
    raw = f"{wire_source}|{published_at}|{title}".encode()
    return hashlib.sha256(raw).hexdigest()[:16]


def fetch_flash_news_wire(
    trade_date: date,
    *,
    page_size: int = 200,
    config=None,
    run_id: str | None = None,
) -> pl.DataFrame:
    """EastMoney 7×24 fast news with wire_id / item_hash for cross-source dedupe."""
    if config is None:
        base = fetch_news_headlines(trade_date, page_size=page_size)
    else:
        base = fetch_news_headlines(
            trade_date,
            page_size=page_size,
            config=config,
            run_id=run_id,
            archive_dataset="flash_news_wire",
        )
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
        rows.append(
            {
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
        )
    if not rows:
        return pl.DataFrame()
    return frame_from_rows(rows, "flash_news_wire").unique(
        subset=["wire_id", "wire_source"], keep="last"
    )
