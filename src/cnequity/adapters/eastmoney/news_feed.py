"""EastMoney 7×24 fast news, fetched since the newest item the lake holds.

The old fetch asked for "today" (Beijing) from the newest item backwards and
dropped everything dated otherwise. Run every 15 minutes, that re-paged the
whole day each time, and — because a run just after midnight asks for the new
day — dropped whatever was published between the day's last run and midnight;
a day the job did not run was lost although the API still pages back to it.
``news_headlines`` and ``flash_news_wire`` also each paged the same list.

This fetch walks newest-first until it passes *since* and keeps every item at
or after it, whatever its date; each row carries its own publish date. The
caller shares one fetch between both datasets (``steps/news_feed.py``), and
the exact page bytes are recorded so each dataset archives its own copy.
"""

from __future__ import annotations

import base64
import logging
from datetime import datetime, timezone
from urllib.parse import urlencode

import polars as pl

from cnequity.adapters.eastmoney.em_auth import EastMoneyClient
from cnequity.adapters.eastmoney.rotation import (
    _NEWS_MAX_PAGES,
    _NEWS_URL,
    _news_symbols,
    _object_rows,
)
from cnequity.domain.market_time import SHANGHAI_TZ

logger = logging.getLogger(__name__)
CST = SHANGHAI_TZ

NEWS_COLUMNS = {
    "news_id": pl.Utf8,
    "publish_date": pl.Date,
    "publish_time": pl.Utf8,
    "title": pl.Utf8,
    "summary": pl.Utf8,
    "related_symbols": pl.Utf8,
    "channel": pl.Utf8,
}


def _item_row(item: dict) -> tuple[dict | None, datetime | None]:
    show_time = str(item.get("showTime") or "")
    try:
        published = datetime.fromisoformat(show_time).replace(tzinfo=CST)
    except ValueError:
        return None, None
    title = str(item.get("title") or "").strip()
    news_id = str(item.get("code") or item.get("realSort") or "").strip()
    if not title or not news_id:
        return None, published
    return (
        {
            "news_id": news_id,
            "publish_date": published.date(),
            "publish_time": published.strftime("%H:%M:%S"),
            "title": title,
            "summary": str(item.get("summary") or "").strip() or None,
            "related_symbols": _news_symbols(item.get("stockList")),
            "channel": "fast_news",
        },
        published,
    )


def fetch_news_since(
    since: datetime,
    *,
    config=None,
    page_size: int = 200,
    recorder: list[dict] | None = None,
) -> tuple[pl.DataFrame, bool]:
    """Items published at or after *since*; and whether the walk reached it.

    ``False`` means the API ran out of pages (or the page cap) before *since*:
    everything older is beyond what it serves, and the caller should say so.
    """
    rows: list[dict] = []
    reached = False
    client_kwargs = {"config": config} if config is not None else {}
    with EastMoneyClient(**client_kwargs) as client:
        sort_end = ""
        seen: set[str] = set()
        for page in range(1, _NEWS_MAX_PAGES + 1):
            params = {
                "client": "web",
                "biz": "web_724",
                "fastColumn": "102",
                "sortEnd": sort_end,
                "pageSize": page_size,
                "req_trace": "1",
            }
            url = f"{_NEWS_URL}?{urlencode(params)}"
            resp = client.get(url, timeout=30.0)
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, dict):
                raise RuntimeError("EastMoney news response is not an object")
            data = payload.get("data")
            if not isinstance(data, dict):
                raise RuntimeError("EastMoney news response data is not an object")
            if not isinstance(data.get("fastNewsList"), list):
                raise RuntimeError("EastMoney news response data.fastNewsList is not a list")
            items = _object_rows(data["fastNewsList"], source="news")
            next_cursor = str(data.get("sortEnd") or "").strip()
            if recorder is not None:
                recorder.append(
                    {
                        "page": page,
                        "url": url,
                        "params": params,
                        "status": resp.status_code,
                        "content_type": resp.headers.get("content-type"),
                        "content_b64": base64.b64encode(resp.content).decode("ascii"),
                        "captured_at": datetime.now(timezone.utc).isoformat(),
                        "sort_end": next_cursor,
                    }
                )
            oldest: datetime | None = None
            for item in items:
                row, published = _item_row(item)
                if published is None:
                    continue
                oldest = published if oldest is None else min(oldest, published)
                if row is not None and published >= since:
                    rows.append(row)
            if oldest is not None and oldest < since:
                reached = True
                break
            if not items or not next_cursor or next_cursor in seen:
                break
            seen.add(next_cursor)
            sort_end = next_cursor
    if not rows:
        return pl.DataFrame(schema=NEWS_COLUMNS), reached
    frame = pl.DataFrame(rows, schema=NEWS_COLUMNS).unique(
        subset=["news_id"], keep="first", maintain_order=True
    )
    return frame, reached
