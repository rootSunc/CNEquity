"""CNINFO regulatory / compliance events (filtered from announcements)."""

from __future__ import annotations

import logging
import re
from datetime import date

import httpx
import polars as pl

from cn_market_lake.adapters.cninfo.announcements import _symbol_from_cninfo, post_with_retry

logger = logging.getLogger(__name__)

_CNINFO_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"

_KEYWORD_TYPES: list[tuple[str, str]] = [
    ("行政处罚", "penalty"),
    ("处罚决定", "penalty"),
    ("立案", "investigation"),
    ("调查", "investigation"),
    ("监管函", "regulatory_letter"),
    ("警示函", "warning_letter"),
    ("处分", "disciplinary"),
]


def _classify_event(title: str) -> str:
    for keyword, event_type in _KEYWORD_TYPES:
        if keyword in title:
            return event_type
    return "regulatory"


def fetch_regulatory_events(
    trade_date: date,
    *,
    client: httpx.Client | None = None,
    config=None,
) -> pl.DataFrame:
    owns = client is None
    if client is None:
        client = httpx.Client(timeout=30.0, headers={"User-Agent": "Mozilla/5.0"})

    ds = trade_date.strftime("%Y-%m-%d")
    pattern = re.compile("|".join(re.escape(k) for k, _ in _KEYWORD_TYPES))
    rows: list[dict] = []

    for column in ("szse", "sse"):
        page = 1
        while True:
            if config is not None:
                config.rate_limit("cninfo")
            payload = {
                "pageNum": page,
                "pageSize": 30,
                "column": column,
                "tabName": "fulltext",
                "plate": "",
                "stock": "",
                "searchkey": "",
                "secid": "",
                "category": "",
                "trade": "",
                "seDate": f"{ds}~{ds}",
            }
            try:
                data = post_with_retry(client, _CNINFO_URL, data=payload)
            except Exception as exc:
                # Don't truncate: short pages drop blacklist rows. Fail loud
                # once retries (post_with_retry) are exhausted.
                logger.warning("CNINFO regulatory page failed (%s p%s): %s", column, page, exc)
                raise RuntimeError(
                    f"CNINFO regulatory pagination failed for {column} page {page}"
                ) from exc

            batch = data.get("announcements") or []
            if not batch:
                break
            total_pages = data.get("totalpages")
            for item in batch:
                title = str(item.get("announcementTitle") or "")
                if not pattern.search(title):
                    continue
                sym = _symbol_from_cninfo(str(item.get("secCode", "")))
                if not sym:
                    continue
                ann_id = str(item.get("announcementId") or item.get("adjunctUrl", ""))
                rows.append(
                    {
                        "event_id": f"reg-{ann_id}",
                        "symbol": sym,
                        "event_date": trade_date,
                        "event_type": _classify_event(title),
                        "title": title,
                    }
                )
            if isinstance(total_pages, int) and page >= total_pages:
                # See announcements.fetch_announcement_index: hasMore cannot
                # be trusted past the server's own reported total — measured
                # live, it stays true forever while replaying page 1's rows.
                break
            if not data.get("hasMore"):
                break
            page += 1

    if owns:
        client.close()
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["event_id"], keep="last")
