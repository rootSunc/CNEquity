"""CNINFO announcement index (batch)."""

from __future__ import annotations

import logging
import time
from datetime import date

import httpx
import polars as pl

from cn_market_lake.domain.symbols import format_symbol, is_all_a_symbol

logger = logging.getLogger(__name__)

_CNINFO_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"

# A single unretried request killing a multi-year backfill walk over a
# transient 504 is how a 30-minute cninfo hiccup turns into hours of redone
# work — see `walk_day_backfill`, which restarts a whole step on any raise
# from its per-day fetch. Retrying here, close to the actual HTTP call, keeps
# the caller's "fail loud on a page" contract for a genuinely broken source
# while surviving the blip. Measured hitting this in production: a 504 on
# `regulatory_events` page 8 of a ~16-year sweep.
# 504 Gateway Time-out is common on deep sse pages of busy disclosure days;
# three tries with a short backoff still lost a 16h announcement walk at
# page 270. Be more patient here — a few extra minutes beats redoing months.
_POST_RETRIES = 6
_POST_BACKOFF_SECONDS = 5.0


def post_with_retry(client: httpx.Client, url: str, *, data: dict) -> dict:
    last_exc: Exception | None = None
    for attempt in range(_POST_RETRIES):
        try:
            resp = client.post(url, data=data)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001 — retried uniformly, re-raised below
            last_exc = exc
            if attempt + 1 < _POST_RETRIES:
                time.sleep(_POST_BACKOFF_SECONDS * (attempt + 1))
    raise last_exc  # type: ignore[misc]


def _symbol_from_cninfo(code: str, org_id: str | None = None) -> str | None:
    code = str(code).zfill(6)
    if code.startswith(("60", "68")):
        exch = "SH"
    elif code.startswith("92"):
        exch = "BJ"
    else:
        exch = "SZ"
    if not is_all_a_symbol(code, exch):
        return None
    return format_symbol(code, exch)


def fetch_announcement_index(
    trade_date: date,
    *,
    client: httpx.Client | None = None,
    config=None,
) -> pl.DataFrame:
    owns = client is None
    if client is None:
        client = httpx.Client(timeout=30.0, headers={"User-Agent": "Mozilla/5.0"})

    ds = trade_date.strftime("%Y-%m-%d")
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
                logger.warning("CNINFO announcement page failed (%s p%s): %s", column, page, exc)
                raise RuntimeError(
                    f"CNINFO announcement pagination failed for {column} page {page}"
                ) from exc

            batch = data.get("announcements") or []
            if not batch:
                break
            total_pages = data.get("totalpages")
            for item in batch:
                sym = _symbol_from_cninfo(str(item.get("secCode", "")))
                if not sym:
                    continue
                ann_id = str(item.get("announcementId") or item.get("adjunctUrl", ""))
                rows.append(
                    {
                        "announcement_id": ann_id,
                        "symbol": sym,
                        "title": str(item.get("announcementTitle") or ""),
                        "announce_date": trade_date,
                        "category": str(item.get("announcementType") or ""),
                        "url": str(item.get("adjunctUrl") or ""),
                    }
                )
            if isinstance(total_pages, int) and page >= total_pages:
                # `hasMore` cannot be trusted past the server's own reported
                # total: measured live, requesting page 2000 of a 105-page
                # day still returns page 1's rows with hasMore still true —
                # an infinite loop with no other exit. total_pages stays
                # correct even on those overshot pages, so it is the one
                # authoritative stop condition.
                break
            if not data.get("hasMore"):
                break
            page += 1

    if owns:
        client.close()
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["announcement_id"], keep="last")
