"""CNINFO regulatory / compliance events (filtered from announcements)."""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable, Mapping
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import polars as pl

from cnequity.adapters.cninfo.announcements import (
    _announcement_id,
    _source_date,
    _symbol_from_cninfo,
    fetch_cninfo_rows,
    replay_cninfo_rows,
)
from cnequity.storage.raw_archive import RawArchiveError, RawPayloadArchive, RawPayloadRecord

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
    metrics: dict[str, Any] | None = None,
    checkpoint_path: str | Path | None = None,
    max_pages_per_slice: int = 100,
    refresh: bool = True,
    checkpoint_ttl_days: int | None = None,
    source_revision: str | None = None,
    raw_archive: RawPayloadArchive | None = None,
    run_id: str | None = None,
    request_scope: str | None = None,
) -> pl.DataFrame:
    return fetch_regulatory_events_range(
        trade_date,
        trade_date,
        client=client,
        config=config,
        metrics=metrics,
        checkpoint_path=checkpoint_path,
        max_pages_per_slice=max_pages_per_slice,
        refresh=refresh,
        checkpoint_ttl_days=checkpoint_ttl_days,
        source_revision=source_revision,
        raw_archive=raw_archive,
        run_id=run_id,
        request_scope=request_scope,
    )


def fetch_regulatory_events_range(
    start: date,
    end: date | None = None,
    *,
    client: httpx.Client | None = None,
    config=None,
    metrics: dict[str, Any] | None = None,
    checkpoint_path: str | Path | None = None,
    max_pages_per_slice: int = 100,
    refresh: bool = True,
    checkpoint_ttl_days: int | None = None,
    source_revision: str | None = None,
    raw_archive: RawPayloadArchive | None = None,
    run_id: str | None = None,
    request_scope: str | None = None,
) -> pl.DataFrame:
    """Fetch regulatory events over a date interval with resumable slicing."""
    if end is None:
        end = start
    pattern = re.compile("|".join(re.escape(k) for k, _ in _KEYWORD_TYPES))
    raw_rows = fetch_cninfo_rows(
        start,
        end,
        client=client,
        config=config,
        label="regulatory",
        metrics=metrics,
        checkpoint_path=checkpoint_path,
        max_pages_per_slice=max_pages_per_slice,
        refresh=refresh,
        checkpoint_ttl_days=checkpoint_ttl_days,
        source_revision=source_revision,
        raw_archive=raw_archive,
        run_id=run_id,
        request_scope=request_scope,
    )
    rows: list[dict] = []
    for item in raw_rows:
        source_date = _source_date(item)
        if source_date is None:
            if start != end:
                continue
            source_date = start
        title = str(item.get("announcementTitle") or "")
        if not pattern.search(title):
            continue
        sym = _symbol_from_cninfo(str(item.get("secCode", "")))
        if not sym:
            continue
        ann_id = _announcement_id(item)
        if ann_id is None:
            logger.warning("CNINFO regulatory announcement missing identity; skipping")
            continue
        rows.append(
            {
                "event_id": f"reg-{ann_id}",
                "symbol": sym,
                "event_date": source_date,
                "event_type": _classify_event(title),
                "title": title,
            }
        )
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["event_id"], keep="last")


def replay_regulatory_events_range(
    archive: RawPayloadArchive | str | Path,
    start: date | None = None,
    end: date | None = None,
    *,
    records: Iterable[RawPayloadRecord | Mapping[str, Any]] | None = None,
    max_pages_per_slice: int = 100,
) -> pl.DataFrame:
    """Rebuild regulatory events offline from verified archived CNINFO pages."""
    if start is None and end is not None:
        start = end
    elif start is not None and end is None:
        end = start
    raw_rows = replay_cninfo_rows(
        archive,
        "regulatory_events",
        start=start,
        end=end,
        records=records,
        max_pages_per_slice=max_pages_per_slice,
    )
    pattern = re.compile("|".join(re.escape(k) for k, _ in _KEYWORD_TYPES))
    rows: list[dict] = []
    for item in raw_rows:
        try:
            source_date = _source_date(item)
        except ValueError as exc:
            raise RawArchiveError("CNINFO archived regulatory row has invalid date") from exc
        if source_date is None:
            if start is None or end is None or start != end:
                raise RawArchiveError(
                    "CNINFO archived broad-range regulatory row is missing its date"
                )
            source_date = start
        if start is not None and source_date < start:
            raise RawArchiveError(
                f"CNINFO archived regulatory row date {source_date.isoformat()} is before "
                f"requested {start.isoformat()}"
            )
        if end is not None and source_date > end:
            raise RawArchiveError(
                f"CNINFO archived regulatory row date {source_date.isoformat()} is after "
                f"requested {end.isoformat()}"
            )
        title = str(item.get("announcementTitle") or "")
        if not pattern.search(title):
            continue
        sym = _symbol_from_cninfo(str(item.get("secCode", "")))
        if not sym:
            continue
        ann_id = _announcement_id(item)
        if ann_id is None:
            continue
        rows.append(
            {
                "event_id": f"reg-{ann_id}",
                "symbol": sym,
                "event_date": source_date,
                "event_type": _classify_event(title),
                "title": title,
            }
        )
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["event_id"], keep="last")


def replay_regulatory_events(
    archive: RawPayloadArchive | str | Path,
    trade_date: date,
    *,
    records: Iterable[RawPayloadRecord | Mapping[str, Any]] | None = None,
    max_pages_per_slice: int = 100,
) -> pl.DataFrame:
    """Single-day convenience wrapper for archived regulatory responses."""
    return replay_regulatory_events_range(
        archive,
        trade_date,
        trade_date,
        records=records,
        max_pages_per_slice=max_pages_per_slice,
    )
