"""One full-market push2 clist sweep, shared by every caller that needs one.

``instruments`` (list_date), ``valuation_metrics``, ``fund_flow`` and the clist
bar fallback all page the same A-share board (``ALL_A_FS``, ~60 pages at
pz=100) and differ only in ``fields``. Paged separately that is ~240 requests
a day against a host that bans IPs for volume; paged once with the union of
their fields it is ~60.

A snapshot is reused only while the market it describes cannot have moved:
it was captured in a closed window (15:30 → next 09:15, Beijing time) and it
is still that same window — or it is a few minutes old. Anything else is
fetched again. Every page's exact response bytes are kept, so a caller that
must archive its wire observation (``fund_flow``) replays the original bytes
with their original capture time rather than a re-serialization.
"""

from __future__ import annotations

import base64
import gzip
import json
import logging
import threading
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from cnequity.domain.http_policy import record_cache_reuse
from cnequity.file_lock import exclusive_lock
from cnequity.storage.atomic import replace_with_retry

if TYPE_CHECKING:
    from cnequity.storage.raw_archive import RawPayloadArchive

logger = logging.getLogger(__name__)

# Union of the full-market callers' fields. f12/f13 identify the row; f26 is
# list_date; f2/f5/f6/f15/f16/f17 bars; f9/f20/f21/f23/f130 valuation;
# f62/f66/f72/f78/f84 fund flow.
SNAPSHOT_FIELDS = "f12,f13,f2,f5,f6,f9,f15,f16,f17,f20,f21,f23,f26,f62,f66,f72,f78,f84,f130"
_SNAPSHOT_FIELD_SET = frozenset(SNAPSHOT_FIELDS.split(","))
_CST = ZoneInfo("Asia/Shanghai")
_CLOSE = time(15, 30)
_OPEN = time(9, 15)
# A snapshot this young is reused even mid-session: two steps of the same run.
_FRESH_FOR = timedelta(minutes=10)
_THREAD_LOCK = threading.Lock()


def covers(fields: str) -> bool:
    return set(f.strip() for f in fields.split(",") if f.strip()) <= _SNAPSHOT_FIELD_SET


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _closed_window(moment: datetime) -> datetime | None:
    """Start of the closed-market window containing *moment*, or None if open."""
    local = moment.astimezone(_CST)
    if local.time() >= _CLOSE:
        return datetime.combine(local.date(), _CLOSE, _CST)
    if local.time() < _OPEN:
        return datetime.combine(local.date() - timedelta(days=1), _CLOSE, _CST)
    return None


def reusable(captured_at: datetime, now: datetime) -> bool:
    if now < captured_at:
        return False
    if now - captured_at <= _FRESH_FOR:
        return True
    window = _closed_window(captured_at)
    return window is not None and window == _closed_window(now)


def _path(config: Any) -> Path:
    from cnequity.meta_layout import SOURCE_CACHE

    return Path(config.meta_root) / SOURCE_CACHE / "push2_snapshots" / "all_a.json.gz"


def _load(path: Path) -> dict | None:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict) or data.get("fields") != SNAPSHOT_FIELDS:
        return None
    return data


def _save(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False)
    replace_with_retry(tmp, path)


class _ReplayedResponse:
    """Just enough of an httpx.Response for ``archive_response``."""

    def __init__(self, page: dict) -> None:
        self.content = base64.b64decode(page["content_b64"])
        self.url = page["url"]
        self.status_code = page["status"]
        self.headers = {"content-type": page.get("content_type") or "application/json"}


def _replay_into_archive(
    data: dict,
    archive: RawPayloadArchive | None,
    dataset: str | None,
    run_id: str | None,
) -> None:
    if archive is None or not dataset:
        return
    from cnequity.adapters.eastmoney.raw import archive_response

    for page in data["pages"]:
        archive_response(
            archive,
            dataset,
            _ReplayedResponse(page),
            request_params={
                "host": page["host"],
                "fields": data["fields"],
                "fs": data["fs"],
                "page": page["page"],
                "page_size": data["page_size"],
                "shared_snapshot": True,
            },
            run_id=run_id,
            url=page["url"],
            observation_id=f"{run_id or 'anonymous'}:{page['host']}:{data['fs']}:{page['page']}",
            pagination={
                "page": page["page"],
                "page_size": data["page_size"],
                "reported_total": page.get("total"),
            },
            captured_at=datetime.fromisoformat(page["captured_at"]),
        )


def shared_all_a_rows(
    client: Any,
    *,
    fs: str,
    page_size: int,
    fetch_pages: Any,
    archive: RawPayloadArchive | None = None,
    archive_dataset: str | None = None,
    archive_run_id: str | None = None,
) -> list[dict]:
    """Rows of the shared full-market sweep, fetching it only when stale.

    *fetch_pages* is the uncached paginator; it is called with the union
    fields and a ``recorder`` list that collects each page's exact response.
    """
    config = client.config
    path = _path(config)
    with _THREAD_LOCK, exclusive_lock(path.with_suffix(".lock"), timeout=1800.0):
        data = _load(path)
        now = _now()
        if (
            data is not None
            and data.get("fs") == fs
            and data.get("page_size") == page_size
            and reusable(datetime.fromisoformat(data["captured_at"]), now)
        ):
            record_cache_reuse(config, "eastmoney_push2", "shared_snapshot")
            logger.info(
                "push2 clist: reusing the shared full-market snapshot from %s (%d rows)",
                data["captured_at"],
                len(data["rows"]),
            )
        else:
            recorder: list[dict] = []
            rows = fetch_pages(
                client, fields=SNAPSHOT_FIELDS, fs=fs, page_size=page_size, recorder=recorder
            )
            data = {
                "fields": SNAPSHOT_FIELDS,
                "fs": fs,
                "page_size": page_size,
                # The sweep's start: the oldest moment any of its rows describes.
                "captured_at": now.isoformat(),
                "rows": rows,
                "pages": recorder,
            }
            _save(path, data)
        _replay_into_archive(data, archive, archive_dataset, archive_run_id)
        return [dict(row) for row in data["rows"]]
