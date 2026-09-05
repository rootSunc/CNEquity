"""CNINFO announcement index (batch)."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import polars as pl

from cnequity.domain.market_time import SHANGHAI_TZ
from cnequity.domain.rate_limit import source_request
from cnequity.domain.symbols import format_symbol, infer_exchange_from_code, is_all_a_symbol
from cnequity.storage.raw_archive import (
    RawArchiveError,
    RawPayloadArchive,
    RawPayloadRecord,
    begin_capture,
)

# Epoch-millisecond bounds used to recognize a Unix ms timestamp regardless of
# which of the three candidate keys carried it: 2000-01-01 / 2100-01-01 in
# epoch ms. Both bounds sit orders of magnitude away from an 8-digit YYYYMMDD
# value, so there is no ambiguity between the two shapes.
_EPOCH_MS_MIN = 946_684_800_000
_EPOCH_MS_MAX = 4_102_444_800_000

logger = logging.getLogger(__name__)

_CNINFO_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
_PAGE_SIZE = 30
#: The site section this endpoint is read through. Not a market selector.
_CNINFO_COLUMN = "szse"

# The server answers at most 100 pages (3000 rows) for any one query: page 101
# silently replays page 1 while ``totalpages`` keeps reporting the real count
# (measured 2026-08-28: 6537 rows, ``totalpages=217``, page 101 == page 1). A
# dense disclosure day therefore cannot be read as a single query, and the
# repeated-page guard alone would only turn that into a failed step.
#
# What makes such a day retrievable is a *filter that partitions it*. These are
# the axes this walk may split on, tried in the order below, and each split is
# accepted only when the deduplicated identities of its buckets reconcile
# exactly to the parent query's ``totalRecordNum``. Coverage is proven per day,
# per split, from the source's own count — the lists here are a mechanism for
# reaching rows, never permission to publish part of a day.
#
# ``column`` is deliberately absent: it is not a partition at all. Live probes
# return the identical all-market result for ``szse``, ``sse`` and ``bj``, so
# walking more than one of them is the same day fetched twice.

#: Market boards. Measured to sum exactly to the unfiltered total on every day
#: probed (2010, 2015, 2018, 2026), so this is the first and cheapest split.
_CNINFO_MARKET_PLATES: tuple[str, ...] = ("sz", "sh", "bj")

#: Sub-boards, for a market that is itself too dense. ``szzx`` (中小板) merged
#: into ``szmb`` in 2021 and answers 0 for later days; it stays for history.
_CNINFO_PLATE_BOARDS: dict[str, tuple[str, ...]] = {
    "sz": ("szmb", "szzx", "szcy"),
    "sh": ("shmb", "shkcp"),
}

#: CSRC 行业门类 A–S. Also measured to sum exactly to the parent total, and it
#: cuts the dominant board differently from the disclosure categories below.
_CNINFO_TRADES: tuple[str, ...] = (
    "农、林、牧、渔业",
    "采矿业",
    "制造业",
    "电力、热力、燃气及水生产和供应业",
    "建筑业",
    "批发和零售业",
    "交通运输、仓储和邮政业",
    "住宿和餐饮业",
    "信息传输、软件和信息技术服务业",
    "金融业",
    "房地产业",
    "租赁和商务服务业",
    "科学研究和技术服务业",
    "水利、环境和公共设施管理业",
    "居民服务、修理和其他服务业",
    "教育",
    "卫生和社会工作",
    "文化、体育和娱乐业",
    "综合",
)

#: Disclosure categories — the last resort, because an announcement can carry
#: more than one and a few carry none, so this cover reconciles less often
#: than the ones above.
_CNINFO_CATEGORIES: tuple[str, ...] = (
    "category_ndbg_szsh",
    "category_bndbg_szsh",
    "category_yjdbg_szsh",
    "category_sjdbg_szsh",
    "category_yjygjxz_szsh",
    "category_qyfpxzcs_szsh",
    "category_dshgg_szsh",
    "category_jshgg_szsh",
    "category_gddh_szsh",
    "category_rcjy_szsh",
    "category_gszl_szsh",
    "category_zj_szsh",
    "category_sf_szsh",
    "category_zf_szsh",
    "category_gqjl_szsh",
    "category_pg_szsh",
    "category_jj_szsh",
    "category_gszq_szsh",
    "category_kzzq_szsh",
    "category_qtrz_szsh",
    "category_gqbd_szsh",
    "category_bcgz_szsh",
    "category_cqdq_szsh",
    "category_fxts_szsh",
    "category_tbclts_szsh",
    "category_tszlq_szsh",
)
#: Request-body keys this walk may set to narrow a query, in the order they
#: appear in a bucket identity.
_PARTITION_AXES: tuple[str, ...] = ("plate", "trade", "category")
_BUCKET_SEPARATOR = "|"

# The checkpoint must be tied to the request/normalization contract.  Bumping
# this value forces a clean walk instead of mixing rows fetched under an older
# CNINFO response shape with the current parser.
CNINFO_SOURCE_REVISION = "cninfo-hisAnnouncement-v4"

# Keep a single date-range walk bounded.  CNINFO has been observed to replay
# page 1 indefinitely under load; broad ranges can therefore be safely split
# into independently validated child slices instead of hanging a backfill.
_DEFAULT_MAX_PAGES_PER_SLICE = 100
_MAX_SLICE_DEPTH = 32

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


def post_with_retry(
    client: httpx.Client,
    url: str,
    *,
    data: dict,
    metrics: dict[str, Any] | None = None,
    config=None,
    on_response: Callable[[Any, object | None, int], None] | None = None,
) -> dict:
    """POST with bounded retries while optionally observing response bytes.

    ``httpx.Response.json()`` is intentionally not the archive boundary: the
    observer receives the response object before it is reduced to a Python
    dict, so adapters can retain the exact response body.  Lightweight test
    doubles that only expose ``json()`` remain supported when no archive is
    requested; an archive-enabled call fails closed rather than persisting a
    deterministic-but-synthetic JSON fallback.
    """
    last_exc: Exception | None = None
    for attempt in range(_POST_RETRIES):
        if metrics is not None:
            metrics["requests"] = int(metrics.get("requests", 0)) + 1
            if attempt:
                # Every attempt after the first is a retry, including the
                # successful attempt after a transient failure.
                metrics["retries"] = int(metrics.get("retries", 0)) + 1
        try:
            # Acquire for each retry attempt, not once for the whole page.
            # This keeps a slow/failed request inside the same global CNINFO
            # cap as concurrent DAG waves and releases the lease before the
            # backoff sleep.
            with source_request(config, "cninfo", metrics=metrics):
                resp = client.post(url, data=data)
                try:
                    resp.raise_for_status()
                except Exception:
                    # Error responses are useful audit evidence too.  The
                    # observer marks them as unparsed and the retry loop still
                    # handles the transport/status failure below.
                    if on_response is not None:
                        on_response(resp, None, attempt)
                    raise
                try:
                    parsed = resp.json()
                except Exception:
                    # Preserve malformed/non-JSON wire bytes before surfacing
                    # the parser failure.  A later retry may provide a valid
                    # body for the same page.
                    if on_response is not None:
                        on_response(resp, None, attempt)
                    raise
                if on_response is not None:
                    on_response(resp, parsed, attempt)
                return parsed
        except RawArchiveError:
            # An archive policy/integrity failure is fail-closed.  Retrying the
            # source request cannot repair missing or tampered evidence and
            # would make it possible to continue without a required archive.
            raise
        except Exception as exc:  # noqa: BLE001 — retried uniformly, re-raised below
            last_exc = exc
            if attempt + 1 < _POST_RETRIES:
                time.sleep(_POST_BACKOFF_SECONDS * (attempt + 1))
    raise last_exc  # type: ignore[misc]


def _symbol_from_cninfo(code: str, org_id: str | None = None) -> str | None:
    code = str(code).strip().zfill(6)
    if len(code) != 6 or not code.isdigit():
        return None
    exch = infer_exchange_from_code(code)
    if not is_all_a_symbol(code, exch):
        return None
    return format_symbol(code, exch)


def _announcement_id(item: dict) -> str | None:
    """Stable CNINFO identity, or None for a row that cannot be keyed."""
    value = item.get("announcementId") or item.get("adjunctUrl")
    text = str(value or "").strip()
    return text or None


def _source_date(item: dict) -> date | None:
    """Extract CNINFO's announcement date when the response carries one."""
    raw = next(
        (
            item[key]
            for key in ("announcementTime", "announcementDate", "announceDate")
            if key in item
        ),
        None,
    )
    if raw is None or str(raw).strip() == "":
        return None
    numeric: int | None = None
    if isinstance(raw, int) and not isinstance(raw, bool):
        numeric = raw
    elif isinstance(raw, str) and raw.strip().isdigit():
        numeric = int(raw.strip())
    if numeric is not None and _EPOCH_MS_MIN <= numeric <= _EPOCH_MS_MAX:
        return datetime.fromtimestamp(numeric / 1000, tz=SHANGHAI_TZ).date()
    text = str(raw).strip().replace("/", "-")
    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise ValueError(f"invalid announcement date {raw!r}") from exc


def _validate_source_date(item: dict, trade_date: date, *, column: str) -> None:
    """Reject a CNINFO row whose own date disagrees with the requested day.

    ``seDate`` is supposed to be an exact server-side filter, but the endpoint
    has returned rows outside that filter in the past. The old adapter stamped
    every row with ``trade_date`` without inspecting ``announcementTime``, so a
    cross-day response could be written into the wrong partition and still pass
    the generic by-date validation. Fixtures and older payloads may omit the
    field; absence is tolerated, but a present malformed or different date is a
    source-contract failure.
    """
    raw = next(
        (
            item[key]
            for key in ("announcementTime", "announcementDate", "announceDate")
            if key in item
        ),
        None,
    )
    if raw is None or str(raw).strip() == "":
        return
    try:
        source_date = _source_date(item)
    except ValueError as exc:
        raise RuntimeError(f"CNINFO {column} row has invalid announcement date {raw!r}") from exc
    if source_date is None:
        return
    if source_date != trade_date:
        raise RuntimeError(
            f"CNINFO {column} row date {source_date.isoformat()} does not match "
            f"requested {trade_date.isoformat()}"
        )


def _validate_source_date_range(item: dict, start: date, end: date, *, column: str) -> None:
    """Validate a row returned for a date interval.

    Legacy fixtures omit the date field, so absence remains tolerated. A
    present field must parse and fall inside the requested interval; otherwise
    a broad query could poison the wrong partition.
    """
    try:
        source_date = _source_date(item)
    except ValueError as exc:
        raw = next(
            (
                item[key]
                for key in ("announcementTime", "announcementDate", "announceDate")
                if key in item
            ),
            None,
        )
        raise RuntimeError(f"CNINFO {column} row has invalid announcement date {raw!r}") from exc
    if source_date is None and start != end:
        raise RuntimeError(
            f"CNINFO {column} row is missing announcement date in broad range "
            f"{start.isoformat()}..{end.isoformat()}"
        )
    if source_date is not None and not (start <= source_date <= end):
        raise RuntimeError(
            f"CNINFO {column} row date {source_date.isoformat()} is outside "
            f"requested {start.isoformat()}..{end.isoformat()}"
        )


def _announcement_batch(data: object, *, column: str, page: int) -> list[dict]:
    """Validate a CNINFO page while isolating malformed rows."""
    if not isinstance(data, dict):
        raise RuntimeError(f"CNINFO response for {column} page {page} is not an object")
    raw_batch = data.get("announcements")
    if raw_batch is None:
        return []
    if not isinstance(raw_batch, list):
        raise RuntimeError(f"CNINFO announcements for {column} page {page} is not a list")
    batch: list[dict] = []
    for index, item in enumerate(raw_batch):
        if not isinstance(item, dict):
            logger.warning(
                "CNINFO announcements: skipping non-object row %s on %s page %s",
                index,
                column,
                page,
            )
            continue
        batch.append(item)
    return batch


def _pagination_total_pages(data: dict, *, column: str, page: int) -> int | None:
    """Normalize the optional CNINFO page count without trusting bad metadata."""
    raw_total = data.get("totalpages")
    if raw_total is None:
        return None
    if isinstance(raw_total, bool):
        raise RuntimeError(
            f"CNINFO totalpages for {column} page {page} is not a non-negative integer"
        )
    if isinstance(raw_total, int):
        total_pages = raw_total
    elif isinstance(raw_total, str) and raw_total.strip().isdigit():
        total_pages = int(raw_total.strip())
    else:
        raise RuntimeError(
            f"CNINFO totalpages for {column} page {page} is not a non-negative integer"
        )
    if total_pages < 0:
        raise RuntimeError(
            f"CNINFO totalpages for {column} page {page} is not a non-negative integer"
        )
    return total_pages


def _pagination_has_more(data: dict, *, column: str, page: int) -> bool:
    """Normalize CNINFO's optional continuation flag."""
    raw_has_more = data.get("hasMore")
    if raw_has_more is None:
        return False
    if isinstance(raw_has_more, bool):
        return raw_has_more
    if isinstance(raw_has_more, str):
        normalized = raw_has_more.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no", ""}:
            return False
    raise RuntimeError(f"CNINFO hasMore for {column} page {page} is not a boolean value")


def _pagination_page_signature(batch: list[dict]) -> str:
    """Stable identity for a page, used to detect a non-advancing source."""
    encoded = json.dumps(batch, sort_keys=True, default=str, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


class _NeedDateSplit(RuntimeError):
    """Internal signal: this date interval cannot be walked safely as one."""


def _pagination_total_records(data: dict, *, column: str, page: int) -> int | None:
    """Read optional record totals used to reconcile unique primary keys."""
    values: dict[str, int] = {}
    for key in (
        "total",
        "totalCount",
        "totalcount",
        "totalRecords",
        "totalrecords",
        "totalRecordNum",
        "totalrecordnum",
        "recordCount",
        "recordcount",
        "count",
    ):
        if key not in data or data[key] is None or str(data[key]).strip() == "":
            continue
        raw = data[key]
        if isinstance(raw, bool):
            raise RuntimeError(
                f"CNINFO {key} for {column} page {page} is not a non-negative integer"
            )
        if isinstance(raw, int):
            total = raw
        elif isinstance(raw, str) and raw.strip().isdigit():
            total = int(raw.strip())
        else:
            raise RuntimeError(
                f"CNINFO {key} for {column} page {page} is not a non-negative integer"
            )
        if total < 0:
            raise RuntimeError(
                f"CNINFO {key} for {column} page {page} is not a non-negative integer"
            )
        values[key] = total
    if not values:
        return None
    if len(set(values.values())) != 1:
        rendered = ", ".join(f"{key}={value}" for key, value in values.items())
        raise RuntimeError(f"CNINFO record totals disagree for {column} page {page}: {rendered}")
    # CNINFO's current wire contract names this field totalRecordNum.  The
    # aliases remain readable for old captures, but can no longer override a
    # conflicting authoritative value.
    for key in ("totalRecordNum", "totalrecordnum"):
        if key in values:
            return values[key]
    return next(iter(values.values()))


def _pagination_total_record_num(data: dict, *, column: str, page: int) -> int | None:
    """Return CNINFO's authoritative current-contract record count, if present."""
    for key in ("totalRecordNum", "totalrecordnum"):
        if key not in data or data[key] is None or str(data[key]).strip() == "":
            continue
        # The general parser performs type/range and cross-alias validation.
        return _pagination_total_records(data, column=column, page=page)
    return None


def _checkpoint_key(column: str, start: date, end: date) -> str:
    return f"{column}:{start.isoformat()}:{end.isoformat()}"


class PartitionCoverageError(RuntimeError):
    """A candidate split did not provably cover the query it replaced.

    Distinct from a transport or contract failure: it means *this* cover is
    unusable, so the caller may try the next axis instead of giving up. What it
    never means is that the shortfall can be published.
    """


def _bucket_column(column: str, filters: Mapping[str, str] | None) -> str:
    """Encode the applied filters into the checkpoint / archive identity."""
    suffix = "".join(
        f"{_BUCKET_SEPARATOR}{axis}={filters[axis]}"
        for axis in _PARTITION_AXES
        if filters and filters.get(axis)
    )
    return f"{column}{suffix}"


def _bucket_parts(value: str) -> tuple[str, dict[str, str]]:
    column, _, encoded = value.partition(_BUCKET_SEPARATOR)
    filters: dict[str, str] = {}
    for part in encoded.split(_BUCKET_SEPARATOR) if encoded else ():
        axis, separator, filter_value = part.partition("=")
        if separator and filter_value:
            filters[axis] = filter_value
    return column, filters


def _partition_axis(parent: Mapping[str, str], child: Mapping[str, str]) -> str | None:
    """The single axis a child bucket refines, or ``None`` if it is not one."""
    changed = [axis for axis in _PARTITION_AXES if child.get(axis) != parent.get(axis)]
    return changed[0] if len(changed) == 1 else None


def _partition_candidates(filters: Mapping[str, str]) -> tuple[tuple[str, tuple[str, ...]], ...]:
    """Covers still available for a cell, cheapest and most reliable first.

    Each entry replaces one query with a set of narrower ones. ``plate``
    appears twice because a market that is itself too dense splits again into
    its own boards; the rest are orthogonal filters that have not been applied
    to this cell yet.
    """
    candidates: list[tuple[str, tuple[str, ...]]] = []
    plate = filters.get("plate")
    if plate is None:
        candidates.append(("plate", _CNINFO_MARKET_PLATES))
    elif plate in _CNINFO_PLATE_BOARDS:
        candidates.append(("plate", _CNINFO_PLATE_BOARDS[plate]))
    if "trade" not in filters:
        candidates.append(("trade", _CNINFO_TRADES))
    if "category" not in filters:
        candidates.append(("category", _CNINFO_CATEGORIES))
    # An axis with no values cannot cover anything; offering it would only
    # turn a real split into an empty one.
    return tuple((axis, values) for axis, values in candidates if values)


def _reconcile_partition_rows(
    rows: list[dict],
    *,
    expected_total: int,
    label: str,
    column: str,
    day: date,
    axis: str,
    required_identities: set[str] | None = None,
) -> list[dict]:
    """Prove that one split's buckets cover the query they replaced."""

    by_identity: dict[str, dict] = {}
    for row in rows:
        identity = _announcement_id(row)
        if identity is None:
            raise PartitionCoverageError(
                f"CNINFO {label} {axis} coverage for {column} {day} contains an unkeyed row"
            )
        by_identity[str(identity)] = row
    if len(by_identity) != expected_total:
        raise PartitionCoverageError(
            f"CNINFO {label} {axis} coverage for {column} {day} returned "
            f"{len(by_identity)} unique rows; unfiltered total is {expected_total}"
        )
    missing_control = set(required_identities or ()) - set(by_identity)
    if missing_control:
        raise PartitionCoverageError(
            f"CNINFO {label} {axis} coverage for {column} {day} replaced "
            f"{len(missing_control)} identity/identities observed in the unfiltered control"
        )
    return list(by_identity.values())


def _empty_checkpoint(identity: str, *, source_revision: str | None = None) -> dict[str, Any]:
    return {
        "version": 2,
        "identity": identity,
        "source_revision": source_revision,
        "slices": {},
    }


def _load_checkpoint(
    path: Path | None,
    identity: str,
    *,
    source_revision: str | None = None,
) -> dict[str, Any]:
    if path is None or not path.exists():
        return _empty_checkpoint(identity, source_revision=source_revision)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"CNINFO checkpoint {path} is unreadable: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("identity") != identity:
        # A checkpoint for another date window must never be silently applied
        # to this request. Treat it as an empty ledger instead.
        return _empty_checkpoint(identity, source_revision=source_revision)
    if source_revision is not None and payload.get("source_revision") != source_revision:
        # A provider revision/contract change invalidates every completed page;
        # keeping a running page from the old response could mix two source
        # versions in one frame.
        return _empty_checkpoint(identity, source_revision=source_revision)
    slices = payload.get("slices")
    if not isinstance(slices, dict):
        raise RuntimeError(f"CNINFO checkpoint {path} has invalid slices")
    return payload


def _checkpoint_completed_is_fresh(record: dict[str, Any], *, ttl_days: int | None) -> bool:
    """Whether a completed CNINFO slice may be reused by an explicit resume."""
    if ttl_days is None:
        return True
    completed_at = record.get("completed_at")
    if not isinstance(completed_at, str):
        return False
    try:
        timestamp = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
    except ValueError:
        return False
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - timestamp <= timedelta(days=ttl_days)


def _checkpoint_running_is_fresh(
    record: dict[str, Any],
    *,
    checkpoint: Mapping[str, Any] | None,
    ttl_days: int | None,
) -> bool:
    """Whether an interrupted running slice is still safe to resume.

    A process can die after persisting several normalized pages.  Reusing that
    partial ledger forever would keep a provider correction hidden even when
    callers explicitly opt into resume.  Prefer the slice's last update time,
    then the checkpoint-level update time written by ``_write_checkpoint``;
    old ledgers with no clock are deliberately treated as expired whenever a
    TTL is configured.
    """
    if ttl_days is None:
        return True
    raw_timestamp = (
        record.get("failed_at")
        or record.get("updated_at")
        or record.get("started_at")
        or (checkpoint or {}).get("updated_at")
    )
    if not isinstance(raw_timestamp, str):
        return False
    try:
        timestamp = datetime.fromisoformat(raw_timestamp.replace("Z", "+00:00"))
    except ValueError:
        return False
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - timestamp <= timedelta(days=ttl_days)


def invalidate_cninfo_checkpoint(path: str | Path) -> None:
    """Explicitly discard a CNINFO pagination checkpoint before a refresh."""
    checkpoint_path = Path(path)
    try:
        checkpoint_path.unlink()
    except FileNotFoundError:
        return


def _write_checkpoint(path: Path | None, payload: dict[str, Any]) -> None:
    if path is None:
        return
    updated_at = datetime.now(timezone.utc).isoformat()
    payload["updated_at"] = updated_at
    slices = payload.get("slices")
    if isinstance(slices, dict):
        for record in slices.values():
            if isinstance(record, dict) and record.get("status") == "running":
                record["updated_at"] = updated_at
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, default=str)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass


def _midpoint(start: date, end: date) -> date:
    return start + timedelta(days=(end - start).days // 2)


def _archive_dataset_for_label(label: str) -> str:
    return {
        "announcement": "announcement_index",
        "regulatory": "regulatory_events",
    }.get(label, label)


def _cninfo_archive(
    config: Any,
    label: str,
    *,
    run_id: str | None = None,
    request_scope: str | None = None,
) -> RawPayloadArchive | None:
    """Build the archive for a critical CNINFO dataset, if policy allows."""
    if config is None:
        return None
    meta_root = getattr(config, "meta_root", None)
    if meta_root is None:
        return None
    dataset = _archive_dataset_for_label(label)
    should_archive = getattr(config, "should_archive_raw", None)
    if callable(should_archive) and not should_archive(dataset):
        return None
    # A Config instance always supplies this field.  The True fallback keeps
    # small offline config doubles safe for these two built-in critical feeds;
    # callers can still explicitly disable the archive.
    scope = str(request_scope or f"range:{label}")
    nonce = begin_capture(config, dataset, run_id, source="cninfo", request_scope=scope)
    archive = RawPayloadArchive(
        meta_root,
        enabled=bool(getattr(config, "raw_archive_enabled", True)),
        datasets=[dataset],
        compression=getattr(config, "raw_archive_compression", "gzip"),
        max_payload_bytes=getattr(config, "raw_archive_max_payload_bytes", None),
        capture_owner=config,
        capture_run_id=run_id,
        capture_source="cninfo",
        capture_scope=scope,
        capture_nonce=nonce,
    )
    # Persisted archive lineage is deliberately distinct from the ephemeral
    # evidence nonce. It lets offline replay keep both exchange columns and
    # every recursive slice inside one top-level fetch invocation.
    archive._invocation_id = uuid.uuid4().hex
    return archive if archive.enabled else None


def _response_wire_payload(response: Any, parsed: object | None) -> tuple[bytes, str, bool]:
    """Return response bytes without manufacturing a normalized page payload."""
    missing = object()
    try:
        content = getattr(response, "content", missing)
    except Exception:  # pragma: no cover - unusual streaming response double
        content = missing
    if content is not missing and content is not None:
        if isinstance(content, bytes):
            return content, "bytes", True
        if isinstance(content, bytearray):
            return bytes(content), "bytes", True
        if isinstance(content, memoryview):
            return content.tobytes(), "bytes", True
        if isinstance(content, str):
            # ``text`` may have been decoded/re-encoded by the client; it is
            # useful for diagnostics but cannot prove the original wire
            # bytes.  Keep it explicitly non-replayable below.
            return content.encode("utf-8"), "text", False
    try:
        text = getattr(response, "text", missing)
    except Exception:  # pragma: no cover - unusual streaming response double
        text = missing
    if text is not missing and text is not None:
        if isinstance(text, bytes):
            return text, "bytes", False
        if isinstance(text, str):
            return text.encode("utf-8"), "text", False
    # A parsed object is not the source observation.  Serializing it here would
    # create a payload that never crossed the wire and would make a replay look
    # more authoritative than the capture actually was.  Callers that require
    # the archive therefore fail closed below; keep this return only so the
    # transport metadata can explain an unsupported response double if a
    # non-critical caller ever asks for it.
    try:
        encoded = json.dumps(parsed, ensure_ascii=False, sort_keys=True, default=str).encode(
            "utf-8"
        )
    except (TypeError, ValueError) as exc:
        raise RawArchiveError("CNINFO response has no readable wire bytes") from exc
    return encoded, "json", False


def _response_http_metadata(
    response: Any, *, payload_bytes: int, wire_exact: bool
) -> dict[str, Any]:
    """Keep useful transport facts while excluding arbitrary header secrets."""
    metadata: dict[str, Any] = {
        "wire_exact": wire_exact,
        "payload_bytes": payload_bytes,
    }
    for attribute in ("http_version", "reason_phrase"):
        value = getattr(response, attribute, None)
        if value is not None:
            metadata[attribute] = str(value)
    headers = getattr(response, "headers", None)
    if headers is not None:
        # An allow-list avoids retaining vendor-specific auth/proxy headers;
        # request metadata is sanitized separately by RawPayloadArchive.
        allowed = {
            "cache-control",
            "content-length",
            "content-type",
            "date",
            "etag",
            "last-modified",
            "server",
            "vary",
        }
        try:
            metadata["headers"] = {
                str(key).lower(): str(value)
                for key, value in dict(headers).items()
                if str(key).lower() in allowed
            }
        except (TypeError, ValueError):
            pass
    return metadata


def _best_effort_pagination_metadata(
    parsed: object | None,
    *,
    column: str,
    page: int,
    start: date,
    end: date,
    capture_id: str | None = None,
) -> dict[str, Any]:
    """Capture page metadata without turning archival into response validation."""
    metadata: dict[str, Any] = {
        "column": column,
        "page": page,
        "page_size": _PAGE_SIZE,
        "slice_start": start.isoformat(),
        "slice_end": end.isoformat(),
    }
    if capture_id:
        metadata["capture_id"] = capture_id
    if not isinstance(parsed, dict):
        metadata["json_parsed"] = False
        return metadata
    raw_batch = parsed.get("announcements")
    metadata["batch_rows"] = len(raw_batch) if isinstance(raw_batch, list) else 0
    metadata["json_parsed"] = True
    for key, aliases in {
        "reported_total_pages": ("totalpages",),
        "reported_total_records": (
            "total",
            "totalCount",
            "totalcount",
            "totalRecords",
            "totalrecords",
            "totalRecordNum",
            "totalrecordnum",
            "recordCount",
            "recordcount",
            "count",
        ),
        "has_more": ("hasMore",),
    }.items():
        for alias in aliases:
            if alias in parsed:
                metadata[key] = parsed[alias]
                break
    return metadata


def _archive_cninfo_response(
    archive: RawPayloadArchive | None,
    *,
    record: dict[str, Any],
    label: str,
    payload: dict[str, Any],
    column: str,
    page: int,
    start: date,
    end: date,
    response: Any,
    parsed: object | None,
    attempt: int,
    run_id: str | None,
    request_scope: str | None,
) -> None:
    """Archive one POST response and register it in the resumable ledger."""
    if archive is None or not archive.should_archive(_archive_dataset_for_label(label)):
        return
    wire, payload_format, wire_exact = _response_wire_payload(response, parsed)
    if not wire_exact:
        raise RawArchiveError(
            "CNINFO response has no exact wire bytes; refusing to create a replayable archive"
        )
    # One capture identifies all retry attempts for this logical request.  It
    # is intentionally distinct from the page fingerprint: identical bytes in
    # two runs still represent two source observations and must retain both
    # provenance records.
    capture_id = str(record.get("capture_id") or "").strip()
    if not capture_id:
        capture_id = uuid.uuid4().hex
        record["capture_id"] = capture_id
    request_params = {
        **payload,
        "label": label,
        "slice_start": start.isoformat(),
        "slice_end": end.isoformat(),
    }
    pagination = _best_effort_pagination_metadata(
        parsed,
        column=column,
        page=page,
        start=start,
        end=end,
        capture_id=capture_id,
    )
    invocation_id = str(getattr(archive, "_invocation_id", "") or "")
    if invocation_id:
        pagination["invocation_id"] = invocation_id
    pagination["attempt"] = attempt
    http_metadata = _response_http_metadata(
        response,
        payload_bytes=len(wire),
        wire_exact=wire_exact,
    )
    http_metadata["json_parsed"] = parsed is not None
    status = getattr(response, "status_code", None)
    try:
        response_status = int(status) if status is not None else None
    except (TypeError, ValueError):
        response_status = None
    response_url = str(getattr(response, "url", "") or "") or _CNINFO_URL
    run_token = str(run_id or "run-unknown").strip() or "run-unknown"
    observation_id = (
        f"{run_token}:{capture_id}:{label}:{column}:{start.isoformat()}:{end.isoformat()}"
        f":page={page}:attempt={attempt}"
    )
    archived = archive.archive(
        _archive_dataset_for_label(label),
        wire,
        source="cninfo",
        request_params=request_params,
        captured_at=datetime.now(timezone.utc),
        run_id=run_id,
        url=response_url,
        response_status=response_status,
        payload_format=payload_format,
        http_metadata=http_metadata,
        pagination=pagination,
        observation_id=observation_id,
        request_scope=request_scope,
    )
    if archived is None:
        return
    refs = record.setdefault("raw_archives", [])
    reference = archived.to_dict()
    if not any(
        isinstance(existing, dict)
        and existing.get("metadata_path") == reference.get("metadata_path")
        for existing in refs
    ):
        refs.append(reference)


def _checkpoint_archive_reusable(
    record: Mapping[str, Any], archive: RawPayloadArchive | None
) -> bool:
    """Require every page observation before reusing a checkpointed slice."""
    if archive is None:
        return True
    references = record.get("raw_archives")
    if not isinstance(references, list) or not references:
        # Checkpoints written before per-page archival cannot silently bypass
        # the configured evidence policy.  The caller will restart page 1.
        return False
    archived_pages: set[int] = set()
    for reference in references:
        if not isinstance(reference, Mapping):
            return False
        metadata_relative = str(reference.get("metadata_path", ""))
        metadata_path = archive.meta_root / metadata_relative
        if (
            not metadata_relative
            or Path(metadata_relative).is_absolute()
            or ".." in Path(metadata_relative).parts
            or not metadata_path.is_file()
        ):
            return False
        try:
            sidecar = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise RawArchiveError(f"invalid CNINFO raw metadata: {metadata_path}") from exc
        if not isinstance(sidecar, Mapping):
            raise RawArchiveError(f"invalid CNINFO raw metadata: {metadata_path}")
        for key in ("payload_sha256", "payload_path", "metadata_path", "dataset", "source"):
            if key in reference and sidecar.get(key) != reference.get(key):
                raise RawArchiveError(f"CNINFO raw metadata {key} mismatch: {metadata_path}")
        try:
            archive.read(reference)
        except FileNotFoundError:
            return False
        # RawArchiveError is deliberately not swallowed: a present but
        # tampered payload must stop the run rather than trigger an untracked
        # network refresh.
        raw_pagination = reference.get("pagination", {})
        if isinstance(raw_pagination, Mapping) and raw_pagination.get("json_parsed") is not False:
            try:
                archived_pages.add(int(raw_pagination["page"]))
            except (KeyError, TypeError, ValueError):
                pass
    # A checkpoint may be edited to retain one valid archive reference while
    # still replaying normalized rows for unarchived pages.  Every page that
    # was accepted before a resumable boundary therefore needs a verified
    # page observation of its own.
    try:
        accepted_pages = int(record.get("pages", 0) or 0)
    except (TypeError, ValueError):
        return False
    if accepted_pages > 0 and not set(range(1, accepted_pages + 1)).issubset(archived_pages):
        return False
    return True


def _fetch_page_slice(
    client: httpx.Client,
    *,
    start: date,
    end: date,
    column: str,
    label: str,
    config=None,
    metrics: dict[str, Any] | None = None,
    checkpoint: dict[str, Any] | None = None,
    checkpoint_path: Path | None = None,
    max_pages_per_slice: int = _DEFAULT_MAX_PAGES_PER_SLICE,
    depth: int = 0,
    refresh: bool = True,
    checkpoint_ttl_days: int | None = None,
    source_revision: str | None = None,
    raw_archive: RawPayloadArchive | None = None,
    run_id: str | None = None,
    request_scope: str | None = None,
    filters: Mapping[str, str] | None = None,
) -> list[dict]:
    """Fetch one bounded slice, recursively splitting unsafe walks."""
    if start > end:
        return []
    if max_pages_per_slice < 1:
        raise ValueError("max_pages_per_slice must be >= 1")
    if depth > _MAX_SLICE_DEPTH:
        raise RuntimeError(
            f"CNINFO {label} pagination exceeded split depth for "
            f"{start.isoformat()}..{end.isoformat()}"
        )

    state = checkpoint or _empty_checkpoint(label, source_revision=source_revision)
    slices = state.setdefault("slices", {})
    filters = dict(filters or {})
    bucket_column = _bucket_column(column, filters)
    key = _checkpoint_key(bucket_column, start, end)
    record = slices.get(key)
    if (
        isinstance(record, dict)
        and record.get("status") == "running"
        and not _checkpoint_running_is_fresh(
            record,
            checkpoint=state,
            ttl_days=checkpoint_ttl_days,
        )
    ):
        # An abandoned running slice is not a durable source observation. Drop
        # its partial rows and restart page 1 so a TTL cannot merely annotate
        # the checkpoint while still serving stale normalized data.
        record.update(
            {
                "status": "running",
                "next_page": 1,
                "pages": 0,
                "rows": [],
                "unique_keys": [],
                "page_signatures": [],
                "expected_total": None,
                "authoritative_total": None,
                "reported_pages": None,
                "completed_at": None,
                "raw_row_count": 0,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "source_revision": source_revision,
                "raw_archives": [],
                "capture_id": uuid.uuid4().hex,
            }
        )
        _write_checkpoint(checkpoint_path, state)
    if (
        isinstance(record, dict)
        and record.get("status") == "complete"
        and not refresh
        and _checkpoint_completed_is_fresh(record, ttl_days=checkpoint_ttl_days)
    ):
        # The archives are verified for their own sake, not for the answer:
        # a tampered sidecar has to stop the run rather than be quietly
        # replaced by a fresh fetch. Whether the rows may be replayed is a
        # separate question, and under an archive it is always no — a new
        # invocation has a new evidence lineage, so these sidecars cannot back
        # what this capture publishes (the split and running branches below
        # rebuild for the same reason).
        _checkpoint_archive_reusable(record, raw_archive)
        rows = record.get("rows", [])
        if raw_archive is None and isinstance(rows, list):
            if metrics is not None:
                metrics["checkpoint_slices"] = int(metrics.get("checkpoint_slices", 0)) + 1
            return [row for row in rows if isinstance(row, dict)]

    if isinstance(record, dict) and record.get("status") == "complete":
        # A completed slice is a cache entry only when the caller explicitly
        # opted into resume. Refresh/TTL expiry must restart at page 1 rather
        # than accidentally continuing from the old ``next_page`` and
        # returning only an empty tail.
        record.update(
            {
                "status": "running",
                "next_page": 1,
                "pages": 0,
                "rows": [],
                "unique_keys": [],
                "page_signatures": [],
                "expected_total": None,
                "authoritative_total": None,
                "reported_pages": None,
                "completed_at": None,
                "raw_row_count": 0,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "source_revision": source_revision,
                "raw_archives": [],
                "capture_id": uuid.uuid4().hex,
            }
        )

    if (
        isinstance(record, dict)
        and record.get("status") == "failed"
        and not _checkpoint_running_is_fresh(
            record,
            checkpoint=state,
            ttl_days=checkpoint_ttl_days,
        )
    ):
        # Failed slices obey the same TTL as interrupted running slices. Once
        # expired, their accepted pages may not be mixed with a corrected
        # provider response.
        record.update(
            {
                "status": "running",
                "next_page": 1,
                "pages": 0,
                "rows": [],
                "unique_keys": [],
                "page_signatures": [],
                "expected_total": None,
                "authoritative_total": None,
                "reported_pages": None,
                "completed_at": None,
                "raw_row_count": 0,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "source_revision": source_revision,
                "raw_archives": [],
                "capture_id": uuid.uuid4().hex,
            }
        )
        record.pop("failure_reason", None)
        record.pop("failure_page", None)
        record.pop("failed_at", None)
        _write_checkpoint(checkpoint_path, state)
    elif isinstance(record, dict) and record.get("status") == "failed":
        # A single-day slice cannot be made safer by another date split, but
        # it can still be retried after the provider recovers.  Keep the
        # accepted rows/page boundary and make the transition explicit so a
        # checkpoint reader never mistakes this terminal observation for a
        # completed slice.  The next invocation resumes at ``next_page``.
        record["status"] = "running"
        record.pop("failure_reason", None)
        record.pop("failure_page", None)
        record.pop("failed_at", None)
        _write_checkpoint(checkpoint_path, state)

    # An interrupted partition. Unlike a date split, this parent cannot be
    # resumed from its children: proving a cover needs the identities the
    # *unfiltered* query returned, and those live only in the capped walk that
    # triggered the split. So the walk is repeated — deliberately, not by
    # falling through an unrecognised status — and the buckets it reaches are
    # still individually checkpointed underneath.
    if isinstance(record, dict) and str(record.get("status", "")).endswith("_split"):
        record.update(
            {
                "status": "running",
                "next_page": 1,
                "pages": 0,
                "rows": [],
                "unique_keys": [],
                "page_signatures": [],
                "expected_total": None,
                "authoritative_total": None,
                "reported_pages": None,
                "completed_at": None,
                "raw_row_count": 0,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "raw_archives": [],
                "capture_id": uuid.uuid4().hex,
            }
        )
        _write_checkpoint(checkpoint_path, state)

    # A previously split parent can be resumed without requesting its broad
    # page again. Child records carry their own progress/checkpoints.
    if isinstance(record, dict) and record.get("status") == "split":
        if raw_archive is not None:
            # A new archive invocation has a new evidence lineage. Rebuild the
            # parent and all descendants in this capture rather than combining
            # an old split-triggering page with newly resumed children.
            record.update(
                {
                    "status": "running",
                    "next_page": 1,
                    "pages": 0,
                    "rows": [],
                    "unique_keys": [],
                    "page_signatures": [],
                    "expected_total": None,
                    "authoritative_total": None,
                    "reported_pages": None,
                    "completed_at": None,
                    "raw_row_count": 0,
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "raw_archives": [],
                    "capture_id": uuid.uuid4().hex,
                }
            )
            _write_checkpoint(checkpoint_path, state)
        else:
            mid = _midpoint(start, end)
            if start >= end:
                raise RuntimeError(
                    f"CNINFO {label} pagination repeated/no-progress page for {column}"
                )
            return _fetch_page_slice(
                client,
                start=start,
                end=mid,
                column=column,
                label=label,
                config=config,
                metrics=metrics,
                checkpoint=state,
                checkpoint_path=checkpoint_path,
                max_pages_per_slice=max_pages_per_slice,
                depth=depth + 1,
                refresh=refresh,
                checkpoint_ttl_days=checkpoint_ttl_days,
                source_revision=source_revision,
                raw_archive=raw_archive,
                run_id=run_id,
                request_scope=request_scope,
                filters=filters,
            ) + _fetch_page_slice(
                client,
                start=mid + timedelta(days=1),
                end=end,
                column=column,
                label=label,
                config=config,
                metrics=metrics,
                checkpoint=state,
                checkpoint_path=checkpoint_path,
                max_pages_per_slice=max_pages_per_slice,
                depth=depth + 1,
                refresh=refresh,
                checkpoint_ttl_days=checkpoint_ttl_days,
                source_revision=source_revision,
                raw_archive=raw_archive,
                run_id=run_id,
                request_scope=request_scope,
                filters=filters,
            )

    if not isinstance(record, dict):
        record = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "status": "running",
            "next_page": 1,
            "pages": 0,
            "rows": [],
            "unique_keys": [],
            "page_signatures": [],
            "expected_total": None,
            "authoritative_total": None,
            "reported_pages": None,
            "completed_at": None,
            "raw_row_count": 0,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "source_revision": source_revision,
            "filters": dict(filters),
            "raw_archives": [],
            "capture_id": uuid.uuid4().hex,
        }
        slices[key] = record
    else:
        record["source_revision"] = source_revision
        # A running checkpoint from before per-page archival must not resume
        # from its normalized rows when raw evidence is required.  Restarting
        # page 1 re-establishes a complete audit trail.
        if raw_archive is not None:
            # The new invocation has a new evidence nonce. Historic sidecars
            # remain valid audit records, but cannot prove rows published by
            # this capture, so rebuild a complete active receipt from page 1.
            record.update(
                {
                    "status": "running",
                    "next_page": 1,
                    "pages": 0,
                    "rows": [],
                    "unique_keys": [],
                    "page_signatures": [],
                    "expected_total": None,
                    "authoritative_total": None,
                    "reported_pages": None,
                    "completed_at": None,
                    "raw_row_count": 0,
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "raw_archives": [],
                    "capture_id": uuid.uuid4().hex,
                }
            )
            _write_checkpoint(checkpoint_path, state)
    raw_rows = [row for row in record.get("rows", []) if isinstance(row, dict)]
    page = max(1, int(record.get("next_page", 1)))
    seen_keys = {str(value) for value in record.get("unique_keys", [])}
    seen_signatures = set(str(value) for value in record.get("page_signatures", []))
    expected_total = record.get("expected_total")
    expected_total = int(expected_total) if expected_total is not None else None
    authoritative_total = record.get("authoritative_total")
    authoritative_total = int(authoritative_total) if authoritative_total is not None else None
    reported_pages = record.get("reported_pages")
    reported_pages = int(reported_pages) if reported_pages is not None else None

    def split_or_raise(reason: str) -> list[dict]:
        if start >= end:
            splittable = authoritative_total is not None and (
                "exceeded" in reason
                or "repeated page" in reason
                or "no unique-key progress" in reason
                or "does not match reported total" in reason
            )
            if splittable:
                # The unfiltered walk reached the server's cap, so it is a
                # *control*, not a result: every identity it did return must
                # still be present after the split, or the split answered a
                # different question.
                control_identities: set[str] = set()
                for control_row in [*raw_rows, *batch]:
                    _validate_source_date(control_row, start, column=column)
                    control_identity = _announcement_id(control_row)
                    if control_identity is None:
                        raise RuntimeError(
                            f"CNINFO {label} unfiltered control for {bucket_column} {start} "
                            "contains an unkeyed row"
                        )
                    control_identities.add(str(control_identity))
                coverage_failure: PartitionCoverageError | None = None
                for axis, values in _partition_candidates(filters):
                    record["status"] = f"{axis}_split"
                    record["rows"] = []
                    record["next_page"] = 1
                    record["pages"] = 0
                    record["unique_keys"] = []
                    record["page_signatures"] = []
                    record["control_total"] = authoritative_total
                    record["reported_pages"] = reported_pages
                    record["raw_row_count"] = 0
                    record["completed_at"] = None
                    _write_checkpoint(checkpoint_path, state)
                    bucket_rows: list[dict] = []
                    try:
                        for value in values:
                            bucket_rows.extend(
                                _fetch_page_slice(
                                    client,
                                    start=start,
                                    end=end,
                                    column=column,
                                    label=label,
                                    config=config,
                                    metrics=metrics,
                                    checkpoint=state,
                                    checkpoint_path=checkpoint_path,
                                    max_pages_per_slice=max_pages_per_slice,
                                    depth=depth + 1,
                                    refresh=refresh,
                                    checkpoint_ttl_days=checkpoint_ttl_days,
                                    source_revision=source_revision,
                                    raw_archive=raw_archive,
                                    run_id=run_id,
                                    request_scope=request_scope,
                                    filters={**filters, axis: value},
                                )
                            )
                        reconciled = _reconcile_partition_rows(
                            bucket_rows,
                            expected_total=authoritative_total,
                            label=label,
                            column=bucket_column,
                            day=start,
                            axis=axis,
                            required_identities=control_identities,
                        )
                    except PartitionCoverageError as exc:
                        # This cover cannot prove it read the whole day — but a
                        # different filter may still partition it, so try the
                        # next axis before failing the slice.
                        coverage_failure = exc
                        if metrics is not None:
                            metrics.setdefault("partition_rejected", []).append(
                                {"axis": axis, "reason": str(exc)}
                            )
                        continue
                    except Exception as exc:
                        record["status"] = "failed"
                        record["failure_reason"] = f"{axis} partition failed: {exc}"
                        record["failure_page"] = 1
                        record["failed_at"] = datetime.now(timezone.utc).isoformat()
                        record["next_page"] = 1
                        record["rows"] = []
                        _write_checkpoint(checkpoint_path, state)
                        raise
                    record["status"] = "complete"
                    record["rows"] = reconciled
                    record["next_page"] = 1
                    record["pages"] = 0
                    record["unique_keys"] = sorted(str(_announcement_id(row)) for row in reconciled)
                    record["page_signatures"] = []
                    record["expected_total"] = authoritative_total
                    record["authoritative_total"] = authoritative_total
                    record["raw_row_count"] = len(reconciled)
                    record["completed_at"] = datetime.now(timezone.utc).isoformat()
                    record["partition_strategy"] = axis
                    _write_checkpoint(checkpoint_path, state)
                    if metrics is not None:
                        metrics["partition_splits"] = int(metrics.get("partition_splits", 0)) + 1
                        metrics["partition_buckets"] = int(
                            metrics.get("partition_buckets", 0)
                        ) + len(values)
                        metrics.setdefault("partition_axes", []).append(axis)
                    return reconciled
                if coverage_failure is not None:
                    record["status"] = "failed"
                    record["failure_reason"] = str(coverage_failure)
                    record["failure_page"] = 1
                    record["failed_at"] = datetime.now(timezone.utc).isoformat()
                    record["next_page"] = 1
                    record["rows"] = []
                    _write_checkpoint(checkpoint_path, state)
                    raise coverage_failure

            # A repeated page or a max-page boundary on one calendar day has
            # no narrower date interval to fall back to. Persist the exact
            # resumable boundary and a terminal reason before raising: this is
            # an operator-visible failed slice, never a silently truncated
            # success, and a later run can retry the same page.
            # Only failures at the current, unaccepted page can safely resume
            # there. Whole-slice contract failures (row-count mismatch or a
            # page cap) invalidate already accepted rows and must restart at
            # page 1 after the provider/configuration is corrected.
            resume_current_page = (
                "repeated page" in reason
                or "no unique-key progress" in reason
                or "empty page before the reported end" in reason
            )
            record["status"] = "failed"
            record["failure_reason"] = reason
            record["failure_page"] = page if resume_current_page else 1
            record["failed_at"] = datetime.now(timezone.utc).isoformat()
            record["rows"] = raw_rows if resume_current_page else []
            record["next_page"] = (
                max(1, int(record.get("next_page", page))) if resume_current_page else 1
            )
            record["pages"] = (
                max(0, int(record.get("pages", page - 1))) if resume_current_page else 0
            )
            record["unique_keys"] = sorted(seen_keys) if resume_current_page else []
            record["page_signatures"] = sorted(seen_signatures) if resume_current_page else []
            record["expected_total"] = expected_total if resume_current_page else None
            record["authoritative_total"] = authoritative_total if resume_current_page else None
            record["reported_pages"] = reported_pages if resume_current_page else None
            record["raw_row_count"] = len(raw_rows) if resume_current_page else 0
            _write_checkpoint(checkpoint_path, state)
            if metrics is not None:
                metrics["terminal_failures"] = int(metrics.get("terminal_failures", 0)) + 1
            raise RuntimeError(
                f"CNINFO {label} pagination failed for {column} page {page}: {reason}"
            )
        record["status"] = "split"
        record["rows"] = []
        record["next_page"] = 1
        record["unique_keys"] = []
        record["page_signatures"] = []
        record["expected_total"] = None
        record["authoritative_total"] = None
        record["reported_pages"] = None
        record["completed_at"] = None
        record["raw_row_count"] = 0
        _write_checkpoint(checkpoint_path, state)
        if metrics is not None:
            metrics["split_reasons"] = int(metrics.get("split_reasons", 0)) + 1
        mid = _midpoint(start, end)
        return _fetch_page_slice(
            client,
            start=start,
            end=mid,
            column=column,
            label=label,
            config=config,
            metrics=metrics,
            checkpoint=state,
            checkpoint_path=checkpoint_path,
            max_pages_per_slice=max_pages_per_slice,
            depth=depth + 1,
            refresh=refresh,
            checkpoint_ttl_days=checkpoint_ttl_days,
            source_revision=source_revision,
            raw_archive=raw_archive,
            run_id=run_id,
            request_scope=request_scope,
            filters=filters,
        ) + _fetch_page_slice(
            client,
            start=mid + timedelta(days=1),
            end=end,
            column=column,
            label=label,
            config=config,
            metrics=metrics,
            checkpoint=state,
            checkpoint_path=checkpoint_path,
            max_pages_per_slice=max_pages_per_slice,
            depth=depth + 1,
            refresh=refresh,
            checkpoint_ttl_days=checkpoint_ttl_days,
            source_revision=source_revision,
            raw_archive=raw_archive,
            run_id=run_id,
            request_scope=request_scope,
            filters=filters,
        )

    while True:
        payload = {
            "pageNum": page,
            "pageSize": _PAGE_SIZE,
            "column": column,
            "tabName": "fulltext",
            "plate": filters.get("plate", ""),
            "stock": "",
            "searchkey": "",
            "secid": "",
            "category": filters.get("category", ""),
            "trade": filters.get("trade", ""),
            "seDate": f"{start.strftime('%Y-%m-%d')}~{end.strftime('%Y-%m-%d')}",
        }
        try:
            kwargs: dict[str, Any] = {"data": payload}
            if metrics is not None:
                kwargs["metrics"] = metrics
            if config is not None:
                kwargs["config"] = config
            if raw_archive is not None:
                kwargs["on_response"] = (
                    lambda response, parsed, attempt, _archive=raw_archive, _record=record, _label=label, _payload=payload, _column=bucket_column, _page=page, _start=start, _end=end, _run_id=run_id, _request_scope=request_scope: (
                        _archive_cninfo_response(
                            _archive,
                            record=_record,
                            label=_label,
                            payload=_payload,
                            column=_column,
                            page=_page,
                            start=_start,
                            end=_end,
                            response=response,
                            parsed=parsed,
                            attempt=attempt,
                            run_id=_run_id,
                            request_scope=_request_scope,
                        )
                    )
                )
            data = post_with_retry(client, _CNINFO_URL, **kwargs)
            batch = _announcement_batch(data, column=column, page=page)
            total_pages = _pagination_total_pages(data, column=column, page=page)
            has_more = _pagination_has_more(data, column=column, page=page)
            has_more_present = data.get("hasMore") is not None
            page_total = _pagination_total_records(data, column=column, page=page)
            page_authoritative_total = _pagination_total_record_num(data, column=column, page=page)
            if expected_total is None:
                expected_total = page_total
            elif page_total is not None and page_total != expected_total:
                raise RuntimeError(
                    f"CNINFO {label} total record count changed for {column} "
                    f"slice {start.isoformat()}..{end.isoformat()}"
                )
            if authoritative_total is None:
                authoritative_total = page_authoritative_total
            elif (
                page_authoritative_total is not None
                and page_authoritative_total != authoritative_total
            ):
                raise RuntimeError(
                    f"CNINFO {label} totalRecordNum changed for {column} "
                    f"slice {start.isoformat()}..{end.isoformat()}"
                )
            if batch and total_pages == 0 and (page_authoritative_total or 0) > _PAGE_SIZE:
                # ``totalpages`` is floor(totalRecordNum / _PAGE_SIZE), so 0 is
                # the ordinary answer for a bucket holding fewer than one full
                # page — every small ``category`` bucket reports it. It is only
                # a contradiction when the source itself claims more records
                # than a single page could carry.
                raise RuntimeError(
                    f"CNINFO {label} for {column} page {page} declared totalpages=0 "
                    f"but reports {page_authoritative_total} records"
                )
            if isinstance(total_pages, int):
                if reported_pages is None:
                    reported_pages = total_pages
                elif reported_pages != total_pages:
                    raise RuntimeError(
                        f"CNINFO {label} totalpages changed for {column} "
                        f"slice {start.isoformat()}..{end.isoformat()}"
                    )
                if total_pages > max_pages_per_slice:
                    return split_or_raise(f"slice exceeded {max_pages_per_slice} reported pages")
            if batch:
                page_signature = _pagination_page_signature(batch)
                if page_signature in seen_signatures:
                    return split_or_raise("pagination repeated page")
                seen_signatures.add(page_signature)
                page_keys = {
                    str(_announcement_id(item))
                    for item in batch
                    if _announcement_id(item) is not None
                }
                # A page may legitimately repeat a primary key when CNINFO
                # emits a corrected/revised announcement (the later payload
                # wins at the final dedupe). Exact page fingerprints above are
                # the non-progress signal; overlapping keys alone are not.
                seen_keys.update(page_keys)
                for item in batch:
                    if start == end:
                        _validate_source_date(item, start, column=column)
                    else:
                        _validate_source_date_range(item, start, end, column=column)
                raw_rows.extend(batch)
        except RawArchiveError:
            # Archive failures are integrity/policy failures, not pagination
            # evidence.  Preserve the concrete error and never substitute an
            # unarchived network retry.
            raise
        except PartitionCoverageError:
            # Raised by ``split_or_raise`` after it has already recorded this
            # slice's terminal state. Re-wrapping it here would overwrite that
            # with a page-level failure that resumes mid-walk.
            raise
        except RuntimeError as exc:
            # Validation/pagination contract errors are safe split candidates
            # for a broad range. Transport failures remain fail-loud and keep
            # the current page in the checkpoint for a later resume.
            message = str(exc)
            if (
                "repeated page" in message
                or "no unique-key progress" in message
                or (start < end and ("announcement date" in message or "row date" in message))
            ):
                return split_or_raise(message)
            record["rows"] = raw_rows
            record["next_page"] = page
            record["pages"] = page - 1
            record["unique_keys"] = sorted(seen_keys)
            record["page_signatures"] = sorted(seen_signatures)
            record["expected_total"] = expected_total
            record["authoritative_total"] = authoritative_total
            record["reported_pages"] = reported_pages
            record["raw_row_count"] = len(raw_rows)
            _write_checkpoint(checkpoint_path, state)
            raise RuntimeError(
                f"CNINFO {label} pagination failed for {column} page {page}: {exc}"
            ) from exc
        except Exception as exc:  # noqa: BLE001 — preserve the page checkpoint
            record["rows"] = raw_rows
            record["next_page"] = page
            record["pages"] = page - 1
            record["unique_keys"] = sorted(seen_keys)
            record["page_signatures"] = sorted(seen_signatures)
            record["expected_total"] = expected_total
            record["authoritative_total"] = authoritative_total
            record["reported_pages"] = reported_pages
            record["raw_row_count"] = len(raw_rows)
            _write_checkpoint(checkpoint_path, state)
            raise RuntimeError(
                f"CNINFO {label} pagination failed for {column} page {page}: {exc}"
            ) from exc

        if not batch:
            if (isinstance(total_pages, int) and total_pages > 0 and page <= total_pages) or (
                total_pages is None and has_more
            ):
                return split_or_raise("empty page before the reported end")
            break

        record["rows"] = raw_rows
        record["next_page"] = page + 1
        record["pages"] = page
        record["unique_keys"] = sorted(seen_keys)
        record["page_signatures"] = sorted(seen_signatures)
        record["expected_total"] = expected_total
        record["authoritative_total"] = authoritative_total
        record["reported_pages"] = reported_pages
        record["raw_row_count"] = len(raw_rows)
        _write_checkpoint(checkpoint_path, state)
        if metrics is not None:
            metrics["pages"] = int(metrics.get("pages", 0)) + 1
        # The record total is what the slice is reconciled against, so it also
        # decides when the walk is done.
        if expected_total is not None and len(raw_rows) >= expected_total:
            break
        # ``totalpages`` is floor(totalRecordNum / _PAGE_SIZE): the final
        # partial page is not counted at all. Measured 2026-01-01 — 1375 rows,
        # ``totalpages=45``, and page 46 holding the last 25 with
        # ``hasMore=false``. Stopping at the reported count would therefore
        # drop the tail of almost every day, so it can only end a walk that has
        # no record total to go by.
        if expected_total is None and isinstance(total_pages, int) and page >= total_pages:
            break
        if page >= max_pages_per_slice:
            return split_or_raise(f"slice exceeded {max_pages_per_slice} pages")
        if isinstance(total_pages, int):
            page += 1
            continue
        if not has_more:
            if not has_more_present and len(batch) >= _PAGE_SIZE:
                page += 1
                continue
            break
        page += 1

    # ``total`` describes the source rows, before the final announcement-id
    # dedupe.  Require an exact match: accepting an overrun would let a
    # malformed response (for example total=1 with two distinct rows) pass as
    # complete.  Legitimate duplicate identities remain valid because both
    # raw rows still count toward the reported total.
    if expected_total is not None and len(raw_rows) != expected_total:
        return split_or_raise(
            f"raw row count {len(raw_rows)} does not match reported total {expected_total} "
            f"(unique keys={len(seen_keys)})"
        )
    record["status"] = "complete"
    record["rows"] = raw_rows
    record["next_page"] = page + 1
    record["pages"] = page
    record["unique_keys"] = sorted(seen_keys)
    record["page_signatures"] = sorted(seen_signatures)
    record["expected_total"] = expected_total
    record["authoritative_total"] = authoritative_total
    record["reported_pages"] = reported_pages
    record["raw_row_count"] = len(raw_rows)
    record["completed_at"] = datetime.now(timezone.utc).isoformat()
    record.pop("partition_strategy", None)
    _write_checkpoint(checkpoint_path, state)
    if metrics is not None:
        metrics["slices_completed"] = int(metrics.get("slices_completed", 0)) + 1
        metrics.setdefault("slices", []).append(
            {
                "column": column,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "pages": page,
                "rows": len(raw_rows),
                "raw_rows": len(raw_rows),
                "unique_keys": len(seen_keys),
                "reported_pages": reported_pages,
                "reported_rows": expected_total,
            }
        )
    return raw_rows


def fetch_cninfo_rows(
    start: date,
    end: date | None = None,
    *,
    client: httpx.Client | None = None,
    config=None,
    label: str = "announcement",
    metrics: dict[str, Any] | None = None,
    checkpoint_path: str | Path | None = None,
    max_pages_per_slice: int = _DEFAULT_MAX_PAGES_PER_SLICE,
    refresh: bool = True,
    checkpoint_ttl_days: int | None = None,
    source_revision: str | None = None,
    raw_archive: RawPayloadArchive | None = None,
    run_id: str | None = None,
    request_scope: str | None = None,
) -> list[dict]:
    """Fetch raw rows over a date range with safe recursive slicing.

    A checkpoint is opt-in so ordinary daily reads never reuse stale rows.
    ``refresh=True`` (the default) also re-reads completed slices, allowing a
    same-date correction to replace an earlier payload. Set ``refresh=False``
    only for an explicit resume; ``checkpoint_ttl_days`` bounds that reuse,
    and ``source_revision`` invalidates the ledger when the provider contract
    revision changes.
    """
    if end is None:
        end = start
    if start > end:
        return []
    owns = client is None
    if client is None:
        client = httpx.Client(timeout=30.0, headers={"User-Agent": "Mozilla/5.0"})
    path = Path(checkpoint_path) if checkpoint_path is not None else None
    identity = f"cninfo:{label}:{start.isoformat()}:{end.isoformat()}"
    if checkpoint_ttl_days is not None and checkpoint_ttl_days < 0:
        raise ValueError("checkpoint_ttl_days must be >= 0")
    checkpoint = _load_checkpoint(path, identity, source_revision=source_revision)
    checkpoint["source_revision"] = source_revision
    checkpoint["checkpoint_ttl_days"] = checkpoint_ttl_days
    request_scope = str(request_scope or f"range:{label}:{start.isoformat()}:{end.isoformat()}")
    if raw_archive is None:
        raw_archive = _cninfo_archive(
            config,
            label,
            run_id=run_id,
            request_scope=request_scope,
        )
    elif config is not None:
        should_archive = getattr(config, "should_archive_raw", None)
        if callable(should_archive) and not should_archive(_archive_dataset_for_label(label)):
            raw_archive = None
        elif raw_archive.enabled:
            # A caller may inject an archive root for replay/fixture tests.
            # Bind that explicit archive to this invocation as well; otherwise
            # publish verification would have to fall back to discovering an
            # unrelated sidecar from the same run.
            dataset = _archive_dataset_for_label(label)
            nonce = begin_capture(
                config,
                dataset,
                run_id,
                source="cninfo",
                request_scope=request_scope,
            )
            raw_archive._capture_owner = config
            raw_archive._capture_run_id = run_id
            raw_archive._capture_source = "cninfo"
            raw_archive._capture_scope = request_scope
            raw_archive._capture_nonce = nonce
            raw_archive._invocation_id = uuid.uuid4().hex
    if raw_archive is not None and not raw_archive.enabled:
        raw_archive = None
    if metrics is not None:
        metrics.setdefault("dataset", label)
        metrics.setdefault("range_start", start.isoformat())
        metrics.setdefault("range_end", end.isoformat())
        metrics.setdefault("started_at", datetime.now().astimezone().isoformat())
    started = time.perf_counter()
    try:
        # One walk, not one per exchange: ``column`` selects a site section,
        # not a market. Live probes return the identical all-market rows for
        # ``szse``, ``sse`` and ``bj`` (2026-08-28: 6537 each, same first page),
        # so the old two-column loop fetched every dense day twice and threw
        # the duplicate away at the announcement-id dedupe.
        rows: list[dict] = _fetch_page_slice(
            client,
            start=start,
            end=end,
            column=_CNINFO_COLUMN,
            label=label,
            config=config,
            metrics=metrics,
            checkpoint=checkpoint,
            checkpoint_path=path,
            max_pages_per_slice=max_pages_per_slice,
            refresh=refresh,
            checkpoint_ttl_days=checkpoint_ttl_days,
            source_revision=source_revision,
            raw_archive=raw_archive,
            run_id=run_id,
            request_scope=request_scope,
        )
        if metrics is not None:
            unique_ids = {
                identity
                for identity in (_announcement_id(row) for row in rows)
                if identity is not None
            }
            metrics["raw_rows"] = len(rows)
            metrics["unique_keys"] = len(unique_ids)
            metrics["duplicate_rows"] = len(rows) - len(unique_ids)
        if metrics is not None:
            metrics["elapsed_seconds"] = float(metrics.get("elapsed_seconds", 0.0)) + (
                time.perf_counter() - started
            )
            metrics["status"] = "success"
        return rows
    except Exception:
        if metrics is not None:
            metrics["elapsed_seconds"] = float(metrics.get("elapsed_seconds", 0.0)) + (
                time.perf_counter() - started
            )
            metrics["status"] = "failed"
        raise
    finally:
        if owns:
            client.close()


def _replay_archive_records(
    archive: RawPayloadArchive | str | Path,
    *,
    dataset: str,
    records: Iterable[RawPayloadRecord | Mapping[str, Any]] | None = None,
) -> tuple[RawPayloadArchive, list[RawPayloadRecord | Mapping[str, Any]]]:
    if isinstance(archive, RawPayloadArchive):
        store = archive
    else:
        store = RawPayloadArchive(archive)
    selected = list(records) if records is not None else store.records(dataset)
    return store, selected


def _replay_request_context(
    record: RawPayloadRecord | Mapping[str, Any],
) -> tuple[str, date, date, int, dict[str, Any], dict[str, Any]] | None:
    if isinstance(record, RawPayloadRecord):
        request = record.request_params
        pagination = record.pagination
    else:
        raw_request = record.get("request_params", {})
        raw_pagination = record.get("pagination", {})
        request = dict(raw_request) if isinstance(raw_request, Mapping) else {}
        pagination = dict(raw_pagination) if isinstance(raw_pagination, Mapping) else {}
    request_column = str(request.get("column") or "").strip()
    archived_column = str(pagination.get("column") or "").strip()
    request_filters = {
        axis: value for axis in _PARTITION_AXES if (value := str(request.get(axis) or "").strip())
    }
    request_bucket = _bucket_column(request_column, request_filters) if request_column else ""
    if request_bucket and archived_column and request_bucket != archived_column:
        raise RawArchiveError(
            "CNINFO archived request filters disagree with pagination metadata: "
            f"request={request_bucket!r}, pagination={archived_column!r}"
        )
    column = request_bucket or archived_column
    request_date_range = str(request.get("seDate") or "").strip()
    pagination_start = str(pagination.get("slice_start") or "").strip()
    pagination_end = str(pagination.get("slice_end") or "").strip()
    pagination_date_range = (
        f"{pagination_start}~{pagination_end}" if pagination_start and pagination_end else ""
    )
    if request_date_range and pagination_date_range and request_date_range != pagination_date_range:
        raise RawArchiveError(
            "CNINFO archived request date range disagrees with pagination metadata: "
            f"request={request_date_range!r}, pagination={pagination_date_range!r}"
        )
    date_range = request_date_range or pagination_date_range
    parts = date_range.split("~", 1)
    if len(parts) != 2:
        return None
    try:
        start = date.fromisoformat(parts[0].strip()[:10])
        end = date.fromisoformat(parts[1].strip()[:10])
    except ValueError:
        return None
    request_page = request.get("pageNum")
    pagination_page = pagination.get("page")
    try:
        page = int(request_page if request_page is not None else pagination_page or 1)
        if (
            request_page is not None
            and pagination_page is not None
            and int(request_page) != int(pagination_page)
        ):
            raise RawArchiveError(
                "CNINFO archived request page disagrees with pagination metadata: "
                f"request={request_page!r}, pagination={pagination_page!r}"
            )
    except (TypeError, ValueError):
        return None
    if not column or page < 1 or start > end:
        return None
    return column, start, end, page, request, pagination


def _replay_response(
    store: RawPayloadArchive,
    record: RawPayloadRecord | Mapping[str, Any],
) -> dict[str, Any] | None:
    metadata = (
        record.http_metadata
        if isinstance(record, RawPayloadRecord)
        else record.get("http_metadata", {})
    )
    if not isinstance(metadata, Mapping) or metadata.get("wire_exact") is not True:
        raise RawArchiveError(
            "CNINFO archive is not marked as an exact wire capture; refusing replay"
        )
    raw = store.read(record)
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise RawArchiveError("CNINFO archived response is not valid JSON") from exc
    return parsed if isinstance(parsed, dict) else None


def _replay_group_contains(
    parent: tuple[str, date, date],
    child: tuple[str, date, date],
) -> bool:
    parent_column, parent_filters = _bucket_parts(parent[0])
    child_column, child_filters = _bucket_parts(child[0])
    if parent_column != child_column:
        return False
    axis = _partition_axis(parent_filters, child_filters)
    if axis is not None and child_filters.get(axis) is not None:
        cover = next(
            (
                values
                for candidate_axis, values in _partition_candidates(parent_filters)
                if candidate_axis == axis
            ),
            (),
        )
        if child_filters[axis] in cover:
            return parent[1:] == child[1:]
    return (
        parent[0] == child[0]
        and parent[1] <= child[1]
        and child[2] <= parent[2]
        and (parent[1] < child[1] or child[2] < parent[2])
    )


def _replay_group_rows(
    key: tuple[str, date, date],
    groups: dict[tuple[str, date, date], list[dict[str, Any]]],
    *,
    max_pages_per_slice: int,
    active: set[tuple[str, date, date]],
    lineage: tuple[str, str] | None = None,
    captured_after: str | None = None,
) -> list[dict]:
    """Replay one archived slice using the live walk's split/stop rules."""
    if key in active:
        raise RawArchiveError(f"CNINFO archived slice recursion cycle: {key!r}")
    active.add(key)
    try:
        pages = groups.get(key, [])
        if lineage is not None:
            run_id, request_scope = lineage
            pages = [
                item
                for item in pages
                if str(item.get("run_id", "")) == run_id
                and str(item.get("request_scope", "")) == request_scope
                and (captured_after is None or str(item.get("captured_at", "")) >= captured_after)
            ]
        if not pages:
            raise RawArchiveError(f"CNINFO archived slice has no pages: {key!r}")
        captures: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for item in pages:
            captures.setdefault(
                (str(item.get("run_id", "")), str(item.get("capture_id", ""))), []
            ).append(item)
        capture_candidates = [
            items for items in captures.values() if any(int(item["page"]) == 1 for item in items)
        ]
        if not capture_candidates:
            raise RawArchiveError(f"CNINFO archived slice is missing page 1: {key!r}")
        # Do not synthesize a source observation by combining pages from
        # different runs/captures. Prefer the newest capture that starts
        # at page 1. Its pagination contract is validated below; if that
        # newest observation is interrupted, replay fails closed instead
        # of silently falling back to older source truth.
        pages = max(
            capture_candidates,
            key=lambda items: max(str(item.get("captured_at", "")) for item in items),
        )
        selected_lineage = (
            str(pages[0].get("run_id", "")),
            str(pages[0].get("request_scope", "")),
        )
        selected_capture_end = max(str(item.get("captured_at", "")) for item in pages)
        by_page: dict[int, dict[str, Any]] = {}
        for item in pages:
            page = int(item["page"])
            previous = by_page.get(page)
            # A retry may produce more than one response for one form.  The
            # last successfully parsed attempt is what post_with_retry would
            # have returned; retain all page observations in the archive, but
            # replay only that selected attempt.
            if previous is None or (
                int(item.get("attempt", 0)),
                str(item.get("captured_at", "")),
            ) >= (int(previous.get("attempt", 0)), str(previous.get("captured_at", ""))):
                by_page[page] = item
        ordered = [by_page[page] for page in sorted(by_page)]
        for item in ordered:
            status = item.get("response_status")
            if status is not None:
                try:
                    status_int = int(status)
                except (TypeError, ValueError) as exc:
                    raise RawArchiveError(
                        f"CNINFO archived response status is invalid: {key!r}"
                    ) from exc
                if not 200 <= status_int < 300:
                    raise RawArchiveError(
                        f"CNINFO latest archived capture has HTTP status {status_int}: {key!r}"
                    )
            if item["pagination"].get("json_parsed") is False:
                raise RawArchiveError(
                    f"CNINFO latest archived capture has an unparsed JSON page: {key!r}"
                )
            response = _replay_response(store=item["store"], record=item["record"])
            if response is None:
                raise RawArchiveError(
                    f"CNINFO latest archived capture JSON is not an object: {key!r}"
                )
            item["response"] = response
        children = sorted(
            (
                child
                for child in groups
                if _replay_group_contains(key, child)
                and any(
                    str(item.get("run_id", "")) == selected_lineage[0]
                    and str(item.get("request_scope", "")) == selected_lineage[1]
                    and str(item.get("captured_at", "")) >= selected_capture_end
                    for item in groups[child]
                )
                and not any(
                    child != middle
                    and _replay_group_contains(key, middle)
                    and _replay_group_contains(middle, child)
                    for middle in groups
                )
            ),
            key=lambda value: (value[1], value[2]),
        )
        # Historic child groups do not imply that the currently selected root
        # capture split. Only this capture's own pagination evidence can make
        # replay descend into its same-lineage children.
        split = False
        signatures: set[str] = set()
        preflight_total: int | None = None
        preflight_authoritative_total: int | None = None
        preflight_pages: int | None = None
        preflight_rows = 0
        root_terminal = False
        for item in ordered:
            batch = _announcement_batch(item["response"], column=key[0], page=item["page"])
            item["batch"] = batch
            try:
                reported_total = _pagination_total_records(
                    item["response"], column=key[0], page=int(item["page"])
                )
                authoritative_total = _pagination_total_record_num(
                    item["response"], column=key[0], page=int(item["page"])
                )
                reported_pages = _pagination_total_pages(
                    item["response"], column=key[0], page=int(item["page"])
                )
                has_more = _pagination_has_more(
                    item["response"], column=key[0], page=int(item["page"])
                )
            except RuntimeError as exc:
                raise RawArchiveError(str(exc)) from exc
            if reported_total is not None:
                if preflight_total is None:
                    preflight_total = reported_total
                elif preflight_total != reported_total:
                    raise RawArchiveError(f"CNINFO archived total changed: {key!r}")
            if authoritative_total is not None:
                if preflight_authoritative_total is None:
                    preflight_authoritative_total = authoritative_total
                elif preflight_authoritative_total != authoritative_total:
                    raise RawArchiveError(f"CNINFO archived totalRecordNum changed: {key!r}")
            if reported_pages is not None:
                if preflight_pages is None:
                    preflight_pages = reported_pages
                elif preflight_pages != reported_pages:
                    raise RawArchiveError(f"CNINFO archived page total changed: {key!r}")
            if key[1] < key[2]:
                for row in batch:
                    try:
                        _validate_source_date_range(row, key[1], key[2], column=key[0])
                    except RuntimeError:
                        # The live walker treats a broad-range date contract
                        # failure as a split trigger. Replay may descend only
                        # when same-lineage children fully cover the parent;
                        # that coverage is enforced below.
                        split = True
            signature = _pagination_page_signature(batch)
            if batch and signature in signatures:
                split = True
            signatures.add(signature)
            page = int(item["page"])
            preflight_rows += len(batch)
            if preflight_pages is not None and preflight_pages > max_pages_per_slice:
                split = True
            if not batch and (
                (preflight_pages is not None and preflight_pages > 0 and page <= preflight_pages)
                or (preflight_pages is None and has_more)
            ):
                split = True
            # CNINFO reports ``totalpages`` as the number of *full* pages,
            # not the last page number.  When a record total is available it
            # is therefore the only safe completion signal: page N+1 may
            # contain the final partial batch even when totalpages == N.
            if preflight_total is not None and preflight_rows >= preflight_total:
                root_terminal = True
            elif (
                preflight_total is None and preflight_pages is not None and page >= preflight_pages
            ):
                root_terminal = True
            elif preflight_total is None and preflight_pages is None and not has_more:
                root_terminal = True
            if preflight_pages is None and page >= max_pages_per_slice and not root_terminal:
                split = True
        if (
            root_terminal
            and preflight_total is not None
            and preflight_rows != preflight_total
            and (key[1] < key[2] or preflight_authoritative_total is not None)
        ):
            split = True
        if split:
            if not children:
                raise RawArchiveError(f"CNINFO archived slice cannot replay split: {key!r}")
            parent_filters = _bucket_parts(key[0])[1]
            partition_children = [
                child for child in children if _bucket_parts(child[0])[1] != parent_filters
            ]
            if partition_children:
                axes = {
                    _partition_axis(parent_filters, _bucket_parts(child[0])[1])
                    for child in partition_children
                }
                axis = axes.pop() if len(axes) == 1 else None
                cover = next(
                    (
                        values
                        for candidate_axis, values in _partition_candidates(parent_filters)
                        if candidate_axis == axis
                    ),
                    (),
                )
                values = {_bucket_parts(child[0])[1].get(axis) for child in partition_children}
                if len(partition_children) != len(children) or values != set(cover):
                    raise RawArchiveError(
                        "CNINFO archived partition children are incomplete after the "
                        f"control row count does not match its total: {key!r}"
                    )
                if preflight_authoritative_total is None:
                    raise RawArchiveError(
                        "CNINFO archived partition split lacks authoritative "
                        f"totalRecordNum: {key!r}"
                    )
                control_identities: set[str] = set()
                for item in ordered:
                    for control_row in item["batch"]:
                        try:
                            _validate_source_date(
                                control_row,
                                key[1],
                                column=_bucket_parts(key[0])[0],
                            )
                        except RuntimeError as exc:
                            raise RawArchiveError(str(exc)) from exc
                        control_identity = _announcement_id(control_row)
                        if control_identity is None:
                            raise RawArchiveError(
                                f"CNINFO archived unfiltered control has an unkeyed row: {key!r}"
                            )
                        control_identities.add(str(control_identity))
                bucket_rows: list[dict] = []
                for child in partition_children:
                    bucket_rows.extend(
                        _replay_group_rows(
                            child,
                            groups,
                            max_pages_per_slice=max_pages_per_slice,
                            active=active,
                            lineage=selected_lineage,
                            captured_after=selected_capture_end,
                        )
                    )
                try:
                    return _reconcile_partition_rows(
                        bucket_rows,
                        expected_total=preflight_authoritative_total,
                        label="archived announcement",
                        column=key[0],
                        day=key[1],
                        axis=str(axis),
                        required_identities=control_identities,
                    )
                except RuntimeError as exc:
                    raise RawArchiveError(str(exc)) from exc
            coverage_cursor = key[1]
            for child in children:
                if child[1] != coverage_cursor:
                    raise RawArchiveError(
                        f"CNINFO archived split children do not fully cover parent: {key!r}"
                    )
                coverage_cursor = child[2] + timedelta(days=1)
            if coverage_cursor != key[2] + timedelta(days=1):
                raise RawArchiveError(
                    f"CNINFO archived split children do not fully cover parent: {key!r}"
                )
            rows: list[dict] = []
            for child in children:
                rows.extend(
                    _replay_group_rows(
                        child,
                        groups,
                        max_pages_per_slice=max_pages_per_slice,
                        active=active,
                        lineage=selected_lineage,
                        captured_after=selected_capture_end,
                    )
                )
            return rows

        rows = []
        expected_total: int | None = None
        expected_pages: int | None = None
        pagination_complete = False
        for index, item in enumerate(ordered):
            page = int(item["page"])
            if page != index + 1:
                raise RawArchiveError(f"CNINFO archived slice is missing page {index + 1}: {key!r}")
            batch = item["batch"]
            rows.extend(batch)
            try:
                reported_int = _pagination_total_records(item["response"], column=key[0], page=page)
                authoritative_int = _pagination_total_record_num(
                    item["response"], column=key[0], page=page
                )
                reported_pages_int = _pagination_total_pages(
                    item["response"], column=key[0], page=page
                )
                has_more = _pagination_has_more(item["response"], column=key[0], page=page)
            except RuntimeError as exc:
                raise RawArchiveError(str(exc)) from exc
            if reported_int is not None:
                if expected_total is None:
                    expected_total = reported_int
                elif expected_total != reported_int:
                    raise RawArchiveError(f"CNINFO archived total changed: {key!r}")
            if expected_total is not None and len(rows) >= expected_total:
                pagination_complete = True
                break
            if reported_pages_int is not None:
                if reported_pages_int == 0 and batch and (authoritative_int or 0) > _PAGE_SIZE:
                    raise RawArchiveError(f"CNINFO archived page total is zero with rows: {key!r}")
                if expected_pages is None:
                    expected_pages = reported_pages_int
                elif expected_pages != reported_pages_int:
                    raise RawArchiveError(f"CNINFO archived page total changed: {key!r}")
                if expected_total is None and page >= expected_pages:
                    pagination_complete = True
                    break
                # With a record total, keep walking toward that total. Without
                # one, the explicit page count is the best available contract;
                # an early hasMore=false cannot hide a missing reported page.
                continue
            has_more_present = item["response"].get("hasMore") is not None
            if not has_more:
                if not has_more_present and len(batch) >= _PAGE_SIZE:
                    if index + 1 >= len(ordered):
                        raise RawArchiveError(
                            f"CNINFO archived slice is missing a full-page tail: {key!r}"
                        )
                    continue
                pagination_complete = True
                break
        if ordered and not pagination_complete:
            raise RawArchiveError(
                f"CNINFO archived slice is missing page {int(ordered[-1]['page']) + 1}: {key!r}"
            )
        # Replay must enforce the same raw-row contract as the live walk.  The
        # final normalized frame may dedupe repeated announcement IDs, but
        # the archived wire rows must equal the provider's declared total.
        if expected_total is not None and len(rows) != expected_total:
            raise RawArchiveError(
                f"CNINFO archived slice raw row count {len(rows)} does not match "
                f"its total {expected_total}: {key!r}"
            )
        return rows
    finally:
        active.remove(key)


def replay_cninfo_rows(
    archive: RawPayloadArchive | str | Path,
    dataset: str = "announcement_index",
    *,
    start: date | None = None,
    end: date | None = None,
    records: Iterable[RawPayloadRecord | Mapping[str, Any]] | None = None,
    max_pages_per_slice: int = _DEFAULT_MAX_PAGES_PER_SLICE,
) -> list[dict]:
    """Replay CNINFO pages/slices offline from verified wire payloads.

    The response bytes are verified by :class:`RawPayloadArchive` before JSON
    parsing.  Parent requests that caused recursive splitting are discarded,
    duplicate identities are intentionally retained here, and callers can
    then apply the same final keep-last revision semantics as the live
    adapters.
    """
    if max_pages_per_slice < 1:
        raise ValueError("max_pages_per_slice must be >= 1")
    store, selected = _replay_archive_records(archive, dataset=dataset, records=records)
    groups: dict[tuple[str, date, date], list[dict[str, Any]]] = {}
    for record in selected:
        context = _replay_request_context(record)
        if context is None:
            continue
        column, slice_start, slice_end, page, request, pagination = context
        if start is not None and slice_end < start:
            continue
        if end is not None and slice_start > end:
            continue
        attempt = pagination.get("attempt", 0)
        try:
            attempt = int(attempt)
        except (TypeError, ValueError):
            attempt = 0
        group_key = (column, slice_start, slice_end)
        groups.setdefault(group_key, []).append(
            {
                "page": page,
                "attempt": attempt,
                # Archive enumeration is content-address/hash ordered, not
                # observation ordered. Use the wire timestamp as the retry
                # tie-breaker so equal attempt numbers from later captures
                # supersede older provider revisions deterministically.
                "captured_at": (
                    record.captured_at
                    if isinstance(record, RawPayloadRecord)
                    else str(record.get("captured_at", ""))
                ),
                "run_id": (
                    str(record.run_id or "")
                    if isinstance(record, RawPayloadRecord)
                    else str(record.get("run_id", ""))
                ),
                "request_scope": (
                    str(record.request_scope or "")
                    if isinstance(record, RawPayloadRecord)
                    else str(record.get("request_scope", ""))
                ),
                "capture_id": str(pagination.get("capture_id", "")),
                "invocation_id": str(pagination.get("invocation_id", "")),
                "response_status": (
                    record.response_status
                    if isinstance(record, RawPayloadRecord)
                    else record.get("response_status")
                ),
                "record": record,
                "store": store,
                "request": request,
                "pagination": pagination,
            }
        )
    if not groups:
        return []
    invocation_items = [
        item for items in groups.values() for item in items if str(item.get("invocation_id", ""))
    ]
    selected_invocation = ""
    if invocation_items:
        requested_start = start or min(key[1] for key in groups)
        requested_end = end or max(key[2] for key in groups)

        def invocation_declares_range(invocation_id: str) -> bool:
            invocation_keys = {
                key
                for key, items in groups.items()
                if any(str(item.get("invocation_id", "")) == invocation_id for item in items)
            }
            return bool(invocation_keys) and (
                min(key[1] for key in invocation_keys) <= requested_start
                and max(key[2] for key in invocation_keys) >= requested_end
            )

        invocation_ids = {str(item["invocation_id"]) for item in invocation_items}
        if start is None and end is None and len(invocation_ids) > 1:
            raise RawArchiveError(
                "CNINFO archive contains multiple fetch invocations; an explicit replay "
                "date range is required to prevent silent historical truncation"
            )
        candidates = [
            item
            for item in invocation_items
            if invocation_declares_range(str(item["invocation_id"]))
        ]
        if not candidates:
            raise RawArchiveError(
                "CNINFO archive has no invocation covering requested range "
                f"{requested_start.isoformat()}..{requested_end.isoformat()}"
            )
        selected_invocation = str(
            max(candidates, key=lambda item: str(item.get("captured_at", "")))["invocation_id"]
        )
        groups = {
            key: [
                item for item in items if str(item.get("invocation_id", "")) == selected_invocation
            ]
            for key, items in groups.items()
        }
        groups = {key: items for key, items in groups.items() if items}
    roots = sorted(
        (
            key
            for key in groups
            if not any(_replay_group_contains(parent, key) for parent in groups)
        ),
        key=lambda value: (
            value[1],
            value[2],
            0 if value[0] == "szse" else 1,
            value[0],
        ),
    )
    rows: list[dict] = []
    for root in roots:
        rows.extend(
            _replay_group_rows(
                root,
                groups,
                max_pages_per_slice=max_pages_per_slice,
                active=set(),
            )
        )
    if selected_invocation and not roots:
        # Every column value this endpoint accepts returns the same all-market
        # result, so there is no second exchange an archive could be missing —
        # only an archive with no replayable root at all.
        raise RawArchiveError("CNINFO latest archived invocation has no replayable root slice")
    return rows


# Explicit alias for callers that think in terms of the individual archived
# pages rather than the normalized row stream.
replay_cninfo_pages = replay_cninfo_rows


def _announcement_rows_to_frame(rows: list[dict], *, start: date, end: date) -> pl.DataFrame:
    out: list[dict] = []
    for item in rows:
        if start == end:
            # Preserve the public single-day contract and its useful error
            # wording for callers/tests that use the legacy API.
            _validate_source_date(item, start, column=_CNINFO_COLUMN)
        try:
            source_date = _source_date(item)
        except ValueError as exc:
            raw = next(
                (
                    item[key]
                    for key in ("announcementTime", "announcementDate", "announceDate")
                    if key in item
                ),
                None,
            )
            raise RuntimeError(
                f"CNINFO announcement row has invalid announcement date {raw!r}"
            ) from exc
        # A missing source date is tolerated for the historical single-day
        # API contract, but cannot be assigned honestly in a broad interval.
        if source_date is None:
            if start != end:
                raise RawArchiveError(
                    "CNINFO archived broad-range row is missing announcement date; "
                    "refusing silent data loss"
                )
            source_date = start
        _validate_source_date_range(item, start, end, column="announcement")
        sym = _symbol_from_cninfo(str(item.get("secCode", "")))
        if not sym:
            continue
        ann_id = _announcement_id(item)
        if ann_id is None:
            logger.warning("CNINFO announcement missing announcementId and adjunctUrl; skipping")
            continue
        out.append(
            {
                "announcement_id": ann_id,
                "symbol": sym,
                "title": str(item.get("announcementTitle") or ""),
                "announce_date": source_date,
                "category": str(item.get("announcementType") or ""),
                "url": str(item.get("adjunctUrl") or ""),
            }
        )
    if not out:
        return pl.DataFrame()
    return pl.DataFrame(out).unique(subset=["announcement_id"], keep="last")


def fetch_announcement_index_range(
    start: date,
    end: date | None = None,
    *,
    client: httpx.Client | None = None,
    config=None,
    metrics: dict[str, Any] | None = None,
    checkpoint_path: str | Path | None = None,
    max_pages_per_slice: int = _DEFAULT_MAX_PAGES_PER_SLICE,
    refresh: bool = True,
    checkpoint_ttl_days: int | None = None,
    source_revision: str | None = None,
    raw_archive: RawPayloadArchive | None = None,
    run_id: str | None = None,
    request_scope: str | None = None,
) -> pl.DataFrame:
    """Fetch announcement index over ``start..end`` with resumable slicing."""
    if end is None:
        end = start
    rows = fetch_cninfo_rows(
        start,
        end,
        client=client,
        config=config,
        label="announcement",
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
    return _announcement_rows_to_frame(rows, start=start, end=end)


def fetch_announcement_index(
    trade_date: date,
    *,
    client: httpx.Client | None = None,
    config=None,
    metrics: dict[str, Any] | None = None,
    checkpoint_path: str | Path | None = None,
    max_pages_per_slice: int = _DEFAULT_MAX_PAGES_PER_SLICE,
    refresh: bool = True,
    checkpoint_ttl_days: int | None = None,
    source_revision: str | None = None,
    raw_archive: RawPayloadArchive | None = None,
    run_id: str | None = None,
    request_scope: str | None = None,
) -> pl.DataFrame:
    # A one-day range intentionally retains the historical exact-date
    # validation behaviour.
    try:
        return fetch_announcement_index_range(
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
    except RuntimeError as exc:
        logger.warning("CNINFO announcement page failed: %s", exc)
        raise


def replay_announcement_index_range(
    archive: RawPayloadArchive | str | Path,
    start: date | None = None,
    end: date | None = None,
    *,
    records: Iterable[RawPayloadRecord | Mapping[str, Any]] | None = None,
    max_pages_per_slice: int = _DEFAULT_MAX_PAGES_PER_SLICE,
) -> pl.DataFrame:
    """Rebuild announcement rows from archived CNINFO responses offline."""
    if start is None and end is not None:
        start = end
    elif start is not None and end is None:
        end = start
    raw_rows = replay_cninfo_rows(
        archive,
        "announcement_index",
        start=start,
        end=end,
        records=records,
        max_pages_per_slice=max_pages_per_slice,
    )
    if start is None and end is None:
        dates = [_source_date(row) for row in raw_rows]
        observed = [value for value in dates if value is not None]
        if not observed:
            return pl.DataFrame()
        start, end = min(observed), max(observed)
    assert start is not None and end is not None
    return _announcement_rows_to_frame(raw_rows, start=start, end=end)


def replay_announcement_index(
    archive: RawPayloadArchive | str | Path,
    trade_date: date,
    *,
    records: Iterable[RawPayloadRecord | Mapping[str, Any]] | None = None,
    max_pages_per_slice: int = _DEFAULT_MAX_PAGES_PER_SLICE,
) -> pl.DataFrame:
    """Single-day convenience wrapper around :func:`replay_announcement_index_range`."""
    return replay_announcement_index_range(
        archive,
        trade_date,
        trade_date,
        records=records,
        max_pages_per_slice=max_pages_per_slice,
    )
