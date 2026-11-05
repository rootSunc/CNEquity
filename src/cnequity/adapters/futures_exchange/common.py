"""Shared plumbing for the futures exchanges' own daily files.

Every exchange here publishes a file per session that lists every live
contract, traded or not, with its settlement price. What differs is how each
one says "there is no file for that day" — measured on a closed session
(2026-09-25): SHFE answers 404 with an HTML page, CZCE 404 with a
「当日无数据」 page, CFFEX a 302, GFEX 200 with a single all-zero 「总计」 row.
Adapters turn all of those into :class:`FuturesDayUnavailable`, so "absent"
means the same thing whichever exchange said it.

DCE answers every request with a JavaScript challenge (HTTP 412 carrying a
``$_ts`` bootstrap). That is an access control, and this project does not work
around it: :class:`FuturesSourceBlocked` exists so a challenge is never read
as "no data".
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import math
import os
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import polars as pl

from cnequity.domain.http_policy import (
    record_cache_reuse,
    record_http_response,
    record_request_event,
)
from cnequity.domain.rate_limit import source_request
from cnequity.run_scope import run_flags_of

logger = logging.getLogger(__name__)

#: Registry and rate-limit label for every official futures exchange file.
SOURCE = "futures_exchange"

_TIMEOUT_SECONDS = 60.0
_NETWORK_ATTEMPTS = 2
_RETRY_PAUSE_SECONDS = 2.0
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
}
_CACHE_ENTRIES = 16
#: The longest disk-cache lifetime any request gets (a session older than a week).
_MAX_DISK_TTL_SECONDS = 30 * 86400
_cache: OrderedDict[str, tuple[float, bytes]] = OrderedDict()
_clients: dict[tuple, object] = {}
_blocked: dict[str, float] = {}
_transport_lock = threading.RLock()
_host_locks: dict[str, threading.RLock] = {}
_cache_lock = threading.Lock()
_pruned_cache_roots: set[Path] = set()


class FuturesDayUnavailable(RuntimeError):
    """The exchange has no file for this session (closed, or not yet published)."""

    reason_code = "source_unavailable"


class FuturesSourceBlocked(RuntimeError):
    """The exchange answered with an access challenge instead of data."""

    reason_code = "source_transient"


class FuturesPayloadError(RuntimeError):
    """The exchange answered, but not with a file this adapter understands."""

    reason_code = "source_payload_invalid"


@dataclass(frozen=True)
class ExchangeDay:
    """One exchange's contracts for one session, split by instrument kind."""

    exchange: str
    trade_date: date
    futures: pl.DataFrame = field(default_factory=pl.DataFrame)
    options: pl.DataFrame = field(default_factory=pl.DataFrame)


def looks_like_challenge(status_code: int, body: bytes) -> bool:
    """A WAF bootstrap page, as opposed to an ordinary error page."""
    head = body[:4096]
    return status_code == 412 or b"$_ts" in head


def _budget_root(config) -> Path | None:
    shared = os.environ.get("CNE_RATE_LIMIT_ROOT")
    return (
        Path(shared).expanduser()
        if shared
        else config.meta_root / "rate_limits"
        if config
        else None
    )


def _host_key(url: str, source: str) -> str:
    # Sina stocks and derivatives share a refusal domain and pacing budget.
    return "sina" if source.startswith("sina") else (urlsplit(url).hostname or "unknown")


def check_circuit(url: str, *, config=None, source: str = SOURCE) -> None:
    from cnequity.storage.derivative_evidence import read_json

    host = _host_key(url, source)
    root = _budget_root(config)
    deadline = _blocked.get(host, 0.0)
    if root is not None:
        deadline = max(deadline, float(read_json(root / f"refusal-{host}.json").get("until", 0)))
    if deadline > time.time() or host in getattr(config, "_derivative_blocked_hosts", set()):
        raise FuturesSourceBlocked(f"{host}: circuit open; do not retry in this run")


def check_response(resp, url: str, *, config=None, source: str = SOURCE) -> None:
    from cnequity.storage.atomic import write_json_atomic

    code = resp.status_code
    html = resp.content.lstrip().lower().startswith((b"<!doctype html", b"<html"))
    if (
        code in (403, 412, 429, 456)
        or 500 <= code < 600
        or looks_like_challenge(code, resp.content)
        or (code == 200 and html)
    ):
        seconds = 300.0
        retry_after = resp.headers.get("Retry-After", "")
        try:
            seconds = max(seconds, float(retry_after))
        except ValueError:
            try:
                until = parsedate_to_datetime(retry_after)
                seconds = max(seconds, (until - datetime.now(timezone.utc)).total_seconds())
            except (ValueError, TypeError, OverflowError):
                pass
        host = _host_key(url, source)
        deadline = time.time() + seconds
        _blocked[host] = deadline
        if config is not None:
            blocked = getattr(config, "_derivative_blocked_hosts", set())
            blocked.add(host)
            config._derivative_blocked_hosts = blocked
            config.defer_source(source, seconds)
        root = _budget_root(config)
        if root is not None:
            write_json_atomic(root / f"refusal-{host}.json", {"until": deadline, "status": code})
        raise FuturesSourceBlocked(f"{url}: upstream refusal (HTTP {code}); cooldown {seconds:g}s")
    if code in (301, 302, 303, 307, 308, 404):
        raise FuturesDayUnavailable(f"{url}: HTTP {code}")
    if code != 200:
        raise FuturesPayloadError(f"{url}: HTTP {code}")
    if resp.content.lstrip().lower().startswith((b"<!doctype html", b"<html")):
        raise FuturesPayloadError(f"{url}: HTML instead of market data")


def fetch_bytes(
    url: str,
    *,
    config=None,
    method: str = "GET",
    data: dict | None = None,
    json_body: dict | None = None,
    follow_redirects: bool = False,
    source: str = SOURCE,
    ttl: float = 3600.0,
    as_of: date | None = None,
    client=None,
    params: dict | None = None,
) -> bytes:
    """Paced, single-flight, cached public market-data requests.

    Cache TTL is bounded even for historical files: exchanges can correct
    them. A refresh bypasses cached responses, never an upstream circuit.
    Disk entries retain exact response bytes; parsers still validate on reuse.
    """
    from contextlib import nullcontext

    from cnequity.file_lock import exclusive_lock
    from cnequity.storage.atomic import write_json_atomic
    from cnequity.storage.derivative_evidence import read_json

    if as_of is not None:
        from cnequity.domain.market_time import shanghai_today

        ttl = _MAX_DISK_TTL_SECONDS if (shanghai_today() - as_of).days > 7 else 3600
    identity = json.dumps([method, url, data, json_body, params, follow_redirects], sort_keys=True)
    key = hashlib.sha256(identity.encode()).hexdigest()
    cache_root = config.meta_root / "derivatives" / "http_cache" if config else None
    path = cache_root / f"{key}.json" if cache_root else None
    if cache_root is not None:
        with _cache_lock:
            first_use = cache_root not in _pruned_cache_roots
            _pruned_cache_roots.add(cache_root)
        if first_use:
            prune_disk_cache(cache_root)
    budget = _budget_root(config)
    host = _host_key(url, source)
    refreshed = getattr(config, "_derivative_refreshed_requests", set())
    refresh = bool(run_flags_of(config).derivatives_refresh) and key not in refreshed
    # Serialize per origin across processes. Independent origins remain free.
    lock = (
        exclusive_lock(budget / f"derivatives-http-{host}.lock", timeout=90)
        if budget
        else nullcontext()
    )
    with _cache_lock:
        host_lock = _host_locks.setdefault(host, threading.RLock())
    with host_lock, lock:
        now = time.time()
        with _cache_lock:
            cached = _cache.get(key)
            if not refresh and cached is not None and now - cached[0] < ttl:
                _cache.move_to_end(key)
                record_cache_reuse(
                    config, SOURCE + "_" + host if source == SOURCE else source, "memory_response"
                )
                return cached[1]
        if path is not None and not refresh:
            saved = read_json(path)
            if now - float(saved.get("time", 0)) < ttl:
                try:
                    body = base64.b64decode(saved["body"], validate=True)
                    if hashlib.sha256(body).hexdigest() == saved.get("sha256"):
                        record_cache_reuse(
                            config,
                            SOURCE + "_" + host if source == SOURCE else source,
                            "disk_response",
                        )
                        return body
                except (ValueError, KeyError):
                    pass
        check_circuit(url, config=config, source=source)
        if client is None:
            # httpx.Client is thread-safe; one pool per request policy avoids
            # retaining clients for every short-lived DAG worker thread.
            pool_key = (follow_redirects, source)
            with _transport_lock:
                if pool_key not in _clients:
                    headers = {
                        **_HEADERS,
                        **({"Referer": "https://finance.sina.com.cn"} if source == "sina" else {}),
                    }
                    _clients[pool_key] = httpx.Client(
                        timeout=_TIMEOUT_SECONDS, headers=headers, follow_redirects=follow_redirects
                    ).__enter__()
                client = _clients[pool_key]
        for attempt in range(_NETWORK_ATTEMPTS):
            try:
                pacing = SOURCE + "_" + host if source == SOURCE else source
                with source_request(config, pacing):
                    check_circuit(url, config=config, source=source)
                    if attempt:
                        record_request_event(config, pacing, "retry")
                    resp = client.request(method, url, data=data, json=json_body, params=params)
                    record_http_response(config, pacing, resp)
                break
            except (httpx.NetworkError, httpx.RemoteProtocolError) as exc:
                if attempt + 1 == _NETWORK_ATTEMPTS:
                    raise FuturesPayloadError(f"{url}: {type(exc).__name__}: {exc}") from exc
                time.sleep(_RETRY_PAUSE_SECONDS)
            except httpx.HTTPError as exc:
                raise FuturesPayloadError(f"{url}: {type(exc).__name__}: {exc}") from exc
        check_response(resp, url, config=config, source=source)
        body = resp.content
        if config is not None and run_flags_of(config).derivatives_refresh:
            refreshed.add(key)
            config._derivative_refreshed_requests = refreshed
        with _cache_lock:
            _cache[key] = (time.time(), body)
            _cache.move_to_end(key)
            while len(_cache) > _CACHE_ENTRIES:
                _cache.popitem(last=False)
        if path is not None:
            write_json_atomic(
                path,
                {
                    "time": time.time(),
                    "body": base64.b64encode(body).decode(),
                    "sha256": hashlib.sha256(body).hexdigest(),
                },
            )
        return body


def prune_disk_cache(cache_root: Path, *, now: float | None = None) -> int:
    """Delete disk entries older than the longest TTL; return how many went.

    No request can reuse such an entry, so it only occupies space: a
    backfill left thousands of them behind. A file's mtime is its write
    time, and reuse never rewrites it. Runs once per cache root per process.
    """
    cutoff = (time.time() if now is None else now) - _MAX_DISK_TTL_SECONDS
    removed = 0
    try:
        entries = list(os.scandir(cache_root))
    except FileNotFoundError:
        return 0
    for entry in entries:
        if not entry.name.endswith(".json") or not entry.is_file(follow_symlinks=False):
            continue
        try:
            if entry.stat(follow_symlinks=False).st_mtime < cutoff:
                os.unlink(entry.path)
                removed += 1
        except FileNotFoundError:
            continue
    if removed:
        logger.info("derivatives http_cache: removed %d expired entries", removed)
    return removed


def clear_cache() -> None:
    """Close pooled connections and clear process state; disk evidence remains."""
    with _transport_lock:
        _cache.clear()
        _blocked.clear()
        _pruned_cache_roots.clear()
        for client in _clients.values():
            client.__exit__(None, None, None)
        _clients.clear()


def decode_text(body: bytes) -> str:
    """The exchanges moved from GBK to UTF-8 at different times; try both."""
    if body.startswith(b"\xef\xbb\xbf"):
        return body[3:].decode("utf-8")
    try:
        return body.decode("utf-8")
    except UnicodeDecodeError:
        return body.decode("gb18030")


def parse_number(value) -> float | None:
    """A published figure; blanks, dashes and thousands separators tolerated."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    text = str(value).replace(",", "").strip()
    if not text or text in {"-", "--", "nan", "None", "null"}:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def parse_price(value) -> float | None:
    """A price field: zero is how the files print "no price", never a price."""
    number = parse_number(value)
    return number if number is not None and number > 0 else None


def parse_option_settle(value) -> float | None:
    """An option's settlement, where zero *is* a price.

    On its last session an out-of-the-money option settles at 0 — every
    exchange prints it that way (CFFEX IO2609 puts, CZCE AP2610 calls and GFEX
    on 2026-09 expiry days). Read as "no price", that zero also hid the put
    side of CFFEX's parity forward on expiry day. A row that is zero for want
    of anything to report — no settle, no trade, no position — is still a
    placeholder and dropped by :func:`is_placeholder`.
    """
    number = parse_number(value)
    return number if number is not None and number >= 0 else None


def drop_placeholders(rows: list[dict]) -> list[dict]:
    return [row for row in rows if not is_placeholder(row)]


def is_placeholder(row: dict) -> bool:
    """A listed-but-not-yet-opened month: no settlement, no position, no trade.

    SHFE's early files carry far months this way (2002-01-07: CU0210–CU0212 at
    settlement 0 with zero open interest). Nothing is known about such a row,
    so it is not a live contract yet; a zero settlement that *does* carry
    positions is kept, and the schema rejects it as the anomaly it is.
    """
    return not row.get("settle") and not row.get("volume") and not row.get("open_interest")
