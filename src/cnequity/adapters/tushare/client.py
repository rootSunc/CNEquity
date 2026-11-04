"""Shared Tushare Pro client: one endpoint, one token, paging and refusals.

Every Tushare-backed dataset goes through here, so the whole source stays
optional in one place: with no token (or ``[sources.tushare]`` disabled)
``tushare_enabled`` is false and nothing sends a request. The endpoint is
``[sources.tushare].api_url`` (default ``api.tushare.pro``); any server that
speaks the same protocol works, and its rows are still Tushare's, stamped
``tushare``. Requests go to ``{api_url}/{api_name}``, the path the official
SDK uses and the official server also accepts.

A refusal in a 200 envelope is deterministic and raised at once: a bad token
or a missing permission never improves on retry, and a quota or rate refusal
cools the source down instead of hammering it. Only transport errors,
timeouts, 408 and 5xx are retried.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Callable, Mapping
from datetime import datetime, timezone

import httpx
import polars as pl

from cnequity.domain.http_policy import (
    SourceCoolingDown,
    record_business_refusal,
    record_http_response,
    record_request_event,
)
from cnequity.domain.rate_limit import source_request
from cnequity.storage.atomic import write_json_atomic

__all__ = [
    "SOURCE",
    "TUSHARE_API_URL",
    "TushareClient",
    "TushareNotConfigured",
    "TusharePermissionDenied",
    "TushareQuotaExhausted",
    "TushareRefused",
    "api_url",
    "probe_permissions",
    "tushare_enabled",
]

logger = logging.getLogger(__name__)

SOURCE = "tushare"
TUSHARE_API_URL = "https://api.tushare.pro"
# Official limits vary by endpoint; some servers cap every call at 3,000 rows.
DEFAULT_PAGE_SIZE = 2000
DEFAULT_MAX_PAGES = 1000
_PERMISSION_CACHE_SECONDS = 7 * 86400
_RATE_MARKERS = ("频繁", "限流", "每分钟", "频率", "超限", "rate limit", "too many requests")
_PERMISSION_MARKERS = ("权限", "积分", "permission")
_DAILY_QUOTA_MARKERS = ("次/天", "每天", "per day")
_INVALID_TOKEN_CODE = 40101
_PERMISSION_CODE = 40203


class TushareNotConfigured(RuntimeError):
    """``[sources.tushare]`` is disabled or has no token."""


class TushareRefused(RuntimeError):
    """A deterministic business refusal (bad token, bad parameters)."""

    def __init__(self, api: str, code: object, message: str):
        super().__init__(f"tushare {api} refused ({code}): {message}")
        self.api = api
        self.code = code
        self.message = message


class TusharePermissionDenied(TushareRefused):
    """The token's points or purchases do not cover this endpoint."""


class TushareQuotaExhausted(TushareRefused):
    """A per-endpoint daily quota is used up (stk_mins: 2 calls a day).

    Unlike a per-minute rate refusal it says nothing about other endpoints,
    so the source is not cooled down; the caller retries tomorrow.
    """


def api_url(config) -> str:
    url = getattr(config, "tushare_api_url", None) if config is not None else None
    return (str(url).strip().rstrip("/") if url else "") or TUSHARE_API_URL


def tushare_enabled(config) -> bool:
    """True only when the source is switched on and a token is configured."""
    if config is None:
        return False
    sources = getattr(config, "sources", None) or {}
    return bool(sources.get("tushare", False) and getattr(config, "tushare_token", None))


def _is_permission(code: object, message: str) -> bool:
    return code in (_PERMISSION_CODE, str(_PERMISSION_CODE)) or any(
        marker in message.lower() for marker in _PERMISSION_MARKERS
    )


class TushareClient:
    """Query Tushare endpoints as DataFrames. Use as a context manager."""

    def __init__(
        self,
        config,
        *,
        client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if not tushare_enabled(config):
            raise TushareNotConfigured(
                "Tushare needs [sources.tushare] enabled and TUSHARE_TOKEN or token_file"
            )
        self.config = config
        self.url = api_url(config)
        self._token = str(config.tushare_token).strip()
        self._own = client is None
        self._client = client or httpx.Client(
            timeout=float(getattr(config, "tushare_timeout_sec", 30.0))
        )
        self._sleep = sleep

    def __enter__(self) -> TushareClient:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        if self._own:
            self._client.close()

    def _post(self, api: str, params: Mapping, fields: str) -> dict:
        config = self.config
        attempts = max(1, int(getattr(config, "max_retries", 1)))
        backoff = float(getattr(config, "retry_backoff_seconds", 0.0))
        payload = {"api_name": api, "token": self._token, "params": dict(params), "fields": fields}
        last_error: Exception | None = None
        for attempt in range(attempts):
            try:
                with source_request(config, SOURCE):
                    if attempt:
                        record_request_event(config, SOURCE, "retry")
                    response = self._client.post(f"{self.url}/{api}", json=payload)
                    record_http_response(config, SOURCE, response)
                    response.raise_for_status()
                    body = response.json()
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status == 429:
                    raise SourceCoolingDown("tushare: HTTP 429 rate-limit response") from exc
                if status != 408 and not (status is not None and 500 <= status <= 599):
                    raise
                last_error = exc
            except (httpx.RequestError, ValueError) as exc:
                last_error = exc
            else:
                if not isinstance(body, dict):
                    raise ValueError(f"tushare {api}: response is not a JSON object")
                code = body.get("code")
                if code in (0, "0"):
                    return body
                message = str(body.get("msg") or body.get("message") or "")
                if any(marker in message.lower() for marker in _DAILY_QUOTA_MARKERS):
                    raise TushareQuotaExhausted(api, code, message)
                if any(marker in message.lower() for marker in _RATE_MARKERS):
                    record_business_refusal(config, SOURCE, kind="business_rate_limit")
                    raise SourceCoolingDown(f"tushare {api}: provider rate-limit response")
                if _is_permission(code, message):
                    raise TusharePermissionDenied(api, code, message)
                raise TushareRefused(api, code, message)
            if attempt + 1 < attempts:
                self._sleep(max(0.0, backoff) * (attempt + 1))
        assert last_error is not None
        raise last_error

    def query(self, api: str, params: Mapping | None = None, fields: str = "") -> pl.DataFrame:
        """One call, as returned. Columns keep the source's names."""
        body = self._post(api, params or {}, fields)
        return _frame(api, body)

    def query_all(
        self,
        api: str,
        params: Mapping | None = None,
        fields: str = "",
        *,
        page_size: int = DEFAULT_PAGE_SIZE,
        max_pages: int = DEFAULT_MAX_PAGES,
    ) -> pl.DataFrame:
        """Every page of one query; a short page ends it.

        A page that comes back exactly full past ``max_pages`` raises rather
        than returning a silently truncated result.
        """
        frames = []
        for page in range(max_pages):
            frame = self.query(
                api, {**(params or {}), "limit": page_size, "offset": page * page_size}, fields
            )
            frames.append(frame)
            if frame.height < page_size:
                break
        else:
            raise RuntimeError(f"tushare {api}: pagination exceeded {max_pages} pages")
        non_empty = [f for f in frames if f.width]
        return pl.concat(non_empty, how="diagonal_relaxed") if non_empty else frames[0]


def _frame(api: str, body: dict) -> pl.DataFrame:
    data = body.get("data")
    if not isinstance(data, dict):
        raise ValueError(f"tushare {api}: response has no data object")
    fields, items = data.get("fields"), data.get("items")
    if not isinstance(fields, list) or not isinstance(items, list):
        raise ValueError(f"tushare {api}: response has invalid fields/items")
    names = [str(field) for field in fields]
    for item in items:
        if not isinstance(item, list) or len(item) != len(names):
            raise ValueError(f"tushare {api}: response contains a malformed row")
    if not items:
        return pl.DataFrame(schema=dict.fromkeys(names, pl.Utf8))
    return pl.DataFrame(items, schema=names, orient="row", infer_schema_length=None, strict=False)


def probe_permissions(
    config,
    probes: Mapping[str, Mapping],
    *,
    client: TushareClient | None = None,
    now: Callable[[], float] = time.time,
) -> dict[str, str]:
    """Which endpoints this token may call: ``ok``, ``denied`` or ``error: …``.

    Results are cached per token and endpoint for a week under
    ``meta/tushare/permissions/``, so a daily run does not spend calls
    rediscovering what the account was sold. A probe uses the smallest
    query the caller supplies. A cooling-down source stops the probe, keeping
    what was learned before it.
    """
    if not tushare_enabled(config):
        return {}
    token_key = hashlib.sha256(str(config.tushare_token).strip().encode()).hexdigest()[:16]
    path = config.meta_root / "tushare" / "permissions" / f"{token_key}.json"
    try:
        cached = json.loads(path.read_text()) if path.exists() else {}
    except (OSError, ValueError):
        cached = {}
    entries = dict(cached.get("endpoints", {})) if cached.get("url") == api_url(config) else {}
    fresh = {
        api: entry
        for api, entry in entries.items()
        if now() - float(entry.get("checked_at", 0)) < _PERMISSION_CACHE_SECONDS
    }
    missing = [api for api in probes if api not in fresh]
    if missing:
        own = client is None
        client = client or TushareClient(config)
        try:
            for api in missing:
                try:
                    client.query(api, {**probes[api], "limit": 1})
                    status = "ok"
                except TusharePermissionDenied:
                    status = "denied"
                except TushareQuotaExhausted:
                    status = "ok"
                except TushareRefused as exc:
                    status = f"error: {exc.code} {exc.message}"
                fresh[api] = {"status": status, "checked_at": now()}
        finally:
            if own:
                client.close()
            write_json_atomic(
                path,
                {
                    "url": api_url(config),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "endpoints": fresh,
                },
                indent=2,
            )
    return {api: fresh[api]["status"] for api in probes}
