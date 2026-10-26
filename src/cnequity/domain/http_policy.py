"""Shared, fail-fast cooldowns for explicit HTTP access refusals.

These are conservative client policies, not promises about provider quotas.
They never rotate identities or routes in response to a refusal.
"""

from __future__ import annotations

import hashlib
import logging
import math
import os
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlsplit

from cnequity.domain.rate_limit import _owner_is_alive, _read_json, _safe_source_name, _write_json
from cnequity.file_lock import exclusive_lock
from cnequity.i18n import N_, Message

logger = logging.getLogger(__name__)


def source_family(source: str) -> str:
    text = str(source).strip().lower()
    if text == "ths_official":
        return text  # independent keyed service, not the public THS website
    if text.startswith("tdx_fin"):
        return "tdx_fin"  # HTTP package server, not the TDX quote protocol
    for prefix in ("ths", "cninfo", "eastmoney_push2", "eastmoney_dc", "sina"):
        if text.startswith(prefix):
            return prefix
    if text.startswith("tdx"):
        return "tdx_protocol"
    if text.startswith("eastmoney") or text in {"em", "datacenter"}:
        return "eastmoney"
    return text


class SourceCoolingDown(RuntimeError):
    """A previous refusal stops this request before any network activity."""

    reason_code = "source_transient"


def retry_after_seconds(value: str, *, now: float | None = None) -> float:
    now = time.time() if now is None else now
    try:
        seconds = float(value)
    except (ValueError, TypeError):
        try:
            deadline = parsedate_to_datetime(value)
            if deadline.tzinfo is None:
                deadline = deadline.replace(tzinfo=timezone.utc)
            seconds = deadline.timestamp() - now
        except (ValueError, TypeError, OverflowError):
            return 0.0
    return max(0.0, seconds) if math.isfinite(seconds) else 0.0


def _path(root: Path, source: str) -> Path:
    return root / f"circuit-{_safe_source_name(source_family(source))}.json"


def _deadline(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0.0
    return float(value) if math.isfinite(value) and value > 0 else 0.0


def check_source_cooldown(root: Path, source: str) -> None:
    state = _read_json(_path(root, source))
    until = _deadline(state.get("until"))
    if until > time.time():
        raise SourceCoolingDown(
            Message(
                N_(
                    "%s: HTTP %s; the shared cooldown has %d seconds left, so no request is sent "
                    "this time. Wait for a later run; do not switch IPs or delete the cooldown "
                    "state to probe repeatedly."
                ),
                source_family(source),
                state.get("status", "refusal"),
                math.ceil(until - time.time()),
            )
        )


def cooldown_status(root: Path, source: str) -> dict:
    """Read one family policy without making state directories or requests."""
    state = _read_json(_path(root, source))
    until = _deadline(state.get("until"))
    now = time.time()
    return {
        "source": source_family(source),
        "kind": state.get("kind"),
        "http_status": state.get("status"),
        "cooldown_until": until if until > now else None,
        "probe_required": bool(state.get("needs_probe")),
        "probe_inflight": bool(state.get("probe_token"))
        and _owner_is_alive(
            {"pid": state.get("probe_pid"), "thread_id": state.get("probe_thread")}
        ),
    }


@contextmanager
def source_probe_slot(root: Path, source: str) -> Iterator[None]:
    """After a refusal, allow one request across processes to test recovery."""
    path = _path(root, source)
    token = None
    if path.exists():
        with exclusive_lock(path.with_suffix(".lock")):
            state = _read_json(path)
            now = time.time()
            if _deadline(state.get("until")) > now:
                check_source_cooldown(root, source)
            if state.get("needs_probe"):
                owner = {"pid": state.get("probe_pid"), "thread_id": state.get("probe_thread")}
                if state.get("probe_token") and _owner_is_alive(owner):
                    raise SourceCoolingDown(
                        Message(
                            N_(
                                "%s: a recovery probe after the cooldown is already running; "
                                "no request is sent this time."
                            ),
                            source_family(source),
                        )
                    )
                token = uuid.uuid4().hex
                state["probe_token"] = token
                state["probe_started_at"] = now
                state["probe_pid"] = os.getpid()
                state["probe_thread"] = threading.get_ident()
                _write_json(path, state)
    try:
        yield
    except BaseException as exc:
        if token is not None:
            with exclusive_lock(path.with_suffix(".lock")):
                state = _read_json(path)
                if state.get("probe_token") == token:
                    response = getattr(exc, "response", None)
                    status = getattr(response, "status_code", None)
                    credentials = _refusal_kind(status, getattr(response, "headers", {}) or {})
                    if credentials == "credentials":
                        state.update(until=0, kind="credentials", status=status, needs_probe=False)
                    else:
                        state["until"] = max(_deadline(state.get("until")), time.time() + 300)
                        state["kind"] = "probe_failed"
                    for key in ("probe_token", "probe_started_at", "probe_pid", "probe_thread"):
                        state.pop(key, None)
                    _write_json(path, state)
        raise
    else:
        if token is not None:
            with exclusive_lock(path.with_suffix(".lock")):
                state = _read_json(path)
                if state.get("probe_token") == token:
                    state["needs_probe"] = False
                    for key in ("probe_token", "probe_started_at", "probe_pid", "probe_thread"):
                        state.pop(key, None)
                    _write_json(path, state)


def _refusal_kind(status: int, headers: object) -> str | None:
    if status == 401 or (status == 403 and headers.get("WWW-Authenticate")):
        return "credentials"
    if status == 429:
        return "rate_limited"
    if status in (403, 412, 456):
        return "access_challenge"
    return None


def record_business_refusal(
    config: object | None, source: str, *, kind: str, cooldown_seconds: float = 300.0
) -> None:
    """Adapters call this only for a known refusal code or challenge page."""
    root = getattr(config, "rate_limit_root", None)
    if not isinstance(root, Path):
        return
    path = _path(root, source)
    root.mkdir(parents=True, exist_ok=True)
    with exclusive_lock(path.with_suffix(".lock")):
        state = _read_json(path)
        state.update(
            until=max(_deadline(state.get("until")), time.time() + max(300.0, cooldown_seconds)),
            kind=kind,
            status=None,
            needs_probe=True,
        )
        for key in ("probe_token", "probe_started_at", "probe_pid", "probe_thread"):
            state.pop(key, None)
        _write_json(path, state)


def record_http_response(
    config: object | None,
    source: str,
    response: object,
    *,
    status: int | None = None,
    expected_json: bool = False,
    meter: bool = True,
) -> None:
    """Record refusals before releasing the source lease; preserve response handling.

    All status parsing, raw archival and exception contracts remain with the
    adapter. A later call (including another process/alias) fails locally.
    """
    status = status if status is not None else getattr(response, "status_code", None)
    if meter:
        try:
            _record_wire_response(config, source, response, status=status)
        except Exception as exc:
            # Optional telemetry must never turn a received source response
            # into a failed fetch or hide its actual refusal status.
            logger.warning("%s: could not save wire response meter: %s", source, exc)
    headers = getattr(response, "headers", {})
    kind = _refusal_kind(status, headers)
    if kind == "credentials":
        return  # a bad key is not evidence that the shared egress is blocked
    if kind is None and expected_json and status == 200:
        body = getattr(response, "content", b"")[:4096].lower()
        if b"<html" in body and any(
            marker in body
            for marker in (
                b"captcha",
                b"challenge",
                b"access denied",
                "验证码".encode(),  # i18n-ignore: text on a challenge page
            )
        ):
            kind = "access_challenge"
    if kind is None:
        return
    root = getattr(config, "rate_limit_root", None)
    if not isinstance(root, Path):
        return  # lightweight clients without shared configuration
    delay = max(300.0, retry_after_seconds(headers.get("Retry-After", "")))
    path = _path(root, source)
    root.mkdir(parents=True, exist_ok=True)
    with exclusive_lock(path.with_suffix(".lock")):
        state = _read_json(path)
        until = _deadline(state.get("until"))
        state.update(
            until=max(until, time.time() + delay),
            status=status,
            kind=kind,
            needs_probe=True,
        )
        for key in ("probe_token", "probe_started_at", "probe_pid", "probe_thread"):
            state.pop(key, None)
        _write_json(path, state)


def _record_wire_response(
    config: object | None, source: str, response: object, *, status: object
) -> None:
    """Aggregate returned response bodies without storing URLs or credentials.

    A transport failure before a response is counted by the admitted-scope
    meter, not this receipt meter. ``body_bytes`` is the decoded response body
    size available to the adapter; it is not a TCP byte count.
    """
    root = getattr(config, "rate_limit_root", None)
    if not isinstance(root, Path):
        return
    try:
        request = getattr(response, "request", None)
        raw_url = str(request.url if request is not None else getattr(response, "url", ""))
        parsed = urlsplit(raw_url)
        host = (parsed.hostname or "unknown").lower()
        path_hash = hashlib.sha256((parsed.path or "/").encode()).hexdigest()[:12]
        endpoint = f"{host}:{path_hash}"
    except (AttributeError, RuntimeError, TypeError, ValueError):
        endpoint = "unknown"
    try:
        body_bytes = len(getattr(response, "content", b""))
    except (RuntimeError, TypeError, ValueError):
        body_bytes = 0
    family = source_family(source)
    name = _safe_source_name(family)
    path = root / f"wire-{name}.json"
    root.mkdir(parents=True, exist_ok=True)
    with exclusive_lock(root / f"wire-{name}.lock"):
        today = time.strftime("%Y-%m-%d", time.localtime())
        saved = _read_json(path)
        if saved.get("policy_day") != today:
            saved = {"version": 1, "policy_day": today, "family": family}
        statuses = saved.get("statuses")
        statuses = dict(statuses) if isinstance(statuses, dict) else {}
        code = str(status) if type(status) is int else "unknown"
        statuses[code] = int(statuses.get(code, 0)) + 1
        endpoints = saved.get("endpoints")
        endpoints = dict(endpoints) if isinstance(endpoints, dict) else {}
        if endpoint not in endpoints and len(endpoints) >= 128:
            endpoint = "other"
        endpoints[endpoint] = int(endpoints.get(endpoint, 0)) + 1
        saved.update(
            responses=int(saved.get("responses", 0)) + 1,
            body_bytes=int(saved.get("body_bytes", 0)) + body_bytes,
            statuses=statuses,
            endpoints=endpoints,
        )
        _write_json(path, saved)


def record_cache_reuse(config: object | None, source: str, cache_name: str) -> None:
    """Count a validated source-response reuse without presenting it as a new fetch."""
    root = getattr(config, "rate_limit_root", None)
    if not isinstance(root, Path):
        return
    family = source_family(source)
    name = _safe_source_name(family)
    label = _safe_source_name(cache_name)
    path = root / f"reuse-{name}.json"
    try:
        root.mkdir(parents=True, exist_ok=True)
        with exclusive_lock(root / f"reuse-{name}.lock"):
            today = time.strftime("%Y-%m-%d", time.localtime())
            saved = _read_json(path)
            if saved.get("policy_day") != today:
                saved = {"version": 1, "policy_day": today, "family": family}
            caches = saved.get("caches")
            caches = dict(caches) if isinstance(caches, dict) else {}
            if label not in caches and len(caches) >= 64:
                label = "other"
            caches[label] = int(caches.get(label, 0)) + 1
            saved.update(hits=int(saved.get("hits", 0)) + 1, caches=caches)
            _write_json(path, saved)
    except Exception as exc:
        logger.warning("%s: could not save cache reuse meter: %s", source, exc)


def record_request_event(config: object | None, source: str, event: str) -> None:
    """Count a tagged extra send after the shared request gate admits it.

    This complements admitted scopes and response receipts. It only covers
    explicitly instrumented retry/fallback branches; the event is not a claim
    about transport-level packet counts.
    """
    if event not in {"retry", "fallback"}:
        raise ValueError(f"unknown request event {event!r}")
    root = getattr(config, "rate_limit_root", None)
    if not isinstance(root, Path):
        return
    family = source_family(source)
    name = _safe_source_name(family)
    path = root / f"events-{name}.json"
    try:
        root.mkdir(parents=True, exist_ok=True)
        with exclusive_lock(root / f"events-{name}.lock"):
            today = time.strftime("%Y-%m-%d", time.localtime())
            saved = _read_json(path)
            if saved.get("policy_day") != today:
                saved = {"version": 1, "policy_day": today, "family": family}
            saved[event] = int(saved.get(event, 0)) + 1
            _write_json(path, saved)
    except Exception as exc:
        logger.warning("%s: could not save request event meter: %s", source, exc)
