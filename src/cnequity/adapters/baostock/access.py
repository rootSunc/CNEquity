"""Published BaoStock free-API limits.

These are vendor rules, not pacing preferences. A calendar day and a calendar
year are both Asia/Shanghai. The client stops before it would cross them:

* at most ``DAILY_REQUEST_LIMIT`` API calls per day (login, query, and logout
  each count as one call);
* one connection at a time, across processes that share the egress ledger;
* after error ``10001011`` / a blacklist message, freeze for
  ``blacklist strikes this year × 6 hours`` and do not log in again during
  that freeze. A repeated refusal while the freeze is still running is the
  same incident and does not add another strike;
* when the response has no release time, do not refresh or probe for at least
  five minutes. The freeze above is longer than that, so the client waits out
  the freeze instead of logging in to ask whether the page has filled in.
"""

from __future__ import annotations

import logging
import math
import re
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from cnequity.domain.http_policy import SourceCoolingDown, source_family
from cnequity.domain.market_time import SHANGHAI_TZ
from cnequity.domain.rate_limit import _read_json, _safe_source_name, _write_json
from cnequity.file_lock import LockUnavailable, exclusive_lock

logger = logging.getLogger(__name__)

BAOSTOCK_BLACKLIST_CODE = "10001011"
DAILY_REQUEST_LIMIT = 50_000
MAX_CONCURRENT_CONNECTIONS = 1
BLACKLIST_HOURS_PER_STRIKE = 6
EMPTY_RELEASE_REFRESH_SECONDS = 5 * 60
# A caller queues this long for the one connection before giving up. Short
# sweeps take turns; one stuck behind a multi-hour sweep stops instead of
# hanging a scheduled run.
CONNECTION_WAIT_SECONDS = 10 * 60

_SHANGHAI = SHANGHAI_TZ
_LEDGER_NAME = "baostock-access.json"
_CONNECTION_LOCK = "baostock-connection.lock"
_RELEASE_AT = re.compile(r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}(?::\d{2})?)")
_connection = threading.local()


def _circuit_path(root: Path) -> Path:
    family = _safe_source_name(source_family("baostock"))
    return root / f"circuit-{family}.json"


def _shanghai(now: float) -> datetime:
    return datetime.fromtimestamp(now, _SHANGHAI)


def _day(now: float) -> str:
    return _shanghai(now).date().isoformat()


def _year(now: float) -> int:
    return _shanghai(now).year


def release_timestamp(message: str) -> float | None:
    """Parse a release time from a vendor message, as Shanghai local time."""
    match = _RELEASE_AT.search(message or "")
    if match is None:
        return None
    text = f"{match.group(1)} {match.group(2)}"
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M")
        except ValueError:
            return None
    return parsed.replace(tzinfo=_SHANGHAI).timestamp()


def cooldown_seconds(strikes: int, *, release_at: float | None, now: float) -> float:
    """How long to stay silent after one new blacklist incident.

    The published freeze is ``strikes × 6 hours``. A vendor release time that
    is later than that is honored. A missing or earlier time is not a reason
    to reconnect: an empty release time still waits at least five minutes,
    and never less than the published freeze.
    """
    formula = max(1, int(strikes)) * BLACKLIST_HOURS_PER_STRIKE * 3600
    if release_at is None:
        return max(formula, float(EMPTY_RELEASE_REFRESH_SECONDS))
    remaining = float(release_at) - now
    if remaining <= 0:
        return max(formula, float(EMPTY_RELEASE_REFRESH_SECONDS))
    return max(formula, remaining)


def _root(config: object | None) -> Path | None:
    root = getattr(config, "rate_limit_root", None)
    return root if isinstance(root, Path) else None


def admit_request(config: object | None, *, now: float | None = None) -> None:
    """Count one API call, or refuse it before it is sent.

    No ledger means a lightweight test double. Production ``Config`` always
    has ``rate_limit_root``, and that ledger is shared by every lake on the
    egress.
    """
    root = _root(config)
    if root is None:
        return
    now = time.time() if now is None else now
    day = _day(now)
    path = root / _LEDGER_NAME
    root.mkdir(parents=True, exist_ok=True)
    with exclusive_lock(path.with_suffix(".lock")):
        saved = _read_json(path)
        requests = saved.get("requests", 0) if saved.get("day") == day else 0
        try:
            count = int(requests)
        except (TypeError, ValueError):
            count = 0
        if count < 0:
            count = 0
        if count >= DAILY_REQUEST_LIMIT:
            raise SourceCoolingDown(
                f"baostock: 今日请求已达 {DAILY_REQUEST_LIMIT} 次上限"
                f"（上海自然日 {day}），本次不发请求。请等到次日再续跑，不要换 IP。"
            )
        _write_json(
            path,
            {"version": 1, "day": day, "requests": count + 1, "limit": DAILY_REQUEST_LIMIT},
        )


def record_blacklist(
    config: object | None,
    message: str = "",
    *,
    now: float | None = None,
) -> dict:
    """Remember one blacklist incident and the time until which we stay silent."""
    now = time.time() if now is None else now
    year = _year(now)
    release_at = release_timestamp(message)
    root = _root(config)
    if root is None:
        seconds = cooldown_seconds(1, release_at=release_at, now=now)
        return {
            "strikes": 1,
            "seconds": seconds,
            "until": now + seconds,
            "release_known": release_at is not None,
            "persisted": False,
        }

    path = _circuit_path(root)
    root.mkdir(parents=True, exist_ok=True)
    with exclusive_lock(path.with_suffix(".lock")):
        state = _read_json(path)
        until = state.get("until")
        try:
            deadline = float(until)
        except (TypeError, ValueError):
            deadline = 0.0
        if not math.isfinite(deadline) or deadline < 0:
            deadline = 0.0
        active = deadline > now and state.get("kind") == "ip_blacklist"
        strikes = state.get("blacklist_strikes", 0)
        try:
            strikes = int(strikes)
        except (TypeError, ValueError):
            strikes = 0
        if strikes < 0 or state.get("blacklist_year") != year:
            strikes = 0
        if active:
            if release_at is not None and release_at > deadline:
                deadline = release_at
            state.update(
                until=deadline,
                kind="ip_blacklist",
                status=BAOSTOCK_BLACKLIST_CODE,
                needs_probe=True,
                blacklist_year=year,
                blacklist_strikes=strikes,
                release_known=release_at is not None,
            )
        else:
            strikes += 1
            seconds = cooldown_seconds(strikes, release_at=release_at, now=now)
            deadline = now + seconds
            state.update(
                until=deadline,
                kind="ip_blacklist",
                status=BAOSTOCK_BLACKLIST_CODE,
                needs_probe=True,
                blacklist_year=year,
                blacklist_strikes=strikes,
                release_known=release_at is not None,
            )
        for key in ("probe_token", "probe_started_at", "probe_pid", "probe_thread"):
            state.pop(key, None)
        _write_json(path, state)
    return {
        "strikes": strikes,
        "seconds": max(0.0, deadline - now),
        "until": deadline,
        "release_known": release_at is not None,
        "persisted": True,
    }


def status(config: object) -> dict:
    """Read the published limits and today's usage without creating files."""
    root = _root(config)
    now = time.time()
    day = _day(now)
    year = _year(now)
    requests = 0
    strikes = 0
    if root is not None:
        ledger = _read_json(root / _LEDGER_NAME)
        if ledger.get("day") == day:
            try:
                requests = max(0, int(ledger.get("requests", 0)))
            except (TypeError, ValueError):
                requests = 0
        circuit = _read_json(_circuit_path(root))
        if circuit.get("blacklist_year") == year:
            try:
                strikes = max(0, int(circuit.get("blacklist_strikes", 0)))
            except (TypeError, ValueError):
                strikes = 0
    return {
        "calendar": "Asia/Shanghai",
        "day": day,
        "requests_today": requests,
        "daily_limit": DAILY_REQUEST_LIMIT,
        "remaining": max(0, DAILY_REQUEST_LIMIT - requests),
        "max_concurrent_connections": MAX_CONCURRENT_CONNECTIONS,
        "blacklist_year": year,
        "blacklist_strikes": strikes,
        "blacklist_hours_per_strike": BLACKLIST_HOURS_PER_STRIKE,
        "empty_release_refresh_seconds": EMPTY_RELEASE_REFRESH_SECONDS,
    }


@contextmanager
def hold_baostock_connection(config: object | None) -> Iterator[None]:
    """Hold the one allowed TCP session until this block logs out.

    A second process or thread waits up to ``CONNECTION_WAIT_SECONDS`` for it,
    then fails before ``login`` instead of opening another connection. The
    same thread may re-enter while it already holds
    the session, so a roster query can run inside the login that opened it.
    """
    root = _root(config)
    if root is None:
        yield
        return
    depth = getattr(_connection, "depth", 0)
    if depth:
        _connection.depth = depth + 1
        try:
            yield
        finally:
            _connection.depth = depth
        return
    path = root / _CONNECTION_LOCK
    try:
        with exclusive_lock(path, timeout=CONNECTION_WAIT_SECONDS):
            _connection.depth = 1
            try:
                yield
            finally:
                _connection.depth = 0
    except LockUnavailable as exc:
        raise SourceCoolingDown(
            f"baostock: 已有连接在使用，等待 {CONNECTION_WAIT_SECONDS // 60} 分钟仍未释放。"
            "该源不允许并发连接，本次不登录。"
        ) from exc
