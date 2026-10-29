from __future__ import annotations

import json
import logging
import math
import os
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from cnequity.file_lock import exclusive_lock

DEFAULT_LOCK_TIMEOUT_SECONDS = 15.0
# Code-level defaults for minimal TOML and programmatic Config callers. The
# packaged example may override these per source; omission must not turn a
# network adapter into an unpaced request loop.
DEFAULT_SOURCE_INTERVALS: dict[str, float] = {
    "eastmoney": 0.5,
    "eastmoney_push2": 4.0,
    "eastmoney_dc": 1.0,
    "ths": 1.0,
    "ths_pages": 3.0,
    "ths_bonus": 3.0,
    "ths_data": 3.0,
    "cninfo": 1.0,
    "pboc": 1.0,
    "nbs": 1.0,
    "exchange": 1.0,
    "futures_exchange": 1.0,
    "sw": 1.0,
    "cni": 1.0,
    "tdx_fin": 1.0,
    "sina": 0.3,
    "sina_bars": 1.0,
    "bse": 1.0,
    "baostock": 1.0,
    "tushare": 1.0,
    "ths_official": 0.4,
    "jin10": 1.0,
}
DEFAULT_UNKNOWN_SOURCE_INTERVAL_SECONDS = 1.0
# Family-wide in-flight defaults for configs that omit source caps. Aliases
# with a stricter published example cap (THS pages, Sina bars) set the family
# default; explicit source caps still determine the family limit when present.
DEFAULT_SOURCE_CONCURRENCY: dict[str, int] = {
    "eastmoney": 4,
    "eastmoney_push2": 1,
    "eastmoney_dc": 2,
    "ths": 1,
    "cninfo": 2,
    "pboc": 2,
    "nbs": 2,
    "exchange": 2,
    "futures_exchange": 1,
    "sw": 1,
    "cni": 1,
    "tdx_fin": 1,
    "sina": 2,
    "bse": 2,
    "baostock": 1,
    "tushare": 2,
    "ths_official": 2,
    "jin10": 1,
}
logger = logging.getLogger(__name__)


def _policy_day(now: float) -> str:
    return time.strftime("%Y-%m-%d", time.localtime(now))


def record_metered_attempt(state_dir: Path | str, source: str, family: str) -> None:
    """Count an admitted wire scope once across processes before it starts.

    The counter is deliberately separate from adapter ``metrics.requests``:
    those metrics also include logical retries and may omit handshakes. This
    ledger counts only scopes that passed local source policy and pacing. It
    does not claim that every scope maps to exactly one HTTP packet.
    """
    root = Path(state_dir)
    root.mkdir(parents=True, exist_ok=True)
    name = _safe_source_name(family)
    path = root / f"meter-{name}.json"
    with exclusive_lock(root / f"meter-{name}.lock"):
        now = time.time()
        day = _policy_day(now)
        previous = _read_json(path)
        aliases = previous.get("aliases") if previous.get("policy_day") == day else None
        aliases = dict(aliases) if isinstance(aliases, dict) else {}
        lane = str(source).strip().lower()
        count = aliases.get(lane, 0)
        aliases[lane] = (count if type(count) is int and count >= 0 else 0) + 1
        _write_json(
            path,
            {
                "version": 1,
                "policy_day": day,
                "family": family,
                "total": sum(value for value in aliases.values() if type(value) is int),
                "aliases": aliases,
            },
        )


@dataclass(frozen=True)
class RateLimitSpec:
    """Pickle-friendly rate limit parameters for worker processes."""

    state_dir: str
    source: str
    min_interval: float
    lock_timeout: float = DEFAULT_LOCK_TIMEOUT_SECONDS
    # A pacing limiter controls starts per second; it cannot prevent several
    # slow requests from being in flight at once.  Keep the concurrency fields
    # on the pickle-friendly spec as well so a low-level TDX call can enforce
    # both contracts at the actual socket boundary.
    concurrency_limit: int | None = None
    concurrency_state_dir: str | None = None
    concurrency_lock_timeout: float | None = None


@dataclass
class RateLimiter:
    """Cross-process fixed-spacing limiter checked at request admission.

    The file lock protects only admission. Sleep outside the lock, then check
    again: a queued caller must observe a cooldown set while it was asleep.
    """

    name: str
    min_interval: float
    state_dir: Path
    lock_timeout: float = DEFAULT_LOCK_TIMEOUT_SECONDS

    def defer(self, seconds: float) -> None:
        """Push this source's next request slot into the future.

        Unlike a local ``sleep``, the deadline is persisted under the same
        cross-process lock as normal pacing.  A vendor-wide refusal can
        therefore stop already queued workers and other CLI processes from
        continuing to hit the source during its cooling-off window.
        """
        seconds = float(seconds)
        if not math.isfinite(seconds) or seconds <= 0:
            return

        self.state_dir.mkdir(parents=True, exist_ok=True)
        lock_path = self.state_dir / f"{self.name}.lock"
        state_path = self.state_dir / f"{self.name}.json"
        with exclusive_lock(lock_path, timeout=self.lock_timeout):
            state = _read_json(state_path)
            try:
                previous_last = float(state.get("last", 0.0))
            except (TypeError, ValueError):
                previous_last = 0.0
            try:
                previous_next = float(state.get("next_allowed_at", 0.0))
            except (TypeError, ValueError):
                previous_next = 0.0
            if not math.isfinite(previous_last) or previous_last < 0:
                previous_last = 0.0
            if not math.isfinite(previous_next) or previous_next < 0:
                previous_next = 0.0
            deadline = max(previous_next, time.time() + seconds)
            state.update(last=previous_last, next_allowed_at=deadline)
            _write_json(state_path, state)

    def wait(self) -> None:
        if not math.isfinite(self.min_interval) or self.min_interval < 0:
            raise ValueError(f"{self.name}: min_interval must be finite and >= 0")
        state_path = self.state_dir / f"{self.name}.json"
        if self.min_interval == 0 and not state_path.exists():
            return
        self.state_dir.mkdir(parents=True, exist_ok=True)
        lock_path = self.state_dir / f"{self.name}.lock"

        while True:
            with exclusive_lock(lock_path, timeout=self.lock_timeout):
                state = _read_json(state_path)
                try:
                    previous_last = float(state.get("last", 0.0))
                    next_allowed_at = float(state.get("next_allowed_at", 0.0))
                except (TypeError, ValueError):
                    previous_last = 0.0
                    next_allowed_at = 0.0
                if not math.isfinite(previous_last) or previous_last < 0:
                    previous_last = 0.0
                if not math.isfinite(next_allowed_at) or next_allowed_at < 0:
                    next_allowed_at = 0.0
                now = time.time()
                try:
                    prior_interval = float(state.get("min_interval", 0))
                except (TypeError, ValueError):
                    prior_interval = 0.0
                if not math.isfinite(prior_interval) or prior_interval < 0:
                    prior_interval = 0.0
                day = _policy_day(now)
                interval = max(
                    self.min_interval,
                    prior_interval if state.get("policy_day") == day else 0.0,
                )
                if previous_last > 0:
                    next_allowed_at = max(next_allowed_at, previous_last + interval)
                delay = max(0.0, next_allowed_at - now)
                if delay == 0:
                    _write_json(
                        state_path,
                        {
                            "last": now,
                            "next_allowed_at": now + interval,
                            "min_interval": interval,
                            "policy_day": day,
                        },
                    )
                    return
            time.sleep(delay)


_CONCURRENCY_SCHEMA_VERSION = 1
_CONCURRENCY_POLL_SECONDS = 0.02
_CONCURRENCY_STALE_SECONDS = 3600.0
# Windows refuses ``os.replace`` while any other handle (an unlocked reader,
# AV, the search indexer, a sync client) has the destination open. These
# ledgers are rewritten on every request, so a short backoff covers the
# transient case; mirrors ``storage.atomic`` without importing polars here.
_REPLACE_ATTEMPTS = 5
_REPLACE_BACKOFF_SEC = 0.05
# Leases whose release could not be persisted. The owning thread stays alive,
# so the owner check alone would count them toward the cap until exit.
_ORPHANED_LEASE_TOKENS: set[str] = set()
# A writer killed between mkstemp and replace leaves ``.<stem>-*.tmp`` behind.
# A live writer replaces within milliseconds, so an hour-old one is garbage.
_STALE_TMP_SECONDS = 3600.0
_SWEPT_TMP_DIRS: set[Path] = set()


def _safe_source_name(source: str) -> str:
    """Return a stable filename component for a source name."""
    out = "".join(char if char.isalnum() or char in "._-" else "_" for char in str(source))
    return out.strip("._") or "source"


def _read_json(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _sweep_stale_tmp(directory: Path) -> None:
    """Drop temp files abandoned by killed writers, once per directory per process."""
    if directory in _SWEPT_TMP_DIRS:
        return
    _SWEPT_TMP_DIRS.add(directory)
    cutoff = time.time() - _STALE_TMP_SECONDS
    for leftover in directory.glob(".*.tmp"):
        try:
            if leftover.stat().st_mtime < cutoff:
                leftover.unlink()
        except OSError:
            continue


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    """Atomically write a small concurrency ledger while holding its lock."""
    path.parent.mkdir(parents=True, exist_ok=True)
    _sweep_stale_tmp(path.parent)
    fd, tmp_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.stem}-",
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        _replace_with_retry(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def _replace_with_retry(tmp_name: str, path: Path) -> None:
    for attempt in range(_REPLACE_ATTEMPTS):
        try:
            os.replace(tmp_name, path)
            return
        except PermissionError:
            if attempt + 1 >= _REPLACE_ATTEMPTS:
                raise
            time.sleep(_REPLACE_BACKOFF_SEC * (2**attempt))


def _windows_pid_alive(pid: int) -> bool:
    """Check a foreign process without sending it a Windows termination signal."""
    import ctypes
    from ctypes import wintypes

    synchronize = 0x00100000
    error_invalid_parameter = 87
    wait_object_0 = 0
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)
    kernel32.WaitForSingleObject.restype = wintypes.DWORD
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL

    handle = kernel32.OpenProcess(synchronize, False, pid)
    if not handle:
        # A missing PID is reclaimable; access denied or an unknown error is
        # not evidence that another worker has stopped using its lease.
        return ctypes.get_last_error() != error_invalid_parameter
    try:
        # Process objects become signaled on exit. A zero-time wait avoids the
        # ambiguity of GetExitCodeProcess when a process exits with code 259.
        return kernel32.WaitForSingleObject(handle, 0) != wait_object_0
    finally:
        kernel32.CloseHandle(handle)


def _owner_is_alive(lease: Mapping[str, object]) -> bool:
    """Best-effort stale lease detection for crashed processes/threads."""
    try:
        pid = int(lease.get("pid", 0) or 0)
        thread_id = int(lease.get("thread_id", 0) or 0)
    except (TypeError, ValueError):
        return False
    if pid <= 0:
        return False
    if pid == os.getpid():
        # A failed request must release in ``finally``; this check is only a
        # recovery path for a thread that was killed without unwinding.
        return any(item.ident == thread_id and item.is_alive() for item in threading.enumerate())
    if sys.platform == "win32":
        return _windows_pid_alive(pid)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except (PermissionError, OSError):
        # Permission denied means the process probably exists.  Do not reclaim
        # a live foreign worker's request merely because we cannot inspect it.
        return True
    return True


@dataclass
class SourceConcurrencyLimiter:
    """Cross-process lease semaphore for one upstream source.

    ``RateLimiter`` reserves a *start time* and deliberately releases its file
    lock before sleeping.  This class uses a separate lease ledger and keeps a
    lease until the caller's network operation returns.  Thus a slow request,
    a parallel DAG wave, and workers in separate processes all count toward the
    same source cap.  The ledger is crash-recoverable: dead owners are removed
    on the next acquire and old malformed leases are bounded by a conservative
    TTL.
    """

    name: str
    limit: int
    state_dir: Path
    lock_timeout: float = DEFAULT_LOCK_TIMEOUT_SECONDS
    stale_seconds: float = _CONCURRENCY_STALE_SECONDS
    _local: threading.local = field(default_factory=threading.local, init=False, repr=False)

    def __post_init__(self) -> None:
        self.state_dir = Path(self.state_dir)
        self.limit = max(1, int(self.limit))

    @property
    def _lock_path(self) -> Path:
        return self.state_dir / f"concurrency-{_safe_source_name(self.name)}.lock"

    @property
    def _state_path(self) -> Path:
        return self.state_dir / f"concurrency-{_safe_source_name(self.name)}.json"

    def _clean_leases(self, payload: Mapping[str, object], now: float) -> list[dict[str, object]]:
        leases = payload.get("leases", [])
        if not isinstance(leases, list):
            return []
        clean: list[dict[str, object]] = []
        for raw in leases:
            if not isinstance(raw, Mapping):
                continue
            try:
                created = float(raw.get("created_at", 0.0) or 0.0)
            except (TypeError, ValueError):
                continue
            # Never expire a live owner solely because a request is slow.  A
            # one-hour (or longer) request still counts toward the cap; dead
            # processes/threads are reclaimed by the owner check below.  A
            # timestamp far in the future is malformed and is discarded.
            if (
                not math.isfinite(created)
                or created <= 0
                or created > now + max(float(self.stale_seconds), 1.0)
            ):
                continue
            try:
                pid = int(raw.get("pid", 0) or 0)
                thread_id = int(raw.get("thread_id", 0) or 0)
            except (TypeError, ValueError, OverflowError):
                continue
            normalized = {**raw, "pid": pid, "thread_id": thread_id}
            if not _owner_is_alive(normalized):
                continue
            token = str(raw.get("token", "")).strip()
            if token and token not in _ORPHANED_LEASE_TOKENS:
                clean.append(
                    {
                        "token": token,
                        "pid": pid,
                        "thread_id": thread_id,
                        "created_at": created,
                    }
                )
        return clean

    def _effective_limit(self, payload: Mapping[str, object], now: float) -> int:
        try:
            previous = max(1, int(payload.get("limit", self.limit)))
        except (TypeError, ValueError):
            previous = self.limit
        if payload.get("policy_day") not in (None, _policy_day(now)):
            return self.limit
        return min(self.limit, previous)

    def acquire(self, *, timeout: float | None = None, metrics: dict | None = None) -> str:
        """Reserve one in-flight slot and return its opaque lease token."""
        started = time.perf_counter()
        deadline = None if timeout is None else started + max(float(timeout), 0.0)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        while True:
            now = time.time()
            token = uuid.uuid4().hex
            lease = {
                "token": token,
                "pid": os.getpid(),
                "thread_id": threading.get_ident(),
                "created_at": now,
            }
            with exclusive_lock(self._lock_path, timeout=self.lock_timeout):
                payload = _read_json(self._state_path)
                leases = self._clean_leases(payload, now)
                limit = self._effective_limit(payload, now)
                if len(leases) < limit:
                    leases.append(lease)
                    _write_json(
                        self._state_path,
                        {
                            "version": _CONCURRENCY_SCHEMA_VERSION,
                            "limit": limit,
                            "policy_day": _policy_day(now),
                            "leases": leases,
                        },
                    )
                    if metrics is not None:
                        metrics["concurrency_wait_seconds"] = float(
                            metrics.get("concurrency_wait_seconds", 0.0) or 0.0
                        ) + (time.perf_counter() - started)
                        metrics["concurrency_peak"] = max(
                            int(metrics.get("concurrency_peak", 0) or 0), len(leases)
                        )
                    return token
                # Persist pruning even when the cap remains full, otherwise a
                # dead owner would only disappear after another process wins a
                # later acquire race.
                previous_leases = payload.get("leases", [])
                previous_count = len(previous_leases) if isinstance(previous_leases, list) else -1
                if len(leases) != previous_count:
                    _write_json(
                        self._state_path,
                        {
                            "version": _CONCURRENCY_SCHEMA_VERSION,
                            "limit": limit,
                            "policy_day": _policy_day(now),
                            "leases": leases,
                        },
                    )
            if deadline is not None and time.perf_counter() >= deadline:
                raise TimeoutError(f"timed out acquiring {self.name} concurrency slot")
            time.sleep(_CONCURRENCY_POLL_SECONDS)

    def release(self, token: str) -> None:
        """Release a lease; repeated release is intentionally idempotent."""
        token = str(token or "").strip()
        if not token:
            return
        try:
            with exclusive_lock(self._lock_path, timeout=self.lock_timeout):
                payload = _read_json(self._state_path)
                now = time.time()
                leases = self._clean_leases(payload, now)
                remaining = [lease for lease in leases if lease.get("token") != token]
                if remaining != leases or not self._state_path.exists():
                    _write_json(
                        self._state_path,
                        {
                            "version": _CONCURRENCY_SCHEMA_VERSION,
                            "limit": self._effective_limit(payload, now),
                            "policy_day": _policy_day(now),
                            "leases": remaining,
                        },
                    )
        except FileNotFoundError:
            return
        except PermissionError as exc:
            # The request itself already finished; a ledger that stays locked
            # past the retries must not abort the caller. The next write from
            # this process prunes the orphaned lease.
            _ORPHANED_LEASE_TOKENS.add(token)
            logger.warning("%s concurrency release not persisted: %s", self.name, exc)

    @contextmanager
    def slot(self, *, timeout: float | None = None, metrics: dict | None = None) -> Iterator[None]:
        # Adapter helpers occasionally compose (for example a retry wrapper
        # around a low-level request helper). Re-entering the same source in
        # one thread must not wait on its own lease at a cap of one. The outer
        # scope still owns the lease for the whole in-flight operation.
        key = (os.getpid(), threading.get_ident())
        active = getattr(self._local, "active", None)
        if active is None:
            active = set()
            self._local.active = active
        if key in active:
            yield
            return
        token = self.acquire(timeout=timeout, metrics=metrics)
        active.add(key)
        try:
            yield
        finally:
            try:
                self.release(token)
            finally:
                active.discard(key)


@contextmanager
def source_slot(
    config: object | None,
    source: str,
    *,
    metrics: dict | None = None,
    timeout: float | None = None,
) -> Iterator[None]:
    """Hold only the configured source slot (without pacing a request)."""
    if config is None:
        yield
        return
    requester = getattr(config, "source_slot", None)
    if requester is not None:
        try:
            context = requester(source, metrics=metrics, timeout=timeout)
        except TypeError:
            context = requester(source)
        with context:
            yield
        return
    limiters = getattr(config, "_rate_limiters", None)
    if limiters is not None and hasattr(limiters, "slot"):
        try:
            context = limiters.slot(source, metrics=metrics, timeout=timeout)
        except TypeError:
            context = limiters.slot(source)
        with context:
            yield
        return
    # Lightweight test doubles from downstream integrations often only expose
    # ``rate_limit``.  They still get a valid context and retain their old
    # pacing assertions; no hidden semaphore can be created without a data root.
    yield


@contextmanager
def source_request(
    config: object | None,
    source: str,
    *,
    metrics: dict | None = None,
    timeout: float | None = None,
) -> Iterator[None]:
    """Pace and hold one source lease around exactly one network operation."""
    started = time.perf_counter()
    if config is None:
        try:
            yield
        except BaseException:
            _record_request_metrics(metrics, started, failed=True)
            raise
        else:
            _record_request_metrics(metrics, started, failed=False)
        return
    requester = getattr(config, "source_request", None)
    if requester is not None:
        try:
            context = requester(source, metrics=metrics, timeout=timeout)
        except TypeError:
            context = requester(source)
        try:
            with context:
                yield
        except BaseException:
            _record_request_metrics(metrics, started, failed=True)
            raise
        else:
            _record_request_metrics(metrics, started, failed=False)
        return
    # Preserve compatibility with simple config doubles.  The real Config
    # routes through SourceRateLimiters, where qps and concurrency are both
    # enforced; a test double cannot provide a shared state directory.
    rate = getattr(config, "rate_limit", None)
    if rate is not None:
        rate(source)
    try:
        with source_slot(config, source, metrics=metrics, timeout=timeout):
            yield
    except BaseException:
        _record_request_metrics(metrics, started, failed=True)
        raise
    else:
        _record_request_metrics(metrics, started, failed=False)


def _record_request_metrics(metrics: dict | None, started: float, *, failed: bool) -> None:
    if metrics is None:
        return
    metrics["request_seconds"] = float(metrics.get("request_seconds", 0.0) or 0.0) + max(
        0.0, time.perf_counter() - started
    )
    if failed:
        metrics["failed_requests"] = int(metrics.get("failed_requests", 0) or 0) + 1


@contextmanager
def source_request_slot_spec(
    spec: RateLimitSpec | None,
    *,
    metrics: dict | None = None,
    timeout: float | None = None,
) -> Iterator[None]:
    """Hold a low-level request slot carried by a :class:`RateLimitSpec`."""
    started = time.perf_counter()
    if spec is None or spec.concurrency_limit is None:
        try:
            if spec is not None:
                record_metered_attempt(spec.state_dir, spec.source, spec.source)
            yield
        except BaseException:
            _record_request_metrics(metrics, started, failed=True)
            raise
        else:
            _record_request_metrics(metrics, started, failed=False)
        return
    state_dir = spec.concurrency_state_dir or spec.state_dir
    limiter = SourceConcurrencyLimiter(
        spec.source,
        max(1, int(spec.concurrency_limit)),
        Path(state_dir),
        lock_timeout=spec.concurrency_lock_timeout or spec.lock_timeout,
    )
    try:
        with limiter.slot(metrics=metrics, timeout=timeout):
            record_metered_attempt(state_dir, spec.source, spec.source)
            yield
    except BaseException:
        _record_request_metrics(metrics, started, failed=True)
        raise
    else:
        _record_request_metrics(metrics, started, failed=False)


def wait_source(
    state_dir: Path | str,
    source: str,
    min_interval: float,
    lock_timeout: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
) -> None:
    RateLimiter(source, min_interval, Path(state_dir), lock_timeout=lock_timeout).wait()


def wait_spec(spec: RateLimitSpec | None) -> None:
    if spec is not None:
        wait_source(
            spec.state_dir,
            spec.source,
            spec.min_interval,
            lock_timeout=spec.lock_timeout,
        )


# Sina's anti-abuse budget, in one place because three different sweeps hit it:
# the live BJ daily-bar fallback, delisted history recovery, and the domestic
# futures board. It answers HTTP 456 — not 429 — and it cools the *vendor*, not
# the endpoint, so a sweep that keeps asking after one strands the other lanes
# too. Anything here that is retried at all is retried after a cooldown, never
# immediately.
SINA_RETRY_STATUS_CODES = frozenset({429, 456, 500, 502, 503, 504})
SINA_RATE_LIMIT_STATUS_CODES = frozenset({429, 456})
SINA_FETCH_ATTEMPTS = 3
SINA_RATE_LIMIT_COOLDOWN_SECONDS = 30.0
SINA_RATE_LIMIT_CIRCUIT_SECONDS = 120.0
