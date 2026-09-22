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
logger = logging.getLogger(__name__)


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
    """Cross-process fixed-spacing limiter using reserved request time slots.

    The file lock protects only the reservation transaction. Waiting for the
    reserved slot happens after the lock is released, so one slow process does
    not make every other process wait behind a sleeping lock holder.
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
            _write_json(
                state_path,
                {"last": previous_last, "next_allowed_at": deadline},
            )

    def wait(self) -> None:
        if self.min_interval <= 0:
            return

        self.state_dir.mkdir(parents=True, exist_ok=True)
        lock_path = self.state_dir / f"{self.name}.lock"
        state_path = self.state_dir / f"{self.name}.json"

        lock_started = time.monotonic()
        with exclusive_lock(lock_path, timeout=self.lock_timeout):
            lock_wait = time.monotonic() - lock_started
            previous_last = 0.0
            next_allowed_at = 0.0
            if state_path.exists():
                try:
                    state = json.loads(state_path.read_text(encoding="utf-8"))
                    previous_last = float(state.get("last", 0.0))
                    next_allowed_at = float(state.get("next_allowed_at", 0.0))
                except (json.JSONDecodeError, TypeError, ValueError):
                    previous_last = 0.0
                    next_allowed_at = 0.0

            if not math.isfinite(previous_last) or previous_last < 0.0:
                previous_last = 0.0
            if not math.isfinite(next_allowed_at) or next_allowed_at < 0.0:
                next_allowed_at = 0.0

            # Migrate the old state format, where `last` was the timestamp of
            # the previous request start. A missing/corrupt state is safe to
            # treat as empty; the first request then gets the current slot.
            if next_allowed_at <= 0.0 and previous_last > 0.0:
                next_allowed_at = previous_last + self.min_interval

            now = time.time()
            slot = max(now, next_allowed_at)
            reserved_next = slot + self.min_interval

            fd, tmp_name = tempfile.mkstemp(
                dir=state_path.parent,
                prefix=f".{state_path.stem}-",
                suffix=".tmp",
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    json.dump(
                        {"last": slot, "next_allowed_at": reserved_next},
                        handle,
                    )
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(tmp_name, state_path)
            except Exception:
                try:
                    os.unlink(tmp_name)
                except OSError:
                    pass
                raise

        sleep_for = max(0.0, slot - time.time())
        if sleep_for:
            time.sleep(sleep_for)
        logger.debug(
            "rate limit %s: lock_wait=%.3fs sleep=%.3fs",
            self.name,
            lock_wait,
            sleep_for,
        )


_CONCURRENCY_SCHEMA_VERSION = 1
_CONCURRENCY_POLL_SECONDS = 0.02
_CONCURRENCY_STALE_SECONDS = 3600.0


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


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    """Atomically write a small concurrency ledger while holding its lock."""
    path.parent.mkdir(parents=True, exist_ok=True)
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
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def _windows_pid_alive(pid: int) -> bool:
    """Liveness probe for a foreign pid on Windows.

    ``os.kill(pid, 0)`` is a POSIX idiom that does not translate: on Windows
    any signal other than the CTRL events is handled by ``TerminateProcess``,
    so it is not a probe at all, and a dead pid surfaces as a plain ``OSError``
    (WinError 87) that the caller cannot tell apart from "not allowed to
    inspect". Open the process for querying instead and read its exit code.
    """
    import ctypes
    from ctypes import wintypes

    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    STILL_ACTIVE = 259
    ERROR_INVALID_PARAMETER = 87

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]

    handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not handle:
        # WinError 87: no process with this pid exists. Anything else
        # (access denied on a protected process) — assume alive.
        return ctypes.get_last_error() != ERROR_INVALID_PARAMETER
    try:
        exit_code = wintypes.DWORD()
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
            return True
        return exit_code.value == STILL_ACTIVE
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
            if token:
                clean.append(
                    {
                        "token": token,
                        "pid": pid,
                        "thread_id": thread_id,
                        "created_at": created,
                    }
                )
        return clean

    def acquire(self, *, timeout: float | None = None, metrics: dict | None = None) -> str:
        """Reserve one in-flight slot and return its opaque lease token."""
        limit = max(1, int(self.limit))
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
                if len(leases) < limit:
                    leases.append(lease)
                    _write_json(
                        self._state_path,
                        {
                            "version": _CONCURRENCY_SCHEMA_VERSION,
                            "limit": limit,
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
                leases = self._clean_leases(payload, time.time())
                remaining = [lease for lease in leases if lease.get("token") != token]
                if remaining != leases or not self._state_path.exists():
                    _write_json(
                        self._state_path,
                        {
                            "version": _CONCURRENCY_SCHEMA_VERSION,
                            "limit": max(1, int(self.limit)),
                            "leases": remaining,
                        },
                    )
        except FileNotFoundError:
            return

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
