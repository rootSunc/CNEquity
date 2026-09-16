"""Shared baostock session: login, per-symbol retry, periodic relogin.

Long sweeps drop sessions; retry each symbol with backoff and refresh every
``_RELOGIN_EVERY``. Failures are returned to the caller (no silent partial).
"""

from __future__ import annotations

import logging
import signal
import socket
import threading
import time
from collections.abc import Callable
from contextlib import nullcontext
from datetime import date

from cnequity.domain.rate_limit import source_request
from cnequity.domain.symbols import parse_symbol
from cnequity.storage.raw_archive import RawArchiveError

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_BACKOFF_SECONDS = (1.0, 3.0, 8.0)
_RELOGIN_EVERY = 300
_LOGIN_RETRIES = 5
_LOGIN_BACKOFF_SECONDS = (2.0, 5.0, 10.0, 20.0, 30.0)
_DEFAULT_MIN_INTERVAL = 1.0
_DEFAULT_BATCH_SIZE = 20
# Official free-API limits: ≤50k requests/day, no concurrent connections;
# exceeding either blacklists the IP (error_code=10001011).
# Measured 2026-07: the free tier also blocks ("黑名单用户") after roughly 43
# queries in a session, with a ~40 minute cooldown — the binding constraint is
# cumulative volume, not request spacing. Batch 50 / rest 45s was what got
# blocked; batch 20 / rest 120s carried 1,658 symbols of valuation and 244 of
# delisted bars without a single block.
_DEFAULT_BATCH_REST = 120.0
# Watchdog: baostock can trickle bytes forever at ~0 CPU; kill past this.
_PER_SYMBOL_DEADLINE_SECONDS = 45.0
# Bound blocked reads (dropped conn can leave ESTABLISHED forever).
_SOCKET_TIMEOUT_SECONDS = 30.0
_LOGIN_DEADLINE_SECONDS = 30.0
_LOGOUT_DEADLINE_SECONDS = 10.0


def _request_context(config, source: str = "baostock"):
    """Use the shared request boundary for real Config instances.

    A few downstream callers intentionally pass tiny config doubles exposing
    only ``rate_limit``. The session driver's legacy per-symbol pacing already
    covers those doubles; adding compatibility calls for login/logout/query
    would change their observable call counts without providing a shared
    ledger. Production ``Config`` always exposes ``source_request``.
    """
    if config is None or getattr(config, "source_request", None) is None:
        return nullcontext()
    return source_request(config, source)


class _FetchDeadline(TimeoutError):
    """Raised in the fetching thread when one vendor query exceeds its budget."""


class _AlarmDeadline(BaseException):
    """Internal cancellation that the SDK's broad Exception handlers cannot eat."""


def _fetch_with_deadline(fetch, deadline: float, on_deadline):
    """Run one vendor query with a hard deadline.

    A background thread can close a socket, but closing a descriptor from
    another thread does not reliably interrupt ``recv`` on macOS. The main
    ingestion path therefore uses ``SIGALRM`` so the blocked Python socket
    call is interrupted in the same thread.

    ``SIGALRM``/``setitimer`` is POSIX-only (absent on Windows) and can only
    be armed from the main thread, so both callers off the main thread and
    every caller on Windows fall through to running ``fetch`` in a daemon
    worker thread instead. That worker cannot be killed if it never returns,
    but the caller does not wait on it past ``deadline`` either way: it is a
    bound on the *caller's* wait, not on the query itself. ``on_deadline``
    still gets a chance to close the underlying connection so the orphaned
    worker fails fast rather than lingering, but nothing here depends on that
    succeeding for the deadline to hold.
    """
    use_alarm = (
        threading.current_thread() is threading.main_thread()
        and hasattr(signal, "setitimer")
        and hasattr(signal, "ITIMER_REAL")
    )
    if use_alarm:
        previous_handler = signal.getsignal(signal.SIGALRM)

        def _alarm_handler(_signum, _frame):
            on_deadline()
            raise _AlarmDeadline(f"vendor query exceeded {deadline:.1f}s deadline")

        signal.signal(signal.SIGALRM, _alarm_handler)
        signal.setitimer(signal.ITIMER_REAL, deadline)
        try:
            return fetch()
        except _AlarmDeadline as exc:
            raise _FetchDeadline(str(exc)) from None
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous_handler)

    outcome: dict = {}
    done = threading.Event()

    def _run():
        try:
            outcome["value"] = fetch()
        except Exception as exc:  # noqa: BLE001 — re-raised in the caller below
            outcome["error"] = exc
        finally:
            done.set()

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    if not done.wait(deadline):
        on_deadline()
        raise _FetchDeadline(f"vendor query exceeded {deadline:.1f}s deadline")
    if "error" in outcome:
        raise outcome["error"]
    return outcome["value"]


def import_baostock():
    try:
        import baostock as bs  # noqa: PLC0415 — hard dep; lazy so import-time stays light
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "baostock is not installed; historical backfill requires it. "
            "Reinstall with `pip install --force-reinstall cnequity` "
            "(or `pip install -e .` from a source checkout)."
        ) from exc
    return bs


class _SessionDeadline(RuntimeError):
    """A session operation exceeded its total budget, even if recv kept moving."""


def _session_call(operation, *, config, label: str, deadline: float):
    expired = False

    def expire():
        nonlocal expired
        expired = True
        _force_close_baostock_socket()

    def invoke():
        # Keep the lease in the worker until the actual vendor call returns.
        with _request_context(config):
            return operation()

    try:
        result = _fetch_with_deadline(invoke, deadline, expire)
    except _FetchDeadline as exc:
        raise _SessionDeadline(f"baostock {label} exceeded {deadline:.1f}s deadline") from exc
    # The vendor catches Exception around recv, including the alarm exception.
    # Closing the socket gets it out of recv, but a swallowed deadline must
    # still fail the session instead of looking like a successful login.
    if expired:
        raise _SessionDeadline(f"baostock {label} exceeded {deadline:.1f}s deadline")
    return result


def _login(bs, *, sleep=time.sleep, config=None) -> None:
    last_msg = "unknown"
    for attempt in range(_LOGIN_RETRIES):
        login = _session_call(
            bs.login, config=config, label="login", deadline=_LOGIN_DEADLINE_SECONDS
        )
        if getattr(login, "error_code", None) == "0":
            return
        last_msg = getattr(login, "error_msg", "missing login response")
        if attempt + 1 < _LOGIN_RETRIES:
            sleep(_LOGIN_BACKOFF_SECONDS[min(attempt, len(_LOGIN_BACKOFF_SECONDS) - 1)])
    raise RuntimeError(f"baostock login failed: {last_msg}")


def _logout(bs, *, config=None) -> None:
    _session_call(bs.logout, config=config, label="logout", deadline=_LOGOUT_DEADLINE_SECONDS)


def _relogin(bs, *, sleep=time.sleep, config=None) -> None:
    try:
        _logout(bs, config=config)
    except _SessionDeadline:
        # Do not establish another connection while an orphaned logout may
        # still own the vendor's process-global socket.
        raise
    except Exception:  # noqa: BLE001 - logout on a dead socket may raise; ignore
        pass
    _login(bs, sleep=sleep, config=config)


def to_baostock_symbol(symbol: str) -> str:
    """``600519.SH`` -> ``sh.600519`` (baostock's market-prefixed form)."""
    info = parse_symbol(symbol)
    prefix = {"SH": "sh", "SZ": "sz", "BJ": "bj"}.get(info.exchange, info.exchange.lower())
    return f"{prefix}.{info.code}"


def _ensure_socket_timeout(timeout: float = _SOCKET_TIMEOUT_SECONDS) -> None:
    """Pin timeout on baostock's live socket (setdefaulttimeout is not enough).

    ``SocketUtil.connect`` creates the socket without ``settimeout``; on some
    platforms the module-global default is ignored once the fd is in a blocking
    ``recv`` loop waiting for baostock's ``<![CDATA[]]>`` terminator.
    """
    try:
        import baostock.common.context as bctx  # noqa: PLC0415 — optional dep, lazy

        sock = getattr(bctx, "default_socket", None)
        if sock is not None:
            sock.settimeout(timeout)
    except Exception:  # noqa: BLE001 — best-effort; never break the sweep
        pass


def _force_close_baostock_socket() -> None:
    """Interrupt baostock's live socket so a blocked/slowloris read raises at once.

    baostock keeps the connection as a module global. ``shutdown(SHUT_RDWR)`` —
    not ``close()`` — is what reliably wakes a ``recv`` blocked in another thread:
    ``close()`` only drops this thread's reference and the peer's read can keep
    hanging. Shut down then close; the next retry relogins onto a fresh socket.
    """
    try:
        import baostock.common.context as bctx  # noqa: PLC0415 — optional dep, lazy

        sock = getattr(bctx, "default_socket", None)
        if sock is None:
            return
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass  # already torn down / not connected
        sock.close()
    except Exception:  # noqa: BLE001 — best-effort interrupt; never raise from the timer
        pass


def _pace_before_symbol(config, *, sleep=time.sleep) -> None:
    """Cross-process min_interval when Config is set; else local default sleep."""
    if config is not None:
        # ``source_request`` reserves both the QPS start and the in-flight
        # lease at each actual query. Keep the legacy call for lightweight
        # doubles, but do not reserve a second pacing slot for production.
        if getattr(config, "source_request", None) is not None:
            return
        config.rate_limit("baostock")
        return
    sleep(_DEFAULT_MIN_INTERVAL)


def _batch_rest(config, index: int, *, sleep=time.sleep) -> None:
    """Extra pause every N symbols so free APIs cool down between bursts."""
    batch = (
        int(getattr(config, "baostock_batch_size", _DEFAULT_BATCH_SIZE))
        if config is not None
        else _DEFAULT_BATCH_SIZE
    )
    rest = (
        float(getattr(config, "baostock_batch_rest_seconds", _DEFAULT_BATCH_REST))
        if config is not None
        else _DEFAULT_BATCH_REST
    )
    if batch <= 0 or rest <= 0:
        return
    # Rest after completing a batch (index is 0-based; rest when about to start next).
    if index > 0 and index % batch == 0:
        logger.info("baostock batch rest %.0fs after %d symbols", rest, index)
        sleep(rest)


def fetch_per_symbol(
    symbols: list[str],
    start: date,
    end: date,
    fetch_one: Callable[[object, str, date, date], list[dict] | None],
    *,
    bs=None,
    sleep=time.sleep,
    label: str = "baostock",
    deadline: float = _PER_SYMBOL_DEADLINE_SECONDS,
    on_deadline: Callable[[], None] = _force_close_baostock_socket,
    config=None,
    rest_after_batch: bool = False,
    request_managed: bool = False,
) -> tuple[list[dict], list[str]]:
    """Drive ``fetch_one(bs, symbol, start, end)`` over ``symbols`` with retry/relogin.

    ``fetch_one`` returns a list of row dicts, or ``None`` on a retryable query
    error (an ``error_code == '0'`` result with zero rows is a legitimate empty
    and must be returned as ``[]``). Returns ``(rows, failed_symbols)``. Fail-loud
    on login failure. ``bs`` / ``sleep`` are injectable for offline tests.

    Pacing (anti-blacklist for free baostock):
    - ``config.rate_limit("baostock")`` before each symbol (cross-process), or
      ``_DEFAULT_MIN_INTERVAL`` when ``config`` is None;
    - batch rest every ``baostock_batch_size`` symbols.

    ``request_managed`` is used by the built-in adapters whose callback wraps
    each individual Baostock query itself. For an injected callback (the
    default), the driver places a source lease inside the deadline worker so a
    timed-out/orphaned query cannot release its slot before the vendor call
    actually returns.
    """
    if bs is None:
        bs = import_baostock()

    prev_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(_SOCKET_TIMEOUT_SECONDS)
    logged_in = False
    rows: list[dict] = []
    failed: list[str] = []
    n_symbols = len(symbols)
    try:
        _login(bs, sleep=sleep, config=config)
        logged_in = True
        _ensure_socket_timeout()
        for i, symbol in enumerate(symbols):
            _batch_rest(config, i, sleep=sleep)
            _pace_before_symbol(config, sleep=sleep)
            if i and _RELOGIN_EVERY and i % _RELOGIN_EVERY == 0:
                try:
                    _relogin(bs, sleep=sleep, config=config)
                    _ensure_socket_timeout()
                except RuntimeError as exc:
                    # Keep rows already collected so the caller can checkpoint;
                    # remaining symbols stay on the resume set.
                    logger.error(
                        "%s mid-sweep login failed at %d/%d: %s; returning partial",
                        label,
                        i + 1,
                        n_symbols,
                        exc,
                    )
                    failed.extend(symbols[i:])
                    break
            # Heartbeat every 10 symbols so multi-hour sweeps look alive on stdout.
            if i == 0 or (i + 1) % 10 == 0 or i + 1 == n_symbols:
                logger.info(
                    "%s progress %d/%d symbol=%s ok_rows=%d failed=%d",
                    label,
                    i + 1,
                    n_symbols,
                    symbol,
                    len(rows),
                    len(failed),
                )
            got: list[dict] | None = None
            abort_remaining = False
            for attempt in range(_MAX_RETRIES):
                try:

                    def invoke(symbol: str = symbol):
                        if request_managed or config is None:
                            return fetch_one(bs, symbol, start, end)
                        with _request_context(config):
                            return fetch_one(bs, symbol, start, end)

                    got = _fetch_with_deadline(
                        invoke,
                        deadline,
                        on_deadline,
                    )
                except Exception as exc:  # noqa: BLE001 — stalled socket / broken pipe
                    if isinstance(exc, RawArchiveError):
                        # A configured critical archive is a publish contract,
                        # not a transient vendor query failure. Retrying and
                        # converting it into a "failed symbol" would let a
                        # captureless repair look like a legitimate empty.
                        raise
                    # A socket timeout, dropped connection, or watchdog-closed
                    # socket raises here; treat it like a query error so the
                    # symbol is retried on a fresh login.
                    logger.warning("%s query error for %s: %s", label, symbol, exc)
                    got = None
                if got is not None:
                    break
                sleep(_BACKOFF_SECONDS[min(attempt, len(_BACKOFF_SECONDS) - 1)])
                try:
                    _relogin(bs, sleep=sleep, config=config)
                    _ensure_socket_timeout()
                except RuntimeError as exc:
                    logger.error(
                        "%s login failed while retrying %s: %s; returning partial",
                        label,
                        symbol,
                        exc,
                    )
                    got = None
                    abort_remaining = True
                    break
            if got is None:
                logger.warning("%s failed for %s after retries", label, symbol)
                failed.append(symbol)
                if abort_remaining:
                    failed.extend(symbols[i + 1 :])
                    break
            else:
                rows.extend(got)
    finally:
        socket.setdefaulttimeout(prev_timeout)
        if logged_in:
            try:
                _logout(bs, config=config)
            except Exception as exc:  # noqa: BLE001 — retain completed query results
                logger.warning("baostock logout failed: %s", exc)

    # Callers that split a long sweep into checkpoint-sized batches need the
    # same cooldown that the in-process loop applies before symbol N+1. Keep
    # the default off so short one-shot adapter calls do not sleep after their
    # final request.
    if rest_after_batch and config is not None and n_symbols:
        batch = int(getattr(config, "baostock_batch_size", _DEFAULT_BATCH_SIZE))
        if batch > 0 and n_symbols % batch == 0:
            _batch_rest(config, n_symbols, sleep=sleep)

    return rows, failed
