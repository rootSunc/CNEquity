"""Shared baostock session: login, per-symbol retry, periodic relogin.

Long sweeps drop sessions; retry each symbol with backoff and refresh every
``_RELOGIN_EVERY``. Failures are returned to the caller (no silent partial).
"""

from __future__ import annotations

import logging
import socket
import threading
import time
from collections.abc import Callable
from datetime import date

from cn_market_lake.domain.symbols import parse_symbol

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


def import_baostock():
    try:
        import baostock as bs  # noqa: PLC0415 — hard dep; lazy so import-time stays light
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "baostock is not installed; historical backfill requires it. "
            "Reinstall with `pip install --force-reinstall cn-market-lake` "
            "(or `pip install -e .` from a source checkout)."
        ) from exc
    return bs


def _login(bs, *, sleep=time.sleep) -> None:
    last_msg = "unknown"
    for attempt in range(_LOGIN_RETRIES):
        login = bs.login()
        if getattr(login, "error_code", "0") == "0":
            return
        last_msg = getattr(login, "error_msg", "unknown")
        if attempt + 1 < _LOGIN_RETRIES:
            sleep(_LOGIN_BACKOFF_SECONDS[min(attempt, len(_LOGIN_BACKOFF_SECONDS) - 1)])
    raise RuntimeError(f"baostock login failed: {last_msg}")


def _relogin(bs, *, sleep=time.sleep) -> None:
    try:
        bs.logout()
    except Exception:  # noqa: BLE001 - logout on a dead socket may raise; ignore
        pass
    _login(bs, sleep=sleep)


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
    """
    if bs is None:
        bs = import_baostock()

    prev_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(_SOCKET_TIMEOUT_SECONDS)
    _login(bs, sleep=sleep)
    _ensure_socket_timeout()
    rows: list[dict] = []
    failed: list[str] = []
    n_symbols = len(symbols)
    try:
        for i, symbol in enumerate(symbols):
            _batch_rest(config, i, sleep=sleep)
            _pace_before_symbol(config, sleep=sleep)
            if i and _RELOGIN_EVERY and i % _RELOGIN_EVERY == 0:
                try:
                    _relogin(bs, sleep=sleep)
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
                watchdog = threading.Timer(deadline, on_deadline)
                watchdog.start()
                try:
                    got = fetch_one(bs, symbol, start, end)
                except Exception as exc:  # noqa: BLE001 — stalled socket / broken pipe
                    # A socket timeout, dropped connection, or watchdog-closed
                    # socket raises here; treat it like a query error so the
                    # symbol is retried on a fresh login.
                    logger.warning("%s query error for %s: %s", label, symbol, exc)
                    got = None
                finally:
                    watchdog.cancel()
                if got is not None:
                    break
                sleep(_BACKOFF_SECONDS[min(attempt, len(_BACKOFF_SECONDS) - 1)])
                try:
                    _relogin(bs, sleep=sleep)
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
        try:
            bs.logout()
        except Exception:  # noqa: BLE001
            pass

    return rows, failed
