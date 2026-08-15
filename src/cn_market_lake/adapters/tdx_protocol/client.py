from __future__ import annotations

import logging
import time
from collections.abc import Callable
from concurrent.futures import as_completed as _as_completed
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from cn_market_lake.adapters.calendar.exchange_calendar import (
    build_trading_calendar,
    ensure_seed_csv,
)
from cn_market_lake.adapters.eastmoney.corporate_actions import fetch_corporate_actions_eastmoney
from cn_market_lake.adapters.eastmoney.trading_status import fetch_trading_status_eastmoney
from cn_market_lake.adapters.tdx_protocol.bars import fetch_bars_paginated
from cn_market_lake.adapters.tdx_protocol.corporate_actions import fetch_corporate_actions_tdx
from cn_market_lake.adapters.tdx_protocol.session import TDX_SESSION_LOCK, close_quotes_client
from cn_market_lake.config import Config
from cn_market_lake.domain.rate_limit import RateLimitSpec, wait_spec
from cn_market_lake.domain.schemas import MOCK_SOURCE, data_version_for, with_provenance
from cn_market_lake.domain.symbols import (
    ETF_PREFIXES,
    PREFIX_WHITELIST,
    format_symbol,
    is_cdr_symbol,
    is_etf_symbol,
)

logger = logging.getLogger(__name__)

_close_quotes_client = close_quotes_client

INDEX_SYMBOLS = [
    ("000001", "SH"),
    ("399001", "SZ"),
    ("399006", "SZ"),
    ("000688", "SH"),
    ("000016", "SH"),
    ("000300", "SH"),
    ("000905", "SH"),
    ("000852", "SH"),
]


class TdxSourceError(RuntimeError):
    """Raised when the TDX source cannot deliver real data.

    Fabricated data is only allowed behind an explicit `allow_mock=True`
    (config `[tdx_protocol].allow_mock`), which skips the network entirely —
    an upstream bestip scan can block indefinitely offline — and returns rows
    labeled `source="mock"` so audit can reject them.
    """


# A validated (host, port) reused across fetches in this process. An upstream
# bestip scan is slow (~75s) and intermittently selects a server that then
# fails the actual fetch. Worse, some bundled hosts are TCP-reachable but
# return zero rows for every symbol (dead data feed), so we validate a
# candidate by actually fetching a known bar before trusting it.
_TDX_SERVER_CACHE: tuple[str, int] | None = None
_TDX_TCP_TIMEOUT = 1.5
_TDX_PROBE_SYMBOL = "000001"  # SSE composite; market=1
_TDX_MAX_CANDIDATES = 16
_TDX_PROBE_CONCURRENCY = 8  # parallel probes; first live responder wins
_TDX_FETCH_ATTEMPTS = 3  # server rotations before a bar fetch fails loud


def reset_tdx_server_cache() -> None:
    """Forget the cached TDX server so the next client re-probes (on failure)."""
    global _TDX_SERVER_CACHE
    _TDX_SERVER_CACHE = None


def _reachable(host: str, port: int, timeout: float = _TDX_TCP_TIMEOUT) -> bool:
    import socket

    sock = socket.socket()
    sock.settimeout(timeout)
    try:
        sock.connect((host, int(port)))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def _serves_data(host: str, port: int, timeout: int) -> bool:
    """A server passes only if it returns a real bar — filters dead feeds.

    Uses ``heartbeat=False`` so the throwaway probe leaves no lingering thread.
    """
    from cn_market_lake.adapters.tdx_protocol.quotes import Quotes

    client = None
    try:
        client = Quotes.factory(server=(host, int(port)), timeout=timeout, heartbeat=False)
        rows = client.bars(_TDX_PROBE_SYMBOL, market=1, start=0, offset=1)
        return bool(rows)
    except Exception:
        return False
    finally:
        if client is not None:
            client.close()


def _candidate_servers(config: Config | None) -> list[tuple[str, int]]:
    """Configured host pool first (in order), then the bundled fallback hosts."""
    import random

    from cn_market_lake.adapters.tdx_protocol.hosts import HQ_HOSTS

    ordered: list[tuple[str, int]] = []
    if config is not None and config.tdx_host_pool:
        for entry in config.tdx_host_pool:
            host, _, port = entry.rpartition(":")
            if host and port.isdigit():
                ordered.append((host, int(port)))

    bundled = [(host, int(port)) for host, port in HQ_HOSTS]
    random.shuffle(bundled)  # spread load across the fallback list
    ordered.extend(bundled)

    seen: set[tuple[str, int]] = set()
    out: list[tuple[str, int]] = []
    for hp in ordered:
        if hp not in seen:
            seen.add(hp)
            out.append(hp)
    return out


def _probe(host: str, port: int, timeout: int) -> bool:
    return _reachable(host, port) and _serves_data(host, port, timeout)


def _pick_reachable_server(config: Config | None = None, timeout: int = 10) -> tuple[str, int]:
    """Probe candidates in parallel; return the first that serves real data.

    Parallel probing means the first future to resolve true is effectively the
    lowest-latency live server, so selection is both fast and fastest-first.
    """
    from concurrent.futures import ThreadPoolExecutor

    candidates = _candidate_servers(config)[:_TDX_MAX_CANDIDATES]
    if not candidates:
        raise TdxSourceError("no TDX candidate servers configured or bundled")

    with ThreadPoolExecutor(max_workers=min(len(candidates), _TDX_PROBE_CONCURRENCY)) as pool:
        futures = {pool.submit(_probe, h, p, timeout): (h, p) for h, p in candidates}
        try:
            for fut in _as_completed(futures):
                if fut.result():
                    return futures[fut]
        finally:
            for fut in futures:
                fut.cancel()
    raise TdxSourceError(
        f"no TDX server responded with data (probed {len(candidates)} host(s); "
        "network down or all feeds degraded)"
    )


def _quotes_client(config: Config | None = None):
    """Build a TDX client bound to a reachable, cached server.

    Isolated so tests can monkeypatch it.
    """
    global _TDX_SERVER_CACHE
    from cn_market_lake.adapters.tdx_protocol.quotes import Quotes

    timeout = config.tdx_connect_timeout_sec if config else 10
    servers = (config.tdx_servers if config else "auto").strip()
    kwargs: dict[str, object] = {
        "multithread": True,
        "heartbeat": True,
        "timeout": timeout,
    }
    if servers.lower() == "auto":
        if _TDX_SERVER_CACHE is None:
            _TDX_SERVER_CACHE = _pick_reachable_server(config, timeout=timeout)
        kwargs["server"] = _TDX_SERVER_CACHE
    else:
        host, sep, port = servers.partition(":")
        if not sep:
            raise TdxSourceError(
                f"invalid [tdx_protocol].servers {servers!r}; use 'auto' or host:port"
            )
        kwargs["server"] = (host.strip(), int(port.strip()))
    return Quotes.factory(**kwargs)


def quotes_client_factory(config: Config | None = None):
    """Callable factory for corporate_actions xdxr (one client per batch)."""
    return lambda: _quotes_client(config)


# A cold TCP handshake to a TDX host occasionally times out under sustained
# load rather than failing outright — measured on a full-market intraday
# seed (7,747 symbols, 50-per-batch), which reconnects on every batch and hit
# a `socket.recv` timeout during setup after ~44 minutes and ~600 prior
# reconnects. One retry, against a re-probed server, clears this without
# escalating all the way to a failed batch.
_CONNECT_RETRY_ATTEMPTS = 2
_CONNECT_RETRY_BACKOFF_SEC = 2.0


def _connect_with_retry(config: Config | None = None):
    """``_quotes_client``, retrying once (with a fresh server) on failure."""
    last_exc: Exception | None = None
    for attempt in range(_CONNECT_RETRY_ATTEMPTS):
        try:
            return _quotes_client(config)
        except Exception as exc:  # noqa: BLE001 — retried, then re-raised as-is
            last_exc = exc
            if attempt + 1 < _CONNECT_RETRY_ATTEMPTS:
                # The cached server just failed us; re-probe rather than
                # retrying the same one straight into the same timeout.
                reset_tdx_server_cache()
                time.sleep(_CONNECT_RETRY_BACKOFF_SEC)
    assert last_exc is not None
    raise last_exc


# TDX market ids: 0=Shenzhen, 1=Shanghai (not "SH"/"SZ" strings).
_TDX_STOCK_MARKETS = ((1, "SH"), (0, "SZ"))


def _asset_type_for(code: str, exch: str) -> str:
    if is_cdr_symbol(code, exch):
        return "cdr"
    if is_etf_symbol(code, exch):
        return "etf"
    return "stock"


def _filter_instrument_frame(pdf: pl.DataFrame, exch: str) -> pl.DataFrame:
    from cn_market_lake.domain.symbols import is_subscription_placeholder

    code_col = "code" if "code" in pdf.columns else pdf.columns[0]
    name_col = "name" if "name" in pdf.columns else pdf.columns[1]
    codes = pdf[code_col].cast(pl.Utf8).str.zfill(6)
    prefixes = PREFIX_WHITELIST.get(exch.upper(), ()) + ETF_PREFIXES.get(exch.upper(), ())
    mask = pl.lit(False)
    for prefix in prefixes:
        mask = mask | codes.str.starts_with(prefix)
    for blocked in range(81, 90):
        mask = mask & ~codes.str.starts_with(str(blocked))
    filtered = pdf.filter(mask)
    if filtered.is_empty():
        return pl.DataFrame(
            schema={
                "symbol": pl.Utf8,
                "name": pl.Utf8,
                "exchange": pl.Utf8,
                "asset_type": pl.Utf8,
                "list_date": pl.Date,
                "delist_date": pl.Date,
                "prev_symbol": pl.Utf8,
            }
        )
    rows = []
    for row in filtered.iter_rows(named=True):
        code = str(row[code_col]).zfill(6)
        name = str(row[name_col])
        if is_subscription_placeholder(name):
            continue
        rows.append(
            {
                "symbol": format_symbol(code, exch),
                "name": name,
                "exchange": exch,
                "asset_type": _asset_type_for(code, exch),
                "list_date": None,
                "delist_date": None,
                "prev_symbol": None,
            }
        )
    if not rows:
        return pl.DataFrame(
            schema={
                "symbol": pl.Utf8,
                "name": pl.Utf8,
                "exchange": pl.Utf8,
                "asset_type": pl.Utf8,
                "list_date": pl.Date,
                "delist_date": pl.Date,
                "prev_symbol": pl.Utf8,
            }
        )
    return pl.DataFrame(rows)


def _mark_mock(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(pl.lit(MOCK_SOURCE).alias("source"))


def _mock_instruments() -> pl.DataFrame:
    rows = []
    for code, exch in [("600519", "SH"), ("000001", "SZ"), ("300750", "SZ"), ("920000", "BJ")]:
        rows.append(
            {
                "symbol": format_symbol(code, exch),
                "name": f"Mock-{code}",
                "exchange": exch,
                "asset_type": "stock",
                "list_date": date(2010, 1, 1),
                "delist_date": None,
                "prev_symbol": None,
            }
        )
    return _mark_mock(pl.DataFrame(rows))


def _mock_calendar(start: date, end: date) -> pl.DataFrame:
    rows = []
    d = start
    while d <= end:
        is_trading = d.weekday() < 5
        rows.append({"trade_date": d, "is_trading": is_trading})
        d += timedelta(days=1)
    return _mark_mock(pl.DataFrame(rows))


def _mock_bars(symbols: list[str], start: date, end: date) -> pl.DataFrame:
    rows = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            for i, sym in enumerate(symbols):
                base = 10.0 + i
                rows.append(
                    {
                        "symbol": sym,
                        "trade_date": d,
                        "open": base,
                        "high": base + 1,
                        "low": base - 0.5,
                        "close": base + 0.2,
                        "volume": 1_000_000,
                        "amount": base * 1_000_000,
                    }
                )
        d += timedelta(days=1)
    return _mark_mock(pl.DataFrame(rows))


def _fail_or_mock(
    dataset: str, reason: str, allow_mock: bool, mock_df: pl.DataFrame
) -> pl.DataFrame:
    if not allow_mock:
        raise TdxSourceError(f"{dataset}: {reason} (set [tdx_protocol].allow_mock for tests)")
    logger.warning("%s: %s; returning mock rows labeled source=%s", dataset, reason, MOCK_SOURCE)
    return mock_df


_MOCK_SHORT_CIRCUIT = "allow_mock enabled; skipping network fetch"


def fetch_instruments(
    *,
    rate_limit: RateLimitSpec | None = None,
    allow_mock: bool = False,
    config: Config | None = None,
) -> pl.DataFrame:
    if allow_mock:
        return _fail_or_mock("instruments", _MOCK_SHORT_CIRCUIT, True, _mock_instruments())
    wait_spec(rate_limit)
    client = None
    try:
        with TDX_SESSION_LOCK:
            client = _quotes_client(config)
            frames = []
            market_errors: list[str] = []
            for market, exch in _TDX_STOCK_MARKETS:
                try:
                    raw = client.stocks(market=market)
                except Exception as exc:
                    market_errors.append(f"{exch}: {exc}")
                    continue
                if raw is None or len(raw) == 0:
                    market_errors.append(f"{exch}: empty response")
                    continue
                pdf = pl.from_pandas(raw) if hasattr(raw, "columns") else pl.DataFrame(raw)
                part = _filter_instrument_frame(pdf, exch)
                if part.height:
                    frames.append(part)
                else:
                    market_errors.append(f"{exch}: no qualifying instruments")
            if market_errors:
                reason = "market fetch failed: " + "; ".join(market_errors)
                return _fail_or_mock("instruments", reason, allow_mock, _mock_instruments())
            if not frames:
                reason = "TDX returned no instruments"
                return _fail_or_mock("instruments", reason, allow_mock, _mock_instruments())
            return pl.concat(frames, how="diagonal_relaxed")
    except ImportError:
        reason = "TDX wire client unavailable"
    except Exception as exc:
        # Drop the cached server so the next attempt (batch retry) re-probes
        # for a live one instead of hammering the same dead host.
        reset_tdx_server_cache()
        reason = f"TDX fetch failed: {exc}"
    finally:
        _close_quotes_client(client)
    return _fail_or_mock("instruments", reason, allow_mock, _mock_instruments())


def fetch_trading_calendar(
    start: date,
    end: date,
    *,
    rate_limit: RateLimitSpec | None = None,
    allow_mock: bool = False,
    curated_root: Path | None = None,
    seed_path: Path | None = None,
) -> pl.DataFrame:
    wait_spec(rate_limit)
    try:
        ensure_seed_csv(seed_path)
        return build_trading_calendar(
            start,
            end,
            seed_path=seed_path,
            curated_root=curated_root,
        )
    except Exception as exc:
        reason = f"calendar seed load failed: {exc}"
        return _fail_or_mock("trading_calendar", reason, allow_mock, _mock_calendar(start, end))


def fetch_daily_bars(
    symbols: list[str],
    start: date,
    end: date,
    *,
    rate_limit: RateLimitSpec | None = None,
    allow_mock: bool = False,
    backfill: bool = False,
    config: Config | None = None,
    on_heartbeat: Callable[[], None] | None = None,
) -> pl.DataFrame:
    if allow_mock:
        return _fail_or_mock(
            "daily_bars", _MOCK_SHORT_CIRCUIT, True, _mock_bars(symbols, start, end)
        )
    client = None
    try:
        with TDX_SESSION_LOCK:
            client = _quotes_client(config)
            rows = []
            for sym in symbols:
                if on_heartbeat is not None:
                    on_heartbeat()
                rows.extend(
                    fetch_bars_paginated(
                        client,
                        sym,
                        start,
                        end,
                        rate_limit=rate_limit,
                        backfill=backfill,
                        on_page=on_heartbeat,
                    )
                )
            if rows:
                return pl.DataFrame(rows)
            reason = "TDX returned no bars"
    except ImportError:
        reason = "TDX wire client unavailable"
    except Exception as exc:
        # Drop the cached server so the next attempt (batch retry) re-probes
        # for a live one instead of hammering the same dead host.
        reset_tdx_server_cache()
        reason = f"TDX fetch failed: {exc}"
    finally:
        _close_quotes_client(client)
    return _fail_or_mock("daily_bars", reason, allow_mock, _mock_bars(symbols, start, end))


def fetch_minute_bars(
    symbols: list[str],
    start: date,
    end: date,
    *,
    frequency: str = "1m",
    rate_limit: RateLimitSpec | None = None,
    backfill: bool = False,
    config: Config | None = None,
    on_heartbeat: Callable[[], None] | None = None,
    max_pages: int | None = None,
    workers: int = 1,
) -> tuple[pl.DataFrame, list[str]]:
    """Intraday bars for *symbols*, one TDX session for the whole batch.

    Returns ``(frame, failed_symbols)``. A symbol that fails does not fail the
    batch — with hundreds of symbols and a per-symbol horizon walk, one bad code
    must not cost the run every other one — so the failures ride alongside the
    frame rather than inside it.

    ``workers`` > 1 splits the symbols across that many threads, each with its
    own TDX connection. It does **not** raise the request rate: the limiter in
    ``wait_spec`` is cross-process and still paces every request, so the ceiling
    is unchanged and the threads only stop us idling on network latency between
    calls. Measured on 48 symbols: 2.35 req/s serial against 8.70 at 4 threads,
    approaching the 10 req/s the 100ms limiter already permits.

    Threads rather than the daily path's ProcessPool: the wire client is not
    fork-safe (which is why macOS is pinned to ``workers = 1`` there), but one
    client per thread is fine, so this works on every platform.

    No mock path. The daily fetch has one because the daily lake must keep
    building in tests and demos when TDX is unreachable; ``minute_bars`` is
    opt-in and empty by default, so a fabricated intraday series would buy
    nothing and could be mistaken for a real 240-bar session.
    """
    from cn_market_lake.adapters.tdx_protocol.minute_bars import fetch_minute_bars_paginated

    def _fetch_one(client, sym: str) -> list[dict]:
        return fetch_minute_bars_paginated(
            client,
            sym,
            start,
            end,
            frequency=frequency,
            rate_limit=rate_limit,
            backfill=backfill,
            on_page=on_heartbeat,
            max_pages=max_pages,
        )

    rows: list[dict] = []
    failed: list[str] = []
    clients: list = []
    lanes = max(1, min(int(workers), len(symbols))) if symbols else 1
    try:
        # Held for the whole batch, as every TDX caller does: it keeps other
        # steps out of the protocol while we run. Inside it we own TDX, so the
        # extra connections below are ours alone and each is touched by exactly
        # one thread — which is the property the lock's comment is about.
        with TDX_SESSION_LOCK:
            if lanes == 1:
                clients = [_connect_with_retry(config)]
                for sym in symbols:
                    if on_heartbeat is not None:
                        on_heartbeat()
                    try:
                        rows.extend(_fetch_one(clients[0], sym))
                    except Exception as exc:  # noqa: BLE001 — recorded, sweep continues
                        logger.warning("%s failed for %s: %s", frequency, sym, exc)
                        failed.append(sym)
            else:
                rows, failed = _fetch_threaded(
                    symbols, lanes, config, _fetch_one, clients, on_heartbeat, frequency
                )
    except ImportError as exc:
        raise TdxSourceError("minute_bars: TDX wire client unavailable") from exc
    except Exception as exc:
        reset_tdx_server_cache()
        raise TdxSourceError(f"minute_bars: TDX fetch failed: {exc}") from exc
    finally:
        for client in clients:
            _close_quotes_client(client)

    df = pl.DataFrame(rows) if rows else pl.DataFrame(schema=_MINUTE_BARS_FETCH_SCHEMA)
    return df, failed


def _fetch_threaded(
    symbols: list[str],
    lanes: int,
    config: Config | None,
    fetch_one: Callable[[object, str], list[dict]],
    clients: list,
    on_heartbeat: Callable[[], None] | None,
    frequency: str,
) -> tuple[list[dict], list[str]]:
    """Run *fetch_one* over *symbols* on *lanes* threads, one client each.

    Symbols are dealt round-robin rather than in contiguous blocks so a lane
    cannot draw a run of illiquid names and finish minutes after the others.
    """
    import threading
    from concurrent.futures import ThreadPoolExecutor

    for _ in range(lanes):
        clients.append(_connect_with_retry(config))

    heartbeat_lock = threading.Lock()

    def _beat() -> None:
        if on_heartbeat is None:
            return
        # The manifest heartbeat writes to SQLite; serialise the callback so
        # concurrent lanes cannot interleave inside it.
        with heartbeat_lock:
            on_heartbeat()

    def _lane(index: int) -> tuple[list[dict], list[str]]:
        client = clients[index]
        lane_rows: list[dict] = []
        lane_failed: list[str] = []
        for sym in symbols[index::lanes]:
            _beat()
            try:
                lane_rows.extend(fetch_one(client, sym))
            except Exception as exc:  # noqa: BLE001 — recorded, sweep continues
                logger.warning("%s failed for %s: %s", frequency, sym, exc)
                lane_failed.append(sym)
        return lane_rows, lane_failed

    rows: list[dict] = []
    failed: list[str] = []
    with ThreadPoolExecutor(max_workers=lanes) as pool:
        for lane_rows, lane_failed in pool.map(_lane, range(lanes)):
            rows.extend(lane_rows)
            failed.extend(lane_failed)
    return rows, failed


def fetch_trade_ticks_batch(
    symbols: list[str],
    sessions: list[date],
    *,
    rate_limit: RateLimitSpec | None = None,
    config: Config | None = None,
    on_heartbeat: Callable[[], None] | None = None,
    workers: int = 1,
) -> tuple[pl.DataFrame, list[str]]:
    """Transaction records for every (symbol, session) pair, one TDX session.

    Returns ``(frame, failed)`` where a failure is a ``"symbol@date"`` label —
    the unit of failure is a symbol-*day*, not a symbol, because a session is
    fetched whole or not at all and one bad day must not discard the rest of a
    symbol's history.

    Threads rather than the daily path's ProcessPool, for the same reason as
    the minute bars: the wire client is not fork-safe, but one client per
    thread is fine. Symbols are dealt round-robin so a lane cannot draw a run
    of illiquid names and finish early while another is still walking Moutai.

    No mock path. The dataset is opt-in and empty by default, so fabricated
    ticks would buy nothing and could be mistaken for a real session.
    """
    from cn_market_lake.adapters.tdx_protocol.trade_ticks import fetch_trade_ticks

    def _lane(client, lane_symbols: list[str], beat) -> tuple[list[dict], list[str]]:
        lane_rows: list[dict] = []
        lane_failed: list[str] = []
        for sym in lane_symbols:
            for session in sessions:
                beat()
                try:
                    lane_rows.extend(fetch_trade_ticks(client, sym, session, rate_limit=rate_limit))
                except Exception as exc:  # noqa: BLE001 — recorded, sweep continues
                    logger.warning("trade_ticks failed for %s on %s: %s", sym, session, exc)
                    lane_failed.append(f"{sym}@{session}")
        return lane_rows, lane_failed

    rows: list[dict] = []
    failed: list[str] = []
    clients: list = []
    lanes = max(1, min(int(workers), len(symbols))) if symbols else 1
    try:
        with TDX_SESSION_LOCK:
            if lanes == 1:
                clients = [_connect_with_retry(config)]
                rows, failed = _lane(clients[0], symbols, on_heartbeat or (lambda: None))
            else:
                import threading
                from concurrent.futures import ThreadPoolExecutor

                for _ in range(lanes):
                    clients.append(_connect_with_retry(config))
                # The manifest heartbeat writes to SQLite; serialise it so
                # concurrent lanes cannot interleave inside it.
                heartbeat_lock = threading.Lock()

                def _beat() -> None:
                    if on_heartbeat is None:
                        return
                    with heartbeat_lock:
                        on_heartbeat()

                with ThreadPoolExecutor(max_workers=lanes) as pool:
                    results = pool.map(
                        lambda i: _lane(clients[i], symbols[i::lanes], _beat), range(lanes)
                    )
                    for lane_rows, lane_failed in results:
                        rows.extend(lane_rows)
                        failed.extend(lane_failed)
    except ImportError as exc:
        raise TdxSourceError("trade_ticks: TDX wire client unavailable") from exc
    except Exception as exc:
        reset_tdx_server_cache()
        raise TdxSourceError(f"trade_ticks: TDX fetch failed: {exc}") from exc
    finally:
        for client in clients:
            _close_quotes_client(client)

    df = pl.DataFrame(rows) if rows else pl.DataFrame(schema=_TRADE_TICKS_FETCH_SCHEMA)
    return df, failed


# Fetch-side shape (pre-provenance), so an all-failed batch still returns a
# frame the writer can validate instead of a schema-less empty one.
_TRADE_TICKS_FETCH_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "tick_seq": pl.Int32,
    "trade_time": pl.Datetime(time_unit="us"),
    "price": pl.Float64,
    "volume": pl.Int64,
    "direction": pl.Utf8,
}

_MINUTE_BARS_FETCH_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "bar_time": pl.Datetime(time_unit="us"),
    "frequency": pl.Utf8,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
    "amount": pl.Float64,
}


def fetch_index_bars(
    start: date,
    end: date,
    *,
    rate_limit: RateLimitSpec | None = None,
    allow_mock: bool = False,
    backfill: bool = False,
    config: Config | None = None,
) -> pl.DataFrame:
    symbols = [format_symbol(c, e) for c, e in INDEX_SYMBOLS]
    if allow_mock:
        return _fail_or_mock(
            "index_bars",
            _MOCK_SHORT_CIRCUIT,
            True,
            _mock_bars(symbols, start, end).with_columns(pl.lit("1d").alias("frequency")),
        )

    def _fetch_once() -> tuple[list[dict], list[str]]:
        with TDX_SESSION_LOCK:
            client = _quotes_client(config)
            rows: list[dict] = []
            missing: list[str] = []
            try:
                for sym in symbols:
                    try:
                        sym_rows = fetch_bars_paginated(
                            client,
                            sym,
                            start,
                            end,
                            rate_limit=rate_limit,
                            backfill=backfill,
                            is_index=True,
                        )
                    except Exception as exc:
                        if backfill:
                            # Rotate server and retry the whole set — some TDX hosts
                            # return corrupt bytes for deep index history.
                            raise TdxSourceError(f"index bars failed for {sym}: {exc}") from exc
                        # Daily mode: treat hard failures as missing so a partial
                        # set cannot advance the watermark (lake previously kept
                        # only 000852.SH on some days while other indices failed).
                        logger.warning("TDX index bars failed for %s: %s", sym, exc)
                        missing.append(sym)
                        continue
                    if not sym_rows:
                        missing.append(sym)
                        continue
                    rows.extend(sym_rows)
            finally:
                _close_quotes_client(client)
            return rows, missing

    reason = "TDX returned no index bars"
    try:
        last_exc: Exception | None = None
        for attempt in range(_TDX_FETCH_ATTEMPTS):
            try:
                rows, missing = _fetch_once()
                # Fail-loud on any incomplete symbol set — both backfill and daily.
                # Accepting a non-empty subset used to leave curated partitions with
                # only one index while audit reported calendar coverage gaps.
                if missing:
                    raise TdxSourceError("index bars returned no rows for: " + ", ".join(missing))
                if rows:
                    return pl.DataFrame(rows).with_columns(pl.lit("1d").alias("frequency"))
                break
            except TdxSourceError as exc:
                last_exc = exc
                reset_tdx_server_cache()
                logger.warning(
                    "index bars attempt %d/%d failed: %s; rotating server",
                    attempt + 1,
                    _TDX_FETCH_ATTEMPTS,
                    exc,
                )
        if last_exc is not None:
            reason = f"TDX fetch failed: {last_exc}"
    except ImportError:
        reason = "TDX wire client unavailable"
    except Exception as exc:
        reset_tdx_server_cache()
        reason = f"TDX fetch failed: {exc}"
    return _fail_or_mock(
        "index_bars",
        reason,
        allow_mock,
        _mock_bars(symbols, start, end).with_columns(pl.lit("1d").alias("frequency")),
    )


def fetch_corporate_actions(
    trade_date: date,
    *,
    symbols: list[str] | None = None,
    backfill: bool = False,
    rate_limit: RateLimitSpec | None = None,
    allow_mock: bool = False,
    primary_only: bool = False,
    config: Config | None = None,
) -> pl.DataFrame:
    empty = pl.DataFrame(
        schema={
            "symbol": pl.Utf8,
            "ex_date": pl.Date,
            "action_type": pl.Utf8,
            "cash_dividend": pl.Float64,
            "bonus_ratio": pl.Float64,
            "transfer_ratio": pl.Float64,
            "allotment_ratio": pl.Float64,
            "allotment_price": pl.Float64,
        }
    )
    if allow_mock:
        return _fail_or_mock("corporate_actions", _MOCK_SHORT_CIRCUIT, True, empty)
    wait_spec(rate_limit)

    frames: list[pl.DataFrame] = []
    try:
        if symbols:
            tdx_df = fetch_corporate_actions_tdx(
                symbols,
                trade_date=trade_date,
                backfill=backfill,
                client_factory=quotes_client_factory(config),
                rate_limit=rate_limit,
            )
            if tdx_df.height:
                frames.append(tdx_df.with_columns(pl.lit("tdx_protocol").alias("source")))
    except ImportError:
        logger.debug("TDX wire client unavailable for corporate_actions")
    except Exception as exc:
        logger.warning("TDX corporate_actions failed: %s", exc)

    try:
        if not primary_only:
            em_df = fetch_corporate_actions_eastmoney(trade_date, backfill=backfill)
            if em_df.height:
                frames.append(em_df.with_columns(pl.lit("eastmoney").alias("source")))
    except Exception as exc:
        logger.warning("EastMoney corporate_actions backup failed: %s", exc)

    if frames:
        out = pl.concat(frames, how="diagonal_relaxed")
        if "source" not in out.columns:
            out = out.with_columns(pl.lit("tdx_protocol").alias("source"))
        else:
            out = out.with_columns(
                pl.when(pl.col("source").is_null())
                .then(pl.lit("tdx_protocol"))
                .otherwise(pl.col("source"))
                .alias("source")
            )
        if not backfill:
            out = out.filter(pl.col("ex_date") == trade_date)
        return out.unique(subset=["symbol", "ex_date", "action_type"], keep="last")

    return _fail_or_mock(
        "corporate_actions",
        "no corporate actions from TDX or EastMoney",
        allow_mock,
        empty,
    )


def fetch_trading_status(
    symbols: list[str],
    trade_date: date,
    *,
    rate_limit: RateLimitSpec | None = None,
    allow_mock: bool = False,
) -> pl.DataFrame:
    def _mock_status() -> pl.DataFrame:
        rows = [
            {
                "symbol": sym,
                "trade_date": trade_date,
                "is_trading": True,
                "status": "normal",
            }
            for sym in symbols
        ]
        return _mark_mock(pl.DataFrame(rows))

    if allow_mock:
        return _fail_or_mock("trading_status", _MOCK_SHORT_CIRCUIT, True, _mock_status())

    wait_spec(rate_limit)
    try:
        df = fetch_trading_status_eastmoney(symbols, trade_date)
        if df.height:
            return df
        reason = "EastMoney returned no trading status rows"
    except Exception as exc:
        reason = f"EastMoney trading_status failed: {exc}"

    return _fail_or_mock(
        "trading_status",
        reason,
        allow_mock,
        _mock_status(),
    )


def normalize_with_source(
    df: pl.DataFrame,
    source: str = "tdx_protocol",
    *,
    dataset: str | None = None,
) -> pl.DataFrame:
    """Stamp provenance. *dataset* selects the version — daily_bars is on v2
    (volume in 股); everything else defaults to v1."""
    return with_provenance(df, source=source, data_version=data_version_for(dataset or ""))
