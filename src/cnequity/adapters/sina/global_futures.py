"""Sina global futures daily K-line (offshore).

Used for research series that EastMoney push2his does not cover reliably
offshore — currently COMEX gold continuous (``GC`` → lake ``GC0.CMX``).

API returns the full history each call; callers filter ``[start, end]``.

Sina labels rows with the CME trade date (session ending 17:00 New York) and
keeps updating the current session's row intraday.  A row is only emitted once
its session has closed; the daily step re-reads the trailing
``RECHECK_DAYS`` so a value captured before a weekend/holiday gets replaced.
Advisory / research only — not A-share hfq, not a backtest input.
"""

from __future__ import annotations

import logging
import math
from datetime import date, datetime, time
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import polars as pl

from cnequity.adapters.numeric import finite_int64
from cnequity.domain.http_policy import record_http_response
from cnequity.domain.rate_limit import source_request

logger = logging.getLogger(__name__)

_URL = (
    "https://stock2.finance.sina.com.cn/futures/api/json.php/"
    "GlobalFuturesService.getGlobalFuturesDailyKLine"
)

# (lake_symbol, sina_symbol, name, exchange)
OFFSHORE_CONTRACTS: tuple[tuple[str, str, str, str], ...] = (("GC0.CMX", "GC", "COMEX黄金", "CMX"),)

DEFAULT_BACKFILL_START = date(2020, 1, 1)

# CME Globex metals close at 17:00 New York; wait for the next session open
# (18:00) so Sina has published the final bar for the closed session.
_SESSION_TZ = ZoneInfo("America/New_York")
_SESSION_FINAL_AFTER = time(18, 0)
RECHECK_DAYS = 7


def session_is_final(trade_date: date, now: datetime | None = None) -> bool:
    """True once the CME session labelled *trade_date* has closed."""
    current = now or datetime.now(tz=_SESSION_TZ)
    final_at = datetime.combine(trade_date, _SESSION_FINAL_AFTER, tzinfo=_SESSION_TZ)
    return current >= final_at


class SinaGlobalFuturesPayloadError(RuntimeError):
    """Raised when Sina's global-futures endpoint returns a malformed payload."""


def _f(x: Any) -> float | None:
    if x is None or x == "":
        return None
    try:
        parsed = float(x)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _parse_rows(
    payload: list[dict],
    *,
    symbol: str,
    name: str,
    exchange: str,
    start: date,
    end: date,
    now: datetime | None = None,
) -> list[dict]:
    rows: list[dict] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            logger.warning(
                "offshore commodity_bars: skipping non-object row %s for %s",
                index,
                symbol,
            )
            continue
        d_raw = item.get("date")
        if not d_raw:
            continue
        try:
            trade_date = date.fromisoformat(str(d_raw)[:10])
        except ValueError:
            continue
        if trade_date < start or trade_date > end:
            continue
        if not session_is_final(trade_date, now):
            logger.info(
                "offshore commodity_bars: skipping unfinished session %s for %s",
                trade_date,
                symbol,
            )
            continue
        close = _f(item.get("close"))
        open_ = _f(item.get("open"))
        high = _f(item.get("high"))
        low = _f(item.get("low"))
        vol = _f(item.get("volume"))
        if (
            close is None
            or open_ is None
            or high is None
            or low is None
            or vol is None
            or not all(math.isfinite(v) for v in (open_, high, low, close, vol))
            or min(open_, high, low, close) <= 0
            or vol < 0
        ):
            logger.warning("offshore commodity_bars: malformed row for %s: %r", symbol, item)
            continue
        try:
            volume = finite_int64(vol, minimum=0)
        except ValueError:
            logger.warning("offshore commodity_bars: malformed volume for %s: %r", symbol, item)
            continue
        oi = _f(item.get("position"))
        rows.append(
            {
                "symbol": symbol,
                "name": name,
                "exchange": exchange,
                "trade_date": trade_date,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "amount": None,
                # `settlement` is a price, not a position count.  Do not use
                # it as a fill when the feed omits open interest: the two
                # fields have different units and the resulting value looks
                # plausible enough to survive schema validation. A main-continuous
                # series always holds positions, so a 0 means the feed carries none
                # (COMEX gold has none on every row), not an empty book.
                "open_interest": oi if oi and oi > 0 else None,
                "source": "sina",
            }
        )
    return rows


def fetch_offshore_commodity_bars_range(
    start: date,
    end: date,
    *,
    contracts: tuple[tuple[str, str, str, str], ...] | None = None,
    client: httpx.Client | None = None,
    config=None,
    strict: bool = False,
    now: datetime | None = None,
) -> pl.DataFrame:
    """Fetch offshore continuous daily OHLC for [*start*, *end*] (inclusive)."""
    if start > end:
        return pl.DataFrame()
    # Preserve an explicit empty universe instead of silently restoring the
    # default offshore contract.
    universe = OFFSHORE_CONTRACTS if contracts is None else contracts
    owns = client is None
    if client is None:
        client = httpx.Client(
            timeout=60.0,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://finance.sina.com.cn",
            },
        )
    rows: list[dict] = []
    try:
        for symbol, sina_sym, name, exchange in universe:
            try:
                with source_request(config, "sina"):
                    resp = client.get(_URL, params={"symbol": sina_sym})
                    record_http_response(config, "sina", resp)
                resp.raise_for_status()
                payload = resp.json()
                if not isinstance(payload, list):
                    raise SinaGlobalFuturesPayloadError(
                        f"Sina global futures response is not a list (got {type(payload).__name__})"
                    )
                part = _parse_rows(
                    payload,
                    symbol=symbol,
                    name=name,
                    exchange=exchange,
                    start=start,
                    end=end,
                    now=now,
                )
                rows.extend(part)
                if not part:
                    logger.warning(
                        "offshore commodity_bars: empty for %s (%s) %s→%s",
                        symbol,
                        sina_sym,
                        start,
                        end,
                    )
            except Exception as exc:
                logger.warning(
                    "offshore commodity_bars: %s (%s) failed: %s: %s",
                    symbol,
                    sina_sym,
                    type(exc).__name__,
                    exc,
                )
                if strict:
                    raise RuntimeError(
                        f"offshore commodity_bars failed for {symbol} ({sina_sym})"
                    ) from exc
    finally:
        if owns:
            client.close()
    if not rows:
        return pl.DataFrame()
    return (
        pl.DataFrame(rows)
        .unique(subset=["symbol", "trade_date"], keep="last")
        .sort(["trade_date", "symbol"])
    )


def fetch_offshore_commodity_bars(
    trade_date: date,
    *,
    start: date | None = None,
    end: date | None = None,
    client: httpx.Client | None = None,
    config=None,
) -> pl.DataFrame:
    """Single-day or explicit range entry (Sina always returns full history)."""
    s = start or trade_date
    e = end or trade_date
    return fetch_offshore_commodity_bars_range(s, e, client=client, config=config)
