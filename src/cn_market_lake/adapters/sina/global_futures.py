"""Sina global futures daily K-line (offshore).

Used for research series that EastMoney push2his does not cover reliably
offshore — currently COMEX gold continuous (``GC`` → lake ``GC0.CMX``).

API returns the full history each call; callers filter ``[start, end]``.
Advisory / research only — not A-share hfq, not a backtest input.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import httpx
import polars as pl

logger = logging.getLogger(__name__)

_URL = (
    "https://stock2.finance.sina.com.cn/futures/api/json.php/"
    "GlobalFuturesService.getGlobalFuturesDailyKLine"
)

# (lake_symbol, sina_symbol, name, exchange)
OFFSHORE_CONTRACTS: tuple[tuple[str, str, str, str], ...] = (("GC0.CMX", "GC", "COMEX黄金", "CMX"),)

DEFAULT_BACKFILL_START = date(2020, 1, 1)


def _f(x: Any) -> float | None:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _parse_rows(
    payload: list[dict],
    *,
    symbol: str,
    name: str,
    exchange: str,
    start: date,
    end: date,
) -> list[dict]:
    rows: list[dict] = []
    for item in payload:
        d_raw = item.get("date")
        if not d_raw:
            continue
        try:
            trade_date = date.fromisoformat(str(d_raw)[:10])
        except ValueError:
            continue
        if trade_date < start or trade_date > end:
            continue
        close = _f(item.get("close"))
        if close is None or close <= 0:
            continue
        open_ = _f(item.get("open"))
        high = _f(item.get("high"))
        low = _f(item.get("low"))
        vol = _f(item.get("volume"))
        oi = _f(item.get("position"))
        settle = _f(item.get("settlement"))
        rows.append(
            {
                "symbol": symbol,
                "name": name,
                "exchange": exchange,
                "trade_date": trade_date,
                "open": open_ if open_ and open_ > 0 else close,
                "high": high if high and high > 0 else close,
                "low": low if low and low > 0 else close,
                "close": close,
                "volume": int(vol) if vol is not None else 0,
                "amount": None,
                "open_interest": oi
                if oi and oi > 0
                else (settle if settle and settle > 0 else None),
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
) -> pl.DataFrame:
    """Fetch offshore continuous daily OHLC for [*start*, *end*] (inclusive)."""
    if start > end:
        return pl.DataFrame()
    universe = contracts or OFFSHORE_CONTRACTS
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
                resp = client.get(_URL, params={"symbol": sina_sym})
                resp.raise_for_status()
                payload = resp.json()
                if not isinstance(payload, list):
                    logger.warning(
                        "offshore commodity_bars: unexpected payload for %s: %s",
                        sina_sym,
                        type(payload).__name__,
                    )
                    continue
                part = _parse_rows(
                    payload,
                    symbol=symbol,
                    name=name,
                    exchange=exchange,
                    start=start,
                    end=end,
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
) -> pl.DataFrame:
    """Single-day or explicit range entry (Sina always returns full history)."""
    s = start or trade_date
    e = end or trade_date
    return fetch_offshore_commodity_bars_range(s, e, client=client)
