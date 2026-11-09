"""One-minute futures bars from Sina, for a chosen set of contracts.

``InnerFuturesNewService.getFewMinLine?type=1`` returns the latest 1023
one-minute bars of one contract, for every exchange (measured 2026-09-25 on
CU2611, M2701, TA2701, IF2612, SC2611, SI2611). That is about two sessions for
a product that trades through the night and five for CFFEX. It is a window,
not a history: nothing older can be asked for, so this dataset only grows
from the day it is switched on, and it must be read every session or bars
fall out of the window unseen.

Bars are labelled by their closing minute (the 09:00 open prints as 09:01),
in Beijing wall-clock time. Night-session bars belong to the *next* trading
day, which is how the exchanges book them: a bar at 21:05 on a Friday, or at
00:30 on the Saturday, is Monday's session. ``trade_date`` is assigned from the
lake's trading calendar on that rule.

Volume and open interest are this vendor's one-sided counts (only recent
sessions are ever served, all after the 2020 switch).
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, time, timedelta

import httpx
import polars as pl

from cnequity.adapters.futures_exchange.common import fetch_bytes, parse_number, parse_price

logger = logging.getLogger(__name__)

SOURCE = "sina"
URL = (
    "https://stock2.finance.sina.com.cn/futures/api/jsonp.php/x/"
    "InnerFuturesNewService.getFewMinLine"
)
_HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn"}
_ARRAY = re.compile(r"\[.*\]", re.S)
#: Bars at or after this wall-clock time, or before ``_NIGHT_END``, are night
#: session and belong to the next trading day.
_NIGHT_START = time(20, 0)
_NIGHT_END = time(3, 0)


def sina_code(symbol: str) -> str:
    """``CU2611.SHF`` → ``CU2611``: Sina spells every exchange four-digit, upper case."""
    return symbol.split(".", 1)[0]


def session_of(bar_time: datetime, sessions: list[date]) -> date | None:
    """The trading day a bar belongs to, or ``None`` outside known sessions."""
    clock = bar_time.time()
    if clock >= _NIGHT_START or clock < _NIGHT_END:
        evening = bar_time.date() if clock >= _NIGHT_START else bar_time.date() - timedelta(days=1)
        return next((d for d in sessions if d > evening), None)
    day = bar_time.date()
    return day if day in sessions else None


def _whole(value) -> int | None:
    number = parse_number(value)
    return None if number is None else int(number)


def parse_minutes(payload: list[dict], *, symbol: str, sessions: list[date]) -> pl.DataFrame:
    rows = []
    exchange = symbol.rsplit(".", 1)[1]
    for item in payload:
        if not isinstance(item, dict) or not item.get("d"):
            continue
        try:
            bar_time = datetime.fromisoformat(str(item["d"]))
        except ValueError:
            continue
        trade_date = session_of(bar_time, sessions)
        if trade_date is None:
            continue
        # A blank count stays None, never 0: the feed prints 0 for a quiet minute.
        volume = _whole(item.get("v"))
        traded = bool(volume)
        rows.append(
            {
                "symbol": symbol,
                "exchange": exchange,
                "trade_date": trade_date,
                "bar_time": bar_time,
                "frequency": "1m",
                "open": parse_price(item.get("o")) if traded else None,
                "high": parse_price(item.get("h")) if traded else None,
                "low": parse_price(item.get("l")) if traded else None,
                "close": parse_price(item.get("c")),
                "volume": volume,
                "open_interest": _whole(item.get("p")),
                "source": SOURCE,
            }
        )
    return pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()


def fetch_minute_bars(
    symbols: list[str], *, sessions: list[date], config=None
) -> tuple[pl.DataFrame, dict[str, str]]:
    """Latest window of 1m bars for each symbol; failures are returned, not raised."""
    frames: list[pl.DataFrame] = []
    failures: dict[str, str] = {}
    with httpx.Client(timeout=30.0, headers=_HEADERS, follow_redirects=True) as client:
        for symbol in symbols:
            try:
                body = fetch_bytes(
                    URL,
                    params={"symbol": sina_code(symbol), "type": "1"},
                    config=config,
                    source=SOURCE,
                    ttl=30,
                    client=client,
                )
                match = _ARRAY.search(body.decode("gbk", "replace"))
                payload = json.loads(match.group(0)) if match else None
                if not payload:
                    failures[symbol] = "no bars served"
                    continue
                frame = parse_minutes(payload, symbol=symbol, sessions=sessions)
                if not frame.is_empty():
                    frames.append(frame)
            except Exception as exc:  # noqa: BLE001 — one contract must not sink the rest
                failures[symbol] = f"{type(exc).__name__}: {exc}"
    frame = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
    return frame, failures
