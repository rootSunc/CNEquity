"""TDX intraday bars, paged backwards from the tip like the daily fetch.

The protocol is the same ``get_security_bars`` call the daily path uses, with a
different category, so nothing in ``_wire`` changes. What differs is what the
server *keeps*: measured 2026-08-01, a standard host serves 22,800 1-minute bars
(95 trading days) and 23,568 5-minute bars (491 days) per symbol, uniformly
across exchanges and liquidity. There is no deeper free source, so a window
older than that returns nothing rather than less — callers must treat the
horizon as a contract, not as a gap to retry.

Bars are labelled by their CLOSING minute, which is the convention this module
preserves: 09:31 is the first bar of the session and covers 09:30–09:31, and
15:00 carries the closing auction.

``volume`` needs no conversion here. TDX is per-frequency, not per-vendor: its
daily K is 手 and the daily adapter multiplies by 100, but intraday bars off the
same parser are already 股 (600519 1m bar vol=59,700 against amount=88,977,784
at ~1490 → 59,716 shares). Applying the daily conversion would inflate every
row by 100×. See :mod:`cn_market_lake.domain.units`.

**A single minute's volume is not reproducible; the day's total is.** Fetching
the same settled window twice returns different ``volume``/``amount`` for ~0.6%
of bars (measured: 257 of 43,920 over 40 symbols × 5 sessions). It is boundary
attribution, not corruption — a trade sitting on a minute edge lands either
side depending on when the server aggregated, and the neighbour compensates
exactly: across all 183 symbol-days in that sample the daily volume totals were
identical and the amount totals matched to 0.00e+00 relative. Sums are exact;
one minute's share count is not. This is a property of the source, unrelated
to concurrency — two *serial* fetches disagreed more (435 rows) than a serial
and a threaded one did (181).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date, datetime, time

import polars as pl

from cn_market_lake.adapters.tdx_protocol._decode import decoded_quantity
from cn_market_lake.domain.rate_limit import RateLimitSpec, wait_spec

logger = logging.getLogger(__name__)

_PAGE_SIZE = 800

# category → (label, bars per full session). The label is what lands in the
# `frequency` column and what the CLI accepts.
FREQUENCIES: dict[str, tuple[int, int]] = {
    "1m": (8, 240),
    "5m": (0, 48),
    "15m": (1, 16),
    "30m": (2, 8),
    "60m": (3, 4),
}

# Continuous-trading windows, as closing-minute labels. A bar at exactly 11:30
# or 15:00 is the last of its half-session; 13:00 is not a bar (13:01 is).
#
# The 13:00 exclusion is not pedantry: the source really does emit bars there.
# 162107.SZ, a barely-traded LOF, returns a 13:00-labelled bar on days it did
# not trade, with zero volume and a stale close carried forward — padding, not
# a tradable minute. An actively traded name emits none (600519 over 2,400 bars
# checked, zero). Keeping them would put a phantom bar in every gap check and
# skew any resampling that assumes fixed bar counts.
SESSIONS: tuple[tuple[time, time], ...] = (
    (time(9, 31), time(11, 30)),
    (time(13, 1), time(15, 0)),
)


class TdxMinuteBarsError(RuntimeError):
    """Raised when a minute-bar page fails and the caller requires completeness."""


def bars_per_session(frequency: str) -> int:
    return FREQUENCIES[frequency][1]


def category_for(frequency: str) -> int:
    try:
        return FREQUENCIES[frequency][0]
    except KeyError:
        raise ValueError(
            f"unsupported intraday frequency {frequency!r} (known: {', '.join(FREQUENCIES)})"
        ) from None


def in_session(stamp: datetime) -> bool:
    """Whether *stamp* is a legal closing-minute label for an A-share session."""
    clock = stamp.time()
    return any(lo <= clock <= hi for lo, hi in SESSIONS)


def _parse_stamp(row: dict) -> datetime | None:
    """Bar timestamp from a wire row, preferring the decoded integer fields.

    The parser emits both the components and a formatted ``datetime`` string;
    the components avoid a reparse and are what the string is built from.
    """
    try:
        return datetime(
            int(row["year"]),
            int(row["month"]),
            int(row["day"]),
            int(row["hour"]),
            int(row["minute"]),
        )
    except (KeyError, TypeError, ValueError):
        pass
    raw = row.get("datetime")
    if not raw:
        return None
    try:
        return datetime.strptime(str(raw)[:16], "%Y-%m-%d %H:%M")
    except ValueError:
        return None


def _rows_to_dicts(
    rows: list[dict],
    sym: str,
    frequency: str,
    start: date,
    end: date,
) -> tuple[list[dict], int]:
    """Convert one page. Returns (in-window rows, count dropped as off-session)."""
    out: list[dict] = []
    off_session = 0
    for row in rows:
        stamp = _parse_stamp(row)
        if stamp is None:
            continue
        trade_date = stamp.date()
        if trade_date < start or trade_date > end:
            continue
        # A bar outside the continuous-trading windows is a decode error, not a
        # tradable minute — keeping it would silently corrupt any resampling
        # and any bar-count gap check built on top.
        if not in_session(stamp):
            off_session += 1
            continue
        out.append(
            {
                "symbol": sym,
                "trade_date": trade_date,
                "bar_time": stamp,
                "frequency": frequency,
                "open": float(row.get("open", 0)),
                "high": float(row.get("high", 0)),
                "low": float(row.get("low", 0)),
                "close": float(row.get("close", 0)),
                "volume": int(decoded_quantity(row.get("volume", row.get("vol", 0)))),
                "amount": decoded_quantity(row.get("amount", 0)),
            }
        )
    return out, off_session


def _page_min_date(rows: list[dict]) -> date | None:
    stamps = [s for s in (_parse_stamp(r) for r in rows) if s is not None]
    return min(s.date() for s in stamps) if stamps else None


def fetch_minute_bars_paginated(
    client,
    sym: str,
    start: date,
    end: date,
    *,
    frequency: str = "1m",
    rate_limit: RateLimitSpec | None = None,
    backfill: bool = False,
    on_page: Callable[[], None] | None = None,
    max_pages: int | None = None,
) -> list[dict]:
    """Intraday bars for *sym* in [start, end], paging back through the tip.

    ``max_pages`` bounds the walk for callers that know the horizon; without it
    the loop still terminates on a short page or on reaching *start*, but a
    symbol whose history runs deeper than the window costs pages that are then
    discarded.
    """
    category = category_for(frequency)
    code, exch = sym.split(".")
    if exch == "BJ":
        # TDX has no Beijing route at all — the daily path routes those symbols
        # to a fallback vendor. There is no intraday fallback, so say so rather
        # than returning an empty list that reads as "no trading".
        raise TdxMinuteBarsError(f"{sym}: TDX serves no Beijing-exchange intraday bars")
    market = 1 if exch == "SH" else 0

    offset_pos = 0
    all_rows: list[dict] = []
    off_session_total = 0
    page = 0

    while True:
        if max_pages is not None and page >= max_pages:
            break
        wait_spec(rate_limit)
        try:
            raw = client.bars(
                symbol=code,
                frequency=category,
                market=market,
                start=offset_pos,
                offset=_PAGE_SIZE,
            )
        except Exception as exc:
            if offset_pos == 0 or backfill:
                raise TdxMinuteBarsError(
                    f"TDX {frequency} page failed for {sym} at start={offset_pos}"
                ) from exc
            logger.warning(
                "TDX %s page failed for %s at start=%s: %s", frequency, sym, offset_pos, exc
            )
            break

        if not raw:
            break

        page_rows, off_session = _rows_to_dicts(raw, sym, frequency, start, end)
        off_session_total += off_session
        all_rows.extend(page_rows)

        page_min = _page_min_date(raw)
        if page_min is not None and page_min < start:
            break
        if len(raw) < _PAGE_SIZE:
            break
        offset_pos += _PAGE_SIZE
        page += 1
        if on_page is not None:
            on_page()

    if off_session_total:
        logger.warning(
            "%s %s: dropped %d bar(s) outside trading sessions",
            sym,
            frequency,
            off_session_total,
        )
    if not all_rows:
        return []

    df = pl.DataFrame(all_rows).unique(
        subset=["symbol", "trade_date", "bar_time", "frequency"], keep="last"
    )
    return df.sort("bar_time").to_dicts()


def pages_for_window(frequency: str, trading_days: int) -> int:
    """Pages needed to cover *trading_days* — the loop's upper bound.

    One extra page: the window rarely aligns to a page boundary, and stopping a
    page early silently truncates the oldest day.
    """
    per_day = bars_per_session(frequency)
    return max(1, -(-trading_days * per_day // _PAGE_SIZE) + 1)
