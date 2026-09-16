"""Unadjusted daily bars from the 同花顺 official API.

Two jobs, both measured before being relied on.

**A licensed footing for the deep history.** ``daily_bars`` splits cleanly by
source: ``ths`` — the unauthenticated 10jqka scraper next door — owns
2001-01-02..2015-12-31 alone, 5,353,397 rows over 2,579 securities with no
second source anywhere, while ``tdx_protocol`` owns 2016 onward and is the only
part the failover config arbitrates. The official history reaches back to about
2005-01-04 (two securities listed before 1996 both stop there, so it is a global
floor rather than a per-security one), which covers 4,403,582 of those rows.
The remaining 949,815 rows before 2005 stay where they are.

**A third opinion on the rest.** 2016 onward is already tdx against eastmoney —
two vendors, and a binary disagreement cannot say which is wrong.

Agreement is not in doubt. Measured 2026-09-08 over 12 securities and three
years: 8,600 comparable rows, and every one of open/high/low/close within
0.5bps, p99 and max both 0.00. ``volume`` is already in 股 and ``turnover`` in
yuan — both ratios measured at exactly 1.0000 against curated — so nothing here
converts units. Do not "align" this with the TDX path, which reports 手.

Only ``adjust=none`` is ever requested. The lake derives hfq from the sina
factors it already holds, and mixing a vendor's own adjusted series into that
would plant a break at every ex-date.
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from cnequity.adapters.ths_official.client import ThsOfficialClient

logger = logging.getLogger(__name__)

__all__ = [
    "ETF_MAX_WINDOW_DAYS",
    "HISTORY_FLOOR",
    "MAX_WINDOW_DAYS",
    "fetch_daily_bars",
    "split_windows",
]

CST = timezone(timedelta(hours=8))
SOURCE = "ths_official"

# Measured: requests for 2004 come back starting 2005-01-04 for securities that
# listed in the 1990s, so this is the service's floor, not a listing date.
HISTORY_FLOOR = date(2005, 1, 1)
# The endpoint rejects a span over ten years with code=1003.
MAX_WINDOW_DAYS = 365 * 10 - 5

# ETFs go through a different endpoint with a different, undocumented limit, and
# a far nastier failure mode. The contract says five calendar years; measured
# 2026-09-12 against 510300.SH ending 2025-12-31, a 1,552-day window returns
# 1,030 bars and a 1,644-day one returns **zero** — no error, no code, just an
# empty item list that reads exactly like a fund with no history. Cap at four
# years, a width that was verified to work, so the trap can never be sprung.
#
# Treat it as a weak peer, not a source. Measured over 25 lake securities marked
# `etf`: six are LOF or OTC funds the endpoint refuses outright (`code=3004 This
# fund does not support market data`, `code=3001 Fund not found`), the peer
# returned 2,435 rows against 13,096 in the lake, and one fund — 159582.SZ —
# prices at exactly one fifth of the lake's while reporting identical volume.
# The lake is demonstrably right there: its own amount/volume divided by close
# sits at 1.00, where the peer's price would put it at 5. A share conversion
# applied to price but not to volume.
ETF_ENDPOINT = "/api/fund/market/historical"
ETF_MAX_WINDOW_DAYS = 365 * 4
_A_SHARE_ENDPOINT = "/api/a-share/prices/historical"

_OUTPUT_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
    "amount": pl.Float64,
}


def _ms(value: date) -> int:
    return int(datetime(value.year, value.month, value.day, tzinfo=CST).timestamp() * 1000)


def split_windows(
    start: date, end: date, max_days: int = MAX_WINDOW_DAYS
) -> list[tuple[date, date]]:
    """Cut ``[start, end]`` into spans the endpoint will accept."""
    windows: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        stop = min(cursor + timedelta(days=max_days), end)
        windows.append((cursor, stop))
        cursor = stop + timedelta(days=1)
    return windows


def fetch_daily_bars(
    symbols: list[str],
    start: date,
    end: date,
    *,
    client: ThsOfficialClient,
    workers: int = 1,
    etf: bool = False,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Unadjusted daily bars for *symbols*, split across accepted windows.

    Set ``etf`` for exchange-traded funds. They are not served by the A-share
    endpoint at all — it answers ``code=1002 Unknown thscode``, which cost 42 of
    200 securities in an unfiltered sample — and their own endpoint carries a
    tighter window limit that fails silently.

    Returns the frame and counters. A security the upstream refuses — delisted
    names answer ``code=1002 Unknown thscode`` — is counted and skipped rather
    than ending the sweep, because 594 of the lake's securities are in exactly
    that position and no sweep of the full universe can avoid them.
    """
    if start < HISTORY_FLOOR:
        logger.info(
            "ths_official bars: requested from %s but the service floors at %s",
            start,
            HISTORY_FLOOR,
        )
    endpoint = ETF_ENDPOINT if etf else _A_SHARE_ENDPOINT
    windows = split_windows(start, end, ETF_MAX_WINDOW_DAYS if etf else MAX_WINDOW_DAYS)
    rows: list[dict] = []
    counters: dict[str, object] = {"requests": 0, "empty": 0, "failed": 0, "bars": 0}
    # Named, not just counted. A symbol the vendor never answered for is absent
    # from the frame exactly like one it answered "nothing" for, and a caller
    # that cannot tell them apart reports the first as evidence of the second.
    unanswered: set[str] = set()
    lock = threading.Lock()

    def one_symbol(symbol: str) -> None:
        local: list[dict] = []
        for window_start, window_end in windows:
            with lock:
                counters["requests"] += 1
            try:
                params = {
                    "thscode": symbol,
                    "interval": "1d",
                    "start": _ms(window_start),
                    "end": _ms(window_end),
                }
                if not etf:
                    # Never forward/backward: the lake derives hfq from the sina
                    # factors, and a vendor's own adjustment would break that.
                    # The fund endpoint takes no adjust parameter at all.
                    params["adjust"] = "none"
                data = client.get(endpoint, **params)
            except Exception as exc:  # noqa: BLE001 — one refusal must not end the sweep
                with lock:
                    counters["failed"] += 1
                    unanswered.add(symbol)
                logger.warning("ths_official bars failed for %s %s: %s", symbol, window_start, exc)
                continue
            items = (data or {}).get("item") or []
            if not items:
                with lock:
                    counters["empty"] += 1
                continue
            for item in items:
                stamp = item.get("date_ms")
                if not stamp:
                    continue
                local.append(
                    {
                        "symbol": symbol,
                        "trade_date": datetime.fromtimestamp(stamp / 1000, tz=CST).date(),
                        "open": item.get("open_price"),
                        "high": item.get("high_price"),
                        "low": item.get("low_price"),
                        # Already 股 and yuan; measured ratio 1.0000 both.
                        "volume": item.get("volume"),
                        "amount": item.get("turnover"),
                        "close": item.get("close_price"),
                    }
                )
        with lock:
            rows.extend(local)
            counters["bars"] += len(local)

    if workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(one_symbol, symbols))
    else:
        for symbol in symbols:
            one_symbol(symbol)

    counters["unanswered_symbols"] = sorted(unanswered)
    if not rows:
        return pl.DataFrame(schema=_OUTPUT_SCHEMA), counters
    frame = pl.DataFrame(rows).select(
        pl.col("symbol").cast(pl.Utf8),
        pl.col("trade_date").cast(pl.Date),
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64).round(0).cast(pl.Int64),
        pl.col("amount").cast(pl.Float64),
    )
    frame = frame.unique(subset=["symbol", "trade_date"], keep="last")
    kept = _drop_impossible_candles(frame)
    counters["impossible_candles"] = frame.height - kept.height
    counters["bars"] = kept.height
    return kept.sort(["symbol", "trade_date"]), counters


def _drop_impossible_candles(frame: pl.DataFrame) -> pl.DataFrame:
    """Discard bars whose own high/low cannot contain their open/close.

    The lake's schema refuses these outright, and rightly — but a single bad
    upstream row must not abort a sweep of millions. Measured over the first
    236,546 rows of the 2005-2015 re-source, two bars were inconsistent by one
    tick: 000055.SZ on 2007-06-28 reports a low of 6.65 against a close of 6.64,
    and 000507.SZ on 2006-10-20 a high of 5.24 against an open of 5.25.

    Dropped rather than repaired. Widening the envelope to fit would invent a
    price that never printed, and there is no way to tell which of the four
    fields is the wrong one.
    """
    traded = pl.col("volume").is_null() | (pl.col("volume") > 0)
    impossible = traded & (
        (pl.col("high") < pl.col("open"))
        | (pl.col("high") < pl.col("close"))
        | (pl.col("low") > pl.col("open"))
        | (pl.col("low") > pl.col("close"))
        | (pl.col("low") > pl.col("high"))
    )
    nonpositive = (
        (pl.col("open") <= 0)
        | (pl.col("high") <= 0)
        | (pl.col("low") <= 0)
        | (pl.col("close") <= 0)
    )
    return frame.filter(~(impossible | nonpositive).fill_null(False))
