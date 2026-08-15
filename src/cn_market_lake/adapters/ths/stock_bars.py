"""同花顺 per-stock daily bars — history older than the primary vendor carries.

TDX serves the lake from 2016; 同花顺 keeps per-year files back to each stock's
listing (贵州茅台 to 2001, 平安银行 to 1991). This module exists to deepen the
research window, not to replace the daily source.

Two rules make the deeper history safe to mix with what is already stored:

**Unadjusted only.** The URL takes ``00`` (raw), never ``01``/``02``. 同花顺 also
publishes qfq and hfq, but its adjustment algorithm differs from Sina's — the two
disagree by up to ~340bps on ex-dividend days, silently, because they treat the
dividend differently. Splicing its hfq onto the existing series would plant a
break at every dividend. Raw prices, by contrast, agree field-for-field (verified
across the 2020 overlap: 243 sessions, zero OHLC mismatches), so the lake keeps
deriving hfq from the Sina factors it already uses. Those factors reach back to
listing too, which is what makes one continuous convention possible.

**Fetched per year.** History is served as one file per calendar year, so a
symbol costs one request per year of history rather than one per day.

``volume`` is already in 股, the lake's unit, so no conversion happens here —
verified twice: ``amount / close / volume`` has a median of 0.999 over the
5,303,037 curated 同花顺 rows, and a live 2015 file for 600519 gives
1,875,063,100 / 202.52 / 9,451,517 = 0.98. Do not "align" this with the TDX
path, which reports 手 and converts. See :mod:`cn_market_lake.domain.units`.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import TYPE_CHECKING, Any

from cn_market_lake.adapters.ths.boards import ThsError, _get, _unwrap_jsonp

if TYPE_CHECKING:
    from cn_market_lake.config import Config

logger = logging.getLogger(__name__)

# 00 = unadjusted; see the module docstring for why never 01/02.
_STOCK_KLINE_URL = "https://d.10jqka.com.cn/v6/line/hs_{code}/00/{part}.js"

# Consecutive symbol failures tolerated before a sweep gives up: past this the
# source has stopped answering, and continuing only deepens a rate-limit penalty.
_DEAD_SOURCE_STREAK = 10


def _parse_stock_kline(payload: dict[str, Any], symbol: str) -> list[dict]:
    rows: list[dict] = []
    raw = str(payload.get("data") or "")
    if not raw:
        return rows
    for line in raw.split(";"):
        parts = line.split(",")
        if len(parts) < 7 or len(parts[0]) != 8:
            continue
        stamp = parts[0]
        try:
            rows.append(
                {
                    "symbol": symbol,
                    "trade_date": date(int(stamp[:4]), int(stamp[4:6]), int(stamp[6:8])),
                    "open": float(parts[1]),
                    "high": float(parts[2]),
                    "low": float(parts[3]),
                    "close": float(parts[4]),
                    "volume": int(float(parts[5])),
                    "amount": float(parts[6]),
                }
            )
        except ValueError:
            continue
    return rows


def fetch_stock_bars(
    symbol: str,
    start: date,
    end: date,
    *,
    config: Config | None = None,
) -> list[dict]:
    """Unadjusted daily bars for one symbol over ``[start, end]``.

    A year file missing is normal — the symbol had not listed yet — and is
    skipped; a transport error is not, and propagates.
    """
    code = symbol[:6]
    rows: list[dict] = []
    seen: set[date] = set()
    for year in range(start.year, end.year + 1):
        try:
            payload = _unwrap_jsonp(
                _get(_STOCK_KLINE_URL.format(code=code, part=year), config=config)
            )
        except ThsError as exc:
            logger.debug("THS %s %s unavailable: %s", symbol, year, exc)
            continue
        for row in _parse_stock_kline(payload, symbol):
            if row["trade_date"] in seen or not (start <= row["trade_date"] <= end):
                continue
            seen.add(row["trade_date"])
            rows.append(row)
    rows.sort(key=lambda r: r["trade_date"])
    return rows


def sweep_stock_bars(
    symbols: list[str],
    start: date,
    end: date,
    *,
    config: Config | None = None,
    on_batch=None,
    batch_size: int = 50,
) -> tuple[list[dict], list[str]]:
    """Bars for many symbols. Returns ``(rows, failed_symbols)``.

    ``on_batch(rows, symbols)`` makes progress durable partway through — a sweep
    of thousands of symbols that dies late must not discard what it already has —
    and clears the accumulator, so a batched caller gets an empty row list back.
    """
    rows: list[dict] = []
    failed: list[str] = []
    batch: list[str] = []
    streak = 0

    for i, symbol in enumerate(symbols, start=1):
        try:
            got = fetch_stock_bars(symbol, start, end, config=config)
            rows.extend(got)
            batch.append(symbol)
            streak = 0
        except Exception as exc:  # noqa: BLE001 — recorded, sweep continues
            logger.warning("THS stock bars failed for %s: %s", symbol, exc)
            failed.append(symbol)
            streak += 1
            if streak >= _DEAD_SOURCE_STREAK:
                logger.error("THS: %d consecutive failures at %s — aborting sweep", streak, symbol)
                break
        if on_batch and (i % batch_size == 0 or i == len(symbols)):
            on_batch(rows, batch)
            rows, batch = [], []
    if on_batch and batch:
        on_batch(rows, batch)
        rows = []
    return rows, failed
