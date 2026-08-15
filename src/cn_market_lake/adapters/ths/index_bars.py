"""同花顺 index bars — benchmark history older than the primary vendor carries.

The lake's index_bars start 2016 alongside the stock bars; 同花顺 keeps indices
back to each one's base date (上证综指 to 1990-12, 深证成指 to 1991-04). Without
them the deepened stock history has no benchmark to measure excess return
against, so every relative statistic stops at 2016 even though the stocks do not.

Same two rules as ``stock_bars``: unadjusted only (indices are not adjusted, but
the ``00`` path keeps one convention), and per-year files so a symbol costs one
request per year rather than one per day.

Index codes are 同花顺's own, not the exchange's — 上证综指 is ``1A0001``, 沪深300
is ``399300``, 上证50 is ``1B0016``. The mapping is explicit below because it is
not derivable: some indices take the SZ-style numeric code, others a 1A/1B
prefix, and guessing silently fetches the wrong series.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import TYPE_CHECKING, Any

from cn_market_lake.adapters.ths.boards import ThsError, _get, _unwrap_jsonp

if TYPE_CHECKING:
    from cn_market_lake.config import Config

logger = logging.getLogger(__name__)

_INDEX_KLINE_URL = "https://d.10jqka.com.cn/v6/line/zs_{code}/00/{part}.js"

# Lake symbol -> 同花顺 index code. Every entry is value-checked against a known
# close, not just probed for a 200: 沪深300 reads 3731.00 on 2015-12-31, which is
# the published figure. A short series here is the index being young (创业板指
# starts 2010-06, 科创50 2019-12), not a fetch failure.
#
# 中证1000 (000852.SH) is deliberately absent. Both `399852` and `1B0852` return
# a series, but it closes 2015 at 10614 against the index's actual ~8378, and it
# starts in 2005 though the index was not published until 2014 — so whatever those
# codes point at, it is not 中证1000. A benchmark that is quietly the wrong series
# is worse than a missing one.
INDEX_CODE_MAP = {
    "000001.SH": "1A0001",  # 上证综指, from 1990-12-19
    "000016.SH": "1B0016",  # 上证50, from 2003-12-31
    "000300.SH": "399300",  # 沪深300, from 2005-01-04
    "000688.SH": "1B0688",  # 科创50, from 2019-12-31
    "000905.SH": "399905",  # 中证500, from 2007-01-15
    "399001.SZ": "399001",  # 深证成指, from 1991-04-03
    "399006.SZ": "399006",  # 创业板指, from 2010-06-01
}


def _parse_index_kline(payload: dict[str, Any], symbol: str) -> list[dict]:
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
                    # Part of the row's identity, not the caller's to supply:
                    # index_bars keys on it and the schema requires it.
                    "frequency": "1d",
                }
            )
        except ValueError:
            continue
    return rows


def fetch_index_bars_history(
    symbol: str,
    start: date,
    end: date,
    *,
    config: Config | None = None,
) -> list[dict]:
    """Daily bars for one index over ``[start, end]``.

    A year file missing is normal — the index had no base date yet — and is
    skipped; an unmapped symbol is a caller error and raises.
    """
    code = INDEX_CODE_MAP.get(symbol)
    if code is None:
        raise KeyError(f"no 同花顺 index code for {symbol}; add it to INDEX_CODE_MAP")
    rows: list[dict] = []
    seen: set[date] = set()
    for year in range(start.year, end.year + 1):
        try:
            payload = _unwrap_jsonp(
                _get(_INDEX_KLINE_URL.format(code=code, part=year), config=config)
            )
        except ThsError as exc:
            logger.debug("THS index %s (%s) %s unavailable: %s", symbol, code, year, exc)
            continue
        for row in _parse_index_kline(payload, symbol):
            if row["trade_date"] in seen or not (start <= row["trade_date"] <= end):
                continue
            seen.add(row["trade_date"])
            rows.append(row)
    rows.sort(key=lambda r: r["trade_date"])
    return rows


def sweep_index_bars(
    symbols: list[str],
    start: date,
    end: date,
    *,
    config: Config | None = None,
) -> tuple[list[dict], list[str]]:
    """Bars for several indices. Returns ``(rows, failed_symbols)``.

    Unlike the stock sweep this does not batch — there are eight indices, not
    thousands, so a failure is worth surfacing per symbol rather than absorbing.
    """
    rows: list[dict] = []
    failed: list[str] = []
    for symbol in symbols:
        try:
            got = fetch_index_bars_history(symbol, start, end, config=config)
            rows.extend(got)
            logger.info("THS index %s: %d bars", symbol, len(got))
        except Exception as exc:  # noqa: BLE001 — recorded, sweep continues
            logger.warning("THS index bars failed for %s: %s", symbol, exc)
            failed.append(symbol)
    return rows, failed
