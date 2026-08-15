"""Daily bars for stocks that have since delisted — the survivorship repair.

`instruments` is a current-roster snapshot, so a stock that delisted in 2019 is
absent from it and has no bars in the lake at all. Not a few missing days: the
whole symbol is gone. Measured against baostock's historical rosters, the lake
holds 83.2% of the stocks that actually traded on 2016-06-30, 94.0% on
2020-06-30, and 99.6% on 2026-06-30 — a clean survivorship curve, and the reason
CLAUDE.md marks the 2020–24 research window `incomplete` and every return and IC
measured on it biased upward.

The live vendors cannot fix this: 同花顺 returns empty for every delisted code
tested, across exchanges and delisting years. baostock can, and serves each one
through to its final session (康得退 to 2021-05-31, 乐视退 to 2020-07-21). Sina
still publishes their adjustment factors, so the recovered bars go through the
same hfq derivation as everything else.

Unadjusted prices, as everywhere else in the lake (`adjustflag="3"`).

``volume`` is already in 股, the lake's unit, so it passes through unconverted:
``amount / close / volume`` has a median of 1.000 over the 374,888 curated
baostock rows. The TDX path reports 手 and multiplies by 100 — that difference
is real, not an inconsistency to iron out. See :mod:`cn_market_lake.domain.units`.
"""

from __future__ import annotations

import logging
from datetime import date

from cn_market_lake.adapters.baostock._session import fetch_per_symbol, import_baostock

logger = logging.getLogger(__name__)

_FIELDS = "date,open,high,low,close,volume,amount,tradestatus"


def _is_stock(bs_code: str) -> bool:
    """Stocks only — baostock's roster also carries indices.

    Shanghai 000xxx is an index (000001 is the composite), Shenzhen 000xxx is a
    stock, so the prefix has to be read per exchange.
    """
    try:
        ex, code = bs_code.split(".")
    except ValueError:
        return False
    if ex == "sh":
        return code.startswith(("60", "688"))
    if ex == "sz":
        return code.startswith(("00", "30"))
    return False


def to_lake_symbol(bs_code: str) -> str:
    ex, code = bs_code.split(".")
    return f"{code}.{'SH' if ex == 'sh' else 'SZ'}"


def roster_on(day: date, *, bs=None, login: bool = True) -> set[str]:
    """Stock codes that actually traded on *day*, in lake symbol form.

    This is the ground truth the current roster cannot provide: it includes
    names that have delisted since.

    Logs in by default — baostock answers an unauthenticated query with an empty
    result rather than an error, so skipping the login would silently return "no
    stocks traded that day" and understate the gap to zero. Pass ``login=False``
    only when the caller already holds a session.
    """
    from cn_market_lake.adapters.baostock._session import _login

    bs = bs or import_baostock()
    if login:
        _login(bs)
    try:
        rs = bs.query_all_stock(day=day.isoformat())
        out: set[str] = set()
        while rs.next():
            code = rs.get_row_data()[0]
            if _is_stock(code):
                out.add(to_lake_symbol(code))
        if not out:
            logger.warning("baostock roster for %s came back empty", day)
        return out
    finally:
        if login:
            bs.logout()


def _fetch_one(bs, symbol: str, start: date, end: date) -> list[dict] | None:
    from cn_market_lake.adapters.baostock._session import to_baostock_symbol

    rs = bs.query_history_k_data_plus(
        to_baostock_symbol(symbol),
        _FIELDS,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        frequency="d",
        adjustflag="3",  # unadjusted; hfq is derived from Sina factors
    )
    if rs.error_code != "0":
        return None  # retryable — the session driver relogins and retries
    rows: list[dict] = []
    while rs.next():
        r = rs.get_row_data()
        # A suspended session comes back with empty price fields; skip rather
        # than write zeros, which would read as a real -100% move.
        if not r[1] or not r[4]:
            continue
        try:
            rows.append(
                {
                    "symbol": symbol,
                    "trade_date": date.fromisoformat(r[0]),
                    "open": float(r[1]),
                    "high": float(r[2]),
                    "low": float(r[3]),
                    "close": float(r[4]),
                    "volume": int(float(r[5] or 0)),
                    "amount": float(r[6] or 0.0),
                }
            )
        except (ValueError, IndexError):
            continue
    return rows


def fetch_delisted_bars(
    symbols: list[str],
    start: date,
    end: date,
    *,
    config=None,
    bs=None,
) -> tuple[list[dict], list[str]]:
    """Bars for recovered symbols. Returns ``(rows, failed_symbols)``."""
    return fetch_per_symbol(
        symbols,
        start,
        end,
        _fetch_one,
        bs=bs,
        label="baostock delisted bars",
        config=config,
    )
