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
is real, not an inconsistency to iron out. See :mod:`cnequity.domain.units`.
"""

from __future__ import annotations

import logging
import math
from datetime import date

from cnequity.adapters.baostock._session import fetch_per_symbol, import_baostock
from cnequity.adapters.numeric import finite_int64
from cnequity.domain.rate_limit import source_request

logger = logging.getLogger(__name__)

# Include the response identity. Baostock normally honors the requested code,
# but a misrouted result must never be relabeled as a delisted symbol.
_FIELDS = "date,code,open,high,low,close,volume,amount,tradestatus"


def _is_stock(bs_code: str) -> bool:
    """Stocks only — baostock's roster also carries indices.

    Shanghai 000xxx is an index (000001 is the composite), Shenzhen 000xxx is a
    stock, so the prefix has to be read per exchange.
    """
    if not isinstance(bs_code, str):
        return False
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


def roster_on(day: date, *, bs=None, login: bool = True, config=None) -> set[str]:
    """Stock codes that actually traded on *day*, in lake symbol form.

    This is the ground truth the current roster cannot provide: it includes
    names that have delisted since.

    Logs in by default — baostock answers an unauthenticated query with an empty
    result rather than an error, so skipping the login would silently return "no
    stocks traded that day" and understate the gap to zero. Pass ``login=False``
    only when the caller already holds a session.
    """
    from cnequity.adapters.baostock._session import _login, _logout

    bs = bs or import_baostock()
    if login:
        _login(bs, config=config)
    try:
        with source_request(config, "baostock"):
            rs = bs.query_all_stock(day=day.isoformat())
        if getattr(rs, "error_code", "0") != "0":
            message = getattr(rs, "error_msg", "") or "unknown error"
            raise RuntimeError(
                f"baostock historical roster query failed for {day}: {rs.error_code} ({message})"
            )
        out: set[str] = set()
        while rs.next():
            row = rs.get_row_data()
            if not row:
                continue
            code = row[0]
            if _is_stock(code):
                out.add(to_lake_symbol(code))
        if not out:
            logger.warning("baostock roster for %s came back empty", day)
        return out
    finally:
        if login:
            try:
                _logout(bs, config=config)
            except Exception as exc:  # noqa: BLE001 — retain the completed roster
                logger.warning("baostock roster logout failed: %s", exc)


def _fetch_one(bs, symbol: str, start: date, end: date, *, config=None) -> list[dict] | None:
    from cnequity.adapters.baostock._session import to_baostock_symbol

    with source_request(config, "baostock"):
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
    identity_mismatches = 0
    reported_fields = list(getattr(rs, "fields", []) or [])
    expected_code = to_baostock_symbol(symbol)
    while rs.next():
        r = rs.get_row_data()
        if len(r) == 9:
            (
                trade_raw,
                reported_code,
                open_raw,
                high_raw,
                low_raw,
                close_raw,
                volume_raw,
                amount_raw,
                status,
            ) = r
            if str(reported_code).strip().lower() != expected_code:
                identity_mismatches += 1
                logger.warning(
                    "baostock delisted bars: skipping row for %s returned as %s",
                    symbol,
                    reported_code,
                )
                continue
        elif len(r) == 8 and not reported_fields:
            # Keep compatibility with old offline fakes that predate `code` in
            # the requested field list; a real result-set with missing `code`
            # must not be attributed to the requested symbol.
            trade_raw, open_raw, high_raw, low_raw, close_raw, volume_raw, amount_raw, status = r
        else:
            if len(r) == 8 and reported_fields:
                identity_mismatches += 1
                logger.warning(
                    "baostock delisted bars: response for %s omitted the code field; retrying",
                    symbol,
                )
            continue
        if status not in ("0", "1"):
            return None
        if status != "1":
            continue
        # A suspended session comes back with empty price fields; skip rather
        # than write zeros, which would read as a real -100% move. The status
        # guard above also rejects carried-forward prices on a suspended day.
        if not open_raw or not close_raw:
            continue
        try:
            trade_date = date.fromisoformat(trade_raw)
            if trade_date < start or trade_date > end:
                continue
            open_ = float(open_raw)
            high = float(high_raw)
            low = float(low_raw)
            close = float(close_raw)
            # Volume is a required bar field: an empty value is unknown, not a
            # zero-volume suspension. Turnover is nullable in the lake because
            # some sources do not publish it; preserve an empty Baostock value
            # as null instead of manufacturing zero turnover.
            if volume_raw is None or str(volume_raw).strip() in {"", "-"}:
                continue
            volume = finite_int64(float(volume_raw), minimum=0)
            if amount_raw is None or str(amount_raw).strip() in {"", "-"}:
                amount = None
            else:
                amount = float(amount_raw)
                if not math.isfinite(amount):
                    continue
            if not all(math.isfinite(value) for value in (open_, high, low, close)):
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "trade_date": trade_date,
                    "open": open_,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": volume,
                    "amount": amount,
                }
            )
        except (TypeError, ValueError, IndexError, OverflowError):
            continue
    if identity_mismatches:
        logger.warning(
            "baostock delisted bars: response for %s contained another code; retrying",
            symbol,
        )
        return None
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

    def fetch_one(bs_session, symbol: str, window_start: date, window_end: date):
        return _fetch_one(bs_session, symbol, window_start, window_end, config=config)

    return fetch_per_symbol(
        symbols,
        start,
        end,
        fetch_one,
        bs=bs,
        label="baostock delisted bars",
        config=config,
        request_managed=True,
    )
