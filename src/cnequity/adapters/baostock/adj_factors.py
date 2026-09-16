"""Adjustment factors reconstructed from Baostock's own adjusted prices.

`adj_factors` is the one core dataset with a single vendor behind it: Sina, and
nothing else. Sina bans by account rather than by endpoint, so the same HTTP 456
that stops a futures sweep also stops the factor fetch, and a dataset every
return in the lake depends on has nowhere to go.

Baostock does not publish factors, but it publishes the same series twice — raw
(`adjustflag=3`) and back-adjusted (`adjustflag=1`) — and their ratio is the
factor, in exactly the multiplier convention this lake stores
(``adj_price = raw * factor``). It is then rescaled to 1.0 at the newest day in
the window, which is the level convention Sina emits.

Measured against Sina on 600519.SH across its 2026-06-26 ex-date: Baostock's
implied step was 0.976883 against Sina's 0.976880 — a 3e-6 disagreement that is
the rounding in Baostock's two-decimal adjusted closes, not a difference of
opinion about the corporate action.

Independent in the way that matters: a different vendor, a different protocol
and a different rate-limit budget from the one that is out.
"""

from __future__ import annotations

import logging
import math
from datetime import date

import polars as pl

from cnequity.adapters.baostock._session import fetch_per_symbol, to_baostock_symbol
from cnequity.domain.rate_limit import source_request

logger = logging.getLogger(__name__)

SOURCE = "baostock"
#: Raw and back-adjusted, in that order: the ratio is the factor.
_RAW_FLAG = "3"
_HFQ_FLAG = "1"
_FIELDS = "date,close"


class BaostockAdjFactorUnavailableError(RuntimeError):
    """Baostock has no usable adjusted series for this symbol."""


def _closes(bs, symbol: str, start: date, end: date, flag: str, *, config) -> dict[date, float]:
    with source_request(config, SOURCE):
        rs = bs.query_history_k_data_plus(
            to_baostock_symbol(symbol),
            _FIELDS,
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            frequency="d",
            adjustflag=flag,
        )
    if rs.error_code != "0":
        return {}
    out: dict[date, float] = {}
    while rs.next():
        row = rs.get_row_data()
        if len(row) < 2:
            continue
        raw_date, raw_close = row[0], row[1]
        if not raw_close:
            continue
        try:
            close = float(raw_close)
            if not math.isfinite(close) or close <= 0:
                continue
            out[date.fromisoformat(raw_date)] = close
        except (TypeError, ValueError):
            continue
    return out


def fetch_adj_factor_series_baostock(
    symbol: str,
    start: date,
    end: date,
    *,
    config=None,
    bs=None,
) -> pl.DataFrame:
    """Per-day ``(trade_date, factor)`` for *symbol*, rescaled to 1.0 at the tip.

    Raises `BaostockAdjFactorUnavailableError` when the vendor cannot answer, so
    a caller can tell "no factors here" from "this symbol has none" — the same
    distinction Sina's own error type carries. Beijing is not covered by this
    vendor at all and is refused before any request.
    """
    if symbol.upper().endswith(".BJ"):
        raise BaostockAdjFactorUnavailableError(f"baostock has no Beijing coverage for {symbol}")

    captured: dict[str, dict[date, float]] = {}

    def _fetch_one(bs_session, sym: str, window_start: date, window_end: date):
        raw = _closes(bs_session, sym, window_start, window_end, _RAW_FLAG, config=config)
        hfq = _closes(bs_session, sym, window_start, window_end, _HFQ_FLAG, config=config)
        if not raw or not hfq:
            # Empty from one side only is a partial answer, which is worse than
            # none: it would rescale against a tip the other series never saw.
            return None
        captured["raw"] = raw
        captured["hfq"] = hfq
        return [{"symbol": sym}]

    _rows, failed = fetch_per_symbol(
        [symbol],
        start,
        end,
        _fetch_one,
        bs=bs,
        label="baostock adj factors",
        config=config,
        request_managed=True,
    )
    if failed or "raw" not in captured:
        raise BaostockAdjFactorUnavailableError(
            f"baostock returned no adjusted series for {symbol}"
        )

    raw, hfq = captured["raw"], captured["hfq"]
    shared = sorted(set(raw) & set(hfq))
    if not shared:
        raise BaostockAdjFactorUnavailableError(
            f"baostock raw and adjusted series for {symbol} share no session"
        )

    implied = {day: hfq[day] / raw[day] for day in shared}
    anchor = implied[shared[-1]]
    if not math.isfinite(anchor) or anchor <= 0:
        raise BaostockAdjFactorUnavailableError(f"baostock tip factor for {symbol} is unusable")

    return pl.DataFrame(
        {
            "trade_date": shared,
            # Sina's level convention: 1.0 at the newest session, below it going
            # back. Without this the two vendors would disagree by a constant
            # and no cross-check could tell that from a real discrepancy.
            "factor": [implied[day] / anchor for day in shared],
        },
        schema={"trade_date": pl.Date, "factor": pl.Float64},
    )
