"""Baostock historical valuation (PE/PB/PS + market cap) for valuation_metrics.

EastMoney's valuation endpoint is a live snapshot (the clist page stamped with
today's ``trade_date``); it cannot replay history. Baostock exposes per-symbol
daily ``peTTM`` / ``pbMRQ`` / ``psTTM`` plus ``amount`` / ``turn`` / ``close``
back to 2016.

Market cap on the backfill path:

- ``float_mv`` — from k-data: ``amount / (turn/100)`` when turn > 0 (元).
  Matches EastMoney ``f21`` units.
- ``total_mv`` — ``close × totalShare`` with year-end (Q4) ``query_profit_data``
  shares asof-joined forward. Q4-only keeps the per-symbol wall clock under the
  session deadline while totalShare changes slowly.

Daily EastMoney snapshots still overwrite the latest day with live ``f20``/``f21``.
Provenance ``source="baostock"`` marks historical rows for audit.

Reliability: baostock throttles/drops a long-held session under a full-market
sweep. ``fetch_valuation_history`` retries each symbol with a fresh login +
backoff, and returns symbols that still failed so the caller can fail loud and
resume. Resume skip requires ≥80% non-null ``float_mv`` per symbol
(``_MV_FILL_DONE_RATIO`` in ``steps/fundamentals``) so a sparse fill cannot
park a decade of null market-cap rows.
"""

from __future__ import annotations

import logging
import time
from datetime import date

import polars as pl

from cn_market_lake.adapters.baostock._session import (
    fetch_per_symbol,
    to_baostock_symbol,
)

logger = logging.getLogger(__name__)

__all__ = ["fetch_valuation_history", "to_baostock_symbol"]

# amount/turn unlock float_mv; close + yearly totalShare unlock total_mv.
_FIELDS = "date,code,close,amount,turn,peTTM,pbMRQ,psTTM"

# Year-end shares only: ~11 calls/symbol vs ~44 for every quarter; totalShare
# rarely jumps intra-year enough to matter for size neutralization.
_SHARES_DEADLINE_BUDGET_YEARS = 15

_OUTPUT_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "pe_ttm": pl.Float64,
    "pb": pl.Float64,
    "ps_ttm": pl.Float64,
    "total_mv": pl.Float64,
    "float_mv": pl.Float64,
}


def _to_float(raw: str | None) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _float_mv_from_turn(amount: float | None, turn: float | None) -> float | None:
    """流通市值 ≈ 成交额 / (换手率/100). turn is percent; empty on suspension."""
    if amount is None or turn is None or turn <= 0 or amount <= 0:
        return None
    return amount / (turn / 100.0)


def _year_end_total_shares(bs, symbol: str, start: date, end: date) -> list[tuple[date, float]]:
    """``(stat_date, totalShare_股)`` from Q4 profit rows in ``[start.year, end.year]``."""
    code = to_baostock_symbol(symbol)
    out: list[tuple[date, float]] = []
    years = range(start.year - 1, end.year + 1)
    year_list = list(years)
    if len(year_list) > _SHARES_DEADLINE_BUDGET_YEARS:
        year_list = list(range(end.year - _SHARES_DEADLINE_BUDGET_YEARS + 2, end.year + 1))
        # Keep one prior year so January dates still asof to previous Q4.
        year_list = [year_list[0] - 1, *year_list] if year_list else year_list
    for year in year_list:
        try:
            rs = bs.query_profit_data(code=code, year=year, quarter=4)
        except Exception as exc:  # noqa: BLE001 — treat like empty; k-data still usable
            logger.warning("baostock profit Q4 failed for %s %s: %s", symbol, year, exc)
            continue
        if getattr(rs, "error_code", "0") != "0":
            continue
        fields = list(getattr(rs, "fields", []) or [])
        while rs.next():
            row = rs.get_row_data()
            data = dict(zip(fields, row, strict=False)) if fields else {}
            if not data and len(row) >= 11:
                # Offline fakes may omit .fields; positional fallback per baostock order.
                data = {
                    "statDate": row[2],
                    "totalShare": row[9],
                }
            shares = _to_float(data.get("totalShare"))
            stat_raw = data.get("statDate") or f"{year}-12-31"
            if shares is None or shares <= 0:
                continue
            try:
                stat = date.fromisoformat(str(stat_raw)[:10])
            except ValueError:
                stat = date(year, 12, 31)
            out.append((stat, shares))
    out.sort(key=lambda x: x[0])
    return out


def _asof_total_share(trade: date, share_points: list[tuple[date, float]]) -> float | None:
    """Latest totalShare with ``stat_date <= trade`` (forward-filled from Q4)."""
    chosen: float | None = None
    for stat, shares in share_points:
        if stat <= trade:
            chosen = shares
        else:
            break
    return chosen


def _year_windows(start: date, end: date) -> list[tuple[date, date]]:
    """Inclusive calendar-year slices — baostock multi-year k-data reads hang."""
    out: list[tuple[date, date]] = []
    for year in range(start.year, end.year + 1):
        w_start = max(start, date(year, 1, 1))
        w_end = min(end, date(year, 12, 31))
        if w_start <= w_end:
            out.append((w_start, w_end))
    return out


def _fetch_one(bs, symbol: str, start: date, end: date) -> list[dict] | None:
    """Rows for one symbol, or ``None`` if the k-data query errored (retryable).

    An ``error_code == '0'`` result with zero rows is a legitimate empty
    (delisted before the window, no baostock coverage) — returns ``[]``, not
    ``None``, so the caller does not treat it as a failure to retry.

    K-data is fetched in calendar-year chunks: a single 2016→today query often
    stalls mid-``rs.next()`` (baostock slowloris); yearly windows stay reliable.
    """
    code = to_baostock_symbol(symbol)
    raw_rows: list[list[str]] = []
    for w_start, w_end in _year_windows(start, end):
        rs = bs.query_history_k_data_plus(
            code,
            _FIELDS,
            start_date=w_start.isoformat(),
            end_date=w_end.isoformat(),
            frequency="d",
            adjustflag="3",  # unadjusted; PE/PB/PS ratios are adjust-independent
        )
        if getattr(rs, "error_code", "0") != "0":
            return None
        # Materialize before the next baostock call — a second query can
        # invalidate the live result-set cursor on the shared socket.
        while rs.next():
            raw_rows.append(list(rs.get_row_data()))

    share_points = _year_end_total_shares(bs, symbol, start, end) if raw_rows else []

    out: list[dict] = []
    for row in raw_rows:
        trade_raw, _code, close_s, amount_s, turn_s, pe, pb, ps = row
        close = _to_float(close_s)
        amount = _to_float(amount_s)
        turn = _to_float(turn_s)
        float_mv = _float_mv_from_turn(amount, turn)
        trade = date.fromisoformat(trade_raw)
        total_share = _asof_total_share(trade, share_points)
        total_mv = (
            close * total_share
            if close is not None and total_share is not None and close > 0
            else None
        )
        out.append(
            {
                "symbol": symbol,
                "trade_date": trade,
                "pe_ttm": _to_float(pe),
                "pb": _to_float(pb),
                "ps_ttm": _to_float(ps),
                "total_mv": total_mv,
                "float_mv": float_mv,
            }
        )
    return out


def fetch_valuation_history(
    symbols: list[str],
    start: date,
    end: date,
    *,
    bs=None,
    sleep=time.sleep,
    config=None,
) -> tuple[pl.DataFrame, list[str]]:
    """Per-symbol daily PE/PB/PS + market cap from baostock over ``[start, end]``.

    Returns ``(dataframe, failed_symbols)``. Fail-loud on login failure. Each
    symbol is retried up to ``_MAX_RETRIES`` times with a fresh session + backoff
    on a query error; symbols still failing are returned in ``failed_symbols``.

    ``bs`` / ``sleep`` / ``config`` are injectable for offline tests. Pass
    ``config`` in production so ``[sources.baostock]`` pacing applies.
    """
    rows, failed = fetch_per_symbol(
        symbols,
        start,
        end,
        _fetch_one,
        bs=bs,
        sleep=sleep,
        label="baostock valuation",
        # Decade of year-chunked k-data + ~11 Q4 profit calls; allow headroom
        # above the 30s socket timeout so a slow-but-alive fetch is not killed.
        deadline=300.0,
        config=config,
    )
    df = pl.DataFrame(rows, schema=_OUTPUT_SCHEMA) if rows else pl.DataFrame(schema=_OUTPUT_SCHEMA)
    return df, failed
