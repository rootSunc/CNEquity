"""Baostock historical valuation (PE/PB/PS + market cap) for valuation_metrics.

EastMoney's valuation endpoint is a live snapshot (the clist page stamped with
today's ``trade_date``); it cannot replay history. Baostock exposes per-symbol
daily ``peTTM`` / ``pbMRQ`` / ``psTTM`` plus ``volume`` / ``turn`` / ``close``
back to 2016.

Market cap on the backfill path, each value labelled with its basis
(``domain/valuation``):

- ``float_mv`` — ``close * volume / (turn/100)`` when turn > 0 (元): the
  closing price times the float that baostock's turnover ratio implies.
  ``amount / (turn/100)``, used until 2026-09, priced that float at the day's
  VWAP instead of the close.
- ``total_mv`` — ``close × totalShare`` with year-end (Q4) ``query_profit_data``
  shares asof-joined forward, an estimate that misses intra-year share changes.
  ``shares_as_of`` names the share count used. Q4-only keeps the per-symbol
  wall clock under the session deadline.

Daily EastMoney snapshots still overwrite the latest day with vendor values.

Reliability: baostock throttles/drops a long-held session under a full-market
sweep. ``fetch_valuation_history`` retries each symbol with a fresh login +
backoff, and returns symbols that still failed so the caller can fail loud and
resume. Resume skip requires ≥80% non-null ``float_mv`` per symbol
(``_MV_FILL_DONE_RATIO`` in ``steps/fundamentals``) so a sparse fill cannot
park a decade of null market-cap rows.
"""

from __future__ import annotations

import logging
import math
import time
from datetime import date

import polars as pl

from cnequity.adapters.baostock._session import (
    check_result,
    fetch_per_symbol,
    to_baostock_symbol,
)
from cnequity.adapters.baostock.wide_history import query_history
from cnequity.domain.http_policy import SourceCoolingDown
from cnequity.domain.rate_limit import source_request
from cnequity.domain.valuation import (
    FLOAT_MV_TURN_IMPLIED,
    TOTAL_MV_YEAR_END_ESTIMATE,
)
from cnequity.domain.value_rules import apply_value_rules

logger = logging.getLogger(__name__)

__all__ = ["fetch_valuation_history", "to_baostock_symbol"]

# Closing price times circulating shares estimates closing market cap.
_FIELDS = "date,code,close,volume,turn,peTTM,pbMRQ,psTTM"

# Year-end shares only: ~11 calls/symbol vs ~44 for every quarter; totalShare
# rarely jumps intra-year enough to matter for size neutralization.
_SHARES_DEADLINE_BUDGET_YEARS = 15

_OUTPUT_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "pe_ttm": pl.Float64,
    "pe_dynamic": pl.Float64,
    "pb": pl.Float64,
    "ps_ttm": pl.Float64,
    "total_mv": pl.Float64,
    "float_mv": pl.Float64,
    "total_mv_basis": pl.Utf8,
    "float_mv_basis": pl.Utf8,
    "shares_as_of": pl.Date,
    # Unadjusted close, for rebuilding total_mv from share_structure before the
    # row is staged; not a valuation_metrics column.
    "close": pl.Float64,
}


def _to_float(raw: str | None) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        parsed = float(raw)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _float_mv_from_turn(
    close: float | None, volume: float | None, turn: float | None
) -> float | None:
    """Closing market cap in yuan; volume is shares, turn is percent."""
    if any(value is None or value <= 0 for value in (close, volume, turn)):
        return None
    return close * volume / (turn / 100.0)


def _year_end_total_shares(
    bs, symbol: str, start: date, end: date, *, config=None
) -> list[tuple[date, float]]:
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
            with source_request(config, "baostock"):
                rs = check_result(
                    bs.query_profit_data(code=code, year=year, quarter=4), config=config
                )
        except SourceCoolingDown:
            raise
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
                    "code": row[0],
                    "statDate": row[2],
                    "totalShare": row[9],
                }
            reported_code = data.get("code")
            if reported_code is not None and str(reported_code).strip().lower() != code:
                raise RuntimeError(
                    f"baostock profit data for {symbol} returned another code: {reported_code}"
                )
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


def _asof_total_share(
    trade: date, share_points: list[tuple[date, float]]
) -> tuple[date, float] | None:
    """Latest ``(stat_date, totalShare)`` with ``stat_date <= trade`` (forward-filled from Q4)."""
    chosen: tuple[date, float] | None = None
    for point in share_points:
        if point[0] <= trade:
            chosen = point
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


def _fetch_one(bs, symbol: str, start: date, end: date, *, config=None) -> list[dict] | None:
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
        rs = query_history(
            bs,
            code,
            _FIELDS,
            start_date=w_start.isoformat(),
            end_date=w_end.isoformat(),
            frequency="d",
            adjustflag="3",
            config=config,
        )
        if getattr(rs, "error_code", "0") != "0":
            return None
        # Materialize before the next baostock call — a second query can
        # invalidate the live result-set cursor on the shared socket.
        while rs.next():
            raw_rows.append(list(rs.get_row_data()))

    share_points = _year_end_total_shares(bs, symbol, start, end, config=config) if raw_rows else []

    out: list[dict] = []
    identity_mismatches = 0
    for index, row in enumerate(raw_rows):
        if len(row) != 8:
            logger.warning(
                "baostock valuation: skipping row %s for %s with %s fields",
                index,
                symbol,
                len(row),
            )
            continue
        trade_raw, _code, close_s, volume_s, turn_s, pe, pb, ps = row
        if str(_code).strip().lower() != code:
            identity_mismatches += 1
            logger.warning(
                "baostock valuation: skipping row %s for %s returned as %s",
                index,
                symbol,
                _code,
            )
            continue
        close = _to_float(close_s)
        volume = _to_float(volume_s)
        turn = _to_float(turn_s)
        float_mv = _float_mv_from_turn(close, volume, turn)
        try:
            trade = date.fromisoformat(trade_raw)
        except (TypeError, ValueError):
            continue
        if trade < start or trade > end:
            continue
        share_point = _asof_total_share(trade, share_points)
        total_mv = (
            close * share_point[1]
            if close is not None and share_point is not None and close > 0
            else None
        )
        out.append(
            {
                "symbol": symbol,
                "trade_date": trade,
                "pe_ttm": _to_float(pe),
                "pe_dynamic": None,
                "pb": _to_float(pb),
                "ps_ttm": _to_float(ps),
                "total_mv": total_mv,
                "float_mv": float_mv,
                "total_mv_basis": TOTAL_MV_YEAR_END_ESTIMATE if total_mv is not None else None,
                "float_mv_basis": FLOAT_MV_TURN_IMPLIED if float_mv is not None else None,
                "shares_as_of": share_point[0] if total_mv is not None else None,
                "close": close,
            }
        )
    if identity_mismatches:
        logger.warning(
            "baostock valuation: response for %s contained another code; retrying",
            symbol,
        )
        return None
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

    def fetch_one(bs_session, symbol: str, window_start: date, window_end: date):
        return _fetch_one(bs_session, symbol, window_start, window_end, config=config)

    rows, failed = fetch_per_symbol(
        symbols,
        start,
        end,
        fetch_one,
        bs=bs,
        sleep=sleep,
        label="baostock valuation",
        # Decade of year-chunked k-data + ~11 Q4 profit calls; allow headroom
        # above the 30s socket timeout so a slow-but-alive fetch is not killed.
        deadline=300.0,
        config=config,
        request_managed=True,
    )
    df = pl.DataFrame(rows, schema=_OUTPUT_SCHEMA) if rows else pl.DataFrame(schema=_OUTPUT_SCHEMA)
    if not df.is_empty():
        df = df.unique(subset=["symbol", "trade_date"], keep="last").sort(["trade_date", "symbol"])
    return apply_value_rules(df, "valuation_metrics"), failed
