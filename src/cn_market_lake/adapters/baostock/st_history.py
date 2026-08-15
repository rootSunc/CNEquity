"""Baostock historical ST labels — backfill source for trading_status (C4).

The daily ``trading_status`` step gets ST flags from EastMoney, which
only expose *today's* ST list — so ST labels in the lake start at the first live
run (2026-07), leaving every earlier backtest window with survivorship /
look-ahead bias (``universe="all_a"`` does not drop names that were ST then).

Baostock's k-data carries a per-day ``isST`` flag back to 2016, so a per-symbol
sweep reconstructs the historical ST label. ``isST`` is binary — it does not
split "ST" from "*ST" — so every ST day maps to ``status="st"``; that is enough
for the universe filter (``EXCLUDED_STATUSES`` covers both). Every traded day
is emitted, including ``status="normal"`` as explicit negative evidence. A
missing row therefore remains unknown rather than being silently interpreted
as non-ST. Suspension is reconstructed separately from bar gaps.
"""

from __future__ import annotations

import time
from datetime import date

import polars as pl

from cn_market_lake.adapters.baostock._session import (
    fetch_per_symbol,
    to_baostock_symbol,
)

__all__ = ["fetch_st_history", "to_baostock_symbol"]

# baostock k-data fields: trading status (1=trading) and the ST flag (1=ST).
_ST_FIELDS = "date,code,tradestatus,isST"

# trading_status columns minus provenance (added by write_fetched).
_OUTPUT_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "is_trading": pl.Boolean,
    "status": pl.Utf8,
}


def _fetch_one_st(bs, symbol: str, start: date, end: date) -> list[dict] | None:
    """Trading-day ST/normal evidence, or ``None`` on a retryable error.

    Unexpected ``isST`` vocabulary fails the entire symbol closed. Treating an
    unknown value as ``normal`` would manufacture negative evidence.
    """
    rs = bs.query_history_k_data_plus(
        to_baostock_symbol(symbol),
        _ST_FIELDS,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        frequency="d",
        adjustflag="3",  # ST flag is adjust-independent
    )
    if getattr(rs, "error_code", "0") != "0":
        return None
    out: list[dict] = []
    while rs.next():
        trade_raw, _code, tradestatus, is_st = rs.get_row_data()
        if tradestatus != "1":
            continue
        if is_st not in ("0", "1"):
            return None
        out.append(
            {
                "symbol": symbol,
                "trade_date": date.fromisoformat(trade_raw),
                "is_trading": True,
                "status": "st" if is_st == "1" else "normal",
            }
        )
    return out


def fetch_st_history(
    symbols: list[str],
    start: date,
    end: date,
    *,
    bs=None,
    sleep=time.sleep,
    config=None,
) -> tuple[pl.DataFrame, list[str]]:
    """Per-symbol historical ST/normal evidence over ``[start, end]``.

    Returns ``(dataframe, failed_symbols)``. Fail-loud on login failure; each
    symbol is retried with a fresh session + backoff and the still-failing ones
    are returned so the caller can surface them and resume. A traded symbol
    that was never ST contributes explicit ``normal`` rows; a symbol with no
    trading sessions in the requested window contributes zero rows.

    ``bs`` / ``sleep`` / ``config`` are injectable for offline tests. Pass
    ``config`` in production for ``[sources.baostock]`` pacing.
    """
    rows, failed = fetch_per_symbol(
        symbols,
        start,
        end,
        _fetch_one_st,
        bs=bs,
        sleep=sleep,
        label="baostock ST",
        config=config,
    )
    df = pl.DataFrame(rows, schema=_OUTPUT_SCHEMA) if rows else pl.DataFrame(schema=_OUTPUT_SCHEMA)
    return df, failed
