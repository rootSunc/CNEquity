"""TDX xdxr (除权除息) → corporate_actions schema."""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cn_market_lake.adapters.tdx_protocol.session import TDX_SESSION_LOCK, close_quotes_client
from cn_market_lake.domain.rate_limit import RateLimitSpec, wait_spec

logger = logging.getLogger(__name__)

_ACTION_TYPES = {
    "cash_dividend": "cash_dividend",
    "bonus": "bonus",
    "transfer": "transfer",
    "allotment": "allotment",
}


def _rows_from_xdxr(symbol: str, pdf: pl.DataFrame) -> list[dict]:
    rows: list[dict] = []
    for record in pdf.iter_rows(named=True):
        year = record.get("year")
        month = record.get("month")
        day = record.get("day")
        if not all(v is not None for v in (year, month, day)):
            continue
        ex_date = date(int(year), int(month), int(day))
        category = int(record.get("category") or 0)
        if category != 1:
            continue

        fenhong = float(record.get("fenhong") or 0)
        songzhuangu = float(record.get("songzhuangu") or 0)
        peigu = float(record.get("peigu") or 0)
        peigujia = float(record.get("peigujia") or 0)

        if fenhong > 0:
            rows.append(
                {
                    "symbol": symbol,
                    "ex_date": ex_date,
                    "action_type": _ACTION_TYPES["cash_dividend"],
                    "cash_dividend": fenhong / 10.0,
                    "bonus_ratio": 0.0,
                    "transfer_ratio": 0.0,
                    "allotment_ratio": None,
                    "allotment_price": None,
                }
            )
        if songzhuangu > 0:
            rows.append(
                {
                    "symbol": symbol,
                    "ex_date": ex_date,
                    "action_type": _ACTION_TYPES["bonus"],
                    "cash_dividend": 0.0,
                    # per-share contract: TDX songzhuangu is 每10股 (combined
                    # 送+转); divide by 10. All total goes to bonus_ratio —
                    # xdxr does not split 送 vs 转, but total mult is exact.
                    "bonus_ratio": songzhuangu / 10.0,
                    "transfer_ratio": 0.0,
                    "allotment_ratio": None,
                    "allotment_price": None,
                }
            )
        if peigu > 0:
            rows.append(
                {
                    "symbol": symbol,
                    "ex_date": ex_date,
                    "action_type": _ACTION_TYPES["allotment"],
                    "cash_dividend": 0.0,
                    "bonus_ratio": 0.0,
                    "transfer_ratio": 0.0,
                    # per-share contract: peigu is 每10股, divide by 10.
                    # peigujia is already a per-share price — leave as-is.
                    "allotment_ratio": peigu / 10.0,
                    "allotment_price": peigujia if peigujia else None,
                }
            )
    return rows


def fetch_xdxr_for_symbol(
    client,
    symbol: str,
    *,
    rate_limit: RateLimitSpec | None = None,
    on_date: date | None = None,
) -> pl.DataFrame:
    wait_spec(rate_limit)
    code, _, exch = symbol.partition(".")
    # ``quotes.xdxr()`` falls back to ``market_for_stock()`` when market is
    # omitted, and that heuristic only distinguishes SH/SZ — it has no notion
    # of 北交所 at all, so every BJ symbol silently queried market=0 (深圳) and
    # got back an empty (not erroring) result. Confirmed live: market=0 returns
    # 0 events for every BJ code sampled; market=2 (北京) returns real ones for
    # the same codes (920002.BJ: 15 events, 920014.BJ: 34, ...). This mirrors
    # the resolution `fetch_bars_paginated` already does correctly for daily
    # bars — the fix here is applying that same pattern to xdxr.
    market = 1 if exch == "SH" else (0 if exch == "SZ" else 2)
    try:
        raw = client.xdxr(symbol=code, market=market)
    except Exception as exc:
        logger.debug("TDX xdxr failed for %s: %s", symbol, exc)
        return pl.DataFrame()

    if raw is None or len(raw) == 0:
        return pl.DataFrame()

    pdf = pl.from_pandas(raw) if hasattr(raw, "columns") else pl.DataFrame(raw)
    rows = _rows_from_xdxr(symbol, pdf)
    if on_date is not None:
        rows = [r for r in rows if r["ex_date"] == on_date]
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows)


def fetch_corporate_actions_tdx(
    symbols: list[str],
    *,
    trade_date: date | None = None,
    backfill: bool = False,
    client_factory,
    rate_limit: RateLimitSpec | None = None,
) -> pl.DataFrame:
    client = None
    frames: list[pl.DataFrame] = []
    on_date = None if backfill else trade_date
    try:
        with TDX_SESSION_LOCK:
            client = client_factory()
            for sym in symbols:
                df = fetch_xdxr_for_symbol(client, sym, rate_limit=rate_limit, on_date=on_date)
                if df.height:
                    frames.append(df)
    finally:
        close_quotes_client(client)

    if not frames:
        return pl.DataFrame(
            schema={
                "symbol": pl.Utf8,
                "ex_date": pl.Date,
                "action_type": pl.Utf8,
                "cash_dividend": pl.Float64,
                "bonus_ratio": pl.Float64,
                "transfer_ratio": pl.Float64,
                "allotment_ratio": pl.Float64,
                "allotment_price": pl.Float64,
            }
        )

    out = pl.concat(frames, how="diagonal_relaxed")
    return out.unique(subset=["symbol", "ex_date", "action_type"], keep="last")
