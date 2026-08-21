"""EastMoney daily bars: tip clist snapshot + historical kline.

Both endpoints report volume in 手 (kline field ``f56``, clist field ``f5``);
the lake stores 股, so both convert here. See :mod:`cnequity.domain.units`.

Unlike the other bar sources, this one is **not** confirmed against curated
data: ``push2his`` is unreachable from many networks (the Sina adapter's
docstring says the same), and the only EastMoney rows in the lake are all-zero
suspension placeholders, so ``amount / close / volume`` has nothing to measure.
The 手 reading comes from the same endpoint and field position that
``commodity_bars`` already documents as 东财口径. If it turns out to be wrong,
``daily_bars_volume_unit`` fires on the first real row rather than letting a
100× error land quietly.
"""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cnequity.adapters.eastmoney.clist import clist_rows_to_symbols, fetch_clist_pages
from cnequity.adapters.eastmoney.common import _to_float, parse_em_ymd
from cnequity.adapters.eastmoney.em_auth import EastMoneyClient
from cnequity.adapters.numeric import finite_int64
from cnequity.domain.symbols import parse_symbol
from cnequity.domain.units import lots_to_shares

logger = logging.getLogger(__name__)

_KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
_MARKET = {"SH": "1", "SZ": "0", "BJ": "2"}

# push2 clist live quote fields → tip OHLC. There is no trade_date in the
# payload; callers stamp the run's session date (same pattern as fund_flow).
_CLIST_BAR_FIELDS = "f12,f13,f17,f15,f16,f2,f5,f6"


def _secid(symbol: str) -> str:
    info = parse_symbol(symbol)
    return f"{_MARKET.get(info.exchange, '0')}.{info.code}"


def _volume_shares(volume: float | None) -> int | None:
    if volume is None:
        return None
    try:
        lots = finite_int64(volume, minimum=0, maximum=(2**63 - 1) // 100)
    except ValueError:
        return None
    return lots_to_shares(lots)


def fetch_daily_bars_clist(
    trade_date: date,
    *,
    symbols: set[str] | list[str] | None = None,
    client: EastMoneyClient | None = None,
    config=None,
) -> pl.DataFrame:
    """Full-market tip bars from push2 clist (~54 pages), stamped *trade_date*.

    Clist is a live cross-section: it cannot invent history. Optional *symbols*
    keeps only gap-fill keys so a later compact ``keep=last`` cannot overwrite
    rows the primary source already staged for the same PK.
    """
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)
    try:
        want = set(symbols) if symbols is not None else None
        rows_raw = fetch_clist_pages(client, fields=_CLIST_BAR_FIELDS)
        rows: list[dict] = []
        rejected = 0
        for sym, item in clist_rows_to_symbols(rows_raw):
            if want is not None and sym not in want:
                continue
            open_ = _to_float(item.get("f17"))
            high = _to_float(item.get("f15"))
            low = _to_float(item.get("f16"))
            close = _to_float(item.get("f2"))
            vol = _to_float(item.get("f5"))
            amount = _to_float(item.get("f6"))
            volume = _volume_shares(vol)
            # A clist row with zero OHLC is an unavailable quote, not a valid
            # traded bar.  EastMoney includes retired/newly-issued names in
            # the live roster, so accepting it here leaks price=0 rows into
            # curated storage and makes every downstream bar query fail the
            # numeric market-data contract.
            if None in (open_, high, low, close, volume) or any(
                value <= 0 for value in (open_, high, low, close)
            ):
                rejected += 1
                continue
            rows.append(
                {
                    "symbol": sym,
                    "trade_date": trade_date,
                    "open": float(open_),
                    "high": float(high),
                    "low": float(low),
                    "close": float(close),
                    "volume": volume,
                    # EastMoney normally supplies amount, but keep an unavailable
                    # value null rather than manufacturing zero turnover.
                    "amount": amount,
                }
            )
        if rejected:
            logger.warning(
                "EastMoney clist bars: rejected %s row(s) with missing/invalid OHLCV",
                rejected,
            )
    finally:
        if owns:
            client.close()
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["symbol", "trade_date"], keep="last")


def fetch_daily_bars_with_status(
    symbols: list[str],
    start: date,
    end: date,
    *,
    client: EastMoneyClient | None = None,
    config=None,
) -> tuple[pl.DataFrame, list[str]]:
    """Per-symbol historical kline (slow). Prefer :func:`fetch_daily_bars_clist` for tip.

    Returns ``(df, fetch_failed_symbols)``. ``fetch_failed_symbols`` holds symbols
    whose request raised (transport/HTTP error); a symbol that returns an empty
    kline series is legitimate (suspension/halt) and is *not* counted here, so
    callers can tell a batch outage apart from ordinary missing sessions.
    """
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)

    beg = start.strftime("%Y%m%d")
    end_s = end.strftime("%Y%m%d")
    rows: list[dict] = []
    rejected = 0
    fetch_failed: list[str] = []
    try:
        for sym in symbols:
            params = {
                "secid": _secid(sym),
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                "klt": "101",
                "fqt": "0",
                "beg": beg,
                "end": end_s,
            }
            try:
                resp = client.get(_KLINE_URL, params=params)
                resp.raise_for_status()
                klines = (resp.json().get("data") or {}).get("klines") or []
            except Exception as exc:
                logger.warning("EastMoney kline failed for %s: %s", sym, exc)
                fetch_failed.append(sym)
                continue

            for line in klines:
                parts = str(line).split(",")
                if len(parts) < 7:
                    rejected += 1
                    continue
                try:
                    trade_date = parse_em_ymd(parts[0])
                except (TypeError, ValueError):
                    rejected += 1
                    continue
                if trade_date < start or trade_date > end:
                    continue
                open_ = _to_float(parts[1])
                close = _to_float(parts[2])
                high = _to_float(parts[3])
                low = _to_float(parts[4])
                volume = _to_float(parts[5])
                amount = _to_float(parts[6])
                volume_shares = _volume_shares(volume)
                if None in (open_, close, high, low, volume_shares):
                    rejected += 1
                    continue
                rows.append(
                    {
                        "symbol": sym,
                        "trade_date": trade_date,
                        "open": open_,
                        "close": close,
                        "high": high,
                        "low": low,
                        "volume": volume_shares,
                        # EastMoney may omit turnover on a valid zero-volume
                        # session; preserve that absence instead of writing a
                        # fabricated zero.
                        "amount": amount,
                    }
                )
    finally:
        if owns:
            client.close()
    if rejected:
        logger.warning("EastMoney kline: rejected %s malformed row(s)", rejected)
    if not rows:
        return pl.DataFrame(), fetch_failed
    return (
        pl.DataFrame(rows)
        .unique(subset=["symbol", "trade_date"], keep="last")
        .sort(["trade_date", "symbol"]),
        fetch_failed,
    )


def fetch_daily_bars(
    symbols: list[str],
    start: date,
    end: date,
    *,
    client: EastMoneyClient | None = None,
    config=None,
) -> pl.DataFrame:
    """Per-symbol historical kline (slow). Prefer :func:`fetch_daily_bars_clist` for tip.

    Compatibility wrapper returning only the DataFrame; callers that need to tell
    connection failures apart from empty (suspended) sessions should use
    :func:`fetch_daily_bars_with_status`.
    """
    df, _ = fetch_daily_bars_with_status(symbols, start, end, client=client, config=config)
    return df
