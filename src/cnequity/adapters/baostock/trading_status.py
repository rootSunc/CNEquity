"""Baostock daily trading status snapshot — backup for trading_status.

``query_all_stock(day)`` returns every SH/SZ A-share with ``tradeStatus``
(1=trading, 0=not trading that day) and ``code_name``. The name prefix carries
the ST / *ST designation, so one request yields both the suspension and the ST
facet — the same two dimensions EastMoney's daily snapshot answers.

Only SH/SZ names are covered; BJ has no baostock coverage and is handled by the
coordinator (see trading-status-failover). Requested symbols missing from the
snapshot are simply absent from the output — the coordinator classifies missing
rows against the previous day's curated baseline.

An unexpected ``tradeStatus`` vocabulary fails the whole snapshot closed:
treating an unknown value as ``normal`` would manufacture negative evidence.
"""

from __future__ import annotations

import time
from datetime import date

import polars as pl

from cnequity.adapters.baostock._session import _login, import_baostock
from cnequity.adapters.exchange.st_lists import is_st_name
from cnequity.domain.symbols import format_symbol

__all__ = ["fetch_trading_status_baostock"]

# trading_status columns minus provenance (added by the coordinator/step).
_OUTPUT_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "is_trading": pl.Boolean,
    "status": pl.Utf8,
}


def _symbol_from_baostock(code: str) -> str | None:
    """``sh.600053`` -> ``600053.SH``; None for non-SH/SZ markets."""
    if "." not in code:
        return None
    prefix, num = code.split(".", 1)
    exchange = {"sh": "SH", "sz": "SZ"}.get(prefix.lower())
    if exchange is None or len(num) != 6 or not num.isdigit():
        return None
    return format_symbol(num, exchange)


def fetch_trading_status_baostock(
    symbols: list[str],
    trade_date: date,
    *,
    bs=None,
    sleep=time.sleep,
    config=None,
) -> pl.DataFrame:
    """One-request daily snapshot from ``query_all_stock(day)``.

    ``bs`` / ``sleep`` are injectable for offline tests; pass ``config`` in
    production for ``[sources.baostock]`` pacing. Fail-loud on login failure
    or an unexpected ``tradeStatus`` vocabulary.
    """
    if config is not None:
        config.rate_limit("baostock")
    if bs is None:
        bs = import_baostock()

    _login(bs, sleep=sleep)
    expected = set(symbols)
    rows: list[dict] = []
    try:
        rs = bs.query_all_stock(day=trade_date.isoformat())
        error_code = getattr(rs, "error_code", "0")
        if error_code != "0":
            raise RuntimeError(
                f"baostock query_all_stock failed: {error_code} "
                f"{getattr(rs, 'error_msg', '')}".strip()
            )
        fields = tuple(getattr(rs, "fields", ()) or ())
        while rs.next():
            rec = dict(zip(fields, rs.get_row_data(), strict=False))
            symbol = _symbol_from_baostock(str(rec.get("code", "")))
            if symbol is None or symbol not in expected:
                continue
            trade_status = str(rec.get("tradeStatus", ""))
            if trade_status not in ("0", "1"):
                raise RuntimeError(
                    f"baostock query_all_stock unexpected tradeStatus={trade_status!r} for {symbol}"
                )
            if trade_status == "0":
                rows.append(
                    {
                        "symbol": symbol,
                        "trade_date": trade_date,
                        "is_trading": False,
                        "status": "suspended",
                    }
                )
            else:
                rows.append(
                    {
                        "symbol": symbol,
                        "trade_date": trade_date,
                        "is_trading": True,
                        "status": "st" if is_st_name(rec.get("code_name")) else "normal",
                    }
                )
    finally:
        try:
            bs.logout()
        except Exception:  # noqa: BLE001 — logout on a dead socket may raise
            pass

    df = pl.DataFrame(rows, schema=_OUTPUT_SCHEMA) if rows else pl.DataFrame(schema=_OUTPUT_SCHEMA)
    if not df.is_empty():
        df = df.unique(subset=["symbol", "trade_date"], keep="last").sort(["trade_date", "symbol"])
    return df
