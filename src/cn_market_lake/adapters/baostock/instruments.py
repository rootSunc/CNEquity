"""Baostock instrument basics — the delisted-symbol source for ``instruments``.

The live sources (TDX snapshot, EastMoney clist) only ever return names that are
*currently* listed, so a lake built from them holds survivors only: a symbol that
delisted in 2019 was never fetched, and no amount of universe filtering can put
it back. That is the survivorship bias ``universe_survivorship_absent`` reports.

``query_stock_basic`` is the one free source that answers the other question —
which codes *used* to exist. Called with no arguments it returns every code
baostock knows, listed and delisted alike, with ``status`` (1=listed, 0=delisted)
and ``outDate`` (the delisting date). It is a single query rather than a
per-symbol sweep, so unlike the ST/valuation backfills it costs one round-trip.

Coverage caveat: baostock's own history starts in 2015 and it does not carry BJ
(北交所) names, so this widens the universe without closing it completely. Rows
are emitted for SH/SZ only; ``fetch_instrument_basics`` reports what it skipped.
"""

from __future__ import annotations

import logging
import time
from datetime import date

import polars as pl

from cn_market_lake.adapters.baostock._session import _login, import_baostock
from cn_market_lake.domain.symbols import (
    format_symbol,
    is_cdr_symbol,
    is_etf_symbol,
)

logger = logging.getLogger(__name__)

__all__ = ["fetch_instrument_basics"]

# query_stock_basic response columns, in order.
_FIELDS = "code,code_name,ipoDate,outDate,type,status"

# baostock `type`: 1=stock, 2=index, 3=other, 4=convertible bond, 5=ETF.
_TYPE_STOCK = "1"
_TYPE_ETF = "5"

# instruments columns minus provenance (added by the caller).
_OUTPUT_SCHEMA = {
    "symbol": pl.Utf8,
    "name": pl.Utf8,
    "exchange": pl.Utf8,
    "asset_type": pl.Utf8,
    "list_date": pl.Date,
    "delist_date": pl.Date,
    "prev_symbol": pl.Utf8,
}


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _symbol_from_baostock(code: str) -> str | None:
    """``sh.600519`` -> ``600519.SH``. None for markets we do not model."""
    if "." not in code:
        return None
    prefix, num = code.split(".", 1)
    exchange = {"sh": "SH", "sz": "SZ", "bj": "BJ"}.get(prefix.lower())
    if exchange is None or not num.isdigit():
        return None
    return format_symbol(num, exchange)


def _asset_type(code: str, exchange: str, bs_type: str) -> str | None:
    """Map a baostock row onto the lake's ``asset_type``. None = not modelled."""
    if bs_type == _TYPE_ETF or is_etf_symbol(code, exchange):
        return "etf"
    if bs_type != _TYPE_STOCK:
        return None
    if is_cdr_symbol(code, exchange):
        return "cdr"
    return "stock"


def fetch_instrument_basics(*, bs=None, sleep=time.sleep) -> pl.DataFrame:
    """Every code baostock knows — listed *and* delisted — as instruments rows.

    ``delist_date`` is baostock's ``outDate`` and is set only for ``status='0'``
    rows; a listed name keeps it null. Fail-loud: a login failure or a non-zero
    ``error_code`` raises rather than returning an empty frame, so a broken
    session can never be mistaken for "no delisted names exist".

    ``bs`` / ``sleep`` are injectable for offline tests.
    """
    if bs is None:
        bs = import_baostock()

    _login(bs, sleep=sleep)
    try:
        rs = bs.query_stock_basic()
        error_code = getattr(rs, "error_code", "0")
        if error_code != "0":
            raise RuntimeError(
                f"baostock query_stock_basic failed: {error_code} "
                f"{getattr(rs, 'error_msg', '')}".strip()
            )
        rows: list[dict] = []
        skipped_market = 0
        skipped_type = 0
        while rs.next():
            code, name, ipo_raw, out_raw, bs_type, status = (rs.get_row_data() + [""] * 6)[:6]
            symbol = _symbol_from_baostock(code)
            if symbol is None:
                skipped_market += 1
                continue
            num, exchange = symbol.split(".")
            asset_type = _asset_type(num, exchange, str(bs_type))
            if asset_type is None:
                skipped_type += 1
                continue
            delisted = str(status) == "0"
            rows.append(
                {
                    "symbol": symbol,
                    "name": name or None,
                    "exchange": exchange,
                    "asset_type": asset_type,
                    "list_date": _parse_date(ipo_raw),
                    # Only trust outDate on rows baostock calls delisted; listed
                    # names sometimes carry a filler date in that column.
                    "delist_date": _parse_date(out_raw) if delisted else None,
                    "prev_symbol": None,
                }
            )
    finally:
        try:
            bs.logout()
        except Exception:  # noqa: BLE001 — logout on a dead socket may raise
            pass

    df = pl.DataFrame(rows, schema=_OUTPUT_SCHEMA) if rows else pl.DataFrame(schema=_OUTPUT_SCHEMA)
    logger.info(
        "baostock instrument basics: %d rows (%d delisted); skipped %d non-SH/SZ/BJ, %d non-equity",
        df.height,
        0 if df.is_empty() else df.filter(pl.col("delist_date").is_not_null()).height,
        skipped_market,
        skipped_type,
    )
    return df
