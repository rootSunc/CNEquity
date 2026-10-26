"""SSE auction day K-lines with quantities in shares and rounded yuan amounts."""

import json
import math
from datetime import datetime

import polars as pl

from cnequity.domain.symbols import is_all_a_symbol, is_etf_symbol

URL_ROOT = "http://yunhq.sse.com.cn:32041/v1/sh1/dayk/"
SZ_URL_ROOT = "http://yunhq.sse.com.cn:32041/v1/sz1/dayk/"
PARAMS = {"begin": "0", "end": "-1", "period": "day"}
PEER_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"


def fund_peer_params(symbol, start, end):
    fund_dayk_url(symbol)
    code, exchange = symbol.split(".")
    return {
        "secid": f"{'1' if exchange == 'SH' else '0'}.{code}",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "0",
        "beg": start.strftime("%Y%m%d"),
        "end": end.strftime("%Y%m%d"),
    }


def parse_fund_peer(payload, symbol):
    """Read peer OHLC/amount only; whole-lot quantity cannot prove odd shares."""
    fund_dayk_url(symbol)
    code, exchange = symbol.split(".")
    response = json.loads(payload)
    data = response.get("data") if isinstance(response, dict) else None
    if (
        not isinstance(data, dict)
        or data.get("code") != code
        or data.get("market") != (1 if exchange == "SH" else 0)
        or not isinstance(data.get("klines"), list)
    ):
        raise ValueError("fund peer identity or daily bars differ")
    rows, seen = [], set()
    for line in data["klines"]:
        parts = str(line).split(",")
        if len(parts) != 11:
            raise ValueError("fund peer field shape differs")
        day = datetime.strptime(parts[0], "%Y-%m-%d").date()
        if day in seen:
            raise ValueError("fund peer repeats a trading session")
        seen.add(day)
        open_, close, high, low, amount = map(float, [*parts[1:5], parts[6]])
        if not all(math.isfinite(v) and v > 0 for v in (open_, close, high, low, amount)):
            continue
        if not low <= min(open_, close) <= max(open_, close) <= high:
            raise ValueError("fund peer prices are inconsistent")
        rows.append(
            dict(
                symbol=symbol,
                trade_date=day,
                open=open_,
                high=high,
                low=low,
                close=close,
                amount=amount,
            )
        )
    return pl.DataFrame(
        rows,
        schema={
            "symbol": pl.String,
            "trade_date": pl.Date,
            **dict.fromkeys(("open", "high", "low", "close", "amount"), pl.Float64),
        },
    )


def fund_dayk_url(symbol: str) -> str:
    code, exchange = symbol.split(".")
    if exchange not in {"SH", "SZ"} or not is_etf_symbol(code, exchange):
        raise ValueError("SSE dayk requires an exchange-listed Shanghai or Shenzhen fund")
    return (URL_ROOT if exchange == "SH" else SZ_URL_ROOT) + code


def parse_sse_fund_dayk(payload: bytes, symbol: str) -> pl.DataFrame:
    fund_dayk_url(symbol)
    return parse_sse_dayk(payload, symbol)


def parse_sse_dayk(payload: bytes, symbol: str, *, trade_dates=None) -> pl.DataFrame:
    code, exchange = symbol.split(".")
    if not (
        (exchange in {"SH", "SZ"} and is_etf_symbol(code, exchange))
        or (exchange == "SH" and is_all_a_symbol(code, exchange))
    ):
        raise ValueError("SSE dayk requires a supported Shanghai security or Shenzhen fund")
    response = json.loads(payload)
    if not isinstance(response, dict) or response.get("code") != code:
        raise ValueError("SSE dayk security identity differs")
    rows = response.get("kline")
    if (
        not isinstance(rows, list)
        or response.get("begin") != 0
        or response.get("total") != len(rows)
        or response.get("end") != len(rows)
    ):
        raise ValueError("SSE dayk history is incomplete")
    result, seen = [], set()
    for row in rows:
        if not isinstance(row, list) or len(row) != 7:
            raise ValueError("SSE dayk field shape differs")
        day = datetime.strptime(str(row[0]), "%Y%m%d").date()
        if day in seen:
            raise ValueError("SSE dayk repeats a trading session")
        seen.add(day)
        if trade_dates is not None and day not in trade_dates:
            continue
        values = row[1:]
        if not all(
            isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) and v >= 0
            for v in values
        ):
            raise ValueError("SSE dayk contains invalid numbers")
        open_, high, low, close, volume, amount = values
        if volume != int(volume) or volume >= 2**63:
            raise ValueError("SSE dayk quantity is not an Int64 share count")
        if min(open_, high, low, close, volume, amount) <= 0:
            continue
        if not low <= min(open_, close) <= max(open_, close) <= high:
            raise ValueError("SSE dayk auction prices are inconsistent")
        # Published amounts are rounded to yuan; never reverse this rounding
        # to infer a quantity. The publisher's actual quantity is mandatory.
        if not low * volume - 1 <= amount <= high * volume + 1:
            raise ValueError("SSE dayk turnover does not match its auction range")
        result.append(
            dict(
                symbol=symbol,
                trade_date=day,
                open=float(open_),
                high=float(high),
                low=float(low),
                close=float(close),
                volume=int(volume),
                amount=float(amount),
            )
        )
    return pl.DataFrame(
        result,
        schema={
            "symbol": pl.String,
            "trade_date": pl.Date,
            **dict.fromkeys(("open", "high", "low", "close", "amount"), pl.Float64),
            "volume": pl.Int64,
        },
    )


def fetch_fund_dayk(symbol: str, *, trade_dates=None, client=None, config=None) -> pl.DataFrame:
    """One exchange fund's auction day bars, quantities in shares.

    TDX reports fund volume in 100-share lots, so a 1-99 share session reads
    as zero lots with positive turnover and is left unpublished; this is the
    publisher's own count. Amounts are rounded to whole yuan.
    """
    import httpx

    from cnequity.domain.http_policy import record_http_response
    from cnequity.domain.rate_limit import source_request

    url = fund_dayk_url(symbol)
    session = client if client is not None else httpx.Client(timeout=30.0)
    try:
        with source_request(config, "exchange"):
            response = session.get(url, params=PARAMS)
            record_http_response(config, "exchange", response)
        response.raise_for_status()
        return parse_sse_dayk(response.content, symbol, trade_dates=trade_dates)
    finally:
        if client is None:
            session.close()
