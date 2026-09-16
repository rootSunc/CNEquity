"""Official closing quotes, read from the exchanges that publish them.

`daily_bars` is arbitrated by TDX against EastMoney. Both are quote vendors:
when they agree, all that establishes is that they do not disagree — neither is
the body that computed the number. This adapter supplies the missing outside
reading, so the comparison in ``quality/authority_checks.py`` can be made
against the publisher rather than against a second redistributor.

Two things measured on 2026-08-30 shape the implementation.

**The two exchanges expose different horizons.** SZSE's report export takes a
date range and serves any past session. SSE publishes no comparable per-stock
history file — its ``yunhq`` endpoint is a *snapshot* of the current session
and carries its own ``date``. So the SSE side can only arbitrate the session it
is currently serving, which is what a same-day audit needs and nothing more.
Callers must read :attr:`ExchangeQuotesResult.covered` rather than assume both
exchanges contributed; a comparison over SZSE alone says nothing about SH.

**The SSE snapshot is live during the session.** Before the close, ``last`` is
the running price, not the close, and comparing it against a settled curated bar
would manufacture drift every intraday run. The snapshot's own ``time`` field
gates this: it is only accepted from the close onward.

Neither exchange covers Beijing. BJ symbols are simply absent from the result
and drop out of the shared universe the check compares over.
"""

from __future__ import annotations

import io
import logging
import warnings
from dataclasses import dataclass
from datetime import date

import polars as pl

from cnequity.domain.rate_limit import source_request
from cnequity.domain.symbols import format_symbol, is_all_a_symbol, is_etf_symbol

logger = logging.getLogger(__name__)

_TIMEOUT_SECONDS = 60.0
# Must match the `[sources.exchange]` section that gates this adapter, exactly
# as in `st_lists`: `SourceRateLimiters.wait` silently no-ops on an
# unregistered name, so a typo here disables pacing without any error.
_SOURCE = "exchange"

QUOTE_COLUMNS = ("symbol", "trade_date", "open", "high", "low", "close", "volume", "amount")
_EMPTY_QUOTES = pl.DataFrame(
    schema={
        "symbol": pl.Utf8,
        "trade_date": pl.Date,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume": pl.Float64,
        "amount": pl.Float64,
    }
)

# SSE's own quote host. One request covers 主板 (60x), 科创板 (688/689) and B
# shares (900); the B shares are dropped by `is_all_a_symbol`. The field order
# of each row follows `select`, so the two must be edited together.
SSE_SELECT = ("code", "open", "high", "low", "last", "volume", "amount")
SSE_URL = (
    "http://yunhq.sse.com.cn:32041/v1/sh1/list/exchange/equity"
    f"?select={','.join(SSE_SELECT)}&begin=0&end=6000"
)
_SSE_HEADERS = {"Referer": "https://www.sse.com.cn/"}
# HHMMSS, as the snapshot reports it. 15:00 is the continuous-auction close;
# the closing call is settled by the time the field passes it.
SSE_CLOSE_TIME = 150000

# 个股日行情. Takes a date range and serves history, unlike SSE.
SZSE_URL = (
    "https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx"
    "&CATALOGID=1815_stock_snapshot&TABKEY=tab1"
    "&txtBeginDate={day}&txtEndDate={day}&random=0.1"
)
_SZSE_HEADERS = {"Referer": "https://www.szse.cn/"}
_SZSE_COLUMNS = {
    "证券代码": "code",
    "开盘": "open",
    "最高": "high",
    "最低": "low",
    "今收": "close",
    "成交量(万股)": "volume",
    "成交金额(万元)": "amount",
}
# The export states both in 万 (10k). Curated is shares and yuan.
_SZSE_SCALE = 10_000.0


class ExchangeQuotesUnavailable(RuntimeError):
    """The publisher served nothing usable for the requested session."""


@dataclass(frozen=True)
class ExchangeQuotesResult:
    """Official quotes plus exactly which exchanges stand behind them.

    ``covered`` is the point of the type. An empty contribution from one
    exchange must never be read as agreement about that exchange, and the
    horizons here differ by design, so a caller that ignores it will report a
    SZSE-only comparison as if it covered the market.
    """

    quotes: pl.DataFrame
    covered: frozenset[str]
    failures: dict[str, str]

    @property
    def is_empty(self) -> bool:
        return self.quotes.is_empty()


def _client():
    from curl_cffi import requests as cr

    return cr


def _keep_symbol(code: str, exchange: str) -> bool:
    """Equities and ETFs, matching what curated `daily_bars` carries."""
    return is_all_a_symbol(code, exchange) or is_etf_symbol(code, exchange)


def _is_untraded_quote(open_: float, high: float, low: float, close: float) -> bool:
    """Whether a row describes a session the security did not trade.

    A halted name is still listed in the board snapshot, and the exchange
    reports its OHL as ``0.0`` while carrying a non-zero reference ``close``.
    Nine SH rows looked like that on 2026-09-15 (``603400.SH`` at
    ``open/high/low = 0.0, close = 54.67``). Written through, those zeros are
    not a cheap price — they are a price the security never traded at, and they
    reach the lake as a >99% single-day drawdown.

    ``close`` alone is not enough to tell the two apart: a real session can
    legitimately have open == high == low == close on a limit-locked day, but
    never at zero.
    """
    return close > 0.0 and open_ <= 0.0 and high <= 0.0 and low <= 0.0


def _finish(rows: list[dict]) -> pl.DataFrame:
    if not rows:
        return _EMPTY_QUOTES.clone()
    return (
        pl.DataFrame(rows, schema_overrides=dict(_EMPTY_QUOTES.schema))
        .unique(subset=["symbol", "trade_date"], keep="last")
        .sort("symbol")
    )


def fetch_sse_daily_quotes(trade_date: date, *, config=None) -> pl.DataFrame:
    """Official SH closes for *trade_date*, or empty when it is not on offer.

    The endpoint serves one session — whichever it is currently publishing — so
    a request for any other date returns empty rather than the wrong day's
    numbers under the requested label.
    """
    try:
        with source_request(config, _SOURCE):
            resp = _client().get(
                SSE_URL, headers=_SSE_HEADERS, impersonate="chrome", timeout=_TIMEOUT_SECONDS
            )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.warning("SSE daily quotes unavailable: %s", exc)
        return _EMPTY_QUOTES.clone()

    raw_date = payload.get("date")
    snapshot_time = payload.get("time")
    try:
        snapshot_day = date(
            int(str(raw_date)[:4]), int(str(raw_date)[4:6]), int(str(raw_date)[6:8])
        )
    except (TypeError, ValueError):
        logger.warning("SSE daily quotes carried an unreadable date %r", raw_date)
        return _EMPTY_QUOTES.clone()
    if snapshot_day != trade_date:
        logger.info(
            "SSE snapshot serves %s, not the requested %s; no SH arbitration this run",
            snapshot_day,
            trade_date,
        )
        return _EMPTY_QUOTES.clone()
    if not isinstance(snapshot_time, int) or snapshot_time < SSE_CLOSE_TIME:
        logger.info(
            "SSE snapshot is mid-session (time=%s); `last` is not a close and is not compared",
            snapshot_time,
        )
        return _EMPTY_QUOTES.clone()

    rows: list[dict] = []
    untraded = 0
    for item in payload.get("list") or []:
        if not isinstance(item, (list, tuple)) or len(item) < len(SSE_SELECT):
            continue
        code = str(item[0]).strip().zfill(6)
        if len(code) != 6 or not code.isdigit() or not _keep_symbol(code, "SH"):
            continue
        try:
            values = [float(v) for v in item[1 : len(SSE_SELECT)]]
        except (TypeError, ValueError):
            continue
        open_, high, low, close, volume, amount = values
        if _is_untraded_quote(open_, high, low, close):
            untraded += 1
            continue
        rows.append(
            {
                "symbol": format_symbol(code, "SH"),
                "trade_date": trade_date,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "amount": amount,
            }
        )
    if untraded:
        logger.info(
            "SSE snapshot: %d listed symbol(s) reported no traded session (OHL=0); skipped",
            untraded,
        )
    if not rows:
        logger.warning("SSE daily quotes returned no usable rows; format may have changed")
    return _finish(rows)


def fetch_szse_daily_quotes(trade_date: date, *, config=None) -> pl.DataFrame:
    """Official SZ closes for *trade_date*, or empty when it is not on offer."""
    try:
        import pandas as pd

        with source_request(config, _SOURCE):
            resp = _client().get(
                SZSE_URL.format(day=trade_date.isoformat()),
                headers=_SZSE_HEADERS,
                impersonate="chrome",
                timeout=_TIMEOUT_SECONDS,
            )
        resp.raise_for_status()
        if not resp.content:
            logger.info("SZSE published no daily quote report for %s", trade_date)
            return _EMPTY_QUOTES.clone()
        with warnings.catch_warnings():
            # As in st_lists: the export ships without a default style and
            # openpyxl says so on every read.
            warnings.filterwarnings("ignore", message="Workbook contains no default style")
            pdf = pd.read_excel(io.BytesIO(resp.content), dtype=str)
    except Exception as exc:
        logger.warning("SZSE daily quotes unavailable: %s", exc)
        return _EMPTY_QUOTES.clone()

    missing = [column for column in _SZSE_COLUMNS if column not in pdf.columns]
    if missing:
        logger.warning("SZSE daily quote report is missing %s", missing)
        return _EMPTY_QUOTES.clone()

    rows: list[dict] = []
    untraded = 0
    for record in pdf[list(_SZSE_COLUMNS)].to_dict("records"):
        code = str(record["证券代码"]).strip().zfill(6)
        if len(code) != 6 or not code.isdigit() or not _keep_symbol(code, "SZ"):
            continue
        try:
            # The export separates thousands and writes "-" for a session the
            # security did not trade in.
            values = {
                field: float(str(record[column]).replace(",", "").strip())
                for column, field in _SZSE_COLUMNS.items()
                if field != "code"
            }
        except (TypeError, ValueError):
            # The export writes "-" for a session the security did not trade.
            untraded += 1
            continue
        if _is_untraded_quote(values["open"], values["high"], values["low"], values["close"]):
            untraded += 1
            continue
        rows.append(
            {
                "symbol": format_symbol(code, "SZ"),
                "trade_date": trade_date,
                "open": values["open"],
                "high": values["high"],
                "low": values["low"],
                "close": values["close"],
                "volume": values["volume"] * _SZSE_SCALE,
                "amount": values["amount"] * _SZSE_SCALE,
            }
        )
    if untraded:
        logger.info(
            "SZSE report for %s: %d listed symbol(s) reported no traded session; skipped",
            trade_date,
            untraded,
        )
    if not rows:
        logger.warning("SZSE daily quotes returned no usable rows for %s", trade_date)
    return _finish(rows)


def fetch_exchange_daily_quotes(trade_date: date, *, config=None) -> ExchangeQuotesResult:
    """Both exchanges, without hiding which of them actually answered."""
    frames: list[pl.DataFrame] = []
    covered: set[str] = set()
    failures: dict[str, str] = {}
    for exchange, fetch in (("sse", fetch_sse_daily_quotes), ("szse", fetch_szse_daily_quotes)):
        try:
            fetched = fetch(trade_date, config=config)
        except Exception as exc:  # noqa: BLE001 — record status for the audit
            failures[exchange] = str(exc)
            logger.warning("%s daily quotes unavailable for %s: %s", exchange, trade_date, exc)
            continue
        if fetched.is_empty():
            failures[exchange] = "no usable rows"
            continue
        frames.append(fetched)
        covered.add(exchange)
    quotes = pl.concat(frames, how="vertical_relaxed") if frames else _EMPTY_QUOTES.clone()
    return ExchangeQuotesResult(quotes=quotes, covered=frozenset(covered), failures=failures)
