"""Current daily quotes from the official Beijing Stock Exchange site.

The BSE quotation page exposes a paginated end-of-day snapshot containing
OHLCV and turnover for the listed board.  It is intentionally a *tip* source:
the endpoint returns the latest session, not a historical series.  Callers
must therefore pass the expected session and reject a response for another
date instead of stamping it onto an older partition.
"""

from __future__ import annotations

import json
import logging
import math
import re
from datetime import date, datetime
from typing import Any

import httpx
import polars as pl

from cnequity.adapters.numeric import finite_int64
from cnequity.domain.rate_limit import source_request

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.bse.cn"
_QUOTATION_PAGE = f"{_BASE_URL}/nq/quotation.html"
_QUOTATION_API = f"{_BASE_URL}/nqhqController/nqhq_en.do"
_PAGE_SIZE = 20
_MAX_PAGES = 100
_CALLBACK_RE = re.compile(r"^[A-Za-z_$][\w$]*\((.*)\);?$", re.DOTALL)
_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": _QUOTATION_PAGE,
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
}


class BseMarketDataError(RuntimeError):
    """Raised when the official BSE quotation response is unusable."""


def _parse_jsonp(text: str) -> Any:
    payload = text.strip()
    match = _CALLBACK_RE.fullmatch(payload)
    if match:
        payload = match.group(1)
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise BseMarketDataError(
            f"BSE quotation response is not valid JSON/JSONP: {text[:120]!r}"
        ) from exc


def _parse_page(text: str) -> tuple[list[dict[str, Any]], int]:
    payload = _parse_jsonp(text)
    if not isinstance(payload, list) or not payload or not isinstance(payload[0], dict):
        raise BseMarketDataError("BSE quotation response has no page object")
    page = payload[0]
    content = page.get("content")
    if content is None:
        raise BseMarketDataError("BSE quotation response is missing content")
    if not isinstance(content, list) or not all(isinstance(row, dict) for row in content):
        raise BseMarketDataError("BSE quotation content is not a list of objects")
    try:
        total = int(page.get("totalElements") or 0)
    except (TypeError, ValueError) as exc:
        raise BseMarketDataError("BSE quotation totalElements is invalid") from exc
    return content, max(0, total)


def _parse_date(value: object) -> date | None:
    text = str(value or "").strip()
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError:
        return None


def _float(value: object, *, minimum: float = 0.0) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed) or parsed < minimum:
        return None
    return parsed


def _row_to_quote(row: dict[str, Any], expected_date: date) -> dict[str, Any] | None:
    code = str(row.get("hqzqdm") or "").strip().zfill(6)
    if len(code) != 6 or not code.isdigit():
        return None
    trade_date = _parse_date(row.get("hqjsrq"))
    if trade_date != expected_date:
        return None
    open_ = _float(row.get("hqjrkp"), minimum=0.0)
    high = _float(row.get("hqzgcj"), minimum=0.0)
    low = _float(row.get("hqzdcj"), minimum=0.0)
    close = _float(row.get("hqzjcj"), minimum=0.0)
    amount = _float(row.get("hqcjje"), minimum=0.0)
    if None in (open_, high, low, close, amount):
        return None
    try:
        volume = finite_int64(row.get("hqcjsl"), minimum=0)
    except (TypeError, ValueError, OverflowError):
        return None
    return {
        "symbol": f"{code}.BJ",
        "trade_date": trade_date,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "amount": amount,
    }


def read_board(
    *,
    client: httpx.Client | None = None,
    config=None,
) -> tuple[list[dict[str, Any]], int]:
    """Walk the whole quotation board, returning its raw rows and its own total.

    The board is a *current* snapshot with no date parameter, so this takes no
    session: callers filter on ``hqjsrq`` themselves.

    A walk that ends on an empty page before the advertised total is a
    truncated board and raises here, for every caller. An *under-filled* page
    does not: the loop paginates by page offset, so it cannot see one. That is
    what the total is returned for — comparing it against ``len(rows)`` is a
    stricter completeness test, and trading status needs it, because Beijing
    encodes a halt as an absence and a short read would otherwise look like the
    exchange suspending itself.
    """
    owns_client = client is None
    if client is None:
        client = httpx.Client(timeout=20.0, follow_redirects=False, headers=_HEADERS)
    rows: list[dict[str, Any]] = []
    total = 0
    try:
        # The first request establishes the WAF cookie.  It may answer 302 to
        # the same URL; do not follow that redirect because it loops on some
        # overseas CDNs, and the cookie is enough for the JSONP endpoint.
        with source_request(config, "bse"):
            client.get(_QUOTATION_PAGE)
        first_page = True
        page = 0
        while first_page or page * _PAGE_SIZE < total:
            if page >= _MAX_PAGES:
                raise BseMarketDataError("BSE quotation pagination exceeded safety limit")
            request_data = {
                "page": page,
                "type_en": '["B"]',
                "sortfield": "hqcjsl",
                "sorttype": "desc",
                "xxfcbj_en": "[2]",
                "zqdm": "",
            }
            with source_request(config, "bse"):
                response = client.post(_QUOTATION_API, data=request_data)
            if getattr(response, "status_code", 200) in {301, 302, 307, 308}:
                # The site occasionally refreshes its WAF cookie with a
                # same-URL redirect. Re-establish the page cookie once; never
                # follow an arbitrary Location header into another endpoint.
                with source_request(config, "bse"):
                    client.get(_QUOTATION_PAGE)
                with source_request(config, "bse"):
                    response = client.post(_QUOTATION_API, data=request_data)
            response.raise_for_status()
            page_rows, page_total = _parse_page(response.text)
            # An empty tail page reports zero; keep the last real figure so it
            # cannot erase the number the walk is being measured against.
            total = page_total or total
            rows.extend(page_rows)
            if not page_rows:
                if page * _PAGE_SIZE < total:
                    raise BseMarketDataError(
                        "BSE quotation pagination ended before the advertised total "
                        f"({page * _PAGE_SIZE}/{total} rows)"
                    )
                break
            first_page = False
            page += 1
    finally:
        if owns_client:
            client.close()
    return rows, total


def fetch_daily_quotes(
    trade_date: date,
    *,
    symbols: list[str] | set[str] | None = None,
    client: httpx.Client | None = None,
    config=None,
) -> pl.DataFrame:
    """Fetch the BSE end-of-day quote snapshot for *trade_date*.

    The endpoint is a current snapshot.  If its session date differs from
    *trade_date*, an empty frame is returned; this makes accidental historical
    backfill stamping impossible.  ``symbols`` limits the returned rows after
    the board-wide pagination, while the response remains board-authoritative.
    """
    wanted = set(symbols) if symbols is not None else None
    raw_rows, _total = read_board(client=client, config=config)
    rows: list[dict[str, Any]] = []
    for raw in raw_rows:
        quote = _row_to_quote(raw, trade_date)
        if quote is not None and (wanted is None or quote["symbol"] in wanted):
            rows.append(quote)

    if not rows:
        return pl.DataFrame(
            schema={
                "symbol": pl.Utf8,
                "trade_date": pl.Date,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
                "amount": pl.Float64,
            }
        )
    return (
        pl.DataFrame(rows)
        .unique(subset=["symbol", "trade_date"], keep="last")
        .sort(["trade_date", "symbol"])
    )


__all__ = ["BseMarketDataError", "fetch_daily_quotes"]
