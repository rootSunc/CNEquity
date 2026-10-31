"""Offshore yuan daily close from Sina's forex day kline.

``fx_susdcnh`` is 美元兑离岸人民币. One call returns the whole published
history. Measured 2026-10-31: bars start 2014-11-07, fields are
date, open, low, high, close, and the close sits a few pips away from
EastMoney's push2his print of the same day. This series is that Sina close.
push2his is not a fallback: that host refuses requests, and splicing the two
prints would invent a day change.

The standard TDX client only speaks the Shanghai, Shenzhen and Beijing stock
markets, so it cannot supply this pair.
"""

from __future__ import annotations

import logging
import math
import re
from datetime import date

import httpx

from cnequity.domain.http_policy import record_http_response
from cnequity.domain.rate_limit import (
    SINA_FETCH_ATTEMPTS,
    SINA_RATE_LIMIT_COOLDOWN_SECONDS,
    SINA_RATE_LIMIT_STATUS_CODES,
    SINA_RETRY_STATUS_CODES,
    source_request,
)

logger = logging.getLogger(__name__)

_URL = (
    "https://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/"
    "var%20_fx_susdcnh=/NewForexService.getDayKLine"
)
_SYMBOL = "fx_susdcnh"
_QUOTED = re.compile(r'"([^"]*)"')


def _finite(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def parse_usdcnh_body(text: str, *, start: date, end: date) -> list[dict]:
    """Closes inside ``[start, end]`` from one Sina JSONP body.

    A bar outside the window is dropped. A body with no dated bar is refused,
    so a changed page cannot pass as an empty series.
    """
    match = _QUOTED.search(text or "")
    quoted = match.group(1).strip() if match else ""
    if not quoted:
        raise ValueError("离岸人民币日K没有行情")
    rows: list[dict] = []
    saw_date = False
    for piece in quoted.split("|"):
        parts = piece.split(",")
        if len(parts) < 5 or not parts[0]:
            continue
        try:
            obs = date.fromisoformat(parts[0][:10])
        except ValueError:
            continue
        saw_date = True
        if not start <= obs <= end:
            continue
        close = _finite(parts[4])
        if close is None or not 1 < close < 20:
            continue
        rows.append(
            {
                "indicator_id": "usdcnh",
                "obs_date": obs,
                "value": close,
                "frequency": "daily",
                "source": "sina",
            }
        )
    if not saw_date:
        raise ValueError("离岸人民币日K没有日期")
    return rows


def fetch_usdcnh_closes(
    start: date,
    end: date,
    *,
    client: httpx.Client | None = None,
    config=None,
) -> list[dict]:
    """Sina closes of 美元兑离岸人民币 inside ``[start, end]``."""
    if start > end:
        return []
    if config is not None and not config.sources.get("sina", True):
        return []
    owns = client is None
    if client is None:
        client = httpx.Client(
            timeout=30.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://finance.sina.com.cn",
            },
        )
    try:
        last: Exception | None = None
        response = None
        for attempt in range(SINA_FETCH_ATTEMPTS):
            try:
                with source_request(config, "sina"):
                    response = client.get(_URL, params={"symbol": _SYMBOL})
                    record_http_response(config, "sina", response)
                response.raise_for_status()
                break
            except Exception as exc:  # noqa: BLE001 — classified on the status below
                last = exc
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status not in SINA_RETRY_STATUS_CODES or attempt + 1 >= SINA_FETCH_ATTEMPTS:
                    raise
                if (
                    status in SINA_RATE_LIMIT_STATUS_CODES
                    and config is not None
                    and hasattr(config, "defer_source")
                ):
                    config.defer_source("sina", SINA_RATE_LIMIT_COOLDOWN_SECONDS)
                logger.warning(
                    "usdcnh: Sina HTTP %s; retry %d/%d",
                    status,
                    attempt + 2,
                    SINA_FETCH_ATTEMPTS,
                )
        if response is None:
            raise last if last is not None else RuntimeError("usdcnh: Sina returned no response")
        return parse_usdcnh_body(response.text, start=start, end=end)
    finally:
        if owns:
            client.close()
