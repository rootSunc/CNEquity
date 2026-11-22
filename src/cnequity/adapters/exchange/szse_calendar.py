"""SZSE's own trading calendar, one month per request.

``monthList`` answers every calendar day of a month with ``jybz`` (``"1"`` a
session, ``"0"`` closed). Measured 2026-11-24: 2016-01..2026-12 agreed with the
holiday table on all 4,017 days, history reaches back at least to 2005, and a
year is served only once the exchange publishes it — 2027 was still empty
nineteen days after the State Council notice. An unpublished month comes back
as ``data: []``, which is "not yet known", never "closed".
"""

from __future__ import annotations

import calendar
import json
import logging
from datetime import date, timedelta

from cnequity.domain.http_policy import record_http_response
from cnequity.domain.rate_limit import source_request

logger = logging.getLogger(__name__)

URL = "https://www.szse.cn/api/report/exchange/onepersistenthour/monthList"
_HEADERS = {"Referer": "https://www.szse.cn/aboutus/calendar/"}
_TIMEOUT_SECONDS = 30.0
# Must match the `[sources.<name>]` section that gates this adapter.
_SOURCE = "exchange"


def month_days(year: int, month: int) -> list[date]:
    return [date(year, month, day) for day in range(1, calendar.monthrange(year, month)[1] + 1)]


def parse_month(payload: bytes | str, year: int, month: int) -> dict[date, bool]:
    """``{day: is_session}`` for one month; empty while the month is unpublished.

    A published month must answer every one of its days with ``0`` or ``1``;
    anything else is a format change and raises rather than becoming a guess.
    """
    rows = json.loads(payload).get("data")
    if not rows:
        return {}
    days: dict[date, bool] = {}
    for row in rows:
        day = date.fromisoformat(row["jyrq"])
        flag = row["jybz"]
        if flag not in {"0", "1"}:
            raise ValueError(f"SZSE calendar {day}: unexpected jybz {flag!r}")
        days[day] = flag == "1"
    if sorted(days) != month_days(year, month):
        raise ValueError(f"SZSE calendar {year}-{month:02d} does not cover exactly that month")
    return days


def fetch_month(year: int, month: int, *, config=None) -> bytes:
    """Raw ``monthList`` payload for one month; raises when unreachable or malformed."""
    from curl_cffi import requests as cr

    with source_request(config, _SOURCE):
        resp = cr.get(
            URL,
            params={"month": f"{year}-{month:02d}"},
            headers=_HEADERS,
            impersonate="chrome",
            timeout=_TIMEOUT_SECONDS,
        )
        record_http_response(config, _SOURCE, resp)
    resp.raise_for_status()
    parse_month(resp.content, year, month)
    return resp.content


def months_between(start: date, end: date) -> list[tuple[int, int]]:
    months: list[tuple[int, int]] = []
    cursor = start.replace(day=1)
    while cursor <= end:
        months.append((cursor.year, cursor.month))
        cursor = (cursor + timedelta(days=32)).replace(day=1)
    return months
