"""Economic calendar from 金十数据's 财经日历 (rili.jin10.com).

EastMoney retired RPT_ECONOMICCALENDAR (code 9501). Jin10 answers one day per
request with the release time, country, indicator, star rating, consensus,
previous, revised and actual values. Measured 2026-10-29: 20-55 events a day,
history back to 2008, and future days carry consensus only.

The API wants the web app's ``x-app-id``/``x-version`` headers; without them
it refuses the request. Every event has a stable ``data_id``, which becomes
the event identity, so a later actual or revision updates the same row.
Its ``indicator_id`` names the series across releases and years, where the
indicator text does not: a central bank's decision keeps one id while its
wording changes.
"""

from __future__ import annotations

import math
from datetime import date, timedelta

import httpx
import polars as pl

from cnequity.adapters.eastmoney.raw import archive_response, configured_archive
from cnequity.domain.http_policy import record_http_response
from cnequity.domain.rate_limit import source_request

SOURCE = "jin10"
_URL = "https://e0430d16720e4211b5e072c26205c890.z3c.jin10.com/get/data"
_HEADERS = {
    "x-app-id": "rU6QIu7JHe2gOUeR",
    "x-version": "1.0.0",
    "Referer": "https://rili.jin10.com/",
    "User-Agent": "Mozilla/5.0",
}
_TIMEOUT_SECONDS = 20.0


def _parse_float(value: object) -> float | None:
    if value is None or value == "" or value == "--":
        return None
    try:
        parsed = float(str(value).replace(",", "").replace("%", ""))
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) else None


def _to_int(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def parse_day(payload: dict, day: date) -> list[dict]:
    """Rows for one day's response; events dated on another day are dropped."""
    if payload.get("status") != 200:
        raise RuntimeError(f"jin10 calendar {day}: status {payload.get('status')!r}")
    rows: list[dict] = []
    for item in payload.get("data") or []:
        data_id = item.get("data_id")
        indicator = str(item.get("indicator_name") or "").strip()
        pub_time = str(item.get("pub_time") or "").strip()
        if data_id is None or not indicator or len(pub_time) < 10:
            continue
        try:
            event_date = date.fromisoformat(pub_time[:10])
        except ValueError:
            continue
        if event_date != day:
            continue
        period = str(item.get("time_period") or "").strip()
        rows.append(
            {
                "event_id": f"{SOURCE}:{data_id}",
                "indicator_code": (
                    f"{SOURCE}:{item['indicator_id']}"
                    if item.get("indicator_id") is not None
                    else None
                ),
                "event_date": event_date,
                "event_time": pub_time[11:16] or None,
                "country": str(item.get("country") or "").strip() or None,
                # The period tells two releases of one indicator apart (an
                # initial and a revised GDP print share a name).
                "indicator": f"{indicator}（{period}）" if period else indicator,
                "importance": _to_int(item.get("star")),
                "forecast": _parse_float(item.get("consensus")),
                "previous": _parse_float(item.get("revised") or item.get("previous")),
                "actual": _parse_float(item.get("actual")),
                "unit": str(item.get("unit") or "").strip() or None,
            }
        )
    return rows


def fetch_economic_calendar_window(
    start: date,
    end: date,
    *,
    config=None,
    run_id: str | None = None,
    client: httpx.Client | None = None,
) -> pl.DataFrame:
    """Events released in [start, end], one request per day."""
    archive = configured_archive(
        config,
        "economic_calendar",
        run_id=run_id,
        request_scope=f"rolling:{start.isoformat()}:{end.isoformat()}",
        source=SOURCE,
    )
    owns = client is None
    if client is None:
        client = httpx.Client(headers=_HEADERS, timeout=_TIMEOUT_SECONDS)
    rows: list[dict] = []
    try:
        day = start
        while day <= end:
            params = {"date": day.isoformat(), "category": "cj"}
            with source_request(config, SOURCE):
                response = client.get(_URL, params=params)
                record_http_response(config, SOURCE, response)
            response.raise_for_status()
            if archive is not None:
                # The EastMoney helper is source-neutral: it stamps the
                # observation id and capture nonce that publication verifies.
                archive_response(
                    archive,
                    "economic_calendar",
                    response,
                    source=SOURCE,
                    request_params=params,
                    run_id=run_id,
                    request_scope=f"rolling:{start.isoformat()}:{end.isoformat()}",
                )
            rows.extend(parse_day(response.json(), day))
            day += timedelta(days=1)
    finally:
        if owns:
            client.close()
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["event_id"], keep="last")


def fetch_economic_calendar(
    trade_date: date,
    *,
    config=None,
    run_id: str | None = None,
) -> pl.DataFrame:
    """Rolling window [trade_date-2, trade_date+14] for snapshot daily runs."""
    return fetch_economic_calendar_window(
        trade_date - timedelta(days=2),
        trade_date + timedelta(days=14),
        config=config,
        run_id=run_id,
    )
