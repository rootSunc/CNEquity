"""Economic events and market holidays from 金十数据's 财经日历.

Both answer on the calendar host with the same web-app headers as the
indicator releases (economic_calendar.py). Measured 2026-10-29:

- ``/get/event`` gives one day of scheduled events (central bank decisions,
  speeches, meetings): 15-20 a day, history back to about 2015. The text is
  the event; there is no value column.
- ``/get/holiday`` gives the next 30 exchange closures on or after the asked
  date, about two months, history back to about 2014. Its note says when
  Stock Connect is closed.

Every row carries a stable ``id``, which becomes the row identity.
"""

from __future__ import annotations

from datetime import date, timedelta

import httpx
import polars as pl

from cnequity.adapters.eastmoney.raw import archive_response, configured_archive
from cnequity.adapters.jin10.economic_calendar import _HEADERS, _TIMEOUT_SECONDS, _to_int
from cnequity.domain.http_policy import record_http_response
from cnequity.domain.rate_limit import source_request

SOURCE = "jin10"
_BASE = "https://e0430d16720e4211b5e072c26205c890.z3c.jin10.com"


def _text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _payload_rows(payload: dict, what: str) -> list[dict]:
    if payload.get("status") != 200:
        raise RuntimeError(f"jin10 {what}: status {payload.get('status')!r}")
    return payload.get("data") or []


def parse_events(payload: dict, day: date) -> list[dict]:
    """Rows for one day's events; events dated on another day are dropped."""
    rows: list[dict] = []
    for item in _payload_rows(payload, f"events {day}"):
        event_id = item.get("id")
        content = _text(item.get("event_content"))
        event_time = str(item.get("event_time") or "").strip()
        if event_id is None or content is None or len(event_time) < 10:
            continue
        try:
            event_date = date.fromisoformat(event_time[:10])
        except ValueError:
            continue
        if event_date != day:
            continue
        # "待定" marks an event whose time is not fixed; its 00:00 is a
        # placeholder, not midnight.
        status = _text(item.get("time_status"))
        rows.append(
            {
                "event_id": f"{SOURCE}:{event_id}",
                "event_date": event_date,
                "event_time": None if status == "待定" else event_time[11:16] or None,
                "time_status": status,
                "country": _text(item.get("country")),
                "importance": _to_int(item.get("star")),
                "people": _text(item.get("people")),
                "content": content,
            }
        )
    return rows


def parse_holidays(payload: dict) -> list[dict]:
    rows: list[dict] = []
    for item in _payload_rows(payload, "holidays"):
        holiday_id = item.get("id")
        try:
            holiday_date = date.fromisoformat(str(item.get("date") or ""))
        except ValueError:
            continue
        if holiday_id is None:
            continue
        rows.append(
            {
                "holiday_id": f"{SOURCE}:{holiday_id}",
                "holiday_date": holiday_date,
                "country": _text(item.get("country")),
                "exchange": _text(item.get("exchange_name")),
                "name": _text(item.get("name")),
                "note": _text(item.get("rest_note")),
            }
        )
    return rows


def _get(client, archive, dataset, path, params, *, config, run_id, scope) -> dict:
    with source_request(config, SOURCE):
        response = client.get(_BASE + path, params=params)
        record_http_response(config, SOURCE, response)
    response.raise_for_status()
    if archive is not None:
        archive_response(
            archive,
            dataset,
            response,
            source=SOURCE,
            request_params=params,
            run_id=run_id,
            request_scope=scope,
        )
    return response.json()


def fetch_economic_events_window(
    start: date,
    end: date,
    *,
    config=None,
    run_id: str | None = None,
    client: httpx.Client | None = None,
) -> pl.DataFrame:
    """Events scheduled in [start, end], one request per day."""
    scope = f"rolling:{start.isoformat()}:{end.isoformat()}"
    archive = configured_archive(
        config, "economic_events", run_id=run_id, request_scope=scope, source=SOURCE
    )
    owns = client is None
    if client is None:
        client = httpx.Client(headers=_HEADERS, timeout=_TIMEOUT_SECONDS)
    rows: list[dict] = []
    try:
        day = start
        while day <= end:
            params = {"date": day.isoformat(), "category": "cj"}
            payload = _get(
                client, archive, "economic_events", "/get/event", params,
                config=config, run_id=run_id, scope=scope,
            )  # fmt: skip
            rows.extend(parse_events(payload, day))
            day += timedelta(days=1)
    finally:
        if owns:
            client.close()
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["event_id"], keep="last")


def fetch_economic_events(
    trade_date: date,
    *,
    config=None,
    run_id: str | None = None,
) -> pl.DataFrame:
    """Rolling window [trade_date-2, trade_date+14], as for the calendar."""
    return fetch_economic_events_window(
        trade_date - timedelta(days=2),
        trade_date + timedelta(days=14),
        config=config,
        run_id=run_id,
    )


def fetch_market_holidays(
    trade_date: date,
    *,
    config=None,
    run_id: str | None = None,
    client: httpx.Client | None = None,
) -> pl.DataFrame:
    """The next 30 closures on or after *trade_date*: one request, ~two months."""
    scope = f"from:{trade_date.isoformat()}"
    archive = configured_archive(
        config, "market_holidays", run_id=run_id, request_scope=scope, source=SOURCE
    )
    owns = client is None
    if client is None:
        client = httpx.Client(headers=_HEADERS, timeout=_TIMEOUT_SECONDS)
    try:
        params = {"date": trade_date.isoformat(), "category": "cj"}
        payload = _get(
            client, archive, "market_holidays", "/get/holiday", params,
            config=config, run_id=run_id, scope=scope,
        )  # fmt: skip
    finally:
        if owns:
            client.close()
    rows = parse_holidays(payload)
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["holiday_id"], keep="last")
