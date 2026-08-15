"""Economic calendar — rolling window snapshot (forecast/previous/actual)."""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from cn_market_lake.adapters.eastmoney.datacenter import fetch_datacenter
from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient

_REPORT = "RPT_ECONOMICCALENDAR"
_COLUMNS = "PUBLISH_DATE,TIME,COUNTRY,INDICATOR,STAR,FORECAST,PREVIOUS,ACTUAL,UNIT"


def _parse_float(value: object) -> float | None:
    if value is None or value == "" or value == "--":
        return None
    try:
        return float(str(value).replace(",", "").replace("%", ""))
    except ValueError:
        return None


def fetch_economic_calendar_window(start: date, end: date) -> pl.DataFrame:
    """Fetch calendar events in [start, end]; empty if source unavailable."""
    rows: list[dict] = []
    with EastMoneyClient() as client:
        data = fetch_datacenter(
            client,
            _REPORT,
            columns=_COLUMNS,
            page_size=500,
            sort_columns="PUBLISH_DATE,TIME",
        )

    for item in data or []:
        pub = str(item.get("PUBLISH_DATE") or item.get("publish_date") or "")[:10]
        if not pub:
            continue
        try:
            event_date = date.fromisoformat(pub)
        except ValueError:
            continue
        if event_date < start or event_date > end:
            continue
        indicator = str(item.get("INDICATOR") or item.get("indicator") or "").strip()
        country = str(item.get("COUNTRY") or item.get("country") or "").strip()
        event_time = str(item.get("TIME") or item.get("time") or "").strip()
        star = item.get("STAR") or item.get("star")
        try:
            importance = int(star) if star is not None else None
        except (TypeError, ValueError):
            importance = None
        event_id = f"{event_date.isoformat()}|{event_time}|{country}|{indicator}"
        rows.append(
            {
                "event_id": event_id,
                "event_date": event_date,
                "event_time": event_time,
                "country": country,
                "indicator": indicator,
                "importance": importance,
                "forecast": _parse_float(item.get("FORECAST") or item.get("forecast")),
                "previous": _parse_float(item.get("PREVIOUS") or item.get("previous")),
                "actual": _parse_float(item.get("ACTUAL") or item.get("actual")),
                "unit": str(item.get("UNIT") or item.get("unit") or "").strip() or None,
            }
        )

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows)


def fetch_economic_calendar(trade_date: date) -> pl.DataFrame:
    """Rolling window [trade_date-2, trade_date+14] for snapshot daily runs."""
    start = trade_date - timedelta(days=2)
    end = trade_date + timedelta(days=14)
    return fetch_economic_calendar_window(start, end)
