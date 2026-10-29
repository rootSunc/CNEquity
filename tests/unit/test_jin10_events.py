"""Economic events and market holidays from 金十数据 (adapters/jin10/events.py)."""

from __future__ import annotations

from datetime import date

import httpx
import polars as pl
import pytest

from cnequity.adapters.jin10.events import (
    fetch_economic_events,
    fetch_economic_events_window,
    fetch_market_holidays,
    parse_events,
    parse_holidays,
)
from cnequity.config import Config
from cnequity.steps import newsboard
from cnequity.steps.common import SnapshotBackfillError


def _event(**overrides) -> dict:
    base = {
        "id": 1189650,
        "event_time": "2026-10-29 23:45",
        "country": "加拿大",
        "star": 3,
        "people": "卡尼",
        "category": "cj",
        "event_content": "加拿大总理卡尼发表讲话。",
        "time_status": None,
    }
    base.update(overrides)
    return base


def _holiday(**overrides) -> dict:
    base = {
        "id": 1145989,
        "date": "2026-10-01",
        "country": "中国",
        "exchange_name": "香港交易所",
        "name": "国庆节",
        "rest_note": "休市一日，南、北向交易关闭。",
    }
    base.update(overrides)
    return base


def test_parse_events_maps_the_fields():
    (row,) = parse_events({"status": 200, "data": [_event()]}, date(2026, 10, 29))
    assert row == {
        "event_id": "jin10:1189650",
        "event_date": date(2026, 10, 29),
        "event_time": "23:45",
        "time_status": None,
        "country": "加拿大",
        "importance": 3,
        "people": "卡尼",
        "content": "加拿大总理卡尼发表讲话。",
    }


def test_an_unfixed_event_time_is_not_midnight():
    (row,) = parse_events(
        {"status": 200, "data": [_event(event_time="2026-10-29 00:00", time_status="待定")]},
        date(2026, 10, 29),
    )
    assert row["event_time"] is None
    assert row["time_status"] == "待定"


def test_parse_events_drops_rows_without_identity_or_from_another_day():
    rows = parse_events(
        {
            "status": 200,
            "data": [
                _event(id=None),
                _event(event_content=""),
                _event(event_time="2026-10-30 08:00"),
                _event(event_time="bad"),
            ],
        },
        date(2026, 10, 29),
    )
    assert rows == []
    with pytest.raises(RuntimeError, match="status"):
        parse_events({"status": 400, "data": None}, date(2026, 10, 29))


def test_parse_holidays_maps_the_fields_and_skips_bad_rows():
    rows = parse_holidays(
        {"status": 200, "data": [_holiday(), _holiday(id=None), _holiday(date="")]}
    )
    assert rows == [
        {
            "holiday_id": "jin10:1145989",
            "holiday_date": date(2026, 10, 1),
            "country": "中国",
            "exchange": "香港交易所",
            "name": "国庆节",
            "note": "休市一日，南、北向交易关闭。",
        }
    ]


def test_events_window_requests_each_day_and_holidays_one_page():
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        day = request.url.params["date"]
        seen.append((request.url.path, day))
        if request.url.path == "/get/holiday":
            return httpx.Response(200, json={"status": 200, "data": [_holiday()]})
        return httpx.Response(
            200, json={"status": 200, "data": [_event(id=day, event_time=f"{day} 10:00")]}
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    events = fetch_economic_events_window(date(2026, 10, 28), date(2026, 10, 29), client=client)
    holidays = fetch_market_holidays(date(2026, 10, 1), client=client)
    assert seen == [
        ("/get/event", "2026-10-28"),
        ("/get/event", "2026-10-29"),
        ("/get/holiday", "2026-10-01"),
    ]
    assert events.height == 2
    assert holidays["holiday_id"].to_list() == ["jin10:1145989"]


def test_daily_events_window_matches_the_calendar(monkeypatch):
    from cnequity.adapters.jin10 import events as module

    calls = {}
    monkeypatch.setattr(
        module,
        "fetch_economic_events_window",
        lambda start, end, **_kwargs: calls.setdefault("window", (start, end)),
    )
    fetch_economic_events(date(2026, 10, 29))
    assert calls["window"] == (date(2026, 10, 27), date(2026, 11, 12))


@pytest.fixture
def cfg(tmp_path):
    return Config(data_root=tmp_path / "data", raw_archive_enabled=False)


def test_steps_write_and_refuse_backfill_or_a_disabled_source(cfg, monkeypatch):
    monkeypatch.setattr(
        newsboard,
        "fetch_economic_events",
        lambda d, config=None: pl.DataFrame(
            parse_events({"status": 200, "data": [_event(event_time=f"{d} 10:00")]}, d)
        ),
    )
    monkeypatch.setattr(
        newsboard,
        "fetch_market_holidays",
        lambda d, config=None: pl.DataFrame(parse_holidays({"status": 200, "data": [_holiday()]})),
    )
    day = date(2026, 10, 29)
    assert newsboard.step_economic_events(cfg, day, "r-events", {})["rows_written"] == 1
    assert newsboard.step_market_holidays(cfg, day, "r-holidays", {})["rows_written"] == 1

    cfg._backfill = True
    with pytest.raises(SnapshotBackfillError, match="market_holidays"):
        newsboard.step_market_holidays(cfg, day, "r", {})
    cfg._backfill = False
    cfg.sources["jin10"] = False
    with pytest.raises(RuntimeError, match="economic_events: jin10 source disabled"):
        newsboard.step_economic_events(cfg, day, "r", {})
