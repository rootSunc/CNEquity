"""Economic calendar from 金十数据 (adapters/jin10/economic_calendar.py)."""

from __future__ import annotations

from datetime import date

import httpx
import pytest

from cnequity.adapters.jin10.economic_calendar import (
    fetch_economic_calendar,
    fetch_economic_calendar_window,
    parse_day,
)


def _item(**overrides) -> dict:
    base = {
        "data_id": 1145437,
        "indicator_id": 21,
        "indicator_name": "美联储利率决定(上限)",
        "time_period": "至10月28日",
        "previous": "4.00",
        "consensus": "4.00",
        "actual": "4",
        "revised": None,
        "pub_time": "2026-10-29 02:00",
        "country": "美国",
        "star": 5,
        "unit": "%",
    }
    base.update(overrides)
    return base


def test_parse_day_maps_the_fields():
    (row,) = parse_day({"status": 200, "data": [_item()]}, date(2026, 10, 29))
    assert row == {
        "event_id": "jin10:1145437",
        "indicator_code": "jin10:21",
        "event_date": date(2026, 10, 29),
        "event_time": "02:00",
        "country": "美国",
        "indicator": "美联储利率决定(上限)（至10月28日）",
        "importance": 5,
        "forecast": 4.0,
        "previous": 4.0,
        "actual": 4.0,
        "unit": "%",
    }


def test_parse_day_prefers_the_revised_previous_and_blanks():
    (row,) = parse_day(
        {"status": 200, "data": [_item(revised="4.25", actual="", consensus="--")]},
        date(2026, 10, 29),
    )
    assert row["previous"] == 4.25
    assert row["actual"] is None
    assert row["forecast"] is None


def test_parse_day_drops_rows_without_identity_or_from_another_day():
    rows = parse_day(
        {
            "status": 200,
            "data": [
                _item(data_id=None),
                _item(indicator_name=""),
                _item(pub_time="2026-10-30 08:00"),
                _item(pub_time="bad"),
            ],
        },
        date(2026, 10, 29),
    )
    assert rows == []


def test_parse_day_rejects_a_refused_request():
    with pytest.raises(RuntimeError, match="status"):
        parse_day({"status": 401, "data": []}, date(2026, 10, 29))


def test_window_requests_each_day_and_keeps_the_latest_reading():
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        day = request.url.params["date"]
        seen.append(day)
        assert request.headers["x-app-id"]
        actual = "4" if day == "2026-10-29" else ""
        return httpx.Response(
            200,
            json={"status": 200, "data": [_item(pub_time=f"{day} 02:00", actual=actual)]},
        )

    client = httpx.Client(
        transport=httpx.MockTransport(handler),
        headers={"x-app-id": "test"},
    )
    frame = fetch_economic_calendar_window(date(2026, 10, 28), date(2026, 10, 30), client=client)
    assert seen == ["2026-10-28", "2026-10-29", "2026-10-30"]
    # One data_id is one event: the same id on several days collapses to one row.
    assert frame.height == 1


def test_daily_window_is_two_days_back_and_two_weeks_ahead(monkeypatch):
    from cnequity.adapters.jin10 import economic_calendar as module

    calls = {}

    def window(start, end, **_kwargs):
        calls["window"] = (start, end)
        return None

    monkeypatch.setattr(module, "fetch_economic_calendar_window", window)
    fetch_economic_calendar(date(2026, 10, 29))
    assert calls["window"] == (date(2026, 10, 27), date(2026, 11, 12))


def test_backfill_walks_by_month_and_fetches_only_missing_days(tmp_path, monkeypatch):
    import polars as pl

    from cnequity.config import Config
    from cnequity.steps import newsboard

    cfg = Config(data_root=tmp_path / "data", raw_archive_enabled=False)
    cfg._backfill = True
    cfg._backfill_start = date(2008, 1, 30)
    cfg._backfill_end = date(2008, 3, 2)
    present = {date(2008, 2, d) for d in range(1, 30)}
    monkeypatch.setattr(newsboard, "_calendar_days_present", lambda config: present)
    windows: list[tuple[date, date]] = []

    def window(start, end, **_kwargs):
        windows.append((start, end))
        return pl.DataFrame(
            parse_day({"status": 200, "data": [_item(pub_time=f"{start} 02:00")]}, start)
        )

    monkeypatch.setattr(newsboard, "fetch_economic_calendar_window", window)
    result = newsboard.step_economic_calendar(cfg, date(2026, 10, 29), "r-bf", {})
    # February is complete in curated; January and March fetch only their own span.
    assert windows == [(date(2008, 1, 30), date(2008, 1, 31)), (date(2008, 3, 1), date(2008, 3, 2))]
    assert result["months_fetched"] == 2
    assert result["months_skipped"] == 1
    assert result["rows_written"] == 2


def test_an_event_dropped_from_a_later_capture_is_marked_withdrawn(tmp_path):
    import polars as pl

    from cnequity.config import Config
    from cnequity.steps.newsboard import _with_withdrawn

    cfg = Config(data_root=tmp_path / "data", raw_archive_enabled=False)
    part = cfg.curated_root / "economic_calendar" / "event_date=2026"
    part.mkdir(parents=True)
    old = pl.DataFrame(
        parse_day(
            {
                "status": 200,
                "data": [
                    _item(data_id=1178833, pub_time="2026-11-05 07:30"),
                    _item(data_id=1, pub_time="2026-12-30 07:30"),
                ],
            },
            date(2026, 11, 5),
        )
    )
    old.write_parquet(part / "part.parquet")
    fresh = pl.DataFrame(
        parse_day(
            {"status": 200, "data": [_item(data_id=1189668, pub_time="2026-11-02 07:30")]},
            date(2026, 11, 2),
        )
    )

    out = _with_withdrawn(cfg, "economic_calendar", fresh, date(2026, 10, 31), date(2026, 11, 14))
    flags = dict(zip(out["event_id"], out["withdrawn"], strict=True))
    # The reissued event is current; the one it replaced inside the window is
    # flagged; nothing outside the window is touched.
    assert flags == {"jin10:1189668": False, "jin10:1178833": True}
