"""Economic calendar window parsing (adapters/eastmoney/economic_calendar.py)."""

from __future__ import annotations

from datetime import date

from cn_market_lake.adapters.eastmoney.economic_calendar import (
    _parse_float,
    fetch_economic_calendar,
    fetch_economic_calendar_window,
)


def test_parse_float_handles_blank_sentinels():
    assert _parse_float(None) is None
    assert _parse_float("") is None
    assert _parse_float("--") is None


def test_parse_float_strips_commas_and_percent():
    assert _parse_float("1,234.5%") == 1234.5


def test_parse_float_returns_none_on_bad_value():
    assert _parse_float("not-a-number") is None


def _raw_item(**overrides) -> dict:
    base = {
        "PUBLISH_DATE": "2024-06-28 08:30:00",
        "TIME": "08:30",
        "COUNTRY": "中国",
        "INDICATOR": "CPI",
        "STAR": 3,
        "FORECAST": "2.1%",
        "PREVIOUS": "2.0%",
        "ACTUAL": "2.2%",
        "UNIT": "%",
    }
    base.update(overrides)
    return base


def test_fetch_economic_calendar_window_filters_and_parses(monkeypatch):
    rows = [
        _raw_item(),
        _raw_item(PUBLISH_DATE="2024-01-01 00:00:00"),  # outside window
        _raw_item(PUBLISH_DATE=""),  # missing date, skipped
        _raw_item(PUBLISH_DATE="not-a-date"),  # unparsable, skipped
    ]
    monkeypatch.setattr(
        "cn_market_lake.adapters.eastmoney.economic_calendar.fetch_datacenter",
        lambda client, report, **kwargs: rows,
    )
    df = fetch_economic_calendar_window(date(2024, 6, 20), date(2024, 6, 30))
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["event_date"] == date(2024, 6, 28)
    assert row["forecast"] == 2.1
    assert row["previous"] == 2.0
    assert row["actual"] == 2.2
    assert row["importance"] == 3
    assert row["unit"] == "%"
    assert row["event_id"] == "2024-06-28|08:30|中国|CPI"


def test_fetch_economic_calendar_window_handles_bad_star_and_lowercase_keys(monkeypatch):
    rows = [
        {
            "publish_date": "2024-06-28",
            "time": "09:00",
            "country": "美国",
            "indicator": "PMI",
            "star": "not-an-int",
            "forecast": None,
            "previous": None,
            "actual": None,
            "unit": "",
        }
    ]
    monkeypatch.setattr(
        "cn_market_lake.adapters.eastmoney.economic_calendar.fetch_datacenter",
        lambda client, report, **kwargs: rows,
    )
    df = fetch_economic_calendar_window(date(2024, 6, 20), date(2024, 6, 30))
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["importance"] is None
    assert row["unit"] is None


def test_fetch_economic_calendar_window_empty_when_no_rows(monkeypatch):
    monkeypatch.setattr(
        "cn_market_lake.adapters.eastmoney.economic_calendar.fetch_datacenter",
        lambda client, report, **kwargs: [],
    )
    df = fetch_economic_calendar_window(date(2024, 6, 20), date(2024, 6, 30))
    assert df.is_empty()


def test_fetch_economic_calendar_window_none_data(monkeypatch):
    monkeypatch.setattr(
        "cn_market_lake.adapters.eastmoney.economic_calendar.fetch_datacenter",
        lambda client, report, **kwargs: None,
    )
    df = fetch_economic_calendar_window(date(2024, 6, 20), date(2024, 6, 30))
    assert df.is_empty()


def test_fetch_economic_calendar_uses_rolling_window(monkeypatch):
    seen = {}

    def _fake_window(start, end):
        seen["start"] = start
        seen["end"] = end
        return "sentinel"

    monkeypatch.setattr(
        "cn_market_lake.adapters.eastmoney.economic_calendar.fetch_economic_calendar_window",
        _fake_window,
    )
    out = fetch_economic_calendar(date(2024, 6, 28))
    assert out == "sentinel"
    assert seen["start"] == date(2024, 6, 26)
    assert seen["end"] == date(2024, 7, 12)
