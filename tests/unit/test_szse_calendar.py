"""SZSE's published calendar: the independent check on the holiday table, and
what extends the calendar past it before a release does."""

from __future__ import annotations

import json
from datetime import date

import polars as pl
import pytest

from cnequity.adapters.calendar.exchange_calendar import (
    calendar_coverage_end,
    confirmed_calendar_end,
    load_official_calendar,
    szse_calendar_month_path,
)
from cnequity.adapters.exchange import szse_calendar
from cnequity.adapters.exchange.szse_calendar import month_days, months_between, parse_month
from cnequity.config import Config
from cnequity.domain.holidays_cn import HOLIDAY_TABLE_END, exchange_may_open
from cnequity.quality.cross_checks import (
    trading_calendar_horizon_findings,
    trading_calendar_official_findings,
)

NEXT_YEAR = HOLIDAY_TABLE_END.year + 1
# A made-up closure in the first month past the table, to show SZSE's answer
# is used there rather than the weekday rule.
SZSE_ONLY_CLOSURE = next(day for day in month_days(NEXT_YEAR, 1)[3:] if day.weekday() < 5)


def _payload(year: int, month: int, closed: frozenset[date] = frozenset()) -> bytes:
    rows = [
        {
            "jyrq": day.isoformat(),
            "jybz": "1" if day.weekday() < 5 and day not in closed else "0",
            "zrxh": day.isoweekday() % 7 + 1,
        }
        for day in month_days(year, month)
    ]
    return json.dumps({"data": rows, "nowdate": "2026-11-24"}).encode()


def _cache(cfg: Config, year: int, month: int, payload: bytes) -> None:
    path = szse_calendar_month_path(cfg.meta_root, year, month)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def test_parse_month_reads_flags_and_treats_unpublished_as_unknown():
    days = parse_month(_payload(2026, 2), 2026, 2)
    assert len(days) == 28
    assert days[date(2026, 2, 2)] is True and days[date(2026, 2, 7)] is False
    assert parse_month(b'{"data":[],"nowdate":"2026-11-24"}', NEXT_YEAR, 1) == {}


def test_parse_month_rejects_a_partial_month_or_unknown_flag():
    rows = json.loads(_payload(2026, 2))["data"]
    with pytest.raises(ValueError, match="exactly that month"):
        parse_month(json.dumps({"data": rows[:-1]}), 2026, 2)
    rows[0]["jybz"] = "2"
    with pytest.raises(ValueError, match="jybz"):
        parse_month(json.dumps({"data": rows}), 2026, 2)


def test_months_between_spans_year_ends():
    assert months_between(date(2026, 11, 24), date(2027, 2, 1)) == [
        (2026, 11),
        (2026, 12),
        (2027, 1),
        (2027, 2),
    ]


def test_coverage_extends_only_through_whole_months_following_the_table():
    jan = parse_month(_payload(NEXT_YEAR, 1), NEXT_YEAR, 1)
    feb = parse_month(_payload(NEXT_YEAR, 2), NEXT_YEAR, 2)
    assert confirmed_calendar_end({}) == HOLIDAY_TABLE_END
    assert confirmed_calendar_end({**jan, **feb}) == date(
        NEXT_YEAR, 2, month_days(NEXT_YEAR, 2)[-1].day
    )
    assert confirmed_calendar_end(feb) == HOLIDAY_TABLE_END  # January is a gap


def test_an_unreadable_cache_file_is_skipped(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _cache(cfg, NEXT_YEAR, 1, _payload(NEXT_YEAR, 1))
    _cache(cfg, NEXT_YEAR, 2, b"not json")
    assert calendar_coverage_end(cfg.meta_root) == date(NEXT_YEAR, 1, 31)
    assert len(load_official_calendar(cfg.meta_root)) == 31


def test_a_table_day_szse_disagrees_with_is_an_error(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    table_closures = frozenset(
        day for day in month_days(2026, 2) if day.weekday() < 5 and not exchange_may_open(day)
    )
    _cache(cfg, 2026, 2, _payload(2026, 2, closed=table_closures))
    assert trading_calendar_official_findings(cfg) == []

    session = date(2026, 2, 24)
    assert exchange_may_open(session)
    _cache(cfg, 2026, 2, _payload(2026, 2, closed=table_closures | {session}))
    (finding,) = trading_calendar_official_findings(cfg)
    assert finding["severity"] == "error"
    assert finding["check"] == "trading_calendar_disagrees_with_szse"
    assert finding["disagree_sample"] == [{"date": "2026-02-24", "szse_is_trading": False}]


def test_published_szse_months_move_the_horizon_check(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    root = cfg.curated_root / "trading_calendar" / f"trade_date={NEXT_YEAR}"
    root.mkdir(parents=True, exist_ok=True)
    meta = {"source": "szse_calendar", "data_version": "v1", "fetched_at": None}
    pl.DataFrame(
        [{"trade_date": date(NEXT_YEAR, 1, 20), "is_trading": True, **meta}]
    ).write_parquet(root / "part-0.parquet")
    assert trading_calendar_horizon_findings(cfg, date(2026, 11, 24))
    _cache(cfg, NEXT_YEAR, 1, _payload(NEXT_YEAR, 1))
    assert trading_calendar_horizon_findings(cfg, date(2026, 11, 24)) == []


def _run_step(cfg: Config, trade_date: date, monkeypatch) -> pl.DataFrame:
    import cnequity.steps.reference as reference

    written: dict = {}

    def _capture(config, run_id, dataset, df):
        written["df"] = df
        return {}

    monkeypatch.setattr(reference, "write_simple", _capture)
    reference.step_trading_calendar(cfg, trade_date, "run-1", {})
    return written["df"]


def test_the_step_extends_the_calendar_with_szse_months(tmp_path, monkeypatch):
    from cnequity.storage.layout import init_data_layout

    cfg = Config(data_root=tmp_path / "lake", sources={"exchange": True})
    init_data_layout(cfg)
    requested: list[tuple[int, int]] = []

    def _fetch(year, month, *, config=None):
        requested.append((year, month))
        if year < NEXT_YEAR:
            return _payload(year, month)
        if (year, month) == (NEXT_YEAR, 1):
            return _payload(year, month, closed=frozenset({SZSE_ONLY_CLOSURE}))
        return b'{"data":[],"nowdate":"2026-11-24"}'

    monkeypatch.setattr(szse_calendar, "fetch_month", _fetch)
    df = _run_step(cfg, date(NEXT_YEAR - 1, 11, 24), monkeypatch)

    assert len(requested) == 13  # this month and twelve ahead, one request each
    assert df["trade_date"].max() == date(NEXT_YEAR, 1, 31)
    past = df.filter(pl.col("trade_date") > HOLIDAY_TABLE_END)
    assert set(past["source"]) == {"szse_calendar"}
    assert past.filter(pl.col("trade_date") == SZSE_ONLY_CLOSURE)["is_trading"][0] is False
    inside = df.filter(pl.col("trade_date") <= HOLIDAY_TABLE_END)
    assert set(inside["source"]) == {"exchange_calendar"}
    # Unpublished months stay unknown: nothing cached for them.
    assert not szse_calendar_month_path(cfg.meta_root, NEXT_YEAR, 2).exists()


def test_the_step_skips_szse_when_the_exchange_source_is_off(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake", sources={"exchange": False})

    def _fetch(*args, **kwargs):
        raise AssertionError("fetched with the exchange source off")

    monkeypatch.setattr(szse_calendar, "fetch_month", _fetch)
    df = _run_step(cfg, date(NEXT_YEAR - 1, 11, 24), monkeypatch)
    assert df["trade_date"].max() == HOLIDAY_TABLE_END


def test_a_failed_fetch_leaves_the_step_on_the_table(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake", sources={"exchange": True})

    def _fetch(*args, **kwargs):
        raise ConnectionError("unreachable")

    monkeypatch.setattr(szse_calendar, "fetch_month", _fetch)
    df = _run_step(cfg, date(NEXT_YEAR - 1, 11, 24), monkeypatch)
    assert df["trade_date"].max() == HOLIDAY_TABLE_END
