"""Exchange closures through the full fetch path, not just the calendar helper.

The calendar module is well tested in isolation. What is not tested is the
integration that matters for data integrity: a by-date backfill or incremental
run that *crosses* a multi-day closure. If a closed weekday is ever treated as
a session, the source returns nothing for it and ``record_missing_dates`` files
a gap that no refetch can fill. ``cne status`` would then report the dataset
incomplete forever.

Two calendars are exercised:

* ``seed``: no curated calendar, so the bundled seed + ``CLOSED_DATES`` decide;
* ``weekday_vendor_calendar``: a curated calendar that marks every weekday as
  trading (the shape the suite's own ``_seed_trading_calendar`` helper writes,
  and what a bad vendor row looks like). ``CLOSED_DATES`` must still win.

National Day 2025 closed 2025-10-01..03 and 2025-10-06..08.
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.run_scope import set_backfill
from cnequity.steps.common import (
    fetch_incremental_daily,
    last_session_on_or_before,
    walk_day_backfill,
)
from cnequity.storage.state import StateStore

WINDOW = (date(2025, 9, 29), date(2025, 10, 10))
CLOSED = [date(2025, 10, d) for d in (1, 2, 3, 6, 7, 8)]
SESSIONS = [date(2025, 9, 29), date(2025, 9, 30), date(2025, 10, 9), date(2025, 10, 10)]
CALENDARS = ["seed", "weekday_vendor_calendar"]


def _weekday_calendar(cfg: Config, start: date, end: date) -> None:
    rows = []
    day = start
    while day <= end:
        rows.append({"trade_date": day, "is_trading": day.weekday() < 5})
        day += timedelta(days=1)
    path = cfg.curated_root / "trading_calendar" / "part-merged.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)


def _lake(tmp_path, calendar: str) -> Config:
    cfg = Config(data_root=tmp_path / "data")
    if calendar == "weekday_vendor_calendar":
        _weekday_calendar(cfg, *WINDOW)
    return cfg


@pytest.mark.parametrize("calendar", CALENDARS)
def test_backfill_across_a_closure_never_requests_or_owes_a_closed_day(tmp_path, calendar):
    cfg = _lake(tmp_path, calendar)
    set_backfill(cfg, start=WINDOW[0], end=WINDOW[1])
    fetched: list[date] = []

    def fetch_one(day: date) -> pl.DataFrame:
        fetched.append(day)
        return pl.DataFrame([{"trade_date": day, "metric_id": "adv_pct", "value": 1.0}])

    out = walk_day_backfill(
        cfg, WINDOW[1], "run-national-day", "market_breadth", fetch_one, source="derived"
    )

    assert fetched == SESSIONS
    assert not set(fetched) & set(CLOSED)
    assert out["days_fetched"] == len(SESSIONS)
    assert out["days_empty"] == 0
    assert out.get("status", "success") == "success"
    assert StateStore(cfg.meta_root).get_missing_dates("market_breadth") == set()


@pytest.mark.parametrize("calendar", CALENDARS)
def test_first_incremental_run_after_a_closure_fetches_only_the_new_session(tmp_path, calendar):
    cfg = _lake(tmp_path, calendar)
    StateStore(cfg.meta_root).set_date("margin_trading", date(2025, 9, 30))
    fetched: list[date] = []

    def fetch(day: date) -> pl.DataFrame:
        fetched.append(day)
        return pl.DataFrame({"trade_date": [day], "symbol": ["600519.SH"], "value": [1.0]})

    df, findings = fetch_incremental_daily(cfg, "margin_trading", date(2025, 10, 9), fetch)

    assert fetched == [date(2025, 10, 9)]
    assert df.height == 1
    assert findings == []
    assert StateStore(cfg.meta_root).get_missing_dates("margin_trading") == set()


def test_a_gap_owed_from_before_a_closure_is_retried_without_adding_closed_days(tmp_path):
    """The durable ledger outranks the calendar walk: a gap recorded before
    the closure is retried on the first session after it, alongside the new
    session, and the closed days in between are not added."""
    cfg = _lake(tmp_path, "seed")
    state = StateStore(cfg.meta_root)
    state.set_date("margin_trading", date(2025, 9, 30))
    state.record_missing_dates("margin_trading", [date(2025, 9, 29)], reason="timeout")
    fetched: list[date] = []

    def fetch(day: date) -> pl.DataFrame:
        fetched.append(day)
        return pl.DataFrame({"trade_date": [day], "symbol": ["600519.SH"], "value": [1.0]})

    fetch_incremental_daily(cfg, "margin_trading", date(2025, 10, 9), fetch)
    assert fetched == [date(2025, 9, 29), date(2025, 10, 9)]


@pytest.mark.parametrize(
    ("asked", "expected"),
    [
        (date(2025, 10, 8), date(2025, 9, 30)),  # last day of National Day 2025
        (date(2025, 10, 4), date(2025, 9, 30)),  # weekend inside the closure
        (date(2026, 2, 23), date(2026, 2, 13)),  # last day of Spring Festival 2026
    ],
)
def test_a_window_ending_inside_a_closure_resolves_to_the_session_before_it(
    tmp_path, asked, expected
):
    """``_staged_daily_bar_symbols(..., end)`` filters ``trade_date == end``.
    An end date inside a closure must resolve to the last real session, or the
    whole market looks unresolved."""
    cfg = Config(data_root=tmp_path / "data")
    assert last_session_on_or_before(cfg, asked) == expected
