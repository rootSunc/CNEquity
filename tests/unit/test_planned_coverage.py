"""The cells a dataset owes, derived from what its spec already declares."""

from __future__ import annotations

from datetime import date

from cnequity.domain.datasets import DATASETS
from cnequity.quality.planned_coverage import LakeCalendar, coverage_of

TODAY = date(2026, 11, 13)


def _of(name):
    return coverage_of(DATASETS[name], TODAY)


def test_a_dataset_walked_by_date_owes_a_cell_per_session():
    bars = _of("daily_bars")
    assert (bars.grain, bars.by, bars.column) == ("session", "rows", "trade_date")
    # Derived day by day, though it keeps no watermark of its own.
    assert _of("minute_bars_15m").grain == "session"
    assert _of("minute_bars_15m").by == "rows"


def test_a_feed_that_publishes_on_weekends_owes_a_cell_per_calendar_day():
    assert _of("announcement_index").grain == "day"
    assert _of("announcement_index").column == "announce_date"


def test_a_dataset_keyed_by_reporting_period_owes_a_cell_per_quarter():
    items = _of("financial_statement_items")
    assert (items.grain, items.column, items.labelled) == ("quarter", "report_period", True)


def test_what_cannot_be_replayed_owes_only_its_latest_session():
    for name in ("instruments", "hot_rank", "sector_members"):
        assert (_of(name).grain, _of(name).by) == ("session", "write"), name
    # A snapshot with a history source can still fill an earlier day.
    assert _of("valuation_metrics").by == "rows"


def test_a_spec_that_names_its_cells_overrides_the_rule():
    assert (_of("pledge_stats").grain, _of("pledge_stats").column) == ("week", "end_date")
    assert _of("northbound_holdings").grain == "quarter"
    assert _of("northbound_holdings").labelled is False  # a date inside the quarter
    assert _of("executives").grain == "day"
    # Membership has a history source, but only its newest state is owed.
    assert _of("index_constituents").by == "write"


def test_what_the_source_serves_bounds_the_cells():
    assert _of("trade_ticks").start == date(2024, 1, 2)
    assert _of("northbound_flows").end == date(2024, 8, 16)
    horizon = _of("minute_bars").start
    assert horizon is not None and date(2026, 5, 1) < horizon < TODAY


def test_every_registered_dataset_gets_a_declaration():
    for spec in DATASETS.values():
        coverage_of(spec, TODAY)


def test_the_calendar_is_read_once_and_widened(monkeypatch):
    asked = []

    def trading_dates(config, start, end):
        asked.append((start, end))
        return [
            day
            for day in (date(2026, 11, 9), date(2026, 11, 10), date(2026, 11, 13))
            if start <= day <= end
        ]

    monkeypatch.setattr("cnequity.quality.planned_coverage.list_trading_dates", trading_dates)
    calendar = LakeCalendar(config=None)
    assert calendar.sessions(date(2026, 11, 9), date(2026, 11, 13)) == [
        date(2026, 11, 9),
        date(2026, 11, 10),
        date(2026, 11, 13),
    ]
    assert calendar.sessions(date(2026, 11, 10), date(2026, 11, 12)) == [date(2026, 11, 10)]
    assert len(asked) == 1
    calendar.sessions(date(2026, 11, 1), date(2026, 11, 10))
    assert asked[-1] == (date(2026, 11, 1), date(2026, 11, 13))


def _cells(states):
    import polars as pl

    return pl.DataFrame(
        {"period": [f"2026-01-{day:02d}" for day in range(1, len(states) + 1)], "state": states}
    )


def test_only_short_holes_inside_the_stored_span_read_as_empty_answers():
    from cnequity.quality.planned_coverage import settled_empty

    # c = covered, g = gap: day 1 is before the first row, days 4 and 9 are short holes,
    # days 11-13 a run of three, day 16 is after the mark.
    states = ["gap" if mark == "g" else "covered" for mark in "gccgccccgcgggccg"]
    found = settled_empty(
        _cells(states), mark="2026-01-15", first="2026-01-02", owed={"2026-01-09"}, long_run=3
    )
    assert found == ["2026-01-04"]


def test_a_dataset_dense_in_practice_keeps_its_holes_as_gaps():
    from cnequity.quality.planned_coverage import settled_empty

    states = ["covered"] * 20 + ["gap"] + ["covered"] * 7
    found = settled_empty(
        _cells(states), mark="2026-01-28", first="2026-01-01", owed=set(), long_run=20
    )
    assert found == []


def test_daily_bars_are_owed_per_symbol_and_only_a_print_covers_a_cell():
    bars = _of("daily_bars")
    assert (bars.grain, bars.entity) == ("session", "symbol")
    assert bars.counts == "volume > 0 OR volume IS NULL"


def test_a_derived_dataset_names_inputs_that_exist_and_never_itself():
    from cnequity.domain.datasets import DATASETS

    assert _of("minute_bars_15m").inputs == ("minute_bars_5m",)
    assert "daily_bars" in _of("industry_index").inputs
    for name, spec in DATASETS.items():
        for source in spec.derived_from:
            assert source in DATASETS, (name, source)
            assert name not in DATASETS[source].derived_from, (name, source)
