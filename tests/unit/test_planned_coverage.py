"""The cells a dataset owes, derived from what its spec already declares."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

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


def test_an_empty_daily_bar_answer_is_final_only_after_three_days():
    from datetime import timedelta

    assert _of("daily_bars").settle == timedelta(days=3)
    assert _of("announcement_index").settle == timedelta(0)


def test_a_market_fact_bounds_margin_trading():
    assert _of("margin_trading").start == date(2010, 3, 31)


def test_where_this_lake_begins_bounds_the_cells_too():
    lake = SimpleNamespace(
        history_from={"fund_flow": date(2023, 9, 11), "trade_ticks": date(2020, 1, 1)}
    )
    assert coverage_of(DATASETS["fund_flow"], TODAY, lake).start == date(2023, 9, 11)
    # The later of the two wins: the source serves trade ticks only from 2024.
    assert coverage_of(DATASETS["trade_ticks"], TODAY, lake).start == date(2024, 1, 2)
    assert coverage_of(DATASETS["block_trades"], TODAY, lake).start is None


def test_where_this_lake_ends_bounds_the_cells():
    lake = SimpleNamespace(history_until={"fund_flow": date(2026, 11, 4)})
    assert coverage_of(DATASETS["fund_flow"], TODAY, lake).end == date(2026, 11, 4)
    assert coverage_of(DATASETS["block_trades"], TODAY, lake).end is None


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
    from cnequity.quality.planned_coverage import dense_in_practice, settled_empty

    history = _cells(["covered"] * 20 + ["gap"] + ["covered"] * 7)
    assert dense_in_practice(history)
    # Over the last few days alone the same hole would look like an answer.
    recent = history.tail(10)
    assert not dense_in_practice(recent)
    assert settled_empty(
        recent, mark="2026-01-28", first="2026-01-19", owed=set(), long_run=20
    ) == ["2026-01-21"]
    assert (
        settled_empty(
            recent,
            mark="2026-01-28",
            first="2026-01-19",
            owed=set(),
            long_run=20,
            dense=dense_in_practice(history),
        )
        == []
    )


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


def test_a_derivation_is_owed_only_on_inputs_it_actually_reads():
    from cnequity.derive.adj_factors import INPUTS as ADJ
    from cnequity.derive.minute_resample import INPUTS as MINUTE
    from cnequity.derive.option_greeks import INPUTS as GREEKS
    from cnequity.domain.datasets import DATASETS, RESAMPLED_MINUTE_DATASETS

    assert set(DATASETS["adj_factors"].derived_from) == set(ADJ)
    assert set(DATASETS["option_greeks"].derived_from) == set(GREEKS)
    for name in RESAMPLED_MINUTE_DATASETS:
        assert set(DATASETS[name].derived_from) <= set(MINUTE)


def test_a_part_answered_day_outside_the_sync_window_still_settles(tmp_path):
    from datetime import UTC, datetime

    from cnequity.config import Config
    from cnequity.quality.planned_coverage import (
        Seed,
        clean_answer,
        record_answers,
        record_seeds,
    )
    from cnequity.storage.catalog_bridge import lake_root
    from equitylake_core import coverage as cov
    from equitylake_core.catalog import Catalog

    cfg = Config(data_root=tmp_path / "data")
    with Catalog(lake_root(cfg), create=True) as catalog:
        cov.prepare(catalog)
        catalog.execute("CREATE TABLE corporate_actions AS SELECT DATE '2026-09-17' AS ex_date")
    old = datetime(2026, 11, 17, tzinfo=UTC)
    record_seeds(cfg, [Seed("corporate_actions", "short", ("2026-09-17",), old, "partial")])
    # 2026-09-17 is more than SYNC_DAYS before the run; a clean fetch of it still settles it.
    settled = clean_answer(cfg, "corporate_actions", [date(2026, 9, 17)], datetime.now(UTC))
    assert settled is not None and settled["outcome"] == "complete"
    record_answers(cfg, "r1", [settled])
    with Catalog(lake_root(cfg), read_only=True) as catalog:
        latest = cov.attempted(catalog, "corporate_actions")
    assert latest.select("period", "outcome").rows() == [("2026-09-17", "complete")]
