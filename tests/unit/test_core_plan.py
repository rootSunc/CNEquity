"""The coverage ledger and the planner: owed cells, covered cells, standing empty answers."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from equitylake_core import coverage as cov
from equitylake_core.coverage import Cell, Coverage, CoverageError
from equitylake_core.extensions import ExtensionUnavailableError
from equitylake_core.lake import Lake
from equitylake_core.plan import Declared, Scope, owed, plan, plan_all, plan_dataset
from equitylake_core.spec import DatasetSpec, Registry, SpecError

UTC = timezone.utc

BARS = DatasetSpec(
    name="bars",
    columns={"day": "DATE", "code": "VARCHAR", "close": "DOUBLE"},
    key=("day", "code"),
    time="day",
    partition="month",
    coverage=Coverage("session"),
)
#: Owed once per code per quarter; a report may come up to 60 days after the quarter.
REPORTS = DatasetSpec(
    name="reports",
    columns={"period_end": "DATE", "code": "VARCHAR", "filed_at": "TIMESTAMPTZ"},
    key=("period_end", "code"),
    time="period_end",
    partition="year",
    coverage=Coverage(
        "quarter", entity="code", settle=timedelta(days=60), recheck=timedelta(days=1)
    ),
)
CODES = DatasetSpec(
    name="codes",
    columns={"code": "VARCHAR", "name": "VARCHAR"},
    key=("code",),
    coverage=Coverage("week", by="write"),
)
NOTES = DatasetSpec(name="notes", columns={"id": "VARCHAR"}, key=("id",))
REGISTRY = Registry([BARS, REPORTS, CODES, NOTES])


class Weekdays:
    def sessions(self, start, end):
        days = (start + timedelta(days=n) for n in range((end - start).days + 1))
        return [day for day in days if day.weekday() < 5]


class Listed:
    """``A`` throughout, ``B`` from 2026-05-01, ``C`` until 2026-02-15."""

    def members(self, dataset, start, end):
        return pl.DataFrame(
            {
                "entity": ["A", "B", "C"],
                "first": [None, date(2026, 5, 1), None],
                "last": [None, None, date(2026, 2, 15)],
            },
            schema={"entity": pl.String, "first": pl.Date, "last": pl.Date},
        )


@pytest.fixture
def lake(tmp_path):
    try:
        return Lake.init(tmp_path / "lake", REGISTRY)
    except ExtensionUnavailableError as exc:
        pytest.skip(str(exc))


def _bars(*days):
    return pl.DataFrame(
        {"day": list(days), "code": ["A"] * len(days), "close": [1.0] * len(days)},
        schema={"day": pl.Date, "code": pl.String, "close": pl.Float64},
    )


def _states(found, dataset):
    return {
        (period, entity) if entity else period: state
        for period, entity, state in found.datasets[dataset].cells.iter_rows()
    }


@pytest.mark.parametrize(
    ("grain", "day", "name", "first", "last"),
    [
        ("day", date(2026, 11, 13), "2026-11-13", date(2026, 11, 13), date(2026, 11, 13)),
        ("week", date(2027, 1, 1), "2026-W53", date(2026, 12, 28), date(2027, 1, 3)),
        ("month", date(2024, 2, 29), "2024-02", date(2024, 2, 1), date(2024, 2, 29)),
        ("quarter", date(2026, 12, 31), "2026Q4", date(2026, 10, 1), date(2026, 12, 31)),
        ("year", date(2026, 7, 1), "2026", date(2026, 1, 1), date(2026, 12, 31)),
    ],
)
def test_a_label_names_the_period_its_day_falls_in(lake, grain, day, name, first, last):
    assert cov.label(grain, day) == name
    assert cov.span(grain, name) == (first, last)
    in_sql = lake.catalog().execute(
        f"SELECT {cov.label_sql(grain, 'DATE ' + repr(day.isoformat()))}"
    )
    assert in_sql.fetchone()[0] == name


def test_periods_cover_a_range_without_a_hole():
    assert cov.periods("quarter", date(2025, 11, 20), date(2026, 4, 1)) == [
        "2025Q4",
        "2026Q1",
        "2026Q2",
    ]
    with pytest.raises(CoverageError):
        cov.span("quarter", "2026Q5")


def test_a_declaration_that_contradicts_itself_is_refused():
    with pytest.raises(CoverageError):
        Coverage("day", by="write", entity="code")
    with pytest.raises(CoverageError):
        Coverage("day", lag=timedelta(days=-1))
    with pytest.raises(SpecError):
        DatasetSpec(name="x", columns={"id": "VARCHAR"}, key=("id",), coverage=Coverage("day"))
    with pytest.raises(SpecError):
        DatasetSpec(
            name="x",
            columns={"day": "DATE"},
            key=("day",),
            time="day",
            coverage=Coverage("day", entity="code"),
        )


def test_a_session_without_rows_is_a_gap_and_one_asked_about_is_empty(lake):
    with lake.write("first") as write:
        write.put("bars", _bars(date(2026, 11, 9), date(2026, 11, 11)))
        write.empty("bars", ["2026-11-10"])
        write.commit()
    scope = Scope(date(2026, 11, 7), date(2026, 11, 12), datasets=("bars",))
    found = plan(lake, scope, calendar=Weekdays())
    assert _states(found, "bars") == {
        "2026-11-09": "covered",
        "2026-11-10": "empty",
        "2026-11-11": "covered",
        "2026-11-12": "gap",
    }
    assert found.gaps() == [Cell("bars", "2026-11-12")]
    assert found.counts()["bars"] == {
        "covered": 2,
        "empty": 1,
        "gap": 1,
        "blocked": 0,
        "not_due": 0,
    }


def test_redo_widens_what_is_asked_again(lake):
    with lake.write("first") as write:
        write.put("bars", _bars(date(2026, 11, 9)))
        write.empty("bars", ["2026-11-10"])
        write.commit()
    days = (date(2026, 11, 9), date(2026, 11, 10))
    again = plan(lake, Scope(*days, redo="empty"), calendar=Weekdays(), universe=Listed())
    assert _states(again, "bars") == {"2026-11-09": "covered", "2026-11-10": "gap"}
    everything = plan(lake, Scope(*days, datasets=("bars",), redo="all"), calendar=Weekdays())
    assert set(_states(everything, "bars").values()) == {"gap"}


def test_rows_arriving_later_cover_a_cell_recorded_empty(lake):
    with lake.write("early") as write:
        write.empty("bars", ["2026-11-10"])
        assert write.commit() is not None  # an attempt alone is worth a snapshot
    with lake.write("late") as write:
        write.put("bars", _bars(date(2026, 11, 10)))
        write.commit()
    scope = Scope(date(2026, 11, 10), date(2026, 11, 10), datasets=("bars",))
    assert _states(plan(lake, scope, calendar=Weekdays()), "bars") == {"2026-11-10": "covered"}


def test_cells_are_owed_per_entity_while_the_universe_holds_it(lake):
    scope = Scope(date(2026, 1, 1), date(2026, 6, 30), datasets=("reports",))
    cells = owed("reports", REPORTS.coverage, scope, universe=Listed())
    assert sorted(cells.select("period", "entity").iter_rows()) == [
        ("2026Q1", "A"),
        ("2026Q1", "C"),
        ("2026Q2", "A"),
        ("2026Q2", "B"),
    ]
    one = Scope(scope.start, scope.end, entities=frozenset({"B"}))
    only = owed("reports", REPORTS.coverage, one, universe=Listed())
    assert only.select("period", "entity").rows() == [("2026Q2", "B")]


def test_an_early_empty_answer_stands_for_a_while_and_a_late_one_for_good():
    # 2026Q1 ended 03-31. Asked on 04-20, inside the 60 days a report may take: "not yet".
    # 2025Q4 was asked about the same day, long after it ended: final.
    asked = datetime(2026, 4, 20, 12, tzinfo=UTC)
    attempts = pl.DataFrame(
        {"period": ["2026Q1", "2025Q4", "2026-04"], "entity": ["C", "A", ""], "at": [asked] * 3},
        schema={"period": pl.String, "entity": pl.String, "at": pl.Datetime("us", "UTC")},
    )

    def standing(now):
        return set(cov.standing(attempts, REPORTS.coverage, now).iter_rows())

    assert standing(asked + timedelta(hours=1)) == {("2026Q1", "C"), ("2025Q4", "A")}
    assert standing(asked + timedelta(days=2)) == {("2025Q4", "A")}


def test_a_day_ends_at_the_market_midnight_not_utc():
    # 2026-11-17 19:27 UTC is already 03:27 on 11-18 in Shanghai: the 17th is over there.
    shanghai = ZoneInfo("Asia/Shanghai")
    asked = datetime(2026, 11, 17, 19, 27, tzinfo=UTC)
    attempts = pl.DataFrame(
        {"period": ["2026-11-17"], "entity": [""], "at": [asked]},
        schema={"period": pl.String, "entity": pl.String, "at": pl.Datetime("us", "UTC")},
    )
    later = asked + timedelta(minutes=1)
    assert cov.standing(attempts, Coverage("day"), later).is_empty()
    assert cov.standing(attempts, Coverage("day"), later, shanghai)["period"].to_list() == [
        "2026-11-17"
    ]
    assert cov.day_start(date(2026, 11, 18), shanghai) == datetime(2026, 11, 17, 16, tzinfo=UTC)


def test_a_refresh_after_the_market_midnight_counts_for_the_new_day(lake):
    shanghai = ZoneInfo("Asia/Shanghai")
    day = date(2026, 11, 18)
    scope = Scope(day - timedelta(days=6), day, datasets=("codes",))
    # 01:00 on the 18th in Shanghai; the 17th still in UTC, the week (11-16..22) the same.
    written = {"codes": datetime(2026, 11, 17, 17, tzinfo=UTC)}
    declared = {"codes": Declared(Coverage("day", by="write"))}
    with lake.catalog() as catalog:
        found = plan_all(catalog, declared, scope, written=written, zone=shanghai)
        utc = plan_all(catalog, declared, scope, written=written)
    assert found["codes"].cells["state"].to_list() == ["covered"]
    assert utc["codes"].cells["state"].to_list() == ["gap"]


def test_a_final_empty_answer_is_not_asked_again(lake):
    with lake.write("first") as write:
        write.empty("reports", [("2025Q4", "A"), Cell("reports", "2025Q4", "C")])
        write.empty("reports", [("2025Q4", "A")])
        write.commit()
    with lake.catalog() as catalog:
        assert cov.attempted(catalog, "reports").height == 2
    scope = Scope(date(2025, 10, 1), date(2026, 3, 31), datasets=("reports",))
    assert _states(plan(lake, scope, universe=Listed()), "reports") == {
        ("2025Q4", "A"): "empty",
        ("2025Q4", "C"): "empty",
        ("2026Q1", "A"): "gap",
        ("2026Q1", "C"): "gap",
    }
    with lake.write("x") as write, pytest.raises(CoverageError):
        write.empty("reports", [Cell("bars", "2025Q4")])


def test_a_cell_is_not_due_before_its_period_and_lag_have_passed(lake):
    lagging = Coverage("month", lag=timedelta(days=10))
    scope = Scope(date(2026, 9, 1), date(2026, 11, 5))
    cells = owed("x", lagging, scope)
    assert dict(cells.select("period", "due").iter_rows()) == {
        "2026-09": True,
        "2026-10": False,  # ended 5 days ago, owed after 10
        "2026-11": False,
    }
    assert owed("x", Coverage("month", start=date(2026, 12, 1)), scope).is_empty()


def test_a_refreshed_dataset_owes_only_its_latest_cell(lake):
    today = datetime.now(UTC).date()
    this_week = cov.label("week", today)
    scope = Scope(today - timedelta(days=30), today + timedelta(days=6), datasets=("codes",))
    # The week has not ended, so with no lag it is not due until the scope reaches its end.
    assert _states(plan(lake, scope), "codes") == {this_week: "gap"}

    codes = pl.DataFrame({"code": ["A"], "name": ["a"]})
    with lake.write("refresh") as write:
        write.replace("codes", codes)
        write.commit()
    assert _states(plan(lake, scope), "codes") == {this_week: "covered"}
    forced = Scope(scope.start, scope.end, datasets=("codes",), redo="all")
    assert _states(plan(lake, forced), "codes") == {this_week: "gap"}
    # Next week owes a refresh of its own.
    later = Scope(scope.start, scope.end + timedelta(days=7), datasets=("codes",))
    assert set(_states(plan(lake, later), "codes").values()) == {"gap"}


def test_a_refresh_that_found_nothing_new_still_counts(lake):
    today = datetime.now(UTC).date()
    scope = Scope(today, today + timedelta(days=6), datasets=("codes",))
    with lake.write("unchanged") as write:
        write.replace(
            "codes",
            pl.DataFrame(
                {"code": [], "name": []}, schema_overrides={"code": pl.String, "name": pl.String}
            ),
        )
        assert write.commit() is None  # nothing new: no snapshot, and no trace of the refresh
    assert set(_states(plan(lake, scope), "codes").values()) == {"gap"}
    with lake.write("unchanged") as write:
        write.empty("codes", [cov.label("week", today)])
        write.commit()
    assert set(_states(plan(lake, scope), "codes").values()) == {"covered"}


def test_cells_past_what_the_source_serves_are_not_owed():
    retired = Coverage("month", start=date(2026, 2, 10), end=date(2026, 3, 31))
    cells = owed("x", retired, Scope(date(2026, 1, 1), date(2026, 6, 30)))
    assert cells["period"].to_list() == ["2026-02", "2026-03"]


def test_a_dataset_without_coverage_is_left_alone(lake):
    scope = Scope(date(2026, 11, 9), date(2026, 11, 9))
    found = plan(lake, scope, calendar=Weekdays(), universe=Listed())
    assert "notes" not in found.datasets
    with pytest.raises(ValueError, match="declares no coverage"):
        plan(lake, Scope(scope.start, scope.end, datasets=("notes",)))
    with pytest.raises(ValueError, match="no calendar"):
        plan(lake, Scope(scope.start, scope.end, datasets=("bars",)))


def test_a_timestamp_column_places_a_row_by_its_utc_date(lake):
    with lake.write("first") as write:
        write.put(
            "reports",
            pl.DataFrame(
                {
                    "period_end": [date(2026, 3, 31)],
                    "code": ["A"],
                    "filed_at": [datetime(2026, 4, 30, 23, 30, tzinfo=UTC)],
                }
            ),
        )
        write.commit()
    with lake.catalog() as catalog:
        by_filing = cov.stored(catalog, "reports", Coverage("month", column="filed_at"))
        assert by_filing.rows() == [("2026-04", "")]
        bounded = cov.stored(
            catalog,
            "reports",
            Coverage("month", column="filed_at"),
            start=date(2026, 5, 1),
            end=date(2026, 5, 31),
        )
        assert bounded.is_empty()


def test_a_column_that_holds_the_label_is_read_as_it_is(lake):
    with lake.write("first") as write:
        write.put("notes", pl.DataFrame({"id": ["2026Q1", "2026Q3"]}))
        write.commit()
    by_label = Coverage("quarter", column="id", labelled=True)
    with lake.catalog() as catalog:
        assert sorted(cov.stored(catalog, "notes", by_label)["period"]) == ["2026Q1", "2026Q3"]
        late = cov.stored(catalog, "notes", by_label, start=date(2026, 5, 1), end=date(2026, 12, 1))
        assert late["period"].to_list() == ["2026Q3"]


def test_cells_proved_by_file_statistics_agree_with_the_rows(lake):
    days = [date(2026, 10, 30), date(2026, 11, 2), date(2026, 11, 3)]
    with lake.write("first") as write:
        write.put("bars", _bars(*days))
        write.commit()
    with lake.catalog() as catalog:
        by_month = cov.stored(catalog, "bars", Coverage("month"), column="day")
        assert sorted(by_month["period"]) == ["2026-10", "2026-11"]
        by_day = cov.stored(catalog, "bars", Coverage("day"), column="day", start=days[1])
        assert sorted(by_day["period"]) == ["2026-11-02", "2026-11-03"]
        assert cov._split_by_statistics(catalog, "bars", "day", "month", None, None)[1] == []


def test_attempts_recorded_by_another_writer_count_like_its_own(lake):
    asked = datetime(2026, 11, 13, 12, tzinfo=UTC)
    rows = pl.DataFrame(
        {
            "dataset": ["bars", "bars"],
            "period": ["2026-11-10", "2026-11-11"],
            "entity": ["", ""],
            "run": ["seed", "seed"],
            "asked_at": [asked, asked],
        }
    )
    with lake.catalog(read_only=False) as catalog:
        assert cov.record(catalog, rows.clear()) is None
        assert cov.record(catalog, rows, message="seed") == catalog.snapshot()
        assert cov.attempted(catalog, "bars")["period"].sort().to_list() == [
            "2026-11-10",
            "2026-11-11",
        ]
    scope = Scope(date(2026, 11, 10), date(2026, 11, 12), datasets=("bars",))
    assert _states(plan(lake, scope, calendar=Weekdays()), "bars") == {
        "2026-11-10": "empty",
        "2026-11-11": "empty",
        "2026-11-12": "gap",
    }


def test_a_refreshed_dataset_can_be_told_when_it_was_written(lake):
    today = datetime.now(UTC).date()
    scope = Scope(today, today + timedelta(days=6), datasets=("codes",))
    with lake.catalog() as catalog:
        told = plan_dataset(catalog, "codes", CODES.coverage, scope, written=datetime.now(UTC))
        unknown = plan_dataset(catalog, "codes", CODES.coverage, scope, written=None)
    assert set(told.cells["state"]) == {"covered"}
    assert set(unknown.cells["state"]) == {"gap"}


def test_a_cell_answered_in_part_stays_owed_until_answered_whole(lake):
    with lake.write("short") as write:
        write.put("bars", _bars(date(2026, 11, 9), date(2026, 11, 10)))
        write.partial("bars", ["2026-11-10"])
        write.commit()
    scope = Scope(date(2026, 11, 9), date(2026, 11, 10), datasets=("bars",))
    assert _states(plan(lake, scope, calendar=Weekdays()), "bars") == {
        "2026-11-09": "covered",
        "2026-11-10": "gap",
    }
    with lake.write("whole") as write:
        write.complete("bars", ["2026-11-10"])
        write.commit()
    assert _states(plan(lake, scope, calendar=Weekdays()), "bars") == {
        "2026-11-09": "covered",
        "2026-11-10": "covered",
    }


DERIVED = DatasetSpec(
    name="derived",
    columns={"day": "DATE", "value": "DOUBLE"},
    key=("day",),
    time="day",
    partition="year",
    coverage=Coverage("session", inputs=("bars", "codes")),
)


def test_a_derived_cell_is_blocked_until_its_inputs_have_answered(tmp_path):
    try:
        lake = Lake.init(tmp_path / "lake", Registry([BARS, CODES, DERIVED]))
    except ExtensionUnavailableError as exc:
        pytest.skip(str(exc))
    with lake.write("inputs") as write:
        write.put("bars", _bars(date(2026, 11, 10)))
        write.empty("bars", ["2026-11-11"])
        write.put("derived", pl.DataFrame({"day": [date(2026, 11, 9)], "value": [1.0]}))
        write.commit()
    scope = Scope(date(2026, 11, 9), date(2026, 11, 12), datasets=("derived",))
    # codes keeps only its latest state and has no time column: it bounds nothing.
    assert _states(plan(lake, scope, calendar=Weekdays()), "derived") == {
        "2026-11-09": "covered",  # rows win, whatever the inputs say
        "2026-11-10": "gap",
        "2026-11-11": "gap",  # an empty input is an answer too
        "2026-11-12": "blocked",
    }


class Suspending(Listed):
    """``A`` is not owed a report for 2026Q2."""

    def excused(self, dataset, start, end):
        return pl.DataFrame({"period": ["2026Q2"], "entity": ["A"]})


def test_cells_the_universe_excuses_are_not_owed(lake):
    scope = Scope(date(2026, 4, 1), date(2026, 9, 30), datasets=("reports",))
    found = plan(lake, scope, universe=Suspending())
    assert ("2026Q2", "A") not in _states(found, "reports")
    assert _states(found, "reports")[("2026Q2", "B")] == "gap"


def test_an_attempt_with_an_unknown_outcome_is_refused(lake):
    rows = pl.DataFrame(
        {
            "dataset": ["bars"],
            "period": ["2026-11-10"],
            "entity": [""],
            "run": ["x"],
            "asked_at": [datetime.now(UTC)],
            "outcome": ["maybe"],
        }
    )
    with lake.catalog(read_only=False) as catalog, pytest.raises(CoverageError):
        cov.record(catalog, rows)


def test_a_day_still_being_published_is_owed_until_it_ends(lake):
    from dataclasses import replace

    shanghai = ZoneInfo("Asia/Shanghai")
    with lake.write("first") as write:
        write.put("bars", _bars(date(2026, 11, 19), date(2026, 11, 20)))
        write.commit()
    scope = Scope(date(2026, 11, 19), date(2026, 11, 20), datasets=("bars",))
    open_day = replace(BARS.coverage, open_day=True)
    # 17:30 on the 20th in Shanghai: the 19th is over, the 20th is not.
    now = datetime(2026, 11, 20, 9, 30, tzinfo=UTC)

    def states(coverage):
        with lake.catalog() as catalog:
            found = plan_dataset(
                catalog,
                "bars",
                coverage,
                scope,
                now=now,
                column="day",
                calendar=Weekdays(),
                zone=shanghai,
            )
        return dict(found.cells.select("period", "state").iter_rows())

    assert states(open_day) == {"2026-11-19": "covered", "2026-11-20": "gap"}
    assert states(BARS.coverage) == {"2026-11-19": "covered", "2026-11-20": "covered"}
