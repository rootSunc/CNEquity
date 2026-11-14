"""A dataset declaration: its defaults, what it refuses, and its partition labels."""

from __future__ import annotations

from datetime import date

import pytest

from equitylake_core.spec import DatasetSpec, Registry, Rule, SpecError

BARS = DatasetSpec(
    name="bars",
    columns={"day": "DATE", "code": "VARCHAR", "close": "DOUBLE"},
    key=("day", "code"),
    time="day",
    partition="month",
    sort=("code", "day"),
)


def test_defaults_put_time_first_in_the_read_order():
    assert BARS.order == ("day", "code")
    assert BARS.sort == ("code", "day")
    plain = DatasetSpec(name="codes", columns={"code": "VARCHAR"}, key=("code",))
    assert plain.sort == plain.order == ("code",)
    assert plain.partition == "none"


@pytest.mark.parametrize(
    "changes, said",
    [
        ({"name": "Daily-Bars"}, "lower-case"),
        ({"columns": {}}, "no columns"),
        ({"key": ()}, "no key"),
        ({"key": ("day", "missing")}, "key column 'missing'"),
        ({"key": ("day", "day")}, "named twice"),
        ({"time": None}, "needs a time column"),
        ({"partition": "week"}, "unknown partition"),
        ({"sort": ("nope",)}, "sort column 'nope'"),
        ({"rules": (Rule("a", "close > 0"), Rule("a", "close < 9"))}, "share a name"),
    ],
)
def test_a_contradictory_declaration_is_refused(changes, said):
    fields = {
        "name": "bars",
        "columns": {"day": "DATE", "code": "VARCHAR"},
        "key": ("day", "code"),
        "time": "day",
        "partition": "month",
    }
    with pytest.raises(SpecError, match=said):
        DatasetSpec(**{**fields, **changes})


@pytest.mark.parametrize(
    "partition, label, period",
    [
        ("year", "2024", (date(2024, 1, 1), date(2024, 12, 31))),
        ("month", "2024-02", (date(2024, 2, 1), date(2024, 2, 29))),
        ("day", "2024-02-29", (date(2024, 2, 29), date(2024, 2, 29))),
    ],
)
def test_a_label_names_the_period_it_covers(partition, label, period):
    spec = DatasetSpec(
        name="t", columns={"day": "DATE"}, key=("day",), time="day", partition=partition
    )
    assert spec.label(date(2024, 2, 29)) == label
    assert spec.period(label) == period


def test_a_directory_that_is_not_a_label_has_no_period():
    assert BARS.period("2024") is None
    assert BARS.period("2024-13") is None
    assert BARS.period("part") is None
    plain = DatasetSpec(name="codes", columns={"code": "VARCHAR"}, key=("code",))
    assert plain.label(date(2024, 1, 1)) == ""
    assert plain.period("") is None


def test_a_key_without_the_time_column_can_span_partitions():
    assert not BARS.key_spans_partitions
    filings = DatasetSpec(
        name="filings",
        columns={"id": "VARCHAR", "filed": "DATE"},
        key=("id",),
        time="filed",
        partition="year",
    )
    assert filings.key_spans_partitions


def test_a_registry_holds_each_dataset_once():
    registry = Registry([BARS])
    assert registry["bars"] is BARS
    assert "bars" in registry and "other" not in registry
    assert registry.names() == ["bars"] and len(registry) == 1
    with pytest.raises(KeyError, match="no dataset named 'other'"):
        registry["other"]
    with pytest.raises(SpecError, match="declared twice"):
        Registry([BARS, BARS])
