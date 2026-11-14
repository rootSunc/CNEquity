"""A lake of declared datasets: rows in, partition files out, one snapshot per write."""

from __future__ import annotations

import os
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from equitylake_core.catalog import CatalogError
from equitylake_core.extensions import ExtensionUnavailableError
from equitylake_core.lake import Lake
from equitylake_core.spec import DatasetSpec, Registry, Rule
from equitylake_core.write import WriteRejected

BARS = DatasetSpec(
    name="bars",
    columns={"day": "DATE", "code": "VARCHAR", "low": "DOUBLE", "high": "DOUBLE"},
    key=("day", "code"),
    time="day",
    partition="month",
    sort=("code", "day"),
    rules=(Rule("high_not_below_low", "high >= low"),),
)
CODES = DatasetSpec(
    name="codes",
    columns={"code": "VARCHAR", "name": "TEXT", "seen_at": "TIMESTAMPTZ"},
    key=("code",),
)
FILINGS = DatasetSpec(
    name="filings",
    columns={"id": "VARCHAR", "filed": "DATE"},
    key=("id",),
    time="filed",
    partition="year",
)
REGISTRY = Registry([BARS, CODES, FILINGS])


@pytest.fixture
def lake(tmp_path):
    try:
        return Lake.init(tmp_path / "lake", REGISTRY)
    except ExtensionUnavailableError as exc:
        pytest.skip(str(exc))


def _bars(*rows):
    return pl.DataFrame(
        rows,
        schema={"day": pl.Date, "code": pl.String, "low": pl.Float64, "high": pl.Float64},
        orient="row",
    )


def _put(lake, dataset, frame, **kwargs):
    with lake.write("test") as write:
        write.put(dataset, frame)
        return write.commit(**kwargs)


def _files(lake, dataset):
    """The dataset's data files now, relative to its directory."""
    home = lake.root / "data" / "main" / dataset
    with lake.catalog() as catalog:
        return sorted(
            Path(path).relative_to(home).as_posix() for path in catalog.data_files(dataset) or []
        )


def _on_disk(lake, dataset):
    """Every file under the dataset's directory, also those only older snapshots hold."""
    return sorted((lake.root / "data" / "main" / dataset).rglob("*.parquet"))


def test_a_lake_must_exist_to_be_opened(tmp_path, lake):
    with pytest.raises(CatalogError, match="no lake at"):
        Lake(tmp_path / "nowhere", REGISTRY)
    assert Lake(lake.root, REGISTRY).load("bars").is_empty()
    # Making a lake where one already is leaves it alone.
    assert Lake.init(lake.root, REGISTRY).snapshot() == lake.snapshot()


def test_rows_come_back_in_the_declared_order_from_one_file_per_partition(lake):
    commit = _put(
        lake,
        "bars",
        _bars(
            (date(2024, 2, 1), "B", 1.0, 2.0),
            (date(2024, 1, 3), "B", 1.0, 2.0),
            (date(2024, 1, 3), "A", 1.0, 2.0),
            (date(2024, 1, 2), "B", 1.0, 2.0),
        ),
    )
    assert commit.rows == {"bars": 4}
    assert commit.partitions == {"bars": ("2024-01", "2024-02")}
    assert commit.snapshot == lake.snapshot()
    loaded = lake.load("bars")
    assert loaded.columns == ["day", "code", "low", "high"]
    assert loaded.select("day", "code").rows() == [
        (date(2024, 1, 2), "B"),
        (date(2024, 1, 3), "A"),
        (date(2024, 1, 3), "B"),
        (date(2024, 2, 1), "B"),
    ]
    files = _files(lake, "bars")
    assert [name.split("/")[0] for name in files] == ["2024-01", "2024-02"]
    # Inside a file the rows follow ``sort``: code first.
    january = pl.read_parquet(lake.root / "data" / "main" / "bars" / files[0])
    assert january["code"].to_list() == ["A", "B", "B"]


def test_a_published_file_is_read_only(lake):
    _put(lake, "bars", _bars((date(2024, 1, 2), "A", 1.0, 2.0)))
    if os.name == "nt":
        pytest.skip("read-only files cannot be deleted on Windows")
    path = lake.root / "data" / "main" / "bars" / _files(lake, "bars")[0]
    assert not os.access(path, os.W_OK)


def test_a_stored_key_is_replaced_and_only_its_partition_is_rewritten(lake):
    _put(
        lake,
        "bars",
        _bars((date(2024, 1, 2), "A", 1.0, 2.0), (date(2024, 2, 1), "A", 1.0, 2.0)),
    )
    before = _files(lake, "bars")
    commit = _put(
        lake, "bars", _bars((date(2024, 1, 2), "A", 1.0, 9.0), (date(2024, 1, 3), "A", 1.0, 2.0))
    )
    assert commit.partitions == {"bars": ("2024-01",)}
    assert commit.rows == {"bars": 2}
    after = _files(lake, "bars")
    assert after[1] == before[1] and after[0] != before[0] and len(after) == 2
    assert lake.load("bars")["high"].to_list() == [9.0, 2.0, 2.0]


def test_a_write_that_changes_nothing_makes_no_snapshot(lake):
    frame = _bars((date(2024, 1, 2), "A", 1.0, 2.0), (date(2024, 2, 1), "A", 1.0, 2.0))
    first = _put(lake, "bars", frame)
    assert _put(lake, "bars", frame) is None
    assert _put(lake, "bars", frame.head(1)) is None
    with lake.write() as write:
        assert write.commit() is None
        write.replace("bars", frame)
        assert write.commit() is None
    assert lake.snapshot() == first.snapshot
    assert [row["snapshot"] for row in lake.revisions("bars")] == [first.snapshot]


def test_replace_removes_the_rows_it_does_not_hold(lake):
    _put(
        lake,
        "bars",
        _bars((date(2024, 1, 2), "A", 1.0, 2.0), (date(2024, 2, 1), "A", 1.0, 2.0)),
    )
    with lake.write() as write:
        write.replace(
            "bars", _bars((date(2024, 1, 2), "A", 1.0, 2.0), (date(2024, 3, 1), "A", 1.0, 2.0))
        )
        commit = write.commit()
    assert commit.partitions == {"bars": ("2024-02", "2024-03")}
    assert lake.load("bars")["day"].to_list() == [date(2024, 1, 2), date(2024, 3, 1)]
    assert [name.split("/")[0] for name in _files(lake, "bars")] == ["2024-01", "2024-03"]


def test_several_puts_and_datasets_commit_as_one_snapshot(lake):
    seen = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
    before = lake.snapshot()
    with lake.write("both") as write:
        write.put("bars", _bars((date(2024, 1, 2), "A", 1.0, 2.0)))
        write.put(
            "bars", _bars((date(2024, 1, 2), "A", 1.0, 3.0), (date(2024, 1, 2), "B", 1.0, 2.0))
        )
        write.put("codes", pl.DataFrame({"code": ["A", "B"], "seen_at": [seen, seen]}))
        commit = write.commit(message="first day")
    assert commit.snapshot == before + 1
    assert commit.rows == {"bars": 2, "codes": 2}
    assert lake.load("bars")["high"].to_list() == [3.0, 2.0]
    codes = lake.load("codes")
    # A declared column the frame did not carry is NULL; an alias type is stored as its real one.
    assert codes["name"].to_list() == [None, None]
    assert codes.schema["name"] == pl.String
    assert codes["seen_at"].to_list() == [seen, seen]
    assert _files(lake, "codes")[0].startswith("part.")
    assert [(row["dataset"], row["run"], row["rows"]) for row in lake.revisions()] == [
        ("bars", "both", 2),
        ("codes", "both", 2),
    ]


def test_a_row_that_breaks_a_hard_rule_is_left_out_and_counted(lake):
    frame = pl.DataFrame(
        {
            "day": [date(2024, 1, 2)] * 5 + [None],
            "code": ["A", "B", "C", "D", "D", "E"],
            "low": [1.0, 5.0, float("nan"), 1.0, 1.0, 1.0],
            "high": [2.0, 4.0, 2.0, 2.0, 3.0, 2.0],
        }
    )
    with lake.write("rules") as write:
        assert write.put("bars", frame) == 1
        commit = write.commit()
    assert commit.rejected == {
        "bars": {
            "duplicate_key": 2,
            "missing_key": 1,
            "not_finite:low": 1,
            "rule:high_not_below_low": 1,
        }
    }
    assert lake.load("bars")["code"].to_list() == ["A"]
    assert lake.revisions("bars")[0]["rejected"] == 5
    with lake.catalog() as catalog:
        rejects = catalog.execute(
            "SELECT reason, rows, sample FROM _rejects ORDER BY reason"
        ).fetchall()
    assert [row[:2] for row in rejects] == [
        ("duplicate_key", 2),
        ("missing_key", 1),
        ("not_finite:low", 1),
        ("rule:high_not_below_low", 1),
    ]
    assert '"code":"B"' in rejects[3][2].replace(" ", "")


def test_a_value_of_the_wrong_type_is_a_rejected_row_and_a_missing_one_is_not(lake):
    frame = pl.DataFrame(
        {
            "day": ["2024-01-02", "2024-01-03", "not a day"],
            "code": ["A", "A", "A"],
            "low": ["1.5", None, "1"],
            "high": ["2", "x", "2"],
        }
    )
    with lake.write() as write:
        write.put("bars", frame)
        commit = write.commit()
    assert commit.rejected == {"bars": {"bad_type:day": 1, "bad_type:high": 1}}
    # Identical rows are one row; a NULL operand leaves a rule undecided, which passes.
    with lake.write() as write:
        write.put(
            "bars", _bars((date(2024, 1, 4), "A", None, 2.0), (date(2024, 1, 4), "A", None, 2.0))
        )
        assert write.commit().rejected == {}
    assert lake.load("bars").rows() == [
        (date(2024, 1, 2), "A", 1.5, 2.0),
        (date(2024, 1, 4), "A", None, 2.0),
    ]


def test_rejects_alone_are_still_recorded(lake):
    with lake.write("bad") as write:
        assert write.put("bars", _bars((date(2024, 1, 2), "A", 5.0, 1.0))) == 0
        commit = write.commit()
    assert commit.rows == {} and commit.rejected == {"bars": {"rule:high_not_below_low": 1}}
    assert lake.revisions("bars") == []
    assert lake.load("bars").is_empty()


def test_a_frame_the_dataset_does_not_fit_is_a_programming_error(lake):
    with lake.write() as write:
        with pytest.raises(ValueError, match="undeclared column.*volume"):
            write.put(
                "bars", pl.DataFrame({"day": [date(2024, 1, 2)], "code": ["A"], "volume": [1]})
            )
        with pytest.raises(ValueError, match="lacks key column.*code"):
            write.put("bars", pl.DataFrame({"day": [date(2024, 1, 2)]}))
        with pytest.raises(KeyError, match="no dataset named"):
            write.put("nothing", pl.DataFrame({"a": [1]}))
        assert write.commit() is None


def test_a_dataset_another_writer_published_to_refuses_the_commit(lake):
    _put(lake, "bars", _bars((date(2024, 1, 2), "A", 1.0, 2.0)))
    with lake.write("slow") as slow:
        slow.put("bars", _bars((date(2024, 1, 3), "A", 1.0, 2.0)))
        original = slow._stage

        def stage_then_lose_the_race(spec, pending):
            changes = original(spec, pending)
            _put(lake, "bars", _bars((date(2024, 1, 4), "A", 1.0, 2.0)))
            return changes

        slow._stage = stage_then_lose_the_race
        with pytest.raises(WriteRejected, match="another writer"):
            slow.commit()
    assert lake.load("bars")["day"].to_list() == [date(2024, 1, 2), date(2024, 1, 4)]
    # The refused write left no file behind: the two there are the other writer's.
    assert len(_files(lake, "bars")) == 1 and len(_on_disk(lake, "bars")) == 2
    assert not any((lake.root / "staging").iterdir())


def test_a_column_type_the_spec_changed_refuses_the_write(lake):
    _put(lake, "bars", _bars((date(2024, 1, 2), "A", 1.0, 2.0)))
    changed = DatasetSpec(
        name="bars",
        columns={"day": "DATE", "code": "VARCHAR", "low": "DOUBLE", "high": "VARCHAR"},
        key=("day", "code"),
        time="day",
        partition="month",
    )
    other = Lake(lake.root, Registry([changed]))
    with other.write() as write:
        write.put("bars", pl.DataFrame({"day": [date(2024, 3, 1)], "code": ["A"], "high": ["x"]}))
        with pytest.raises(
            WriteRejected, match="high is DOUBLE in the lake and VARCHAR in the spec"
        ):
            write.commit()
    assert len(_on_disk(lake, "bars")) == 1


def test_a_column_the_spec_gained_reads_as_null_in_older_files(lake):
    _put(lake, "bars", _bars((date(2024, 1, 2), "A", 1.0, 2.0)))
    grown = DatasetSpec(
        name="bars",
        columns={**BARS.columns, "volume": "BIGINT"},
        key=("day", "code"),
        time="day",
        partition="month",
    )
    other = Lake(lake.root, Registry([grown]))
    with other.write() as write:
        write.put("bars", pl.DataFrame({"day": [date(2024, 2, 1)], "code": ["A"], "volume": [7]}))
        write.commit()
    assert other.load("bars", columns=["day", "volume"]).rows() == [
        (date(2024, 1, 2), None),
        (date(2024, 2, 1), 7),
    ]


def test_one_key_cannot_be_stored_in_two_partitions(lake):
    _put(lake, "filings", pl.DataFrame({"id": ["a"], "filed": [date(2023, 5, 1)]}))
    with lake.write() as write:
        write.put("filings", pl.DataFrame({"id": ["a"], "filed": [date(2024, 5, 1)]}))
        with pytest.raises(WriteRejected, match="more than one partition"):
            write.commit()
    assert lake.load("filings")["filed"].to_list() == [date(2023, 5, 1)]


def test_a_read_takes_a_date_range_columns_and_a_snapshot(lake):
    first = _put(
        lake,
        "bars",
        _bars(
            (date(2024, 1, 2), "A", 1.0, 2.0),
            (date(2024, 1, 31), "A", 1.0, 2.0),
            (date(2024, 2, 1), "A", 1.0, 2.0),
        ),
    )
    _put(lake, "bars", _bars((date(2024, 3, 1), "A", 1.0, 2.0)))
    assert lake.load("bars", start="2024-01-31", end=date(2024, 2, 1))["day"].to_list() == [
        date(2024, 1, 31),
        date(2024, 2, 1),
    ]
    assert lake.load("bars", start="2024-02-02", end="2024-02-28").is_empty()
    assert lake.load("bars", columns=["code", "day"]).columns == ["code", "day"]
    assert lake.load("bars", snapshot=first.snapshot).height == 3
    assert lake.load("bars").height == 4
    with pytest.raises(ValueError, match="no column"):
        lake.load("bars", columns=["volume"])
    with pytest.raises(ValueError, match="no time column"):
        lake.load("codes", start="2024-01-01")


def test_a_timestamp_range_is_taken_on_utc_dates(lake):
    late = datetime(2024, 1, 2, 23, 59, tzinfo=timezone.utc)
    early = datetime(2024, 1, 3, 0, 0, tzinfo=timezone.utc)
    events = DatasetSpec(
        name="events",
        columns={"at": "TIMESTAMPTZ", "what": "VARCHAR"},
        key=("at",),
        time="at",
        partition="day",
    )
    other = Lake(lake.root, Registry([events]))
    with other.write() as write:
        write.put("events", pl.DataFrame({"at": [late, early], "what": ["x", "y"]}))
        assert write.commit().partitions == {"events": ("2024-01-02", "2024-01-03")}
    assert other.load("events", end="2024-01-02")["what"].to_list() == ["x"]
    assert other.load("events", start="2024-01-03")["what"].to_list() == ["y"]


def test_the_summary_says_how_much_each_dataset_holds(lake):
    assert [(item.dataset, item.rows, item.snapshot) for item in lake.summary()] == [
        ("bars", 0, None),
        ("codes", 0, None),
        ("filings", 0, None),
    ]
    commit = _put(
        lake,
        "bars",
        _bars((date(2024, 1, 2), "A", 1.0, 2.0), (date(2024, 2, 1), "A", 1.0, 2.0)),
    )
    bars = lake.summary()[0]
    assert (bars.rows, bars.files, bars.first, bars.last) == (2, 2, "2024-01-02", "2024-02-01")
    assert bars.snapshot == commit.snapshot


def test_files_laid_out_some_other_way_are_not_written_over(lake):
    with lake.catalog(read_only=False) as catalog:
        catalog.execute("CREATE TABLE bars (day DATE, code VARCHAR, low DOUBLE, high DOUBLE)")
        catalog.execute("INSERT INTO bars VALUES (DATE '2024-01-02', 'A', 1.0, 2.0)")
    assert not Path(_files(lake, "bars")[0]).parent.name
    with lake.write() as write:
        write.put("bars", _bars((date(2024, 1, 3), "A", 1.0, 2.0)))
        with pytest.raises(CatalogError, match="not laid out as a partition file"):
            write.commit()
