"""One lake write: partition replacements, checks and records commit as one snapshot."""

from __future__ import annotations

from datetime import date

import duckdb
import pytest

from equitylake_core.catalog import Catalog
from equitylake_core.extensions import ExtensionUnavailableError
from equitylake_core.write import Write, WriteRejected


@pytest.fixture
def lake(tmp_path):
    try:
        catalog = Catalog(tmp_path / "lake", create=True)
    except ExtensionUnavailableError as exc:
        pytest.skip(str(exc))
    yield catalog
    catalog.close()


def _file(path, rows, columns="symbol, trade_date, close"):
    path.parent.mkdir(parents=True, exist_ok=True)
    values = ", ".join("(" + ", ".join(str(cell) for cell in row) + ")" for row in rows)
    duckdb.sql(
        f"COPY (SELECT * FROM (VALUES {values}) t({columns})) TO '{path.as_posix()}' (FORMAT parquet)"
    )
    return path


JAN = "trade_date >= DATE '2024-01-01' AND trade_date < DATE '2024-02-01'"
FEB = "trade_date >= DATE '2024-02-01' AND trade_date < DATE '2024-03-01'"


def _bars(lake, tmp_path):
    write = Write(lake)
    write.replace(
        "bars",
        [
            _file(tmp_path / "a.parquet", [("'A'", "DATE '2024-01-02'", 1.0)]),
            _file(tmp_path / "b.parquet", [("'A'", "DATE '2024-02-01'", 2.0)]),
        ],
        order_by=("trade_date", "symbol"),
        partitioned_by=("year(trade_date)", "month(trade_date)"),
    )
    return write.commit(message="create")


def _rows(lake, table="bars"):
    return lake.execute(f"SELECT symbol, trade_date, close FROM {table} ORDER BY 2, 1").fetchall()


def test_a_replacement_changes_only_its_partition(lake, tmp_path):
    _bars(lake, tmp_path)
    write = Write(lake)
    write.replace(
        "bars",
        [
            _file(
                tmp_path / "jan.parquet",
                [("'A'", "DATE '2024-01-02'", 9.0), ("'B'", "DATE '2024-01-03'", 3.0)],
            )
        ],
        where=JAN,
    )
    result = write.commit(message="january")
    assert result.rows == {"bars": 2}
    assert _rows(lake) == [
        ("A", date(2024, 1, 2), 9.0),
        ("B", date(2024, 1, 3), 3.0),
        ("A", date(2024, 2, 1), 2.0),
    ]
    # Readers can scan the files directly: no delete files are left behind.
    assert lake.data_files("bars") is not None


def test_rows_outside_their_partition_refuse_the_whole_write(lake, tmp_path):
    _bars(lake, tmp_path)
    before = lake.snapshot()
    write = Write(lake)
    write.replace(
        "bars", [_file(tmp_path / "x.parquet", [("'A'", "DATE '2024-02-05'", 5.0)])], where=JAN
    )
    with pytest.raises(WriteRejected, match="outside the partition"):
        write.commit()
    assert lake.snapshot() == before
    assert len(_rows(lake)) == 2


def test_a_failed_check_commits_nothing(lake, tmp_path):
    _bars(lake, tmp_path)
    before = lake.snapshot()
    write = Write(lake)
    write.replace("bars", [], where=FEB)
    write.record("_writes", {"table": "bars", "note": "drop february"})
    write.check(
        "rows kept",
        lambda catalog: (
            "february vanished"
            if catalog.execute(f"SELECT count(*) FROM bars WHERE {FEB}").fetchone()[0] == 0
            else None
        ),
    )
    with pytest.raises(WriteRejected, match="rows kept: february vanished"):
        write.commit()
    assert lake.snapshot() == before
    assert len(_rows(lake)) == 2 and not lake.has_table("_writes")


def test_tables_and_records_share_one_snapshot(lake, tmp_path):
    _bars(lake, tmp_path)
    write = Write(lake)
    write.replace(
        "bars", [_file(tmp_path / "f.parquet", [("'A'", "DATE '2024-02-01'", 7.0)])], where=FEB
    )
    write.replace(
        "actions",
        [
            _file(
                tmp_path / "act.parquet",
                [("'A'", "DATE '2024-02-01'", 0.5)],
                columns="symbol, ex_date, cash",
            )
        ],
    )
    write.record("_writes", {"table": "bars", "revision": 2, "unique": True})
    result = write.commit(message="two tables")
    at = f"AT (VERSION => {result.snapshot})"
    assert lake.execute(f"SELECT close FROM bars {at} WHERE {FEB}").fetchall() == [(7.0,)]
    assert lake.execute(f"SELECT count(*) FROM actions {at}").fetchone()[0] == 1
    assert lake.execute(f'SELECT revision, "unique" FROM _writes {at}').fetchall() == [(2, True)]
    # One snapshot earlier, none of it exists.
    earlier = f"AT (VERSION => {result.snapshot - 1})"
    assert lake.execute(f"SELECT close FROM bars {earlier} WHERE {FEB}").fetchall() == [(2.0,)]
    with pytest.raises(duckdb.CatalogException):
        lake.execute(f"SELECT count(*) FROM actions {earlier}")


def test_a_column_new_to_the_files_is_added(lake, tmp_path):
    _bars(lake, tmp_path)
    write = Write(lake)
    write.replace(
        "bars",
        [
            _file(
                tmp_path / "v.parquet",
                [("'A'", "DATE '2024-02-01'", 2.0, 100)],
                columns="symbol, trade_date, close, volume",
            )
        ],
        where=FEB,
    )
    write.commit()
    assert lake.execute("SELECT volume FROM bars ORDER BY trade_date").fetchall() == [
        (None,),
        (100,),
    ]


DAY = "trade_date = DATE '2024-01-03'"


def test_replacing_part_of_a_file_is_one_file_complete_snapshot(lake, tmp_path):
    # One unpartitioned file holds three days; only one of them is replaced.
    write = Write(lake)
    write.replace(
        "bars",
        [
            _file(
                tmp_path / "all.parquet",
                [
                    ("'A'", "DATE '2024-01-02'", 1.0),
                    ("'A'", "DATE '2024-01-03'", 2.0),
                    ("'A'", "NULL::DATE", 4.0),
                ],
            )
        ],
    )
    created = write.commit().snapshot
    write = Write(lake)
    write.replace(
        "bars", [_file(tmp_path / "day.parquet", [("'B'", "DATE '2024-01-03'", 3.0)])], where=DAY
    )
    result = write.commit()
    assert result.snapshot == created + 1 == lake.snapshot()
    # The commit's own snapshot can be read from its files, with no fold after it.
    assert lake.data_files("bars", result.snapshot) is not None
    assert not lake.has_deletes("bars")
    # The other day stays, and so does the row the predicate cannot decide.
    assert lake.execute("SELECT symbol, close FROM bars ORDER BY close").fetchall() == [
        ("A", 1.0),
        ("B", 3.0),
        ("A", 4.0),
    ]


def test_removing_part_of_a_file_keeps_the_rest(lake, tmp_path):
    write = Write(lake)
    write.replace(
        "bars",
        [
            _file(
                tmp_path / "all.parquet",
                [("'A'", "DATE '2024-01-02'", 1.0), ("'A'", "DATE '2024-01-03'", 2.0)],
            )
        ],
    )
    write.commit()
    write = Write(lake)
    write.replace("bars", [], where=DAY)
    result = write.commit()
    assert lake.data_files("bars", result.snapshot) is not None
    assert _rows(lake) == [("A", date(2024, 1, 2), 1.0)]


def test_a_record_can_be_built_inside_the_write(lake, tmp_path):
    _bars(lake, tmp_path)
    write = Write(lake)
    write.replace("bars", [], where=FEB)
    write.record(
        "_writes",
        lambda catalog: {"rows": catalog.execute("SELECT count(*) FROM bars").fetchone()[0]},
    )
    write.commit()
    assert lake.execute("SELECT rows FROM _writes").fetchall() == [(1,)]


def test_instants_read_back_in_utc_whatever_the_machine_zone(lake, tmp_path, monkeypatch):
    monkeypatch.setenv("TZ", "Asia/Shanghai")
    path = tmp_path / "t.parquet"
    duckdb.sql(
        "COPY (SELECT 'A' AS symbol, TIMESTAMPTZ '2024-01-02 03:04:05+00' AS fetched_at) "
        f"TO '{path.as_posix()}' (FORMAT parquet)"
    )
    write = Write(lake)
    write.replace("stamps", [path])
    write.commit()

    frame = lake.relation("SELECT * FROM stamps").pl()
    assert str(frame.schema["fetched_at"].time_zone) == "UTC"
    assert lake.execute("SELECT current_setting('TimeZone')").fetchone()[0] == "UTC"


def _placed(lake, table, name, rows, columns="symbol, trade_date, close"):
    return _file(lake.table_dir(table) / name, rows, columns)


def test_a_swap_registers_files_as_they_are_and_drops_files_whole(lake, tmp_path):
    jan = _placed(
        lake, "bars", "2024-01/part.aaaaaaaa.parquet", [("'A'", "DATE '2024-01-02'", 1.0)]
    )
    feb = _placed(
        lake, "bars", "2024-02/part.bbbbbbbb.parquet", [("'A'", "DATE '2024-02-01'", 2.0)]
    )
    write = Write(lake)
    write.swap("bars", add=[jan, feb])
    assert write.commit(message="create").rows == {"bars": 2}
    first = lake.snapshot()
    # The files are the table's data files themselves: nothing was written again.
    assert sorted(lake.data_files("bars")) == sorted([jan.as_posix(), feb.as_posix()])

    again = _placed(
        lake, "bars", "2024-02/part.cccccccc.parquet", [("'A'", "DATE '2024-02-01'", 2.5)]
    )
    write = Write(lake)
    write.swap("bars", add=[again], remove=[feb])
    write.commit(message="restate february")

    assert _rows(lake) == [("A", date(2024, 1, 2), 1.0), ("A", date(2024, 2, 1), 2.5)]
    assert lake.snapshot() == first + 1 and not lake.has_deletes("bars")
    assert sorted(lake.data_files("bars")) == sorted([jan.as_posix(), again.as_posix()])
    # The earlier snapshot still reads the file it had.
    old = lake.execute(f"SELECT close FROM bars AT (VERSION => {first}) ORDER BY 1").fetchall()
    assert old == [(1.0,), (2.0,)]


def test_a_swap_refuses_a_file_that_is_no_longer_the_tables(lake, tmp_path):
    jan = _placed(
        lake, "bars", "2024-01/part.aaaaaaaa.parquet", [("'A'", "DATE '2024-01-02'", 1.0)]
    )
    write = Write(lake)
    write.swap("bars", add=[jan])
    write.commit()
    write = Write(lake)
    write.swap("bars", remove=[lake.table_dir("bars") / "2024-01" / "gone.parquet"])
    with pytest.raises(WriteRejected, match="not data files of the table"):
        write.commit()
    assert _rows(lake) == [("A", date(2024, 1, 2), 1.0)]


def test_a_swap_drops_a_file_named_with_either_separator(lake, tmp_path):
    """DuckLake on Windows reports a registered file back with backslashes."""
    jan = _placed(
        lake, "bars", "2024-01/part.aaaaaaaa.parquet", [("'A'", "DATE '2024-01-02'", 1.0)]
    )
    write = Write(lake)
    write.swap("bars", add=[jan])
    write.commit()
    again = _placed(
        lake, "bars", "2024-01/part.cccccccc.parquet", [("'A'", "DATE '2024-01-02'", 1.5)]
    )
    write = Write(lake)
    write.swap("bars", add=[again], remove=[jan.as_posix().replace("/", "\\")])
    write.commit()
    assert _rows(lake) == [("A", date(2024, 1, 2), 1.5)]
    assert lake.data_files("bars") == [again.as_posix()]


def test_a_directory_name_is_never_read_as_a_column_value(lake, tmp_path):
    odd = _placed(
        lake,
        "bars",
        "trade_date=1999-01/part.aaaaaaaa.parquet",
        [("'A'", "DATE '2024-01-02'", 1.0)],
    )
    write = Write(lake)
    write.swap("bars", add=[odd])
    write.commit()
    assert _rows(lake) == [("A", date(2024, 1, 2), 1.0)]


def test_a_file_that_does_not_fit_is_found_before_the_write_and_conformed(lake, tmp_path):
    jan = _placed(
        lake, "bars", "2024-01/part.aaaaaaaa.parquet", [("'A'", "DATE '2024-01-02'", 1.0)]
    )
    write = Write(lake)
    write.swap("bars", add=[jan])
    write.commit()
    # A wider type than the table's column: the catalog would refuse it mid-transaction.
    wide = _file(tmp_path / "wide.parquet", [("'B'", "DATE '2024-01-03'", "CAST(7 AS HUGEINT)")])
    assert lake.file_fits("bars", jan) and not lake.file_fits("bars", wide)
    target = lake.table_dir("bars") / "2024-01" / "part.dddddddd.parquet"
    lake.conform_file("bars", wide, target)
    assert lake.file_fits("bars", target)
    write = Write(lake)
    write.swap("bars", add=[target])
    write.commit()
    assert _rows(lake)[-1] == ("B", date(2024, 1, 3), 7.0)


def test_a_file_outside_the_lake_is_not_registered(lake, tmp_path):
    stray = _file(tmp_path / "stray.parquet", [("'A'", "DATE '2024-01-02'", 1.0)])
    write = Write(lake)
    write.swap("bars", add=[stray])
    with pytest.raises(Exception, match="not under this lake's data path"):
        write.commit()
    assert not lake.has_table("bars")


def test_a_created_table_has_the_declared_types_before_its_first_file(lake, tmp_path):
    write = Write(lake)
    write.create("bars", {"symbol": "VARCHAR", "trade_date": "DATE", "close": "DOUBLE"})
    target = _file(
        lake.table_dir("bars") / "part.parquet", [("'A'", "DATE '2024-01-02'", "1::FLOAT")]
    )
    write.swap("bars", add=[target])
    assert write.commit().rows == {"bars": 1}
    # The file's single float is read as the table's double, not the other way round.
    assert lake.column_types("bars") == {
        "symbol": "VARCHAR",
        "trade_date": "DATE",
        "close": "DOUBLE",
    }
    # Naming a table that exists changes nothing about it.
    again = Write(lake)
    again.create("bars", {"symbol": "BIGINT"})
    again.record("_notes", {"note": "x"})
    again.commit()
    assert lake.column_types("bars")["symbol"] == "VARCHAR"
