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
