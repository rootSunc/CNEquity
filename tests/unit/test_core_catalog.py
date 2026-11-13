"""The lake catalog: creation, snapshots, transactions, Parquet import, concurrent readers."""

from __future__ import annotations

import sqlite3
import subprocess
import sys
import textwrap
from datetime import date

import duckdb
import pytest

from equitylake_core.catalog import CATALOG_FILE, Catalog, CatalogError
from equitylake_core.extensions import ExtensionUnavailableError


@pytest.fixture
def lake(tmp_path):
    try:
        catalog = Catalog(tmp_path / "lake", create=True)
    except ExtensionUnavailableError as exc:  # offline machine without the extensions
        pytest.skip(str(exc))
    yield catalog
    catalog.close()


def _write(path, rows, columns="symbol, trade_date, close"):
    path.parent.mkdir(parents=True, exist_ok=True)
    values = ", ".join("(" + ", ".join(str(cell) for cell in row) + ")" for row in rows)
    duckdb.sql(
        f"COPY (SELECT * FROM (VALUES {values}) t({columns})) TO '{path.as_posix()}' (FORMAT parquet)"
    )


def test_a_missing_lake_is_an_error_unless_created(tmp_path):
    with pytest.raises(CatalogError, match="no lake catalog"):
        Catalog(tmp_path / "nowhere")


def test_the_catalog_database_is_in_wal_mode(lake):
    con = sqlite3.connect(lake.root / CATALOG_FILE)
    assert con.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    con.close()


def test_a_transaction_is_one_snapshot_and_a_failure_leaves_none(lake):
    before = lake.snapshot()
    with lake.transaction(message="two tables"):
        lake.execute("CREATE TABLE a AS SELECT 1 AS x")
        lake.execute("CREATE TABLE b AS SELECT 2 AS y")
    assert lake.snapshot() == before + 1
    assert lake.tables() == ["a", "b"]

    with pytest.raises(RuntimeError, match="gate"), lake.transaction():
        lake.execute("INSERT INTO a VALUES (99)")
        raise RuntimeError("gate")
    assert lake.snapshot() == before + 1
    assert lake.execute("SELECT count(*) FROM a").fetchone()[0] == 1


def test_import_reads_by_column_name_and_ignores_directory_names(lake, tmp_path):
    source = tmp_path / "old"
    # The directory looks like a Hive partition of the real date column.
    _write(
        source / "trade_date=2024-01" / "part.parquet",
        [("'B'", "DATE '2024-01-03'", 2.0), ("'A'", "DATE '2024-01-02'", 1.0)],
    )
    _write(
        source / "trade_date=2024-02" / "part.parquet",
        [("'A'", "DATE '2024-02-01'", 3.0, 7)],
        columns="symbol, trade_date, close, volume",
    )
    result = lake.import_parquet(
        "bars",
        source / "**" / "*.parquet",
        partition_by="trade_date",
        sort_by=("symbol", "trade_date"),
    )
    assert result.rows == 3 and result.snapshot == lake.snapshot()
    rows = lake.execute(
        "SELECT symbol, trade_date, close, volume FROM bars ORDER BY 1, 2"
    ).fetchall()
    assert rows == [
        ("A", date(2024, 1, 2), 1.0, None),
        ("A", date(2024, 2, 1), 3.0, 7),
        ("B", date(2024, 1, 3), 2.0, None),
    ]
    with pytest.raises(CatalogError, match="already exists"):
        lake.import_parquet("bars", source / "**" / "*.parquet")


def test_an_older_snapshot_stays_readable(lake):
    lake.execute("CREATE TABLE t AS SELECT 1 AS x")
    first = lake.snapshot()
    lake.execute("INSERT INTO t VALUES (2)")
    assert lake.execute(f"SELECT count(*) FROM t AT (VERSION => {first})").fetchone()[0] == 1
    assert lake.execute("SELECT count(*) FROM t").fetchone()[0] == 2


def test_another_process_reads_committed_state_during_a_write(lake):
    lake.execute("CREATE TABLE t AS SELECT 1 AS x")
    reader = textwrap.dedent(
        """
        import sys
        from equitylake_core.catalog import Catalog
        with Catalog(sys.argv[1], read_only=True) as lake:
            print(lake.execute("SELECT count(*) FROM t").fetchone()[0])
        """
    )

    def count():
        out = subprocess.run(
            [sys.executable, "-c", reader, str(lake.root)],
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert out.returncode == 0, out.stderr[-400:]
        return int(out.stdout.strip())

    with lake.transaction():
        lake.execute("INSERT INTO t VALUES (2)")
        assert count() == 1
    assert count() == 2


def test_a_read_only_catalog_refuses_writes(lake):
    lake.execute("CREATE TABLE t AS SELECT 1 AS x")
    with Catalog(lake.root, read_only=True) as reader, pytest.raises(duckdb.Error):
        reader.execute("INSERT INTO t VALUES (2)")


def test_a_moved_lake_reads_its_own_files(lake, tmp_path):
    lake.execute("CREATE TABLE t AS SELECT 1 AS x")
    root = lake.root
    lake.close()
    moved = tmp_path / "moved"
    root.rename(moved)
    for read_only in (True, False):
        with Catalog(moved, read_only=read_only) as again:
            files = again.data_files("t")
            assert files and all(str(moved) in path for path in files)
            assert again.execute("SELECT count(*) FROM t").fetchone()[0] == 1
