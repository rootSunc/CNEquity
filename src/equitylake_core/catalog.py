"""The lake catalog: immutable Parquet files plus one transactional catalog database.

A lake directory holds ``catalog.sqlite`` (which files belong to which
snapshot) and ``data/`` (the files). Every committed write is one snapshot,
and the snapshot number is the version of the whole lake: a reader pins one
integer and gets a consistent set of tables.

Every table is kept *file-complete*: its rows are exactly the rows of the
data files the catalog lists for it. Small writes are not inlined into the
catalog database, and deletions are folded back into rewritten files right
after the write. A reader can then hand the file list to any Parquet engine
(Polars here) and get the table, with that engine's own predicate pushdown.
``data_files`` checks the property before returning a list.

The catalog database is SQLite in WAL mode. In the default journal mode a
reader that holds a result open blocks the writer's commit; a DuckDB-file
catalog cannot be read by another process while a writer has it attached.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from equitylake_core.extensions import ensure_extensions

CATALOG_FILE = "catalog.sqlite"
DATA_DIR = "data"
ALIAS = "lake"
METADATA = f"__ducklake_metadata_{ALIAS}"


class CatalogError(RuntimeError):
    """The lake directory is missing, or is not a lake."""


@dataclass(frozen=True)
class DataFile:
    """One data file of a file-complete table, with the range of one column in it."""

    path: str
    rows: int
    bytes: int
    # Smallest and largest value of the column asked about, as the catalog
    # records them (text); ``None`` when the file has no statistics for it.
    low: str | None = None
    high: str | None = None


@dataclass(frozen=True)
class ImportResult:
    table: str
    rows: int
    snapshot: int


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _enable_wal(path: Path) -> None:
    """Journal mode is stored in the database file; set it before the first table exists."""
    con = sqlite3.connect(path)
    try:
        mode = con.execute("PRAGMA journal_mode=WAL").fetchone()[0]
    finally:
        con.close()
    if str(mode).lower() != "wal":
        raise CatalogError(f"could not switch {path} to WAL journal mode (got {mode})")


class Catalog:
    """One open connection to a lake directory.

    ``create=True`` makes the directory and an empty catalog when missing.
    ``read_only=True`` attaches without write access; several read-only
    processes and one writer can use the lake at the same time.
    """

    def __init__(
        self,
        root: str | Path,
        *,
        create: bool = False,
        read_only: bool = False,
        memory_limit: str | None = None,
        temp_directory: str | Path | None = None,
        extension_dir: str | Path | None = None,
        extension_mirror: str | None = None,
    ):
        if create and read_only:
            raise ValueError("a read-only catalog cannot be created")
        self.root = Path(root).expanduser().resolve()
        self.read_only = read_only
        catalog_path = self.root / CATALOG_FILE
        created = not catalog_path.exists()
        if created:
            if not create:
                raise CatalogError(f"no lake catalog at {catalog_path}")
            (self.root / DATA_DIR).mkdir(parents=True, exist_ok=True)
            _enable_wal(catalog_path)
        self.con = duckdb.connect()
        try:
            if memory_limit:
                self.con.execute(f"SET memory_limit = {quote_literal(memory_limit)}")
            if temp_directory:
                self.con.execute(
                    f"SET temp_directory = {quote_literal(Path(temp_directory).as_posix())}"
                )
            ensure_extensions(self.con, local_dir=extension_dir, mirror=extension_mirror)
            # The catalog stores the absolute data path it was created with.
            # Always resolve files under this catalog's own directory instead,
            # so a lake that was moved, restored or cloned reads its own files
            # and never the original's.
            data_path = f"DATA_PATH {quote_literal((self.root / DATA_DIR).as_posix() + '/')}"
            options = ("READ_ONLY, " if read_only else "") + f"{data_path}, OVERRIDE_DATA_PATH true"
            self.con.execute(
                f"ATTACH {quote_literal('ducklake:sqlite:' + catalog_path.as_posix())} "
                f"AS {ALIAS} ({options})"
            )
            self.con.execute(f"USE {ALIAS}")
            if created:
                self.keep_rows_in_files()
        except Exception:
            self.con.close()
            raise

    def close(self) -> None:
        self.con.close()

    def __enter__(self) -> Catalog:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def execute(self, query: str, parameters: Sequence[Any] | None = None):
        return self.con.execute(query, parameters) if parameters else self.con.execute(query)

    def relation(self, query: str, parameters: Sequence[Any] | None = None):
        """A lazy DuckDB relation for *query*, convertible to Arrow, Polars or pandas.

        A row fetched with ``fetchone`` leaves its result open, and DuckDB
        refuses a new relation until that result is finished; drop it first.
        """
        self.con.abort()
        return self.con.sql(query, params=parameters) if parameters else self.con.sql(query)

    def snapshot(self) -> int:
        """The number of the latest committed snapshot."""
        return self.con.execute(
            f"SELECT max(snapshot_id) FROM ducklake_snapshots({quote_literal(ALIAS)})"
        ).fetchone()[0]

    def tables(self) -> list[str]:
        rows = self.con.execute(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_catalog = {quote_literal(ALIAS)} ORDER BY 1"
        ).fetchall()
        return [row[0] for row in rows]

    def has_table(self, table: str) -> bool:
        return table in self.tables()

    @contextmanager
    def transaction(
        self, *, message: str | None = None, author: str | None = None
    ) -> Iterator[Catalog]:
        """Everything written inside becomes one snapshot, or nothing does."""
        self.con.execute("BEGIN")
        try:
            if message is not None:
                self.con.execute(
                    f"CALL ducklake_set_commit_message({quote_literal(ALIAS)}, "
                    f"{quote_literal(author or '')}, {quote_literal(message)})"
                )
            yield self
        except BaseException:
            self.con.execute("ROLLBACK")
            raise
        self.con.execute("COMMIT")

    def keep_rows_in_files(self) -> None:
        """Stop inlining small writes into the catalog database, and move any inlined rows to files."""
        self.con.execute(
            f"CALL ducklake_set_option({quote_literal(ALIAS)}, 'data_inlining_row_limit', 0)"
        )
        self.con.execute(f"CALL ducklake_flush_inlined_data({quote_literal(ALIAS)})")

    def fold_deletes(self, table: str) -> None:
        """Rewrite every data file of *table* that has deletions, so its files hold exactly its rows."""
        self.con.execute(
            f"CALL ducklake_rewrite_data_files({quote_literal(ALIAS)}, {quote_literal(table)}, "
            "delete_threshold => 0)"
        )

    def _alive(self, alias: str, snapshot: int | None) -> str:
        """SQL: a metadata row of *alias* is part of the table at *snapshot* (latest if None)."""
        if snapshot is None:
            return f"{alias}.end_snapshot IS NULL"
        return (
            f"{alias}.begin_snapshot <= {int(snapshot)} AND "
            f"({alias}.end_snapshot IS NULL OR {alias}.end_snapshot > {int(snapshot)})"
        )

    def data_files(self, table: str, snapshot: int | None = None) -> list[str] | None:
        """The data files whose rows are exactly *table*'s rows, or ``None``.

        At the latest snapshot, or at *snapshot* (an earlier one stays readable
        until its files are cleaned up). ``None`` means the table is not
        file-complete there (a deletion not yet folded, or rows inlined in the
        catalog database), or an older snapshot's files are gone; read it
        through DuckDB instead.
        """
        version = "" if snapshot is None else f", snapshot_version => {int(snapshot)}"
        listed = self.con.execute(
            f"SELECT data_file, delete_file FROM "
            f"ducklake_list_files({quote_literal(ALIAS)}, ?{version})",
            [table],
        ).fetchall()
        if any(delete is not None for _, delete in listed):
            return None
        at = "" if snapshot is None else f" AT (VERSION => {int(snapshot)})"
        rows = self.con.execute(
            f"SELECT count(*) FROM {ALIAS}.{quote_identifier(table)}{at}"
        ).fetchone()[0]
        in_files = self.con.execute(
            f"SELECT coalesce(sum(f.record_count), 0) FROM {METADATA}.ducklake_data_file f "
            f"JOIN {METADATA}.ducklake_table t USING (table_id) "
            f"WHERE t.table_name = ? AND {self._alive('t', snapshot)} "
            f"AND {self._alive('f', snapshot)}",
            [table],
        ).fetchone()[0]
        if rows != in_files:
            return None
        paths = [path for path, _ in listed]
        if snapshot is not None and not all(Path(path).is_file() for path in paths):
            return None
        return paths

    def file_stats(
        self, table: str, column: str | None = None, snapshot: int | None = None
    ) -> list[DataFile] | None:
        """The data files of *table* with their row counts and sizes, or ``None``.

        With *column*, each file also carries that column's smallest and
        largest value, from the statistics the catalog keeps per file, so a
        caller can tell which files can hold a given range without opening
        them. *snapshot* and ``None`` as for ``data_files``.
        """
        paths = self.data_files(table, snapshot)
        if paths is None:
            return None
        by_name = {Path(path).name: path for path in paths}
        alive = f"{self._alive('t', snapshot)} AND {self._alive('f', snapshot)}"
        if column is None:
            stats = self.con.execute(
                f"SELECT f.path, f.record_count, f.file_size_bytes, NULL, NULL "
                f"FROM {METADATA}.ducklake_data_file f "
                f"JOIN {METADATA}.ducklake_table t USING (table_id) "
                f"WHERE t.table_name = ? AND {alive}",
                [table],
            ).fetchall()
        else:
            stats = self.con.execute(
                f"SELECT f.path, f.record_count, f.file_size_bytes, s.min_value, s.max_value "
                f"FROM {METADATA}.ducklake_data_file f "
                f"JOIN {METADATA}.ducklake_table t USING (table_id) "
                f"JOIN {METADATA}.ducklake_column c ON c.table_id = t.table_id "
                f"AND c.column_name = ? AND {self._alive('c', snapshot)} "
                f"LEFT JOIN {METADATA}.ducklake_file_column_stats s "
                "ON s.data_file_id = f.data_file_id AND s.column_id = c.column_id "
                f"WHERE t.table_name = ? AND {alive}",
                [column, table],
            ).fetchall()
        files = [
            DataFile(by_name[Path(path).name], int(rows), int(size), low, high)
            for path, rows, size, low, high in stats
            if Path(path).name in by_name
        ]
        if len(files) != len(paths):
            return None
        return sorted(files, key=lambda f: (f.low or "", f.path))

    def set_compression(self, codec: str = "zstd") -> None:
        self.con.execute(
            f"CALL ducklake_set_option({quote_literal(ALIAS)}, 'parquet_compression', "
            f"{quote_literal(codec)})"
        )

    def import_parquet(
        self,
        table: str,
        source: str | Path,
        *,
        partition_by: str | None = None,
        sort_by: Sequence[str] = (),
        message: str | None = None,
    ) -> ImportResult:
        """Create *table* from a tree of Parquet files, as one snapshot.

        *source* is a glob. Files whose columns differ are read by column
        name, so a column missing from an older file is NULL there. Directory
        names are never read as column values. *partition_by* names a date or
        timestamp column; the table is then partitioned by its year and
        month. Rows are written in *sort_by* order so that a scan filtered on
        the leading key can skip row groups.
        """
        if self.has_table(table):
            raise CatalogError(f"table {table} already exists in the lake")
        scan = (
            f"read_parquet({quote_literal(Path(source).as_posix())}, "
            "union_by_name = true, hive_partitioning = false)"
        )
        name = quote_identifier(table)
        order = ", ".join(quote_identifier(column) for column in sort_by)
        with self.transaction(message=message or f"import {table}"):
            self.con.execute(f"CREATE TABLE {name} AS SELECT * FROM {scan} LIMIT 0")
            if partition_by:
                column = quote_identifier(partition_by)
                self.con.execute(
                    f"ALTER TABLE {name} SET PARTITIONED BY (year({column}), month({column}))"
                )
            rows = self.con.execute(
                f"INSERT INTO {name} SELECT * FROM {scan}" + (f" ORDER BY {order}" if order else "")
            ).fetchone()[0]
        return ImportResult(table=table, rows=rows, snapshot=self.snapshot())

    def merge_small_files(self) -> None:
        self.con.execute(f"CALL ducklake_merge_adjacent_files({quote_literal(ALIAS)})")
