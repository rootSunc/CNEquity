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
#: In a lake directory that has no data of its own: the data directory it reads.
DATA_LINK = "data.at"
ALIAS = "lake"
METADATA = f"__ducklake_metadata_{ALIAS}"


_INTEGERS = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT")


def _widens(found: str, wanted: str) -> bool:
    """Whether a file column of type *found* is read exactly as a table column of type *wanted*."""
    if found == wanted:
        return True
    if found in _INTEGERS and wanted in _INTEGERS:
        return _INTEGERS.index(found) < _INTEGERS.index(wanted)
    return found == "FLOAT" and wanted == "DOUBLE"


#: Layouts of the catalog database whose per-file statistics this code reads
#: directly. Anything else is read through the files' footers instead.
KNOWN_METADATA_FORMATS = frozenset({"1.1"})


def lake_data_root(root: str | Path) -> Path:
    """The directory holding the data files of the lake at *root*."""
    link = Path(root) / DATA_LINK
    if link.is_file():
        return Path(link.read_text(encoding="utf-8").strip())
    return Path(root).expanduser().resolve() / DATA_DIR


def copy_catalog(source: str | Path, target: str | Path) -> None:
    """Make *target* a lake whose catalog is a copy of *source*'s and whose files are *source*'s.

    What is written to the copy changes only the copy's catalog: a table
    there can gain and drop files without *source* seeing any of it, and no
    data file is copied. A file registered in the copy is placed in
    *source*'s data directory, where it belongs to no snapshot of *source*;
    the caller removes it when the copy is done with. Never expire snapshots
    or clean up files through the copy: the files are not its own.
    """
    import sqlite3
    from contextlib import closing

    source, target = Path(source), Path(target)
    target.mkdir(parents=True, exist_ok=True)
    (target / CATALOG_FILE).unlink(missing_ok=True)
    with (
        closing(
            sqlite3.connect(f"file:{(source / CATALOG_FILE).as_posix()}?mode=ro", uri=True)
        ) as original,
        closing(sqlite3.connect(target / CATALOG_FILE)) as copied,
    ):
        original.backup(copied)
        copied.execute("PRAGMA journal_mode=WAL")
    (target / DATA_LINK).write_text(lake_data_root(source).as_posix(), encoding="utf-8")


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
        link = self.root / DATA_LINK
        #: True for a copy of another lake's catalog that reads that lake's files.
        self.borrows_data = link.is_file()
        self.data_root = (
            Path(link.read_text(encoding="utf-8").strip())
            if self.borrows_data
            else self.root / DATA_DIR
        )
        self._rows_in_files: bool | None = None
        self._format: str | None = None
        self._records: dict[tuple[str, str], tuple[int, Any]] = {}
        catalog_path = self.root / CATALOG_FILE
        created = not catalog_path.exists()
        if created:
            if not create:
                raise CatalogError(f"no lake catalog at {catalog_path}")
            (self.root / DATA_DIR).mkdir(parents=True, exist_ok=True)
            _enable_wal(catalog_path)
        self.con = duckdb.connect()
        try:
            # Instants read back in UTC whatever zone this machine is in, and a
            # date taken from one (a partition, a filter) is the same everywhere.
            self.con.execute("SET TimeZone = 'UTC'")
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
            data_path = f"DATA_PATH {quote_literal(self.data_root.as_posix() + '/')}"
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

    def records(self, table: str, columns: str = "*"):
        """Every row of the small record table *table*, as a Polars frame; filter it in Python.

        A filtered scan of any table makes DuckLake read the whole lake's file
        list and per-file column statistics from SQLite: the backend does not
        narrow that read to the table scanned. On a lake of 34,000 files that
        is 0.13 s per query, while reading a record table whole costs a few
        milliseconds. A read-only connection keeps the frame until the lake's
        snapshot moves; a writer reads afresh, so it sees its own open
        transaction. *columns* may name virtual columns such as ``snapshot_id``.
        """
        if self.read_only:
            snapshot = self.snapshot()
            kept = self._records.get((table, columns))
            if kept is not None and kept[0] == snapshot:
                return kept[1]
        frame = self.relation(f"SELECT {columns} FROM {quote_identifier(table)}").pl()
        if self.read_only:
            self._records[(table, columns)] = (snapshot, frame)
        return frame

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
        self._rows_in_files = None

    def table_dir(self, table: str) -> Path:
        """Where a writer places the data files it will register for *table*."""
        return self.data_root / "main" / table

    def register_file(self, table: str, path: str | Path) -> None:
        """Make the Parquet file *path* a data file of *table*, as it is.

        The file must already be under this lake's data path and fit the
        table (:meth:`file_fits`); from here on the catalog owns it. Columns
        are matched by name, a column the file lacks reads as NULL, and
        directory names are never read as column values.
        """
        target = Path(path).resolve()
        if self.data_root.resolve() not in target.parents:
            raise CatalogError(f"{target} is not under this lake's data path")
        self.con.execute(
            f"CALL ducklake_add_data_files({quote_literal(ALIAS)}, {quote_literal(table)}, "
            f"{quote_literal(target.as_posix())}, hive_partitioning => false, "
            "allow_missing => true)"
        )

    def column_types(self, table: str) -> dict[str, str]:
        return {
            row[0]: row[1]
            for row in self.con.execute(f"DESCRIBE {ALIAS}.{quote_identifier(table)}").fetchall()
        }

    def file_types(self, paths: Sequence[str | Path]) -> dict[str, str]:
        """The column types a table made from the Parquet files *paths* has, read by name."""
        listed = ", ".join(quote_literal(Path(path).as_posix()) for path in paths)
        return {
            row[0]: row[1]
            for row in self.con.execute(
                f"DESCRIBE SELECT * FROM read_parquet([{listed}], union_by_name => true, "
                "hive_partitioning => false)"
            ).fetchall()
        }

    def file_fits(
        self, table: str, path: str | Path, *, types: dict[str, str] | None = None
    ) -> bool:
        """Whether *path* can be registered for *table* as it is.

        Every column the file shares with the table must have the table's
        type or one it widens from without loss (a smaller signed integer, a
        single float for a double): the catalog refuses anything else (a
        wider type, an untyped all-null column, a timestamp without a zone),
        and a refusal inside a transaction aborts it. A column only the file
        has is the caller's to add first. *types* stands for a table that
        does not exist yet.
        """
        if types is None:
            if not self.has_table(table):
                return True
            types = self.column_types(table)
        found = self.file_types([path])
        return all(_widens(kind, types[name]) for name, kind in found.items() if name in types)

    def conform_file(
        self,
        table: str,
        source: str | Path,
        target: str | Path,
        *,
        types: dict[str, str] | None = None,
    ) -> None:
        """Write *source* again at *target* with *table*'s column types, in the same row order."""
        wanted = self.column_types(table) if types is None else types
        columns = ", ".join(
            f"CAST({quote_identifier(name)} AS {wanted[name]}) AS {quote_identifier(name)}"
            if name in wanted
            else quote_identifier(name)
            for name in self.file_types([source])
        )
        Path(target).parent.mkdir(parents=True, exist_ok=True)
        self.con.execute(
            f"COPY (SELECT {columns} FROM read_parquet({quote_literal(Path(source).as_posix())}, "
            f"hive_partitioning => false)) TO {quote_literal(Path(target).as_posix())} "
            "(FORMAT parquet, COMPRESSION zstd)"
        )

    def unpartition(self, table: str) -> None:
        """Stop the catalog from partitioning *table*; its files stay as they are.

        A table whose writer registers files keeps one file set per
        partition of its own, so the catalog's partitioning adds nothing, and
        it refuses a registered file that is not laid out its way. A snapshot
        of its own: the catalog cannot commit this with a registered file.
        """
        self.con.execute(f"ALTER TABLE {ALIAS}.{quote_identifier(table)} RESET PARTITIONED BY")

    def keep_rows_inline(self, table: str, rows: int = 100) -> None:
        """Keep small writes to *table* in the catalog database instead of one file each.

        For a table of records (a row per publish): written a row at a time
        and read by key, it would otherwise grow by a tiny file per write and
        slow down with every one. Data tables stay in files (``keep_rows_in_files``).
        """
        name = f"main.{table}"
        set_already = self.con.execute(
            f"SELECT 1 FROM ducklake_options({quote_literal(ALIAS)}) WHERE option_name = "
            "'data_inlining_row_limit' AND scope = 'TABLE' AND scope_entry = ? AND value = ?",
            [name, str(int(rows))],
        ).fetchone()
        if set_already is None:
            self.con.execute(
                f"CALL ducklake_set_option({quote_literal(ALIAS)}, 'data_inlining_row_limit', "
                f"{int(rows)}, table_name => {quote_literal(table)})"
            )

    def fold_deletes(self, table: str) -> None:
        """Rewrite every data file of *table* that has deletions, so its files hold exactly its rows."""
        self.con.execute(
            f"CALL ducklake_rewrite_data_files({quote_literal(ALIAS)}, {quote_literal(table)}, "
            "delete_threshold => 0)"
        )

    def has_deletes(self, table: str) -> bool:
        """Whether a data file of *table* has deletions not yet folded into it."""
        return (
            self.con.execute(
                f"SELECT 1 FROM ducklake_list_files({quote_literal(ALIAS)}, ?) "
                "WHERE delete_file IS NOT NULL LIMIT 1",
                [table],
            ).fetchone()
            is not None
        )

    def _alive(self, alias: str, snapshot: int | None) -> str:
        """SQL: a metadata row of *alias* is part of the table at *snapshot* (latest if None)."""
        if snapshot is None:
            return f"{alias}.end_snapshot IS NULL"
        return (
            f"{alias}.begin_snapshot <= {int(snapshot)} AND "
            f"({alias}.end_snapshot IS NULL OR {alias}.end_snapshot > {int(snapshot)})"
        )

    def rows_in_files(self) -> bool:
        """Whether small writes go to data files rather than into the catalog database."""
        if self._rows_in_files is None:
            row = self.con.execute(
                f"SELECT value FROM ducklake_options({quote_literal(ALIAS)}) "
                "WHERE option_name = 'data_inlining_row_limit' AND scope = 'GLOBAL'"
            ).fetchone()
            self._rows_in_files = row is not None and str(row[0]) == "0"
        return self._rows_in_files

    def data_files(self, table: str, snapshot: int | None = None) -> list[str] | None:
        """The data files whose rows are exactly *table*'s rows, or ``None``.

        At the latest snapshot, or at *snapshot* (an earlier one stays readable
        until its files are cleaned up). ``None`` means the table is not
        file-complete there (a deletion not yet folded, or a catalog that
        inlines small writes), or an older snapshot's files are gone; read it
        through DuckDB instead.
        """
        if not self.rows_in_files():
            return None
        version = "" if snapshot is None else f", snapshot_version => {int(snapshot)}"
        listed = self.con.execute(
            f"SELECT data_file, delete_file FROM "
            f"ducklake_list_files({quote_literal(ALIAS)}, ?{version})",
            [table],
        ).fetchall()
        if any(delete is not None for _, delete in listed):
            return None
        paths = [path for path, _ in listed]
        if snapshot is not None and not all(Path(path).is_file() for path in paths):
            return None
        return paths

    def metadata_format(self) -> str | None:
        """The DuckLake format version of this catalog, or ``None`` if it does not say."""
        if self._format is None:
            row = self.con.execute(
                f"SELECT value FROM ducklake_options({quote_literal(ALIAS)}) "
                "WHERE option_name = 'version' AND scope = 'GLOBAL'"
            ).fetchone()
            self._format = str(row[0]) if row else ""
        return self._format or None

    def file_stats(
        self, table: str, column: str | None = None, snapshot: int | None = None
    ) -> list[DataFile] | None:
        """The data files of *table* with their row counts and sizes, or ``None``.

        With *column*, each file also carries that column's smallest and
        largest value, so a caller can tell which files can hold a given
        range without opening them. *snapshot* and ``None`` as for
        ``data_files``.

        The numbers come from the statistics the catalog keeps per file when
        its database has a layout this code knows (``KNOWN_METADATA_FORMATS``).
        Those tables are the catalog's own and not a public interface, so
        under any other layout the same numbers are read from the files'
        footers instead: slower, and never wrong.
        """
        paths = self.data_files(table, snapshot)
        if paths is None:
            return None
        if not paths:
            return []
        stats = None
        if self.metadata_format() in KNOWN_METADATA_FORMATS:
            try:
                stats = self._catalog_stats(table, column, snapshot, paths)
            except duckdb.Error:
                stats = None
        if stats is None:
            stats = self._footer_stats(column, paths)
        return sorted(stats, key=lambda f: (f.low or "", f.path))

    def _catalog_stats(
        self, table: str, column: str | None, snapshot: int | None, paths: list[str]
    ) -> list[DataFile] | None:
        by_name = {Path(path).name: path for path in paths}
        alive = f"{self._alive('t', snapshot)} AND {self._alive('f', snapshot)}"
        if column is None:
            found = self.con.execute(
                f"SELECT f.path, f.record_count, f.file_size_bytes, NULL, NULL "
                f"FROM {METADATA}.ducklake_data_file f "
                f"JOIN {METADATA}.ducklake_table t USING (table_id) "
                f"WHERE t.table_name = ? AND {alive}",
                [table],
            ).fetchall()
        else:
            found = self.con.execute(
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
            for path, rows, size, low, high in found
            if Path(path).name in by_name
        ]
        return files if len(files) == len(paths) else None

    def _footer_stats(self, column: str | None, paths: list[str]) -> list[DataFile]:
        listed = "[" + ", ".join(quote_literal(path) for path in paths) + "]"
        rows = dict(
            self.con.execute(
                f"SELECT file_name, num_rows FROM parquet_file_metadata({listed})"
            ).fetchall()
        )
        bounds: dict[str, tuple[str | None, str | None]] = {}
        if column is not None:
            for name, low, high in self.con.execute(
                f"SELECT file_name, min(stats_min_value), max(stats_max_value) "
                f"FROM parquet_metadata({listed}) WHERE path_in_schema = ? GROUP BY 1",
                [column],
            ).fetchall():
                bounds[name] = (low, high)
        return [
            DataFile(
                path,
                int(rows.get(path, 0)),
                Path(path).stat().st_size,
                *bounds.get(path, (None, None)),
            )
            for path in paths
        ]

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

    def export_parquet(
        self,
        table: str,
        destination: str | Path,
        *,
        partition_by: str | None = None,
        where: str | None = None,
    ) -> int:
        """Write *table*'s rows to plain Parquet under *destination*; returns the row count.

        For tools that read Parquet directly: the lake's own data files belong
        to the catalog, which rewrites and removes them. The export is one
        statement, so it is one snapshot of the table. With *partition_by* (a
        date or timestamp column) the files go into ``year=YYYY/month=M/``
        directories; those names are not columns of the table, so a reader
        that takes directory names as values adds two columns and overwrites
        none. Without it the table is one file, ``<table>.parquet``.
        """
        target = Path(destination)
        name = quote_identifier(table)
        scope = f" WHERE {where}" if where else ""
        rows = self.con.execute(f"SELECT count(*) FROM {name}{scope}").fetchone()[0]
        target.mkdir(parents=True, exist_ok=True)
        if partition_by is None:
            self.con.execute(
                f"COPY (SELECT * FROM {name}{scope}) TO "
                f"{quote_literal((target / f'{table}.parquet').as_posix())} "
                "(FORMAT parquet, COMPRESSION zstd)"
            )
            return rows
        if not rows:
            return 0
        column = quote_identifier(partition_by)
        self.con.execute(
            f'COPY (SELECT *, year({column}) AS "year", month({column}) AS "month" '
            f"FROM {name}{scope}) TO {quote_literal(target.as_posix())} "
            '(FORMAT parquet, COMPRESSION zstd, PARTITION_BY ("year", "month"), '
            "OVERWRITE_OR_IGNORE)"
        )
        return rows

    def merge_small_files(self) -> None:
        self.con.execute(f"CALL ducklake_merge_adjacent_files({quote_literal(ALIAS)})")
