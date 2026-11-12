"""The lake catalog: immutable Parquet files plus one transactional catalog database.

A lake directory holds ``catalog.sqlite`` (which files belong to which
snapshot) and ``data/`` (the files). Every committed write is one snapshot,
and the snapshot number is the version of the whole lake: a reader pins one
integer and gets a consistent set of tables.

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


class CatalogError(RuntimeError):
    """The lake directory is missing, or is not a lake."""


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
        if not catalog_path.exists():
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
            options = (
                "READ_ONLY"
                if read_only
                else (f"DATA_PATH {quote_literal((self.root / DATA_DIR).as_posix() + '/')}")
            )
            self.con.execute(
                f"ATTACH {quote_literal('ducklake:sqlite:' + catalog_path.as_posix())} "
                f"AS {ALIAS} ({options})"
            )
            self.con.execute(f"USE {ALIAS}")
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
