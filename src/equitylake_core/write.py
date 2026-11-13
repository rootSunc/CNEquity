"""One write to the lake: partition replacements, checks and records, committed as one snapshot.

A writer prepares Parquet files for the partitions it changes, then hands
them to a :class:`Write`. On commit, inside one catalog transaction, each
partition's rows are replaced by the files' rows, every check runs against
the result, and the records (who wrote what, and why) are appended. Readers
see all of it at once, or, if anything fails, none of it.

A partition is named by a SQL predicate over the table's columns, not by a
directory: the catalog lays its files out by its own rules. Files whose rows
fall outside their predicate are refused, so a replacement can never touch a
partition it did not name.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from equitylake_core.catalog import Catalog, quote_identifier, quote_literal


class WriteRejected(RuntimeError):
    """A replacement or a check refused the write; nothing was committed."""


@dataclass(frozen=True)
class Replacement:
    table: str
    files: tuple[str, ...]
    #: Selects the rows being replaced; ``None`` replaces the whole table.
    where: str | None = None
    order_by: tuple[str, ...] = ()
    #: Partition expressions for a table this write creates, e.g. ``year(trade_date)``.
    partitioned_by: tuple[str, ...] = ()


#: A check gets the catalog inside the open transaction and returns a refusal, or ``None``.
Check = Callable[[Catalog], "str | None"]


@dataclass(frozen=True)
class WriteResult:
    snapshot: int
    #: Rows each table holds inside the replaced partitions after the write.
    rows: dict[str, int]


@dataclass
class Write:
    catalog: Catalog
    replacements: list[Replacement] = field(default_factory=list)
    checks: list[tuple[str, Check]] = field(default_factory=list)
    records: list[tuple[str, dict[str, object]]] = field(default_factory=list)

    def replace(
        self,
        table: str,
        files: Sequence[str | Path],
        *,
        where: str | None = None,
        order_by: Sequence[str] = (),
        partitioned_by: Sequence[str] = (),
    ) -> None:
        """Make *files* the rows of *table* selected by *where* (no files: delete them)."""
        self.replacements.append(
            Replacement(
                table,
                tuple(Path(path).as_posix() for path in files),
                where,
                tuple(order_by),
                tuple(partitioned_by),
            )
        )

    def check(self, name: str, check: Check) -> None:
        self.checks.append((name, check))

    def record(self, table: str, row: dict[str, object]) -> None:
        """Append *row* to the metadata table *table* in the same snapshot."""
        self.records.append((table, dict(row)))

    def commit(self, *, message: str | None = None) -> WriteResult:
        if not self.replacements and not self.records:
            raise ValueError("nothing to write")
        rows: dict[str, int] = {}
        with self.catalog.transaction(message=message):
            for item in self.replacements:
                rows[item.table] = rows.get(item.table, 0) + _apply(self.catalog, item)
            for name, check in self.checks:
                refusal = check(self.catalog)
                if refusal:
                    raise WriteRejected(f"{name}: {refusal}")
            for table, row in self.records:
                _append(self.catalog, table, row)
        snapshot = self.catalog.snapshot()
        # Keep every table file-complete: a replacement leaves delete files
        # behind, which a direct file scan would read as live rows.
        for table in dict.fromkeys(item.table for item in self.replacements):
            self.catalog.fold_deletes(table)
        return WriteResult(snapshot, rows)


def _scan(files: tuple[str, ...]) -> str:
    listed = ", ".join(quote_literal(path) for path in files)
    return f"read_parquet([{listed}], union_by_name => true, hive_partitioning => false)"


def _apply(catalog: Catalog, item: Replacement) -> int:
    table = quote_identifier(item.table)
    order = (
        " ORDER BY " + ", ".join(quote_identifier(column) for column in item.order_by)
        if item.order_by
        else ""
    )
    if not catalog.has_table(item.table):
        if not item.files:
            return 0
        catalog.execute(f"CREATE TABLE {table} AS SELECT * FROM {_scan(item.files)}{order}")
        if item.partitioned_by:
            catalog.execute(
                f"ALTER TABLE {table} SET PARTITIONED BY ({', '.join(item.partitioned_by)})"
            )
        return catalog.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    where = f" WHERE {item.where}" if item.where else ""
    catalog.execute(f"DELETE FROM {table}{where}")
    if not item.files:
        return 0
    _add_missing_columns(catalog, item.table, item.files)
    inserted = catalog.execute(
        f"INSERT INTO {table} BY NAME SELECT * FROM {_scan(item.files)}{order}"
    ).fetchone()[0]
    if item.where:
        selected = catalog.execute(f"SELECT count(*) FROM {table}{where}").fetchone()[0]
        if selected != inserted:
            raise WriteRejected(
                f"{item.table}: {inserted - selected} of {inserted} row(s) fall outside "
                f"the partition they replace ({item.where})"
            )
    return inserted


def _add_missing_columns(catalog: Catalog, table: str, files: tuple[str, ...]) -> None:
    present = {row[0] for row in catalog.execute(f"DESCRIBE {quote_identifier(table)}").fetchall()}
    for name, kind, *_ in catalog.execute(f"DESCRIBE SELECT * FROM {_scan(files)}").fetchall():
        if name not in present:
            catalog.execute(
                f"ALTER TABLE {quote_identifier(table)} ADD COLUMN {quote_identifier(name)} {kind}"
            )


def _sql_type(value: object) -> str:
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE"
    return "VARCHAR"


def _append(catalog: Catalog, table: str, row: dict[str, object]) -> None:
    name = quote_identifier(table)
    if not catalog.has_table(table):
        columns = ", ".join(
            f"{quote_identifier(key)} {_sql_type(value)}" for key, value in row.items()
        )
        catalog.execute(f"CREATE TABLE {name} ({columns})")
    else:
        present = {r[0] for r in catalog.execute(f"DESCRIBE {name}").fetchall()}
        for key, value in row.items():
            if key not in present:
                catalog.execute(
                    f"ALTER TABLE {name} ADD COLUMN {quote_identifier(key)} {_sql_type(value)}"
                )
    keys = list(row)
    columns = ", ".join(quote_identifier(key) for key in keys)
    marks = ", ".join("?" for _ in keys)
    catalog.execute(f"INSERT INTO {name} ({columns}) VALUES ({marks})", [row[key] for key in keys])
