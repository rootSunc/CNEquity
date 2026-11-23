"""Read a dataset's committed rows from the lake catalog instead of scanning its generation.

On whenever the lake has a catalog. The
catalog answers only when its table holds exactly the revision the read is
pinned to; in every other case the caller gets ``None`` and scans the
revision store, so the catalog can never return older data than the scan.
"""

from __future__ import annotations

import logging
import threading
from datetime import date
from pathlib import Path
from typing import NamedTuple

import polars as pl

from cnequity.config.resources import resources_for
from cnequity.domain.datasets import granularity_for_dataset
from cnequity.domain.partitions import partition_value
from cnequity.storage.catalog_bridge import (
    archived_revision,
    catalog_is_current,
    keys_unique,
    lake_root,
    revision_snapshot,
    synced_revision_id,
)
from equitylake_core.catalog import ALIAS, CATALOG_FILE, Catalog, quote_identifier
from equitylake_core.catalog_cache import OpenCatalogs

logger = logging.getLogger(__name__)

_lock = threading.Lock()
#: One open catalog per lake read lately. Each holds DuckDB's scan threads (one
#: per core by default), so a process reading many lakes keeps two open, not one
#: per lake it ever read (see OpenCatalogs); a third lake costs a reopen.
_open = OpenCatalogs(limit=2)


class CatalogRows(NamedTuple):
    frame: pl.DataFrame
    # One row per primary key: nothing left to dedupe.
    unique_keys: bool


def _catalog(config) -> Catalog | None:
    root = lake_root(config).resolve()
    if not (root / CATALOG_FILE).exists():
        return None
    budget = resources_for(config, "read")
    catalog = _open.get(root, budget)
    if catalog is None:
        catalog = _open.put(root, Catalog(root, read_only=True, **budget.options()), budget)
    return catalog


def close(config) -> None:
    """Close the cached connection to *config*'s lake, so the next read sees it as it is now."""
    root = lake_root(config).resolve()
    with _lock:
        _tables.clear()
        _schemas.clear()
        _open.close(root)


def close_all() -> None:
    with _lock:
        _tables.clear()
        _schemas.clear()
        _open.close()


def read_published(
    config,
    dataset: str,
    *,
    partition_col: str | None,
    start: date | None = None,
    end: date | None = None,
    symbols: list[str] | None = None,
    revision_id: str | None = None,
) -> CatalogRows | None:
    """The committed rows of *dataset* in the requested range, or ``None`` to scan instead.

    Filters mean what they mean in ``scan_parquet_root``: inclusive bounds on
    the partition column, and membership in *symbols*. With *revision_id* the
    catalog answers only if it holds that revision; without, only if it holds
    the committed one.
    """
    try:
        with _lock:
            catalog = _catalog(config)
            if catalog is None:
                return None
            if revision_id is None:
                if not catalog_is_current(config, catalog, dataset):
                    return None
            else:
                if synced_revision_id(catalog, dataset) != revision_id:
                    return None
            table = quote_identifier(dataset)
            types = {row[0]: row[1] for row in catalog.execute(f"DESCRIBE {table}").fetchall()}
            unique = bool(keys_unique(catalog, dataset))
            from cnequity.domain.core_registry import core_spec
            from cnequity.domain.datasets import DATASETS
            from equitylake_core import read as core_read

            if dataset in DATASETS:
                spec = core_spec(dataset, columns=types)
                date_bounds = start is not None or end is not None
                can_use_core = (not date_bounds) or (
                    partition_col == spec.time
                    and spec.time is not None
                    and types.get(spec.time) != "VARCHAR"
                )
                if can_use_core:
                    where = (
                        pl.col("symbol").is_in(symbols)
                        if (symbols and "symbol" in spec.columns)
                        else None
                    )
                    frame = core_read.load(
                        catalog,
                        spec,
                        start=start if date_bounds else None,
                        end=end if date_bounds else None,
                        where=where,
                        sort=False,
                    )
                    return CatalogRows(frame, unique)
            clauses: list[str] = []
            values: list[object] = []
            if partition_col and partition_col in types and (start is not None or end is not None):
                low, high = start, end
                if types[partition_col] == "VARCHAR":
                    granularity = granularity_for_dataset(dataset)
                    low = partition_value(start, granularity) if start is not None else None
                    high = partition_value(end, granularity) if end is not None else None
                column = quote_identifier(partition_col)
                if low is not None:
                    clauses.append(f"{column} >= ?")
                    values.append(low)
                if high is not None:
                    clauses.append(f"{column} <= ?")
                    values.append(high)
            if symbols and "symbol" in types:
                clauses.append("symbol IN (" + ", ".join("?" for _ in symbols) + ")")
                values.extend(symbols)
            where_sql = (" WHERE " + " AND ".join(clauses)) if clauses else ""
            relation = catalog.relation(f"SELECT * FROM {ALIAS}.{table}{where_sql}", values)
            return CatalogRows(relation.pl(), unique)
    except Exception as exc:  # noqa: BLE001 - the scan is always a correct answer
        logger.warning("catalog read of %s failed, scanning instead: %s", dataset, exc)
        return None


def generation_identity(root: Path) -> tuple[str, str] | None:
    """``(dataset, revision_id)`` when *root* is a committed generation directory.

    Generations live at ``<meta>/revisions/data/<dataset>/<revision_id>``.
    """
    root = Path(root)
    parts = root.parts
    if len(parts) >= 4 and parts[-3] == "data" and parts[-4] == "revisions":
        return parts[-2], parts[-1]
    return None


def _resolve_revision(
    catalog, dataset: str, revision_id: str
) -> tuple[str, int | None, bool] | None:
    """``(table, snapshot, unique_keys)`` holding *dataset* at *revision_id*.

    The dataset's own table at the revision's snapshot (``None`` = latest),
    or, for a revision from before 0.25.0 that a hold kept, the archive table
    it was moved into (whose keys are not known to be unique).
    """
    if synced_revision_id(catalog, dataset) == revision_id:
        return dataset, None, bool(keys_unique(catalog, dataset))
    found = revision_snapshot(catalog, dataset, revision_id)
    if found is not None:
        return dataset, found[0], found[1]
    archived = archived_revision(catalog, dataset, revision_id)
    return None if archived is None else (archived, None, False)


def _at(snapshot: int | None) -> str:
    return "" if snapshot is None else f" AT (VERSION => {int(snapshot)})"


class PublishedTable(NamedTuple):
    files: list[str]
    # Column name -> DuckDB type, in table order.
    columns: dict[str, str]
    unique_keys: bool


_MEMO_LIMIT = 2048
# (open catalog, snapshot, dataset, revision) -> PublishedTable or None; only
# the latest snapshot's answers are kept. Keyed on the connection, not the
# path: two copies of a catalog at one path can share a snapshot number.
_tables: dict[tuple[Catalog, int, str, str], PublishedTable | None] = {}
_schemas: dict[tuple[Catalog, int, str, str], pl.Schema] = {}
# (files, column, label SQL) -> partition labels. Data files never change once
# written, so the same files always hold the same labels.
_labels: dict[tuple[tuple[str, ...], str, str], list[str]] = {}


def _published_table(catalog, dataset: str, revision_id: str) -> PublishedTable | None:
    resolved = _resolve_revision(catalog, dataset, revision_id)
    if resolved is None:
        return None
    table, snapshot, unique = resolved
    files = catalog.data_files(table, snapshot)
    if not files:
        return None
    described = catalog.execute(
        f"DESCRIBE SELECT * FROM {ALIAS}.{quote_identifier(table)}{_at(snapshot)}"
    ).fetchall()
    columns = {row[0]: row[1] for row in described}
    return PublishedTable(files, columns, unique)


def published_table(config, dataset: str, revision_id: str) -> PublishedTable | None:
    """The catalog's data files for exactly that revision of *dataset*.

    ``None`` when the catalog does not hold that revision, or the table is not
    file-complete; the caller then reads the generation.
    """
    try:
        with _lock:
            catalog = _catalog(config)
            if catalog is None:
                return None
            # Determined by the catalog's snapshot and the revision: a summary
            # asks the same question several times per dataset per page.
            key = (catalog, catalog.snapshot(), dataset, revision_id)
            if key in _tables:
                return _tables[key]
            table = _published_table(catalog, dataset, revision_id)
            if len(_tables) >= _MEMO_LIMIT or any(k[:2] != key[:2] for k in _tables):
                _tables.clear()
            _tables[key] = table
            return table
    except Exception as exc:  # noqa: BLE001 - the generation is always a correct answer
        logger.warning("catalog files of %s unavailable, reading the generation: %s", dataset, exc)
        return None


def published_files(config, dataset: str, revision_id: str) -> tuple[list[str], pl.Schema] | None:
    """The catalog's data files for that revision, with the table's Polars schema."""
    table = published_table(config, dataset, revision_id)
    if table is None:
        return None
    try:
        with _lock:
            catalog = _catalog(config)
            key = (catalog, catalog.snapshot(), dataset, revision_id)
            schema = _schemas.get(key)
            if schema is None:
                table_name, snapshot, _ = _resolve_revision(catalog, dataset, revision_id) or (
                    dataset,
                    None,
                    False,
                )
                relation = catalog.relation(
                    f"SELECT * FROM {ALIAS}.{quote_identifier(table_name)}{_at(snapshot)} LIMIT 0"
                )
                schema = relation.pl().schema
                if len(_schemas) >= _MEMO_LIMIT or any(k[:2] != key[:2] for k in _schemas):
                    _schemas.clear()
                _schemas[key] = schema
    except Exception as exc:  # noqa: BLE001
        logger.warning("catalog schema of %s unavailable, scanning instead: %s", dataset, exc)
        return None
    return table.files, schema


def files_sql(table: PublishedTable, con) -> str:
    """A DuckDB relation over the table's files with exactly the table's columns.

    Files can carry the catalog's internal row-lineage columns, and a column
    added after a file was written is absent from that file; both are
    normalised here. *con* is used only to look at the files' columns.
    """
    paths = ", ".join("'" + path.replace("'", "''") + "'" for path in table.files)
    scan = f"read_parquet([{paths}], union_by_name = true, hive_partitioning = false)"
    present = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {scan}").fetchall()}
    select = ", ".join(
        f"CAST({quote_identifier(name) if name in present else 'NULL'} AS {kind}) "
        f"AS {quote_identifier(name)}"
        for name, kind in table.columns.items()
    )
    return f"(SELECT {select} FROM {scan})"


def published_stats(config, dataset: str, revision_id: str, *, column: str | None):
    """Per-file statistics for exactly that revision of *dataset*, with its unique-keys flag.

    ``None`` under the same conditions as :func:`published_table`.
    """
    try:
        with _lock:
            catalog = _catalog(config)
            if catalog is None:
                return None
            resolved = _resolve_revision(catalog, dataset, revision_id)
            if resolved is None:
                return None
            table_name, snapshot, unique = resolved
            relation = catalog.relation(
                f"SELECT * FROM {ALIAS}.{quote_identifier(table_name)}{_at(snapshot)} LIMIT 0"
            )
            schema = relation.pl().schema
            if column is not None and column not in schema.names():
                column = None
            stats = catalog.file_stats(table_name, column, snapshot)
            if stats is None:
                return None
            return stats, unique, schema
    except Exception as exc:  # noqa: BLE001 - the generation is always a correct answer
        logger.warning(
            "catalog statistics of %s unavailable, reading the generation: %s", dataset, exc
        )
        return None


def catalog_partition_values(
    config, dataset: str, revision_id: str, *, column: str
) -> list[str] | None:
    """The partition labels (``2024-06``, ``2024Q2``) that hold rows in that revision.

    Labelled at the dataset's granularity, as its partition directories were.
    ``None`` when the catalog does not hold that revision.
    """
    from cnequity.domain.datasets import DATASETS

    table = published_table(config, dataset, revision_id)
    spec = DATASETS.get(dataset)
    if table is None or spec is None or column not in table.columns:
        return None
    name = quote_identifier(column)
    kind = table.columns[column]
    granularity = spec.partition_granularity
    if kind.startswith(("DATE", "TIMESTAMP")):
        label = {
            "day": f"strftime({name}, '%Y-%m-%d')",
            "month": f"strftime({name}, '%Y-%m')",
            "year": f"strftime({name}, '%Y')",
            "quarter": f"(year({name})::VARCHAR || 'Q' || quarter({name})::VARCHAR)",
        }.get(granularity)
        if label is None:
            return None
    else:
        label = f"CAST({name} AS VARCHAR)"
    memo = (tuple(table.files), column, label)
    with _lock:
        hit = _labels.get(memo)
    if hit is not None:
        return list(hit)
    import duckdb

    con = duckdb.connect()
    try:
        resources_for(config, "read").apply(con)
        source = files_sql(table, con)
        rows = con.execute(
            f"SELECT DISTINCT {label} FROM {source} WHERE {name} IS NOT NULL"
        ).fetchall()
    finally:
        con.close()
    labels = sorted(str(row[0]) for row in rows if row[0] is not None)
    with _lock:
        if len(_labels) >= _MEMO_LIMIT:
            _labels.clear()
        _labels[memo] = labels
    return list(labels)
