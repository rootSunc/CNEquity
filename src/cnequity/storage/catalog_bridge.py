"""Bring the revision store's committed datasets into the lake catalog, and prove they match.

The catalog (``equitylake_core.catalog``) is where datasets will be published.
Until publishing moves there, this module copies each dataset's committed
generation into it and compares the two copies row for row. It reads the
revision store and never writes to it; the catalog lives in its own
directory beside ``curated/`` and ``meta/``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import duckdb

from cnequity.domain.datasets import DATASETS
from cnequity.domain.schemas import PRIMARY_KEYS
from cnequity.storage.revisions import RevisionStore
from equitylake_core.catalog import Catalog, ImportResult, quote_identifier, quote_literal

LAKE_DIR = "lake"

# Below this many rows a table is one file; partitioning it would only add tiny files.
PARTITION_MIN_ROWS = 5_000_000


@dataclass(frozen=True)
class Comparison:
    dataset: str
    revision_rows: int
    catalog_rows: int
    same_content: bool

    @property
    def matches(self) -> bool:
        return self.revision_rows == self.catalog_rows and self.same_content


def lake_root(config) -> Path:
    return Path(config.data_root) / LAKE_DIR


def committed_source(config, dataset: str) -> tuple[str, str] | None:
    """The Parquet glob and revision id of *dataset*'s committed generation, if it has one."""
    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root, create=False)
    pointer = store.current_pointer(dataset)
    root = store.current_root(dataset)
    if pointer is None or root is None or not any(root.rglob("*.parquet")):
        return None
    return (root / "**" / "*.parquet").as_posix(), str(pointer.get("revision_id", ""))


def _scan(glob: str) -> str:
    return f"read_parquet({quote_literal(glob)}, union_by_name = true, hive_partitioning = false)"


def _partition_column(dataset: str, glob: str) -> str | None:
    column = DATASETS[dataset].partition_col
    if not column:
        return None
    probe = duckdb.connect()
    try:
        described = {
            row[0]: row[1]
            for row in probe.execute(f"DESCRIBE SELECT * FROM {_scan(glob)}").fetchall()
        }
        if not described.get(column, "").startswith(("DATE", "TIMESTAMP")):
            return None
        rows = probe.execute(f"SELECT count(*) FROM {_scan(glob)}").fetchone()[0]
    finally:
        probe.close()
    return column if rows >= PARTITION_MIN_ROWS else None


def import_dataset(config, catalog: Catalog, dataset: str) -> ImportResult | None:
    """Copy *dataset*'s committed generation into the catalog as one snapshot."""
    source = committed_source(config, dataset)
    if source is None:
        return None
    glob, revision_id = source
    return catalog.import_parquet(
        dataset,
        glob,
        partition_by=_partition_column(dataset, glob),
        sort_by=tuple(PRIMARY_KEYS.get(dataset, ())),
        message=f"import {dataset} from revision {revision_id}",
    )


def compare_dataset(config, catalog: Catalog, dataset: str) -> Comparison | None:
    """Compare the catalog table with the committed generation, whole rows at a time.

    Each side is reduced to its row count and two order-independent digests of
    every row (an XOR and a sum of row hashes), so the comparison does not
    depend on file layout or row order, and a pair of duplicated rows cannot
    cancel out.
    """
    source = committed_source(config, dataset)
    if source is None or not catalog.has_table(dataset):
        return None
    table = quote_identifier(dataset)
    columns = [row[0] for row in catalog.execute(f"DESCRIBE {table}").fetchall()]
    row = "hash(" + ", ".join(quote_identifier(column) for column in columns) + ")"
    digest = f"SELECT count(*), bit_xor({row}), sum({row}::HUGEINT) FROM "
    old = catalog.execute(digest + _scan(source[0])).fetchone()
    new = catalog.execute(digest + table).fetchone()
    return Comparison(
        dataset=dataset,
        revision_rows=old[0],
        catalog_rows=new[0],
        same_content=old[1:] == new[1:],
    )


# --- keeping the catalog in step with the revision store ---------------------------

SYNC_TABLE = "_revision_sync"


@dataclass(frozen=True)
class SyncResult:
    dataset: str
    # "current": nothing to do; "imported": first copy; "partitions": only the
    # changed partitions were replaced; "replaced": the whole table was.
    action: str
    revision_id: str
    partitions: int = 0


def _ensure_sync_table(catalog: Catalog) -> None:
    catalog.execute(
        f"CREATE TABLE IF NOT EXISTS {SYNC_TABLE} "
        "(dataset VARCHAR, revision_id VARCHAR, files VARCHAR)"
    )


def synced_revision(catalog: Catalog, dataset: str) -> tuple[str, dict[str, str]] | None:
    """The revision the catalog table was last brought to, with that revision's files."""
    if not catalog.has_table(SYNC_TABLE):
        return None
    row = catalog.execute(
        f"SELECT revision_id, files FROM {SYNC_TABLE} WHERE dataset = ?", [dataset]
    ).fetchone()
    return (row[0], json.loads(row[1])) if row else None


def _record_sync(catalog: Catalog, dataset: str, revision_id: str, files: dict[str, str]) -> None:
    catalog.execute(f"DELETE FROM {SYNC_TABLE} WHERE dataset = ?", [dataset])
    catalog.execute(
        f"INSERT INTO {SYNC_TABLE} VALUES (?, ?, ?)",
        [dataset, revision_id, json.dumps(files, sort_keys=True)],
    )


def _committed_files(config, dataset: str) -> tuple[str, Path, dict[str, str]] | None:
    """Revision id, generation root and ``relative path -> digest`` of the committed files."""
    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root, create=False)
    pointer = store.current_pointer(dataset)
    root = store.current_root(dataset)
    if pointer is None or root is None:
        return None
    files: dict[str, str] = {}
    try:
        receipt = json.loads((Path(config.meta_root) / pointer["receipt"]).read_text("utf-8"))
        prefix = f"{dataset}/"
        for entry in receipt["generation_files"]:
            path = entry["path"]
            files[path[len(prefix) :] if path.startswith(prefix) else path] = entry["sha256"]
    except (KeyError, OSError, ValueError):
        files = {}
    on_disk = {path.relative_to(root).as_posix() for path in root.rglob("*.parquet")}
    if set(files) != on_disk:
        # No usable receipt: identify a file by its size and modification time.
        # Files carried over between generations are hard links and keep both.
        files = {}
        for relative in on_disk:
            stat = (root / relative).stat()
            files[relative] = f"{stat.st_size}:{stat.st_mtime_ns}"
    if not files:
        return None
    return str(pointer.get("revision_id", "")), root, files


def _partition_of(relative: str) -> str | None:
    head = relative.split("/", 1)[0]
    return head if "=" in head and "/" in relative else None


def _partition_predicate(column: str, column_type: str, granularity: str, value: str) -> str | None:
    """SQL selecting the rows that live in partition directory ``<column>=<value>``."""
    name, literal = quote_identifier(column), quote_literal(value)
    if column_type.startswith(("DATE", "TIMESTAMP")):
        if granularity == "day":
            return f"CAST({name} AS DATE) = CAST({literal} AS DATE)"
        if granularity == "month":
            return f"strftime({name}, '%Y-%m') = {literal}"
        if granularity == "year":
            return f"strftime({name}, '%Y') = {literal}"
        if granularity == "quarter":
            return f"(year({name})::VARCHAR || 'Q' || quarter({name})::VARCHAR) = {literal}"
        return None
    if column_type == "VARCHAR" and granularity == "quarter":
        return f"{name} = {literal}"
    return None


def _file_list(root: Path, relatives: list[str]) -> str:
    paths = ", ".join(quote_literal((root / relative).as_posix()) for relative in sorted(relatives))
    return f"read_parquet([{paths}], union_by_name = true, hive_partitioning = false)"


def _add_missing_columns(catalog: Catalog, table: str, scan: str) -> None:
    have = {row[0] for row in catalog.execute(f"DESCRIBE {table}").fetchall()}
    for row in catalog.execute(f"DESCRIBE SELECT * FROM {scan}").fetchall():
        if row[0] not in have:
            catalog.execute(f"ALTER TABLE {table} ADD COLUMN {quote_identifier(row[0])} {row[1]}")


class _PartitionSyncMismatch(RuntimeError):
    """The partition predicate did not select exactly the rows of the partition's files."""


def _order_by(dataset: str) -> str:
    keys = PRIMARY_KEYS.get(dataset, ())
    return (" ORDER BY " + ", ".join(quote_identifier(key) for key in keys)) if keys else ""


def _replace_partitions(
    catalog: Catalog, dataset: str, root: Path, new: dict[str, str], changed: list[str]
) -> None:
    table = quote_identifier(dataset)
    spec = DATASETS[dataset]
    types = {row[0]: row[1] for row in catalog.execute(f"DESCRIBE {table}").fetchall()}
    predicates = []
    for partition in changed:
        column, _, value = partition.partition("=")
        predicate = (
            _partition_predicate(column, types.get(column, ""), spec.partition_granularity, value)
            if column == spec.partition_col
            else None
        )
        if predicate is None:
            raise _PartitionSyncMismatch(f"no row predicate for partition {partition}")
        predicates.append(predicate)
    where = " OR ".join(f"({predicate})" for predicate in predicates)
    relatives = [relative for relative in new if _partition_of(relative) in set(changed)]
    catalog.execute(f"DELETE FROM {table} WHERE {where}")
    if not relatives:
        return
    scan = _file_list(root, relatives)
    _add_missing_columns(catalog, table, scan)
    inserted = catalog.execute(
        f"INSERT INTO {table} BY NAME SELECT * FROM {scan}{_order_by(dataset)}"
    ).fetchone()[0]
    selected = catalog.execute(f"SELECT count(*) FROM {table} WHERE {where}").fetchone()[0]
    if selected != inserted:
        raise _PartitionSyncMismatch(
            f"{dataset}: predicate selects {selected} rows, partition files hold {inserted}"
        )


def _replace_table(catalog: Catalog, dataset: str, root: Path, new: dict[str, str]) -> None:
    table = quote_identifier(dataset)
    scan = _file_list(root, list(new))
    _add_missing_columns(catalog, table, scan)
    catalog.execute(f"DELETE FROM {table}")
    catalog.execute(f"INSERT INTO {table} BY NAME SELECT * FROM {scan}{_order_by(dataset)}")


def sync_dataset(config, catalog: Catalog, dataset: str) -> SyncResult | None:
    """Bring the catalog table to the committed revision, replacing only what changed.

    The change is found by comparing the committed file list with the one
    recorded at the last sync. Changed partitions are replaced inside one
    transaction together with the sync record; if the rows a partition
    predicate selects are not exactly the rows its files hold, that
    transaction is rolled back and the whole table is replaced instead.
    """
    committed = _committed_files(config, dataset)
    if committed is None:
        return None
    revision_id, root, new = committed
    _ensure_sync_table(catalog)
    if not catalog.has_table(dataset):
        import_dataset(config, catalog, dataset)
        _record_sync(catalog, dataset, revision_id, new)
        return SyncResult(dataset, "imported", revision_id)
    previous = synced_revision(catalog, dataset)
    if previous is not None and previous[0] == revision_id:
        return SyncResult(dataset, "current", revision_id)

    changed: list[str] | None = None
    if previous is not None:
        old = previous[1]
        partitions = {_partition_of(relative) for relative in set(old) | set(new)}
        if None not in partitions:
            changed = sorted(
                partition
                for partition in partitions
                if {r: d for r, d in old.items() if _partition_of(r) == partition}
                != {r: d for r, d in new.items() if _partition_of(r) == partition}
            )
    message = f"sync {dataset} to revision {revision_id}"
    if changed is not None:
        try:
            with catalog.transaction(message=message):
                if changed:
                    _replace_partitions(catalog, dataset, root, new, changed)
                _record_sync(catalog, dataset, revision_id, new)
            return SyncResult(dataset, "partitions", revision_id, len(changed))
        except _PartitionSyncMismatch:
            pass
    with catalog.transaction(message=message):
        _replace_table(catalog, dataset, root, new)
        _record_sync(catalog, dataset, revision_id, new)
    return SyncResult(dataset, "replaced", revision_id)


def catalog_is_current(config, catalog: Catalog, dataset: str) -> bool:
    """True when the catalog table holds exactly the committed revision."""
    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root, create=False)
    pointer = store.current_pointer(dataset)
    previous = synced_revision(catalog, dataset)
    return (
        pointer is not None
        and previous is not None
        and previous[0] == str(pointer.get("revision_id", ""))
    )


def sync_all(config, catalog: Catalog) -> list[SyncResult]:
    """Bring every dataset with a committed generation to its committed revision."""
    results = []
    for dataset in sorted(DATASETS):
        result = sync_dataset(config, catalog, dataset)
        if result is not None:
            results.append(result)
    return results


def sync_summary(results: list[SyncResult]) -> dict:
    """Counts by action, and the datasets that were not already current."""
    counts: dict[str, int] = {}
    for result in results:
        counts[result.action] = counts.get(result.action, 0) + 1
    return {
        "datasets": len(results),
        "actions": counts,
        "changed": [
            {"dataset": r.dataset, "action": r.action, "partitions": r.partitions}
            for r in results
            if r.action != "current"
        ],
    }


@dataclass(frozen=True)
class _StoreRoots:
    """The paths the bridge reads from a config, taken from a revision store instead."""

    meta_root: Path
    curated_root: Path
    derived_root: Path
    data_root: Path


def sync_published(store: RevisionStore, dataset: str) -> SyncResult | None:
    """Sync *dataset* right after *store* published it. Does nothing in a lake without a catalog."""
    from equitylake_core.catalog import CATALOG_FILE

    roots = _StoreRoots(
        meta_root=store.meta_root,
        curated_root=store.curated_root,
        derived_root=store.derived_root,
        data_root=store.meta_root.parent,
    )
    root = lake_root(roots)
    if not (root / CATALOG_FILE).exists():
        return None
    with Catalog(root, memory_limit="4GB", temp_directory=root / "tmp") as catalog:
        return sync_dataset(roots, catalog, dataset)
