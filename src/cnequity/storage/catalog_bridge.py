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
from equitylake_core.catalog import METADATA, Catalog, ImportResult, quote_identifier, quote_literal

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
        "(dataset VARCHAR, revision_id VARCHAR, files VARCHAR, unique_keys BOOLEAN)"
    )
    columns = {row[0] for row in catalog.execute(f"DESCRIBE {SYNC_TABLE}").fetchall()}
    if "unique_keys" not in columns:  # a catalog synced before the flag existed
        catalog.execute(f"ALTER TABLE {SYNC_TABLE} ADD COLUMN unique_keys BOOLEAN")


def synced_revision(catalog: Catalog, dataset: str) -> tuple[str, dict[str, str]] | None:
    """The revision the catalog table was last brought to, with that revision's files."""
    if not catalog.has_table(SYNC_TABLE):
        return None
    row = catalog.execute(
        f"SELECT revision_id, files FROM {SYNC_TABLE} WHERE dataset = ?", [dataset]
    ).fetchone()
    return (row[0], json.loads(row[1])) if row else None


def keys_unique(catalog: Catalog, dataset: str) -> bool | None:
    """Whether the catalog table has one row per primary key; ``None`` when not yet checked."""
    if not catalog.has_table(SYNC_TABLE):
        return None
    columns = {row[0] for row in catalog.execute(f"DESCRIBE {SYNC_TABLE}").fetchall()}
    if "unique_keys" not in columns:
        return None
    row = catalog.execute(
        f"SELECT unique_keys FROM {SYNC_TABLE} WHERE dataset = ?", [dataset]
    ).fetchone()
    return row[0] if row else None


def _record_sync(
    catalog: Catalog,
    dataset: str,
    revision_id: str,
    files: dict[str, str],
    unique: bool | None = None,
) -> None:
    catalog.execute(f"DELETE FROM {SYNC_TABLE} WHERE dataset = ?", [dataset])
    catalog.execute(
        f"INSERT INTO {SYNC_TABLE} VALUES (?, ?, ?, ?)",
        [dataset, revision_id, json.dumps(files, sort_keys=True), unique],
    )


# A whole-table key check groups every row; past this size it does not fit in
# memory, and the table is checked one partition at a time instead.
WHOLE_TABLE_CHECK_MAX_ROWS = 100_000_000


def _has_duplicate_keys(catalog: Catalog, dataset: str, where: str | None = None) -> bool:
    keys = ", ".join(quote_identifier(key) for key in PRIMARY_KEYS[dataset])
    scope = f" WHERE {where}" if where else ""
    return (
        catalog.execute(
            f"SELECT 1 FROM {quote_identifier(dataset)}{scope} "
            f"GROUP BY {keys} HAVING count(*) > 1 LIMIT 1"
        ).fetchone()
        is not None
    )


def _partition_key_column(catalog: Catalog, dataset: str) -> str | None:
    """The date partition column, when it is part of the key: partitions then cannot share a key."""
    column = DATASETS[dataset].partition_col
    if not column or column not in PRIMARY_KEYS.get(dataset, ()):
        return None
    types = {
        row[0]: row[1]
        for row in catalog.execute(f"DESCRIBE {quote_identifier(dataset)}").fetchall()
    }
    return column if types.get(column, "").startswith(("DATE", "TIMESTAMP")) else None


def _table_keys_unique(catalog: Catalog, dataset: str) -> bool:
    """One row per primary key in the whole table. PIT datasets keep vintages, so never."""
    keys = PRIMARY_KEYS.get(dataset) or []
    table = quote_identifier(dataset)
    columns = {row[0] for row in catalog.execute(f"DESCRIBE {table}").fetchall()}
    if DATASETS[dataset].pit or not keys or not set(keys) <= columns:
        return False
    rows = catalog.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    if rows <= WHOLE_TABLE_CHECK_MAX_ROWS:
        return not _has_duplicate_keys(catalog, dataset)
    column = _partition_key_column(catalog, dataset)
    if column is None:
        return False
    name = quote_identifier(column)
    months = catalog.execute(f"SELECT DISTINCT year({name}), month({name}) FROM {table}").fetchall()
    return not any(
        _has_duplicate_keys(catalog, dataset, f"year({name}) = {year} AND month({name}) = {month}")
        for year, month in months
        if year is not None
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
) -> str:
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
        return where
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
    return where


def _replace_table(catalog: Catalog, dataset: str, root: Path, new: dict[str, str]) -> None:
    table = quote_identifier(dataset)
    scan = _file_list(root, list(new))
    _add_missing_columns(catalog, table, scan)
    catalog.execute(f"DELETE FROM {table}")
    catalog.execute(f"INSERT INTO {table} BY NAME SELECT * FROM {scan}{_order_by(dataset)}")


HISTORY_TABLE = "_revision_history"
#: One row per revision published into the catalog, written in the snapshot that
#: holds its rows. The catalog stamps every row with the snapshot that wrote it
#: (the ``snapshot_id`` virtual column), so the row is also the revision's
#: history entry: there is nothing to record afterwards, and nothing to lose.
REVISIONS_TABLE = "_revisions"


def _ensure_history_table(catalog: Catalog) -> None:
    catalog.execute(
        f"CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} "
        "(dataset VARCHAR, revision_id VARCHAR, snapshot BIGINT, unique_keys BOOLEAN)"
    )


def revision_snapshots(catalog: Catalog) -> list[tuple[str, str, int, bool]]:
    """``(dataset, revision_id, snapshot, unique_keys)`` of every revision the catalog can name.

    Revisions published into the catalog come from ``_revisions``; revisions
    synced in from a generation come from the history table.
    """
    rows: list[tuple[str, str, int, bool]] = []
    if catalog.has_table(REVISIONS_TABLE):
        rows += catalog.execute(
            f"SELECT dataset, revision_id, snapshot_id, unique_keys FROM {REVISIONS_TABLE}"
        ).fetchall()
    if catalog.has_table(HISTORY_TABLE):
        rows += catalog.execute(
            f"SELECT dataset, revision_id, snapshot, unique_keys FROM {HISTORY_TABLE}"
        ).fetchall()
    return [(d, r, int(s), bool(u)) for d, r, s, u in rows]


def revision_snapshot(catalog: Catalog, dataset: str, revision_id: str) -> tuple[int, bool] | None:
    """The catalog snapshot that holds *dataset* at *revision_id*, with its unique-keys flag.

    A read pinned to an earlier revision is served from that snapshot's files
    for as long as the catalog keeps them.
    """
    if catalog.has_table(REVISIONS_TABLE):
        row = catalog.execute(
            f"SELECT snapshot_id, unique_keys FROM {REVISIONS_TABLE} "
            "WHERE dataset = ? AND revision_id = ? ORDER BY snapshot_id DESC LIMIT 1",
            [dataset, revision_id],
        ).fetchone()
        if row:
            return int(row[0]), bool(row[1])
    if not catalog.has_table(HISTORY_TABLE):
        return None
    row = catalog.execute(
        f"SELECT snapshot, unique_keys FROM {HISTORY_TABLE} "
        "WHERE dataset = ? AND revision_id = ? ORDER BY snapshot DESC LIMIT 1",
        [dataset, revision_id],
    ).fetchone()
    return (int(row[0]), bool(row[1])) if row else None


def _record_history(
    catalog: Catalog, dataset: str, revision_id: str, *, snapshot: int | None = None
) -> None:
    """Remember which snapshot now holds *dataset* at *revision_id*.

    Only for a revision synced in from a generation: recorded after the data
    commit and the delete fold, in a commit of its own, so the snapshot named
    is a file-complete one. A crash in between only loses the entry; that
    revision is then read from its generation. A revision published into the
    catalog needs no entry (see ``REVISIONS_TABLE``).
    """
    _ensure_history_table(catalog)
    if revision_snapshot(catalog, dataset, revision_id) is not None:
        return
    if snapshot is None:
        snapshot = catalog.snapshot()
    catalog.execute(
        f"INSERT INTO {HISTORY_TABLE} VALUES (?, ?, ?, ?)",
        [dataset, revision_id, snapshot, bool(keys_unique(catalog, dataset))],
    )


def sync_dataset(config, catalog: Catalog, dataset: str) -> SyncResult | None:
    """Sync *dataset*, then leave its table file-complete (see ``equitylake_core.catalog``)."""
    result = _sync_dataset(config, catalog, dataset)
    if result is None:
        return None
    if result.action != "current":
        catalog.fold_deletes(dataset)
    _record_history(catalog, dataset, result.revision_id)
    return result


def _sync_dataset(config, catalog: Catalog, dataset: str) -> SyncResult | None:
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
        _record_sync(catalog, dataset, revision_id, new, _table_keys_unique(catalog, dataset))
        return SyncResult(dataset, "imported", revision_id)
    previous = synced_revision(catalog, dataset)
    was_unique = keys_unique(catalog, dataset)
    if previous is not None and previous[0] == revision_id:
        if was_unique is None:  # synced before the flag existed: check once
            _record_sync(catalog, dataset, revision_id, new, _table_keys_unique(catalog, dataset))
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
                unique = was_unique
                if changed:
                    where = _replace_partitions(catalog, dataset, root, new, changed)
                    if was_unique and _partition_key_column(catalog, dataset):
                        # Other partitions cannot hold these keys: check the new rows only.
                        unique = not _has_duplicate_keys(catalog, dataset, where)
                    else:
                        unique = _table_keys_unique(catalog, dataset)
                elif unique is None:
                    unique = _table_keys_unique(catalog, dataset)
                _record_sync(catalog, dataset, revision_id, new, unique)
            return SyncResult(dataset, "partitions", revision_id, len(changed))
        except _PartitionSyncMismatch:
            pass
    with catalog.transaction(message=message):
        _replace_table(catalog, dataset, root, new)
        _record_sync(catalog, dataset, revision_id, new, _table_keys_unique(catalog, dataset))
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
    catalog.keep_rows_in_files()
    results = []
    for dataset in sorted(DATASETS):
        result = sync_dataset(config, catalog, dataset)
        if result is not None:
            results.append(result)
        if catalog.has_table(dataset) and catalog.data_files(dataset) is None:
            catalog.fold_deletes(dataset)
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


# --- retention ---------------------------------------------------------------------

#: Files an expired snapshot no longer needs are deleted only after this long, so
#: a read that started before the expiry finishes on files that still exist.
FILE_GRACE_HOURS = 24


@dataclass(frozen=True)
class RetentionResult:
    snapshots_kept: int
    snapshots_expired: int
    files_deleted: int
    applied: bool


def retained_snapshots(catalog: Catalog, *, keep: int, held: set[str]) -> set[int]:
    """Snapshots to keep: each dataset's ``keep`` newest revisions, held ones, and the latest.

    A snapshot is lake-wide, so keeping one keeps every table's files as of it.
    ``held`` are lifecycle object ids (``revision/<dataset>/<revision_id>``).
    """
    keep_ids = {catalog.snapshot()}
    rows = sorted(
        ((d, r, s) for d, r, s, _ in revision_snapshots(catalog)), key=lambda row: -row[2]
    )
    per_dataset: dict[str, int] = {}
    for dataset, revision_id, snapshot in rows:
        newest = per_dataset.get(dataset, 0)
        if newest < keep:
            keep_ids.add(int(snapshot))
            per_dataset[dataset] = newest + 1
        elif f"revision/{dataset}/{revision_id}" in held:
            keep_ids.add(int(snapshot))
    return keep_ids


def apply_retention(
    catalog: Catalog,
    *,
    keep: int,
    held: set[str] = frozenset(),
    apply: bool = False,
    grace_hours: int = FILE_GRACE_HOURS,
) -> RetentionResult:
    """Expire every snapshot :func:`retained_snapshots` does not keep, then delete their files.

    Files are deleted only once they have waited ``grace_hours`` since the
    snapshot that last used them expired. Without *apply* nothing changes.

    The DuckLake maintenance functions apply only as ``CALL``; inside a
    ``SELECT`` they report what they would do and change nothing. Their result
    rows carry timestamps with a time zone, which DuckDB hands to Python only
    through ``pytz`` (not a dependency), so the rows are dropped unread and the
    effect is counted from the catalog's own tables.
    """
    keep_ids = retained_snapshots(catalog, keep=keep, held=held)
    every = {
        int(row[0])
        for row in catalog.execute(
            f"SELECT snapshot_id FROM {METADATA}.ducklake_snapshot"
        ).fetchall()
    }
    expire = sorted(every - keep_ids)
    if not apply:
        return RetentionResult(len(every & keep_ids), len(expire), 0, False)
    if expire:
        versions = "[" + ", ".join(str(snapshot) for snapshot in expire) + "]"
        catalog.execute(f"CALL ducklake_expire_snapshots('lake', versions => {versions})")
        catalog.con.abort()
    remaining = catalog.execute(f"SELECT count(*) FROM {METADATA}.ducklake_snapshot").fetchone()[0]
    scheduled = f"SELECT count(*) FROM {METADATA}.ducklake_files_scheduled_for_deletion"
    before = catalog.execute(scheduled).fetchone()[0]
    catalog.execute(
        "CALL ducklake_cleanup_old_files('lake', "
        f"older_than => now() - INTERVAL ({int(grace_hours)}) HOUR)"
    )
    catalog.con.abort()
    after = catalog.execute(scheduled).fetchone()[0]
    return RetentionResult(len(every & keep_ids), len(every) - int(remaining), before - after, True)


def catalog_retention(config, *, apply: bool) -> dict | None:
    """Apply revision retention to the lake catalog; ``None`` for a lake without one.

    Keeps ``[storage.retention].keep_generations`` revisions of every dataset,
    like the revision store, and every revision a lifecycle hold protects.
    """
    from cnequity.storage.lifecycle import LifecycleStore
    from equitylake_core.catalog import CATALOG_FILE

    root = lake_root(config)
    if not (root / CATALOG_FILE).exists():
        return None
    keep = getattr(config, "revision_keep_generations", 5)
    keep = keep if isinstance(keep, int) and not isinstance(keep, bool) and keep >= 1 else 5
    registry = LifecycleStore(config.meta_root).registry() or {}
    held = set(registry.get("holds") or {})
    with Catalog(root) as catalog:
        result = apply_retention(catalog, keep=keep, held=held, apply=apply)
    return {
        "keep_revisions": keep,
        "snapshots_kept": result.snapshots_kept,
        "snapshots_expired": result.snapshots_expired,
        "files_deleted": result.files_deleted,
        "applied": result.applied,
        "file_grace_hours": FILE_GRACE_HOURS,
    }
