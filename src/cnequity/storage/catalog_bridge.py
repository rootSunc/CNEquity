"""Bring the revision store's committed datasets into the lake catalog, and prove they match.

The catalog (``equitylake_core.catalog``) is where datasets will be published.
Until publishing moves there, this module copies each dataset's committed
generation into it and compares the two copies row for row. It reads the
revision store and never writes to it; the catalog lives in its own
directory beside ``curated/`` and ``meta/``.
"""

from __future__ import annotations

import json
import threading
from contextlib import ExitStack, nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

from cnequity.config.resources import resources_for
from cnequity.config.retention import revision_keep, revision_keep_days
from cnequity.domain.datasets import DATASETS
from cnequity.domain.schemas import PRIMARY_KEYS
from cnequity.meta_layout import LAKE_DIR
from cnequity.storage.revision_records import (
    HISTORY_TABLE,
    RECEIPT_COLUMNS,
    SYNC_TABLE,
    receipt_columns,
)
from cnequity.storage.revisions import RevisionStore
from equitylake_core.catalog import (
    ALIAS,
    KNOWN_METADATA_FORMATS,
    METADATA,
    REVISIONS_TABLE,
    Catalog,
    ImportResult,
    quote_identifier,
    quote_literal,
)
from equitylake_core.lock import LockTimeout, lake_files, lake_read


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


def open_lake(config):
    """Open *config*'s lake through the market-neutral :class:`equitylake_core.lake.Lake`."""
    from cnequity.domain.core_registry import core_registry
    from equitylake_core.lake import Lake

    return Lake(lake_root(config), core_registry())


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


def import_dataset(config, catalog: Catalog, dataset: str) -> ImportResult | None:
    """Register *dataset*'s committed generation in the catalog as one snapshot."""
    committed = _committed_files(config, dataset)
    if committed is None:
        return None
    revision_id, root, files = committed
    with catalog.transaction(message=f"import {dataset} from revision {revision_id}"):
        rows = _register(catalog, dataset, root, add=sorted(files), remove=[])
    return ImportResult(dataset, rows, catalog.snapshot())


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


@dataclass(frozen=True)
class SyncResult:
    dataset: str
    # "current": nothing to do; "imported": first copy; "partitions": only the
    # changed partitions were replaced; "replaced": the whole table was.
    action: str
    revision_id: str
    partitions: int = 0


SYNC_FILES_TABLE = "_sync_files"
#: Where retention moved a kept revision's reads: a later snapshot holding the
#: same rows of that dataset, kept in place of the one the revision was recorded at.
READS_TABLE = "_revision_reads"
#: The record an earlier release kept: the same rows, each carrying its file list.
LEGACY_SYNC_TABLE = "_revision_sync"


def _ensure_sync_table(catalog: Catalog) -> None:
    if not catalog.has_table(SYNC_TABLE) and catalog.has_table(LEGACY_SYNC_TABLE):
        # Writing a fresh record beside the old one would hide every dataset in it.
        raise RuntimeError("the lake's sync records were not prepared before this write")
    catalog.execute(
        f"CREATE TABLE IF NOT EXISTS {SYNC_TABLE} "
        "(dataset VARCHAR, revision_id VARCHAR, unique_keys BOOLEAN)"
    )
    catalog.execute(
        f"CREATE TABLE IF NOT EXISTS {SYNC_FILES_TABLE} (dataset VARCHAR, files VARCHAR)"
    )


def prepare_record_tables(catalog: Catalog) -> None:
    """Make the tables of records and keep their rows in the catalog database.

    A row per dataset or per publish, read by key: as data files they grow by
    a tiny file per write, and every reader pays for all of them. Called
    outside a transaction, once per writer: the catalog cannot set this for a
    table the transaction made.

    The sync record used to carry each generation's file list (hundreds of
    kilobytes for a large dataset) in the row every reader looks up, in a
    table of data files. Its rows are moved here once, in one transaction
    with dropping it: the lists into a table only the sync from generations
    reads.
    """
    legacy = catalog.has_table(LEGACY_SYNC_TABLE)
    for table, columns in (
        (SYNC_TABLE, "dataset VARCHAR, revision_id VARCHAR, unique_keys BOOLEAN"),
        (SYNC_FILES_TABLE, "dataset VARCHAR, files VARCHAR"),
        # The columns a reader looks a revision up by; a publish adds the rest.
        (
            REVISIONS_TABLE,
            "dataset VARCHAR, revision_id VARCHAR, unique_keys BOOLEAN",
        ),
    ):
        if not catalog.has_table(table):
            catalog.execute(f"CREATE TABLE {table} ({columns})")
        catalog.keep_rows_inline(table)
    # Both tables of revision records carry what a receipt says, so a reader
    # can ask either before anything has been written to it.
    _ensure_history_table(catalog)
    have = set(catalog.column_types(REVISIONS_TABLE))
    for name, kind in RECEIPT_COLUMNS:
        if name not in have:
            catalog.execute(f"ALTER TABLE {REVISIONS_TABLE} ADD COLUMN {name} {kind}")
    if not legacy:
        return
    described = {row[0] for row in catalog.execute(f"DESCRIBE {LEGACY_SYNC_TABLE}").fetchall()}
    unique = "unique_keys" if "unique_keys" in described else "NULL"
    rows = catalog.execute(
        f"SELECT dataset, revision_id, files, {unique} FROM {LEGACY_SYNC_TABLE}"
    ).fetchall()
    with catalog.transaction(message="keep sync records in the catalog database"):
        catalog.execute(f"DELETE FROM {SYNC_TABLE}")
        catalog.execute(f"DELETE FROM {SYNC_FILES_TABLE}")
        for dataset, revision_id, files, flag in rows:
            catalog.execute(
                f"INSERT INTO {SYNC_TABLE} VALUES (?, ?, ?)", [dataset, revision_id, flag]
            )
            if files and files != "{}":
                catalog.execute(f"INSERT INTO {SYNC_FILES_TABLE} VALUES (?, ?)", [dataset, files])
        catalog.execute(f"DROP TABLE {LEGACY_SYNC_TABLE}")


class _Records(NamedTuple):
    """The small record tables of one catalog snapshot, read whole."""

    #: dataset -> (revision_id, unique_keys) of the sync record.
    synced: dict[str, tuple[str, bool | None]]
    #: (dataset, revision_id) -> (snapshot, unique_keys), the latest snapshot holding it.
    snapshots: dict[tuple[str, str], tuple[int, bool]]


_records_lock = threading.Lock()


def _records(catalog: Catalog) -> _Records | None:
    """The record tables of a read-only catalog at its current snapshot, read once.

    Every filtered query on a DuckLake table costs about 0.13 s on the real
    lake whatever it reads (an unfiltered read of these tables costs 0.02 s),
    and a compact resolves a revision several hundred times. A writer's
    catalog is never cached: its open transaction is newer than any snapshot.

    The records are kept on the connection, by snapshot. Not by path: the
    publication gate rehearses each candidate on a fresh copy of the catalog
    at one path, and two copies reach the same snapshot number with
    different contents.
    """
    if not catalog.read_only:
        return None
    current = catalog.snapshot()
    with _records_lock:
        kept = getattr(catalog, "_cnequity_records", None)
    if kept is not None and kept[0] == current:
        return kept[1]
    tables = set(catalog.tables())
    synced: dict[str, tuple[str, bool | None]] = {}
    state = (
        SYNC_TABLE
        if SYNC_TABLE in tables
        else (LEGACY_SYNC_TABLE if LEGACY_SYNC_TABLE in tables else None)
    )
    if state is not None:
        columns = {row[0] for row in catalog.execute(f"DESCRIBE {state}").fetchall()}
        unique = "unique_keys" if "unique_keys" in columns else "NULL"
        for dataset, revision_id, flag in catalog.execute(
            f"SELECT dataset, revision_id, {unique} FROM {state}"
        ).fetchall():
            synced[dataset] = (revision_id, flag)
    snapshots: dict[tuple[str, str], tuple[int, bool]] = {}
    # The history table first, so the record table wins where both name a revision.
    if HISTORY_TABLE in tables:
        for dataset, revision_id, snapshot, flag in catalog.execute(
            f"SELECT dataset, revision_id, snapshot, unique_keys FROM {HISTORY_TABLE}"
        ).fetchall():
            if snapshot is None:
                continue
            old = snapshots.get((dataset, revision_id))
            if old is None or int(snapshot) > old[0]:
                snapshots[(dataset, revision_id)] = (int(snapshot), bool(flag))
    if REVISIONS_TABLE in tables:
        latest: dict[tuple[str, str], tuple[int, bool]] = {}
        for dataset, revision_id, snapshot, flag in catalog.execute(
            f"SELECT dataset, revision_id, snapshot_id, unique_keys FROM {REVISIONS_TABLE}"
        ).fetchall():
            old = latest.get((dataset, revision_id))
            if old is None or int(snapshot) > old[0]:
                latest[(dataset, revision_id)] = (int(snapshot), bool(flag))
        snapshots.update(latest)
    if READS_TABLE in tables:
        snapshots.update(_read_overrides(catalog))
    found = _Records(synced, snapshots)
    with _records_lock:
        catalog._cnequity_records = (current, found)
    return found


def _state_table(catalog: Catalog) -> str | None:
    """The table holding the sync record: the old one until a writer has moved it."""
    if catalog.has_table(SYNC_TABLE):
        return SYNC_TABLE
    return LEGACY_SYNC_TABLE if catalog.has_table(LEGACY_SYNC_TABLE) else None


def synced_revision_id(catalog: Catalog, dataset: str) -> str | None:
    """The revision the catalog table was last brought to."""
    records = _records(catalog)
    if records is not None:
        entry = records.synced.get(dataset)
        return entry[0] if entry else None
    table = _state_table(catalog)
    if table is None:
        return None
    row = catalog.execute(
        f"SELECT revision_id FROM {table} WHERE dataset = ?", [dataset]
    ).fetchone()
    return row[0] if row else None


def synced_revision_ids(catalog: Catalog) -> dict[str, str]:
    """The revision every catalog table was last brought to, by dataset."""
    table = _state_table(catalog)
    if table is None:
        return {}
    return dict(catalog.execute(f"SELECT dataset, revision_id FROM {table}").fetchall())


def synced_revision(catalog: Catalog, dataset: str) -> tuple[str, dict[str, str]] | None:
    """The revision the catalog table was last brought to, with that revision's files."""
    revision_id = synced_revision_id(catalog, dataset)
    if revision_id is None:
        return None
    if _state_table(catalog) == LEGACY_SYNC_TABLE:
        source = LEGACY_SYNC_TABLE
    else:
        source = SYNC_FILES_TABLE if catalog.has_table(SYNC_FILES_TABLE) else None
    files = (
        catalog.execute(f"SELECT files FROM {source} WHERE dataset = ?", [dataset]).fetchone()
        if source
        else None
    )
    return revision_id, json.loads(files[0]) if files and files[0] else {}


def keys_unique(catalog: Catalog, dataset: str) -> bool | None:
    """Whether the catalog table has one row per primary key; ``None`` when not yet checked."""
    records = _records(catalog)
    if records is not None:
        entry = records.synced.get(dataset)
        return entry[1] if entry else None
    table = _state_table(catalog)
    if table is None:
        return None
    try:
        row = catalog.execute(
            f"SELECT unique_keys FROM {table} WHERE dataset = ?", [dataset]
        ).fetchone()
    except Exception:  # a catalog synced before the flag existed
        return None
    return row[0] if row else None


def _record_sync(
    catalog: Catalog,
    dataset: str,
    revision_id: str,
    files: dict[str, str],
    unique: bool | None = None,
) -> None:
    catalog.execute(f"DELETE FROM {SYNC_TABLE} WHERE dataset = ?", [dataset])
    catalog.execute(f"INSERT INTO {SYNC_TABLE} VALUES (?, ?, ?)", [dataset, revision_id, unique])
    catalog.execute(f"DELETE FROM {SYNC_FILES_TABLE} WHERE dataset = ?", [dataset])
    if files:
        catalog.execute(
            f"INSERT INTO {SYNC_FILES_TABLE} VALUES (?, ?)",
            [dataset, json.dumps(files, sort_keys=True)],
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


def files_have_duplicate_keys(catalog: Catalog, dataset: str, files: list[str]) -> bool:
    """Whether the data files *files* hold two rows with one primary key."""
    if not files:
        return False
    keys = ", ".join(quote_identifier(key) for key in PRIMARY_KEYS[dataset])
    listed = ", ".join(quote_literal(path) for path in files)
    return (
        catalog.execute(
            f"SELECT 1 FROM read_parquet([{listed}], union_by_name => true, "
            f"hive_partitioning => false) GROUP BY {keys} HAVING count(*) > 1 LIMIT 1"
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


def _lake_key(relative: str) -> tuple[str, str]:
    """The lake folder and partition-file name a generation file goes by."""
    head, _, rest = relative.partition("/")
    if "=" in head and rest:
        return head.partition("=")[2], Path(rest).name
    return "", Path(relative).name


def _laid_out_files(catalog: Catalog, dataset: str) -> dict[tuple[str, str] | None, list[str]]:
    """The table's data files by lake folder and name; ``None`` collects files not laid out so."""
    from cnequity.storage.lake_write import _LAID_OUT, logical_name

    table = catalog.table_dir(dataset)
    found: dict[tuple[str, str] | None, list[str]] = {}
    for path in _listed_files(catalog, dataset):
        relative = Path(path).relative_to(table) if table in Path(path).parents else None
        key = None
        if relative is not None and len(relative.parts) <= 2 and _LAID_OUT.match(relative.name):
            key = (relative.parts[0] if len(relative.parts) == 2 else "", logical_name(path))
        found.setdefault(key, []).append(path)
    return found


def _listed_files(catalog: Catalog, dataset: str) -> list[str]:
    return [
        row[0]
        for row in catalog.execute(
            f"SELECT data_file FROM ducklake_list_files('{ALIAS}', ?)", [dataset]
        ).fetchall()
    ]


#: Where a file goes whose rows are not all of the partition its directory names.
MIXED_FOLDER = "_mixed"


def _misfiled(catalog: Catalog, dataset: str, root: Path, relatives: list[str]) -> set[str]:
    """The generation files holding a row outside the partition their directory names.

    The lake reads a partition's range off its folder, so a file is filed
    under a partition only if every row in it belongs there. An old writer
    could leave a month's file with another month's rows in it; such a file
    is kept apart, and split the first time a partition in it changes.
    """
    from cnequity.domain.partitions import parse_partition, partition_value

    spec = DATASETS[dataset]
    if not spec.partition_col or not relatives:
        return set()
    column = quote_identifier(spec.partition_col)
    listed = ", ".join(quote_literal((root / relative).as_posix()) for relative in relatives)
    try:
        bounds = catalog.execute(
            f"SELECT filename, min({column}), max({column}), count(*) - count({column}) "
            f"FROM read_parquet([{listed}], union_by_name => true, hive_partitioning => false, "
            "filename => true) GROUP BY filename"
        ).fetchall()
    except Exception:  # no such column in these files: nothing can be said of them
        return set(relatives)
    by_path = {Path(path).as_posix(): (low, high, nulls) for path, low, high, nulls in bounds}
    out = set()
    for relative in relatives:
        folder = _lake_key(relative)[0]
        low, high, nulls = by_path.get((root / relative).as_posix(), (None, None, 1))
        period = parse_partition(folder) if folder else None
        fits = period is not None and low is not None and not nulls
        if fits:
            try:
                first, last = _as_day(low), _as_day(high)
                fits = (
                    partition_value(period.start, spec.partition_granularity) == folder
                    and period.start <= first
                    and last <= period.end
                )
            except (TypeError, ValueError):
                # Not a date (a quarter label): the value itself names the partition.
                fits = str(low) == folder and str(high) == folder
        if not fits:
            out.add(relative)
    return out


def _as_day(value):
    from datetime import date, datetime

    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _register(
    catalog: Catalog, dataset: str, root: Path, *, add: list[str], remove: list[str]
) -> int:
    """Inside an open transaction: make the generation files *add* data files of the table.

    Each is linked into the lake under ``<table>/<partition value>/`` and
    registered as it is; a file whose column types the table cannot read is
    written once more with the table's types. *remove* are the lake files
    they replace. Returns the rows registered.
    """
    from cnequity.storage.lake_write import _place, _protect, lake_file_name
    from equitylake_core.write import Swap, apply_swap

    sources = [root / relative for relative in add]
    types = None
    if sources:
        types = catalog.file_types(sources)
        if catalog.has_table(dataset):
            types = {**types, **catalog.column_types(dataset)}
    table = catalog.table_dir(dataset)
    misfiled = _misfiled(catalog, dataset, root, add)
    placed: list[Path] = []
    try:
        for relative, source in zip(add, sources, strict=True):
            folder, name = _lake_key(relative)
            if relative in misfiled:
                folder = MIXED_FOLDER
            target = (table / folder if folder else table) / lake_file_name(name)
            placed.append(target)
            if catalog.file_fits(dataset, source, types=types):
                _place(source, target)
            else:
                catalog.conform_file(dataset, source, target, types=types)
            _protect(target)
        return apply_swap(
            catalog,
            Swap(dataset, tuple(path.as_posix() for path in placed), tuple(remove)),
        )
    except BaseException:
        for path in placed:
            path.unlink(missing_ok=True)
        raise


def _ensure_history_table(catalog: Catalog) -> None:
    catalog.execute(
        f"CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} "
        "(dataset VARCHAR, revision_id VARCHAR, snapshot BIGINT, unique_keys BOOLEAN)"
    )
    have = set(catalog.column_types(HISTORY_TABLE))
    for name, kind in RECEIPT_COLUMNS:
        if name not in have:
            catalog.execute(f"ALTER TABLE {HISTORY_TABLE} ADD COLUMN {name} {kind}")


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
            f"SELECT dataset, revision_id, snapshot, unique_keys FROM {HISTORY_TABLE} "
            "WHERE snapshot IS NOT NULL"
        ).fetchall()
    return [(d, r, int(s), bool(u)) for d, r, s, u in rows]


def _read_overrides(catalog: Catalog) -> dict[tuple[str, str], tuple[int, bool]]:
    return {
        (dataset, revision_id): (int(snapshot), bool(flag))
        for dataset, revision_id, snapshot, flag in catalog.execute(
            f"SELECT dataset, revision_id, snapshot, unique_keys FROM {READS_TABLE}"
        ).fetchall()
    }


def revision_read_snapshots(catalog: Catalog) -> list[tuple[str, str, int, bool]]:
    """Like :func:`revision_snapshots`, but each revision at the snapshot it is read from.

    That is the recorded one unless retention moved the revision's reads to a
    later snapshot holding the same rows (:data:`READS_TABLE`).
    """
    moved = _read_overrides(catalog) if catalog.has_table(READS_TABLE) else {}
    found: dict[tuple[str, str], tuple[int, bool]] = {}
    for dataset, revision_id, snapshot, flag in revision_snapshots(catalog):
        key = (dataset, revision_id)
        if key not in found or snapshot > found[key][0]:
            found[key] = (snapshot, flag)
    found.update(moved)
    return [(d, r, s, u) for (d, r), (s, u) in found.items()]


def revision_snapshot(catalog: Catalog, dataset: str, revision_id: str) -> tuple[int, bool] | None:
    """The catalog snapshot that holds *dataset* at *revision_id*, with its unique-keys flag.

    A read pinned to an earlier revision is served from that snapshot's files
    for as long as the catalog keeps them.
    """
    records = _records(catalog)
    if records is not None:
        return records.snapshots.get((dataset, revision_id))
    if catalog.has_table(READS_TABLE):
        row = catalog.execute(
            f"SELECT snapshot, unique_keys FROM {READS_TABLE} "
            "WHERE dataset = ? AND revision_id = ?",
            [dataset, revision_id],
        ).fetchone()
        if row:
            return int(row[0]), bool(row[1])
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
        "WHERE dataset = ? AND revision_id = ? AND snapshot IS NOT NULL "
        "ORDER BY snapshot DESC LIMIT 1",
        [dataset, revision_id],
    ).fetchone()
    return (int(row[0]), bool(row[1])) if row else None


def _record_history(
    catalog: Catalog,
    dataset: str,
    revision_id: str,
    *,
    snapshot: int | None = None,
    receipt: dict | None = None,
) -> None:
    """Remember which snapshot now holds *dataset* at *revision_id*.

    Only for a revision synced in from a generation: recorded after the data
    commit and the delete fold, in a commit of its own, so the snapshot named
    is a file-complete one. A crash in between only loses the entry; that
    revision is then read from its generation. A revision published into the
    catalog needs no entry (see ``REVISIONS_TABLE``). *receipt* is the
    revision's receipt; the entry keeps what it says (:data:`RECEIPT_COLUMNS`).
    """
    _ensure_history_table(catalog)
    if revision_snapshot(catalog, dataset, revision_id) is not None:
        return
    if snapshot is None:
        snapshot = catalog.snapshot()
    values = {
        "dataset": dataset,
        "revision_id": revision_id,
        "snapshot": snapshot,
        "unique_keys": bool(keys_unique(catalog, dataset)),
        **(receipt_columns(receipt) if receipt else {}),
    }
    names = ", ".join(values)
    marks = ", ".join("?" for _ in values)
    catalog.execute(
        f"INSERT INTO {HISTORY_TABLE} ({names}) VALUES ({marks})", list(values.values())
    )


def sync_dataset(config, catalog: Catalog, dataset: str) -> SyncResult | None:
    """Sync *dataset*, then leave its table file-complete (see ``equitylake_core.catalog``)."""
    prepare_record_tables(catalog)
    result = _sync_dataset(config, catalog, dataset)
    if result is None:
        return None
    if result.action != "current":
        catalog.fold_deletes(dataset)
    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root, create=False)
    receipt = store.receipt_file(dataset)
    if receipt is not None and receipt.revision_id != result.revision_id:
        receipt = None
    _record_history(
        catalog, dataset, result.revision_id, receipt=receipt.to_dict() if receipt else None
    )
    return result


def _sync_dataset(config, catalog: Catalog, dataset: str) -> SyncResult | None:
    """Bring the catalog table to the committed revision, replacing only the files that changed.

    The generation's files become the table's data files as they are: each is
    linked into the lake and registered, and the lake file it replaces is
    dropped whole, in one transaction with the sync record. The change is
    found by comparing the committed file list with the one recorded at the
    last sync. A table whose files are not laid out this way (the catalog
    wrote them itself) is replaced whole, once.
    """
    committed = _committed_files(config, dataset)
    if committed is None:
        return None
    revision_id, root, new = committed
    _ensure_sync_table(catalog)
    existed = catalog.has_table(dataset)
    previous = synced_revision(catalog, dataset) if existed else None
    was_unique = keys_unique(catalog, dataset) if existed else None
    if previous is not None and previous[0] == revision_id:
        if was_unique is None:  # synced before the flag existed: check once
            _record_sync(catalog, dataset, revision_id, new, _table_keys_unique(catalog, dataset))
        return SyncResult(dataset, "current", revision_id)

    held = _laid_out_files(catalog, dataset) if existed else {}
    wanted: dict[tuple[str, str], str] = {}
    for relative in new:
        wanted.setdefault(_lake_key(relative), relative)
    whole = (
        not existed
        or previous is None
        or None in held
        or len(wanted) != len(new)
        or any(len(paths) > 1 for paths in held.values())
    )
    if whole:
        add = sorted(new)
        remove = sorted(path for paths in held.values() for path in paths)
    else:
        old = previous[1]
        add, remove = [], []
        for key, relative in sorted(wanted.items()):
            if key in held and old.get(relative) == new[relative]:
                continue
            add.append(relative)
            remove.extend(held.get(key, []))
        remove.extend(path for key, paths in held.items() if key not in wanted for path in paths)
    if existed and None in held:
        # Registered files are refused by a table the catalog partitions itself.
        catalog.unpartition(dataset)
    with catalog.transaction(message=f"sync {dataset} to revision {revision_id}"):
        if add or remove:
            _register(catalog, dataset, root, add=add, remove=remove)
        _record_sync(catalog, dataset, revision_id, new, _table_keys_unique(catalog, dataset))
    if not existed:
        return SyncResult(dataset, "imported", revision_id)
    if whole:
        return SyncResult(dataset, "replaced", revision_id)
    folders = {_lake_key(relative)[0] for relative in add} | {
        Path(path).parent.name for path in remove
    }
    return SyncResult(dataset, "partitions", revision_id, len(folders))


def catalog_is_current(config, catalog: Catalog, dataset: str) -> bool:
    """True when the catalog table holds exactly the lake's current revision of *dataset*."""
    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root, create=False)
    pointer = store.current_pointer(dataset)
    return pointer is not None and synced_revision_id(catalog, dataset) == str(
        pointer.get("revision_id", "")
    )


def sync_all(config, catalog: Catalog, progress=None) -> list[SyncResult]:
    """Bring every dataset with a committed generation to its committed revision.

    *progress* is called with each dataset's name before it is synced.
    """
    catalog.keep_rows_in_files()
    prepare_record_tables(catalog)
    results = []
    for dataset in sorted(DATASETS):
        if (
            progress is not None
            and committed_source(config, dataset) is not None
            and not catalog_is_current(config, catalog, dataset)
        ):
            progress(dataset)
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


# --- export -------------------------------------------------------------------------


class ExportRefused(RuntimeError):
    """The dataset cannot be exported as asked; nothing was written."""


def export_dataset(
    config, dataset: str, destination: Path, *, start=None, end=None
) -> dict[str, object]:
    """Export *dataset*'s published rows to plain Parquet under ``destination/<dataset>``.

    Only a dataset whose published revision the catalog holds is exported, so
    the files are what readers of the lake see. *start* and *end* (dates,
    inclusive) select rows by the dataset's partition column.
    """
    from equitylake_core.catalog import CATALOG_FILE

    if dataset not in DATASETS:
        raise ExportRefused(f"unknown dataset: {dataset}")
    root = lake_root(config)
    if not (root / CATALOG_FILE).exists():
        raise ExportRefused("this lake has no lake catalog")
    target = Path(destination) / dataset
    if target.exists() and any(target.iterdir()):
        raise ExportRefused(f"{target} is not empty")
    with (
        lake_read(root),
        Catalog(root, read_only=True, **resources_for(config, "export").options()) as catalog,
        catalog.transaction(),
    ):
        if not catalog_is_current(config, catalog, dataset):
            raise ExportRefused(
                f"the lake catalog does not hold the published revision of {dataset}; "
                "run cne upgrade --apply first"
            )
        types = {
            row[0]: row[1]
            for row in catalog.execute(f"DESCRIBE {quote_identifier(dataset)}").fetchall()
        }
        column = DATASETS[dataset].partition_col
        dated = bool(column) and types.get(column, "").startswith(("DATE", "TIMESTAMP"))
        clauses = []
        if start is not None or end is not None:
            if not dated:
                raise ExportRefused(f"{dataset} has no date column to select a range by")
            name = quote_identifier(column)
            if start is not None:
                clauses.append(f"CAST({name} AS DATE) >= DATE {quote_literal(str(start))}")
            if end is not None:
                clauses.append(f"CAST({name} AS DATE) <= DATE {quote_literal(str(end))}")
        revision = synced_revision(catalog, dataset)
        # One snapshot for the revision identity, schema, count and copy.
        rows = catalog.export_parquet(
            dataset,
            target,
            partition_by=column if dated else None,
            where=" AND ".join(clauses) or None,
        )
        snapshot = catalog.snapshot()
    files = sorted(target.rglob("*.parquet"))
    return {
        "dataset": dataset,
        "rows": rows,
        "files": len(files),
        "bytes": sum(path.stat().st_size for path in files),
        "revision_id": revision[0] if revision else None,
        "snapshot": snapshot,
        "path": str(target),
    }


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
    active_readers: bool = False
    #: Bytes of data files no kept snapshot uses; deleted once past the grace.
    #: ``None`` where the catalog's layout is not one this code reads.
    bytes_freed: int | None = None


@dataclass(frozen=True)
class RetentionPlan:
    #: Snapshots that stay: the latest and one per group of kept revisions.
    keep: set[int]
    #: (dataset, revision_id) -> (snapshot, unique_keys) for each kept
    #: revision now read from a snapshot other than the one it was recorded at.
    reads: dict[tuple[str, str], tuple[int, bool]]
    #: Every snapshot the catalog has now, with its time (epoch seconds).
    snapshots: dict[int, float]


_NEVER = 1 << 62


def _table_changes(catalog: Catalog) -> dict[str, list[int]] | None:
    """Snapshots that changed each table, by table name; ``None`` without a known layout.

    The catalog keeps this log for expired snapshots too, so it says whether
    a table stayed the same between two snapshots even when the ones in
    between are gone. Every kind of change counts, a file rewrite included:
    keeping one snapshot more is the only cost of counting too much.
    """
    import re

    if catalog.metadata_format() not in KNOWN_METADATA_FORMATS:
        return None
    names: dict[int, str] = {
        int(table_id): name
        for table_id, name in catalog.execute(
            f"SELECT table_id, table_name FROM {METADATA}.ducklake_table"
        ).fetchall()
    }
    changed: dict[str, set[int]] = {}
    for snapshot, made in catalog.execute(
        f"SELECT snapshot_id, changes_made FROM {METADATA}.ducklake_snapshot_changes"
    ).fetchall():
        for table_id in re.findall(r"[a-z_]+:(\d+)", made or ""):
            name = names.get(int(table_id))
            if name is not None:
                changed.setdefault(name, set()).add(int(snapshot))
    return {name: sorted(snapshots) for name, snapshots in changed.items()}


def plan_retention(
    catalog: Catalog,
    *,
    keep: int,
    held: set[str] = frozenset(),
    keep_days: int = 0,
    now: float | None = None,
) -> RetentionPlan:
    """Which snapshots retention keeps, and where each kept revision is read from.

    A revision is kept when it is one of its dataset's ``keep`` newest and was
    current at some time in the last ``keep_days`` days (0: at any time), or
    when a hold protects it (``held`` are ``revision/<dataset>/<revision_id>``).

    A snapshot is lake-wide: keeping one keeps every table's files as of it.
    So a kept revision is not tied to the snapshot it was recorded at. Its
    rows stay the same from that snapshot until the next change to its table,
    and any snapshot in between serves it. One snapshot is chosen for as many
    kept revisions as it can serve, the latest such: an old snapshot kept for
    a dataset that rarely changes would keep every other table's replaced
    files with it. Without the catalog's change log, each revision is read
    from its recorded snapshot only.
    """
    import time

    snapshots = {
        int(snapshot): float(stamp)
        for snapshot, stamp in catalog.execute(
            f"SELECT snapshot_id, epoch(snapshot_time) FROM ducklake_snapshots('{ALIAS}')"
        ).fetchall()
    }
    live = sorted(snapshots)
    latest = live[-1]
    now = time.time() if now is None else now
    changes = _table_changes(catalog)

    recorded: dict[tuple[str, str], tuple[int, bool]] = {}
    for dataset, revision_id, snapshot, flag in revision_snapshots(catalog):
        key = (dataset, revision_id)
        if key not in recorded or snapshot > recorded[key][0]:
            recorded[key] = (snapshot, flag)

    def end_of(dataset: str, start: int) -> int:
        """The first snapshot after *start* that changed *dataset*'s table."""
        if changes is None:
            return start + 1
        from bisect import bisect_right

        changed = changes.get(dataset, [])
        at = bisect_right(changed, start)
        return changed[at] if at < len(changed) else _NEVER

    def time_of(snapshot: int) -> float:
        """When *snapshot* was taken, or if it expired, an upper bound: the next one kept."""
        from bisect import bisect_left

        if snapshot in snapshots:
            return snapshots[snapshot]
        at = bisect_left(live, snapshot)
        return snapshots[live[at]] if at < len(live) else now

    by_dataset: dict[str, list[tuple[int, str, bool]]] = {}
    for (dataset, revision_id), (snapshot, flag) in recorded.items():
        by_dataset.setdefault(dataset, []).append((snapshot, revision_id, flag))
    cutoff = now - keep_days * 86400 if keep_days else None
    wanted: list[tuple[int, int, str, str, bool]] = []
    for dataset, revisions in by_dataset.items():
        for rank, (start, revision_id, flag) in enumerate(sorted(revisions, reverse=True)):
            end = end_of(dataset, start)
            recent = rank < keep and (cutoff is None or end == _NEVER or time_of(end) >= cutoff)
            if recent or f"revision/{dataset}/{revision_id}" in held:
                wanted.append((end, start, dataset, revision_id, flag))

    from bisect import bisect_left

    chosen = {latest}
    reads: dict[tuple[str, str], tuple[int, bool]] = {}
    for end, start, dataset, revision_id, flag in sorted(wanted):
        # The latest live snapshot in [start, end): the greedy choice for
        # covering intervals with the fewest points, and the newest files.
        at = bisect_left(live, min(end, latest + 1)) - 1
        if at < 0 or live[at] < start:
            continue  # Nothing left to read it from.
        taken = [s for s in chosen if start <= s < end]
        snapshot = max(taken) if taken else live[at]
        chosen.add(snapshot)
        if snapshot != start:
            reads[(dataset, revision_id)] = (snapshot, flag)
    return RetentionPlan(chosen, reads, snapshots)


def retained_snapshots(
    catalog: Catalog, *, keep: int, held: set[str], keep_days: int = 0
) -> set[int]:
    """The snapshots :func:`plan_retention` keeps."""
    return plan_retention(catalog, keep=keep, held=held, keep_days=keep_days).keep


def _bytes_freed(catalog: Catalog, keep: set[int]) -> int | None:
    """Bytes of the data files that no snapshot in *keep* uses."""
    if catalog.metadata_format() not in KNOWN_METADATA_FORMATS:
        return None
    rows = catalog.execute(
        f"SELECT begin_snapshot, end_snapshot, file_size_bytes FROM {METADATA}.ducklake_data_file "
        "WHERE end_snapshot IS NOT NULL"
    ).fetchall()
    kept = sorted(keep)
    from bisect import bisect_left

    freed = 0
    for begin, end, size in rows:
        at = bisect_left(kept, int(begin))
        if at == len(kept) or kept[at] >= int(end):
            freed += int(size or 0)
    return freed


def _write_reads(catalog: Catalog, reads: dict[tuple[str, str], tuple[int, bool]]) -> None:
    """Replace :data:`READS_TABLE` with *reads*, in one snapshot."""
    if not catalog.has_table(READS_TABLE):
        catalog.execute(
            f"CREATE TABLE {READS_TABLE} "
            "(dataset VARCHAR, revision_id VARCHAR, snapshot BIGINT, unique_keys BOOLEAN)"
        )
    catalog.keep_rows_inline(READS_TABLE)
    if _read_overrides(catalog) == reads:
        return
    with catalog.transaction(message="retention: read kept revisions from kept snapshots"):
        catalog.execute(f"DELETE FROM {READS_TABLE}")
        for (dataset, revision_id), (snapshot, flag) in sorted(reads.items()):
            catalog.execute(
                f"INSERT INTO {READS_TABLE} VALUES (?, ?, ?, ?)",
                [dataset, revision_id, snapshot, flag],
            )


def apply_retention(
    catalog: Catalog,
    *,
    keep: int,
    held: set[str] = frozenset(),
    keep_days: int = 0,
    apply: bool = False,
    grace_hours: int = FILE_GRACE_HOURS,
) -> RetentionResult:
    """Expire every snapshot :func:`plan_retention` does not keep, then delete their files.

    Before anything expires, each kept revision's reads move to the snapshot
    kept for it, in a commit of their own. Files are deleted only once they
    have waited ``grace_hours`` since the snapshot that last used them
    expired. Without *apply* nothing changes.

    The DuckLake maintenance functions apply only as ``CALL``; inside a
    ``SELECT`` they report what they would do and change nothing. Their result
    rows carry timestamps with a time zone, which DuckDB hands to Python only
    through ``pytz`` (not a dependency), so the rows are dropped unread and the
    effect is counted from the files in the lake before and after.
    """
    keep = revision_keep(None, keep)
    keep_days = revision_keep_days(None, keep_days)
    if keep == 0:
        every = int(
            catalog.execute(f"SELECT count(*) FROM ducklake_snapshots('{ALIAS}')").fetchone()[0]
        )
        return RetentionResult(every, 0, 0, False)
    plan = plan_retention(catalog, keep=keep, held=held, keep_days=keep_days)
    every = set(plan.snapshots)
    expire = sorted(every - plan.keep)
    freed = _bytes_freed(catalog, plan.keep)
    if not apply:
        return RetentionResult(len(every & plan.keep), len(expire), 0, False, bytes_freed=freed)
    if catalog.borrows_data:
        raise RuntimeError("a copy of a lake's catalog never deletes the lake's files")
    with ExitStack() as locks:
        try:
            locks.enter_context(lake_files(catalog.root, shared=False, timeout=0))
        except LockTimeout:
            return RetentionResult(len(every), 0, 0, False, active_readers=True)
        _write_reads(catalog, plan.reads)
        _expired_count, locked_any = catalog.expire_snapshots(expire)
        remaining = {
            int(row[0])
            for row in catalog.execute(
                f"SELECT snapshot_id FROM ducklake_snapshots('{ALIAS}')"
            ).fetchall()
        }
        files_deleted = catalog.cleanup_old_files(grace_hours=grace_hours)
    return RetentionResult(
        len(remaining),
        len(every - remaining),
        files_deleted,
        True,
        locked_any,
        freed,
    )


def delete_orphans(catalog: Catalog, *, grace_hours: int = FILE_GRACE_HOURS) -> int:
    """Delete data files no snapshot registers, once older than *grace_hours*; returns how many.

    A write places its files in the lake before the transaction that
    registers them, and a rehearsal places them for a copy of the catalog
    only. A process that dies in between leaves them behind, registered
    nowhere. The age is the file's modification time, which a hard-linked
    file shares with its source: a file linked from a generation keeps that
    generation's age. A commit places its files under the lake lock, which
    the caller holds; a rehearsal places them without it, but only files the
    run itself just wrote, younger than any grace. The caller runs this only
    on a lake that has switched to its catalog: before that, the migration
    places generation files without the lock.
    """
    return catalog.delete_orphaned_files(grace_hours=grace_hours)


def catalog_retention(config, *, apply: bool, keep: int | None = None) -> dict | None:
    """Apply revision retention to the lake catalog; ``None`` for a lake without one.

    Keeps, of every dataset, the revisions among its ``keep`` (or
    ``[storage.retention].keep_generations``) newest that were current within
    ``[storage.retention].keep_days``, and every revision a lifecycle hold
    protects (see :func:`plan_retention`).
    """
    from cnequity.storage.lifecycle import LifecycleStore
    from equitylake_core.catalog import CATALOG_FILE

    root = lake_root(config)
    if not (root / CATALOG_FILE).exists():
        return None
    keep = revision_keep(config, keep)
    keep_days = revision_keep_days(config)
    apply = apply and keep > 0
    from cnequity.file_lock import lake_mutation_lock

    orphans = 0
    # Expiring snapshots and deleting files change the lake: the whole of it
    # runs under the lake lock, as a writable catalog connection must.
    lock = lake_mutation_lock(config.meta_root, blocking=True) if apply else nullcontext()
    with lock:
        registry = (
            LifecycleStore(config.meta_root).retention_registry(refresh=apply) if keep else None
        )
        held = set(registry["holds"]) if registry else set()
        with Catalog(
            root,
            read_only=not apply,
            **resources_for(config, "maintenance").options(scratch=root / "tmp"),
        ) as catalog:
            result = apply_retention(
                catalog, keep=keep, held=held, keep_days=keep_days, apply=apply
            )
            switched = RevisionStore(
                config.meta_root, config.curated_root, config.derived_root, create=False
            ).publishes_to_catalog()
            if result.applied and switched:
                with ExitStack() as readers:
                    try:
                        readers.enter_context(lake_files(root, shared=False, timeout=0))
                    except LockTimeout:
                        pass  # A shared reader or rehearsal started after the expiry pass.
                    else:
                        orphans = delete_orphans(catalog)
    return {
        "keep_revisions": keep,
        "keep_days": keep_days,
        "snapshots_kept": result.snapshots_kept,
        "snapshots_expired": result.snapshots_expired,
        "bytes_freed": result.bytes_freed,
        "files_deleted": result.files_deleted,
        "orphans_deleted": orphans,
        "applied": result.applied,
        "active_readers": result.active_readers,
        "file_grace_hours": FILE_GRACE_HOURS,
    }
