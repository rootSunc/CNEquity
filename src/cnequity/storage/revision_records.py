"""A lake's revisions as its catalog records them.

Every revision published into the catalog has a ``_revisions`` row, written in
the snapshot that holds its rows; a revision synced in from a generation, or
one only a receipt file describes, has a ``_revision_history`` row with the
same columns (``RECEIPT_COLUMNS``). The ``_sync_state`` table names each
dataset's current revision. Together they say everything the revision store's
pointer and receipt files say, apart from the receipt's per-file lists.

``RevisionStore`` reads a dataset's revision from here once the lake has had
the ``revision-records`` migration, which imports the receipts a lake kept
before; until then it reads its files, which still agree.

Every read of a dataset resolves its current revision, so the answer is cached
per catalog and kept for as long as the catalog's latest snapshot is the same.
The snapshot number comes from DuckLake's ``current_snapshot()`` on a read-only
connection this module keeps open, which sees every later commit: about
1.4 ms on a real lake. A read resolves a few revisions, so that is well under
1% of it.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from cnequity.meta_layout import LAKE_DIR, REVISION_DATA, REVISION_RECORDS, STORAGE_RECORD
from equitylake_core.catalog import ALIAS, CATALOG_FILE, REVISIONS_TABLE, Catalog

#: The migration that imports the receipts; until a lake has had it, its files are read.
MIGRATION_ID = "revision-records"
#: Lakes built by this release or later never wrote a receipt the catalog lacks.
INTRODUCED_IN = "0.25.0"

_POINTER_SCHEMA_VERSION = 1
STORAGE_CATALOG = "catalog"
HISTORY_TABLE = "_revision_history"
SYNC_TABLE = "_sync_state"

#: What a revision's receipt records. A ``_revisions`` row carries these columns,
#: and so does a history row: a revision synced in from a generation, or one
#: imported from the receipts a lake kept before (``snapshot`` empty when the
#: catalog no longer holds it).
RECEIPT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("revision", "BIGINT"),
    ("run_id", "VARCHAR"),
    ("committed_at", "VARCHAR"),
    ("schema_version", "BIGINT"),
    ("contract_fingerprint", "VARCHAR"),
    ("content_digest", "VARCHAR"),
    ("changed_partitions", "VARCHAR"),
    ("metadata", "VARCHAR"),
)


def receipt_columns(receipt: dict) -> dict[str, object]:
    """The :data:`RECEIPT_COLUMNS` of a receipt (a ``DatasetRevision.to_dict()``)."""
    return {
        "revision": receipt["revision"],
        "run_id": receipt.get("run_id"),
        "committed_at": receipt.get("committed_at"),
        "schema_version": receipt.get("schema_version"),
        "contract_fingerprint": receipt.get("contract_fingerprint"),
        "content_digest": receipt.get("content_digest"),
        "changed_partitions": json.dumps(list(receipt.get("changed_partitions") or ())),
        "metadata": json.dumps(receipt.get("metadata") or {}, sort_keys=True, default=str),
    }


_COLUMNS = ", ".join(name for name, _ in RECEIPT_COLUMNS)


@dataclass(frozen=True)
class RevisionRecord:
    """One revision of a dataset, as the catalog records it."""

    dataset: str
    revision: int
    revision_id: str
    run_id: str | None
    committed_at: str | None
    schema_version: int | None
    contract_fingerprint: str | None
    content_digest: str | None
    changed_partitions: tuple[str, ...]
    metadata: dict[str, Any]
    #: The catalog snapshot that holds it; ``None`` once the catalog let it go.
    snapshot: int | None

    @property
    def generation_path(self) -> str:
        return (REVISION_DATA / self.dataset / self.revision_id).as_posix()

    @property
    def receipt_path(self) -> str | None:
        if self.revision == 0:
            return None
        return (
            REVISION_RECORDS / self.dataset / f"{self.revision:08d}-{self.revision_id}.json"
        ).as_posix()

    def pointer(self) -> dict[str, Any]:
        """What the revision store's ``current.json`` says for this revision."""
        pointer = {
            "schema_version": _POINTER_SCHEMA_VERSION,
            "dataset": self.dataset,
            "revision": self.revision,
            "revision_id": self.revision_id,
            "generation_path": self.generation_path,
            "receipt": self.receipt_path,
            "content_digest": self.content_digest,
            "committed_at": self.committed_at,
        }
        if self.metadata.get("storage") == STORAGE_CATALOG:
            pointer["storage"] = STORAGE_CATALOG
        return pointer


def _record(row) -> RevisionRecord:
    (
        dataset,
        revision_id,
        revision,
        run_id,
        committed,
        schema,
        contract,
        digest,
        parts,
        meta,
        snap,
    ) = row
    return RevisionRecord(
        dataset=dataset,
        revision=int(revision),
        revision_id=revision_id,
        run_id=run_id,
        committed_at=committed,
        schema_version=None if schema is None else int(schema),
        contract_fingerprint=contract,
        content_digest=digest,
        changed_partitions=tuple(json.loads(parts) if parts else ()),
        metadata=json.loads(meta) if meta else {},
        snapshot=None if snap is None else int(snap),
    )


def _all_records_sql(where: str = "") -> str:
    """Every recorded revision, a published one's row first when both tables name it."""
    return (
        "SELECT dataset, revision_id, " + _COLUMNS + ", snapshot FROM ("
        f"SELECT dataset, revision_id, {_COLUMNS}, snapshot_id AS snapshot, 0 AS rank "
        f"FROM {REVISIONS_TABLE} "
        f"UNION ALL SELECT dataset, revision_id, {_COLUMNS}, snapshot, 1 AS rank "
        f"FROM {HISTORY_TABLE} WHERE revision IS NOT NULL"
        f") {where} "
        "QUALIFY row_number() OVER (PARTITION BY dataset, revision_id "
        "ORDER BY rank, snapshot DESC NULLS LAST) = 1"
    )


@dataclass
class CatalogRecords:
    """The revisions of one catalog as of one snapshot."""

    root: Path
    snapshot: int
    #: Each dataset's current revision; a dataset whose record is missing maps to ``None``.
    current: dict[str, RevisionRecord | None]
    _history: dict[str, list[RevisionRecord]] = field(default_factory=dict)
    _history_loaded: bool = False

    def pointer(self, dataset: str) -> dict[str, Any] | None:
        record = self.current.get(dataset)
        return None if record is None else record.pointer()

    def history(self, dataset: str) -> list[RevisionRecord]:
        """Every recorded revision of *dataset*, oldest first."""
        if not self._history_loaded:
            # Every dataset in one query: each filtered query costs a full
            # scan of the union (~0.17 s on a real lake), and the storage
            # inventory asks for every dataset in turn.
            rows = _query(self.root, _all_records_sql() + " ORDER BY dataset, revision")
            for row in rows:
                self._history.setdefault(row[0], []).append(_record(row))
            self._history_loaded = True
        return self._history.get(dataset, [])

    def revision_ids(self) -> set[tuple[str, str]]:
        """``(dataset, revision id)`` of every revision the catalog still holds rows for."""
        rows = _query(
            self.root,
            "SELECT dataset, revision_id FROM (" + _all_records_sql() + ") "
            "WHERE snapshot IS NOT NULL",
        )
        return {(dataset, revision_id) for dataset, revision_id in rows}

    def find(self, dataset: str, revision: int | str) -> RevisionRecord | None:
        """The revision of *dataset* with this number or id."""
        for record in self.history(dataset):
            if (isinstance(revision, int) and record.revision == revision) or (
                isinstance(revision, str) and record.revision_id == revision
            ):
                return record
        return None


_lock = threading.Lock()
_readers: dict[Path, Catalog] = {}
_cache: dict[Path, CatalogRecords] = {}
_settings: dict[Path, tuple[tuple[int, int], bool]] = {}


def _query(root: Path, sql: str, parameters: list | None = None) -> list:
    with _lock:
        for attempt in (0, 1):
            catalog = _readers.get(root)
            if catalog is None:
                catalog = _readers[root] = Catalog(root, read_only=True)
            try:
                return catalog.execute(sql, parameters).fetchall()
            except Exception:
                # A stale connection (the lake was moved or rebuilt): open it once more.
                _readers.pop(root, None)
                catalog.close()
                if attempt:
                    raise
    raise AssertionError("unreachable")


def latest_snapshot(root: Path) -> int | None:
    """The latest snapshot of the catalog at *root*, through DuckLake's public interface."""
    (row,) = _query(root, f"SELECT id FROM {ALIAS}.current_snapshot()")
    return None if row[0] is None else int(row[0])


def _stamp(path: Path) -> int:
    try:
        return path.stat().st_mtime_ns
    except FileNotFoundError:
        return 0


def enabled(meta_root: Path | str) -> bool:
    """Whether the catalog is the record of the revisions of the lake at *meta_root*.

    True for a lake that started on its catalog, and for an earlier one once
    the ``revision-records`` migration imported its receipts. Only such a
    lake is written: a publish records its revision nowhere else.
    """
    return _enabled(Path(meta_root))


def _enabled(meta: Path) -> bool:
    """Whether *meta*'s lake publishes into its catalog and its receipts are all in it."""
    from cnequity.storage.lake_record import lake_file

    storage, lake = meta / STORAGE_RECORD, lake_file(meta)
    stamps = (_stamp(storage), _stamp(lake))
    known = _settings.get(meta)
    if known is not None and known[0] == stamps:
        return known[1]
    try:
        setting = json.loads(storage.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        setting = None
    try:
        record = json.loads(lake.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        record = None
    if not isinstance(setting, dict):
        enabled = False
    else:
        from cnequity.storage.lake_record import _parse

        created = record.get("created_with") if isinstance(record, dict) else None
        done = (record.get("migrations") or {}) if isinstance(record, dict) else {}
        enabled = setting.get("publish") == STORAGE_CATALOG and (
            setting.get("revisions") == STORAGE_CATALOG
            or MIGRATION_ID in done
            or (created is not None and _parse(created) >= _parse(INTRODUCED_IN))
        )
    _settings[meta] = (stamps, enabled)
    return enabled


def _load_current(root: Path) -> dict[str, RevisionRecord | None]:
    """Each dataset's current revision as the catalog records it; ``None`` where it has no record."""
    columns = ", ".join(f"r.{column}" for column in _COLUMNS.split(", "))
    rows = _query(
        root,
        f"SELECT s.dataset, r.revision_id, {columns}, r.snapshot FROM {SYNC_TABLE} s "
        f"LEFT JOIN ({_all_records_sql()}) r "
        "ON r.dataset = s.dataset AND r.revision_id = s.revision_id",
    )
    return {row[0]: (None if row[1] is None else _record(row)) for row in rows}


def records_for(meta_root: Path) -> CatalogRecords | None:
    """The catalog's revisions for the lake whose metadata is *meta_root*; ``None`` to read files.

    The lake keeps its catalog beside ``meta/`` (``<data root>/lake``).
    """
    meta = Path(meta_root)
    root = (meta.parent / LAKE_DIR).resolve()
    catalog_file = root / CATALOG_FILE
    if not catalog_file.is_file() or not _enabled(meta):
        return None
    snapshot = latest_snapshot(root)
    if snapshot is None:
        return None
    with _lock:
        cached = _cache.get(root)
    if cached is not None and cached.snapshot == snapshot:
        return cached
    records = CatalogRecords(root, snapshot, _load_current(root))
    with _lock:
        _cache[root] = records
    return records


def forget(meta_root: Path | None = None) -> None:
    """Drop cached records and close readers (all of them, or *meta_root*'s lake)."""
    with _lock:
        roots = (
            list(_readers) if meta_root is None else [(Path(meta_root).parent / LAKE_DIR).resolve()]
        )
        for root in roots:
            _cache.pop(root, None)
            reader = _readers.pop(root, None)
            if reader is not None:
                reader.close()
        if meta_root is None:
            _cache.clear()
            _settings.clear()


# --- the migration: bring every receipt into the catalog ------------------------------

_RECEIPT_NAME = "*-*.json"


def _published_chain(meta: Path, dataset: str, pointer: dict) -> tuple[list[dict], int]:
    """The receipts of *dataset*'s published revisions, newest first, and how many others it has.

    Walked back from the current pointer along ``base_revision_id`` (or, for a
    receipt written before that link existed, the previous number when it is
    the only one): a receipt off that chain belongs to a publish that stopped.
    """
    by_id: dict[str, dict] = {}
    by_number: dict[int, list[dict]] = {}
    for path in (meta / REVISION_RECORDS / dataset).glob(_RECEIPT_NAME):
        number, _, revision_id = path.stem.partition("-")
        if not number.isdigit():
            continue
        try:
            receipt = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if not isinstance(receipt, dict) or receipt.get("revision_id") != revision_id:
            continue
        by_id[revision_id] = receipt
        by_number.setdefault(int(number), []).append(receipt)
    chain: list[dict] = []
    current = by_id.get(str(pointer.get("revision_id")))
    while current is not None:
        chain.append(current)
        base = (current.get("metadata") or {}).get("base_revision_id")
        if base:
            current = by_id.get(str(base))
        else:
            candidates = by_number.get(int(current["revision"]) - 1, [])
            current = candidates[0] if len(candidates) == 1 else None
    return chain, len(by_id) - len(chain)


def _records_revisions(meta: Path) -> bool:
    """Whether ``storage.json`` says the lake's catalog is the record of its revisions."""
    try:
        setting = json.loads((meta / STORAGE_RECORD).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return isinstance(setting, dict) and setting.get("revisions") == STORAGE_CATALOG


def import_receipts(config, *, apply: bool) -> dict:
    """Preview, or with *apply* perform, the import of every published receipt into the catalog.

    Each published revision the catalog has no record of gets a
    ``_revision_history`` row carrying its receipt; a row the sync from
    generations wrote is completed with it. A revision the catalog still
    holds keeps its snapshot; one it let go gets none. Afterwards every
    dataset's current revision, as the catalog then reads it, must match the
    pointer file, or the migration fails and the files stay the record.
    """
    import polars as pl

    from cnequity.file_lock import lake_mutation_lock
    from cnequity.storage.catalog_bridge import (
        keys_unique,
        lake_root,
        prepare_record_tables,
        synced_revision_ids,
    )
    from cnequity.storage.revisions import RevisionStore

    meta = Path(config.meta_root)
    root = lake_root(config)
    report: dict[str, Any] = {"datasets": 0, "receipts": 0, "unpublished": 0, "rows_changed": 0}
    if not (root / CATALOG_FILE).is_file() or _records_revisions(meta):
        # A lake that already keeps its revisions in its catalog has no newer
        # receipt: its pointer files stopped at the last revision they recorded.
        return report
    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root, create=False)
    datasets = sorted(
        path.parent.name
        for path in (meta / REVISION_RECORDS).glob("*/current.json")
        if path.parent.name != "data"
    )
    from contextlib import nullcontext

    locked = lake_mutation_lock(meta, blocking=True) if apply else nullcontext()
    with locked, Catalog(root, read_only=not apply) as catalog:
        if apply:
            prepare_record_tables(catalog)
        recorded: set[tuple[str, str]] = set()
        history: dict[tuple[str, str], tuple[int | None, bool | None]] = {}
        if REVISIONS_TABLE in catalog.tables():
            recorded |= set(
                catalog.execute(f"SELECT dataset, revision_id FROM {REVISIONS_TABLE}").fetchall()
            )
        if HISTORY_TABLE in catalog.tables():
            filled = "revision" in catalog.column_types(HISTORY_TABLE)
            for dataset, revision_id, snapshot, unique, revision in catalog.execute(
                f"SELECT dataset, revision_id, snapshot, unique_keys, "
                f"{'revision' if filled else 'NULL'} FROM {HISTORY_TABLE}"
            ).fetchall():
                history[(dataset, revision_id)] = (snapshot, unique)
                if revision is not None:
                    recorded.add((dataset, revision_id))
        current = synced_revision_ids(catalog)
        latest = catalog.snapshot()
        rows: list[dict[str, Any]] = []
        for dataset in datasets:
            pointer = store.pointer_file(dataset)
            if pointer is None:
                continue
            report["datasets"] += 1
            if pointer.get("revision") == 0:
                chain = [{"dataset": dataset, "revision": 0, "revision_id": pointer["revision_id"]}]
                unpublished = 0
            else:
                chain, unpublished = _published_chain(meta, dataset, pointer)
            report["unpublished"] += unpublished
            for receipt in chain:
                key = (dataset, str(receipt["revision_id"]))
                if key in recorded:
                    continue
                snapshot, unique = history.get(key, (None, None))
                if snapshot is None and current.get(dataset) == key[1]:
                    snapshot, unique = latest, keys_unique(catalog, dataset)
                rows.append(
                    {
                        "dataset": dataset,
                        "revision_id": key[1],
                        "snapshot": snapshot,
                        "unique_keys": unique,
                        **receipt_columns(receipt),
                    }
                )
        report["receipts"] = report["rows_changed"] = len(rows)
        if not apply or not rows:
            return report
        frame = pl.DataFrame(
            rows,
            schema={
                "dataset": pl.Utf8,
                "revision_id": pl.Utf8,
                "snapshot": pl.Int64,
                "unique_keys": pl.Boolean,
                "revision": pl.Int64,
                "run_id": pl.Utf8,
                "committed_at": pl.Utf8,
                "schema_version": pl.Int64,
                "contract_fingerprint": pl.Utf8,
                "content_digest": pl.Utf8,
                "changed_partitions": pl.Utf8,
                "metadata": pl.Utf8,
            },
        )
        # A row fetched with fetchone leaves its result open; finish it first.
        catalog.con.abort()
        catalog.con.register("imported_receipts", frame)
        with catalog.transaction(message="import revision receipts"):
            catalog.execute(
                f"DELETE FROM {HISTORY_TABLE} h WHERE EXISTS (SELECT 1 FROM imported_receipts i "
                "WHERE i.dataset = h.dataset AND i.revision_id = h.revision_id)"
            )
            catalog.execute(f"INSERT INTO {HISTORY_TABLE} BY NAME SELECT * FROM imported_receipts")
        catalog.con.unregister("imported_receipts")
    forget(meta)
    _check_agreement(config, store, datasets)
    return report


def _check_agreement(config, store, datasets: list[str]) -> None:
    """Fail unless the catalog names the same current revision as every pointer file."""
    from cnequity.storage.catalog_bridge import lake_root

    root = lake_root(config).resolve()
    snapshot = latest_snapshot(root)
    records = CatalogRecords(root, snapshot or 0, _load_current(root))
    differ = []
    for dataset in datasets:
        expected = store.pointer_file(dataset) or {}
        found = records.pointer(dataset) or {}
        if any(
            expected.get(key) != found.get(key)
            for key in ("revision", "revision_id", "generation_path", "receipt", "content_digest")
        ):
            differ.append(dataset)
    if differ:
        raise RuntimeError(
            "the catalog's record of the current revision differs from the revision store for: "
            + ", ".join(differ)
        )
