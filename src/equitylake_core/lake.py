"""A lake of declared datasets: rows go in, partition files come out, one snapshot per write.

A :class:`Lake` is a catalog directory plus the :class:`~equitylake_core.spec.Registry`
of the market it serves. A writer hands it rows (:meth:`LakeWrite.put`,
:meth:`LakeWrite.replace`); on :meth:`LakeWrite.commit` every partition those
rows change is written once, as one file sorted by the dataset's key, and all
of them become visible in one catalog snapshot together with a ``_revisions``
row per dataset.

Layout: ``data/main/<dataset>/<partition label>/part.<8 hex>.parquet``, or
straight under ``data/main/<dataset>/`` for a dataset in one file. A file
belongs to exactly one partition and is never written again; the directory
name is the partition, so a reader needs no catalog internals to prune.

What a write refuses, and what it only records:

* A frame with a column the dataset does not declare, or without a key
  column, is a programming error: :class:`ValueError`, nothing staged.
* A row that breaks a hard rule (a NULL key, a value that is not of the
  column's type, a NaN or infinite number, a declared :class:`Rule`, two
  different rows with one key in the same frame) is left out and counted in
  ``_rejects``; the rest of the frame is stored.
* A write that would change nothing makes no snapshot.

A write also says what it asked a source for and got nothing to store
(:meth:`LakeWrite.empty`). That goes into ``_attempts`` in the same snapshot,
so the planner does not take an empty cell for one nobody asked about.

Only the commit holds the lake lock. Merging runs without it and is
protected by a check under the lock: a dataset another writer published to
since this write looked at it refuses the commit, and the write is run again.
"""

from __future__ import annotations

import re
import shutil
import uuid
from collections.abc import Iterable, Sequence
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

from equitylake_core.catalog import (
    CATALOG_FILE,
    Catalog,
    CatalogError,
    quote_identifier,
    quote_literal,
)
from equitylake_core.catalog import REVISIONS_TABLE as REVISIONS
from equitylake_core.coverage import ATTEMPT_COLUMNS, ATTEMPTS, Cell, cell_keys
from equitylake_core.lock import LAKE_LOCK_FILE, lake_operation, lake_read
from equitylake_core.read import load
from equitylake_core.spec import DatasetSpec, Registry
from equitylake_core.write import Write, WriteRejected, files_digest

#: Rows a write left out, counted by reason, with a few of them as JSON.
REJECTS = "_rejects"
STAGING_DIR = "staging"
#: The lake lock (:func:`equitylake_core.lock.lake_lock`); its writable catalog holds it.
LOCK_FILE = LAKE_LOCK_FILE

_RECORD_TABLES = {
    REVISIONS: (
        "dataset VARCHAR, run VARCHAR, partitions VARCHAR, rows BIGINT, "
        "rows_digest VARCHAR, digest_algorithm VARCHAR, rejected BIGINT"
    ),
    REJECTS: "dataset VARCHAR, run VARCHAR, reason VARCHAR, rows BIGINT, sample VARCHAR",
    ATTEMPTS: ATTEMPT_COLUMNS,
}
_FLOATS = frozenset({"DOUBLE", "FLOAT"})
_SAMPLE_ROWS = 3
_UNSAFE = re.compile(r"[^A-Za-z0-9_.-]+")


@dataclass(frozen=True)
class Commit:
    """What one committed write changed."""

    snapshot: int
    #: Rows each dataset holds in the partitions this write changed.
    rows: dict[str, int]
    partitions: dict[str, tuple[str, ...]]
    #: Rows left out of each dataset, by the reason they were refused.
    rejected: dict[str, dict[str, int]]


@dataclass(frozen=True)
class DatasetSummary:
    dataset: str
    rows: int
    files: int
    #: Smallest and largest value of the time column, as text; ``None`` without one.
    first: str | None
    last: str | None
    #: The snapshot of the dataset's latest revision; ``None`` if it was never written.
    snapshot: int | None


@dataclass
class _Pending:
    table: str
    replace: bool = False
    #: reason -> [rows, samples]
    rejects: dict[str, list[Any]] = field(default_factory=dict)


@dataclass(frozen=True)
class _Change:
    label: str
    add: tuple[Path, ...]
    remove: tuple[str, ...]


def _prepare_records(catalog: Catalog) -> None:
    """Make the record tables and keep their rows in the catalog database.

    Outside a transaction: the catalog cannot set inlining for a table the
    same transaction made.
    """
    for table, columns in _RECORD_TABLES.items():
        if not catalog.has_table(table):
            catalog.execute(f"CREATE TABLE {table} ({columns})")
        catalog.keep_rows_inline(table)


def _head(catalog: Catalog, dataset: str) -> int | None:
    """The snapshot of *dataset*'s latest revision."""
    if not catalog.has_table(REVISIONS):
        return None
    rows = catalog.records(REVISIONS, "dataset, snapshot_id").filter(pl.col("dataset") == dataset)
    return None if rows.is_empty() else int(rows["snapshot_id"].max())


def _canonical(catalog: Catalog, kind: str) -> str:
    """The name the catalog reports for SQL type *kind* (``TEXT`` is ``VARCHAR``)."""
    return str(catalog.execute(f"SELECT typeof(CAST(NULL AS {kind}))").fetchone()[0])


class Lake:
    """The lake at *root*, holding the datasets *registry* declares."""

    def __init__(self, root: str | Path, registry: Registry):
        self.root = Path(root).expanduser().resolve()
        self.registry = registry
        if not (self.root / CATALOG_FILE).exists():
            raise CatalogError(f"no lake at {self.root}")

    @classmethod
    def init(cls, root: str | Path, registry: Registry) -> Lake:
        """Make an empty lake at *root*; a lake already there is left as it is."""
        with Catalog(root, create=True) as catalog:
            _prepare_records(catalog)
        return cls(root, registry)

    def catalog(self, *, read_only: bool = True) -> Catalog:
        return Catalog(self.root, read_only=read_only)

    def write(self, run: str | None = None) -> LakeWrite:
        """A write to this lake; use as a context manager. *run* names it in the records."""
        return LakeWrite(self, run)

    def load(
        self,
        dataset: str,
        *,
        start: date | str | None = None,
        end: date | str | None = None,
        columns: Sequence[str] | None = None,
        snapshot: int | None = None,
        where=None,
    ):
        """The rows of *dataset* as a Polars frame; see :func:`equitylake_core.read.load`."""
        with (
            lake_operation(self.root),
            self.catalog() as catalog,
            catalog.pin_snapshot(snapshot) as version,
        ):
            return load(
                catalog,
                self.registry[dataset],
                start=start,
                end=end,
                columns=columns,
                snapshot=version,
                where=where,
            )

    def snapshot(self) -> int:
        """The version of the whole lake: the number of its latest snapshot."""
        with self.catalog() as catalog:
            return catalog.snapshot()

    def summary(self) -> list[DatasetSummary]:
        """Each declared dataset's size and the span of time it covers."""
        found = []
        with self.catalog() as catalog:
            tables = set(catalog.tables())
            for spec in self.registry:
                stats = catalog.file_stats(spec.name, spec.time) if spec.name in tables else []
                if stats is None:
                    raise CatalogError(f"{spec.name} is not file-complete")
                lows = [item.low for item in stats if item.low is not None]
                highs = [item.high for item in stats if item.high is not None]
                found.append(
                    DatasetSummary(
                        spec.name,
                        sum(item.rows for item in stats),
                        len(stats),
                        min(lows) if lows else None,
                        max(highs) if highs else None,
                        _head(catalog, spec.name),
                    )
                )
        return found

    def revisions(self, dataset: str | None = None) -> list[dict[str, Any]]:
        """The writes that changed *dataset* (every dataset if ``None``), oldest first."""
        with self.catalog() as catalog:
            if not catalog.has_table(REVISIONS):
                return []
            cols = {row[0] for row in catalog.execute(f"DESCRIBE {REVISIONS}").fetchall()}
            scope = "" if dataset is None else "WHERE r.dataset = ?"
            if {"run", "partitions", "rows", "rejected"}.issubset(cols):
                select = (
                    "SELECT r.dataset, r.snapshot_id AS snapshot, "
                    "(s.snapshot_time AT TIME ZONE 'UTC') AS committed_at, "
                    "r.run, r.partitions, r.rows, r.rejected "
                )
            else:
                run_col = "r.run_id AS run" if "run_id" in cols else "NULL AS run"
                part_col = (
                    "r.changed_partitions AS partitions"
                    if "changed_partitions" in cols
                    else "NULL AS partitions"
                )
                rev_col = (
                    ", r.revision_id, r.revision"
                    if {"revision_id", "revision"}.issubset(cols)
                    else ""
                )
                select = (
                    f"SELECT r.dataset, r.snapshot_id AS snapshot, "
                    f"(s.snapshot_time AT TIME ZONE 'UTC') AS committed_at, "
                    f"{run_col}, {part_col}{rev_col} "
                )
            cursor = catalog.execute(
                select
                + f"FROM {REVISIONS} r JOIN ducklake_snapshots('lake') s "
                + f"ON s.snapshot_id = r.snapshot_id {scope} ORDER BY r.snapshot_id, r.dataset",
                [dataset] if dataset is not None else None,
            )
            names = [column[0] for column in cursor.description]
            found = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
        for row in found:
            row["committed_at"] = row["committed_at"].replace(tzinfo=timezone.utc)
        return found


class LakeWrite:
    """Rows on their way into a lake; nothing is visible to readers before :meth:`commit`."""

    def __init__(self, lake: Lake, run: str | None = None):
        self.lake = lake
        self.run = run or f"run-{datetime.now():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
        with ExitStack() as resources:
            resources.enter_context(lake_operation(lake.root))
            self.catalog = resources.enter_context(lake.catalog())
            self._resources = resources.pop_all()
        self._pending: dict[str, _Pending] = {}
        self._empty: list[tuple[str, str, str, str]] = []
        self._types: dict[str, dict[str, str]] = {}
        self._staging = (
            lake.root / STAGING_DIR / f"{_UNSAFE.sub('_', self.run)}.{uuid.uuid4().hex[:8]}"
        )

    def __enter__(self) -> LakeWrite:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        shutil.rmtree(self._staging, ignore_errors=True)
        self._resources.close()

    # -- taking rows -------------------------------------------------------------

    def put(self, dataset: str, frame) -> int:
        """Add *frame*'s rows to *dataset*; a row whose key is stored replaces the stored row.

        Returns the rows accepted. *frame* is a Polars frame or an Arrow table.
        """
        return self._take(dataset, frame, replace=False)

    def replace(self, dataset: str, frame) -> int:
        """Make *frame*'s rows all of *dataset*: stored rows it does not hold are removed."""
        return self._take(dataset, frame, replace=True)

    def empty(self, dataset: str, cells: Iterable[str | tuple[str, str] | Cell]) -> None:
        """Record that *cells* of *dataset* were asked for and there was nothing to store.

        A cell is a period label, or a period and an entity. For a dataset
        covered by write, any one period says the refresh found nothing new.
        """
        self._answer(dataset, cells, "empty")

    def partial(self, dataset: str, cells: Iterable[str | tuple[str, str] | Cell]) -> None:
        """Record that *cells* were answered only in part: they stay owed though they hold rows."""
        self._answer(dataset, cells, "partial")

    def complete(self, dataset: str, cells: Iterable[str | tuple[str, str] | Cell]) -> None:
        """Record that *cells* were answered whole, settling an earlier :meth:`partial`."""
        self._answer(dataset, cells, "complete")

    def _answer(self, dataset: str, cells, outcome: str) -> None:
        self.lake.registry[dataset]  # an unknown dataset is a programming error
        self._empty.extend(
            (dataset, period, entity, outcome) for period, entity in cell_keys(dataset, cells)
        )

    def _spec_types(self, spec: DatasetSpec) -> dict[str, str]:
        if spec.name not in self._types:
            self._types[spec.name] = {
                name: _canonical(self.catalog, kind) for name, kind in spec.columns.items()
            }
        return self._types[spec.name]

    def _take(self, dataset: str, frame, *, replace: bool) -> int:
        spec = self.lake.registry[dataset]
        arrow = frame.to_arrow() if hasattr(frame, "to_arrow") else frame
        present = set(arrow.column_names)
        unknown = sorted(present - set(spec.columns))
        if unknown:
            raise ValueError(f"{dataset}: the frame has undeclared column(s) {', '.join(unknown)}")
        missing = [column for column in spec.key if column not in present]
        if missing:
            raise ValueError(f"{dataset}: the frame lacks key column(s) {', '.join(missing)}")
        con, q = self.catalog.con, quote_identifier
        pending = self._pending.get(dataset)
        if pending is None or replace:
            pending = _Pending(f'temp.main."_el_pending_{dataset}"', replace)
            first = True
        else:
            first = False
        # A row taken with ``fetchone`` leaves its result open, and the connection
        # refuses to register or unregister anything until that result is dropped.
        con.abort()
        con.register("_el_incoming", arrow)
        try:
            self._conform(spec, present)
            columns = ", ".join(q(column) for column in spec.columns)
            con.execute(
                'CREATE OR REPLACE TEMP TABLE temp.main."_el_good" AS '
                f'SELECT DISTINCT {columns} FROM temp.main."_el_conformed" WHERE _reason IS NULL'
            )
            keys = ", ".join(q(column) for column in spec.key)
            same_key = " AND ".join(f"g.{q(column)} = d.{q(column)}" for column in spec.key)
            con.execute(
                'CREATE OR REPLACE TEMP TABLE temp.main."_el_dups" AS '
                f'SELECT {keys} FROM temp.main."_el_good" GROUP BY {keys} HAVING count(*) > 1'
            )
            self._note_rejects(spec, pending, same_key)
            con.execute(
                f'DELETE FROM temp.main."_el_good" g USING temp.main."_el_dups" d WHERE {same_key}'
            )
            accepted = con.execute('SELECT count(*) FROM temp.main."_el_good"').fetchone()[0]
            if first:
                con.execute(
                    f'CREATE OR REPLACE TEMP TABLE {pending.table} AS SELECT * FROM temp.main."_el_good"'
                )
            else:
                con.execute(
                    f'DELETE FROM {pending.table} g USING temp.main."_el_good" d WHERE {same_key}'
                )
                con.execute(f'INSERT INTO {pending.table} SELECT * FROM temp.main."_el_good"')
        finally:
            con.abort()
            con.unregister("_el_incoming")
            for name in ("_el_conformed", "_el_good", "_el_dups"):
                con.execute(f'DROP TABLE IF EXISTS temp.main."{name}"')
        self._pending[dataset] = pending
        return int(accepted)

    def _conform(self, spec: DatasetSpec, present: set[str]) -> None:
        """Cast the incoming rows to the declared types and give each refused row its reason."""
        q = quote_identifier
        types = self._spec_types(spec)
        casts, bad = [], []
        for name, kind in spec.columns.items():
            if name in present:
                casts.append(f"TRY_CAST({q(name)} AS {kind}) AS {q(name)}")
                bad.append(
                    f"WHEN {q(name)} IS NOT NULL AND TRY_CAST({q(name)} AS {kind}) IS NULL "
                    f"THEN {quote_literal('bad_type:' + name)}"
                )
            else:
                casts.append(f"CAST(NULL AS {kind}) AS {q(name)}")
        reasons = ["WHEN _bad IS NOT NULL THEN _bad"]
        reasons.append(
            "WHEN " + " OR ".join(f"{q(column)} IS NULL" for column in spec.key) + " "
            "THEN 'missing_key'"
        )
        for name in spec.columns:
            if types[name] in _FLOATS:
                reasons.append(
                    f"WHEN isnan({q(name)}) OR isinf({q(name)}) "
                    f"THEN {quote_literal('not_finite:' + name)}"
                )
        for rule in spec.rules:
            reasons.append(f"WHEN ({rule.ok}) IS FALSE THEN {quote_literal('rule:' + rule.name)}")
        self.catalog.con.execute(
            'CREATE OR REPLACE TEMP TABLE temp.main."_el_conformed" AS '
            f"SELECT * EXCLUDE (_bad), CASE {' '.join(reasons)} END AS _reason FROM ("
            f"SELECT {', '.join(casts)}, CASE {' '.join(bad)} END AS _bad FROM _el_incoming)"
        )

    def _note_rejects(self, spec: DatasetSpec, pending: _Pending, same_key: str) -> None:
        con = self.catalog.con
        columns = ", ".join(quote_identifier(column) for column in spec.columns)
        found = con.execute(
            'SELECT _reason, count(*) FROM temp.main."_el_conformed" '
            "WHERE _reason IS NOT NULL GROUP BY 1 ORDER BY 1"
        ).fetchall()
        sources = {
            reason: f'SELECT {columns} FROM temp.main."_el_conformed" WHERE _reason = '
            f"{quote_literal(reason)}"
            for reason, _ in found
        }
        counts = dict(found)
        duplicated = con.execute(
            'SELECT count(*) FROM temp.main."_el_good" g '
            f'WHERE EXISTS (SELECT 1 FROM temp.main."_el_dups" d WHERE {same_key})'
        ).fetchone()[0]
        if duplicated:
            counts["duplicate_key"] = duplicated
            sources["duplicate_key"] = (
                f'SELECT {columns} FROM temp.main."_el_good" g '
                f'WHERE EXISTS (SELECT 1 FROM temp.main."_el_dups" d WHERE {same_key})'
            )
        for reason, rows in counts.items():
            entry = pending.rejects.setdefault(reason, [0, []])
            entry[0] += int(rows)
            room = _SAMPLE_ROWS - len(entry[1])
            if room > 0:
                entry[1].extend(
                    row[0]
                    for row in con.execute(
                        f"SELECT to_json(r)::VARCHAR FROM ({sources[reason]} LIMIT {room}) r"
                    ).fetchall()
                )

    # -- committing --------------------------------------------------------------

    def _existing(self, spec: DatasetSpec) -> dict[str, list[str]]:
        """The dataset's data files by partition label, read off their directories."""
        if not self.catalog.has_table(spec.name):
            return {}
        paths = self.catalog.data_files(spec.name)
        if paths is None:
            raise CatalogError(f"{spec.name} is not file-complete; it cannot be written by files")
        home = self.catalog.table_dir(spec.name)
        found: dict[str, list[str]] = {}
        for path in paths:
            parent = Path(path).parent
            label = "" if parent == home else parent.name
            laid_out = parent == home or parent.parent == home
            if spec.partition == "none":
                laid_out = laid_out and label == ""
            else:
                laid_out = laid_out and spec.period(label) is not None
            if not laid_out:
                raise CatalogError(
                    f"{spec.name}: {path} is not laid out as a partition file of the dataset"
                )
            found.setdefault(label, []).append(path)
        return found

    def _stored(self, spec: DatasetSpec, files: Sequence[str]) -> str:
        """SQL: the rows of *files* with the declared columns and types."""
        q = quote_identifier
        listed = ", ".join(quote_literal(path) for path in files)
        scan = f"read_parquet([{listed}], union_by_name => true, hive_partitioning => false)"
        present = {
            row[0] for row in self.catalog.execute(f"DESCRIBE SELECT * FROM {scan}").fetchall()
        }
        select = ", ".join(
            f"CAST({q(name) if name in present else 'NULL'} AS {kind}) AS {q(name)}"
            for name, kind in spec.columns.items()
        )
        return f"(SELECT {select} FROM {scan})"

    def _stage(self, spec: DatasetSpec, pending: _Pending) -> list[_Change]:
        """Write the new file of every partition the pending rows change."""
        con, q = self.catalog.con, quote_identifier
        existing = self._existing(spec)
        columns = ", ".join(q(column) for column in spec.columns)
        labels = {
            row[0]
            for row in con.execute(
                f"SELECT DISTINCT {spec.label_sql()} FROM {pending.table}"
            ).fetchall()
        }
        if pending.replace:
            labels |= set(existing)
        same_key = " AND ".join(f"s.{q(column)} = i.{q(column)}" for column in spec.key)
        order = ", ".join(q(column) for column in spec.sort)
        changes = []
        for label in sorted(labels):
            old = existing.get(label, [])
            incoming = (
                f"(SELECT {columns} FROM {pending.table} "
                f"WHERE {spec.label_sql()} = {quote_literal(label)})"
            )
            merged = incoming
            if old:
                stored = self._stored(spec, old)
                adds = con.execute(
                    f"SELECT 1 FROM (SELECT * FROM {incoming} EXCEPT SELECT * FROM {stored}) LIMIT 1"
                ).fetchone()
                drops = (
                    pending.replace
                    and con.execute(
                        f"SELECT 1 FROM (SELECT * FROM {stored} EXCEPT SELECT * FROM {incoming}) LIMIT 1"
                    ).fetchone()
                )
                if not adds and not drops:
                    continue
                if not pending.replace:
                    merged = (
                        f"(SELECT * FROM {stored} s WHERE NOT EXISTS "
                        f"(SELECT 1 FROM {incoming} i WHERE {same_key}) "
                        f"UNION ALL SELECT * FROM {incoming})"
                    )
            rows = con.execute(f"SELECT count(*) FROM {merged}").fetchone()[0]
            add: tuple[Path, ...] = ()
            if rows:
                target = self._staging / spec.name / (label or "_") / "part.parquet"
                target.parent.mkdir(parents=True, exist_ok=True)
                con.execute(
                    f"COPY (SELECT * FROM {merged} ORDER BY {order}) TO "
                    f"{quote_literal(target.as_posix())} (FORMAT parquet, COMPRESSION zstd)"
                )
                add = (target,)
            elif not old:
                continue
            changes.append(_Change(label, add, tuple(old)))
        return changes

    def _check_columns(self, catalog: Catalog, spec: DatasetSpec) -> None:
        """Refuse a spec whose type for a stored column is not the stored one.

        A column the spec gained is added by the write; a changed type needs
        the stored rows rewritten, which is a migration, not a write.
        """
        if not catalog.has_table(spec.name):
            return
        have = catalog.column_types(spec.name)
        for name, kind in self._spec_types(spec).items():
            if name in have and have[name] != kind:
                raise WriteRejected(
                    f"{spec.name}: column {name} is {have[name]} in the lake and {kind} in the spec"
                )

    @staticmethod
    def _key_check(spec: DatasetSpec):
        keys = ", ".join(quote_identifier(column) for column in spec.key)

        def check(catalog: Catalog) -> str | None:
            repeated = catalog.execute(
                f"SELECT count(*) FROM (SELECT {keys} FROM {quote_identifier(spec.name)} "
                f"GROUP BY {keys} HAVING count(*) > 1)"
            ).fetchone()[0]
            return (
                f"{repeated} key(s) would be stored in more than one partition"
                if repeated
                else None
            )

        return check

    def commit(self, *, message: str | None = None) -> Commit | None:
        """Publish everything taken so far as one snapshot; ``None`` if it changes nothing."""
        catalog, registry = self.catalog, self.lake.registry
        try:
            with lake_read(self.lake.root):
                bases = {dataset: _head(catalog, dataset) for dataset in self._pending}
                plans = {
                    dataset: changes
                    for dataset, pending in self._pending.items()
                    if (changes := self._stage(registry[dataset], pending))
                }
            rejected = {
                dataset: {reason: entry[0] for reason, entry in pending.rejects.items()}
                for dataset, pending in self._pending.items()
                if pending.rejects
            }
            if not plans and not rejected and not self._empty:
                return None
            rows: dict[str, int] = {}
            with self.lake.catalog(read_only=False) as catalog, Write(catalog) as write:
                _prepare_records(catalog)
                for dataset in plans:
                    if _head(catalog, dataset) != bases[dataset]:
                        raise WriteRejected(
                            f"{dataset} was published to by another writer; run this write again"
                        )
                for dataset in plans:
                    self._check_columns(catalog, registry[dataset])
                for dataset, changes in plans.items():
                    spec = registry[dataset]
                    write.create(dataset, spec.columns)
                    add = []
                    for change in changes:
                        home = catalog.table_dir(dataset)
                        for source in change.add:
                            target = (
                                home / change.label if change.label else home
                            ) / f"part.{uuid.uuid4().hex[:8]}.parquet"
                            write.place(source, target)
                            add.append(target)
                    remove = [path for change in changes for path in change.remove]
                    write.swap(dataset, add=add, remove=remove)
                    if spec.key_spans_partitions:
                        write.check(f"{dataset} key", self._key_check(spec))
                    write.record(
                        REVISIONS,
                        self._revision(dataset, changes, add, rejected.get(dataset, {}), rows),
                    )
                for dataset, reasons in rejected.items():
                    for reason, count in reasons.items():
                        samples = self._pending[dataset].rejects[reason][1]
                        write.record(
                            REJECTS,
                            {
                                "dataset": dataset,
                                "run": self.run,
                                "reason": reason,
                                "rows": count,
                                "sample": "[" + ", ".join(samples) + "]",
                            },
                        )
                moment = datetime.now(timezone.utc)
                for dataset, period, entity, outcome in dict.fromkeys(self._empty):
                    write.record(
                        ATTEMPTS,
                        {
                            "dataset": dataset,
                            "period": period,
                            "entity": entity,
                            "run": self.run,
                            "asked_at": moment,
                            "outcome": outcome,
                        },
                    )
                result = write.commit(message=message or self.run)
        finally:
            for pending in self._pending.values():
                self.catalog.con.execute(f"DROP TABLE IF EXISTS {pending.table}")
            self._pending.clear()
            self._empty.clear()
            shutil.rmtree(self._staging, ignore_errors=True)
        return Commit(
            result.snapshot,
            rows,
            {
                dataset: tuple(change.label for change in changes)
                for dataset, changes in plans.items()
            },
            rejected,
        )

    def _revision(
        self,
        dataset: str,
        changes: list[_Change],
        files: list[Path],
        rejected: dict[str, int],
        rows: dict[str, int],
    ):
        """The ``_revisions`` row of this write for *dataset*, built inside the transaction."""

        def row(catalog: Catalog) -> dict[str, object]:
            digest = files_digest(catalog, dataset, files)
            rows[dataset] = digest.rows
            return {
                "dataset": dataset,
                "run": self.run,
                "partitions": ",".join(change.label for change in changes),
                "rows": digest.rows,
                "rows_digest": digest.digest,
                "digest_algorithm": digest.algorithm,
                "rejected": sum(rejected.values()),
            }

        return row
