"""Writes to the lake catalog: check out the partitions a run changes, then publish them.

A run never edits published data in place. It checks out each partition it is
about to change into a folder of its own (``staging/_checkout/<run_id>``),
filled from the catalog's rows and laid out the way the writers lay out a
partition, so the existing merge and dedupe code works on it unchanged.
Publishing replaces exactly the partitions that changed, in one catalog
transaction for every dataset of the write, with the quality checks and the
revision records inside it: readers see all of it or none of it.

A run that dies leaves only its checkout folder; the catalog is untouched. A
dataset whose catalog revision moved after its checkout is refused, so two
writers can never silently overwrite each other's partition.

Only the commit holds the lake lock. Reading, merging and the publication
gate run without it, so a long repair or derivation never keeps another
run waiting; what they computed is protected instead by the checks the
commit makes under the lock: the checkout's base, the inputs a writer names
(:meth:`LakeWrite.expect`) and the revisions its :func:`planned_on` scope
read. A refused write publishes nothing and is run again.

During the move to the catalog the revision store still keeps a receipt and a
pointer per revision (marked ``storage: catalog``, with no generation), so
every reader keyed on them keeps working.
"""

from __future__ import annotations

import contextlib
import contextvars
import os
import re
import shutil
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from cnequity.config.resources import resources_for
from cnequity.domain.datasets import DATASETS
from cnequity.file_lock import lake_mutation_lock
from cnequity.meta_layout import LAKE_DIR
from cnequity.storage.catalog_bridge import (
    _ensure_sync_table,
    _partition_key_column,
    _partition_predicate,
    _record_sync,
    _table_keys_unique,
    files_have_duplicate_keys,
    keys_unique,
    lake_root,
    prepare_record_tables,
    synced_revision_id,
    synced_revision_ids,
)
from cnequity.storage.locator import layer_name
from cnequity.storage.parquet import emit_partition, merge_group_keys
from cnequity.storage.revisions import DatasetRevision, RevisionFile, RevisionStore, sha256_file
from equitylake_core.catalog import (
    ALIAS,
    CATALOG_FILE,
    REVISIONS_TABLE,
    Catalog,
    quote_identifier,
    quote_literal,
)
from equitylake_core.lock import lake_read
from equitylake_core.write import (
    Write,
    WriteRejected,
    digest_algorithm,
    files_digest,
    rows_digest,
)

CHECKOUT_DIR = "_checkout"

Stamps = dict[str, tuple[int, int]]


def _stamps(directory: Path) -> Stamps:
    if not directory.is_dir():
        return {}
    return {
        path.relative_to(directory).as_posix(): (path.stat().st_size, path.stat().st_mtime_ns)
        for path in directory.rglob("*.parquet")
    }


#: A data file this module placed in the lake: ``<logical stem>.<8 hex>.parquet``.
_LAID_OUT = re.compile(r"^(?P<stem>.+)\.[0-9a-f]{8}\.parquet$")


def lake_file_name(logical: str) -> str:
    """A fresh name in the lake for the partition file *logical* (``part-merged.parquet``)."""
    return f"{Path(logical).stem}.{uuid.uuid4().hex[:8]}.parquet"


def logical_name(path: str | Path) -> str:
    """The partition file a lake data file is (its name without the random part)."""
    found = _LAID_OUT.match(Path(path).name)
    return f"{found['stem']}.parquet" if found else Path(path).name


def _place(source: Path, target: Path) -> None:
    """Make *target* the same file as *source*: a hard link, or a copy across volumes."""
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.link(source, target)
    except OSError:
        shutil.copy2(source, target)


def _protect(path: Path) -> None:
    """A published file is never written again; make an attempt fail instead of corrupt it."""
    if os.name != "nt":  # a read-only file cannot be deleted on Windows, and the catalog must
        path.chmod(0o444)


class InputChanged(WriteRejected):
    """A dataset the write read, or publishes, was published by another write meanwhile."""

    def __init__(self, dataset: str, since: str = "this one read it"):
        super().__init__(f"{dataset} was published by another write since {since}; run it again")
        self.dataset = dataset


class PlanScope:
    """The catalog revision of every dataset when a writer started reading.

    A writer that plans from the published rows (a repair, a derivation)
    does not hold the lake lock while it reads, merges and runs the gate;
    only its commit does. Each :class:`LakeWrite` opened inside the scope
    then refuses to publish a dataset another write has published since the
    scope opened: what it computed may rest on rows that are gone. Its own
    commits move the scope along, so a writer may publish a dataset twice.
    """

    def __init__(self, revisions: dict[str, str]):
        self.revisions = revisions

    def check(self, catalog: Catalog, datasets) -> None:
        for dataset in datasets:
            if _catalog_revision(catalog, dataset) != self.revisions.get(dataset):
                raise InputChanged(dataset)


_SCOPE: contextvars.ContextVar[PlanScope | None] = contextvars.ContextVar(
    "plan_scope", default=None
)


@contextlib.contextmanager
def planned_on(config) -> Iterator[PlanScope | None]:
    """Pin what the block reads to the lake's current revisions; see :class:`PlanScope`.

    Use it where the block starts reading. Nested, the outer scope holds. A
    lake without a catalog yet has nothing to pin, and gets ``None``.
    """
    outer = _SCOPE.get()
    if outer is not None:
        yield outer
        return
    root = lake_root(config)
    if not (root / CATALOG_FILE).exists():
        yield None
        return
    with Catalog(root, read_only=True) as catalog:
        scope = PlanScope(synced_revision_ids(catalog))
    token = _SCOPE.set(scope)
    try:
        yield scope
    finally:
        _SCOPE.reset(token)


@dataclass(frozen=True)
class PartitionSwap:
    """What publishing one changed partition does to the dataset's files."""

    label: str
    #: Files in the checkout to register, by their name there.
    add: tuple[Path, ...]
    #: Data files of the lake this write was checked out of, dropped whole.
    remove: tuple[str, ...]
    #: Data files of that lake the partition keeps as they are.
    keep: tuple[str, ...] = ()


class Checkout:
    """The partitions of one dataset that a write changes, checked out of the catalog.

    The lake keeps a dataset as the files its writers produce: every data file
    holds rows of one partition, under ``<table>/<partition value>/``. Checking
    a partition out links those files into the run's folder under the names
    the writers know (``part-merged.parquet``, buckets), so nothing is copied
    and the merge code works on them as it does on a partition directory:
    it rewrites only the files it must. Publishing then registers the new and
    rewritten files and drops the ones they replace.

    A partition whose rows sit in files not laid out that way (written by the
    catalog itself, possibly holding several partitions) is checked out by
    its rows instead. If it changes, every partition sharing those files is
    written out with it, so each file is split once, the first time it is
    touched.
    """

    def __init__(self, write, dataset: str, *, root: Path | None = None, link: bool = True):
        self.dataset = dataset
        self.spec = DATASETS[dataset]
        #: Pass this where a writer expects ``curated_root`` or ``derived_root``.
        self.layer_root = write.root / layer_name(dataset) if root is None else root.parent
        self.root = self.layer_root / dataset if root is None else root
        self._write = write
        self._link = link
        self._checked: dict[str, Stamps] = {}
        self._removed: set[str] = set()
        # The lake's files that held each touched partition when it was first touched.
        self._files: dict[str, list[str]] = {}
        # Partitions checked out as links: the lake file behind each name.
        self._linked: dict[str, dict[str, str]] = {}
        # Untouched partitions written out because they share a file with a changed one.
        self._split: set[str] = set()
        self._types: dict[str, str] | None = None
        # False: not looked at yet.
        self._folders: dict[str, list[str]] | None | bool = False
        self.base_revision = _catalog_revision(write.catalog, dataset)

    def _target(self, label: str) -> Path:
        if bool(label) != bool(self.spec.partition_col):
            raise ValueError(f"{self.dataset}: partition label {label!r} does not fit its layout")
        return self.root / label if label else self.root

    def layout_dir(self, label: str, catalog: Catalog | None = None) -> Path:
        """Where the lake keeps the files of partition *label*."""
        table = (catalog or self._write.catalog).table_dir(self.dataset)
        return table / label.partition("=")[2] if label else table

    def _laid_out(self, path: str, label: str) -> bool:
        found = Path(path)
        return found.parent == self.layout_dir(label) and bool(_LAID_OUT.match(found.name))

    def _by_folder(self) -> dict[str, list[str]] | None:
        """The lake's files by partition folder, or ``None`` if some file is not in one.

        When every data file sits in the folder of exactly one partition, the
        files of a partition are the files of its folder, and nothing has to
        be asked of the rows.
        """
        if self._folders is not False:
            return self._folders
        from cnequity.domain.datasets import granularity_for_dataset
        from cnequity.domain.partitions import parse_partition, partition_value

        catalog = self._write.catalog
        table = catalog.table_dir(self.dataset)
        granularity = granularity_for_dataset(self.dataset)
        folders: dict[str, list[str]] | None = {}
        for (path,) in catalog.execute(
            f"SELECT data_file FROM ducklake_list_files('{ALIAS}', ?)", [self.dataset]
        ).fetchall():
            found = Path(path)
            parts = found.relative_to(table).parts if table in found.parents else ()
            folder = parts[0] if len(parts) == 2 else "" if len(parts) == 1 else None
            fits = folder is not None and bool(_LAID_OUT.match(found.name))
            if fits and self.spec.partition_col:
                period = parse_partition(folder) if folder else None
                fits = period is not None and partition_value(period.start, granularity) == folder
            elif fits:
                fits = folder == ""
            if not fits:
                folders = None
                break
            folders.setdefault(folder, []).append(path)
        self._folders = folders
        return folders

    def _files_of(self, label: str) -> list[str]:
        """The lake's data files that hold rows of *label* now."""
        catalog = self._write.catalog
        if not catalog.has_table(self.dataset):
            return []
        folders = self._by_folder()
        if folders is not None:
            return sorted(folders.get(label.partition("=")[2] if label else "", []))
        where = self.where(label)
        scope = f" WHERE {where}" if where else ""
        return sorted(
            row[0]
            for row in catalog.execute(
                f"SELECT DISTINCT filename FROM {quote_identifier(self.dataset)}{scope}"
            ).fetchall()
        )

    def _touch(self, label: str) -> list[str]:
        if label not in self._files:
            self._files[label] = self._files_of(label)
        return self._files[label]

    def _write_out(self, label: str) -> None:
        """Fill *label*'s folder from its published rows, as the writers lay a partition out."""
        target = self._target(label)
        target.mkdir(parents=True, exist_ok=True)
        rows = self._published_rows(label)
        if rows.height:
            emit_partition(
                rows, self.dataset, affected=[], existing=[], read_dir=None, out_dir=target
            )

    def partition(self, label: str = "") -> Path:
        """Check out ``<column>=<value>`` (``""`` for an unpartitioned dataset) and return it.

        It holds the partition's published files under the writers' names;
        checking it out again returns it as it is.
        """
        target = self._target(label)
        if label in self._checked:
            return target
        files = self._touch(label)
        if files and all(self._laid_out(path, label) for path in files):
            target.mkdir(parents=True, exist_ok=True)
            linked = {}
            for path in files:
                name = logical_name(path)
                if name in linked:
                    raise WriteRejected(
                        f"{self.dataset}: {label or 'the table'} holds two files named {name}"
                    )
                # Windows cannot protect a linked file from being written in place.
                if self._link and os.name != "nt":
                    _place(Path(path), target / name)
                else:
                    shutil.copy2(path, target / name)
                    (target / name).chmod(0o644)
                linked[name] = path
            self._linked[label] = linked
        else:
            self._write_out(label)
        self._checked[label] = _stamps(target)
        return target

    def labels(self) -> list[str]:
        """Every published partition of the dataset (``[""]`` when it has none)."""
        column = self.spec.partition_col
        if not column:
            return [""]
        catalog = self._write.catalog
        if not catalog.has_table(self.dataset):
            return []
        return self._labels_where("")

    def _labels_where(self, scope: str) -> list[str]:
        from cnequity.domain.datasets import granularity_for_dataset
        from cnequity.storage.parquet import _partition_values

        column = self.spec.partition_col
        values = self._write.catalog.relation(
            f"SELECT DISTINCT {quote_identifier(column)} "
            f"FROM {quote_identifier(self.dataset)}{scope}"
        ).pl()
        return sorted(
            {
                f"{column}={value}"
                for value in _partition_values(
                    values, column, granularity_for_dataset(self.dataset)
                )
                .unique()
                .to_list()
            }
        )

    def rows(self, label: str = "") -> pl.DataFrame:
        """The published rows of partition *label*, read without checking it out."""
        return self._published_rows(label)

    def rewrite(self, label: str, rows: pl.DataFrame) -> None:
        """Make *rows* the whole of partition *label*; no rows removes it.

        For a writer that read the partition through :meth:`rows` and decided
        its new content: the partition is not written to disk first.
        """
        target = self._target(label)
        self._touch(label)
        self._linked.pop(label, None)
        if target.exists():
            shutil.rmtree(target)
        self._checked.setdefault(label, {})
        self._removed.discard(label)
        if rows.is_empty():
            self._removed.add(label)
            return
        target.mkdir(parents=True)
        emit_partition(rows, self.dataset, affected=[], existing=[], read_dir=None, out_dir=target)

    def replace_with(self, label: str, files: list[Path]) -> None:
        """Make *files* (a partition's files from elsewhere) the whole of *label*; none removes it."""
        target = self._target(label)
        self._touch(label)
        self._linked.pop(label, None)
        if target.exists():
            shutil.rmtree(target)
        self._checked[label] = {}
        self._removed.discard(label)
        if not files:
            self._removed.add(label)
            return
        target.mkdir(parents=True)
        for path in files:
            # A copy: the source is a tree its writer may still change.
            shutil.copy2(path, target / Path(path).name)

    def remove(self, label: str) -> None:
        """Delete the partition *label* from the dataset."""
        target = self.partition(label)
        shutil.rmtree(target)
        self._removed.add(label)

    def changed(self) -> dict[str, list[Path]]:
        """Each partition this write changes, with every file it now holds (none: removed).

        That includes a partition nobody edited but which shares a lake file
        with an edited one: the file is dropped, so its rows are written out.
        """
        if self.spec.partition_col and self.root.is_dir():
            strays = sorted(
                path.name
                for path in self.root.iterdir()
                if path.is_dir() and path.name not in self._checked
            )
            if strays:
                # A writer wrote a partition nobody checked out: publishing
                # it would replace published rows the writer never read.
                raise WriteRejected(
                    f"{self.dataset}: partitions written without checkout: {strays}"
                )
        out: dict[str, list[Path]] = {}
        for label, before in self._checked.items():
            directory = self.root / label if label else self.root
            if label in self._removed and not directory.exists():
                out[label] = []
                continue
            after = _stamps(directory)
            if after != before or label in self._split:
                out[label] = [directory / relative for relative in sorted(after)]
        self._split_shared(out)
        return out

    def _split_shared(self, out: dict[str, list[Path]]) -> None:
        """Write out the partitions that share a not-laid-out file with a changed one."""
        if not self.spec.partition_col:
            return
        pending = list(out)
        while pending:
            label = pending.pop()
            shared = [
                path for path in self._files.get(label, []) if not self._laid_out(path, label)
            ]
            if not shared:
                continue
            listed = ", ".join(quote_literal(path) for path in shared)
            for other in self._labels_where(f" WHERE filename IN ({listed})"):
                if other in self._checked:
                    continue
                self._touch(other)
                self._write_out(other)
                self._checked[other] = {}
                self._split.add(other)
                directory = self.root / other
                out[other] = [directory / name for name in sorted(_stamps(directory))]
                pending.append(other)

    def swaps(self) -> list[PartitionSwap]:
        """For each changed partition, the files to register and the lake files to drop."""
        swaps = []
        for label, files in self.changed().items():
            held = self._files.get(label, [])
            linked = self._linked.get(label)
            if linked is None:
                swaps.append(PartitionSwap(label, tuple(files), tuple(held)))
                continue
            directory = self.root / label if label else self.root
            before, after = self._checked[label], _stamps(directory)
            add = [directory / name for name in sorted(after) if before.get(name) != after[name]]
            remove = [
                path
                for name, path in sorted(linked.items())
                if name not in after or before.get(name) != after[name]
            ]
            keep = [path for path in sorted(linked.values()) if path not in remove]
            swaps.append(PartitionSwap(label, tuple(add), tuple(remove), tuple(keep)))
        return swaps

    def has_foreign_files(self) -> bool:
        """Whether a touched partition's rows sit in files the catalog laid out itself."""
        return any(
            not self._laid_out(path, label)
            for label, files in self._files.items()
            for path in files
        )

    def where(self, label: str) -> str | None:
        """The SQL predicate selecting *label*'s rows; ``None`` for the whole table."""
        if not label:
            return None
        column, _, value = label.partition("=")
        if column != self.spec.partition_col:
            raise ValueError(
                f"{self.dataset}: {label} is not a {self.spec.partition_col} partition"
            )
        types = self._column_types()
        predicate = _partition_predicate(
            column,
            types.get(column) or _declared_type(self.dataset, column),
            self.spec.partition_granularity,
            value,
        )
        if predicate is None:
            raise ValueError(f"{self.dataset}: no row predicate for partition {label}")
        return predicate

    def _column_types(self) -> dict[str, str]:
        if self._types is None:
            catalog = self._write.catalog
            self._types = (
                {
                    row[0]: row[1]
                    for row in catalog.execute(
                        f"DESCRIBE {quote_identifier(self.dataset)}"
                    ).fetchall()
                }
                if catalog.has_table(self.dataset)
                else {}
            )
        return self._types

    def _published_rows(self, label: str) -> pl.DataFrame:
        catalog = self._write.catalog
        if not catalog.has_table(self.dataset):
            return pl.DataFrame()
        where = self.where(label)
        query = f"SELECT * FROM {quote_identifier(self.dataset)}"
        if where:
            query += f" WHERE {where}"
        keys = merge_group_keys(self.dataset)
        if keys:
            query += " ORDER BY " + ", ".join(quote_identifier(key) for key in keys)
        return catalog.relation(query).pl()


@dataclass(frozen=True)
class DatasetPublish:
    """What a write publishes for one dataset."""

    schema_version: int
    contract_fingerprint: str
    metadata: dict | None = None
    #: A revision another lake made, republished under its own number and id.
    identity: dict | None = None


class LakeWrite:
    """One run's write to the lake: checkouts now, one catalog commit at :meth:`publish`."""

    def __init__(self, config, run_id: str):
        self.config = config
        self.run_id = run_id
        self.root = Path(config.staging_root) / CHECKOUT_DIR / run_id
        self.store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
        root = lake_root(config)
        if not (root / CATALOG_FILE).exists():
            # The first write to a lake that publishes into its catalog creates it.
            with Catalog(root, create=True) as created:
                prepare_record_tables(created)
        # Checkouts only read: the lake lock is taken at publish, not for the
        # whole of a derive or a compact's merge.
        with contextlib.ExitStack() as resources:
            resources.enter_context(lake_read(root))
            self.catalog = resources.enter_context(
                Catalog(
                    root,
                    read_only=True,
                    **resources_for(config, "write").options(scratch=root / "tmp"),
                )
            )
            self._resources = resources.pop_all()
        self._checkouts: dict[str, Checkout] = {}
        # Files placed in the lake for a rehearsal on a copy of its catalog.
        self._rehearsed: list[Path] = []
        self._checks: list[tuple[str, Callable[[Catalog], str | None]]] = []
        self._scope = _SCOPE.get()
        #: Revisions of datasets the write read but does not publish, checked at commit.
        self._expected: dict[str, str | None] = {}

    @classmethod
    def for_store(cls, store: RevisionStore, run_id: str) -> LakeWrite:
        """A write for a caller that has a revision store but no config."""
        from types import SimpleNamespace

        data_root = Path(store.meta_root).parent
        roots = SimpleNamespace(
            data_root=data_root,
            meta_root=store.meta_root,
            curated_root=store.curated_root,
            derived_root=store.derived_root,
            staging_root=data_root / "staging",
        )
        return cls(roots, run_id)

    def __enter__(self) -> LakeWrite:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close(keep=exc_type is not None)

    def _drop_rehearsed(self) -> None:
        for path in self._rehearsed:
            path.unlink(missing_ok=True)
        self._rehearsed.clear()

    def close(self, *, keep: bool = False) -> Path | None:
        """Release the catalog and the checkout; *keep* moves it aside for inspection."""
        try:
            self._drop_rehearsed()
        finally:
            self._resources.close()
        if not self.root.exists():
            return None
        if not keep:
            shutil.rmtree(self.root, ignore_errors=True)
            return None
        quarantine = Path(self.config.data_root) / "_quarantine"
        quarantine.mkdir(parents=True, exist_ok=True)
        target = quarantine / f"checkout-{self.run_id}-{uuid.uuid4().hex[:8]}"
        shutil.move(str(self.root), target)
        return target

    def can_write(self, dataset: str) -> bool:
        """Whether *dataset* can be written here: the catalog holds its published revision."""
        pointer = self.store.current_pointer(dataset)
        held = _catalog_revision(self.catalog, dataset)
        if pointer is None:
            return held is None and not self.catalog.has_table(dataset)
        return held == pointer.get("revision_id")

    def checkout_staged(self, dataset: str, files: list[Path]) -> Checkout:
        """Check out the partitions *files* (a run's staged rows) will change."""
        if not self.can_write(dataset):
            raise WriteRejected(f"{dataset}: the lake catalog does not hold its published revision")
        checkout = self.checkout(dataset)
        for label in staged_partitions(dataset, files):
            checkout.partition(label)
        return checkout

    def has(self, dataset: str) -> bool:
        return dataset in self._checkouts

    def discard(self, dataset: str) -> Path | None:
        """Drop *dataset*'s checkout from this write, keeping its files aside for inspection."""
        checkout = self._checkouts.pop(dataset, None)
        if checkout is None or not checkout.root.exists():
            return None
        quarantine = Path(self.config.data_root) / "_quarantine"
        quarantine.mkdir(parents=True, exist_ok=True)
        target = quarantine / f"{dataset}-{self.run_id}-{uuid.uuid4().hex[:8]}"
        shutil.move(str(checkout.root), target)
        return target

    def checkout(self, dataset: str) -> Checkout:
        if dataset not in self._checkouts:
            self._checkouts[dataset] = Checkout(self, dataset)
        return self._checkouts[dataset]

    def expect(self, dataset: str, revision_id: str | None) -> None:
        """Refuse the commit unless *dataset* is still at *revision_id* (an input it read).

        The revision is the one the revision store's pointer named when the
        input was read, which an input the catalog does not hold also has.
        """
        self._expected[dataset] = revision_id

    def check(self, name: str, check: Callable[[Catalog], str | None]) -> None:
        """Run *check* on the written rows before they commit; a refusal commits nothing."""
        self._checks.append((name, check))

    def publish(
        self,
        datasets: dict[str, DatasetPublish],
        *,
        message: str | None = None,
        record: Callable[[Write], None] | None = None,
    ) -> dict[str, DatasetRevision]:
        """Publish every changed partition of *datasets* as one snapshot.

        Returns the new revision of each dataset that changed. Raises
        :class:`~equitylake_core.write.WriteRejected` and publishes nothing when a
        check refuses or a dataset moved since its checkout.
        """
        changes = self._changes(datasets)
        if not changes:
            return {}
        root = lake_root(self.config)
        with lake_mutation_lock(self.config.meta_root, blocking=True):
            with Catalog(
                root, **resources_for(self.config, "write").options(scratch=root / "tmp")
            ) as catalog:
                prepare_record_tables(catalog)
                return self._commit(
                    catalog, self.store, changes, datasets, message, advance=True, record=record
                )

    def rehearse(
        self, config, datasets: dict[str, DatasetPublish], *, message: str | None = None
    ) -> dict[str, DatasetRevision]:
        """Publish *datasets* into another lake: a clone of this one, under *config*.

        The clone then holds exactly what :meth:`publish` would commit here,
        and can be read like any lake. Uncommitted rows of an open catalog
        transaction are visible only to the connection that wrote them, so a
        check that reads data files cannot run inside the transaction; it
        runs on the clone instead. This lake and the checkouts are untouched.
        """
        changes = self._changes(datasets)
        if not changes:
            return {}
        store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
        root = lake_root(config)
        with Catalog(
            root, **resources_for(config, "write").options(scratch=root / "tmp")
        ) as catalog:
            return self._commit(catalog, store, changes, datasets, message, advance=False)

    def _changes(self, datasets) -> dict[str, dict[str, list[Path]]]:
        changes: dict[str, dict[str, list[Path]]] = {}
        for dataset in datasets:
            if dataset not in self._checkouts:
                continue
            changed = self._checkouts[dataset].changed()
            if changed:
                changes[dataset] = changed
        return changes

    def _commit(
        self,
        catalog: Catalog,
        store: RevisionStore,
        changes: dict[str, dict[str, list[Path]]],
        datasets: dict[str, DatasetPublish],
        message: str | None,
        *,
        advance: bool,
        record: Callable[[Write], None] | None = None,
    ) -> dict[str, DatasetRevision]:
        for dataset in changes:
            checkout = self._checkouts[dataset]
            if _catalog_revision(catalog, dataset) != checkout.base_revision:
                raise InputChanged(dataset, "checkout")
        for dataset, revision_id in self._expected.items() if advance else ():
            if (store.current_pointer(dataset) or {}).get("revision_id") != revision_id:
                raise InputChanged(dataset)
        if self._scope is not None:
            self._scope.check(catalog, changes)
        self._drop_rehearsed()
        swaps = {dataset: self._checkouts[dataset].swaps() for dataset in changes}
        receipts = {
            dataset: self._prepare(
                store, dataset, changes[dataset], swaps[dataset], datasets[dataset]
            )
            for dataset in changes
        }
        with Write(catalog) as write:
            holding: dict[str, list[str]] = {}
            for dataset, found in swaps.items():
                add, remove, holding[dataset] = self._place_files(write, dataset, found)
                write.swap(dataset, add=add, remove=remove)
            for name, check in self._checks:
                write.check(name, check)
            for dataset, receipt in receipts.items():
                where = " OR ".join(
                    f"({clause})"
                    for label in changes[dataset]
                    if (clause := self._checkouts[dataset].where(label))
                )
                files = holding[dataset]
                write.check(f"{dataset} sync state", _sync_recorder(dataset, receipt, where, files))
                write.record(REVISIONS_TABLE, _revision_recorder(receipt, where, files))
            # One snapshot holds the files, the sync state and the revision
            # rows, and it is file-complete: a pinned read needs nothing more.
            if record is not None:
                record(write)
            write.commit(message=message or f"run {self.run_id}")
        if catalog.borrows_data:
            # Placed in the lake's own data directory for a copy of its catalog:
            # they belong to no snapshot of the lake, and go when the copy does.
            self._rehearsed.extend(write.placed)
        for dataset, receipt in receipts.items():
            store.index_catalog_revision(receipt)
            if advance and self._scope is not None:
                self._scope.revisions[dataset] = receipt.revision_id
        return receipts

    def _place_files(
        self, write: Write, dataset: str, swaps: list[PartitionSwap]
    ) -> tuple[list[Path], list[str], list[str]]:
        """Put each partition's new files in *catalog*'s lake.

        Returns the files placed, the lake files to drop, and every file that
        holds a changed partition once both are done.
        """
        catalog = write.catalog
        checkout = self._checkouts[dataset]
        ours, theirs = self.catalog.data_root, catalog.data_root
        add: list[Path] = []
        remove: list[str] = []
        holding: list[str] = []
        if checkout.has_foreign_files() and catalog.has_table(dataset):
            # Registered files are refused by a table the catalog partitions itself.
            catalog.unpartition(dataset)
        for swap in swaps:
            where = checkout.where(swap.label)
            if where and swap.add:
                listed = ", ".join(quote_literal(path.as_posix()) for path in swap.add)
                outside = catalog.execute(
                    f"SELECT count(*) FROM read_parquet([{listed}], union_by_name => true, "
                    f"hive_partitioning => false) WHERE ({where}) IS NOT TRUE"
                ).fetchone()[0]
                if outside:
                    raise WriteRejected(
                        f"{dataset}: {outside} row(s) written to {swap.label} do not belong to it"
                    )
            directory = checkout.layout_dir(swap.label, catalog)
            for source in swap.add:
                target = directory / lake_file_name(source.name)
                conform_to = None if catalog.file_fits(dataset, source) else dataset
                write.place(source, target, conform_to=conform_to)
                add.append(target)
                holding.append(target.as_posix())
            # A copy of the lake's catalog reads the same files at the same place.
            remove.extend(
                (theirs / Path(path).relative_to(ours)).as_posix() for path in swap.remove
            )
            holding.extend((theirs / Path(path).relative_to(ours)).as_posix() for path in swap.keep)
        return add, remove, holding

    def _prepare(
        self,
        store: RevisionStore,
        dataset: str,
        changed: dict[str, list[Path]],
        swaps: list[PartitionSwap],
        publish: DatasetPublish,
    ) -> DatasetRevision:
        base = self._checkouts[dataset].root
        # The files this revision wrote: a file a partition keeps is not hashed again.
        records = tuple(
            RevisionFile(
                path=(Path(dataset) / path.relative_to(base)).as_posix(),
                size_bytes=path.stat().st_size,
                sha256=sha256_file(path),
            )
            for swap in swaps
            for path in swap.add
        )
        partitions = tuple(f"{dataset}/{label}" if label else dataset for label in changed)
        removed = [f"{dataset}/{label}" for label, files in changed.items() if not files]
        return store.next_catalog_revision(
            dataset,
            run_id=self.run_id,
            files=records,
            changed_partitions=partitions,
            schema_version=publish.schema_version,
            contract_fingerprint=publish.contract_fingerprint,
            metadata={
                **dict(publish.metadata or {}),
                **({"removed_partitions": removed} if removed else {}),
            },
            identity=publish.identity,
        )


class _Reader:
    """What a :class:`Checkout` needs of its owner, for a read-only export."""

    def __init__(self, catalog: Catalog, root: Path):
        self.catalog = catalog
        self.root = root


def published_layout(data_root: Path, dataset: str) -> dict[str, list[tuple[Path, str]]] | None:
    """Each published partition of *dataset*: its lake files and the names its writers give them.

    Lake files are never changed in place, so the list stays true until
    retention removes a file. ``None`` when some file does not sit in its
    partition's folder (written by the catalog itself), so the partition has
    to be read by its rows instead; see :func:`export_tree`.
    """
    with Catalog(Path(data_root) / LAKE_DIR, read_only=True) as catalog:
        checkout = Checkout(
            _Reader(catalog, Path(data_root)), dataset, root=Path(data_root) / dataset, link=False
        )
        layout: dict[str, list[tuple[Path, str]]] = {}
        for label in checkout.labels():
            files = checkout._touch(label)
            if not files or not all(checkout._laid_out(path, label) for path in files):
                return None
            layout[label] = [(Path(path), logical_name(path)) for path in files]
        return layout


def export_tree(data_root: Path, dataset: str, destination: Path) -> None:
    """Write *dataset*'s published catalog rows to *destination* in its partition layout."""
    destination.mkdir(parents=True, exist_ok=True)
    with Catalog(Path(data_root) / LAKE_DIR, read_only=True) as catalog:
        checkout = Checkout(
            _Reader(catalog, destination.parent), dataset, root=destination, link=False
        )
        for label in checkout.labels():
            checkout.partition(label)


def _declared_type(dataset: str, column: str) -> str:
    """The SQL type *column* has in *dataset*'s schema, for a table not created yet."""
    from cnequity.domain.schemas import DATASET_SCHEMAS

    dtype = DATASET_SCHEMAS.get(dataset, {}).get(column)
    if dtype == pl.Utf8:
        return "VARCHAR"
    if isinstance(dtype, pl.Datetime) or dtype == pl.Datetime:
        return "TIMESTAMP"
    return "DATE"


def staged_partitions(dataset: str, files: list[Path]) -> list[str]:
    """The partition labels (``<column>=<value>``) the staged *files* fall into."""
    from cnequity.domain.datasets import granularity_for_dataset
    from cnequity.storage.parquet import _partition_values

    column = DATASETS[dataset].partition_col
    if not column:
        return [""]
    labels: set[str] = set()
    for path in files:
        frame = pl.read_parquet(path, columns=[column])
        if frame.is_empty():
            continue
        values = _partition_values(frame, column, granularity_for_dataset(dataset))
        labels.update(f"{column}={value}" for value in values.unique().to_list())
    return sorted(labels)


def _sync_recorder(dataset: str, receipt: DatasetRevision, where: str, files: list[str]):
    """Record, inside the write, which revision the table holds and whether keys are unique.

    *files* hold the changed partitions after the write. They are read as
    files: the catalog would plan each query over every file of the table.
    """

    def record(catalog: Catalog) -> None:
        was_unique = keys_unique(catalog, dataset)
        if was_unique and (not where or _partition_key_column(catalog, dataset)):
            # Other partitions cannot hold these keys: check the changed ones only.
            unique = not files_have_duplicate_keys(catalog, dataset, files)
        else:
            unique = _table_keys_unique(catalog, dataset)
        _ensure_sync_table(catalog)
        _record_sync(catalog, dataset, receipt.revision_id, {}, unique)
        return None

    return record


def _revision_recorder(receipt: DatasetRevision, where: str, files: list[str]):
    """The ``_revisions`` row of *receipt*, built inside the write that publishes it.

    The row records a digest of the published rows of the changed partitions
    (the whole table when it has none), computed from *files*, the data files
    that hold them after the write. :func:`verify_revisions` computes it
    again through the catalog at the revision's snapshot: the two agree only
    if the catalog serves exactly the rows that were published.
    """

    def row(catalog: Catalog) -> dict[str, object]:
        digest = files_digest(catalog, receipt.dataset, files)
        return {
            **_revision_row(receipt),
            # Set by the sync recorder earlier in the same transaction.
            "unique_keys": bool(keys_unique(catalog, receipt.dataset)),
            "rows": digest.rows,
            "rows_digest": digest.digest,
            "digest_algorithm": digest.algorithm,
            "digest_where": where,
        }

    return row


@dataclass(frozen=True)
class RevisionCheck:
    dataset: str
    revision_id: str
    snapshot: int
    #: "match"; "mismatch"; "expired" (the snapshot or its files are gone);
    #: "other_algorithm" (recorded by a DuckDB whose row hash may differ).
    status: str
    rows: int


def verify_revisions(catalog: Catalog, dataset: str | None = None) -> list[RevisionCheck]:
    """Compare each published revision's recorded row digest with the rows its snapshot holds."""
    # A publish adds the digest columns; a catalog nothing has been published
    # into has the table without them, and no revision to check.
    if not catalog.has_table(REVISIONS_TABLE) or "rows_digest" not in catalog.column_types(
        REVISIONS_TABLE
    ):
        return []
    recorded = catalog.execute(
        f"SELECT dataset, revision_id, snapshot_id, rows, rows_digest, digest_algorithm, "
        f"digest_where FROM {REVISIONS_TABLE} ORDER BY snapshot_id, dataset"
    ).fetchall()
    kept = {
        int(row[0])
        for row in catalog.execute(
            f"SELECT snapshot_id FROM ducklake_snapshots('{ALIAS}')"
        ).fetchall()
    }
    checks = []
    for name, revision_id, snapshot, rows, digest, algorithm, where in recorded:
        if dataset is not None and name != dataset:
            continue
        if int(snapshot) not in kept:
            status = "expired"
        elif algorithm != digest_algorithm():
            status = "other_algorithm"
        else:
            try:
                now = rows_digest(catalog, name, where=where or None, snapshot=int(snapshot))
            except Exception:  # the snapshot's files were already cleaned up
                status = "expired"
            else:
                status = "match" if now.digest == digest else "mismatch"
        checks.append(RevisionCheck(name, revision_id, int(snapshot), status, int(rows)))
    return checks


def _revision_row(receipt: DatasetRevision) -> dict[str, object]:
    from cnequity.storage.revision_records import receipt_columns

    return {
        "dataset": receipt.dataset,
        "revision_id": receipt.revision_id,
        **receipt_columns(receipt.to_dict()),
    }


def _catalog_revision(catalog: Catalog, dataset: str) -> str | None:
    return synced_revision_id(catalog, dataset)


def catalog_revision(store: RevisionStore, dataset: str) -> str | None:
    """The revision id the catalog holds for *dataset*, or ``None`` without a catalog."""
    from equitylake_core.catalog import CATALOG_FILE

    root = Path(store.meta_root.parent) / LAKE_DIR
    if not (root / CATALOG_FILE).exists():
        return None
    with Catalog(root, read_only=True) as catalog:
        return _catalog_revision(catalog, dataset)


def publish_frames(config, frames: dict[str, pl.DataFrame], *, run_id: str, message: str) -> dict:
    """Publish whole datasets held in memory as one catalog commit.

    Each frame replaces its dataset, partition by partition. For the small
    lakes and one-off moves that have no staging run: the demo and sample
    lakes, and the ``legacy-datasets`` migration.
    """
    from cnequity.domain.contracts import contract_fingerprint, dataset_contract
    from cnequity.domain.datasets import granularity_for_dataset
    from cnequity.storage import revision_records
    from cnequity.storage.parquet import _partition_values

    publishes: dict[str, DatasetPublish] = {}
    with LakeWrite(config, run_id) as write:
        for dataset, rows in frames.items():
            column = DATASETS[dataset].partition_col
            contract = dataset_contract(dataset)
            publishes[dataset] = DatasetPublish(
                int(contract["schema_version"]), contract_fingerprint(contract)
            )
            checkout = write.checkout(dataset)
            if column is None:
                checkout.rewrite("", rows)
                continue
            labels = _partition_values(rows, column, granularity_for_dataset(dataset))
            for (value,), group in (
                rows.with_columns(labels.alias("__label__"))
                .partition_by("__label__", as_dict=True, maintain_order=True)
                .items()
            ):
                checkout.rewrite(f"{column}={value}", group.drop("__label__"))
        published = write.publish(publishes, message=message)
    revision_records.forget(config.meta_root)
    return published
