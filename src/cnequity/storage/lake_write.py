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

During the move to the catalog the revision store still keeps a receipt and a
pointer per revision (marked ``storage: catalog``, with no generation), so
every reader keyed on them keeps working.
"""

from __future__ import annotations

import shutil
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from cnequity.domain.datasets import DATASETS
from cnequity.domain.schemas import PRIMARY_KEYS
from cnequity.file_lock import lake_mutation_lock
from cnequity.storage.catalog_bridge import (
    _ensure_sync_table,
    _has_duplicate_keys,
    _partition_key_column,
    _partition_predicate,
    _record_history,
    _record_sync,
    _table_keys_unique,
    keys_unique,
    lake_root,
    synced_revision,
)
from cnequity.storage.locator import layer_name
from cnequity.storage.parquet import emit_partition, merge_group_keys
from cnequity.storage.revisions import DatasetRevision, RevisionFile, RevisionStore, sha256_file
from equitylake_core.catalog import Catalog, quote_identifier
from equitylake_core.write import Write, WriteRejected

CHECKOUT_DIR = "_checkout"
#: One row per published revision, written in the same snapshot as its rows.
REVISIONS_TABLE = "_revisions"

Stamps = dict[str, tuple[int, int]]


def _stamps(directory: Path) -> Stamps:
    if not directory.is_dir():
        return {}
    return {
        path.relative_to(directory).as_posix(): (path.stat().st_size, path.stat().st_mtime_ns)
        for path in directory.rglob("*.parquet")
    }


class Checkout:
    """The partitions of one dataset that a write changes, checked out of the catalog."""

    def __init__(self, write, dataset: str, *, root: Path | None = None):
        self.dataset = dataset
        self.spec = DATASETS[dataset]
        #: Pass this where a writer expects ``curated_root`` or ``derived_root``.
        self.layer_root = write.root / layer_name(dataset) if root is None else root.parent
        self.root = self.layer_root / dataset if root is None else root
        self._write = write
        self._checked: dict[str, Stamps] = {}
        self._removed: set[str] = set()
        self._types: dict[str, str] | None = None
        self.base_revision = _catalog_revision(write.catalog, dataset)

    def partition(self, label: str = "") -> Path:
        """Check out ``<column>=<value>`` (``""`` for an unpartitioned dataset) and return it.

        It holds the partition's published rows, written as the partition's
        merged file or buckets; checking it out again returns it as it is.
        """
        if bool(label) != bool(self.spec.partition_col):
            raise ValueError(f"{self.dataset}: partition label {label!r} does not fit its layout")
        target = self.root / label if label else self.root
        if label in self._checked:
            return target
        target.mkdir(parents=True, exist_ok=True)
        rows = self._published_rows(label)
        if rows.height:
            emit_partition(
                rows, self.dataset, affected=[], existing=[], read_dir=None, out_dir=target
            )
        self._checked[label] = _stamps(target)
        return target

    def remove(self, label: str) -> None:
        """Delete the partition *label* from the dataset."""
        target = self.partition(label)
        shutil.rmtree(target)
        self._removed.add(label)

    def changed(self) -> dict[str, list[Path]]:
        """Each checked-out partition a writer changed, with its files (none: removed)."""
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
            if after != before:
                out[label] = [directory / relative for relative in sorted(after)]
        return out

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
            column, types.get(column, "DATE"), self.spec.partition_granularity, value
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


class LakeWrite:
    """One run's write to the lake: checkouts now, one catalog commit at :meth:`publish`."""

    def __init__(self, config, run_id: str):
        self.config = config
        self.run_id = run_id
        self.root = Path(config.staging_root) / CHECKOUT_DIR / run_id
        self.store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
        root = lake_root(config)
        self.catalog = Catalog(root, memory_limit="4GB", temp_directory=root / "tmp")
        self._checkouts: dict[str, Checkout] = {}
        # Datasets published from a whole working tree: root, and changed partitions.
        self._trees: dict[str, tuple[Path, list[str]]] = {}
        self._checks: list[tuple[str, Callable[[Catalog], str | None]]] = []

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

    def close(self, *, keep: bool = False) -> Path | None:
        """Release the catalog and the checkout; *keep* moves it aside for inspection."""
        self.catalog.close()
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
        """Whether *dataset* can be written here: the catalog holds its published revision.

        A dataset the catalog is behind on (or has never seen while the store
        has it) stays on the generation path until ``sync-catalog`` catches up.
        """
        pointer = self.store.current_pointer(dataset)
        held = _catalog_revision(self.catalog, dataset)
        if pointer is None:
            return held is None and not self.catalog.has_table(dataset)
        return held == pointer.get("revision_id")

    def checkout_staged(self, dataset: str, files: list[Path]) -> Checkout | None:
        """Check out the partitions *files* (a run's staged rows) will change, or ``None``."""
        if not self.can_write(dataset):
            return None
        checkout = self.checkout(dataset)
        for label in staged_partitions(dataset, files):
            checkout.partition(label)
        return checkout

    def has(self, dataset: str) -> bool:
        return dataset in self._checkouts or dataset in self._trees

    def stage_tree(self, dataset: str, root: Path, partitions: list[str]) -> None:
        """Publish *partitions* of *dataset* from the working tree *root*, whole.

        For writers that read their own history from a working tree the store
        keeps verified against the catalog (``RevisionStore.materialize_current``):
        each named partition is replaced by every file it holds there, and a
        partition missing there is removed.
        """
        self.checkout(dataset)  # records the revision the write is based on
        self._trees[dataset] = (Path(root), sorted(set(partitions)))

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

    def check(self, name: str, check: Callable[[Catalog], str | None]) -> None:
        """Run *check* on the written rows before they commit; a refusal commits nothing."""
        self._checks.append((name, check))

    def publish(
        self, datasets: dict[str, DatasetPublish], *, message: str | None = None
    ) -> dict[str, DatasetRevision]:
        """Publish every changed partition of *datasets* as one snapshot.

        Returns the new revision of each dataset that changed. Raises
        :class:`~equitylake_core.write.WriteRejected` and publishes nothing when a
        check refuses or a dataset moved since its checkout.
        """
        changes: dict[str, dict[str, list[Path]]] = {}
        for dataset in datasets:
            if dataset in self._trees:
                root, labels = self._trees[dataset]
                changed = {
                    label: sorted((root / label if label else root).rglob("*.parquet"))
                    if (root / label if label else root).is_dir()
                    else []
                    for label in labels
                }
            elif dataset in self._checkouts:
                changed = self._checkouts[dataset].changed()
            else:
                continue
            if changed:
                changes[dataset] = changed
        if not changes:
            return {}
        with lake_mutation_lock(self.config.meta_root, blocking=True):
            for dataset in changes:
                checkout = self._checkouts[dataset]
                if _catalog_revision(self.catalog, dataset) != checkout.base_revision:
                    raise WriteRejected(f"{dataset} was published by another write since checkout")
            receipts = {
                dataset: self._prepare(dataset, changed, datasets[dataset])
                for dataset, changed in changes.items()
            }
            write = Write(self.catalog)
            for dataset, changed in changes.items():
                checkout = self._checkouts[dataset]
                for label, files in changed.items():
                    write.replace(
                        dataset,
                        files,
                        where=checkout.where(label),
                        order_by=PRIMARY_KEYS.get(dataset, ()),
                    )
            for name, check in self._checks:
                write.check(name, check)
            for dataset, receipt in receipts.items():
                where = " OR ".join(
                    f"({clause})"
                    for label in changes[dataset]
                    if (clause := self._checkouts[dataset].where(label))
                )
                write.check(f"{dataset} sync state", _sync_recorder(dataset, receipt, where))
                write.record(REVISIONS_TABLE, _revision_row(receipt))
            write.commit(message=message or f"run {self.run_id}")
            # The snapshot after the delete folds: same rows, and file-complete.
            snapshot = self.catalog.snapshot()
            with self.catalog.transaction(message=f"history of run {self.run_id}"):
                for dataset, receipt in receipts.items():
                    _record_history(self.catalog, dataset, receipt.revision_id, snapshot=snapshot)
            for dataset, receipt in receipts.items():
                self.store.finish_catalog_revision(receipt)
                if dataset not in self._trees:
                    # Keep the working tree a valid cache of what was published.
                    self.store.advance_working_tree(
                        dataset, self._checkouts[dataset].base_revision, changes[dataset]
                    )
        return receipts

    def _prepare(
        self, dataset: str, changed: dict[str, list[Path]], publish: DatasetPublish
    ) -> DatasetRevision:
        base = self._trees[dataset][0] if dataset in self._trees else self._checkouts[dataset].root
        records = tuple(
            RevisionFile(
                path=(Path(dataset) / path.relative_to(base)).as_posix(),
                size_bytes=path.stat().st_size,
                sha256=sha256_file(path),
            )
            for files in changed.values()
            for path in files
        )
        partitions = tuple(f"{dataset}/{label}" if label else dataset for label in changed)
        removed = [f"{dataset}/{label}" for label, files in changed.items() if not files]
        return self.store.prepare_catalog_revision(
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
        )


class _Reader:
    """What a :class:`Checkout` needs of its owner, for a read-only export."""

    def __init__(self, catalog: Catalog, root: Path):
        self.catalog = catalog
        self.root = root


def export_tree(data_root: Path, dataset: str, destination: Path) -> None:
    """Write *dataset*'s published catalog rows to *destination* in its partition layout."""
    from cnequity.domain.datasets import granularity_for_dataset
    from cnequity.storage.parquet import _partition_values

    destination.mkdir(parents=True, exist_ok=True)
    with Catalog(Path(data_root) / "lake", read_only=True) as catalog:
        checkout = Checkout(_Reader(catalog, destination.parent), dataset, root=destination)
        column = checkout.spec.partition_col
        if not column:
            checkout.partition("")
            return
        if not catalog.has_table(dataset):
            return
        values = catalog.relation(
            f"SELECT DISTINCT {quote_identifier(column)} FROM {quote_identifier(dataset)}"
        ).pl()
        labels = sorted(
            {
                f"{column}={value}"
                for value in _partition_values(values, column, granularity_for_dataset(dataset))
                .unique()
                .to_list()
            }
        )
        for label in labels:
            checkout.partition(label)


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


def _sync_recorder(dataset: str, receipt: DatasetRevision, where: str):
    """Record, inside the write, which revision the table holds and whether keys are unique."""

    def record(catalog: Catalog) -> None:
        was_unique = keys_unique(catalog, dataset)
        if where and was_unique and _partition_key_column(catalog, dataset):
            unique = not _has_duplicate_keys(catalog, dataset, where)
        else:
            unique = _table_keys_unique(catalog, dataset)
        _ensure_sync_table(catalog)
        _record_sync(catalog, dataset, receipt.revision_id, {}, unique)
        return None

    return record


def _revision_row(receipt: DatasetRevision) -> dict[str, object]:
    import json

    return {
        "dataset": receipt.dataset,
        "revision": receipt.revision,
        "revision_id": receipt.revision_id,
        "run_id": receipt.run_id,
        "committed_at": receipt.committed_at,
        "schema_version": receipt.schema_version,
        "contract_fingerprint": receipt.contract_fingerprint,
        "content_digest": receipt.content_digest,
        "changed_partitions": json.dumps(list(receipt.changed_partitions)),
        "metadata": json.dumps(receipt.metadata, sort_keys=True, default=str),
    }


def _catalog_revision(catalog: Catalog, dataset: str) -> str | None:
    synced = synced_revision(catalog, dataset)
    return synced[0] if synced else None


def catalog_revision(store: RevisionStore, dataset: str) -> str | None:
    """The revision id the catalog holds for *dataset*, or ``None`` without a catalog."""
    from equitylake_core.catalog import CATALOG_FILE

    root = Path(store.meta_root.parent) / "lake"
    if not (root / CATALOG_FILE).exists():
        return None
    with Catalog(root, read_only=True) as catalog:
        return _catalog_revision(catalog, dataset)
