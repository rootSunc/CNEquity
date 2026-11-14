"""What the repairs that filter stored rows share: the root they read and their publish.

``publish_repair`` used to be a private ``_publish`` in ``orphan_symbols`` that
ten repairs imported. Because ``RevisionStore.commit`` read a change made only
of deleted partitions as "unchanged", it also rewrote an untouched partition to
force the publish; ``commit`` now takes the deleted partitions directly.

On a lake that publishes into its catalog a repair never builds the dataset's
working tree: it reads the published rows where they are (``repair_root``),
and the publish reads one partition at a time from the catalog and checks out
only the partitions it changes.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from cnequity.config import Config
from cnequity.storage.locator import layer_root
from cnequity.storage.parquet import CuratedWriter
from cnequity.storage.revisions import RevisionStore

logger = logging.getLogger(__name__)


def repair_root(
    config: Config, store: RevisionStore, dataset: str, *, register: bool
) -> Path | None:
    """The root a repair scans for *dataset*'s published rows, or ``None`` without data.

    Scan it with ``scan_parquet_root``: on a lake that publishes into its
    catalog it may name a revision that has no directory, so it must not be
    walked. Elsewhere it is ``RevisionStore.repair_base``.
    """
    if not store.commits_to_catalog(dataset):
        return store.repair_base(dataset, register=register)
    from cnequity.query.parquet_scan import dataset_has_parquet
    from cnequity.storage.read_context import read_root

    root = Path(read_root(config, dataset))
    return root if dataset_has_parquet(root, committed=False) else None


def scan_published(config: Config, dataset: str, **filters) -> pl.LazyFrame | None:
    """A scan of *dataset*'s published rows wherever they live, or ``None`` without data.

    *filters* are ``scan_parquet_root``'s (``partition_col``, ``symbols``, ...).
    """
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
    from cnequity.storage.read_context import read_root

    root = Path(read_root(config, dataset))
    if not dataset_has_parquet(root, committed=False):
        return None
    return scan_parquet_root(root, committed=False, **filters)


class PartitionRewrite:
    """Read a dataset partition by partition and publish the ones a repair replaces.

    One shape for both kinds of lake. On a lake that publishes into its
    catalog the rows come from the catalog one partition at a time and only
    the replaced partitions are checked out; elsewhere they come from the
    committed generation's partition directories and the replacements are
    written into the working tree. Either way the replaced partitions pass
    the publication gate before one revision is committed, and a blocked or
    failed publish leaves the published data as it was.

    Use it as a context manager. Without *apply* it only reads.
    """

    def __init__(
        self, config: Config, store: RevisionStore, dataset: str, *, run_id: str, apply: bool
    ):
        from cnequity.domain.datasets import DATASETS

        self.config, self.store, self.dataset = config, store, dataset
        self.run_id, self.apply = run_id, apply
        self.partition_col = DATASETS[dataset].partition_col
        if self.partition_col is None:
            raise ValueError(f"{dataset} is not partitioned")
        self.rewritten = 0
        self.emptied = 0
        self._catalog = store.commits_to_catalog(dataset)
        self._write = None
        self._reader = None
        self._checkout = None
        self._base: Path | None = None
        self._seeded = False
        self._changed: dict[str, Path] = {}
        self._removed: list[str] = []

    def __enter__(self) -> PartitionRewrite:
        if self.apply:
            from cnequity.storage.catalog_migration import require_catalog_lake

            require_catalog_lake(self.config)
            self._catalog = self.store.commits_to_catalog(self.dataset)
        if not self._catalog:
            self._base = self.store.repair_base(self.dataset, register=self.apply)
            return self
        from cnequity.query import catalog_read
        from cnequity.storage.catalog_bridge import lake_root
        from cnequity.storage.lake_write import Checkout, LakeWrite, _Reader
        from equitylake_core.catalog import Catalog

        # Scans of the repair's plan may hold the catalog open in this process.
        catalog_read.close(self.config)
        if self.apply:
            self._write = LakeWrite(self.config, self.run_id)
            self._checkout = self._write.checkout(self.dataset)
        else:
            self._reader = Catalog(lake_root(self.config), read_only=True)
            self._checkout = Checkout(
                _Reader(self._reader, Path(self.config.staging_root)), self.dataset
            )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._write is not None:
            self._write.close(keep=exc_type is not None)
        if self._reader is not None:
            self._reader.close()

    def labels(self) -> list[str]:
        """Every published partition, as ``<column>=<value>``."""
        if self._catalog:
            return self._checkout.labels()
        if self._base is None:
            return []
        return sorted(
            path.name
            for path in self._base.iterdir()
            if path.is_dir() and any(path.rglob("*.parquet"))
        )

    def rows(self, label: str) -> pl.DataFrame | None:
        """The published rows of *label*, or ``None`` when it holds none."""
        if self._catalog:
            frame = self._checkout.rows(label)
            return None if frame.is_empty() else frame
        if self._base is None:
            return None
        files = sorted((self._base / label).rglob("*.parquet"))
        if not files:
            return None
        return pl.concat([pl.read_parquet(path) for path in files], how="diagonal_relaxed")

    def replace(self, label: str, rows: pl.DataFrame) -> None:
        """Make *rows* the whole of partition *label*; no rows removes it."""
        if not self.apply:
            raise RuntimeError("a preview cannot replace a partition")
        column, _, value = label.partition("=")
        if column != self.partition_col or not value:
            raise ValueError(f"{self.dataset}: {label!r} is not a {self.partition_col} partition")
        if rows.is_empty():
            self.emptied += 1
        else:
            self.rewritten += 1
        if self._catalog:
            self._checkout.rewrite(label, rows)
            return
        if not self._seeded:
            # The commit takes the named partitions from the working tree, so
            # the tree must hold the published revision before the first one.
            self.store.materialize_current(self.dataset)
            self._seeded = True
        layer = layer_root(self.config, self.dataset)
        if rows.is_empty():
            target = layer / self.dataset / label
            if target.exists():
                import shutil

                shutil.rmtree(target)
            self._changed.pop(label, None)
            self._removed.append(label)
            return
        self._changed[label] = CuratedWriter(layer).write_partition(
            self.dataset, column, value, rows, "part-merged.parquet"
        )

    def publish(self, metadata: dict, *, symbols: frozenset[str] = frozenset()) -> dict | None:
        """Gate and commit the replaced partitions; ``None`` when nothing was replaced."""
        from cnequity.domain.contracts import contract_fingerprint, dataset_contract
        from cnequity.quality.publication import check_repair_publication

        contract = dataset_contract(self.dataset)
        schema_version = int(contract["schema_version"])
        fingerprint = contract_fingerprint(contract)
        if self._catalog:
            from cnequity.storage.lake_write import DatasetPublish

            found = self._checkout.changed()
            if not found:
                return None
            files = [path for paths in found.values() for path in paths]
            publication = check_repair_publication(
                self.config,
                self.dataset,
                self.run_id,
                files,
                symbols=symbols,
                lake_write=self._write,
            )
            revision = self._write.publish(
                {
                    self.dataset: DatasetPublish(
                        schema_version,
                        fingerprint,
                        {**metadata, "publication_audit": publication["report_path"]},
                    )
                },
                message=f"{self.dataset} revision from {self.run_id}",
            ).get(self.dataset)
        else:
            if not self._changed and not self._removed:
                return None
            changed = list(self._changed.values())
            try:
                publication = check_repair_publication(
                    self.config, self.dataset, self.run_id, changed, symbols=symbols
                )
                revision = self.store.commit(
                    self.dataset,
                    run_id=self.run_id,
                    changed_files=changed,
                    removed_partitions=self._removed,
                    schema_version=schema_version,
                    contract_fingerprint=fingerprint,
                    metadata={**metadata, "publication_audit": publication["report_path"]},
                )
            except Exception:
                # Leave the working tree as published, not half rewritten.
                self.store.materialize_current(self.dataset)
                raise
        if revision is None:
            return None
        return {
            "revision": revision.revision,
            "revision_id": revision.revision_id,
            "publication_audit": publication["report_path"],
        }


def publish_repair(
    config: Config,
    store: RevisionStore,
    dataset: str,
    *,
    keep: Callable[[pl.DataFrame], pl.DataFrame],
    reason: str,
    metadata: dict,
) -> dict:
    """Rewrite the partitions ``keep`` changes and publish one gated revision.

    The shared end of the repairs that filter stored rows: every partition
    goes through *keep*; a partition it changes is rewritten, one it empties
    is deleted, and the result passes the publication gate before one
    revision is committed. A repair that changes nothing raises, so a no-op is
    never mistaken for a fix.
    """
    run_id = f"{reason}-{dataset}-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    with PartitionRewrite(config, store, dataset, run_id=run_id, apply=True) as rewrite:
        for label in rewrite.labels():
            frame = rewrite.rows(label)
            if frame is None:
                continue
            kept = keep(frame)
            if kept is frame or (kept.height == frame.height and kept.equals(frame)):
                continue
            rewrite.replace(label, kept)
        published = rewrite.publish(
            {"reason": reason, **metadata, "partitions_emptied": rewrite.emptied}
        )
    if published is None:
        raise RuntimeError(f"{dataset}: {reason} produced no revision; nothing published")
    logger.info("%s: %s published revision %s", dataset, reason, published["revision"])
    return {
        "revision": published["revision"],
        "partitions_rewritten": rewrite.rewritten,
        "partitions_emptied": rewrite.emptied,
        "publication_audit": published["publication_audit"],
    }


def publish_staged(
    config: Config,
    store: RevisionStore,
    dataset: str,
    rows: pl.DataFrame,
    *,
    run_id: str,
    base_root: Path,
    metadata: dict,
    batch: str = "repair-0",
    partition_col: str | None = "trade_date",
    symbols: frozenset[str] = frozenset(),
) -> dict | None:
    """Merge *rows* into *dataset* by primary key and publish one gated revision.

    The shared end of the repairs that add or restate rows: the rows are
    staged in a folder of their own (so no ingestion or retry can sweep them
    up), merged the way a daily run merges a batch, and the changed partitions
    pass the publication gate before one revision is committed. Returns the
    revision number and the audit's path, or ``None`` when the merge changed
    nothing.

    On a lake that publishes into its catalog only the partitions the rows
    fall into are checked out; elsewhere the merge runs in the working tree
    against *base_root*, the committed root the repair planned from.
    """
    from tempfile import TemporaryDirectory

    from cnequity.domain.contracts import contract_fingerprint, dataset_contract
    from cnequity.quality.publication import check_repair_publication
    from cnequity.storage.catalog_migration import require_catalog_lake
    from cnequity.storage.parquet import StagingWriter, compact_dataset

    require_catalog_lake(config)
    contract = dataset_contract(dataset)
    schema_version = int(contract["schema_version"])
    fingerprint = contract_fingerprint(contract)
    layer = layer_root(config, dataset)
    changed: list[Path] = []
    with TemporaryDirectory(prefix=f"cne-repair-{dataset}-") as directory:
        staging = Path(directory)
        staged = StagingWriter(staging).write_batch(dataset, run_id, batch, rows)
        if store.commits_to_catalog(dataset):
            from cnequity.query import catalog_read
            from cnequity.storage.lake_write import DatasetPublish, LakeWrite

            # Scans of the repair's plan may hold the catalog open in this process.
            catalog_read.close(config)
            with LakeWrite(config, run_id) as write:
                checkout = write.checkout_staged(dataset, [staged])
                if checkout is None:
                    raise RuntimeError(
                        f"{dataset}: the lake catalog does not hold its published revision"
                    )
                compact_dataset(
                    staging,
                    layer,
                    dataset,
                    run_id,
                    partition_col=partition_col,
                    changed_files=changed,
                    base_root=checkout.root,
                    out_root=checkout.layer_root,
                )
                files = [path for found in checkout.changed().values() for path in found]
                if not files:
                    return None
                publication = check_repair_publication(
                    config, dataset, run_id, files, symbols=symbols, lake_write=write
                )
                revision = write.publish(
                    {
                        dataset: DatasetPublish(
                            schema_version,
                            fingerprint,
                            {**metadata, "publication_audit": publication["report_path"]},
                        )
                    },
                    message=f"{dataset} revision from {run_id}",
                ).get(dataset)
        else:
            store.materialize_current(dataset)
            try:
                compact_dataset(
                    staging,
                    layer,
                    dataset,
                    run_id,
                    partition_col=partition_col,
                    changed_files=changed,
                    base_root=base_root,
                )
                if not changed:
                    return None
                publication = check_repair_publication(
                    config, dataset, run_id, changed, symbols=symbols
                )
                revision = store.commit(
                    dataset,
                    run_id=run_id,
                    changed_files=changed,
                    schema_version=schema_version,
                    contract_fingerprint=fingerprint,
                    metadata={**metadata, "publication_audit": publication["report_path"]},
                )
            except Exception:
                # Leave the working tree as published, not half merged.
                store.materialize_current(dataset)
                raise
    if revision is None:
        return None
    return {"revision": revision.revision, "publication_audit": publication["report_path"]}
