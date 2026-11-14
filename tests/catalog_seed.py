"""Build a test lake the way a lake is built now: rows published into its catalog.

Tests used to seed a lake with ``RevisionStore.commit``, the generation commit
releases before 0.25.0 wrote. :func:`publish_rows` publishes through the same
:class:`~cnequity.storage.lake_write.LakeWrite` every writer uses, so a test
reads and writes what a real lake holds.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl


def publish_rows(config, dataset: str, rows: pl.DataFrame, *, run_id: str = "seed"):
    """Publish *rows* as the whole of the partitions they fall into; returns the revision.

    Partitions the rows do not touch keep what they held, and a partition is
    replaced, not merged: to add rows to one, pass its old rows too. ``None``
    when nothing changed. A lake that still holds generations has to be moved
    onto its catalog first (``migrate_to_catalog``).
    """
    from cnequity.domain.contracts import contract_fingerprint, dataset_contract
    from cnequity.domain.datasets import DATASETS, granularity_for_dataset
    from cnequity.query import catalog_read
    from cnequity.storage.catalog_migration import _holds_data
    from cnequity.storage.lake_record import start_on_catalog
    from cnequity.storage.lake_write import DatasetPublish, LakeWrite
    from cnequity.storage.parquet import _partition_values
    from cnequity.storage.revisions import RevisionStore

    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root, create=False)
    if not store.publishes_to_catalog():
        if _holds_data(config):
            raise ValueError("this test lake holds generations; move it onto its catalog first")
        Path(config.meta_root).mkdir(parents=True, exist_ok=True)
        start_on_catalog(config)
    column = DATASETS[dataset].partition_col
    contract = dataset_contract(dataset)
    publish = DatasetPublish(int(contract["schema_version"]), contract_fingerprint(contract))
    # A reader of this process may hold the catalog open.
    catalog_read.close(config)
    with LakeWrite(config, run_id) as write:
        checkout = write.checkout(dataset)
        if column is None:
            checkout.rewrite("", rows)
        else:
            labels = _partition_values(rows, column, granularity_for_dataset(dataset))
            for (value,), group in (
                rows.with_columns(labels.alias("__label__"))
                .partition_by("__label__", as_dict=True, maintain_order=True)
                .items()
            ):
                checkout.rewrite(f"{column}={value}", group.drop("__label__"))
        published = write.publish({dataset: publish}, message=f"{dataset} seeded by {run_id}")
    catalog_read.close(config)
    return published.get(dataset)
