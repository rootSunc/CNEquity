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


def record_revisions(config) -> None:
    """Finish moving a generation lake that was synced into a catalog onto it, as `cne upgrade` does.

    The catalog then records every revision and the lake publishes there:
    the state every writer requires.
    """
    from cnequity.storage import revision_records
    from cnequity.storage.catalog_migration import _record_revisions

    Path(config.meta_root).mkdir(parents=True, exist_ok=True)
    _record_revisions(config)
    revision_records.forget()


def commit_tree(
    store,
    dataset: str,
    *,
    run_id: str,
    changed_files,
    schema_version: int,
    contract_fingerprint: str,
    metadata: dict | None = None,
    removed_partitions=(),
    _locked: bool = False,
):
    """Publish a hand-written tree's changes, the way ``RevisionStore.commit`` did.

    Many tests write parquet files under ``curated/<dataset>/`` (or
    ``derived/``) and published them with ``store.commit``. The tree is taken
    as what the dataset holds: every catalog partition that a changed file's
    rows fall into is published with all of the tree's rows in it, and with
    *removed_partitions* every published partition the tree no longer has
    rows for is removed. Returns the revision, or ``None`` when nothing
    changed.
    """
    from types import SimpleNamespace

    import polars as pl

    from cnequity.domain.datasets import DATASETS, granularity_for_dataset
    from cnequity.query import catalog_read
    from cnequity.storage.lake_record import start_on_catalog
    from cnequity.storage.lake_write import DatasetPublish, LakeWrite
    from cnequity.storage.parquet import _partition_values

    changed_files = [Path(path) for path in changed_files]
    if not changed_files and not removed_partitions:
        return None
    tree = store._layer_root(dataset) / dataset
    column = DATASETS[dataset].partition_col

    def read(paths) -> pl.DataFrame:
        frames = [pl.read_parquet(path) for path in paths if Path(path).exists()]
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    def labelled(frame: pl.DataFrame) -> pl.DataFrame:
        if column is None:
            return frame.with_columns(pl.lit("").alias("__label__"))
        labels = _partition_values(frame, column, granularity_for_dataset(dataset))
        return frame.with_columns(
            pl.concat_str(pl.lit(f"{column}="), labels.cast(pl.Utf8)).alias("__label__")
        )

    everything = read(sorted(tree.rglob("*.parquet")) if tree.is_dir() else [])
    if not everything.is_empty():
        everything = labelled(everything)
    touched = read(changed_files)
    labels = set() if touched.is_empty() else set(labelled(touched)["__label__"].to_list())

    data_root = Path(store.meta_root).parent
    roots = SimpleNamespace(
        data_root=data_root,
        meta_root=Path(store.meta_root),
        curated_root=store.curated_root,
        derived_root=store.derived_root,
        staging_root=data_root / "staging",
    )
    Path(store.meta_root).mkdir(parents=True, exist_ok=True)
    start_on_catalog(roots)
    catalog_read.close(roots)
    with LakeWrite(roots, run_id) as write:
        checkout = write.checkout(dataset)
        if removed_partitions:
            held = set() if everything.is_empty() else set(everything["__label__"].to_list())
            labels |= set(checkout.labels()) - held
        for label in sorted(labels):
            rows = (
                everything.filter(pl.col("__label__") == label).drop("__label__")
                if not everything.is_empty()
                else pl.DataFrame()
            )
            checkout.rewrite(label, rows)
        published = write.publish(
            {dataset: DatasetPublish(schema_version, contract_fingerprint, metadata)},
            message=f"{dataset} seeded by {run_id}",
        )
    catalog_read.close(roots)
    return published.get(dataset)


def publish_folders(config, *datasets: str) -> list[str]:
    """Publish what a test wrote by hand under ``curated/`` or ``derived/``; returns the datasets.

    A lake on its catalog reads only the catalog, so a fixture written as
    Parquet folders is invisible until published. Each named dataset (every
    dataset with a folder when none is named) is published with all of its
    folder's rows; the folder stays, and is never read.
    """
    import polars as pl

    from cnequity.domain.datasets import DATASETS
    from cnequity.storage.locator import logical_root

    names = datasets or tuple(sorted(DATASETS))
    published = []
    for name in names:
        folder = logical_root(config, name)
        files = sorted(folder.rglob("*.parquet")) if folder.is_dir() else []
        if not files:
            if datasets:
                raise ValueError(f"no Parquet under {folder}")
            continue
        # A folder unchanged since it was last published is already in the
        # catalog; publishing it again would also undo what a writer under
        # test published over it since.
        stamp = tuple((str(path), path.stat().st_size, path.stat().st_mtime_ns) for path in files)
        key = folder.resolve()
        if _published_folders.get(key) == stamp:
            continue
        rows = pl.concat([pl.read_parquet(path) for path in files], how="diagonal_relaxed")
        publish_rows(config, name, rows, run_id=f"seed-folder-{name}")
        _published_folders[key] = stamp
        published.append(name)
    return published


#: What each folder held when :func:`publish_folders` last published it.
_published_folders: dict = {}


def compact_into_lake(config, dataset: str, run_id: str, **kwargs) -> int:
    """Compact *run_id*'s staged *dataset* into the lake and publish it, as the compact step does.

    The merge runs in a checkout of the published partitions and the result
    is published; *kwargs* go to ``compact_dataset`` (``instruments`` goes to
    ``compact_instruments``, whose ``trade_date`` defaults to today). Returns
    the rows the merge wrote.
    """
    from datetime import date

    from cnequity.domain.contracts import contract_fingerprint, dataset_contract
    from cnequity.query import catalog_read
    from cnequity.storage.lake_record import start_on_catalog
    from cnequity.storage.lake_write import DatasetPublish, LakeWrite
    from cnequity.storage.parquet import StagingWriter, compact_dataset

    Path(config.meta_root).mkdir(parents=True, exist_ok=True)
    start_on_catalog(config)
    staged = kwargs.pop("staging_files", None) or StagingWriter(config.staging_root).list_run_files(
        dataset, run_id
    )
    catalog_read.close(config)
    with LakeWrite(config, run_id) as write:
        checkout = write.checkout_staged(dataset, staged)
        if dataset == "instruments":
            from cnequity.storage.instruments import compact_instruments

            rows, _findings = compact_instruments(
                config.staging_root,
                config.curated_root,
                run_id,
                kwargs.pop("trade_date", date.today()),
                base_root=checkout.root,
                staging_files=staged,
                out_root=checkout.layer_root,
                **kwargs,
            )
        else:
            rows = compact_dataset(
                config.staging_root,
                config.curated_root,
                dataset,
                run_id,
                base_root=checkout.root,
                staging_files=staged,
                out_root=checkout.layer_root,
                **kwargs,
            )
        if checkout.changed():
            contract = dataset_contract(dataset)
            write.publish(
                {
                    dataset: DatasetPublish(
                        int(contract["schema_version"]), contract_fingerprint(contract)
                    )
                },
                message=f"{dataset} compacted by {run_id}",
            )
    catalog_read.close(config)
    return rows


def derive_into_lake(config, dataset: str, derive, *args, run_id: str = "test-derive", **kwargs):
    """Run *derive* (a ``derive_*``/``compute_adj_factors`` function) and publish what it wrote.

    The derivation writes into a ``DerivedOutput`` checkout, as ``cne derive``
    and the finalize steps give it, and the checkout is published when it
    changed anything. Returns what *derive* returned.
    """
    from cnequity.domain.contracts import contract_fingerprint, dataset_contract
    from cnequity.query import catalog_read
    from cnequity.storage.derived_output import DerivedOutput
    from cnequity.storage.lake_record import start_on_catalog
    from cnequity.storage.lake_write import DatasetPublish

    Path(config.meta_root).mkdir(parents=True, exist_ok=True)
    start_on_catalog(config)
    catalog_read.close(config)
    with DerivedOutput(config, dataset, run_id) as output:
        result = derive(config, *args, output=output, **kwargs)
        if output.changed():
            contract = dataset_contract(dataset)
            output.lake_write.publish(
                {
                    dataset: DatasetPublish(
                        int(contract["schema_version"]), contract_fingerprint(contract)
                    )
                },
                message=f"{dataset} derived by {run_id}",
            )
    catalog_read.close(config)
    return result
