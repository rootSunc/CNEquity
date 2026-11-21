"""Lay a catalog table out the way the lake's writers keep one: a file set per partition.

A lake that moved onto its catalog before 0.25.0 had its tables written by
the catalog itself: one file for a whole table, or a file per month whatever
the dataset's own partitions are. Publishing a partition there drops the
files that hold it, so every other partition in those files has to be written
again. This module does that once for a whole table, ahead of time: each
partition's rows are written out as the writers lay a partition out, the
files are registered, and the catalog's own files are dropped, in one
transaction that changes no row.

Rows are compared before and after by digest inside that transaction; a
difference rolls it back. The dataset's revision does not change: the same
rows are published, in other files.
"""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path

from cnequity.config.resources import resources_for
from cnequity.file_lock import lake_mutation_lock
from cnequity.storage.catalog_bridge import _laid_out_files, _listed_files, lake_root
from cnequity.storage.lake_write import Checkout, _Reader, lake_file_name
from equitylake_core.catalog import Catalog
from equitylake_core.write import Write, rows_digest


class LayoutError(RuntimeError):
    """A table could not be laid out; it is as it was."""


def needs_layout(catalog: Catalog, dataset: str) -> bool:
    """Whether *dataset*'s table holds a file the lake's writers did not lay out."""
    return catalog.has_table(dataset) and None in _laid_out_files(catalog, dataset)


def datasets_to_lay_out(config) -> list[str]:
    from cnequity.domain.datasets import DATASETS
    from equitylake_core.catalog import CATALOG_FILE

    root = lake_root(config)
    if not (root / CATALOG_FILE).exists():
        return []
    with Catalog(root, read_only=True) as catalog:
        return [name for name in sorted(DATASETS) if needs_layout(catalog, name)]


def lay_out_dataset(config, catalog: Catalog, dataset: str, *, attempts: int = 3) -> dict | None:
    """Give *dataset*'s table one file set per partition; ``None`` when it already has.

    The partitions are written out without the lake lock. The lock is taken
    only to swap the files, and only if the table still holds the files that
    were read; a publish in between starts the work again.
    """
    if not needs_layout(catalog, dataset):
        return None
    scratch = lake_root(config) / "tmp" / f"layout-{uuid.uuid4().hex[:8]}"
    try:
        for _ in range(attempts):
            before = sorted(_listed_files(catalog, dataset))
            shutil.rmtree(scratch, ignore_errors=True)
            checkout = Checkout(_Reader(catalog, scratch), dataset, root=scratch / dataset)
            written: list[tuple[str, Path]] = []
            for label in _write_out_all(catalog, checkout):
                folder = checkout._target(label)
                written.extend((label, path) for path in sorted(folder.rglob("*.parquet")))
            with lake_mutation_lock(config.meta_root, blocking=True):
                if sorted(_listed_files(catalog, dataset)) != before:
                    continue  # published meanwhile: read it again
                return _swap_in(catalog, dataset, checkout, written, before)
        raise LayoutError(f"{dataset}: kept being published while it was laid out; run again")
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


#: Day partitions smaller than this on average are read a month at a time.
_MONTHLY_BELOW_ROWS = 500_000


def _write_out_all(catalog: Catalog, checkout: Checkout) -> list[str]:
    """Write every partition of the checkout's dataset out; returns their labels.

    A partition at a time, which keeps one partition in memory. Asking the
    catalog for each of a few thousand small day partitions costs more than
    reading them, so those are read a month at a time and split here.
    """
    from cnequity.domain.datasets import granularity_for_dataset
    from cnequity.storage.parquet import _partition_values, emit_partition, merge_group_keys
    from equitylake_core.catalog import quote_identifier

    dataset = checkout.dataset
    column = checkout.spec.partition_col
    labels = checkout.labels()
    name = quote_identifier(dataset)
    rows = catalog.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
    monthly = (
        bool(column)
        and granularity_for_dataset(dataset) == "day"
        and catalog.column_types(dataset).get(column) == "DATE"
        and len(labels) > 31
        and rows / len(labels) < _MONTHLY_BELOW_ROWS
    )
    if not monthly:
        for label in labels:
            checkout._write_out(label)
        return labels
    col = quote_identifier(column)
    keys = merge_group_keys(dataset)
    order = (" ORDER BY " + ", ".join(quote_identifier(key) for key in keys)) if keys else ""
    months = sorted({label.partition("=")[2][:7] for label in labels})
    for month in months:
        frame = catalog.relation(
            f"SELECT * FROM {name} WHERE {col} >= CAST('{month}-01' AS DATE) "
            f"AND {col} < CAST('{month}-01' AS DATE) + INTERVAL 1 MONTH{order}"
        ).pl()
        values = _partition_values(frame, column, "day")
        for (value,), group in (
            frame.with_columns(values.alias("__label__"))
            .partition_by("__label__", as_dict=True, maintain_order=True)
            .items()
        ):
            target = checkout._target(f"{column}={value}")
            target.mkdir(parents=True, exist_ok=True)
            emit_partition(
                group.drop("__label__"),
                dataset,
                affected=[],
                existing=[],
                read_dir=None,
                out_dir=target,
            )
    return labels


def _swap_in(
    catalog: Catalog,
    dataset: str,
    checkout: Checkout,
    written: list[tuple[str, Path]],
    old: list[str],
) -> dict:
    digest = rows_digest(catalog, dataset)
    # Registered files are refused by a table the catalog partitions itself.
    catalog.unpartition(dataset)

    def check_rows(rewritten: Catalog) -> None:
        after = rows_digest(rewritten, dataset)
        if after.digest != digest.digest:
            raise LayoutError(
                f"{dataset}: the rows written out ({after.rows}) are not the rows "
                f"the table held ({digest.rows}); nothing was changed"
            )

    with Write(catalog) as write:
        for label, source in written:
            target = checkout.layout_dir(label, catalog) / lake_file_name(source.name)
            conform_to = None if catalog.file_fits(dataset, source) else dataset
            write.place(source, target, conform_to=conform_to)
        write.swap(dataset, add=write.placed, remove=old)
        write.check("same rows after layout", check_rows)
        write.commit(message=f"lay out {dataset}")
    return {
        "dataset": dataset,
        "rows": digest.rows,
        "files_before": len(old),
        "files": len(write.placed),
    }


def lay_out_all(config, progress=None) -> list[dict]:
    """Lay out every table of *config*'s lake that needs it.

    *progress* is called with each such table's name before it is laid out.
    """
    root = lake_root(config)
    done = []
    with Catalog(
        root, **resources_for(config, "maintenance").options(scratch=root / "tmp")
    ) as catalog:
        from cnequity.domain.datasets import DATASETS

        for dataset in sorted(DATASETS):
            if progress is not None and needs_layout(catalog, dataset):
                progress(dataset)
            report = lay_out_dataset(config, catalog, dataset)
            if report is not None:
                done.append(report)
    return done
