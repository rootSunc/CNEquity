"""Where a derivation writes its dataset: a checkout of the lake catalog.

A derivation computes rows and writes whole partitions, or merges into the
ones already there. A :class:`DerivedOutput` gives it one shape for that:

* ``partition(label)`` is a directory holding that partition's published
  rows, to merge into in place;
* ``write(label, rows, name)`` replaces a partition outright;
* ``remove(label)`` deletes one;
* ``layer_root`` goes where a writer expects ``derived_root``.

Opened with the run id of the step that publishes the result
(``steps.finalize``, ``cne derive``), which reads :meth:`changed_files`
afterwards. There is no other kind: a lake on its catalog reads nothing
else, so a derivation written anywhere but a checkout would be lost.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from cnequity.domain.datasets import DATASETS


def _same_facts(stored: pl.DataFrame, rows: pl.DataFrame) -> bool:
    """Whether two frames hold the same rows, apart from when they were fetched."""
    columns = sorted(set(rows.columns) - {"fetched_at"})
    if stored.height != rows.height or sorted(set(stored.columns) - {"fetched_at"}) != columns:
        return False
    try:
        left = stored.select(columns).sort(columns)
        right = rows.select(columns).cast(dict(left.schema)).sort(columns)
    except pl.exceptions.PolarsError:
        return False
    return left.equals(right)


class DerivedOutput:
    def __init__(self, config, dataset: str, run_id: str):
        self.config, self.dataset, self.run_id = config, dataset, run_id
        self.spec = DATASETS[dataset]
        #: The catalog write and the checkout, set on entering.
        self.lake_write = None
        self.checkout = None
        self.layer_root: Path | None = None
        self._labels: set[str] | None = None

    @property
    def root(self) -> Path:
        return self._checkout.root

    @property
    def _checkout(self):
        if self.checkout is None:
            raise RuntimeError(
                f"{self.dataset}: a DerivedOutput is written inside its `with` block"
            )
        return self.checkout

    def __enter__(self) -> DerivedOutput:
        from cnequity.storage.catalog_migration import require_catalog_lake
        from cnequity.storage.lake_write import LakeWrite

        require_catalog_lake(self.config)
        self.lake_write = LakeWrite(self.config, self.run_id)
        self.checkout = self.lake_write.checkout(self.dataset)
        self.layer_root = self.checkout.layer_root
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.lake_write is not None:
            self.lake_write.close(keep=exc_type is not None)

    def _label(self, label: str) -> str:
        column, _, value = label.partition("=")
        if column != self.spec.partition_col or not value:
            raise ValueError(
                f"{self.dataset}: {label!r} is not a {self.spec.partition_col} partition"
            )
        return value

    def partition(self, label: str) -> Path:
        """The directory of partition *label*, holding its published rows."""
        self._label(label)
        return self._checkout.partition(label)

    def rows(self, label: str) -> pl.DataFrame | None:
        """The rows partition *label* holds, read without checking it out; ``None`` if none."""
        self._label(label)
        checkout = self._checkout
        if label not in checkout._checked:
            stored = checkout.rows(label)
            return None if stored.is_empty() else stored
        files = sorted((checkout.root / label).rglob("*.parquet"))
        if not files:
            return None
        return pl.concat([pl.read_parquet(path) for path in files], how="diagonal_relaxed")

    def labels(self) -> list[str]:
        """Every published partition, as ``<column>=<value>``."""
        return self._checkout.labels()

    def write(self, label: str, rows: pl.DataFrame, name: str) -> bool:
        """Replace *label* and report whether anything was written.

        *name* is kept for callers that named the partition file; a checkout
        lays out a partition's files itself.
        """
        self._label(label)
        # A rebuild that computes what is already published changes nothing:
        # only the time it was computed differs, and that is not a restatement.
        if _same_facts(self._checkout.rows(label), rows):
            return False
        self._checkout.rewrite(label, rows)
        return True

    def exists(self, label: str) -> bool:
        """Whether partition *label* is published."""
        self._label(label)
        if self._labels is None:
            self._labels = set(self._checkout.labels())
        return label in self._labels

    def remove(self, label: str) -> None:
        self._label(label)
        self._checkout.rewrite(label, pl.DataFrame())

    def changed_files(self) -> list[Path]:
        """The files this derivation wrote or changed."""
        return [path for files in self._checkout.changed().values() for path in files]

    def changed(self) -> bool:
        """Whether there is anything to publish, a removed partition included."""
        return bool(self._checkout.changed())
