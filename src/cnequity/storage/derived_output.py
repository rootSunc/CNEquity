"""Where a derivation writes its dataset: a checkout of the lake catalog.

A derivation computes rows and writes whole partitions, or merges into the
ones already there. A :class:`DerivedOutput` gives it one shape for that:

* ``partition(label)`` is a directory holding that partition's published
  rows, to merge into in place;
* ``write(label, rows, name)`` replaces a partition outright;
* ``remove(label)`` deletes one;
* ``layer_root`` goes where a writer expects ``derived_root``.

Opened with a run id by the step that publishes the result
(``steps.finalize``), which reads :meth:`changed_files` afterwards. Without a
run id it is a plain directory under the dataset's layer root that nothing
publishes: what a derivation called on its own writes.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import polars as pl

from cnequity.domain.datasets import DATASETS
from cnequity.storage.locator import layer_root
from cnequity.storage.revisions import RevisionStore, sha256_file


def _identity(root: Path) -> dict[str, tuple[int, str]]:
    """Every file under *root* with its size and digest, without following links."""
    if not root.exists():
        return {}
    return {
        path.relative_to(root).as_posix(): (path.stat().st_size, sha256_file(path))
        for path in RevisionStore._walk_files(root)
    }


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
    def __init__(self, config, dataset: str, run_id: str | None = None):
        self.config, self.dataset, self.run_id = config, dataset, run_id
        self.spec = DATASETS[dataset]
        self.layer_root = Path(layer_root(config, dataset))
        #: The catalog write and the checkout, when the lake publishes into its catalog.
        self.lake_write = None
        self.checkout = None
        self._before: dict[str, tuple[int, str]] | None = None
        self._labels: set[str] | None = None

    @property
    def root(self) -> Path:
        return self.layer_root / self.dataset

    def __enter__(self) -> DerivedOutput:
        if self.run_id is None:
            self._before = _identity(self.root)
            return self
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
        if self.checkout is not None:
            return self.checkout.partition(label)
        target = self.root / label
        target.mkdir(parents=True, exist_ok=True)
        return target

    def rows(self, label: str) -> pl.DataFrame | None:
        """The rows partition *label* holds, read without checking it out; ``None`` if none."""
        self._label(label)
        if self.checkout is not None:
            if label in self.checkout._checked:
                files = sorted((self.checkout.root / label).rglob("*.parquet"))
            else:
                stored = self.checkout.rows(label)
                return None if stored.is_empty() else stored
        else:
            files = sorted((self.root / label).rglob("*.parquet"))
        if not files:
            return None
        return pl.concat([pl.read_parquet(path) for path in files], how="diagonal_relaxed")

    def labels(self) -> list[str]:
        """Every published partition, as ``<column>=<value>``."""
        if self.checkout is not None:
            return self.checkout.labels()
        if not self.root.is_dir():
            return []
        return sorted(
            path.name
            for path in self.root.iterdir()
            if path.is_dir() and any(path.rglob("*.parquet"))
        )

    def write(self, label: str, rows: pl.DataFrame, name: str) -> bool:
        """Replace *label* and report whether anything was written."""
        value = self._label(label)
        if self.checkout is not None:
            # A rebuild that computes what is already published changes nothing:
            # only the time it was computed differs, and that is not a restatement.
            if _same_facts(self.checkout.rows(label), rows):
                return False
            self.checkout.rewrite(label, rows)
            return True
        from cnequity.storage.parquet import CuratedWriter

        CuratedWriter(self.layer_root).write_partition(
            self.dataset, self.spec.partition_col, value, rows, name
        )
        return True

    def exists(self, label: str) -> bool:
        """Whether partition *label* is published."""
        self._label(label)
        if self.checkout is not None:
            if self._labels is None:
                self._labels = set(self.checkout.labels())
            return label in self._labels
        return (self.root / label).exists()

    def remove(self, label: str) -> None:
        self._label(label)
        if self.checkout is not None:
            self.checkout.rewrite(label, pl.DataFrame())
            return
        shutil.rmtree(self.root / label, ignore_errors=True)

    def changed_files(self) -> list[Path]:
        """The files this derivation wrote or changed."""
        if self.checkout is not None:
            return [path for files in self.checkout.changed().values() for path in files]
        after = _identity(self.root)
        before = self._before or {}
        return [self.root / name for name, found in after.items() if before.get(name) != found]

    def changed(self) -> bool:
        """Whether there is anything to publish, a removed partition included."""
        if self.checkout is not None:
            return bool(self.checkout.changed())
        return bool(self.changed_files())
