"""Bring the revision store's committed datasets into the lake catalog, and prove they match.

The catalog (``equitylake_core.catalog``) is where datasets will be published.
Until publishing moves there, this module copies each dataset's committed
generation into it and compares the two copies row for row. It reads the
revision store and never writes to it; the catalog lives in its own
directory beside ``curated/`` and ``meta/``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from cnequity.domain.datasets import DATASETS
from cnequity.domain.schemas import PRIMARY_KEYS
from cnequity.storage.revisions import RevisionStore
from equitylake_core.catalog import Catalog, ImportResult, quote_identifier, quote_literal

LAKE_DIR = "lake"

# Below this many rows a table is one file; partitioning it would only add tiny files.
PARTITION_MIN_ROWS = 5_000_000


@dataclass(frozen=True)
class Comparison:
    dataset: str
    revision_rows: int
    catalog_rows: int
    same_content: bool

    @property
    def matches(self) -> bool:
        return self.revision_rows == self.catalog_rows and self.same_content


def lake_root(config) -> Path:
    return Path(config.data_root) / LAKE_DIR


def committed_source(config, dataset: str) -> tuple[str, str] | None:
    """The Parquet glob and revision id of *dataset*'s committed generation, if it has one."""
    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root, create=False)
    pointer = store.current_pointer(dataset)
    root = store.current_root(dataset)
    if pointer is None or root is None or not any(root.rglob("*.parquet")):
        return None
    return (root / "**" / "*.parquet").as_posix(), str(pointer.get("revision_id", ""))


def _scan(glob: str) -> str:
    return f"read_parquet({quote_literal(glob)}, union_by_name = true, hive_partitioning = false)"


def _partition_column(dataset: str, glob: str) -> str | None:
    column = DATASETS[dataset].partition_col
    if not column:
        return None
    probe = duckdb.connect()
    try:
        described = {
            row[0]: row[1]
            for row in probe.execute(f"DESCRIBE SELECT * FROM {_scan(glob)}").fetchall()
        }
        if not described.get(column, "").startswith(("DATE", "TIMESTAMP")):
            return None
        rows = probe.execute(f"SELECT count(*) FROM {_scan(glob)}").fetchone()[0]
    finally:
        probe.close()
    return column if rows >= PARTITION_MIN_ROWS else None


def import_dataset(config, catalog: Catalog, dataset: str) -> ImportResult | None:
    """Copy *dataset*'s committed generation into the catalog as one snapshot."""
    source = committed_source(config, dataset)
    if source is None:
        return None
    glob, revision_id = source
    return catalog.import_parquet(
        dataset,
        glob,
        partition_by=_partition_column(dataset, glob),
        sort_by=tuple(PRIMARY_KEYS.get(dataset, ())),
        message=f"import {dataset} from revision {revision_id}",
    )


def compare_dataset(config, catalog: Catalog, dataset: str) -> Comparison | None:
    """Compare the catalog table with the committed generation, whole rows at a time.

    Each side is reduced to its row count and two order-independent digests of
    every row (an XOR and a sum of row hashes), so the comparison does not
    depend on file layout or row order, and a pair of duplicated rows cannot
    cancel out.
    """
    source = committed_source(config, dataset)
    if source is None or not catalog.has_table(dataset):
        return None
    table = quote_identifier(dataset)
    columns = [row[0] for row in catalog.execute(f"DESCRIBE {table}").fetchall()]
    row = "hash(" + ", ".join(quote_identifier(column) for column in columns) + ")"
    digest = f"SELECT count(*), bit_xor({row}), sum({row}::HUGEINT) FROM "
    old = catalog.execute(digest + _scan(source[0])).fetchone()
    new = catalog.execute(digest + table).fetchone()
    return Comparison(
        dataset=dataset,
        revision_rows=old[0],
        catalog_rows=new[0],
        same_content=old[1:] == new[1:],
    )
