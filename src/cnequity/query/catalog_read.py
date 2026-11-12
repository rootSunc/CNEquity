"""Read a dataset's committed rows from the lake catalog instead of scanning its generation.

On when the lake has a catalog; ``CNE_READ_CATALOG=0`` turns it off. The
catalog answers only when its table holds exactly the revision the read is
pinned to; in every other case the caller gets ``None`` and scans the
revision store, so the catalog can never return older data than the scan.
"""

from __future__ import annotations

import logging
import os
import threading
from datetime import date
from pathlib import Path
from typing import NamedTuple

import polars as pl

from cnequity.domain.datasets import granularity_for_dataset
from cnequity.domain.partitions import partition_value
from cnequity.storage.catalog_bridge import (
    catalog_is_current,
    keys_unique,
    lake_root,
    synced_revision,
)
from equitylake_core.catalog import ALIAS, CATALOG_FILE, Catalog, quote_identifier

logger = logging.getLogger(__name__)

ENV_SWITCH = "CNE_READ_CATALOG"

_lock = threading.Lock()
_open: dict[Path, Catalog] = {}


class CatalogRows(NamedTuple):
    frame: pl.DataFrame
    # One row per primary key: nothing left to dedupe.
    unique_keys: bool


def enabled() -> bool:
    return os.environ.get(ENV_SWITCH, "").strip().lower() not in {"0", "false", "no", "off"}


def _catalog(config) -> Catalog | None:
    root = lake_root(config).resolve()
    if not (root / CATALOG_FILE).exists():
        return None
    catalog = _open.get(root)
    if catalog is None:
        catalog = Catalog(root, read_only=True)
        # Stored timestamps are UTC; without this they come back in the machine's zone.
        catalog.execute("SET TimeZone = 'UTC'")
        _open[root] = catalog
    return catalog


def close_all() -> None:
    with _lock:
        for catalog in _open.values():
            catalog.close()
        _open.clear()


def read_published(
    config,
    dataset: str,
    *,
    partition_col: str | None,
    start: date | None = None,
    end: date | None = None,
    symbols: list[str] | None = None,
    revision_id: str | None = None,
) -> CatalogRows | None:
    """The committed rows of *dataset* in the requested range, or ``None`` to scan instead.

    Filters mean what they mean in ``scan_parquet_root``: inclusive bounds on
    the partition column, and membership in *symbols*. With *revision_id* the
    catalog answers only if it holds that revision; without, only if it holds
    the committed one.
    """
    if not enabled():
        return None
    try:
        with _lock:
            catalog = _catalog(config)
            if catalog is None:
                return None
            if revision_id is None:
                if not catalog_is_current(config, catalog, dataset):
                    return None
            else:
                synced = synced_revision(catalog, dataset)
                if synced is None or synced[0] != revision_id:
                    return None
            table = quote_identifier(dataset)
            types = {row[0]: row[1] for row in catalog.execute(f"DESCRIBE {table}").fetchall()}
            clauses: list[str] = []
            values: list[object] = []
            if partition_col and partition_col in types and (start is not None or end is not None):
                low, high = start, end
                if types[partition_col] == "VARCHAR":
                    granularity = granularity_for_dataset(dataset)
                    low = partition_value(start, granularity) if start is not None else None
                    high = partition_value(end, granularity) if end is not None else None
                column = quote_identifier(partition_col)
                if low is not None:
                    clauses.append(f"{column} >= ?")
                    values.append(low)
                if high is not None:
                    clauses.append(f"{column} <= ?")
                    values.append(high)
            if symbols and "symbol" in types:
                clauses.append("symbol IN (" + ", ".join("?" for _ in symbols) + ")")
                values.extend(symbols)
            where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
            unique = bool(keys_unique(catalog, dataset))
            relation = catalog.relation(f"SELECT * FROM {ALIAS}.{table}{where}", values)
            return CatalogRows(relation.pl(), unique)
    except Exception as exc:  # noqa: BLE001 - the scan is always a correct answer
        logger.warning("catalog read of %s failed, scanning instead: %s", dataset, exc)
        return None
