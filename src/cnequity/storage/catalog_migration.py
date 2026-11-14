"""Move an existing lake onto the lake catalog.

The opt-in migration ``lake-catalog`` (``cne migrate --only lake-catalog``).
It copies every published dataset into the catalog, proves each copy holds
the same rows as the generation it came from, and then records in
``meta/storage.json`` that the lake publishes into its catalog. From then on
each dataset moves over at its next publish; the generations already written
stay where they are, readable, until the old publishing layer is removed.

The copy and the comparison read immutable generations, so they run without
the lake lock and other runs keep publishing meanwhile. The switch is made
under the lock, after catching up on whatever those runs published and
comparing it too.
"""

from __future__ import annotations

from pathlib import Path

from cnequity.domain.datasets import DATASETS
from cnequity.file_lock import lake_mutation_lock
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.catalog_bridge import (
    catalog_is_current,
    committed_source,
    compare_dataset,
    lake_root,
    sync_all,
)
from cnequity.storage.revisions import STORAGE_CATALOG, RevisionStore
from equitylake_core.catalog import CATALOG_FILE, Catalog


class CatalogMigrationError(RuntimeError):
    """The lake was not switched; the catalog may hold copies, which a rerun reuses."""


def _published(config) -> list[str]:
    return [name for name in sorted(DATASETS) if committed_source(config, name) is not None]


def _behind(config, datasets: list[str]) -> list[str]:
    """Datasets whose published revision the catalog does not hold."""
    root = lake_root(config)
    if not (root / CATALOG_FILE).exists():
        return list(datasets)
    with Catalog(root, read_only=True) as catalog:
        return [name for name in datasets if not catalog_is_current(config, catalog, name)]


def _mismatched(config, catalog: Catalog, datasets: list[str]) -> list[str]:
    bad = []
    for name in datasets:
        comparison = compare_dataset(config, catalog, name)
        if comparison is None or not comparison.matches:
            bad.append(name)
    return bad


def migrate_to_catalog(config, *, apply: bool) -> dict:
    """Preview, or with *apply* perform, the move of *config*'s lake onto its catalog."""
    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root, create=False)
    gated = sorted(spec.name for spec in config.failover_datasets if spec.revision_gate)
    if gated:
        raise CatalogMigrationError(
            "catalog publishing does not support revision_gate yet; "
            f"remove it from: {', '.join(gated)}"
        )
    datasets = _published(config)
    behind = _behind(config, datasets)
    switched = store.publishes_to_catalog()
    report: dict = {
        "datasets": len(datasets),
        "to_copy": behind,
        "switched": switched,
        # What `cne migrate` counts: datasets still to copy, and the switch itself.
        "rows_changed": len(behind) + (0 if switched else 1),
    }
    if not apply or not report["rows_changed"]:
        return report

    root = lake_root(config)
    with Catalog(root, create=True, memory_limit="8GB", temp_directory=root / "tmp") as catalog:
        copied = [r.dataset for r in sync_all(config, catalog) if r.action != "current"]
        # Every dataset is compared, not only the ones just copied: a catalog
        # built earlier is trusted with the lake's publishes from here on.
        bad = _mismatched(config, catalog, _published(config))
        if bad:
            raise CatalogMigrationError(
                f"the catalog copy differs from the published rows of: {', '.join(bad)}"
            )
        with lake_mutation_lock(config.meta_root, blocking=True):
            late = [r.dataset for r in sync_all(config, catalog) if r.action != "current"]
            bad = _mismatched(config, catalog, late)
            current = _published(config)
            bad += [name for name in current if not catalog_is_current(config, catalog, name)]
            if bad:
                raise CatalogMigrationError(
                    f"the catalog copy differs from the published rows of: "
                    f"{', '.join(sorted(set(bad)))}"
                )
            write_json_atomic(
                Path(config.meta_root) / "storage.json", {"publish": STORAGE_CATALOG}, indent=2
            )
    report.update(copied=sorted(set(copied) | set(late)), verified=len(current), switched=True)
    return report
