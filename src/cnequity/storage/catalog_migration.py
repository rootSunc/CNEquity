"""Move an existing lake onto the lake catalog.

The migration ``lake-catalog`` (``cne migrate``), owed by every lake built
before 0.25.0. It copies every published dataset into the catalog, proves each copy holds
the same rows as the generation it came from, imports every revision's
receipt, and then records in ``meta/storage.json`` that the lake publishes into
its catalog and keeps its revisions there. The copy is
the generation's own files, linked into the lake and registered: nothing is
written again. A lake already on a catalog an earlier release wrote gets its
tables laid out the same way (``storage.catalog_layout``). From then on
each dataset moves over at its next publish; the generations already written
stay where they are, readable, until the old publishing layer is removed.

The copy and the comparison read immutable generations, so they run without
the lake lock and other runs keep publishing meanwhile. The switch is made
under the lock, after catching up on whatever those runs published and
comparing it too.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path

from cnequity.config.resources import resources_for
from cnequity.domain.datasets import DATASETS
from cnequity.file_lock import lake_mutation_lock
from cnequity.meta_layout import STORAGE_RECORD
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.catalog_bridge import (
    catalog_is_current,
    committed_source,
    compare_dataset,
    lake_root,
    sync_all,
)
from cnequity.storage.lake_record import LakeMigrationRequired
from cnequity.storage.revisions import STORAGE_CATALOG, RevisionStore
from equitylake_core.catalog import CATALOG_FILE, Catalog

_UPGRADE_IN_PROGRESS: ContextVar[bool] = ContextVar("cne_upgrade_in_progress", default=False)


@contextmanager
def upgrade_in_progress():
    """Allow mid-upgrade repair migrations to write while `cne upgrade` is running."""
    token = _UPGRADE_IN_PROGRESS.set(True)
    try:
        yield
    finally:
        _UPGRADE_IN_PROGRESS.reset(token)


class CatalogMigrationError(RuntimeError):
    """The lake was not switched; the catalog may hold copies, which a rerun reuses."""


def require_catalog_lake(config) -> None:
    """Refuse to write a lake that still publishes generations.

    Every writer publishes into the lake catalog. A lake built by an earlier
    release has to be moved there once, by the ``lake-catalog`` migration,
    which copies and verifies what it has published; until then nothing
    writes to it. A lake that holds nothing yet simply starts on the catalog.
    """
    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root, create=False)
    if store.publishes_to_catalog():
        from cnequity.storage.revision_records import enabled

        if not enabled(config.meta_root):
            raise LakeMigrationRequired(
                "this lake keeps revision receipts its catalog does not record yet, and the catalog "
                "is where a publish records its revision; run `cne upgrade` to see what importing "
                "them does and `cne upgrade --apply` to do it, then run this again"
            )
        if not _UPGRADE_IN_PROGRESS.get():
            from cnequity.storage.lake_record import _parse, read_lake

            record = read_lake(config)
            created = record.get("created_with")
            done = record.get("migrations") or {}
            started_pre_025_upgrade = (
                created is not None and _parse(created) < _parse("0.25.0")
            ) or (created is None and "lake-catalog" in done)
            required = {"attempts-ledger", "meta-layout"}
            missing = sorted(required - set(done))
            if started_pre_025_upgrade and missing:
                raise LakeMigrationRequired(
                    "this lake's 0.25.0 upgrade did not finish "
                    f"(missing {', '.join(missing)}); run `cne upgrade --apply` "
                    "to complete it before writing to the lake"
                )
        return
    if not _holds_data(config):
        from cnequity.storage.lake_record import start_on_catalog

        Path(config.meta_root).mkdir(parents=True, exist_ok=True)
        start_on_catalog(config)
        return
    raise LakeMigrationRequired(
        "this lake was built before 0.25.0 and has to be upgraded once before anything "
        "writes to it; run `cne upgrade` to see what that does and `cne upgrade --apply` "
        "to do it, then run this again (reading the lake needs no upgrade)"
    )


def _holds_data(config) -> bool:
    if any((Path(config.meta_root) / "revisions").glob("*/current.json")):
        return True
    return any(
        next(Path(layer).rglob("*.parquet"), None) is not None
        for layer in (config.curated_root, config.derived_root)
        if Path(layer).is_dir()
    )


def _published(config) -> list[str]:
    return [name for name in sorted(DATASETS) if committed_source(config, name) is not None]


def _behind(config, datasets: list[str]) -> list[str]:
    """Datasets whose published revision the catalog does not hold."""
    root = lake_root(config)
    if not (root / CATALOG_FILE).exists():
        return list(datasets)
    with Catalog(root, read_only=True) as catalog:
        return [name for name in datasets if not catalog_is_current(config, catalog, name)]


def _mismatched(config, catalog: Catalog, datasets: list[str], progress=None) -> list[str]:
    bad = []
    for name in datasets:
        if progress is not None:
            progress(name)
        comparison = compare_dataset(config, catalog, name)
        if comparison is None or not comparison.matches:
            bad.append(name)
    return bad


def _record_revisions(config) -> None:
    """Make the catalog the record of the lake's revisions, as it is on a new lake.

    Every receipt is imported and checked against the pointer files; only then
    does ``storage.json`` say the catalog records them, which is what lets a
    writer publish. Nothing writes in between: a lake that publishes into its
    catalog but does not record its revisions there is refused.
    """
    from cnequity.storage.revision_records import import_receipts

    import_receipts(config, apply=True)
    write_json_atomic(
        Path(config.meta_root) / STORAGE_RECORD,
        {"publish": STORAGE_CATALOG, "revisions": STORAGE_CATALOG},
        indent=2,
    )


def migrate_to_catalog(config, *, apply: bool, progress=None) -> dict:
    """Preview, or with *apply* perform, the move of *config*'s lake onto its catalog.

    *progress*, when given, is called with a stage (``copy``, ``verify``,
    ``lay_out``) and a dataset name as the work reaches each one.
    """

    def tell(stage: str):
        return None if progress is None else (lambda dataset: progress(stage, dataset))

    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root, create=False)
    datasets = _published(config)
    from cnequity.storage.catalog_layout import datasets_to_lay_out, lay_out_all

    behind = _behind(config, datasets)
    switched = store.publishes_to_catalog()
    # Tables an earlier release had the catalog write itself: laid out once,
    # so that a publish never has to write a partition it did not change.
    to_lay_out = datasets_to_lay_out(config)
    report: dict = {
        "datasets": len(datasets),
        "to_copy": behind,
        "to_lay_out": to_lay_out,
        "switched": switched,
        # What `cne migrate` counts: datasets to copy or lay out, and the switch itself.
        "rows_changed": len(behind) + len(to_lay_out) + (0 if switched else 1),
    }
    if not apply or not report["rows_changed"]:
        return report
    if switched and not behind:
        report["laid_out"] = [item["dataset"] for item in lay_out_all(config, tell("lay_out"))]
        return report

    root = lake_root(config)
    with Catalog(
        root, create=True, **resources_for(config, "maintenance").options(scratch=root / "tmp")
    ) as catalog:
        copied = [
            r.dataset for r in sync_all(config, catalog, tell("copy")) if r.action != "current"
        ]
        # Every dataset is compared, not only the ones just copied: a catalog
        # built earlier is trusted with the lake's publishes from here on.
        bad = _mismatched(config, catalog, _published(config), tell("verify"))
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
                Path(config.meta_root) / STORAGE_RECORD, {"publish": STORAGE_CATALOG}, indent=2
            )
    _record_revisions(config)
    report.update(copied=sorted(set(copied) | set(late)), verified=len(current), switched=True)
    report["laid_out"] = [item["dataset"] for item in lay_out_all(config, tell("lay_out"))]
    return report
