"""Remove what the generation publish layer left on a lake that moved onto its catalog.

Releases before 0.25.0 published a dataset as a generation directory under
``meta/revisions/data``, named by a pointer file and described by a receipt,
and kept a mutable copy under ``curated/`` or ``derived/``. A lake on its
catalog reads and writes none of them: its rows are the catalog's files and
its revisions the catalog's records. The ``drop-generations`` migration
deletes them.

A generation someone holds (``cne storage hold``, a case, a reference file
that cites it) stays, with its receipt, and stays readable by revision id:
it is the only copy of that revision's rows. So does anything this module
cannot account for: a generation without a receipt, a directory under
``curated/`` that is not a dataset, a dataset the catalog does not hold.
"""

from __future__ import annotations

import os
import shutil
import stat
from pathlib import Path
from typing import Any

MIGRATION_ID = "drop-generations"

_LAYERS = ("curated", "derived")
#: The generation holding revision 0, what a dataset was before its first recorded revision.
_BASELINE = "legacy"


def _tree_size(root: Path) -> tuple[int, int]:
    """Files under *root* and the bytes that go when it does (a file linked elsewhere stays)."""
    files = freed = 0
    for directory, _dirnames, names in os.walk(root, followlinks=False):
        for name in names:
            info = os.lstat(os.path.join(directory, name))
            files += 1
            if stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                freed += info.st_size
    return files, freed


def _held_revisions(meta: Path, *, refresh: bool) -> set[str]:
    """Object ids of the revisions a hold protects.

    With *refresh*, the reference roots are read again first, so a revision a
    file started citing since the holds were last derived is held too. That
    hashes every reference file (minutes on a large lake), so a preview goes
    by the holds as recorded.
    """
    from cnequity.storage.lifecycle import LifecycleStore, reference_fingerprint

    store = LifecycleStore(meta)
    registry = store.registry()
    if registry is None:
        return set()
    if refresh and (
        reference_fingerprint(registry["reference_roots"], meta_root=store.meta)
        != registry["reference_fingerprint"]
    ):
        # A reference file changed since the holds were last derived; a
        # refresh only adds holds.
        store.refresh_references()
        registry = store.registry() or registry
    return {
        key for key, holds in registry["holds"].items() if key.startswith("revision/") and holds
    }


def _uncatalogued(config, datasets: set[str]) -> set[str]:
    """Datasets among *datasets* the catalog records no current revision for."""
    from cnequity.storage.revision_records import records_for

    records = records_for(Path(config.meta_root))
    current = records.current if records is not None else {}
    return {name for name in datasets if current.get(name) is None}


def _mismatched(config) -> set[str]:
    """Datasets with a revision whose rows in the catalog are not the rows recorded for it."""
    from cnequity.config.resources import resources_for
    from cnequity.storage.catalog_bridge import lake_root
    from cnequity.storage.lake_write import verify_revisions
    from equitylake_core.catalog import Catalog

    root = lake_root(config)
    with Catalog(
        root,
        read_only=True,
        **resources_for(config, "maintenance").options(scratch=root / "tmp"),
    ) as catalog:
        return {c.dataset for c in verify_revisions(catalog) if c.status == "mismatch"}


def _plan(config, *, refresh_holds: bool) -> dict[str, Any]:
    from cnequity.domain.datasets import DATASETS
    from cnequity.storage.lifecycle import LifecycleStore
    from cnequity.storage.locator import layer_name

    meta = Path(config.meta_root)
    lake = Path(config.data_root)
    revisions = meta / "revisions"
    held = _held_revisions(meta, refresh=refresh_holds)

    generations, kept_generations = [], []
    for obj in LifecycleStore(meta).inventory():
        reasons = [r for r in obj["blocked_reasons"] if r != "missing_current_pointer"]
        if obj["revision_id"] == _BASELINE:
            # The copy of the dataset from before it had revisions: it never had a receipt.
            reasons = []
        if obj["object_id"] in held:
            reasons.append("explicit_hold")
        target = kept_generations if reasons else generations
        target.append(
            {"dataset": obj["dataset"], "revision_id": obj["revision_id"], "path": obj["path"]}
            | ({"kept_because": reasons} if reasons else {})
        )
    kept_ids = {(g["dataset"], g["revision_id"]) for g in kept_generations}

    records = []
    for folder in (
        sorted(p for p in revisions.iterdir() if p.is_dir()) if revisions.is_dir() else ()
    ):
        if folder.name == "data":
            continue
        for path in sorted(folder.glob("*.json")):
            revision_id = path.stem.partition("-")[2]
            if path.name != "current.json" and (folder.name, revision_id) in kept_ids:
                continue
            records.append(path)

    trees, strays = [], []
    for layer in _LAYERS:
        root = lake / layer
        if not root.is_dir():
            continue
        for path in sorted(root.iterdir()):
            spec = DATASETS.get(path.name)
            if path.is_dir() and not path.is_symlink() and spec and layer_name(spec) == layer:
                trees.append(path)
            else:
                strays.append(path.relative_to(lake).as_posix())
    working = [p for p in (meta / "working_trees",) if p.is_dir()]
    return {
        "generations": generations,
        "kept_generations": kept_generations,
        "records": records,
        "trees": trees,
        "strays": strays,
        "working": working,
    }


def drop_generations(config, *, apply: bool) -> dict[str, Any]:
    """Preview, or with *apply* delete, the generation layer's leftovers.

    Does nothing on a lake whose revisions the catalog does not yet record.
    A dataset the catalog records no current revision for keeps its
    directories and is listed under ``skipped_datasets``; so does, with
    *apply*, one whose rows in the catalog are not the rows recorded for it.
    """
    from cnequity.file_lock import lake_mutation_lock
    from cnequity.storage import revision_records
    from cnequity.storage.atomic import fsync_dir
    from cnequity.storage.revisions import _reject_symlink_path

    meta = Path(config.meta_root)
    report: dict[str, Any] = {"applied": apply, "rows_changed": 0}
    if not revision_records.enabled(meta):
        report["skipped"] = "the lake catalog does not record this lake's revisions yet"
        return report

    with lake_mutation_lock(meta, blocking=True):
        plan = _plan(config, refresh_holds=apply)
        datasets = {g["dataset"] for g in plan["generations"]} | {p.name for p in plan["trees"]}
        skipped = _uncatalogued(config, datasets)
        if apply and datasets:
            skipped |= _mismatched(config) & datasets
        generations = [g for g in plan["generations"] if g["dataset"] not in skipped]
        trees = [p for p in plan["trees"] if p.name not in skipped]
        records = [p for p in plan["records"] if p.parent.name not in skipped]

        files = freed = 0
        for path in [meta / g["path"] for g in generations] + trees + plan["working"]:
            count, size = _tree_size(path)
            files += count
            freed += size
        report.update(
            {
                "generations": len(generations),
                "kept_generations": len(plan["kept_generations"]),
                "kept": plan["kept_generations"][:50],
                "revision_files": len(records),
                "dataset_directories": [p.relative_to(config.data_root).as_posix() for p in trees],
                "left_in_place": plan["strays"],
                "skipped_datasets": sorted(skipped),
                "files": files + len(records),
                "freed_bytes": freed,
                "rows_changed": len(generations) + len(trees) + len(records),
            }
        )
        if not apply:
            return report

        for generation in generations:
            path = meta / generation["path"]
            relative = Path(generation["path"])
            if relative.parts[:3] != ("revisions", "data", generation["dataset"]):
                raise RuntimeError(f"refusing to delete unexpected generation path: {path}")
            _reject_symlink_path(path, label="generation")
            shutil.rmtree(path)
        for path in trees + plan["working"]:
            _reject_symlink_path(path, label="dataset directory")
            shutil.rmtree(path)
        for path in records:
            path.unlink()
        # Leave no empty shells behind; a folder that still holds something stays.
        for folder in sorted((meta / "revisions").glob("**/"), reverse=True):
            if folder.is_dir() and not any(folder.iterdir()):
                folder.rmdir()
        for layer in _LAYERS:
            root = Path(config.data_root) / layer
            if root.is_dir() and not any(root.iterdir()):
                root.rmdir()
        fsync_dir(meta)
        revision_records.forget(meta)
    return report
