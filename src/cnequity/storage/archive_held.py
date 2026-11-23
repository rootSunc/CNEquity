"""Move the held revisions from before 0.25.0 into the lake catalog.

``drop-generations`` deletes the generation directories the catalog replaced
but keeps one a hold protects (``cne storage hold``, a case's dependency):
it is the only copy of that revision's rows, still read by revision id
(``load(revision=...)``, a case's pinned reads). Releases after 0.25.x no
longer read generation directories at all, so the ``archive-held-revisions``
migration moves each into a catalog table of its own
(:func:`~cnequity.storage.catalog_bridge.archive_generation`): its files are
linked, not copied, its rows are compared with the generation's, and only
then are the directory and its receipt file deleted. Reads by revision id
then come from the catalog like every other read.

Catalog retention drops an archive table once no hold protects its revision.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

MIGRATION_ID = "archive-held-revisions"


def _generations(meta: Path) -> list[tuple[str, str, Path]]:
    """``(dataset, revision id, directory)`` of every generation directory left on disk."""
    data = meta / "revisions" / "data"
    if not data.is_dir():
        return []
    return [
        (dataset.name, generation.name, generation)
        for dataset in sorted(data.iterdir())
        if dataset.is_dir() and not dataset.is_symlink()
        for generation in sorted(dataset.iterdir())
        if generation.is_dir()
        and not generation.is_symlink()
        and next(generation.rglob("*.parquet"), None) is not None
    ]


def archive_held_revisions(config, *, apply: bool) -> dict[str, Any]:
    """Preview, or with *apply* perform, the move of held generations into the catalog."""
    from cnequity.config.resources import resources_for
    from cnequity.file_lock import lake_mutation_lock
    from cnequity.storage import revision_records
    from cnequity.storage.catalog_bridge import archive_generation, archived_revision, lake_root
    from cnequity.storage.lifecycle import LifecycleStore
    from cnequity.storage.revisions import _reject_symlink_path
    from equitylake_core.catalog import Catalog

    meta = Path(config.meta_root)
    report: dict[str, Any] = {"applied": apply, "rows_changed": 0}
    if not revision_records.enabled(meta):
        report["skipped"] = "the lake catalog does not record this lake's revisions yet"
        return report
    found = _generations(meta)
    registry = LifecycleStore(meta).retention_registry() if found else None
    held = set(registry["holds"]) if registry else set()
    chosen = [g for g in found if f"revision/{g[0]}/{g[1]}" in held]
    report.update(
        {
            "revisions": [f"{dataset}/{revision_id}" for dataset, revision_id, _ in chosen],
            # Left by drop-generations because the catalog does not record their
            # dataset, or not held: neither is this migration's to move.
            "left_in_place": [
                f"{dataset}/{revision_id}"
                for dataset, revision_id, _ in found
                if f"revision/{dataset}/{revision_id}" not in held
            ],
            "rows_changed": len(chosen),
        }
    )
    if not apply or not chosen:
        return report

    root = lake_root(config)
    moved: dict[str, int] = {}
    with lake_mutation_lock(meta, blocking=True):
        with Catalog(
            root, **resources_for(config, "maintenance").options(scratch=root / "tmp")
        ) as catalog:
            for dataset, revision_id, directory in chosen:
                if archived_revision(catalog, dataset, revision_id) is None:
                    moved[f"{dataset}/{revision_id}"] = archive_generation(
                        catalog, dataset, revision_id, directory
                    )
        # Only after every archive committed: a failure above leaves each
        # generation that was not archived where it was, readable as before.
        for dataset, revision_id, directory in chosen:
            _reject_symlink_path(directory, label="generation")
            shutil.rmtree(directory)
            for receipt in (meta / "revisions" / dataset).glob(f"*-{revision_id}.json"):
                receipt.unlink()
        for folder in sorted((meta / "revisions").glob("**/"), reverse=True):
            if folder.is_dir() and not any(folder.iterdir()):
                folder.rmdir()
        revision_records.forget(meta)
    report["rows"] = moved
    return report
