"""Release the holds an earlier release derived from revision ids found in files.

Releases before 0.25.0 scanned every text file under ``meta/`` and the
operator's registered notes for 32-hex revision ids and held each one they
cited. A scan only ever added holds, so a snapshot manifest, a cache, a
validation report or an audit note that once quoted an id kept that revision,
and every file a lake-wide snapshot shared with it, for good. Holds are now
placed on purpose (``cne storage hold``, a case's dependency). The
``explicit-holds`` migration releases the derived ones, records each release
in the registry, and drops the registered reference roots.

It only removes protection. What then goes is deleted by catalog retention
(after its grace period) and the ``drop-generations`` migration, and cannot be
recovered: the preview lists every revision left without a hold.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

MIGRATION_ID = "explicit-holds"

#: Revisions listed by name in a report; the counts cover all of them.
_LISTED = 200


def _source_group(source: str, roots: list[Path]) -> str:
    """The folder a citing file lay in, two levels below its reference root."""
    path = Path(source)
    for root in roots:
        if path.is_relative_to(root):
            parts = path.relative_to(root).parts
            return "/".join((root.name, *parts[:1]))
    return path.parent.name or "?"


def _generation_bytes(meta: Path, object_ids: list[str]) -> int:
    """Bytes ``drop-generations`` would free for the generations of *object_ids*."""
    from cnequity.storage.drop_generations import _tree_size

    freed = 0
    for object_id in object_ids:
        _kind, dataset, revision_id = object_id.split("/", 2)
        folder = meta / "revisions" / "data" / dataset / revision_id
        if folder.is_dir():
            freed += _tree_size(folder)[1]
    return freed


def _catalog_bytes(config, held: set[str]) -> int | None:
    """Bytes of lake files catalog retention would free with only *held* protected."""
    from cnequity.config.retention import revision_keep, revision_keep_days
    from cnequity.storage.catalog_bridge import _bytes_freed, lake_root, plan_retention
    from equitylake_core.catalog import CATALOG_FILE, Catalog

    root = lake_root(config)
    keep = revision_keep(config)
    if keep == 0 or not (root / CATALOG_FILE).is_file():
        return None
    with Catalog(root, read_only=True) as catalog:
        plan = plan_retention(catalog, keep=keep, held=held, keep_days=revision_keep_days(config))
        return _bytes_freed(catalog, plan.keep)


def release_reference_holds(config, *, apply: bool) -> dict[str, Any]:
    """Preview, or with *apply* release, every hold derived from a file's citation.

    Manual holds and case dependencies stay. The report says how many objects
    lose a reference hold, which of them keep another hold, the folders the
    citing files lay in, and what the two cleanups would then free.
    """
    from contextlib import nullcontext

    from cnequity.file_lock import lake_mutation_lock
    from cnequity.storage.lifecycle import LifecycleStore
    from cnequity.storage.lifecycle import retired_hold as retired

    store = LifecycleStore(config.meta_root)
    report: dict[str, Any] = {"applied": False, "rows_changed": 0}
    lock = lake_mutation_lock(store.meta, blocking=True) if apply else nullcontext()
    with lock:
        registry = store.registry()
        if registry is None:
            report["skipped"] = "this lake has no hold registry"
            return report
        roots = [Path(spec["path"]) for spec in registry.get("reference_roots", [])]
        groups: Counter[str] = Counter()
        released, unprotected = [], []
        for object_id, reasons in sorted(registry["holds"].items()):
            derived = [r for r in reasons if retired(r)]
            if not derived:
                continue
            released.append(object_id)
            for group in {_source_group(str(r.get("source", "")), roots) for r in derived}:
                groups[group] += 1
            if len(derived) == len(reasons):
                unprotected.append(object_id)
        remaining = {
            oid
            for oid, reasons in registry["holds"].items()
            if any(not retired(r) for r in reasons)
        }
        revisions = [oid for oid in unprotected if oid.startswith("revision/")]
        report.update(
            {
                "released_objects": len(released),
                "still_held": len(released) - len(unprotected),
                "unprotected_revisions": len(revisions),
                "unprotected": revisions[:_LISTED],
                "citing_folders": dict(groups.most_common()),
                "reference_roots": [str(root) for root in roots],
                "generation_bytes_freed": _generation_bytes(store.meta, revisions),
                "catalog_bytes_freed": _catalog_bytes(config, remaining),
            }
        )
        if not apply or not (released or "reference_roots" in registry):
            return report
        for object_id in released:
            store._drop(registry, object_id, f"{MIGRATION_ID}: reference holds retired", retired)
        registry.pop("reference_roots", None)
        registry.pop("reference_fingerprint", None)
        store._save(registry)
        report.update({"applied": True, "rows_changed": 0})
    return report
