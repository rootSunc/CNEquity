"""Shared additive holds for the existing staging, source and log cleaners."""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
from pathlib import Path

from cnequity.file_lock import lake_mutation_lock
from cnequity.storage.lifecycle import LifecycleError, LifecycleStore, reference_fingerprint
from cnequity.storage.lifecycle.snapshot import read_dependencies
from cnequity.storage.revisions import _reject_symlink_path


@contextmanager
def resource_holds(meta_root: Path, *, dry_run: bool = False):
    """Coordinate protection registration and cleanup through the publication lock.

    Existing age/readiness rules remain necessary. Holds never expire implicitly,
    and force cleanup cannot bypass them. A failed reference scan stops cleanup.
    """
    store = LifecycleStore(meta_root)
    with nullcontext() if dry_run else lake_mutation_lock(store.meta):
        registry = store.registry()
        if registry is None:
            if read_dependencies(store.meta) is not None:
                raise LifecycleError("Rebind imported lifecycle dependencies before cleanup")
            yield set()
        else:
            if (
                reference_fingerprint(registry["reference_roots"], meta_root=store.meta)
                != registry["reference_fingerprint"]
            ):
                raise LifecycleError(
                    "References changed; refresh the lifecycle import before cleanup"
                )
            yield set(registry["holds"])


def resource_exists(meta_root: Path, object_id: str) -> bool:
    """Recognize only cleaner-owned objects below the configured lake."""
    parts = object_id.split("/")
    if any(p in {"", ".", ".."} for p in parts):
        return False
    kind, *relative = parts
    if kind == "staging" and len(relative) == 1:
        paths = list((meta_root.parent / "staging").glob(f"*/run_id={relative[0]}"))
        # A glob character must never broaden a manually selected run ID.
        paths = [p for p in paths if p.name == f"run_id={relative[0]}"]
    elif kind == "source_snapshot" and len(relative) == 4:
        paths = [meta_root / "source_snapshots" / Path(*relative)]
    elif (
        kind == "log"
        and len(relative) == 1
        and relative[0].startswith("cne-")
        and relative[0].endswith(".log")
    ):
        paths = [meta_root.parent / "logs" / relative[0]]
    else:
        return False
    for path in paths:
        _reject_symlink_path(path)
    return any(p.is_file() if kind == "log" else p.is_dir() for p in paths)
