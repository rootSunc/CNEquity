"""Portable protection evidence; never exports local deletion authority."""

from __future__ import annotations

from pathlib import Path

from cnequity.storage.lifecycle import LifecycleError, LifecycleStore, _read, case_holds, digest

DEPENDENCIES_PATH = Path("lifecycle/snapshot-dependencies.json")


def export_dependencies(store: LifecycleStore) -> dict | None:
    registry = store.registry()
    if registry is None:
        # Re-exporting a restored lake must not drop its unbound protection.
        return read_dependencies(store.meta)
    protection = {
        "origin_lake_id": registry["lake_id"],
        "holds": registry["holds"],
        "cases": registry.get("cases", []),
        "reference_roots": registry["reference_roots"],
    }
    return {
        "schema_version": 1,
        "protection": protection,
        "protection_digest": digest(protection),
        "external_bytes_materialized": False,
        "requires_rebinding": True,
    }


def read_dependencies(meta_root: Path) -> dict | None:
    path = meta_root / DEPENDENCIES_PATH
    if not path.exists() and not path.is_symlink():
        return None
    value = _read(path)
    protection = value.get("protection")
    if (
        value.get("schema_version") != 1
        or value.get("external_bytes_materialized") is not False
        or value.get("requires_rebinding") is not True
        or not isinstance(protection, dict)
        or digest(protection) != value.get("protection_digest")
        or not isinstance(protection.get("holds"), dict)
        or not isinstance(protection.get("cases"), list)
        or not isinstance(protection.get("reference_roots"), list)
    ):
        raise LifecycleError("Invalid snapshot lifecycle dependencies")
    for oid, reasons in protection["holds"].items():
        if not isinstance(oid, str) or not isinstance(reasons, list) or not reasons:
            raise LifecycleError("Invalid snapshot lifecycle hold")
    for oid, reasons in case_holds(protection["cases"]).items():
        if any(reason not in protection["holds"].get(oid, []) for reason in reasons):
            raise LifecycleError("Snapshot case dependencies lack protection")
    return value
