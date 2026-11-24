"""Bring a lake built by an earlier release up to this one, in one step.

An upgrade that changes how the lake is stored used to mean reading a
changelog and running the right commands in the right order. This module is
what ``cne upgrade`` runs instead:

* :func:`plan` says what the lake still owes and whether this machine can do
  it (the DuckDB extensions the catalog needs, free disk space), without
  changing anything;
* :func:`back_up` keeps a copy of the small files that say what the lake
  publishes (pointers, the catalog database, the lake record);
* :func:`run` applies every owed migration in order, reporting progress, and
  records each one, so a run that stops can simply be started again.

The data itself is never copied by the upgrade: generations stay where they
are while their files are linked into the lake, and ``drop-generations`` then
removes the retired ``curated/``, ``derived/`` and ``meta/revisions/data``
trees once verified. :func:`back_up` saves the pre-upgrade pointers, receipts
and catalog database; once ``drop-generations`` runs, rolling back to an
earlier release requires a full copy of ``data_root`` taken beforehand.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import tempfile
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from cnequity.meta_layout import (
    LAKE_DIR,
    LAKE_RECORD,
    LEGACY_STATE,
    META_DIR,
    REVISION_RECORDS,
    STORAGE_RECORD,
)
from cnequity.sqlite_readonly import connect_readonly


@dataclass(frozen=True)
class Check:
    #: "extensions", "disk" or "links".
    name: str
    ok: bool
    detail: str


def _tree_bytes(root: Path) -> int:
    if not root.is_dir():
        return 0
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def _can_link(folder: Path) -> bool:
    """Whether a file under *folder* can be hard-linked there (same volume, a filesystem that links)."""
    probe_dir = folder
    while not probe_dir.is_dir() and probe_dir.parent != probe_dir:
        probe_dir = probe_dir.parent
    if not probe_dir.is_dir():
        return False
    source = probe_dir / f".link-probe-{os.getpid()}"
    target = probe_dir / f".link-probe-{os.getpid()}-2"
    try:
        source.write_bytes(b"")
        os.link(source, target)
        return True
    except OSError:
        return False
    finally:
        source.unlink(missing_ok=True)
        target.unlink(missing_ok=True)


def plan(config) -> dict:
    """What upgrading *config*'s lake would do, and whether it can be done here."""
    from cnequity.storage.catalog_bridge import lake_root
    from cnequity.storage.catalog_layout import datasets_to_lay_out
    from cnequity.storage.catalog_migration import migrate_to_catalog
    from cnequity.storage.migrations import owed, package_version, read_lake

    pending = [migration.id for migration in owed(config)]
    catalog = (
        migrate_to_catalog(config, apply=False)
        if "lake-catalog" in pending
        else {"to_copy": [], "to_lay_out": datasets_to_lay_out(config), "switched": True}
    )
    checks: list[Check] = []
    if catalog["to_copy"] or catalog["to_lay_out"] or not catalog["switched"]:
        checks.append(_check_extensions())
        root = lake_root(config)
        links = _can_link(root / "tmp")
        generations = Path(config.meta_root) / "revisions" / "data"
        needed = sum(_tree_bytes(root / "data" / "main" / name) for name in catalog["to_lay_out"])
        if not links:
            # Without links every generation file is copied into the lake.
            needed += sum(_tree_bytes(generations / name) for name in catalog["to_copy"])
        free = shutil.disk_usage(root if root.exists() else Path(config.data_root)).free
        checks.append(
            Check(
                "disk",
                free > needed * 1.1,
                f"needs about {needed / 1e9:.1f} GB while it runs, {free / 1e9:.1f} GB free",
            )
        )
        if not links and catalog["to_copy"]:
            checks.append(
                Check(
                    "links",
                    True,
                    "this filesystem cannot hard-link, so data files are copied instead of linked",
                )
            )
    return {
        "version": package_version(),
        "created_with": read_lake(config).get("created_with"),
        "migrations": pending,
        "catalog": catalog,
        "checks": checks,
        "needed": bool(pending),
        "ready": all(check.ok for check in checks),
    }


def _check_extensions() -> Check:
    from equitylake_core.catalog import Catalog
    from equitylake_core.extensions import ExtensionUnavailableError

    with tempfile.TemporaryDirectory(prefix="cne-upgrade-probe-") as folder:
        try:
            Catalog(Path(folder) / LAKE_DIR, create=True).close()
        except ExtensionUnavailableError as exc:
            return Check("extensions", False, str(exc))
    return Check("extensions", True, "the DuckDB extensions the lake catalog needs are available")


def back_up(config) -> Path:
    """Copy what says which data the lake publishes; returns the backup folder.

    Pointers and receipts of the current revisions, the lake record, the
    storage setting and the catalog database: megabytes, whatever the lake's
    size. The data files are not copied, and the upgrade does not change them.
    """
    from cnequity.storage.catalog_bridge import lake_root
    from equitylake_core.catalog import CATALOG_FILE

    stamp = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    target = Path(config.data_root) / "backups" / f"upgrade-{stamp}"
    meta = Path(config.meta_root)
    for source in (
        meta / STORAGE_RECORD,
        meta / LAKE_RECORD,
        meta / LEGACY_STATE / LAKE_RECORD,  # a lake from before 0.25 that has not read it since
        *sorted((meta / REVISION_RECORDS).glob("*/current.json")),
    ):
        if source.is_file():
            stored = target / META_DIR / source.relative_to(meta)
            stored.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, stored)
    catalog = lake_root(config) / CATALOG_FILE
    if catalog.is_file():
        (target / LAKE_DIR).mkdir(parents=True, exist_ok=True)
        with (
            closing(connect_readonly(catalog)) as original,
            closing(sqlite3.connect(target / LAKE_DIR / CATALOG_FILE)) as copied,
        ):
            original.backup(copied)
    target.mkdir(parents=True, exist_ok=True)
    return target


#: How long ``cne upgrade --apply`` waits for reads already under way.
READERS_WAIT_SECONDS = 30.0


def run(config, progress: Callable[[str, str, str], None] | None = None) -> dict:
    """Apply every migration the lake owes, in order; returns what each did.

    *progress* is called with a migration id, a stage and a dataset name.
    A migration that fails stops the run; the ones before it are recorded and
    are not repeated when the upgrade is started again.
    """
    from cnequity.storage.catalog_bridge import lake_root
    from cnequity.storage.catalog_migration import upgrade_in_progress
    from equitylake_core.lock import early_readers_path, lake_maintenance

    root = lake_root(config)
    # A scan that is running gets a little time to finish; anything that keeps
    # reading (`cne serve`, `cne mcp`) has to be stopped first.
    with lake_maintenance(root, timeout=READERS_WAIT_SECONDS), upgrade_in_progress():
        result = _run(config, progress)
    # Readers of the lake lock the lake's own files from now on, so nothing
    # waits on this one. Removed once released: Windows cannot delete a file
    # this process still holds open. A busy attempt leaves it: it is how the
    # next attempt sees the readers still running.
    try:
        early_readers_path(root).unlink(missing_ok=True)
    except OSError:
        pass
    return result


def _run(config, progress) -> dict:
    from cnequity.storage.catalog_migration import migrate_to_catalog
    from cnequity.storage.migrations import affected, owed, record

    results: list[dict] = []
    for migration in owed(config):
        started = datetime.now(timezone.utc)
        try:
            if migration.id == "lake-catalog":
                report = migrate_to_catalog(
                    config,
                    apply=True,
                    progress=None
                    if progress is None
                    else lambda stage, dataset: progress("lake-catalog", stage, dataset),
                )
            else:
                if progress is not None:
                    progress(migration.id, "repair", "")
                report = migration.apply(config, apply=True)
        except Exception as exc:  # noqa: BLE001 — reported; later ones may depend on it
            results.append({"id": migration.id, "error": f"{type(exc).__name__}: {exc}"})
            return {"done": False, "migrations": results}
        record(config, migration, report)
        results.append(
            {
                "id": migration.id,
                "affected": affected(report),
                "seconds": round((datetime.now(timezone.utc) - started).total_seconds(), 1),
                "report": report,
            }
        )
    return {"done": True, "migrations": results}
