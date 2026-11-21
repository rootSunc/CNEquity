"""``meta/lake.json``: which version built this lake and what it has had.

Kept apart from ``storage.migrations`` because creating a lake needs only the
record, while the migrations run repairs — operations that publish through the
quality gate and so sit well above storage.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from cnequity.storage.atomic import write_json_atomic

LAKE_FILE = Path("lake.json")
#: Where it lived before 0.25 (``meta/state/``); moved on the first read.
LEGACY_LAKE_FILE = Path("state") / "lake.json"
FORMAT = 1


class LakeMigrationRequired(RuntimeError):
    """A writer was asked to publish on a lake that has not moved onto its catalog."""


def package_version() -> str:
    try:
        return version("cnequity")
    except PackageNotFoundError:
        return "0.0.0"


def _parse(value: str) -> tuple[int, ...]:
    parts = []
    for piece in str(value).split("."):
        digits = "".join(ch for ch in piece if ch.isdigit())
        parts.append(int(digits) if digits else 0)
    return tuple(parts)


def lake_path(config) -> Path:
    return lake_file(Path(config.meta_root))


def lake_file(meta_root: Path) -> Path:
    """``meta/lake.json``; an older lake's ``meta/state/lake.json`` is moved there first.

    The migrations record themselves in it, so it cannot wait for one.
    """
    path, legacy = Path(meta_root) / LAKE_FILE, Path(meta_root) / LEGACY_LAKE_FILE
    if not path.exists() and legacy.exists():
        try:
            legacy.replace(path)
        except FileNotFoundError:
            pass  # another process moved it first
        except OSError:
            return legacy
    return path


def read_lake(config) -> dict:
    """``lake.json``, or an empty record for a lake from before it existed."""
    import json

    path = lake_path(config)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _write_lake(config, record: dict) -> None:
    record.setdefault("format", FORMAT)
    write_json_atomic(lake_path(config), record, indent=2, sort_keys=True)


def stamp_new_lake(config) -> None:
    """Record the creating version on a lake that holds no data yet.

    Only an empty lake is stamped: an old lake re-running `cne init` must not
    claim this version built it, or every migration it still owes would vanish.
    """
    from cnequity.query.parquet_scan import dataset_has_parquet

    if lake_path(config).exists():
        return
    curated = Path(config.curated_root)
    if curated.is_dir() and any(dataset_has_parquet(p) for p in curated.iterdir() if p.is_dir()):
        return
    _write_lake(config, {"created_with": package_version(), "migrations": {}})
    start_on_catalog(config)


STORAGE_FILE = "storage.json"


def start_on_catalog(config) -> None:
    """Record that this lake publishes into its catalog (``meta/storage.json``).

    A new lake starts there: the catalog itself is created by its first
    publish, and it is the only record of the lake's revisions from the start
    (``revisions``). An existing lake gets here through the ``lake-catalog``
    migration, which copies and verifies its published data first, and the
    ``revision-records`` migration, which imports the receipts it kept.
    """
    path = Path(config.meta_root) / STORAGE_FILE
    if not path.exists():
        write_json_atomic(path, {"publish": "catalog", "revisions": "catalog"}, indent=2)


def affected(report: Any) -> int:
    """How many rows (or settled keys) a repair report says it touches.

    A report that separates what it read (``rows``) from what it rewrites
    (``rows_changed``) is counted by the latter.
    """
    if isinstance(report, dict) and isinstance(report.get("rows_changed"), int):
        return report["rows_changed"]
    if isinstance(report, dict):
        total = 0
        for key, value in report.items():
            if key in {"rows", "rows_dropped"} and isinstance(value, int):
                total += value
            elif key in {"resolved_sessions", "resolved_prelisting_keys"} and isinstance(
                value, list
            ):
                total += len(value)
            elif isinstance(value, dict):
                total += affected(value)
        return total
    return 0


def record_migration(config, migration_id: str, report: dict) -> None:
    """Note in ``lake.json`` that the migration *migration_id* ran here, applied or found nothing."""
    from datetime import datetime, timezone

    lake = read_lake(config)
    lake.setdefault("migrations", {})[migration_id] = {
        "status": "applied" if affected(report) else "not_needed",
        "affected": affected(report),
        "at": datetime.now(timezone.utc).isoformat(),
        "version": package_version(),
    }
    _write_lake(config, lake)
