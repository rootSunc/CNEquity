"""``meta/state/lake.json``: which version built this lake and what it has had.

Kept apart from ``storage.migrations`` because creating a lake needs only the
record, while the migrations run repairs — operations that publish through the
quality gate and so sit well above storage.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from cnequity.storage.atomic import write_json_atomic

LAKE_FILE = Path("state") / "lake.json"
FORMAT = 1


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
    return Path(config.meta_root) / LAKE_FILE


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
