from __future__ import annotations

from cnequity.config import Config
from cnequity.orchestrator.manifest import Manifest
from cnequity.query.views import ensure_duckdb_views


def init_data_layout(config: Config) -> None:
    dirs = [
        config.staging_root,
        config.curated_root,
        config.derived_root,
        config.meta_root,
        config.meta_root / "quality" / "findings",
        config.meta_root / "quality" / "source_diffs",
        config.meta_root / "source_snapshots",
        config.meta_root / "raw",
        config.meta_root / "state",
        config.meta_root / "adj_factors_cache",
        config.meta_root / "seeds",
        config.meta_root / "on_demand",
        config.data_root / "duckdb",
        config.data_root / "raw",
        config.data_root / "backups",
    ]
    from cnequity.storage.lake_record import stamp_new_lake

    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
    stamp_new_lake(config)
    Manifest(config.manifest_path)
    ensure_duckdb_views(config)
