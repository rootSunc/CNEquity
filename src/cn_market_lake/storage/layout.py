from __future__ import annotations

from cn_market_lake.config import Config
from cn_market_lake.orchestrator.manifest import Manifest
from cn_market_lake.query.views import ensure_duckdb_views


def init_data_layout(config: Config) -> None:
    dirs = [
        config.staging_root,
        config.curated_root,
        config.derived_root,
        config.meta_root,
        config.meta_root / "quality" / "findings",
        config.meta_root / "quality" / "source_diffs",
        config.meta_root / "source_snapshots",
        config.meta_root / "state",
        config.meta_root / "adj_factors_cache",
        config.meta_root / "seeds",
        config.meta_root / "on_demand",
        config.data_root / "duckdb",
        config.data_root / "raw",
        config.data_root / "backups",
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
    Manifest(config.manifest_path)
    ensure_duckdb_views(config)
