from datetime import date

import polars as pl

import cn_market_lake.steps  # noqa: F401
from cn_market_lake.config import Config
from cn_market_lake.steps.finalize import step_compact
from cn_market_lake.storage import StagingWriter


def test_compact_only_merges_datasets_staged_in_run(tmp_path):
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    run_id = "run-compact-test"
    writer = StagingWriter(cfg.staging_root)

    writer.write_batch(
        "fund_flow",
        run_id,
        "batch-0",
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [date(2024, 6, 28)],
                "main_net_inflow": [1.0],
                "super_large_net_inflow": [0.0],
                "large_net_inflow": [0.0],
                "medium_net_inflow": [0.0],
                "small_net_inflow": [0.0],
                "source": ["eastmoney"],
                "data_version": ["v1"],
                "fetched_at": ["2024-06-28T00:00:00+00:00"],
            }
        ),
    )

    out = step_compact(cfg, date(2024, 6, 28), run_id, {})
    assert out["rows_written"] == 1
    curated = cfg.curated_root / "fund_flow" / "trade_date=2024-06-28" / "part-merged.parquet"
    assert curated.exists()
    assert not (cfg.curated_root / "daily_bars").exists()
