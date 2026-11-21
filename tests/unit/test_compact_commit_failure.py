"""A failed revision commit must not strand the datasets published before it."""

from __future__ import annotations

from datetime import date

import polars as pl

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.steps.finalize import step_compact
from cnequity.storage import StagingWriter
from cnequity.storage.state import StateStore

DAY = date(2024, 6, 28)
RUN = "run-commit-failure"
_PROV = {
    "source": ["eastmoney"],
    "data_version": ["v1"],
    "fetched_at": ["2024-06-28T00:00:00+00:00"],
}


def _stage(cfg: Config) -> None:
    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "fund_flow",
        RUN,
        "batch-0",
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [DAY],
                "main_net_inflow": [1.0],
                "super_large_net_inflow": [0.0],
                "large_net_inflow": [0.0],
                "medium_net_inflow": [0.0],
                "small_net_inflow": [0.0],
                **_PROV,
            }
        ),
    )
    writer.write_batch(
        "margin_trading",
        RUN,
        "batch-0",
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [DAY],
                "margin_balance": [1.0],
                "margin_buy": [1.0],
                "short_balance": [0.0],
                "short_sell_volume": [0.0],
                **_PROV,
            }
        ),
    )
    state = StateStore(cfg.meta_root)
    for dataset in ("fund_flow", "margin_trading"):
        state.record_missing_dates(dataset, [DAY], reason="test_gap")
        state.mark_staged_request_days(dataset, RUN, [DAY])


def test_a_successful_compact_settles_every_dataset(tmp_path):
    from cnequity.query.state import dataset_state

    cfg = Config(data_root=tmp_path / "data")
    _stage(cfg)

    step_compact(cfg, DAY, RUN, {})

    state = StateStore(cfg.meta_root)
    for dataset in ("fund_flow", "margin_trading"):
        assert dataset_state(dataset, config=cfg).revision == 1
        assert DAY not in state.get_missing_dates(dataset)
        assert "staged_request_days" not in state.get_payload(dataset)
        assert state.get_date(dataset) == DAY
