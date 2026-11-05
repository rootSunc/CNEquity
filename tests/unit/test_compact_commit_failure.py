"""A failed revision commit must not strand the datasets published before it."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.domain.datasets import PARTITION_COLS
from cnequity.steps.finalize import step_compact
from cnequity.storage import StagingWriter
from cnequity.storage.revisions import RevisionStore
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


def test_datasets_committed_before_a_failure_are_settled(tmp_path, monkeypatch):
    # fund_flow is compacted (and committed) before margin_trading.
    order = list(PARTITION_COLS)
    assert order.index("fund_flow") < order.index("margin_trading")

    cfg = Config(data_root=tmp_path / "data")
    _stage(cfg)
    original = RevisionStore.commit

    def failing_commit(self, dataset, **kwargs):
        if dataset == "margin_trading":
            raise OSError("simulated publish failure")
        return original(self, dataset, **kwargs)

    monkeypatch.setattr(RevisionStore, "commit", failing_commit)

    with pytest.raises(OSError, match="simulated publish failure"):
        step_compact(cfg, DAY, RUN, {})

    state = StateStore(cfg.meta_root)
    # Published: its generation is current, so its ledger must say so.
    assert state.get_revision("fund_flow") == 1
    assert DAY not in state.get_missing_dates("fund_flow")
    assert RUN not in state.get_payload("fund_flow").get("staged_request_days", {})
    assert state.get_date("fund_flow") == DAY
    # Not published: everything stays owed, so a retry still has work to do.
    assert state.get_revision("margin_trading") is None
    assert DAY in state.get_missing_dates("margin_trading")
    assert RUN in state.get_payload("margin_trading")["staged_request_days"]
    assert state.get_date("margin_trading") is None


def test_a_successful_compact_settles_every_dataset(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _stage(cfg)

    step_compact(cfg, DAY, RUN, {})

    state = StateStore(cfg.meta_root)
    for dataset in ("fund_flow", "margin_trading"):
        assert state.get_revision(dataset) == 1
        assert DAY not in state.get_missing_dates(dataset)
        assert "staged_request_days" not in state.get_payload(dataset)
        assert state.get_date(dataset) == DAY
