"""Compaction publishing into the lake catalog matches compaction into generations."""

from __future__ import annotations

from dataclasses import replace
from datetime import date

import polars as pl
import pytest

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.steps.finalize import step_compact
from cnequity.storage import StagingWriter
from cnequity.storage.catalog_bridge import lake_root, sync_all
from cnequity.storage.revisions import RevisionStore
from equitylake_core.catalog import Catalog
from equitylake_core.extensions import ExtensionUnavailableError

FIRST, SECOND = date(2024, 6, 27), date(2024, 7, 1)
_PROV = {"source": "eastmoney", "data_version": "v1", "fetched_at": "2024-07-01T00:00:00+00:00"}


def _flows(day: date, inflows: dict[str, float]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "symbol": symbol,
                "trade_date": day,
                "main_net_inflow": value,
                "super_large_net_inflow": 0.0,
                "large_net_inflow": 0.0,
                "medium_net_inflow": 0.0,
                "small_net_inflow": 0.0,
                **_PROV,
            }
            for symbol, value in inflows.items()
        ]
    )


def _margins(day: date, balances: dict[str, float]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "symbol": symbol,
                "trade_date": day,
                "margin_balance": value,
                "margin_buy": 1.0,
                "short_balance": 0.0,
                "short_sell_volume": 0.0,
                **_PROV,
            }
            for symbol, value in balances.items()
        ]
    )


def _run(cfg: Config, run_id: str, day: date, flows, margins) -> dict:
    writer = StagingWriter(cfg.staging_root)
    writer.write_batch("fund_flow", run_id, "batch-0", flows)
    writer.write_batch("margin_trading", run_id, "batch-0", margins)
    return step_compact(cfg, day, run_id, {})


def _lake(root, mode: str) -> Config:
    cfg = Config(data_root=root / mode)
    _run(
        cfg,
        "r1",
        FIRST,
        _flows(FIRST, {"600519.SH": 1.0, "000001.SZ": 2.0}),
        _margins(FIRST, {"600519.SH": 10.0}),
    )
    if mode == "catalog":
        try:
            with Catalog(lake_root(cfg), create=True) as lake:
                sync_all(cfg, lake)
        except ExtensionUnavailableError as exc:
            pytest.skip(str(exc))
        cfg = replace(cfg, storage_publish="catalog")
    # A new month for both, and a row restated for a day the first run published.
    _run(
        cfg,
        "r2",
        SECOND,
        pl.concat(
            [
                _flows(SECOND, {"600519.SH": 3.0}),
                _flows(FIRST, {"000001.SZ": 2.5}),
            ]
        ),
        _margins(SECOND, {"600519.SH": 11.0, "000001.SZ": 5.0}),
    )
    return cfg


def test_a_catalog_compaction_publishes_what_a_generation_compaction_does(tmp_path, monkeypatch):
    from cnequity.query import catalog_read, load

    monkeypatch.setenv(catalog_read.ENV_SWITCH, "1")
    generation = _lake(tmp_path, "generation")
    catalog = _lake(tmp_path, "catalog")
    catalog_read.close_all()

    for dataset in ("fund_flow", "margin_trading"):
        expected = load(dataset, config=generation).drop("fetched_at")
        got = load(dataset, config=catalog).drop("fetched_at")
        assert got.equals(expected), dataset
        pointer = RevisionStore(catalog.meta_root, catalog.curated_root).current_pointer(dataset)
        assert pointer["storage"] == "catalog" and pointer["revision"] == 2
        # Nothing was written to the working tree or a generation.
        assert not (catalog.meta_root / pointer["generation_path"]).exists()
    with Catalog(lake_root(catalog), read_only=True) as lake:
        snapshots = lake.execute("SELECT DISTINCT snapshot_id FROM _revisions").fetchall()
    assert len(snapshots) == 1, "both datasets of the run publish in one snapshot"
    # The run's checkout is gone once it published.
    assert not (catalog.staging_root / "_checkout" / "r2").exists()
