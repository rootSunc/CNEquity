"""Fund xdxr history: skipped while funds are not ingested, caught up once they are."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

import cnequity.steps.events as events
from cnequity.config import Config
from cnequity.orchestrator.manifest import Manifest
from cnequity.storage.layout import init_data_layout

DAY = date(2026, 10, 16)


@pytest.fixture
def cfg(tmp_path, monkeypatch):
    config = Config(data_root=tmp_path / "data", tdx_allow_mock=True)
    init_data_layout(config)
    monkeypatch.setattr(events, "load_symbols", lambda c: ["600519.SH", "510300.SH", "159915.SZ"])
    monkeypatch.setattr(
        "cnequity.steps.common.instrument_metadata",
        lambda c: pl.DataFrame(
            {
                "symbol": ["600519.SH", "510300.SH", "159915.SZ"],
                "asset_type": ["stock", "etf", "etf"],
            }
        ),
    )
    return config


def test_a_lake_without_fund_bars_sweeps_only_stocks(cfg):
    assert events._sweep_universe(cfg) == ["600519.SH"]
    cfg.ingest_listed_funds = True
    assert events._sweep_universe(cfg) == ["600519.SH", "510300.SH", "159915.SZ"]


def _receipt(cfg, run_id, symbols):
    manifest = Manifest(cfg.manifest_path)
    manifest.start_batch(
        run_id,
        "old",
        task_id="corporate_actions_chunk",
        dataset="corporate_actions",
        symbols=symbols,
        blocks_compaction=False,
    )
    manifest.finish_batch(run_id, "old", "success")


def test_enabling_fund_ingestion_sweeps_each_unswept_fund_once(cfg, monkeypatch):
    cfg.ingest_listed_funds = True
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    _receipt(cfg, run_id, ["510300.SH"])
    monkeypatch.setattr(
        "cnequity.steps.bars.bar_scope._daily_fund_bar_symbols",
        lambda c, d: ["510300.SH", "159915.SZ"],
    )
    asked: list[list[str]] = []
    staged: list[pl.DataFrame] = []

    def fetch(trade_date, *, symbols, **kwargs):
        asked.append(list(symbols))
        return pl.DataFrame(
            {
                "symbol": ["159915.SZ", "159915.SZ"],
                "ex_date": [date(2020, 3, 2), date(2030, 1, 1)],
                "action_type": ["split", "split"],
            }
        )

    monkeypatch.setattr(events, "fetch_corporate_actions", fetch)
    # The real adapter captures the wire response under the chunk scope.
    monkeypatch.setattr(events, "verify_raw_archive", lambda *a, **k: None)
    monkeypatch.setattr(
        events,
        "write_fetched",
        lambda config, run, dataset, frame, **kw: (
            staged.append(frame) or {"rows_written": frame.height}
        ),
    )

    written, finding = events._sweep_unswept_fund_history(cfg, DAY, run_id, None)
    assert asked == [["159915.SZ"]]
    assert written == 1  # the 2030 row is outside the history window
    assert finding is None
    assert staged[0]["ex_date"].to_list() == [date(2020, 3, 2)]
    # The receipt makes the next run ask for nothing.
    assert events._sweep_unswept_fund_history(cfg, DAY, run_id, None) == (0, None)
    assert asked == [["159915.SZ"]]


def test_a_failed_chunk_stays_owed_and_says_so(cfg, monkeypatch):
    cfg.ingest_listed_funds = True
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    monkeypatch.setattr(
        "cnequity.steps.bars.bar_scope._daily_fund_bar_symbols", lambda c, d: ["510300.SH"]
    )

    def down(*args, **kwargs):
        raise ConnectionError("tdx down")

    monkeypatch.setattr(events, "fetch_corporate_actions", down)
    written, finding = events._sweep_unswept_fund_history(cfg, DAY, run_id, None)
    assert written == 0
    assert finding["check"] == "fund_history_unswept"
    assert finding["symbols"] == ["510300.SH"]
    assert "510300.SH" not in events._swept_symbols(cfg)


def test_a_lake_without_funds_never_catches_up(cfg, monkeypatch):
    monkeypatch.setattr(
        events, "fetch_corporate_actions", lambda *a, **k: pytest.fail("no fund sweep")
    )
    assert events._sweep_unswept_fund_history(cfg, DAY, "r", None) == (0, None)
