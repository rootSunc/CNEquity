"""Every Baostock suspension pass keeps its own staged evidence."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.orchestrator.manifest import Manifest
from cnequity.steps.bars.gapfill import _learn_suspensions_from_baostock
from cnequity.storage.layout import init_data_layout

START, END = date(2026, 9, 17), date(2026, 9, 18)


def _halted(symbols):
    return pl.DataFrame(
        {
            "symbol": [s for s in symbols for _ in (START, END)],
            "trade_date": [d for _ in symbols for d in (START, END)],
            "is_trading": [False] * (2 * len(symbols)),
            "status": ["suspended"] * (2 * len(symbols)),
            "risk_warning": [True] * (2 * len(symbols)),
        }
    )


def test_two_passes_over_one_window_keep_both_sets_of_evidence(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    cfg.sources["baostock"] = True
    init_data_layout(cfg)
    run_id = Manifest(cfg.manifest_path).start_run("init")
    monkeypatch.setattr(
        "cnequity.adapters.baostock.st_history.fetch_st_history",
        lambda symbols, start, end, config: (_halted(symbols), []),
    )
    for symbols in (["000016.SZ", "002731.SZ"], ["000582.SZ"]):
        missing = {(s, d) for s in symbols for d in (START, END)}
        confirmed, _ = _learn_suspensions_from_baostock(cfg, run_id, missing, START, END)
        assert confirmed == missing

    staged = pl.concat(
        [pl.read_parquet(p) for p in (cfg.staging_root / "trading_status").rglob("*.parquet")]
    )
    assert set(staged["symbol"]) == {"000016.SZ", "002731.SZ", "000582.SZ"}
