import json
from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.manifest import Manifest
from cnequity.run_scope import set_backfill
from cnequity.steps import events


def _rows(symbol: str) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [symbol],
            "ex_date": [date(2024, 6, 28)],
            "action_type": ["cash_dividend"],
            "cash_dividend": [1.0],
            "bonus_ratio": [0.0],
            "transfer_ratio": [0.0],
            "allotment_ratio": [None],
            "allotment_price": [None],
            "source": ["tdx_protocol"],
        }
    )


def test_corporate_actions_stages_successful_chunks_for_retry(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake", batch_size=2, raw_archive_enabled=False)
    set_backfill(cfg, active=True, start=date(2024, 1, 1), end=date(2024, 6, 28))
    monkeypatch.setattr(
        events, "load_symbols", lambda _cfg: ["600519.SH", "000001.SZ", "300750.SZ"]
    )
    calls = []

    def fake_fetch(_trade_date, *, symbols, **_kwargs):
        calls.append(symbols)
        if symbols == ["300750.SZ"]:
            raise RuntimeError("simulated chunk outage")
        return _rows(symbols[0])

    monkeypatch.setattr(events, "fetch_corporate_actions", fake_fetch)

    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("init", {})
    manifest.start_batch(run_id, "parent", "corporate_actions", "corporate_actions")
    out = events.step_corporate_actions(cfg, date(2024, 6, 28), run_id, {"_batch_id": "parent"})

    assert sorted(calls) == [["300750.SZ"], ["600519.SH", "000001.SZ"]]
    assert out["failed_symbols"] == ["300750.SZ"]
    assert out["status"] == "failed"
    assert out["rows_written"] == 1
    child = [
        row
        for row in manifest.get_batches_for_run(run_id)
        if row["task_id"] == "corporate_actions_chunk"
    ]
    assert len(child) == 1
    assert json.loads(child[0]["symbols_json"]) == ["600519.SH", "000001.SZ"]
    # Chunk receipts describe scope; the parent batch is what gates compaction
    # and what a retry re-runs.
    assert not child[0]["blocks_compaction"]
    # How many chunks the sweep set out to do, so one successful receipt can be
    # told apart from a plan that only ever had one.
    performance = manifest.get_run_metadata(run_id)["performance"]["corporate_actions"]
    assert performance["chunks_planned"] == 2, "the plan size must survive a failed chunk"
    assert performance["chunk_symbols"] == 3


def test_engine_retry_only_fetches_unreceipted_chunks(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake", batch_size=2, raw_archive_enabled=False)
    set_backfill(cfg, active=True, start=date(2024, 1, 1), end=date(2024, 6, 28))
    monkeypatch.setattr(
        events,
        "load_symbols",
        lambda _cfg: ["600519.SH", "000001.SZ", "300750.SZ"],
    )
    calls = []
    fail_third = {True}

    def fake_fetch(_trade_date, *, symbols, **_kwargs):
        calls.append(list(symbols))
        if symbols == ["300750.SZ"] and fail_third:
            raise RuntimeError("simulated chunk outage")
        return _rows(symbols[0])

    monkeypatch.setattr(events, "fetch_corporate_actions", fake_fetch)

    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run(
        "init",
        {"backfill": True, "phases": ["phase2a_corporate_actions"]},
    )
    first = engine._run_step("corporate_actions", date(2024, 6, 28), run_id, {})
    assert first["status"] == "failed"

    fail_third.clear()
    retry = engine._retry_run_locked(run_id, date(2024, 6, 28), auto_finalize=False)

    # The first sweep's chunks may finish in either order; the retry asks
    # only for the chunk that never got a receipt.
    assert sorted(calls[:2]) == [["300750.SZ"], ["600519.SH", "000001.SZ"]]
    assert calls[2:] == [["300750.SZ"]]
    assert retry["status"] == "success"
    parent_batches = [
        row
        for row in engine.manifest.get_batches_for_run(run_id)
        if row["task_id"] == "corporate_actions"
    ]
    assert sorted(row["status"] for row in parent_batches) == ["success", "superseded"]


def test_a_sweep_spreads_chunks_over_lanes_and_keeps_every_receipt(tmp_path, monkeypatch):
    """One TDX connection waits ~0.3s on every reply; lanes fill that wait."""
    import threading
    import time

    cfg = Config(
        data_root=tmp_path / "lake",
        batch_size=1,
        raw_archive_enabled=False,
        source_concurrency={"tdx_protocol": 3},
    )
    set_backfill(cfg, active=True, start=date(2024, 1, 1), end=date(2024, 6, 28))
    symbols = ["600519.SH", "000001.SZ", "300750.SZ", "601318.SH", "000858.SZ", "002594.SZ"]
    monkeypatch.setattr(events, "load_symbols", lambda _cfg: symbols)
    active = {"now": 0, "peak": 0}
    lock = threading.Lock()
    threads = set()

    def fake_fetch(_trade_date, *, symbols, progress_report, **_kwargs):
        with lock:
            active["now"] += 1
            active["peak"] = max(active["peak"], active["now"])
            threads.add(threading.get_ident())
        time.sleep(0.05)
        progress_report(1)
        with lock:
            active["now"] -= 1
        if symbols == ["300750.SZ"]:
            raise RuntimeError("simulated chunk outage")
        return _rows(symbols[0])

    monkeypatch.setattr(events, "fetch_corporate_actions", fake_fetch)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("init", {})
    manifest.start_batch(run_id, "parent", "corporate_actions", "corporate_actions")
    out = events.step_corporate_actions(cfg, date(2024, 6, 28), run_id, {"_batch_id": "parent"})

    assert active["peak"] == 3, "the configured TDX cap is the lane count"
    assert threading.get_ident() not in threads, "fetches run off the writing thread"
    assert out["failed_symbols"] == ["300750.SZ"]
    receipts = {
        json.loads(row["symbols_json"])[0]
        for row in manifest.get_batches_for_run(run_id)
        if row["task_id"] == "corporate_actions_chunk"
    }
    assert receipts == set(symbols) - {"300750.SZ"}


def test_lanes_never_exceed_the_pacing_floor():
    cfg = Config(data_root="/unused", source_concurrency={"tdx_protocol": 16})
    assert events._xdxr_lanes(cfg, 81) == 4
    assert events._xdxr_lanes(cfg, 2) == 2
    single = Config(data_root="/unused", source_concurrency={"tdx_protocol": 1})
    assert events._xdxr_lanes(single, 81) == 1


def test_a_stopped_sweep_sends_no_further_requests():
    from cnequity.adapters.tdx_protocol import corporate_actions as tdx_ca

    asked: list[str] = []

    class _Client:
        def xdxr(self, *, symbol, market):
            asked.append(symbol)
            return None

    stop = {"now": False}

    def report(done):
        stop["now"] = done >= 2

    import pytest

    with pytest.raises(tdx_ca.SweepStopped):
        tdx_ca.fetch_corporate_actions_tdx(
            ["600519.SH", "000001.SZ", "300750.SZ", "601318.SH"],
            backfill=True,
            client_factory=_Client,
            progress_report=report,
            should_stop=lambda: stop["now"],
        )
    assert asked == ["600519", "000001"]
