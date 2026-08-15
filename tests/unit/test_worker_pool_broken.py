"""A worker killed by the OS must not wipe the whole daily_bars run.

`ProcessPoolExecutor` poisons its entire pool when one worker dies:
`BrokenProcessPool` propagates to every not-yet-collected future. Under memory
pressure that turned one dead batch into a failed run (2026-07-22, re-fetching
7,684 symbols while other work was running). The pool path now falls back to a
serial retry of whatever never got a verdict.
"""

from __future__ import annotations

from concurrent.futures.process import BrokenProcessPool
from datetime import date

import pytest

from cn_market_lake.config import load_config
from cn_market_lake.config.bootstrap import path_for_toml
from cn_market_lake.orchestrator.manifest import Manifest
from cn_market_lake.orchestrator.worker_pool import fetch_daily_bars_parallel
from cn_market_lake.storage.layout import init_data_layout


@pytest.fixture
def worker_config(tmp_path):
    cfg_path = tmp_path / "test.toml"
    cfg_path.write_text(
        f"""
[data]
root = "{path_for_toml(tmp_path / "data")}"

[orchestrator]
workers = 4
batch_size = 1

[tdx_protocol]
allow_mock = true
"""
    )
    return load_config(cfg_path)


def test_broken_pool_falls_back_to_serial(worker_config, monkeypatch):
    init_data_layout(worker_config)
    run_id = Manifest(worker_config.manifest_path).start_run("test")

    class _DeadPool:
        """Stand-in pool: submit() works, but collecting a result explodes the
        pool exactly as an OS-killed worker would."""

        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def submit(self, fn, task):
            class _F:
                def result(self, timeout=None):
                    raise BrokenProcessPool("A process in the process pool died")

            return _F()

    monkeypatch.setattr(
        "cn_market_lake.orchestrator.worker_pool.ProcessPoolExecutor",
        lambda *a, **k: _DeadPool(),
    )
    monkeypatch.setattr(
        "cn_market_lake.orchestrator.worker_pool.as_completed", lambda futures: list(futures)
    )

    serial: list[str] = []

    def _fake_fetch(symbols, start, end, **kwargs):
        # Records that the serial-retry path ran, returns a minimal frame. The
        # heartbeat callback is invoked so the manifest bookkeeping is exercised.
        import polars as pl

        if kwargs.get("on_heartbeat"):
            kwargs["on_heartbeat"]()
        serial.extend(symbols)
        return pl.DataFrame(
            {
                "symbol": symbols,
                "trade_date": [start] * len(symbols),
                "open": [1.0] * len(symbols),
                "high": [1.0] * len(symbols),
                "low": [1.0] * len(symbols),
                "close": [1.0] * len(symbols),
                "volume": [100] * len(symbols),
                "amount": [100.0] * len(symbols),
            }
        )

    monkeypatch.setattr("cn_market_lake.orchestrator.worker_pool.fetch_daily_bars", _fake_fetch)
    # Let the real normalize_with_source stamp source/data_version/fetched_at so
    # the staging write passes schema validation.

    result = fetch_daily_bars_parallel(
        worker_config,
        ["600519.SH", "000001.SZ", "600000.SH"],
        date(2024, 6, 27),
        date(2024, 6, 27),
        run_id,
        "daily_bars",
    )
    # Every symbol was recovered through the serial retry, not lost with the pool.
    assert set(serial) == {"600519.SH", "000001.SZ", "600000.SH"}
    assert result["rows_written"] == 3


def test_broken_pool_skips_batches_already_success(worker_config, monkeypatch):
    """Child finished success before the pool died — do not demote via re-fetch."""
    init_data_layout(worker_config)
    manifest = Manifest(worker_config.manifest_path)
    run_id = manifest.start_run("test")
    tip = date(2024, 6, 27)
    done_id = f"{tip.isoformat()}_{tip.isoformat()}-batch-0"
    # Simulate a worker that wrote staging + finish_batch before the pool broke.
    manifest.start_batch(
        run_id,
        done_id,
        task_id="daily_bars",
        dataset="daily_bars",
        symbols=["600519.SH"],
        window_start=tip.isoformat(),
        window_end=tip.isoformat(),
    )
    manifest.finish_batch(run_id, done_id, "success", rows_read=7, rows_written=7)

    class _DeadPool:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def submit(self, fn, task):
            class _F:
                def result(self, timeout=None):
                    raise BrokenProcessPool("A process in the process pool died")

            return _F()

    monkeypatch.setattr(
        "cn_market_lake.orchestrator.worker_pool.ProcessPoolExecutor",
        lambda *a, **k: _DeadPool(),
    )
    monkeypatch.setattr(
        "cn_market_lake.orchestrator.worker_pool.as_completed", lambda futures: list(futures)
    )

    fetched: list[str] = []

    def _fake_fetch(symbols, start, end, **kwargs):
        import polars as pl

        if kwargs.get("on_heartbeat"):
            kwargs["on_heartbeat"]()
        fetched.extend(symbols)
        return pl.DataFrame(
            {
                "symbol": symbols,
                "trade_date": [start] * len(symbols),
                "open": [1.0] * len(symbols),
                "high": [1.0] * len(symbols),
                "low": [1.0] * len(symbols),
                "close": [1.0] * len(symbols),
                "volume": [100] * len(symbols),
                "amount": [100.0] * len(symbols),
            }
        )

    monkeypatch.setattr("cn_market_lake.orchestrator.worker_pool.fetch_daily_bars", _fake_fetch)

    result = fetch_daily_bars_parallel(
        worker_config,
        ["600519.SH", "000001.SZ"],
        tip,
        tip,
        run_id,
        "daily_bars",
    )
    # First batch already success — must not be re-fetched (would demote it).
    assert "600519.SH" not in fetched
    assert fetched == ["000001.SZ"]
    assert manifest.get_batch(run_id, done_id)["status"] == "success"
    assert result["rows_written"] == 7 + 1
