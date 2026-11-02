"""A worker killed by the OS must not wipe the whole daily_bars run.

`ProcessPoolExecutor` poisons its entire pool when one worker dies:
`BrokenProcessPool` propagates to every not-yet-collected future. Under memory
pressure that turned one dead batch into a failed run (2026-07-22, re-fetching
7,684 symbols while other work was running). The pool path now falls back to a
serial retry of whatever never got a verdict.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import Future
from concurrent.futures.process import BrokenProcessPool
from datetime import date

import pytest

from cnequity.config import load_config
from cnequity.config.bootstrap import path_for_toml
from cnequity.orchestrator.manifest import Manifest
from cnequity.orchestrator.worker_pool import fetch_daily_bars_parallel
from cnequity.storage.layout import init_data_layout


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
        "cnequity.orchestrator.worker_pool.ProcessPoolExecutor",
        lambda *a, **k: _DeadPool(),
    )
    monkeypatch.setattr(
        "cnequity.orchestrator.worker_pool.as_completed",
        lambda futures, timeout=None: list(futures),
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

    monkeypatch.setattr("cnequity.orchestrator.worker_pool.fetch_daily_bars", _fake_fetch)
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


def test_process_pool_interrupt_cancels_queued_batches(worker_config, monkeypatch):
    monkeypatch.setattr("sys.platform", "linux")
    init_data_layout(worker_config)
    run_id = Manifest(worker_config.manifest_path).start_run("test")
    pools: list[object] = []

    class _InterruptPool:
        def __init__(self, *args, **kwargs):
            self.futures: list[Future] = []
            self.shutdown_calls: list[tuple[bool, bool]] = []
            pools.append(self)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def submit(self, fn, task):
            future = Future()
            if not self.futures:
                future.set_exception(KeyboardInterrupt())
            self.futures.append(future)
            return future

        def shutdown(self, *, wait=True, cancel_futures=False):
            self.shutdown_calls.append((wait, cancel_futures))
            if cancel_futures:
                for future in self.futures:
                    future.cancel()

    monkeypatch.setattr(
        "cnequity.orchestrator.worker_pool.ProcessPoolExecutor",
        _InterruptPool,
    )
    monkeypatch.setattr(
        "cnequity.orchestrator.worker_pool.as_completed",
        lambda futures, timeout=None: list(futures),
    )

    with pytest.raises(KeyboardInterrupt):
        fetch_daily_bars_parallel(
            worker_config,
            ["600519.SH", "000001.SZ", "600000.SH"],
            date(2024, 6, 27),
            date(2024, 6, 27),
            run_id,
            "daily_bars",
        )

    pool = pools[0]
    assert (True, True) in pool.shutdown_calls
    assert all(future.cancelled() for future in pool.futures[1:])


def test_interrupt_says_it_is_waiting_for_batches_in_flight(worker_config, monkeypatch, caplog):
    """Ctrl-C waits for lanes already talking to the source, and says so.

    A hundred-symbol batch takes tens of seconds; silent, that wait looked
    like a hang and drew a second Ctrl-C.
    """
    monkeypatch.setattr("sys.platform", "linux")
    init_data_layout(worker_config)
    run_id = Manifest(worker_config.manifest_path).start_run("test")

    class _Pool:
        def __init__(self, *args, **kwargs):
            self.futures: list[Future] = []

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def submit(self, fn, task):
            future = Future()
            if not self.futures:
                future.set_exception(KeyboardInterrupt())
            else:
                future.set_running_or_notify_cancel()
            self.futures.append(future)
            return future

        def shutdown(self, *, wait=True, cancel_futures=False):
            pass

    monkeypatch.setattr("cnequity.orchestrator.worker_pool.ProcessPoolExecutor", _Pool)
    monkeypatch.setattr(
        "cnequity.orchestrator.worker_pool.as_completed",
        lambda futures, timeout=None: list(futures),
    )
    with caplog.at_level("WARNING"), pytest.raises(KeyboardInterrupt):
        fetch_daily_bars_parallel(
            worker_config,
            ["600519.SH", "000001.SZ", "600000.SH"],
            date(2024, 6, 27),
            date(2024, 6, 27),
            run_id,
            "daily_bars",
        )
    assert "letting 2 daily_bars batch(es) already in flight finish" in caplog.text


def test_process_pool_interrupt_during_submission_cancels_submitted_batches(
    worker_config, monkeypatch
):
    monkeypatch.setattr("sys.platform", "linux")
    init_data_layout(worker_config)
    run_id = Manifest(worker_config.manifest_path).start_run("test")
    pools: list[object] = []

    class _SubmissionInterruptPool:
        def __init__(self, *args, **kwargs):
            self.futures: list[Future] = []
            self.shutdown_calls: list[tuple[bool, bool]] = []
            pools.append(self)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def submit(self, fn, task):
            if self.futures:
                raise KeyboardInterrupt
            future = Future()
            self.futures.append(future)
            return future

        def shutdown(self, *, wait=True, cancel_futures=False):
            self.shutdown_calls.append((wait, cancel_futures))
            if cancel_futures:
                for future in self.futures:
                    future.cancel()

    monkeypatch.setattr(
        "cnequity.orchestrator.worker_pool.ProcessPoolExecutor",
        _SubmissionInterruptPool,
    )

    with pytest.raises(KeyboardInterrupt):
        fetch_daily_bars_parallel(
            worker_config,
            ["600519.SH", "000001.SZ", "600000.SH"],
            date(2024, 6, 27),
            date(2024, 6, 27),
            run_id,
            "daily_bars",
        )

    pool = pools[0]
    assert (True, True) in pool.shutdown_calls
    assert pool.futures[0].cancelled()


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
        "cnequity.orchestrator.worker_pool.ProcessPoolExecutor",
        lambda *a, **k: _DeadPool(),
    )
    monkeypatch.setattr(
        "cnequity.orchestrator.worker_pool.as_completed",
        lambda futures, timeout=None: list(futures),
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

    monkeypatch.setattr("cnequity.orchestrator.worker_pool.fetch_daily_bars", _fake_fetch)

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


def test_completed_future_does_not_receive_fake_stale_timeout(worker_config, monkeypatch):
    # This regression targets the Linux ProcessPool path.  macOS intentionally
    # resolves ``auto`` to the safe thread backend now.
    monkeypatch.setattr("sys.platform", "linux")
    init_data_layout(worker_config)
    run_id = Manifest(worker_config.manifest_path).start_run("test")
    observed_timeouts: list[object] = []

    class _CompletedPool:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def submit(self, fn, task):
            class _F:
                def result(self, timeout=None):
                    observed_timeouts.append(timeout)
                    return {"rows_read": 1, "rows_written": 1}

            return _F()

    monkeypatch.setattr(
        "cnequity.orchestrator.worker_pool.ProcessPoolExecutor",
        lambda *a, **k: _CompletedPool(),
    )
    monkeypatch.setattr(
        "cnequity.orchestrator.worker_pool.as_completed",
        lambda futures, timeout=None: list(futures),
    )

    result = fetch_daily_bars_parallel(
        worker_config,
        ["600519.SH", "000001.SZ"],
        date(2024, 6, 27),
        date(2024, 6, 27),
        run_id,
        "daily_bars",
    )

    assert observed_timeouts == [None, None]
    assert result["had_error"] is False
    assert result["rows_written"] == 2


def test_process_pool_submits_only_pending_batches_and_sizes_pool_to_pending(
    worker_config, monkeypatch
):
    """A committed success is neither submitted nor counted a second time."""
    monkeypatch.setattr("sys.platform", "linux")
    init_data_layout(worker_config)
    manifest = Manifest(worker_config.manifest_path)
    run_id = manifest.start_run("test")
    tip = date(2024, 6, 27)
    done_id = f"{tip.isoformat()}_{tip.isoformat()}-batch-0"
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

    submitted: list[str] = []
    widths: list[int] = []

    class _Pool:
        def __init__(self, *args, **kwargs):
            widths.append(kwargs["max_workers"])

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def submit(self, fn, task):
            submitted.append(task[6])

            class _Future:
                def result(self, timeout=None):
                    return {"rows_read": 1, "rows_written": 1, "batch_id": task[6]}

            return _Future()

    monkeypatch.setattr(
        "cnequity.orchestrator.worker_pool.ProcessPoolExecutor",
        lambda *args, **kwargs: _Pool(*args, **kwargs),
    )
    monkeypatch.setattr(
        "cnequity.orchestrator.worker_pool.as_completed",
        lambda futures, timeout=None: list(futures),
    )

    result = fetch_daily_bars_parallel(
        worker_config,
        ["600519.SH", "000001.SZ", "600000.SH"],
        tip,
        tip,
        run_id,
        "daily_bars",
    )

    assert submitted == [
        f"{tip.isoformat()}_{tip.isoformat()}-batch-1",
        f"{tip.isoformat()}_{tip.isoformat()}-batch-2",
    ]
    assert widths == [2]
    assert result["rows_written"] == 9
    assert manifest.get_batch(run_id, done_id)["status"] == "success"


def test_macos_uses_independent_tdx_thread_budget(tmp_path, monkeypatch):
    """A one-process macOS config can still fan out safe TDX batch threads."""
    import polars as pl

    # Constructing Config directly keeps this test independent of the example
    # template and makes the legacy/global-vs-specialized split explicit.
    from cnequity.config import Config

    cfg = Config(
        data_root=tmp_path / "data",
        workers=1,
        tdx_daily_workers=2,
        tdx_daily_backend="auto",
        batch_size=1,
    )
    init_data_layout(cfg)
    run_id = Manifest(cfg.manifest_path).start_run("test")
    calls: list[list[str]] = []

    def _fetch(symbols, start, end, **kwargs):
        calls.append(list(symbols))
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

    monkeypatch.setattr("cnequity.orchestrator.worker_pool.fetch_daily_bars", _fetch)
    monkeypatch.setattr(
        "cnequity.orchestrator.worker_pool.ProcessPoolExecutor",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("process pool used on macOS")),
    )

    result = fetch_daily_bars_parallel(
        cfg,
        ["600519.SH", "000001.SZ"],
        date(2024, 6, 27),
        date(2024, 6, 27),
        run_id,
        "daily_bars",
    )

    assert result["had_error"] is False
    assert result["rows_written"] == 2
    assert {symbol for batch in calls for symbol in batch} == {"600519.SH", "000001.SZ"}


def test_process_pool_never_forks_the_orchestrator(worker_config, monkeypatch):
    """A forked worker inherits polars' thread pool without its threads and hangs (#56)."""
    monkeypatch.setattr("sys.platform", "linux")
    init_data_layout(worker_config)
    run_id = Manifest(worker_config.manifest_path).start_run("test")
    contexts: list[object] = []

    class _CompletedPool:
        def __init__(self, *a, mp_context=None, **k):
            contexts.append(mp_context)

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def submit(self, fn, task):
            class _F:
                def result(self, timeout=None):
                    return {"rows_read": 1, "rows_written": 1}

            return _F()

    monkeypatch.setattr("cnequity.orchestrator.worker_pool.ProcessPoolExecutor", _CompletedPool)
    monkeypatch.setattr(
        "cnequity.orchestrator.worker_pool.as_completed",
        lambda futures, timeout=None: list(futures),
    )

    fetch_daily_bars_parallel(
        worker_config,
        ["600519.SH", "000001.SZ"],
        date(2024, 6, 27),
        date(2024, 6, 27),
        run_id,
        "daily_bars",
    )

    assert len(contexts) == 1
    assert contexts[0] is not None
    assert contexts[0].get_start_method() in {"forkserver", "spawn"}


def _hang_forever(task):
    """A worker that never returns and never heartbeats, like #56's."""
    time.sleep(3600)


def _slow_but_alive(task):
    """A worker slower than the stall window that keeps heartbeating."""
    symbols, start, end, _, dataset, run_id, batch_id = task[:7]
    manifest = Manifest(task[9])
    manifest.start_batch(
        run_id,
        batch_id,
        task_id=dataset,
        dataset=dataset,
        symbols=symbols,
        window_start=start,
        window_end=end,
    )
    for _ in range(12):
        time.sleep(0.25)
        manifest.touch_batch_heartbeat(run_id, batch_id)
    manifest.finish_batch(run_id, batch_id, "success", rows_read=1, rows_written=1)
    return {"rows_read": 1, "rows_written": 1, "batch_id": batch_id, "failed_symbols": []}


def _real_process_pool(worker_config, monkeypatch, worker):
    """Run the real process pool, with *worker* in place of the TDX fetch."""
    worker_config.tdx_daily_executor = lambda: "process"
    worker_config.batch_stall_seconds = 1
    monkeypatch.setattr("cnequity.orchestrator.worker_pool._worker_fetch_batch", worker)
    init_data_layout(worker_config)
    manifest = Manifest(worker_config.manifest_path)
    run_id = manifest.start_run("test")
    started = time.monotonic()
    result = fetch_daily_bars_parallel(
        worker_config,
        ["600519.SH", "000001.SZ"],
        date(2024, 6, 27),
        date(2024, 6, 28),
        run_id,
        "daily_bars",
    )
    return result, manifest.get_batches_for_run(run_id), time.monotonic() - started


def test_stalled_pool_is_killed_and_retried_in_process(worker_config, monkeypatch, caplog):
    """A hung worker used to hold the run at "still working" forever (#56)."""
    with caplog.at_level(logging.WARNING, logger="cnequity.orchestrator.worker_pool"):
        result, batches, elapsed = _real_process_pool(worker_config, monkeypatch, _hang_forever)

    assert elapsed < 60
    assert "no worker reported progress" in caplog.text
    assert "likely OOM" not in caplog.text
    assert result["had_error"] is False
    assert result["rows_written"] == 4  # mock rows from the in-process retry
    assert [b["status"] for b in batches] == ["success", "success"]


def test_slow_pool_that_heartbeats_is_not_killed(worker_config, monkeypatch, caplog):
    with caplog.at_level(logging.WARNING, logger="cnequity.orchestrator.worker_pool"):
        result, batches, elapsed = _real_process_pool(worker_config, monkeypatch, _slow_but_alive)

    assert elapsed > 2  # outlived the one-second stall window
    assert "no worker reported progress" not in caplog.text
    assert result["rows_written"] == 2  # the workers' own rows, not a retry's
    assert [b["status"] for b in batches] == ["success", "success"]


def test_pool_context_falls_back_to_spawn_without_a_forkserver(monkeypatch):
    """Windows CI: tests patch sys.platform to linux, which makes
    get_all_start_methods() list forkserver on an interpreter that cannot build it."""
    import multiprocessing

    from cnequity.orchestrator.worker_pool import _process_pool_context

    real = multiprocessing.get_context

    def without_forkserver(method=None):
        if method == "forkserver":
            raise ValueError("cannot find context for 'forkserver'")
        return real(method)

    monkeypatch.setattr(multiprocessing, "get_context", without_forkserver)
    monkeypatch.setattr("sys.platform", "linux")

    assert _process_pool_context().get_start_method() == "spawn"
