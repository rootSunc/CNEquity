"""Long fetches have to say what they are doing.

`asl init` runs for hours. Before this it printed nothing until the closing
JSON, which is indistinguishable from hung — and a process that looks hung gets
killed, losing the hours it had already banked.
"""

from __future__ import annotations

import logging

from ashare_lake.orchestrator.worker_pool import _hms


def test_durations_read_as_durations():
    assert _hms(9) == "9s"
    assert _hms(75) == "1m15s"
    assert _hms(7245) == "2h00m"


def test_progress_is_logged_per_batch(caplog, config, monkeypatch):
    """One line per batch, from the parent, so serial and pooled runs read the
    same way."""
    from ashare_lake.orchestrator import worker_pool

    monkeypatch.setattr(
        worker_pool,
        "fetch_daily_bars",
        lambda *a, **k: __import__("polars").DataFrame(
            {"symbol": ["600519.SH"], "trade_date": [__import__("datetime").date(2026, 7, 31)]}
        ),
    )
    monkeypatch.setattr(worker_pool, "normalize_with_source", lambda df, **k: df)
    monkeypatch.setattr(worker_pool.StagingWriter, "write_batch", lambda *a, **k: None)

    config.workers = 1
    config.batch_size = 1
    with caplog.at_level(logging.INFO, logger="ashare_lake.orchestrator.worker_pool"):
        worker_pool.fetch_daily_bars_parallel(
            config,
            ["600519.SH", "000001.SZ", "300750.SZ"],
            __import__("datetime").date(2026, 7, 30),
            __import__("datetime").date(2026, 7, 31),
            "run-1",
        )
    lines = [r.message for r in caplog.records if "batches" in r.message]
    assert len(lines) == 3
    assert "1/3 batches" in lines[0] and "3/3 batches" in lines[-1]
    assert "left" in lines[0]


def test_a_failed_batch_still_advances_the_counter(caplog, config, monkeypatch):
    """Otherwise a run with failures looks stalled at 4/54 forever."""
    from ashare_lake.orchestrator import worker_pool

    def _boom(*a, **k):
        raise RuntimeError("source down")

    monkeypatch.setattr(worker_pool, "fetch_daily_bars", _boom)
    config.workers = 1
    config.batch_size = 1
    with caplog.at_level(logging.INFO, logger="ashare_lake.orchestrator.worker_pool"):
        out = worker_pool.fetch_daily_bars_parallel(
            config,
            ["600519.SH", "000001.SZ"],
            __import__("datetime").date(2026, 7, 30),
            __import__("datetime").date(2026, 7, 31),
            "run-2",
        )
    lines = [r.message for r in caplog.records if "batches" in r.message]
    assert "2/2 batches" in lines[-1]
    assert "FAILED" in lines[-1]
    assert out["had_error"] is True


def test_quiet_silences_progress_but_not_warnings():
    from ashare_lake.cli.main import _progress_logging

    _progress_logging(quiet=True)
    assert logging.getLogger().level == logging.WARNING
    _progress_logging()
    assert logging.getLogger().level == logging.INFO


def test_http_clients_stay_quiet():
    """httpx logs a line per request; a full-market sweep would bury the
    progress this exists to surface."""
    from ashare_lake.cli.main import _progress_logging

    _progress_logging()
    assert logging.getLogger("httpx").level == logging.WARNING
