"""Long fetches have to say what they are doing.

`cne init` runs for hours. Before this it printed nothing until the closing
JSON, which is indistinguishable from hung — and a process that looks hung gets
killed, losing the hours it had already banked.
"""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cnequity.orchestrator.worker_pool import _hms


def _one_bar() -> pl.DataFrame:
    return pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [date(2026, 7, 31)]})


def test_durations_read_as_durations():
    assert _hms(9) == "9s"
    assert _hms(75) == "1m15s"
    assert _hms(7245) == "2h00m"


def test_progress_is_logged_per_batch(caplog, config, monkeypatch):
    """One line per batch, from the parent, so serial and pooled runs read the
    same way."""
    from cnequity.orchestrator import worker_pool

    monkeypatch.setattr(
        worker_pool,
        "fetch_daily_bars",
        lambda *a, **k: __import__("polars").DataFrame(
            {"symbol": ["600519.SH"], "trade_date": [__import__("datetime").date(2026, 7, 31)]}
        ),
    )
    monkeypatch.setattr(worker_pool, "normalize_with_source", lambda df, *a, **k: df)
    monkeypatch.setattr(worker_pool.StagingWriter, "write_batch", lambda *a, **k: None)

    config.workers = 1
    config.batch_size = 1
    with caplog.at_level(logging.INFO, logger="cnequity.orchestrator.worker_pool"):
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
    # No ETA until a full round of lanes has drained: before that the estimate
    # is dominated by setup and by the lanes still filling.
    assert "left" not in lines[0]
    assert "left" in lines[-1]


def test_a_failed_batch_still_advances_the_counter(caplog, config, monkeypatch):
    """Otherwise a run with failures looks stalled at 4/54 forever."""
    from cnequity.orchestrator import worker_pool

    def _boom(*a, **k):
        raise RuntimeError("source down")

    monkeypatch.setattr(worker_pool, "fetch_daily_bars", _boom)
    config.workers = 1
    config.batch_size = 1
    with caplog.at_level(logging.INFO, logger="cnequity.orchestrator.worker_pool"):
        out = worker_pool.fetch_daily_bars_parallel(
            config,
            ["600519.SH", "000001.SZ"],
            __import__("datetime").date(2026, 7, 30),
            __import__("datetime").date(2026, 7, 31),
            "run-2",
        )
    lines = [r.message for r in caplog.records if "batches" in r.message]
    assert "2/2 batches" in lines[-1]
    # The scope, not the batch size: one symbol TDX had no rows for must not
    # read as a hundred lost names.
    assert "(1/1 symbols failed)" in lines[-1]
    assert out["had_error"] is True


def test_quiet_silences_progress_but_not_warnings():
    from cnequity.cli._shared import _progress_logging

    _progress_logging(quiet=True)
    assert logging.getLogger().level == logging.WARNING
    _progress_logging()
    assert logging.getLogger().level == logging.INFO


def test_http_clients_stay_quiet():
    """httpx logs a line per request; a full-market sweep would bury the
    progress this exists to surface."""
    from cnequity.cli._shared import _progress_logging

    _progress_logging()
    assert logging.getLogger("httpx").level == logging.WARNING


def test_scale_is_announced_before_the_first_batch_lands(caplog, config, monkeypatch):
    """A progress line needs a whole batch; the preamble needs nothing.

    It also states the shape of the cost: `--start D --end D` is still a
    per-symbol sweep, which is the surprise behind every one-day backfill that
    ran for half an hour.
    """
    from cnequity.orchestrator import worker_pool

    monkeypatch.setattr(worker_pool, "fetch_daily_bars", lambda *a, **k: _one_bar())
    monkeypatch.setattr(worker_pool, "normalize_with_source", lambda df, *a, **k: df)
    monkeypatch.setattr(worker_pool.StagingWriter, "write_batch", lambda *a, **k: None)

    config.workers = 1
    config.batch_size = 2
    with caplog.at_level(logging.INFO, logger="cnequity.orchestrator.worker_pool"):
        worker_pool.fetch_daily_bars_parallel(
            config,
            ["600519.SH", "000001.SZ", "300750.SZ"],
            date(2026, 7, 31),
            date(2026, 7, 31),
            "run-3",
        )
    preamble = [r.message for r in caplog.records if "batch(es)" in r.message]
    assert len(preamble) == 1
    assert "3 symbol(s)" in preamble[0]
    assert "2026-07-31..2026-07-31" in preamble[0]
    assert "2 batch(es) of up to 2" in preamble[0]


def test_a_step_says_it_started(caplog, tmp_path, monkeypatch):
    """Otherwise the first word about a step is the one reporting it done."""
    from cnequity.config import Config
    from cnequity.orchestrator.engine import JobEngine
    from cnequity.orchestrator.registry import StepEntry
    from cnequity.storage.layout import init_data_layout

    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run("init")
    monkeypatch.setattr(
        "cnequity.orchestrator.engine.get_step",
        lambda name: StepEntry(
            fn=lambda *args: {"rows_read": 0, "rows_written": 0},
            group="test",
            requires_workers=False,
        ),
    )

    with caplog.at_level(logging.INFO, logger="cnequity.orchestrator.engine"):
        engine._run_step("instruments", date(2026, 7, 31), run_id, {})

    messages = [r.message for r in caplog.records]
    assert "Step instruments starting" in messages
    assert any(m.startswith("Step instruments success") for m in messages)


def test_heartbeat_names_the_step_the_silence_belongs_to(caplog):
    """A long quiet step must not read as a hang."""
    from cnequity import progress

    with caplog.at_level(logging.INFO, logger="cnequity.progress"):
        with progress.step_scope("daily_bars"):
            progress._beat_once(interval_seconds=0.0)
    assert any("still working: daily_bars" in r.message for r in caplog.records)


def test_heartbeat_stays_quiet_when_nothing_is_running(caplog):
    """An idle process has nothing to reassure anyone about."""
    from cnequity import progress

    with caplog.at_level(logging.INFO, logger="cnequity.progress"):
        progress._beat_once(interval_seconds=0.0)
    assert not [r for r in caplog.records if "still working" in r.message]


def test_heartbeat_waits_out_a_talkative_run(caplog):
    """Anything else logging is proof of life; the heartbeat adds nothing."""
    from cnequity import progress

    with caplog.at_level(logging.INFO, logger="cnequity.progress"):
        with progress.step_scope("daily_bars"):
            progress._beat_once(interval_seconds=3600.0)
    assert not [r for r in caplog.records if "still working" in r.message]


def test_a_partial_batch_failure_reports_the_scope_not_the_batch(caplog, config, monkeypatch):
    """One missing symbol out of a hundred is not a hundred missing symbols."""
    from cnequity.orchestrator import worker_pool

    def _partial(symbols, *a, **k):
        raise worker_pool.DailyBarCoverageError(
            "daily_bars: TDX returned no rows for 1 requested symbol(s): 300750.SZ",
            missing_symbols=["300750.SZ"],
        )

    monkeypatch.setattr(worker_pool, "fetch_daily_bars", _partial)
    config.workers = 1
    config.batch_size = 3
    with caplog.at_level(logging.INFO, logger="cnequity.orchestrator.worker_pool"):
        out = worker_pool.fetch_daily_bars_parallel(
            config,
            ["600519.SH", "000001.SZ", "300750.SZ"],
            date(2026, 7, 31),
            date(2026, 7, 31),
            "run-5",
        )
    line = [r.message for r in caplog.records if "batches" in r.message][-1]
    assert "(1/3 symbols failed)" in line
    assert out["failed_symbols"] == ["300750.SZ"]


def test_eta_waits_for_a_full_round_of_lanes(caplog, config, monkeypatch):
    """With four lanes the first four batches land together; an estimate drawn
    from them said 34m on a run that took 10m."""
    from cnequity.orchestrator import worker_pool

    monkeypatch.setattr(worker_pool, "fetch_daily_bars", lambda *a, **k: _one_bar())
    monkeypatch.setattr(worker_pool, "normalize_with_source", lambda df, *a, **k: df)
    monkeypatch.setattr(worker_pool.StagingWriter, "write_batch", lambda *a, **k: None)
    monkeypatch.setattr(config, "tdx_daily_worker_count", lambda: 4)
    monkeypatch.setattr(config, "tdx_daily_executor", lambda: "thread")

    config.batch_size = 1
    with caplog.at_level(logging.INFO, logger="cnequity.orchestrator.worker_pool"):
        worker_pool.fetch_daily_bars_parallel(
            config,
            [f"60000{i}.SH" for i in range(6)],
            date(2026, 7, 31),
            date(2026, 7, 31),
            "run-6",
        )
    lines = [r.message for r in caplog.records if "batches" in r.message]
    assert "4 lane(s)" in [r.message for r in caplog.records if "batch(es)" in r.message][0]
    assert all("left" not in line for line in lines[:4])
    assert all("left" in line for line in lines[4:])


def test_a_run_leaves_a_log_file_behind(tmp_path, capsys):
    """Terminal progress only helps someone watching it; a run that failed
    after three hours has to leave something to read."""
    import logging as logging_mod
    from types import SimpleNamespace

    from cnequity.cli._shared import _progress_logging, attach_log_file

    _progress_logging()
    path = attach_log_file(SimpleNamespace(data_root=tmp_path / "lake"), "init")
    try:
        assert path is not None
        assert path.parent == tmp_path / "lake" / "logs"
        assert "Logging to" in capsys.readouterr().err
        logging_mod.getLogger("cnequity.test").info("a line worth keeping")
        assert "a line worth keeping" in path.read_text(encoding="utf-8")
    finally:
        for handler in list(logging_mod.getLogger().handlers):
            if isinstance(handler, logging_mod.FileHandler):
                logging_mod.getLogger().removeHandler(handler)
                handler.close()


def test_the_log_file_honours_cne_log_dir(tmp_path, monkeypatch):
    """The pipeline scripts already point this at their own directory."""
    import logging as logging_mod
    from types import SimpleNamespace

    from cnequity.cli._shared import attach_log_file

    monkeypatch.setenv("CNE_LOG_DIR", str(tmp_path / "elsewhere"))
    path = attach_log_file(SimpleNamespace(data_root=tmp_path / "lake"), "run-daily")
    try:
        assert path is not None
        assert path.parent == tmp_path / "elsewhere"
    finally:
        for handler in list(logging_mod.getLogger().handlers):
            if isinstance(handler, logging_mod.FileHandler):
                logging_mod.getLogger().removeHandler(handler)
                handler.close()


def test_an_unwritable_log_dir_does_not_stop_the_run(tmp_path, monkeypatch):
    """`cne init` is what creates the tree this would live in."""
    from types import SimpleNamespace

    from cnequity.cli._shared import attach_log_file

    blocker = tmp_path / "not-a-dir"
    blocker.write_text("")
    monkeypatch.setenv("CNE_LOG_DIR", str(blocker / "logs"))
    assert attach_log_file(SimpleNamespace(data_root=tmp_path / "lake"), "init") is None


def test_a_sweep_reports_first_every_hundredth_and_last(caplog):
    """The shape the TDX xdxr sweep already used, now available for one call."""
    from cnequity.progress import sweep_progress

    log = logging.getLogger("cnequity.test.sweep")
    with caplog.at_level(logging.INFO, logger="cnequity.test.sweep"):
        report = sweep_progress(log, "statements", 250)
        for done in range(1, 251):
            report(done)
    reported = [r.message for r in caplog.records]
    assert [m.split(" · ")[0] for m in reported] == [
        "statements 1/250 symbols",
        "statements 100/250 symbols",
        "statements 200/250 symbols",
        "statements 250/250 symbols",
    ]
    # One item in, elapsed is still mostly setup — no estimate is offered.
    assert "left" not in reported[0]
    assert "left" in reported[1]


def test_a_chunked_sweep_still_reports(caplog):
    """Reporting on landing exactly on a multiple would never fire for a sweep
    advancing 50 at a time past a threshold of 100."""
    from cnequity.progress import sweep_progress

    log = logging.getLogger("cnequity.test.chunked")
    with caplog.at_level(logging.INFO, logger="cnequity.test.chunked"):
        report = sweep_progress(log, "valuation_metrics", 300, every=100)
        for done in (75, 150, 225, 300):
            report(done)
    counts = [m.split(" ")[1] for m in (r.message for r in caplog.records)]
    # The first chunk to land reports too, whatever its size: it is the one
    # that proves the sweep is moving.
    assert counts == ["75/300", "150/300", "225/300", "300/300"]


def test_an_unresolved_key_failure_says_what_to_do(tmp_path):
    """A count is not an action. The findings file, the source probe, the retry
    and a scoped repair are all known at the point of the raise."""
    from cnequity.config import Config
    from cnequity.steps.bars import _unresolved_key_remedy

    cfg = Config(data_root=tmp_path / "data")
    text = _unresolved_key_remedy(
        cfg,
        "run-7",
        {"600519.SH", "000001.SZ", "300750.SZ", "601318.SH"},
        date(2026, 9, 15),
        date(2026, 9, 15),
    )
    assert str(cfg.meta_root / "quality" / "findings" / "run-7.json") in text
    assert "cne sources probe" in text
    assert "cne retry --run-id run-7" in text
    # A repair line has to stay pasteable: three keys and an ellipsis, not a
    # thousand symbols wrapped across the terminal.
    assert "--symbols 000001.SZ,300750.SZ,600519.SH,... " in text
    assert "--start 2026-09-15 --end 2026-09-15" in text


def test_a_one_session_backfill_is_told_the_window_is_free(caplog, config, monkeypatch):
    """One request returns up to 800 bars, so a month filled a day at a time
    pays for thirty sweeps and receives what one would have returned."""
    from cnequity.orchestrator import worker_pool

    monkeypatch.setattr(worker_pool, "fetch_daily_bars", lambda *a, **k: _one_bar())
    monkeypatch.setattr(worker_pool, "normalize_with_source", lambda df, *a, **k: df)
    monkeypatch.setattr(worker_pool.StagingWriter, "write_batch", lambda *a, **k: None)

    config.batch_size = 2
    config._backfill = True
    with caplog.at_level(logging.INFO, logger="cnequity.orchestrator.worker_pool"):
        worker_pool.fetch_daily_bars_parallel(
            config, ["600519.SH", "000001.SZ"], date(2026, 7, 31), date(2026, 7, 31), "run-8"
        )
    assert any("Backfill a range in one go" in r.message for r in caplog.records)


def test_the_daily_job_is_not_nagged_about_its_one_session(caplog, config, monkeypatch):
    """A one-session window is the daily job's whole point."""
    from cnequity.orchestrator import worker_pool

    monkeypatch.setattr(worker_pool, "fetch_daily_bars", lambda *a, **k: _one_bar())
    monkeypatch.setattr(worker_pool, "normalize_with_source", lambda df, *a, **k: df)
    monkeypatch.setattr(worker_pool.StagingWriter, "write_batch", lambda *a, **k: None)

    config.batch_size = 2
    config._backfill = False
    with caplog.at_level(logging.INFO, logger="cnequity.orchestrator.worker_pool"):
        worker_pool.fetch_daily_bars_parallel(
            config, ["600519.SH", "000001.SZ"], date(2026, 7, 31), date(2026, 7, 31), "run-9"
        )
    assert not any("Backfill a range in one go" in r.message for r in caplog.records)
