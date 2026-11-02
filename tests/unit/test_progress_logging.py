"""Long fetches have to say what they are doing.

`cne init` runs for hours. Before this it printed nothing until the closing
JSON, which is indistinguishable from hung — and a process that looks hung gets
killed, losing the hours it had already banked.
"""

from __future__ import annotations

import contextlib
import logging
from datetime import date

import polars as pl
import pytest

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
        raise ConnectionError("source down")

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


def test_heartbeat_adds_what_a_step_says_it_is_waiting_on(caplog):
    """#56 showed only "still working: daily_bars" for 23 minutes."""
    from cnequity import progress

    def broken():
        raise RuntimeError("manifest locked")

    with caplog.at_level(logging.INFO, logger="cnequity.progress"):
        with progress.step_scope("daily_bars"):
            with progress.detail_scope(lambda: "batch-17 quiet for 9m"):
                with progress.detail_scope(broken), progress.detail_scope(lambda: None):
                    progress._beat_once(interval_seconds=0.0)
            progress._beat_once(interval_seconds=0.0)
    messages = [r.message for r in caplog.records]
    assert messages.count("batch-17 quiet for 9m") == 1  # gone once its scope ends
    assert sum("still working" in m for m in messages) == 2


def test_daily_bars_heartbeat_names_the_quietest_batch(config):
    from cnequity.orchestrator.manifest import Manifest
    from cnequity.orchestrator.worker_pool import _describe_in_flight
    from cnequity.storage.layout import init_data_layout

    init_data_layout(config)
    manifest = Manifest(config.manifest_path)
    run_id = manifest.start_run("test")
    describe = _describe_in_flight(manifest, run_id, "daily_bars")
    assert describe() is None  # nothing in flight yet
    for batch_id, symbols in [
        ("batch-0", ["600519.SH"]),
        ("batch-1", ["000001.SZ", "000002.SZ", "000004.SZ", "000005.SZ"]),
    ]:
        manifest.start_batch(
            run_id,
            batch_id,
            task_id="daily_bars",
            dataset="daily_bars",
            symbols=symbols,
            window_start="2024-06-27",
            window_end="2024-06-28",
        )
    with manifest._connect() as conn:
        conn.execute(
            "UPDATE ingestion_batches SET heartbeat_at = ? WHERE batch_id = 'batch-1'",
            ("2000-01-01T00:00:00+00:00",),
        )
    line = str(describe())
    assert "2 batch(es) in flight" in line
    assert "batch-1" in line and "batch-0" not in line
    assert "000001.SZ, 000002.SZ, 000004.SZ …" in line


@pytest.mark.skipif(not hasattr(__import__("signal"), "SIGUSR1"), reason="no SIGUSR1")
def test_sigusr1_dumps_the_stack_of_a_blocked_run():
    """Where a stuck run is stuck, without installing py-spy first (#56)."""
    import signal
    import subprocess
    import sys
    import time

    code = (
        "import sys, threading\n"
        "from cnequity.progress import enable_stack_dumps\n"
        "assert enable_stack_dumps()\n"
        "def stuck_in_this_frame():\n"
        "    print('ready', flush=True)\n"
        "    threading.Event().wait(30)\n"
        "stuck_in_this_frame()\n"
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", code], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    try:
        assert proc.stdout.readline().strip() == "ready"
        time.sleep(0.2)
        proc.send_signal(signal.SIGUSR1)
        time.sleep(0.5)
    finally:
        proc.kill()
    _, stderr = proc.communicate(timeout=10)
    assert "stuck_in_this_frame" in stderr


def test_heartbeat_says_how_to_dump_stacks_once(caplog, monkeypatch):
    from cnequity import progress

    monkeypatch.setattr(progress, "_stack_dumps", True)
    monkeypatch.setattr(progress, "_hinted", False)
    with caplog.at_level(logging.INFO, logger="cnequity.progress"):
        with progress.step_scope("daily_bars"):
            progress._beat_once(interval_seconds=0.0)
            progress._beat_once(interval_seconds=0.0)
    assert sum("kill -USR1" in r.message for r in caplog.records) == 1


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


def test_the_heartbeat_thread_never_calls_time_sleep():
    """The thread must not be throttled by anything a test can replace.

    It used to be `while True: time.sleep(5.0)`. `module.time` is the shared
    `time` module, so a test faking `time.sleep` to capture its arguments —
    `test_retry_hardening` does exactly that — replaced it process-wide. The
    throttle became a no-op `list.append` and the thread spun, putting tens of
    millions of entries into that test's list. It only broke when the window
    happened to span the thread's wake-up, so it flaked instead of failing.
    """
    import threading
    import time

    from cnequity import progress

    calls: list[float] = []
    real_sleep = time.sleep
    progress.start_heartbeat(interval_seconds=0.05)
    try:
        time.sleep = calls.append  # type: ignore[assignment]
        # Several poll intervals: a sleep-throttled loop would be spinning by
        # now, and even one honest call would show up here.
        real_sleep(0.3)
    finally:
        time.sleep = real_sleep  # type: ignore[assignment]
        progress.stop_heartbeat()

    assert calls == []
    assert not [t for t in threading.enumerate() if t.name == "cne-heartbeat"]


def test_stop_heartbeat_lets_a_later_start_begin_again():
    """`cne init` reconfigures logging and starts the heartbeat a second time."""
    import threading

    from cnequity import progress

    def running() -> list[threading.Thread]:
        return [t for t in threading.enumerate() if t.name == "cne-heartbeat"]

    progress.stop_heartbeat()  # idempotent when nothing is running
    progress.start_heartbeat(interval_seconds=0.05)
    assert len(running()) == 1
    progress.stop_heartbeat()
    assert not running()
    progress.start_heartbeat(interval_seconds=0.05)
    assert len(running()) == 1
    progress.stop_heartbeat()
    assert not running()


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
    from them said 34m on a run that took 10m.

    The clock is driven by the test rather than read from the platform. Six
    mocked batches finish inside one tick of the Windows monotonic clock
    (~15.6ms), so `per_batch` came out as exactly zero and the ETA was
    correctly suppressed — failing a test that was asking about the lane rule,
    not about timer resolution.
    """
    import itertools

    from cnequity.orchestrator import worker_pool

    ticks = itertools.count(start=1000.0, step=0.5)
    monkeypatch.setattr(worker_pool.time, "monotonic", lambda: next(ticks))
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


@pytest.mark.usefixtures("chinese_output")
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
        assert "日志写到" in capsys.readouterr().err
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
    assert "cne run retry --run-id run-7" in text
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


#: The groups whose commands reach a source or rewrite the lake — the ones
#: where a user can be left staring at a silent terminal. Named per group, not
#: per command, so a new subcommand is covered the day it lands.
FETCHING_GROUPS: tuple[str, ...] = ("backfill", "run", "ths-official")


def _subcommands(group_name: str):
    import click

    from cnequity.cli.main import cli

    entry = cli.get_command(click.Context(cli), group_name)
    assert entry is not None, f"`cne {group_name}` is not registered"
    if not isinstance(entry, click.Group):
        return {group_name: entry}
    ctx = click.Context(entry)
    return {
        f"{group_name} {name}": entry.get_command(ctx, name) for name in entry.list_commands(ctx)
    }


@pytest.mark.parametrize(
    "command", sorted(name for group in FETCHING_GROUPS for name in _subcommands(group))
)
def test_fetching_commands_persist_job_logs(tmp_path, monkeypatch, command):
    """Exercise each CLI entry point; a failing offline job must leave its log."""
    from click.testing import CliRunner

    from cnequity.cli.main import cli
    from cnequity.config.bootstrap import path_for_toml

    # Enumerating the live command groups above also catches a new command
    # without an offline scenario here. Only the work is stubbed, not logging.
    scenarios = {
        "backfill": (["daily_bars"], "cnequity.cli.backfill_cmds._backfill_once"),
        "run daily": ([], "cnequity.orchestrator.engine.JobEngine.run_job"),
        "run events": ([], "cnequity.orchestrator.engine.JobEngine.run_job"),
        "run retry": (["--run-id", "offline"], "cnequity.cli.run_cmds._retry_single_run"),
        "run compact": (["--run-id", "offline"], "cnequity.orchestrator.engine.JobEngine.run_step"),
        "run clean": (["--dry-run"], "cnequity.cli.maintain_cmds.clean_staging"),
        "run maintain": ([], "cnequity.cli.schedule_cmds.subprocess.run"),
        "ths-official capture": (
            ["--what", "corporate-actions"],
            "cnequity.steps.fundamentals.snapshot_corporate_actions_ths_official",
        ),
        "ths-official backfill": (
            [],
            "cnequity.steps.fundamentals.backfill_statement_gap_ths_official",
        ),
        "ths-official repair-bars": ([], "cnequity.steps.bars.repair_deep_history_ths_official"),
        "ths-official resource-sectors": (
            [],
            "cnequity.steps.rotation.resource_sector_bars_ths_official",
        ),
    }
    argv, target = scenarios[command]
    marker = f"offline job reached: {command}"

    class OfflineJobFailure(RuntimeError):
        pass

    def job(*args, **kwargs):
        logging.getLogger("cnequity.test_job").info(marker)
        raise OfflineJobFailure("controlled offline failure")

    monkeypatch.setattr(target, job)
    monkeypatch.delenv("CNE_LOG_DIR", raising=False)
    monkeypatch.setenv("HITHINK_FINANCE_API_KEY", "offline-test-key")
    lake = tmp_path / "lake"
    cfg = tmp_path / "test.toml"
    cfg.write_text(
        f'[data]\nroot = "{path_for_toml(lake)}"\n'
        "[orchestrator]\nworkers = 1\n"
        '[[job.daily.waves]]\nname = "bars"\nsteps = ["daily_bars"]\n'
        '[job.events.groups.news]\nsteps = ["news_headlines"]\n'
        "[sources.ths_official]\nenabled = true\nbackfill = true\nverify = true\n",
        encoding="utf-8",
    )
    root = logging.getLogger()
    prior_handlers = list(root.handlers)
    prior_level = root.level
    package = logging.getLogger("cnequity")
    prior_package_level = package.level
    try:
        result = CliRunner().invoke(cli, [*command.split(), *argv, "--config", str(cfg)])
        assert isinstance(result.exception, OfflineJobFailure), result.output
        logs = list((lake / "logs").glob("*.log"))
        assert logs, result.output
        assert any(marker in path.read_text(encoding="utf-8") for path in logs)
    finally:
        from cnequity.progress import stop_heartbeat

        stop_heartbeat()
        for handler in root.handlers[:]:
            if handler not in prior_handlers:
                root.removeHandler(handler)
                handler.close()
        root.handlers = prior_handlers
        root.setLevel(prior_level)
        package.setLevel(prior_package_level)


def test_a_job_log_keeps_info_under_a_host_root_at_warning(tmp_path, monkeypatch):
    """A host that configured logging at WARNING still gets the job's progress."""
    from cnequity.cli._shared import attach_log_file
    from cnequity.config import Config

    monkeypatch.delenv("CNE_LOG_DIR", raising=False)
    root = logging.getLogger()
    package = logging.getLogger("cnequity")
    prior = (list(root.handlers), root.level, package.level)
    host = logging.StreamHandler()
    root.handlers = [host]
    root.setLevel(logging.WARNING)
    package.setLevel(logging.NOTSET)
    try:
        path = attach_log_file(Config(data_root=tmp_path), "offline", quiet=True)
        logging.getLogger("cnequity.test_job").warning("kept")
        attach_log_file(Config(data_root=tmp_path), "offline-info")
        logging.getLogger("cnequity.test_job").info("progress line")
        assert path is not None
        written = "".join(p.read_text(encoding="utf-8") for p in (tmp_path / "logs").glob("*.log"))
        assert "progress line" in written
        # The host's own root level is left as it was.
        assert root.level == logging.WARNING
    finally:
        from cnequity.progress import stop_heartbeat

        stop_heartbeat()
        for handler in root.handlers[:]:
            if handler is not host:
                handler.close()
        root.handlers = prior[0]
        root.setLevel(prior[1])
        package.setLevel(prior[2])


@pytest.mark.usefixtures("chinese_output")
def test_the_log_notice_never_contaminates_the_json_on_stdout(tmp_path, monkeypatch):
    """`cne ... | jq` has to keep working after a command learns to log.

    Wiring `attach_log_file` into a command that prints JSON broke two tests
    that read `result.output` — stdout and stderr interleaved. The notice
    belongs on stderr precisely so the machine-readable half stays clean, and
    that is the property worth pinning, not the two call sites that noticed.
    """
    import json

    from click.testing import CliRunner

    from cnequity.cli.main import cli
    from cnequity.config.bootstrap import path_for_toml

    monkeypatch.setenv("HITHINK_FINANCE_API_KEY", "sk-test")
    monkeypatch.setattr(
        "cnequity.steps.fundamentals.backfill_statement_gap_ths_official",
        lambda config, run_id, **kwargs: {"rows_read": 0, "rows_written": 0},
    )
    config = tmp_path / "cnequity.toml"
    config.write_text(
        f'[data]\nroot = "{path_for_toml(tmp_path / "lake")}"\n\n'
        "[sources.ths_official]\nenabled = true\nverify = true\nbackfill = true\n"
    )

    result = CliRunner().invoke(cli, ["ths-official", "backfill", "--config", str(config)])

    assert result.exit_code == 0, result.output
    assert "日志写到" in result.stderr
    assert "日志写到" not in result.stdout
    json.loads(result.stdout)  # the contract: stdout parses on its own


def test_every_command_gets_process_logging(caplog):
    """Wired once at the root, so no command can be added without it.

    Before this, `cne init` and the fetching commands had progress output and
    everything else ran silent: a slow `cne verify` or a `snapshot export`
    hashing gigabytes looked hung, and a library warning during a `status` went
    nowhere at all.
    """
    import logging

    import click

    from cnequity.cli._root import SectionedGroup
    from cnequity.cli.main import cli

    # The two that own their logging are declared, not incidental.
    assert SectionedGroup.OWNS_ITS_LOGGING == {"mcp", "serve"}

    ctx = click.Context(cli)
    for name in SectionedGroup.OWNS_ITS_LOGGING:
        assert cli.get_command(ctx, name) is not None, f"{name} is no longer a command"

    # Root-level wiring leaves the pipeline's own INFO records reaching a handler.
    logging.getLogger().handlers.clear()
    cli.__class__._wire_process_logging(cli, click.Context(cli))
    assert logging.getLogger().handlers, "no handler installed for the pipeline's records"
    assert logging.getLogger().level <= logging.INFO


@contextlib.contextmanager
def _capture_cli_records():
    """Collect `cnequity.cli` records directly.

    `caplog` puts its handler on the root logger, and the CLI configures
    logging with `basicConfig(force=True)` — which replaces root handlers, so
    caplog's is gone before the command runs. A handler on this logger is not
    touched by that.
    """
    import logging

    records: list[logging.LogRecord] = []

    class _Collect(logging.Handler):
        def emit(self, record):
            records.append(record)

    logger = logging.getLogger("cnequity.cli")
    handler = _Collect(level=logging.DEBUG)
    logger.addHandler(handler)
    previous = logger.level
    logger.setLevel(logging.DEBUG)
    try:
        yield records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous)


def _lake_config(tmp_path):
    cfg = tmp_path / "cnequity.toml"
    cfg.write_text(f'[data]\nroot = "{(tmp_path / "lake").as_posix()}"\n', encoding="utf-8")
    return str(cfg)


@pytest.mark.usefixtures("chinese_output")
@pytest.mark.parametrize(
    ("argv", "level", "needle"),
    [
        (["verify", "--dataset", "nope"], "ERROR", "未知数据集"),
        (
            ["backfill", "daily_bars", "--start", "2026-01-02", "--end", "2026-01-01"],
            "ERROR",
            "--start 必须早于或等于 --end",
        ),
        (["run", "daily", "--group", "nosuch"], "ERROR", "未知调度组"),
    ],
)
def test_a_failure_becomes_a_log_record(argv, level, needle, tmp_path):
    """Click prints `Error:` and stops; a scheduled run reads the log, not stderr.

    A failure that never became a record left a log file whose last line is
    whatever the command happened to be doing when it died.
    """
    from click.testing import CliRunner

    from cnequity.cli.main import cli

    cfg = _lake_config(tmp_path)
    with _capture_cli_records() as records:
        CliRunner().invoke(cli, [*argv, "--config", cfg])
    assert records, f"{argv} failed without a log record"
    assert any(r.levelname == level and needle in r.getMessage() for r in records), [
        (r.levelname, r.getMessage()) for r in records
    ]


def test_one_failure_makes_one_record(tmp_path):
    """A nested command must not be recorded once per group it passed through."""
    from click.testing import CliRunner

    from cnequity.cli.main import cli

    cfg = _lake_config(tmp_path)
    with _capture_cli_records() as records:
        CliRunner().invoke(cli, ["run", "daily", "--group", "nosuch", "--config", cfg])
    assert len(records) == 1, [r.getMessage() for r in records]
    assert records[0].getMessage().startswith("run daily:"), records[0].getMessage()


@pytest.mark.parametrize(
    "argv",
    [
        ["status"],
        ["verify"],
        ["run", "clean", "--dry-run"],
        ["contract", "show", "--dataset", "daily_bars"],
        ["sources", "resilience"],
    ],
)
def test_logging_never_lands_on_stdout(argv, tmp_path):
    """stdout carries the command's answer; logging goes to stderr.

    Wiring logging into every command would otherwise break every machine
    caller at once — several of these print JSON that is piped straight into
    `json.loads`, and one stray line is a parse error with no clue where it
    came from.
    """
    from click.testing import CliRunner

    from cnequity.cli.main import cli

    cfg = _lake_config(tmp_path)
    CliRunner().invoke(
        cli,
        [
            "init",
            "--profile",
            "sample",
            "--data-root",
            str(tmp_path / "lake"),
            "--config-out",
            cfg,
            "--days",
            "3",
        ],
    )
    result = CliRunner().invoke(cli, [*argv, "--config", cfg])
    assert "Logging to " not in result.stdout, result.stdout[:200]
    for marker in (" INFO ", " WARNING ", " ERROR ", "cnequity.cli:"):
        assert marker not in result.stdout, f"{marker!r} on stdout: {result.stdout[:200]}"


def test_the_cli_does_not_hijack_a_host_applications_logging():
    """Importing this CLI and running a query must leave the host's logging alone.

    Wiring `_progress_logging` at the root gave every command `force=True`,
    which replaces the root handlers — so a `contract show` called from inside
    another program destroyed that program's logging. The commands that own the
    terminal for hours still take over; a query has no business doing so.
    """
    import io
    import logging
    import threading

    from click.testing import CliRunner

    from cnequity.cli.main import cli

    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    root = logging.getLogger()
    root.addHandler(handler)
    previous = root.level
    root.setLevel(logging.DEBUG)
    try:
        result = CliRunner().invoke(cli, ["contract", "show", "--dataset", "daily_bars"])
        assert result.exit_code == 0, result.output
        assert handler in root.handlers, "the host's handler was replaced"
        assert not [t for t in threading.enumerate() if t.name == "cne-heartbeat"], (
            "a heartbeat thread was left in the host process"
        )
    finally:
        root.removeHandler(handler)
        root.setLevel(previous)


@pytest.mark.parametrize(
    ("argv", "needle"),
    [
        (["--bogus-flag"], "No such option"),
        (["nosuchcmd"], "No such command"),
        (["status", "--bogus"], "No such option"),
        (["run", "nosuch"], "No such command"),
        (["verify", "--runs", "--days", "abc"], "not a valid integer"),
        (["backfill"], "Missing argument"),
    ],
)
def test_every_class_of_failure_leaves_exactly_one_record(argv, needle, tmp_path):
    """Including the group's own options, which `invoke` never sees.

    `cne --bogus-flag` fails while Click builds the context, before any command
    is dispatched — so the record for it has to come from `make_context`.
    """
    from click.testing import CliRunner

    from cnequity.cli.main import cli

    with _capture_cli_records() as records:
        CliRunner().invoke(cli, argv)
    assert len(records) == 1, [r.getMessage() for r in records]
    assert needle in records[0].getMessage(), records[0].getMessage()


def test_the_interior_gap_message_says_what_to_do(tmp_path):
    """The gate that ends `cne backfill daily_bars` on a small lake.

    It said only "593 interior symbol×session key(s) remain absent" — a count
    with nothing to act on. Asserted on the message the helper produces rather
    than on the source that calls it: the first version matched
    `_unresolved_key_remedy(` inside the finding literal and broke the moment
    the call moved into a local, which says nothing about what a user reads.
    """
    from cnequity.config import Config
    from cnequity.steps.bars import _unresolved_key_remedy

    cfg = Config(data_root=tmp_path / "lake")
    remedy = _unresolved_key_remedy(
        cfg, "run-7", {"000001.SZ", "600519.SH"}, date(2026, 9, 1), date(2026, 9, 16)
    )
    message = (
        "daily_bars 2026-09-01..2026-09-16: 593 interior symbol×session key(s) "
        f"remain absent; refusing to checkpoint{remedy}"
    )

    assert "findings" in message, "the findings file is not named"
    assert "cne sources probe" in message
    assert "cne run retry --run-id run-7" in message
    assert "cne backfill daily_bars --symbols" in message


def test_a_staged_step_names_each_call_as_it_starts(caplog):
    from cnequity.progress import stage_progress

    log = logging.getLogger("cnequity.test.stages")
    with caplog.at_level(logging.INFO, logger="cnequity.test.stages"):
        stage = stage_progress(log, "instruments", 3)
        stage("TDX security list")
        stage("Beijing board", held=8015)
        stage("write", held=8103)
    messages = [r.message for r in caplog.records]
    assert [m.split(" · ")[0] for m in messages] == [
        "instruments 1/3 TDX security list",
        "instruments 2/3 Beijing board",
        "instruments 3/3 write",
    ]
    assert messages[1].endswith("8015 so far")


def test_the_instruments_step_is_not_silent(tmp_path, monkeypatch, caplog):
    """It ran 234s on a full-market init without one log line of its own."""
    from cnequity.config import Config
    from cnequity.steps import reference

    cfg = Config(
        data_root=tmp_path / "data",
        tdx_allow_mock=True,
        sources={"bse": False, "eastmoney": False, "exchange": False, "baostock": False},
    )
    monkeypatch.setattr(reference, "write_simple", lambda *a, **k: {"rows_written": 4})
    with caplog.at_level(logging.INFO, logger="cnequity.steps.reference"):
        reference.step_instruments(cfg, date(2026, 9, 15), "run-1", {})
    stages = [r.message.split(" · ")[0] for r in caplog.records if r.message.startswith("instr")]
    assert stages[0] == "instruments 1/5 TDX security list"
    assert stages[-1] == "instruments 5/5 write"


def test_a_click_failure_is_not_printed_twice(tmp_path):
    """Click echoes the error itself; the terminal log handler must not repeat it."""
    import subprocess
    import sys

    proc = subprocess.run(
        [sys.executable, "-c", "from cnequity.cli.main import cli; cli()", "status"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode != 0
    assert proc.stderr.count("No config found") == 1, proc.stderr
