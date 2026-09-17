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
FETCHING_GROUPS: tuple[str, ...] = ("backfill", "run", "ths-official", "delisted")

#: Within those groups, the commands that only read and print. `delisted
#: status` summarises a catalogue already on disk; there is no interval during
#: which anyone could wonder whether it died.
READ_ONLY: frozenset[str] = frozenset({"delisted status"})


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


def test_the_read_only_exemptions_still_name_real_commands():
    """A stale exemption silently excuses whatever later takes that name."""
    registered = {name for group in FETCHING_GROUPS for name in _subcommands(group)}
    assert READ_ONLY <= registered, f"READ_ONLY names nothing registered: {READ_ONLY - registered}"


@pytest.mark.parametrize("group", FETCHING_GROUPS)
def test_every_fetching_command_wires_progress(group):
    """A command that runs for an hour must say so, in every group.

    The original fix reached `init`, `run daily`/`run events` and `backfill`
    and stopped there. `cne ths-official backfill` then ran 75 minutes writing
    nothing to the terminal and nothing to `logs/` — the same
    "完全不知道程序的死活" the issue was about, in a group the sweep had not
    looked at. `cne delisted backfill` had drifted further: an open-coded
    `logging.basicConfig` that silenced only httpx and wrote no log file at all,
    so neither the heartbeat nor the log tee reached it.
    """
    import inspect

    missing = [
        name
        for name, command in _subcommands(group).items()
        if name not in READ_ONLY
        and (
            "_progress_logging(" not in inspect.getsource(command.callback)
            or "attach_log_file(" not in inspect.getsource(command.callback)
        )
    ]
    assert not missing, (
        f"`cne {group}` subcommands {missing} run without progress logging; call "
        "_progress_logging() and attach_log_file() as the rest of the group does"
    )


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
    assert "Logging to" in result.stderr
    assert "Logging to" not in result.stdout
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


@pytest.mark.parametrize(
    ("argv", "level", "needle"),
    [
        (["verify", "--dataset", "nope"], "ERROR", "unknown dataset"),
        (
            ["backfill", "daily_bars", "--start", "2026-01-02", "--end", "2026-01-01"],
            "ERROR",
            "--start must be on or before --end",
        ),
        (["run", "daily", "--group", "nosuch"], "ERROR", "Unknown group"),
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
