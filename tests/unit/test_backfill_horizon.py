"""Backfill guards for horizon-limited and chunked datasets."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import click
import polars as pl
import pytest

from cnequity.cli import backfill_cmds
from cnequity.cli.main import cli
from cnequity.domain.datasets import DatasetSpec, get_dataset
from cnequity.domain.market_time import shanghai_today


def test_horizon_guard_refuses_a_window_the_source_cannot_serve():
    spec = get_dataset("minute_bars")
    today = shanghai_today()
    too_old = spec.earliest_available(today) - timedelta(days=1)
    with pytest.raises(click.ClickException) as excinfo:
        backfill_cmds._guard_history_horizon("minute_bars", too_old)
    message = str(excinfo.value)
    # The error has to say the data does not exist, not that the run failed —
    # otherwise it reads as a lake bug rather than a vendor limit.
    assert "older than the source horizon" in message
    assert str(spec.history_horizon_days) in message
    assert str(spec.earliest_available(today)) in message


def test_horizon_guard_allows_a_window_inside_the_horizon():
    inside = get_dataset("minute_bars").earliest_available(shanghai_today()) + timedelta(days=1)
    backfill_cmds._guard_history_horizon("minute_bars", inside)


def test_horizon_guard_is_a_no_op_without_a_horizon():
    # daily_bars has no vendor ceiling; a 2001 start must stay legal.
    backfill_cmds._guard_history_horizon("daily_bars", date(2001, 1, 1))
    backfill_cmds._guard_history_horizon("minute_bars", None)


@pytest.mark.parametrize("dataset", ["announcement_index", "regulatory_events"])
def test_cninfo_default_backfill_is_date_chunked_without_explicit_range(dataset, monkeypatch):
    from types import SimpleNamespace

    cfg = SimpleNamespace()
    seen: dict = {}

    def _chunked(config, name, start, end, chunk_days):
        seen.update(
            config=config,
            dataset=name,
            start=start,
            end=end,
            chunk_days=chunk_days,
        )
        return {"status": "success", "rows_written": 0}

    monkeypatch.setattr(backfill_cmds, "_backfill_chunked", _chunked)
    monkeypatch.setattr(backfill_cmds, "shanghai_today", lambda: date(2026, 8, 31))

    result = backfill_cmds._backfill_once(cfg, dataset)

    assert result["status"] == "success"
    assert seen == {
        "config": cfg,
        "dataset": dataset,
        "start": date(2010, 1, 1),
        "end": date(2026, 8, 31),
        "chunk_days": 31,
    }


def test_earliest_available_converts_trading_days_to_calendar_days():
    spec = DatasetSpec("x", tier="L1", history_horizon_days=242)
    # A year of sessions is a calendar year, not 242 calendar days.
    assert spec.earliest_available(date(2026, 8, 1)) == date(2025, 8, 1)
    assert DatasetSpec("y", tier="L1").earliest_available(date(2026, 8, 1)) is None


class FakeEngine:
    """Records the window each sub-run saw, via the config it is handed."""

    instances: list[FakeEngine] = []

    def __init__(self, cfg):
        self.cfg = cfg
        self.windows: list[tuple[date, date]] = []
        self.compacted: list[str] = []
        self.finished: list[tuple[str, str]] = []
        self.manifest = self
        FakeEngine.instances.append(self)

    def run_job(self, name, *, steps, backfill, finalize_run):
        self.windows.append((self.cfg._backfill_start, self.cfg._backfill_end))
        return {
            "run_id": f"run-{len(self.windows)}",
            "status": self._status(len(self.windows)),
            "rows_read": 10,
            "rows_written": 10,
        }

    def _status(self, index: int) -> str:
        return "success"

    def run_step(self, step, trade_date, run_id):
        self.compacted.append(run_id)
        return {"rows_written": 10}

    def finish_run(self, run_id, status, *args, **kwargs):
        self.finished.append((run_id, status))

    def aggregate_run_status(self, run_id):
        """The receipts behind the run status.

        A real run records one per dataset, and a non-core step that raised
        leaves the *run* degraded while its own receipt says failed. That
        receipt is what `_run_had_step_failure` reads, so a fake engine has to
        carry it too; by default there is no hidden failure.
        """
        return {"core_failures": [], "degraded_results": self._degraded_results(run_id)}

    def _degraded_results(self, run_id):
        return []


@pytest.fixture(autouse=True)
def _reset_engines():
    FakeEngine.instances.clear()
    yield
    FakeEngine.instances.clear()


def test_chunked_backfill_slices_the_window_and_compacts_each_slice(tmp_path, monkeypatch):
    monkeypatch.setattr(backfill_cmds, "JobEngine", FakeEngine)
    cfg = type("Cfg", (), {})()

    result = backfill_cmds._backfill_chunked(
        cfg, "minute_bars", date(2026, 7, 1), date(2026, 7, 25), chunk_days=10
    )

    engine = FakeEngine.instances[0]
    assert engine.windows == [
        (date(2026, 7, 1), date(2026, 7, 10)),
        (date(2026, 7, 11), date(2026, 7, 20)),
        (date(2026, 7, 21), date(2026, 7, 25)),
    ]
    # Every slice is drained to curated before the next one stages anything —
    # that is the whole point, since compact holds a run's staging in memory.
    assert engine.compacted == ["run-1", "run-2", "run-3"]
    assert result["status"] == "success"
    assert result["rows_written"] == 30
    assert len(result["slices"]) == 3


def test_chunked_backfill_stops_at_a_failed_slice_and_reports_where_to_resume(
    tmp_path, monkeypatch
):
    class FailingSecond(FakeEngine):
        def _status(self, index):
            return "failed" if index == 2 else "success"

    monkeypatch.setattr(backfill_cmds, "JobEngine", FailingSecond)
    cfg = type("Cfg", (), {})()

    result = backfill_cmds._backfill_chunked(
        cfg, "minute_bars", date(2026, 7, 1), date(2026, 7, 25), chunk_days=10
    )

    assert result["status"] == "failed"
    # The first slice stays in curated; the caller resumes from the one that broke.
    assert result["resume_from"] == date(2026, 7, 11)
    assert len(result["slices"]) == 2
    # The failed slice is compacted too — whatever it staged before failing
    # must not be stranded (see test_finish_backfill_run_compacts_on_failure).
    assert FailingSecond.instances[0].compacted == ["run-1", "run-2"]


def test_finish_backfill_run_compacts_on_failure():
    """A single (unchunked) backfill call that raises partway through — e.g.
    announcement_index, which is not date-chunked — must not strand whatever
    walk_day_backfill already flushed to staging just because the overall
    call's status is "failed". Measured in production: a run that flushed 21
    clean days before an exception on day 22 kept those 21 days sitting in
    staging, invisible to `load()`, until this was fixed."""
    engine = FakeEngine(cfg=None)
    result = {"run_id": "run-x", "status": "failed", "rows_read": 5, "rows_written": 5}

    out = backfill_cmds._finish_backfill_run(engine, result)

    assert out["compact"] == {"rows_written": 10}
    assert engine.compacted == ["run-x"]


def test_finish_backfill_run_surfaces_compact_warning():
    class WarningCompact(FakeEngine):
        def run_step(self, step, trade_date, run_id):
            self.compacted.append(run_id)
            return {"rows_written": 10, "status": "warning"}

    engine = WarningCompact(cfg=None)
    result = {"run_id": "run-warning", "status": "success", "rows_written": 5}

    out = backfill_cmds._finish_backfill_run(engine, result)

    assert out["status"] == "warning"
    assert engine.compacted == ["run-warning"]


def test_minute_bars_declares_symbol_chunking_not_date_chunking():
    # Tip-paged: date chunks re-walk tip→start every slice. Symbol chunks keep
    # one walk per name and still bound compact memory.
    assert get_dataset("minute_bars").backfill_chunk_symbols == 200
    assert get_dataset("minute_bars").backfill_chunk_days is None
    assert get_dataset("minute_bars_5m").backfill_chunk_symbols == 200
    assert get_dataset("daily_bars").backfill_chunk_days is None
    assert get_dataset("daily_bars").backfill_chunk_symbols is None


def test_all_intraday_scope_excludes_symbols_outside_listing_window(monkeypatch):
    from cnequity.steps import intraday

    monkeypatch.setattr(
        intraday,
        "instrument_metadata",
        lambda _config: pl.DataFrame(
            {
                "symbol": ["600519.SH", "600695.SH", "300001.SZ"],
                "list_date": [date(2001, 1, 1), date(1993, 1, 1), date(2027, 1, 1)],
                "delist_date": [None, date(2022, 6, 14), None],
            },
            schema={
                "symbol": pl.Utf8,
                "list_date": pl.Date,
                "delist_date": pl.Date,
            },
        ),
    )

    assert intraday._filter_all_scope_to_listed_symbols(
        object(),
        ["600519.SH", "600695.SH", "300001.SZ", "000001.SZ"],
        date(2026, 8, 19),
        date(2026, 8, 21),
    ) == ["600519.SH", "000001.SZ"]


def test_symbol_chunked_backfill_walks_full_window_per_symbol_batch(monkeypatch):
    monkeypatch.setattr(backfill_cmds, "JobEngine", FakeEngine)
    symbols = [f"{i:06d}.SH" for i in range(250)]
    monkeypatch.setattr("cnequity.steps.intraday.resolve_scope", lambda _cfg: symbols)
    cfg = type(
        "Cfg",
        (),
        {
            "minute_bars_scope": "index:000300.SH",
            "minute_bars_symbols": [],
        },
    )()

    result = backfill_cmds._backfill_symbol_chunked(
        cfg, "minute_bars", date(2026, 3, 11), date(2026, 8, 1), chunk_symbols=200
    )

    engine = FakeEngine.instances[0]
    # Two symbol batches, each covering the full requested window once.
    assert engine.windows == [
        (date(2026, 3, 11), date(2026, 8, 1)),
        (date(2026, 3, 11), date(2026, 8, 1)),
    ]
    assert engine.compacted == ["run-1", "run-2"]
    assert result["status"] == "success"
    assert result["rows_written"] == 20
    assert [c["symbols_from"] for c in result["chunks"]] == [1, 201]
    assert [c["symbols_to"] for c in result["chunks"]] == [200, 250]
    # Scope restored so a later step in the same process sees the original.
    assert cfg.minute_bars_scope == "index:000300.SH"
    assert cfg.minute_bars_symbols == []


def test_symbol_chunked_backfill_stops_and_reports_resume_symbol(monkeypatch):
    class FailingSecond(FakeEngine):
        def _status(self, index):
            return "failed" if index == 2 else "success"

    monkeypatch.setattr(backfill_cmds, "JobEngine", FailingSecond)
    symbols = [f"{i:06d}.SH" for i in range(250)]
    monkeypatch.setattr("cnequity.steps.intraday.resolve_scope", lambda _cfg: symbols)
    cfg = type(
        "Cfg",
        (),
        {"minute_bars_scope": "all", "minute_bars_symbols": []},
    )()

    result = backfill_cmds._backfill_symbol_chunked(
        cfg, "minute_bars", date(2026, 3, 11), date(2026, 8, 1), chunk_symbols=200
    )

    assert result["status"] == "failed"
    assert result["resume_from_symbol"] == "000200.SH"
    assert len(result["chunks"]) == 2
    # The failed chunk is compacted too — see test_finish_backfill_run_compacts_on_failure.
    assert FailingSecond.instances[0].compacted == ["run-1", "run-2"]


def _backfill_argv(dataset: str, *extra: str) -> list[str]:
    return ["backfill", dataset, "--config", "cfg.toml", *extra]


def test_symbols_flag_overrides_scope_for_intraday(tmp_path, monkeypatch):
    """A one-off pull must not require editing the config first."""
    seen: dict = {}

    class Cfg:
        minute_bars_enabled = False
        minute_bars_scope = "index:000300.SH"
        minute_bars_symbols: list[str] = []
        minute_bars_frequencies = ["1m"]

    cfg = Cfg()

    def fake_backfill(config, dataset):
        seen["cfg"] = config
        return {"status": "success", "rows_written": 0}

    monkeypatch.setattr(backfill_cmds, "_cfg", lambda _p: cfg)
    monkeypatch.setattr(backfill_cmds, "_backfill_once", fake_backfill)

    from click.testing import CliRunner

    result = CliRunner().invoke(
        cli,
        _backfill_argv("minute_bars_5m", "--symbols", "600519.sh, 000001.SZ"),
    )
    assert result.exit_code == 0, result.output
    assert cfg.minute_bars_scope == "watchlist"
    assert cfg.minute_bars_symbols == ["600519.SH", "000001.SZ"]
    # Enabled for this run, and the dataset's own frequency added, so a config
    # with intraday off still serves a deliberate one-off backfill.
    assert cfg.minute_bars_enabled is True
    assert "5m" in cfg.minute_bars_frequencies


def test_symbols_flag_scopes_daily_bar_repairs(tmp_path, monkeypatch):
    from types import SimpleNamespace

    cfg = SimpleNamespace()
    monkeypatch.setattr(backfill_cmds, "_cfg", lambda _p: cfg)
    monkeypatch.setattr(
        backfill_cmds,
        "_backfill_once",
        lambda config, dataset: {"status": "success", "rows_written": 0},
    )

    from click.testing import CliRunner

    result = CliRunner().invoke(cli, _backfill_argv("daily_bars", "--symbols", "600519.SH"))
    assert result.exit_code == 0, result.output
    assert cfg._backfill_symbols == ["600519.SH"]


def test_bse_tip_repair_requires_one_session_and_symbols(tmp_path, monkeypatch):
    from types import SimpleNamespace

    cfg = SimpleNamespace()
    monkeypatch.setattr(backfill_cmds, "_cfg", lambda _p: cfg)
    monkeypatch.setattr(
        backfill_cmds,
        "_backfill_once",
        lambda config, dataset: {"status": "success", "rows_written": 1},
    )

    from click.testing import CliRunner

    runner = CliRunner()
    missing_symbols = runner.invoke(
        cli,
        _backfill_argv(
            "daily_bars",
            "--start",
            "2026-08-21",
            "--end",
            "2026-08-21",
            "--bse-tip-repair",
        ),
    )
    assert missing_symbols.exit_code != 0
    assert "requires --symbols" in missing_symbols.output

    different_days = runner.invoke(
        cli,
        _backfill_argv(
            "daily_bars",
            "--start",
            "2026-08-20",
            "--end",
            "2026-08-21",
            "--symbols",
            "920000.BJ",
            "--bse-tip-repair",
        ),
    )
    assert different_days.exit_code != 0
    assert "same explicit --start and --end" in different_days.output


def test_symbols_flag_points_each_dataset_at_its_own_config_block():
    """The override must not write minute_bars keys for a trade_ticks run.

    It used to: the flag was hardcoded to `cfg.minute_bars_*`, so a
    `--symbols` tick pull silently scoped the wrong dataset and then fetched
    nothing.
    """
    from cnequity.config import Config

    cfg = Config(data_root=Path("/tmp/lake"))
    backfill_cmds._override_scope(cfg, "trade_ticks", ["600519.SH", "000001.SZ"])
    assert cfg.trade_ticks_enabled is True
    assert cfg.trade_ticks_scope == "watchlist"
    assert cfg.trade_ticks_symbols == ["600519.SH", "000001.SZ"]
    assert cfg.minute_bars_enabled is False

    backfill_cmds._override_scope(cfg, "minute_bars_5m", ["600519.SH"])
    assert cfg.minute_bars_enabled is True
    assert cfg.minute_bars_symbols == ["600519.SH"]
    assert "5m" in cfg.minute_bars_frequencies


def test_symbols_flag_lifts_the_tick_ceiling_for_a_hand_typed_list():
    """The ceiling stops an unnoticed sweep, not a list the user just typed."""
    from cnequity.config import Config

    cfg = Config(data_root=Path("/tmp/lake"))
    cfg.trade_ticks_max_symbols = 2
    backfill_cmds._override_scope(cfg, "trade_ticks", ["600519.SH", "000001.SZ", "300750.SZ"])
    assert cfg.trade_ticks_max_symbols == 3


def test_tick_horizon_guard_does_not_suggest_narrowing_the_scope():
    """A fixed floor has no narrower scope that reaches further back.

    The minute-bar message tells you to narrow the watchlist, because there the
    limit is a per-symbol bar count and an illiquid name does reach deeper.
    Repeating that advice here would send someone editing a setting that cannot
    help them.
    """
    with pytest.raises(click.ClickException) as excinfo:
        backfill_cmds._guard_history_horizon("trade_ticks", date(2023, 1, 1))
    message = str(excinfo.value)
    assert "history floor" in message
    assert "narrow" not in message
    assert "2024-01-02" in message


def test_top_holders_floor_is_the_pit_boundary_not_the_data_boundary():
    """2003 is where the *disclosure dates* start, not where the data starts.

    RPT_F10_EH_HOLDERS reaches into the 1990s, but it carries no NOTICE_DATE and
    borrows one from RPT_F10_EH_FREEHOLDERS, which serves 0 rows before 2003
    (13,853 in 2003). Rows that cannot borrow a date are dropped rather than
    stamped with the period end, so a backfill reaching further back fetches
    ~112k rows across 1999-2002 and writes none of them. Refusing the window is
    the difference between a clear message and four wasted hours.
    """
    from cnequity.domain.datasets import get_dataset

    assert get_dataset("top_holders").history_floor_date == date(2003, 1, 1)
    with pytest.raises(click.ClickException) as excinfo:
        backfill_cmds._guard_history_horizon("top_holders", date(2001, 1, 1))
    message = str(excinfo.value)
    assert "history floor" in message
    assert "2003-01-01" in message
    assert "narrow" not in message, "a fixed floor has no narrower scope that helps"


def test_northbound_flows_declares_the_exchange_opening_floor():
    spec = get_dataset("northbound_flows")
    assert spec.history_floor_date == date(2014, 11, 17)
    with pytest.raises(click.ClickException) as excinfo:
        backfill_cmds._guard_history_horizon("northbound_flows", date(2014, 11, 16))
    message = str(excinfo.value)
    assert "history floor" in message
    assert "2014-11-17" in message
    assert "narrow" not in message


def test_chunked_backfill_keeps_a_warning_status_across_later_successes(monkeypatch):
    class WarnsSecond(FakeEngine):
        def _status(self, index):
            return "warning" if index == 2 else "success"

    monkeypatch.setattr(backfill_cmds, "JobEngine", WarnsSecond)
    cfg = type("Cfg", (), {})()

    result = backfill_cmds._backfill_chunked(
        cfg, "minute_bars", date(2026, 7, 1), date(2026, 7, 25), chunk_days=10
    )
    # A warning does not stop the sweep, and a later success must not paper
    # over the earlier warning.
    assert result["status"] == "warning"
    assert len(result["slices"]) == 3


def test_cli_backfill_takes_the_symbol_chunked_path_when_both_dates_given(monkeypatch):
    import types

    from click.testing import CliRunner

    monkeypatch.setattr(backfill_cmds, "JobEngine", FakeEngine)
    symbols = [f"{i:06d}.SH" for i in range(250)]
    monkeypatch.setattr("cnequity.steps.intraday.resolve_scope", lambda _cfg: symbols)
    cfg = types.SimpleNamespace(
        minute_bars_scope="index:000300.SH",
        minute_bars_symbols=[],
        minute_bars_enabled=True,
        minute_bars_frequencies=["1m"],
    )
    monkeypatch.setattr(backfill_cmds, "_cfg", lambda _p: cfg)

    result = CliRunner().invoke(
        cli,
        _backfill_argv("minute_bars", "--start", "2026-07-01", "--end", "2026-07-25"),
    )
    assert result.exit_code == 0, result.output
    engine = FakeEngine.instances[0]
    # Tip-paged path: full window once per symbol batch (200 + 50), not date slices.
    assert engine.windows == [
        (date(2026, 7, 1), date(2026, 7, 25)),
        (date(2026, 7, 1), date(2026, 7, 25)),
    ]


def test_cli_backfill_exits_nonzero_when_the_result_is_not_success(monkeypatch):
    import types

    from click.testing import CliRunner

    class AllFail(FakeEngine):
        def _status(self, index):
            return "failed"

    monkeypatch.setattr(backfill_cmds, "JobEngine", AllFail)
    monkeypatch.setattr("cnequity.steps.intraday.resolve_scope", lambda _cfg: ["600519.SH"])
    cfg = types.SimpleNamespace(
        minute_bars_scope="watchlist",
        minute_bars_symbols=["600519.SH"],
        minute_bars_enabled=True,
        minute_bars_frequencies=["1m"],
    )
    monkeypatch.setattr(backfill_cmds, "_cfg", lambda _p: cfg)

    result = CliRunner().invoke(
        cli,
        _backfill_argv("minute_bars", "--start", "2026-07-01", "--end", "2026-07-10"),
    )
    assert result.exit_code == 1


class NonCoreStepRaised(FakeEngine):
    """A run where the step raised but the tier softened the run status.

    `aggregate_run_status` calls a non-core failure a degraded *run*: in the
    daily job the other datasets still landed. 35 of the registered steps are
    non-core, so a single-dataset sweep that read only the run status called
    this success.
    """

    def _status(self, index):
        return "degraded"

    def _degraded_results(self, run_id):
        return [{"dataset": "regulatory_events", "status": "failed"}]


def test_a_non_core_slice_that_raised_fails_the_sweep_instead_of_reporting_success(
    monkeypatch,
):
    monkeypatch.setattr(backfill_cmds, "JobEngine", NonCoreStepRaised)
    cfg = type("Cfg", (), {})()

    result = backfill_cmds._backfill_chunked(
        cfg, "regulatory_events", date(2026, 7, 1), date(2026, 7, 25), chunk_days=10
    )

    assert result["status"] == "failed"
    # Stop at the first slice rather than grinding through a systemic failure.
    assert len(result["slices"]) == 1
    assert result["resume_from"] == date(2026, 7, 1)
    assert FakeEngine.instances[-1].finished == [("run-1", "failed")]


def test_a_symbol_chunked_slice_that_raised_also_fails_the_sweep(monkeypatch):
    monkeypatch.setattr(backfill_cmds, "JobEngine", NonCoreStepRaised)
    monkeypatch.setattr("cnequity.steps.intraday.resolve_scope", lambda _cfg: ["600519.SH"])
    cfg = type("Cfg", (), {"minute_bars_scope": "", "minute_bars_symbols": []})()

    result = backfill_cmds._backfill_symbol_chunked(
        cfg, "minute_bars", date(2026, 7, 1), date(2026, 7, 25), 1
    )

    assert result["status"] == "failed"
    assert result["resume_from_symbol"] == "600519.SH"
    assert FakeEngine.instances[-1].finished == [("run-1", "failed")]


class DegradedWithoutFailure(FakeEngine):
    """A slice that genuinely had nothing to do — no receipt failed.

    `regulatory_events` reports this for a window that predates the
    announcements it derives from. The sweep must carry on to the slices that
    *are* covered, and must not call the overall result a success.
    """

    def _status(self, index):
        return "degraded" if index == 1 else "success"


def test_a_degraded_slice_keeps_the_sweep_going_but_not_its_success_claim(monkeypatch):
    monkeypatch.setattr(backfill_cmds, "JobEngine", DegradedWithoutFailure)
    cfg = type("Cfg", (), {})()

    result = backfill_cmds._backfill_chunked(
        cfg, "regulatory_events", date(2026, 7, 1), date(2026, 7, 25), chunk_days=10
    )

    assert result["status"] == "degraded"
    assert len(result["slices"]) == 3
    assert result["resume_from"] is None


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("daily_bar", "Did you mean: daily_bars"),
        # A step name is not a backfill target; the docs say `cne backfill <dataset>`.
        ("daily_bars_history", "Did you mean: daily_bars"),
        ("zzzz", "unknown dataset 'zzzz'"),
    ],
)
def test_a_mistyped_dataset_is_an_error_with_near_misses(name, expected):
    """The registry lookup used to raise KeyError straight through the CLI."""
    with pytest.raises(click.ClickException) as excinfo:
        backfill_cmds._require_known_dataset(name)

    assert expected in str(excinfo.value)
    assert "cne status --datasets" in str(excinfo.value)


def test_a_real_dataset_passes_the_name_check():
    assert backfill_cmds._require_known_dataset("daily_bars") is None
