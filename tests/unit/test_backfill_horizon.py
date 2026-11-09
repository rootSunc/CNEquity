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
from cnequity.run_scope import backfill_of


def _clamp(dataset: str, start: date, end: date):
    """The window ``cne backfill`` actually fetches for *dataset*, and its finding.

    Commit c2d6c7c3 replaced the CLI's refusal with this clamp: a range the
    source cannot serve stays a recorded gap, and the part it can serve is
    still delivered.
    """
    from types import SimpleNamespace

    from cnequity.config import Config
    from cnequity.orchestrator.engine import JobEngine
    from cnequity.run_scope import set_backfill

    cfg = Config(data_root=Path("/tmp/lake"))
    set_backfill(cfg, active=True, start=start, end=end)
    scoped, finding = JobEngine._backfill_source_scope(SimpleNamespace(config=cfg), dataset, end)
    return backfill_of(scoped), finding


def test_a_window_older_than_the_horizon_is_clamped_and_reported():
    today = shanghai_today()
    earliest = get_dataset("minute_bars").earliest_available(today)

    window, finding = _clamp("minute_bars", earliest - timedelta(days=30), today)

    assert window.start == earliest
    assert finding["check"] == "source_scope_incomplete"
    assert finding["severity"] == "warning"
    assert finding["effective_scope"]["start"] == earliest.isoformat()


def test_a_window_inside_the_horizon_is_left_alone():
    today = shanghai_today()
    inside = get_dataset("minute_bars").earliest_available(today) + timedelta(days=1)

    window, finding = _clamp("minute_bars", inside, today)

    assert (window.start, finding) == (inside, None)


def test_a_dataset_without_a_limit_keeps_an_old_start():
    # daily_bars has no vendor ceiling; a 2001 start must stay legal.
    window, finding = _clamp("daily_bars", date(2001, 1, 1), date(2001, 2, 1))

    assert (window.start, finding) == (date(2001, 1, 1), None)


def test_a_window_entirely_before_the_floor_is_a_capability_limit():
    from cnequity.orchestrator.outcomes import CapabilityLimitError

    with pytest.raises(CapabilityLimitError, match="outside available history"):
        _clamp("northbound_flows", date(2014, 1, 1), date(2014, 6, 30))


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
        window = backfill_of(self.cfg)
        self.windows.append((window.start, window.end))
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
    # No lake behind this config: skip the before/after lineage fingerprint.
    monkeypatch.setattr(backfill_cmds, "_bar_fingerprint", lambda config, window: None)

    from click.testing import CliRunner

    result = CliRunner().invoke(cli, _backfill_argv("daily_bars", "--symbols", "600519.SH"))
    assert result.exit_code == 0, result.output
    assert backfill_of(cfg).symbol_list() == ["600519.SH"]


@pytest.mark.usefixtures("chinese_output")
def test_margin_source_override_is_one_run_only_and_dataset_scoped(monkeypatch):
    from types import SimpleNamespace

    from click.testing import CliRunner

    cfg = SimpleNamespace(margin_trading_source="eastmoney")
    monkeypatch.setattr(backfill_cmds, "_cfg", lambda _p: cfg)
    monkeypatch.setattr(
        backfill_cmds,
        "_backfill_once",
        lambda config, dataset: {"status": "success", "rows_written": 0},
    )

    runner = CliRunner()
    result = runner.invoke(cli, _backfill_argv("margin_trading", "--source", "exchange"))
    assert result.exit_code == 0, result.output
    assert cfg.margin_trading_source == "exchange"

    invalid = runner.invoke(cli, _backfill_argv("daily_bars", "--source", "exchange"))
    assert invalid.exit_code != 0
    assert "只支持 margin_trading" in invalid.output


@pytest.mark.usefixtures("chinese_output")
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
    # `cne repair bse-tip-turnover` takes one --date, so the window is one session
    # by construction; the symbols are still required.
    missing_symbols = runner.invoke(cli, ["repair", "bse-tip-turnover", "--date", "2026-08-21"])
    assert missing_symbols.exit_code != 0
    assert "--symbols" in missing_symbols.output


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


@pytest.mark.parametrize(
    ("dataset", "floor"),
    [
        ("trade_ticks", date(2024, 1, 2)),
        # 2003 is where the *disclosure dates* start, not where the data starts:
        # RPT_F10_EH_HOLDERS reaches into the 1990s but borrows its NOTICE_DATE
        # from RPT_F10_EH_FREEHOLDERS, which serves 0 rows before 2003, and rows
        # that cannot borrow a date are dropped. Fetching 1999-2002 writes none.
        ("top_holders", date(2003, 1, 1)),
        ("northbound_flows", date(2014, 11, 17)),
    ],
)
def test_a_fixed_floor_clamps_the_start_to_the_floor(dataset, floor):
    assert get_dataset(dataset).history_floor_date == floor

    window, finding = _clamp(dataset, floor - timedelta(days=400), floor + timedelta(days=30))

    assert window.start == floor
    assert finding["check"] == "source_scope_incomplete"


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


@pytest.mark.usefixtures("chinese_output")
@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("daily_bar", "是不是想找：daily_bars"),
        # A step name is not a backfill target; the docs say `cne backfill <dataset>`.
        ("daily_bars_history", "是不是想找：daily_bars"),
        ("zzzz", "未知数据集 'zzzz'"),
    ],
)
def test_a_mistyped_dataset_is_an_error_with_near_misses(name, expected):
    """The registry lookup used to raise KeyError straight through the CLI."""
    with pytest.raises(click.ClickException) as excinfo:
        backfill_cmds._require_known_dataset(name)

    assert expected in str(excinfo.value)
    assert "cne status --datasets" in str(excinfo.value)
