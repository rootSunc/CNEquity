"""`cne init --profile` — how much of the market to build, on one axis.

demo and sample make a tiny lake (they were `cne demo`); quick and full make
a market. The axis is the point: the option that used to pick between two
commands now picks a depth, and each end refuses the other end's options.
"""

from __future__ import annotations

from datetime import date

import pytest
from click.testing import CliRunner

from cnequity.cli._shared import ingest_scope_label
from cnequity.cli.main import cli
from cnequity.cli.setup_cmds import QUICK_PROFILE_YEARS, _echo_init_plan, _init_history_start
from cnequity.config import Config
from cnequity.config.bootstrap import path_for_toml
from cnequity.domain.market_time import shanghai_today
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.worker_pool import _window_backfill
from cnequity.run_scope import backfill_of, set_backfill
from cnequity.steps.common import BACKFILL_START


def _write_config(tmp_path) -> str:
    cfg_path = tmp_path / "cnequity.toml"
    cfg_path.write_text(
        f"""
[data]
root = "{path_for_toml(tmp_path / "data")}"

[tdx_protocol]
allow_mock = true

[job.init.phases]
names = ["phase1_reference"]
"""
    )
    return str(cfg_path)


# --- window selection ------------------------------------------------------


def test_full_profile_leaves_each_step_its_own_default():
    """None, not BACKFILL_START: index_constituents and industry_members have
    their own floors, and pinning them all to 2016 would widen those sweeps."""
    assert _init_history_start("full", None, date(2026, 8, 2)) is None


def test_quick_profile_is_three_calendar_years():
    today = date(2026, 8, 2)
    assert _init_history_start("quick", None, today) == today.replace(
        year=today.year - QUICK_PROFILE_YEARS
    )


def test_quick_profile_survives_a_leap_day():
    start = _init_history_start("quick", None, date(2028, 2, 29))
    assert start == date(2025, 3, 1)


def test_since_overrides_the_profile():
    assert _init_history_start("quick", "2019-01-01", date(2026, 8, 2)) == date(2019, 1, 1)


@pytest.mark.usefixtures("chinese_output")
@pytest.mark.parametrize(
    ("universe", "label"),
    [
        ("all_a", "沪深京全市场 A 股"),
        ("all_a_sh_sz", "沪深 A 股（配置排除北交所）"),
        ("all_instruments", "instruments 全部场内标的（含 ETF/LOF）"),
    ],
)
def test_ingest_scope_labels_do_not_overstate_the_configured_market(universe, label):
    assert ingest_scope_label(universe) == label


@pytest.mark.usefixtures("chinese_output")
@pytest.mark.parametrize("resume", [False, True])
def test_init_plan_explains_optional_st_and_automatic_delisted_recovery(capsys, resume):
    _echo_init_plan(
        "quick",
        since_str=None,
        history_start=date(2023, 8, 2),
        trade_date=date(2026, 8, 2),
        resume=resume,
        config_path="configs/cnequity.toml",
        ingest_universe="all_a",
    )
    output = capsys.readouterr().out
    assert "窗口内退市日线自动恢复" in output
    assert "全市场历史 ST 扫描不属于 init" in output
    assert "cne backfill trading_status" in output
    assert "400" not in output


# --- the CLI wiring --------------------------------------------------------


def _capture_backfill_start(tmp_path, monkeypatch, args: list[str]):
    seen: dict = {}

    def fake_run_init_phases(self, trade_date=None, **kwargs):
        seen["start"] = backfill_of(self.config).start
        return {"run_id": "init-run", "status": "success", "phases": []}

    monkeypatch.setattr(JobEngine, "run_init_phases", fake_run_init_phases)
    result = CliRunner().invoke(cli, ["init", "--config", _write_config(tmp_path), *args])
    assert result.exit_code == 0, result.output
    return seen.get("start"), result.output


@pytest.mark.usefixtures("chinese_output")
def test_quick_profile_reaches_the_engine(tmp_path, monkeypatch):
    start, output = _capture_backfill_start(
        tmp_path, monkeypatch, ["--profile", "quick", "--trade-date", "2026-08-02"]
    )
    assert start == date(2023, 8, 2)
    assert "2023-08-02" in output
    assert "沪深京全市场" in output
    assert "批次进度和 ETA" in output
    assert "全市场历史 ST 扫描不属于 init" in output
    assert "窗口内退市日线自动恢复" in output
    assert "cne backfill trading_status" in output


@pytest.mark.usefixtures("chinese_output")
def test_default_init_is_the_shallow_window(tmp_path, monkeypatch):
    """A bare `cne init` takes the quick window — a usable lake on the first run.

    A deeper window needs more pages, while even a shallow window still pays
    per-symbol request overhead. Quick retains a multi-year research window.
    """
    # Derived, not hardcoded: a bare `cne init` anchors on today, so a literal
    # date here passes only on the day it was written and fails at the next
    # midnight for reasons that have nothing to do with the behaviour under test.
    # The CLI anchors market windows on the exchange-local date, not the host
    # machine's date. Around UTC/local midnight those can differ by one day.
    # On a weekend or holiday it anchors on the last finished session instead.
    from cnequity.cli.demo import _last_trading_day
    from cnequity.config import Config

    today = _last_trading_day(Config(data_root=tmp_path / "anchor"), shanghai_today())
    try:
        expected = today.replace(year=today.year - QUICK_PROFILE_YEARS)
    except ValueError:  # Feb 29 — same fallback the CLI uses
        expected = date(today.year - QUICK_PROFILE_YEARS, 3, 1)

    start, output = _capture_backfill_start(tmp_path, monkeypatch, [])
    assert start == expected
    assert "历史窗口" in output
    assert "cne backfill daily_bars" in output, "must say how to deepen"


@pytest.mark.usefixtures("chinese_output")
def test_full_profile_still_takes_everything(tmp_path, monkeypatch):
    start, output = _capture_backfill_start(tmp_path, monkeypatch, ["--profile", "full"])
    assert start is None
    assert "历史窗口" not in output
    assert "日线从 2016-01-01 起" in output
    assert "深历史需要更多分页" in output


@pytest.mark.usefixtures("chinese_output")
def test_custom_init_window_does_not_claim_a_fixed_duration(tmp_path, monkeypatch):
    _, output = _capture_backfill_start(
        tmp_path,
        monkeypatch,
        ["--profile", "quick", "--since", "2020-01-01", "--trade-date", "2026-08-02"],
    )

    assert "自定义历史窗口 2020-01-01 .. 2026-08-02" in output
    assert "耗时取决于窗口长度" in output
    assert "约 1 小时" not in output
    assert "约 3 小时" not in output


@pytest.mark.usefixtures("chinese_output")
def test_resume_reports_remaining_work_instead_of_the_full_run_estimate(tmp_path, monkeypatch):
    start, output = _capture_backfill_start(tmp_path, monkeypatch, ["--resume"])

    assert start is None
    assert "续跑计划" in output
    assert "只处理失败/缺失阶段" in output
    assert "原 run 的历史范围保持不变" in output
    assert "约 1 小时" not in output
    assert "约 3 小时" not in output


@pytest.mark.usefixtures("chinese_output")
def test_resume_since_is_the_only_explicit_history_override(tmp_path, monkeypatch):
    start, output = _capture_backfill_start(
        tmp_path,
        monkeypatch,
        ["--resume", "--since", "2020-01-01", "--trade-date", "2026-08-02"],
    )

    assert start == date(2020, 1, 1)
    assert "显式 --since=2020-01-01 覆盖历史起点" in output


def test_quick_profile_prints_how_to_deepen(tmp_path, monkeypatch):
    _, output = _capture_backfill_start(tmp_path, monkeypatch, ["--profile", "quick"])
    assert "cne backfill daily_bars" in output


# --- the two traps a shallower window opens --------------------------------


def test_shallow_window_stays_strict_about_pagination_failures(tmp_path):
    """A backfill raises on a mid-pagination failure instead of keeping the pages
    that arrived. That used to be inferred from `start == 2016-01-01`, which a
    quick init does not match — inferring it would make the shallow path the
    lenient one, and lose a symbol's older years without saying so."""
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, active=True)
    assert _window_backfill(cfg, date(2023, 8, 2)) is True


def test_daily_window_is_still_lenient(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    assert _window_backfill(cfg, date(2026, 8, 2)) is False
    # The historical sentinel keeps working for callers that never set the flag.
    assert _window_backfill(cfg, BACKFILL_START) is True


def test_resume_reuses_the_original_window(tmp_path, monkeypatch):
    """A resume runs from a fresh process days later. Without the window on the
    run record it would default to full depth and fetch years the operator
    deliberately skipped."""
    from cnequity.orchestrator.manifest import Manifest

    cfg = Config(data_root=tmp_path / "data")
    cfg.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run(
        "init",
        {
            "phases": ["phase1_reference"],
            "trade_date": "2026-08-02",
            "history_start": "2023-08-02",
        },
    )

    engine = JobEngine(cfg)
    monkeypatch.setattr(
        JobEngine, "_retry_run", lambda self, *a, **k: {"rows_read": 0, "rows_written": 0}
    )
    monkeypatch.setattr(
        JobEngine, "run_job", lambda self, *a, **k: {"status": "success", "results": []}
    )
    monkeypatch.setattr(JobEngine, "_finalize_init_run", lambda self, *a, **k: "success")

    engine.resume_init(date(2026, 8, 5), run_id=run_id)
    assert backfill_of(engine.config).start == date(2023, 8, 2)


def test_resume_does_not_override_an_explicit_window(tmp_path, monkeypatch):
    from cnequity.orchestrator.manifest import Manifest

    cfg = Config(data_root=tmp_path / "data")
    cfg.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    run_id = Manifest(cfg.manifest_path).start_run(
        "init", {"phases": ["phase1_reference"], "history_start": "2023-08-02"}
    )
    set_backfill(cfg, start=date(2016, 1, 1))

    engine = JobEngine(cfg)
    monkeypatch.setattr(
        JobEngine, "_retry_run", lambda self, *a, **k: {"rows_read": 0, "rows_written": 0}
    )
    monkeypatch.setattr(
        JobEngine, "run_job", lambda self, *a, **k: {"status": "success", "results": []}
    )
    monkeypatch.setattr(JobEngine, "_finalize_init_run", lambda self, *a, **k: "success")

    engine.resume_init(date(2026, 8, 5), run_id=run_id)
    assert backfill_of(engine.config).start == date(2016, 1, 1)
    assert engine.manifest.get_run_metadata(run_id)["history_start"] == "2016-01-01"

    next_engine = JobEngine(Config(data_root=tmp_path / "data"))
    next_engine.resume_init(date(2026, 8, 6), run_id=run_id)
    assert backfill_of(next_engine.config).start == date(2016, 1, 1)


def test_init_records_the_window_on_the_run(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    cfg.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    set_backfill(cfg, start=date(2023, 8, 2))
    engine = JobEngine(cfg)

    captured: dict = {}

    def fake_execute(self, run_id, trade_date, phases, **kwargs):
        captured["meta"] = self.manifest.get_run_metadata(run_id)
        return {"run_id": run_id, "status": "success", "phases": []}

    monkeypatch.setattr(JobEngine, "_execute_init_phases", fake_execute)
    engine.run_init_phases(trade_date=date(2026, 8, 2))
    assert captured["meta"]["history_start"] == "2023-08-02"


@pytest.mark.parametrize("profile", ["full", "quick"])
def test_every_profile_keeps_the_full_cross_section(profile, tmp_path, monkeypatch):
    """Shallower, never narrower. A symbol filter here would bake in the exact
    survivorship bias the delisted backfill profile exists to repair."""
    start, _ = _capture_backfill_start(tmp_path, monkeypatch, ["--profile", profile])
    cfg = Config(data_root=tmp_path / "data")
    if start:
        set_backfill(cfg, start=start)
    assert getattr(cfg, "_scope_symbols", None) is None
    assert backfill_of(cfg).symbol_list() is None


# --- the tiny-lake end of the same axis ------------------------------------


def _capture_demo(monkeypatch, args: list[str]):
    """Run `cne init --profile demo|sample` with both runners stubbed."""
    seen: dict = {}

    def fake_demo(**kwargs):
        seen["runner"] = "demo"
        seen.update(kwargs)

    def fake_sample(**kwargs):
        seen["runner"] = "sample"
        seen.update(kwargs)

    monkeypatch.setattr("cnequity.cli.demo.run_demo", fake_demo)
    monkeypatch.setattr("cnequity.cli.demo.run_sample_demo", fake_sample)

    def never(*a, **k):  # the tiny lake must not enter the init phases
        raise AssertionError("init phases ran for a demo profile")

    monkeypatch.setattr(JobEngine, "run_init_phases", never)
    result = CliRunner().invoke(cli, ["init", *args])
    return seen, result


@pytest.mark.parametrize(("profile", "runner"), [("demo", "demo"), ("sample", "sample")])
def test_a_tiny_lake_profile_routes_to_its_own_runner(profile, runner, monkeypatch, tmp_path):
    seen, result = _capture_demo(
        monkeypatch,
        ["--profile", profile, "--data-root", str(tmp_path / "lake"), "--days", "5"],
    )
    assert result.exit_code == 0, result.output
    assert seen["runner"] == runner
    assert seen["days"] == 5
    assert seen["data_root"] == tmp_path / "lake"
    assert seen["force"] is False


def test_a_tiny_lake_profile_needs_no_config(monkeypatch, tmp_path):
    """`cne demo` never read a config, and folding it into init must not add one."""
    monkeypatch.chdir(tmp_path)
    seen, result = _capture_demo(monkeypatch, ["--profile", "sample"])
    assert result.exit_code == 0, result.output
    assert seen["runner"] == "sample"


@pytest.mark.usefixtures("chinese_output")
@pytest.mark.parametrize(
    ("args", "rejected"),
    [
        (["--profile", "sample", "--resume"], "--resume"),
        (["--profile", "demo", "--layout-only"], "--layout-only"),
        (["--profile", "demo", "--since", "2020-01-01"], "--since"),
        (["--profile", "quick", "--symbols", "600519.SH"], "--symbols"),
        (["--profile", "full", "--intraday"], "--intraday"),
        (["--profile", "quick", "--config-out", "x.toml"], "--config-out"),
        (["--profile", "full", "--force"], "--force"),
    ],
)
def test_each_end_of_the_axis_refuses_the_other_end_options(args, rejected, monkeypatch):
    seen, result = _capture_demo(monkeypatch, args)
    assert result.exit_code != 0
    assert f"{rejected} 对" in result.output
    assert not seen, "nothing should have run"
