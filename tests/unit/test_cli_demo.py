"""Offline coverage for `cne init --profile demo` (real network is mocked)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import click
import polars as pl
import pytest
from click.testing import CliRunner

from cnequity.cli.main import cli
from cnequity.config import load_config, validate_config
from cnequity.config.bootstrap import path_for_toml
from cnequity.domain.schemas import validate_dataframe, with_provenance
from cnequity.orchestrator.registry import STEP_REGISTRY, StepEntry


def _inst_frame(symbols: list[str]) -> pl.DataFrame:
    rows = []
    for sym in symbols:
        code, exch = sym.split(".")
        rows.append(
            {
                "symbol": sym,
                "name": f"Name-{code}",
                "exchange": exch,
                "asset_type": "stock",
                "list_date": date(2010, 1, 1),
                "delist_date": None,
                "prev_symbol": None,
            }
        )
    return with_provenance(pl.DataFrame(rows), source="tdx_protocol", data_version="v1")


def _bars_frame(symbols: list[str], start: date, end: date) -> pl.DataFrame:
    rows = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            for sym in symbols:
                rows.append(
                    {
                        "symbol": sym,
                        "trade_date": d,
                        "open": 10.0,
                        "high": 11.0,
                        "low": 9.0,
                        "close": 10.5,
                        "volume": 1000,
                        "amount": 1.0e6,
                        "source": "tdx_protocol",
                        "data_version": "v1",
                        "fetched_at": "2024-06-28T00:00:00+00:00",
                    }
                )
        d = date.fromordinal(d.toordinal() + 1)
    return pl.DataFrame(rows)


@pytest.mark.parametrize("profile", ["demo", "sample"])
def test_generated_demo_config_validates_without_enabling_daily_jobs(tmp_path, profile):
    from cnequity.cli.demo import _write_demo_toml, _write_sample_toml

    config_path = tmp_path / "config.toml"
    writer = _write_demo_toml if profile == "demo" else _write_sample_toml
    writer(config_path, tmp_path / "lake")
    cfg = load_config(config_path)
    assert cfg.daily_waves == []
    assert validate_config(cfg) == []
    result = CliRunner().invoke(cli, ["config", "validate", "--config", str(config_path)])
    assert result.exit_code == 0, result.output
    result = CliRunner().invoke(cli, ["run", "daily", "--config", str(config_path)])
    assert result.exit_code != 0
    assert "no daily schedule" in result.output
    assert "cne init" in result.output


@pytest.mark.usefixtures("chinese_output")
def test_cne_demo_offline(tmp_path, monkeypatch):
    symbols = ["600519.SH", "000001.SZ"]
    monkeypatch.setattr("cnequity.cli.demo._probe_tdx", lambda cfg: None)
    monkeypatch.setattr(
        "cnequity.adapters.tdx_protocol.client.fetch_instruments",
        lambda **kwargs: _inst_frame(symbols),
    )
    monkeypatch.setattr(
        "cnequity.adapters.tdx_protocol.client.normalize_with_source",
        lambda df, *_a, **_k: df,
    )

    def fake_calendar(config, trade_date, run_id, context):
        # Staging, like the real step: the demo has to publish it, and for a
        # while it did not — the step logged 2,818 rows while `cne status
        # --datasets` reported trading_calendar empty and `SELECT * FROM
        # trading_calendar` returned nothing.
        from cnequity.storage import StagingWriter

        rows = []
        d = date(2024, 5, 1)
        while d <= date(2024, 6, 28):
            rows.append(
                {
                    "trade_date": d,
                    "is_trading": d.weekday() < 5,
                    "source": "seed",
                    "data_version": "v1",
                    "fetched_at": "2024-06-28T00:00:00+00:00",
                }
            )
            d = date.fromordinal(d.toordinal() + 1)
        df = validate_dataframe(pl.DataFrame(rows), "trading_calendar")
        StagingWriter(config.staging_root).write_batch("trading_calendar", run_id, "batch-0", df)
        return {"rows_read": df.height, "rows_written": df.height}

    def fake_daily_bars(config, trade_date, run_id, context):
        from cnequity.storage import StagingWriter

        start = getattr(config, "_backfill_start", date(2024, 6, 1))
        end = getattr(config, "_backfill_end", trade_date)
        df = validate_dataframe(_bars_frame(symbols, start, end), "daily_bars")
        StagingWriter(config.staging_root).write_batch("daily_bars", run_id, "batch-0", df)
        return {"rows_read": df.height, "rows_written": df.height}

    def fake_compact(config, trade_date, run_id, context):
        from cnequity.storage.parquet import compact_dataset
        from cnequity.storage.state import StateStore

        total = 0
        for dataset in ("trading_calendar", "daily_bars"):
            if not (config.staging_root / dataset / f"run_id={run_id}").exists():
                continue
            total += compact_dataset(config.staging_root, config.curated_root, dataset, run_id)
            StateStore(config.meta_root).set_date(dataset, trade_date)
        return {"rows_read": total, "rows_written": total}

    originals = {
        name: STEP_REGISTRY[name] for name in ("trading_calendar", "daily_bars", "compact")
    }
    STEP_REGISTRY["trading_calendar"] = StepEntry(fn=fake_calendar, group="core")
    STEP_REGISTRY["daily_bars"] = StepEntry(fn=fake_daily_bars, group="core", requires_workers=True)
    STEP_REGISTRY["compact"] = StepEntry(fn=fake_compact, group="finalize")
    try:
        data_root = tmp_path / "demo-lake"
        config_out = tmp_path / "demo.toml"
        result = CliRunner().invoke(
            cli,
            [
                "init",
                "--profile",
                "demo",
                "--symbols",
                ",".join(symbols),
                "--days",
                "10",
                "--data-root",
                str(data_root),
                "--config-out",
                str(config_out),
                "--trade-date",
                "2024-06-28",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "探测 TDX" in result.output
        assert "600519.SH" in result.output
        assert config_out.exists()
        assert (data_root / "curated" / "instruments" / "part-merged.parquet").exists()
        assert list((data_root / "curated" / "daily_bars").glob("**/*.parquet"))
        # Fetched is not published: the calendar must reach curated too.
        assert list((data_root / "curated" / "trading_calendar").glob("**/*.parquet"))
    finally:
        STEP_REGISTRY.update(originals)


def test_demo_help_lists_command():
    """The tiny-lake path is a profile of `init`, and its options say so."""
    result = CliRunner().invoke(cli, ["init", "--help"])
    assert result.exit_code == 0
    assert "--symbols" in result.output
    assert "--days" in result.output
    assert "--research" in result.output
    assert "--force" in result.output
    # `--sample` used to be a flag on `cne demo`; it is now a profile value,
    # which is what makes demo/sample/quick/full one axis instead of two forks.
    assert "demo|sample|quick|full" in result.output


def test_sample_then_demo_with_defaults_do_not_collide(tmp_path, monkeypatch):
    """The sample's closing hint, `cne init --profile demo`, must just work.

    Both profiles used to default to configs/cnequity.demo.toml and
    data/cnequity-demo, so the demo refused the config the sample had written,
    and with --force would have mixed real bars into a source=mock lake.
    """
    monkeypatch.chdir(tmp_path)
    sample = CliRunner().invoke(cli, ["init", "--profile", "sample", "--days", "3"])
    assert sample.exit_code == 0, sample.output
    assert (tmp_path / "configs" / "cnequity.sample.toml").exists()
    assert any((tmp_path / "data" / "cnequity-sample").rglob("*.parquet"))
    assert not (tmp_path / "configs" / "cnequity.demo.toml").exists()

    seen = {}

    def fake_run_demo(**kwargs):
        seen.update(kwargs)
        return {}

    monkeypatch.setattr("cnequity.cli.demo.run_demo", fake_run_demo)
    demo = CliRunner().invoke(cli, ["init", "--profile", "demo"])
    assert demo.exit_code == 0, demo.output
    assert seen["data_root"] == Path("data/cnequity-demo")
    assert seen["config_out"] == Path("configs/cnequity.demo.toml")


def test_demo_refuses_a_data_root_holding_the_sample(tmp_path, monkeypatch):
    from cnequity.cli.demo import run_demo, run_sample_demo

    monkeypatch.setattr(
        "cnequity.cli.demo._probe_tdx",
        lambda *_a, **_k: pytest.fail("the refusal must come before any network"),
    )
    root = tmp_path / "lake"
    run_sample_demo(
        symbols=["600519.SH"], days=3, data_root=root, config_out=tmp_path / "sample.toml"
    )
    with pytest.raises(click.ClickException, match="synthetic sample"):
        run_demo(
            symbols=["600519.SH"],
            days=3,
            data_root=root,
            config_out=tmp_path / "demo.toml",
            force=True,
        )


def test_missing_user_config_points_at_the_sample_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "cnequity.sample.toml").write_text("", encoding="utf-8")
    result = CliRunner().invoke(cli, ["status"])
    assert result.exit_code != 0
    assert "--config configs/cnequity.sample.toml" in result.output


@pytest.mark.usefixtures("chinese_output")
def test_cne_demo_sample_needs_no_network(tmp_path, monkeypatch):
    def fail_on_network(*args, **kwargs):
        raise AssertionError("sample mode must not touch TDX")

    monkeypatch.setattr("cnequity.cli.demo._probe_tdx", fail_on_network)
    data_root = tmp_path / "sample-lake"
    config_out = tmp_path / "sample.toml"

    result = CliRunner().invoke(
        cli,
        [
            "init",
            "--profile",
            "sample",
            "--symbols",
            "600519.SH,000001.SZ",
            "--days",
            "5",
            "--data-root",
            str(data_root),
            "--config-out",
            str(config_out),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "离线样例" in result.output
    assert "source=mock" in result.output
    assert config_out.exists()
    bars = pl.read_parquet(list((data_root / "curated" / "daily_bars").glob("**/*.parquet")))
    assert bars.height == 10
    assert bars["source"].unique().to_list() == ["mock"]

    status = CliRunner().invoke(cli, ["status", "--datasets", "--config", str(config_out)])
    assert status.exit_code == 0, status.output
    assert "sample 湖" in status.output
    assert "STALE" not in status.output

    audit = CliRunner().invoke(cli, ["audit", "--config", str(config_out)])
    assert audit.exit_code == 0, audit.output
    assert "0 error" in audit.output
    assert "预期 info" in audit.output


def test_sample_ends_ready_for_the_panel_and_an_agent(tmp_path):
    """The README sends a first-time user from the demo straight to an agent and
    the dashboard: the printed MCP command must carry the absolute config path,
    and the panel must open on real counts rather than an all-red registry."""
    from fastapi.testclient import TestClient

    from cnequity.config import load_config
    from cnequity.serve.app import create_app
    from cnequity.storage.stats import stats_freshness

    data_root = tmp_path / "sample-lake"
    config_out = tmp_path / "sample.toml"
    result = CliRunner().invoke(
        cli,
        [
            "init",
            "--profile",
            "sample",
            "--days",
            "5",
            "--data-root",
            str(data_root),
            "--config-out",
            str(config_out),
        ],
    )
    assert result.exit_code == 0, result.output
    assert f"claude mcp add cnequity -- cne mcp --config {config_out.resolve()}" in result.output

    cfg = load_config(config_out)
    assert not stats_freshness(cfg).stale
    health = (
        TestClient(create_app(cfg, read_only=True), base_url="http://127.0.0.1")
        .get("/api/health")
        .json()
    )
    assert health["empty_required"] == []
    assert health["rows"] > 0


def test_star_hint_shows_once_per_lake_and_only_on_a_terminal(tmp_path, monkeypatch, capsys):
    from cnequity.cli._shared import star_hint_once
    from cnequity.config import Config

    cfg = Config(data_root=tmp_path)
    monkeypatch.setattr("sys.stderr.isatty", lambda: False, raising=False)
    star_hint_once(cfg)
    assert "Star" not in capsys.readouterr().err

    monkeypatch.setattr("sys.stderr.isatty", lambda: True, raising=False)
    star_hint_once(cfg)
    assert "Star" in capsys.readouterr().err
    star_hint_once(cfg)
    assert "Star" not in capsys.readouterr().err


@pytest.mark.usefixtures("chinese_output")
def test_sample_preserves_a_different_existing_config_without_force(tmp_path):
    data_root = tmp_path / "sample-lake"
    config_out = tmp_path / "existing.toml"
    original = '[data]\nroot = "/important/lake"\n'
    config_out.write_text(original, encoding="utf-8")

    result = CliRunner().invoke(
        cli,
        [
            "init",
            "--profile",
            "sample",
            "--data-root",
            str(data_root),
            "--config-out",
            str(config_out),
        ],
    )

    assert result.exit_code != 0
    assert "配置已存在且内容不同" in result.output
    assert "--force" in result.output
    assert config_out.read_text(encoding="utf-8") == original
    assert not data_root.exists()


def test_sample_force_overwrites_a_different_existing_config(tmp_path):
    data_root = tmp_path / "sample-lake"
    config_out = tmp_path / "existing.toml"
    config_out.write_text('[data]\nroot = "/old/lake"\n', encoding="utf-8")

    result = CliRunner().invoke(
        cli,
        [
            "init",
            "--profile",
            "sample",
            "--days",
            "2",
            "--data-root",
            str(data_root),
            "--config-out",
            str(config_out),
            "--force",
        ],
    )

    assert result.exit_code == 0, result.output
    written = config_out.read_text(encoding="utf-8")
    assert 'profile = "sample"' in written
    # The POSIX spelling, because that is what the writer guarantees: a bare
    # Windows `C:\Users\…` is not a valid TOML basic string (`\U` starts a
    # hex escape), so `path_for_toml` renders every root with forward slashes.
    # Asserting the native form passed on Linux and made this a Windows-only
    # failure of a correct writer.
    assert path_for_toml(data_root) in written


def test_demo_config_force_is_atomic_when_replacement_is_interrupted(tmp_path, monkeypatch):
    from cnequity.cli.demo import _write_sample_toml

    config_out = tmp_path / "existing.toml"
    original = '[data]\nroot = "/important/lake"\n'
    config_out.write_text(original, encoding="utf-8")

    def interrupt_replace(source, destination):
        raise KeyboardInterrupt

    monkeypatch.setattr("cnequity.cli.demo.os.replace", interrupt_replace)

    with pytest.raises(KeyboardInterrupt):
        _write_sample_toml(config_out, tmp_path / "new-lake", force=True)

    assert config_out.read_text(encoding="utf-8") == original
    assert list(tmp_path.glob(".existing.toml.*.tmp")) == []


def test_sample_allows_an_identical_existing_config_without_force(tmp_path):
    from cnequity.cli.demo import _write_sample_toml

    data_root = tmp_path / "sample-lake"
    config_out = tmp_path / "sample.toml"
    _write_sample_toml(config_out, data_root)

    result = CliRunner().invoke(
        cli,
        [
            "init",
            "--profile",
            "sample",
            "--days",
            "2",
            "--data-root",
            str(data_root),
            "--config-out",
            str(config_out),
        ],
    )

    assert result.exit_code == 0, result.output


@pytest.mark.usefixtures("chinese_output")
def test_cne_demo_sample_rejects_live_only_modes(tmp_path):
    result = CliRunner().invoke(
        cli,
        ["init", "--profile", "sample", "--research", "--data-root", str(tmp_path / "lake")],
    )

    assert result.exit_code != 0
    assert "不能和" in result.output


def test_return_summary_compares_raw_and_adjusted_series():
    from cnequity.cli.demo import _return_summary

    raw = pl.DataFrame(
        {
            "trade_date": [date(2024, 6, 27), date(2024, 6, 28)],
            "close": [10.0, 12.0],
        }
    )
    adjusted = raw.with_columns(
        pl.Series("adj_close", [20.0, 30.0]),
        pl.Series("adj_is_exact", [True, True]),
    )

    result = _return_summary(raw, adjusted)

    assert result["raw_return"] == pytest.approx(0.2)
    assert result["adjusted_return"] == pytest.approx(0.5)
    assert result["exact"] is True
    assert result["rows"] == 2


def test_research_demo_derives_factors_and_requires_exact_rows(monkeypatch):
    from cnequity.cli.demo import _run_research_demo

    calls: dict[str, object] = {}

    class Result:
        failed: list[str] = []
        findings: list[dict] = []

    def fake_compute(config, **kwargs):
        calls["derive"] = kwargs
        return Result()

    raw = pl.DataFrame(
        {
            "trade_date": [date(2024, 6, 27), date(2024, 6, 28)],
            "close": [10.0, 12.0],
        }
    )
    adjusted = raw.with_columns(
        pl.Series("adj_close", [20.0, 30.0]),
        pl.Series("adj_is_exact", [True, True]),
    )

    def fake_load(dataset, **kwargs):
        calls.setdefault("loads", []).append((dataset, kwargs))
        return adjusted if kwargs.get("adjust") else raw

    monkeypatch.setattr("cnequity.derive.adj_factors.compute_adj_factors", fake_compute)
    monkeypatch.setattr("cnequity.query.reader.load", fake_load)

    result = _run_research_demo(
        object(),
        ["600519.SH"],
        date(2024, 6, 27),
        date(2024, 6, 28),
    )

    assert calls["derive"] == {
        "adjust_type": "hfq",
        "refresh_symbols": ["600519.SH"],
        "full": True,
    }
    assert calls["loads"][1][1]["strict_adj"] is True
    assert result["adjusted_return"] == pytest.approx(0.5)


def test_write_demo_toml_escapes_windows_paths(tmp_path):
    """Bare ``C:\\Users\\…`` is invalid TOML; follow-up ``cne query`` would fail."""
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib

    from cnequity.cli.demo import _write_demo_toml

    out = tmp_path / "demo.toml"
    # Simulate a native Windows resolve() result even on Unix CI.
    data_root = tmp_path / "Users" / "测试" / "lake"
    data_root.mkdir(parents=True)
    _write_demo_toml(out, data_root)

    payload = tomllib.loads(out.read_text(encoding="utf-8"))
    assert "\\" not in payload["data"]["root"]
    assert Path(payload["data"]["root"]).name == "lake"
    # `cne verify` / `cne audit` read this to know the lake is five symbols and
    # not a market; without it they judge it against all 42 datasets.
    assert payload["data"]["profile"] == "demo"


def test_probe_tdx_closes_its_client():
    """Probe must close the client even though the heartbeat is now a daemon.

    Before the close, an abandoned non-daemon thread kept the interpreter
    alive after all six demo steps had already printed.
    """
    from unittest.mock import patch

    from cnequity.cli import demo as demo_mod

    client = object()
    with (
        patch.object(demo_mod, "_quotes_client", create=True),
        patch(
            "cnequity.adapters.tdx_protocol.client._quotes_client",
            return_value=client,
        ),
        patch("cnequity.adapters.tdx_protocol.session.close_quotes_client") as closer,
    ):
        demo_mod._probe_tdx(object())

    closer.assert_called_once_with(client)


def _minute_frame(symbols: list[str], day: date, bars: int = 240) -> pl.DataFrame:
    """A synthetic full session, right-labelled like the real source."""
    from datetime import datetime, timedelta

    from cnequity.adapters.tdx_protocol.minute_bars import in_session

    stamps: list[datetime] = []
    stamp = datetime(day.year, day.month, day.day, 9, 31)
    while len(stamps) < bars:
        if in_session(stamp):
            stamps.append(stamp)
        stamp += timedelta(minutes=1)
    rows = [
        {
            "symbol": sym,
            "trade_date": day,
            "bar_time": s,
            "frequency": "1m",
            "open": 10.0,
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
            "volume": 100,
            "amount": 1000.0,
        }
        for sym in symbols
        for s in stamps
    ]
    return with_provenance(pl.DataFrame(rows), source="tdx_protocol", data_version="v1")


@pytest.mark.usefixtures("chinese_output")
def test_cne_demo_intraday_offline(tmp_path, monkeypatch):
    """`cne init --profile demo --intraday` adds a 7th step and prints a real session."""
    symbols = ["600519.SH", "000001.SZ"]
    monkeypatch.setattr("cnequity.orchestrator.engine.shanghai_today", lambda: date(2024, 6, 28))
    monkeypatch.setattr("cnequity.cli.demo._probe_tdx", lambda cfg: None)
    monkeypatch.setattr(
        "cnequity.adapters.tdx_protocol.client.fetch_instruments",
        lambda **kwargs: _inst_frame(symbols),
    )
    monkeypatch.setattr(
        "cnequity.adapters.tdx_protocol.client.normalize_with_source",
        lambda df, *_a, **_k: df,
    )

    def fake_calendar(config, trade_date, run_id, context):
        from cnequity.storage.atomic import write_parquet_atomic

        rows = []
        d = date(2024, 5, 1)
        while d <= date(2024, 6, 28):
            rows.append(
                {
                    "trade_date": d,
                    "is_trading": d.weekday() < 5,
                    "source": "seed",
                    "data_version": "v1",
                    "fetched_at": "2024-06-28T00:00:00+00:00",
                }
            )
            d = date.fromordinal(d.toordinal() + 1)
        df = validate_dataframe(pl.DataFrame(rows), "trading_calendar")
        out = config.curated_root / "trading_calendar" / "trade_date=2024"
        out.mkdir(parents=True, exist_ok=True)
        write_parquet_atomic(out / "part-000.parquet", df)
        return {"rows_read": df.height, "rows_written": df.height}

    def fake_daily_bars(config, trade_date, run_id, context):
        from cnequity.storage import StagingWriter

        start = getattr(config, "_backfill_start", date(2024, 6, 1))
        end = getattr(config, "_backfill_end", trade_date)
        df = validate_dataframe(_bars_frame(symbols, start, end), "daily_bars")
        StagingWriter(config.staging_root).write_batch("daily_bars", run_id, "batch-0", df)
        return {"rows_read": df.height, "rows_written": df.height}

    def fake_minute_bars(config, trade_date, run_id, context):
        from cnequity.storage import StagingWriter

        # The demo must have configured the watchlist scope before we ran.
        assert config.minute_bars_enabled
        assert config.minute_bars_scope == "watchlist"
        df = validate_dataframe(_minute_frame(symbols, date(2024, 6, 28)), "minute_bars")
        StagingWriter(config.staging_root).write_batch("minute_bars", run_id, "batch-0", df)
        return {"rows_read": df.height, "rows_written": df.height}

    def fake_compact(config, trade_date, run_id, context):
        from cnequity.storage.parquet import compact_dataset

        total = 0
        for dataset in ("daily_bars", "minute_bars"):
            total += compact_dataset(config.staging_root, config.curated_root, dataset, run_id)
        return {"rows_read": total, "rows_written": total}

    names = ("trading_calendar", "daily_bars", "minute_bars", "compact")
    originals = {name: STEP_REGISTRY[name] for name in names}
    STEP_REGISTRY["trading_calendar"] = StepEntry(fn=fake_calendar, group="core")
    STEP_REGISTRY["daily_bars"] = StepEntry(fn=fake_daily_bars, group="core", requires_workers=True)
    STEP_REGISTRY["minute_bars"] = StepEntry(fn=fake_minute_bars, group="intraday")
    STEP_REGISTRY["compact"] = StepEntry(fn=fake_compact, group="finalize")
    try:
        data_root = tmp_path / "demo-lake"
        result = CliRunner().invoke(
            cli,
            [
                "init",
                "--profile",
                "demo",
                "--intraday",
                "--symbols",
                ",".join(symbols),
                "--days",
                "10",
                "--data-root",
                str(data_root),
                "--config-out",
                str(tmp_path / "demo.toml"),
                "--trade-date",
                "2024-06-28",
            ],
        )
        assert result.exit_code == 0, result.output
        # Seven steps rather than six, and the session shape is reported.
        assert "minute_bars 1 分钟线" in result.output
        assert "是完整的 240 根/日" in result.output
        assert "bar_time 是这一分钟的**收盘**时刻" in result.output
        assert 'load("minute_bars"' in result.output
        assert list((data_root / "curated" / "minute_bars").glob("**/*.parquet"))
    finally:
        STEP_REGISTRY.update(originals)


def test_cne_demo_without_intraday_stays_six_steps(tmp_path, monkeypatch):
    """The default demo must not fetch intraday bars or mention them."""
    from cnequity.cli import demo as demo_mod

    called = []
    monkeypatch.setattr(demo_mod, "_run_intraday_demo", lambda *a, **k: called.append(1) or {})
    assert demo_mod._intraday_hint(None, None, "600519.SH") == ""
    assert called == []


@pytest.mark.usefixtures("chinese_output")
def test_run_intraday_demo_raises_when_the_step_fails(tmp_path):
    from cnequity.cli.demo import _run_intraday_demo
    from cnequity.config import Config

    cfg = Config(data_root=tmp_path / "lake")
    for sub in ("staging", "curated", "meta", "derived"):
        (cfg.data_root / sub).mkdir(parents=True, exist_ok=True)

    class FailingEngine:
        def run_job(self, *a, **k):
            return {"status": "failed"}

    with pytest.raises(click.ClickException, match="minute_bars 失败"):
        _run_intraday_demo(cfg, FailingEngine(), ["600519.SH"], date(2024, 6, 28), days=5)


@pytest.mark.usefixtures("chinese_output")
def test_run_intraday_demo_raises_when_no_rows_come_back(tmp_path, monkeypatch):
    from cnequity.cli.demo import _run_intraday_demo
    from cnequity.config import Config

    cfg = Config(data_root=tmp_path / "lake")
    for sub in ("staging", "curated", "meta", "derived"):
        (cfg.data_root / sub).mkdir(parents=True, exist_ok=True)

    class SucceedingEngine:
        def run_job(self, *a, **k):
            return {"status": "success"}

    monkeypatch.setattr("cnequity.query.reader.load", lambda *a, **k: pl.DataFrame({"symbol": []}))
    with pytest.raises(click.ClickException, match="没有返回任何行"):
        _run_intraday_demo(cfg, SucceedingEngine(), ["600519.SH"], date(2024, 6, 28), days=5)


def test_run_intraday_demo_retains_lake_when_source_cannot_serve_history(tmp_path, monkeypatch):
    from cnequity.cli.demo import _run_intraday_demo
    from cnequity.config import Config

    cfg = Config(data_root=tmp_path / "lake")

    class LimitedEngine:
        def run_job(self, *a, **k):
            return {
                "status": "degraded",
                "execution_status": "completed",
                "fallback": [{"reason_codes": ["capability_limit"]}],
            }

    monkeypatch.setattr("cnequity.query.reader.load", lambda *a, **k: pl.DataFrame())
    result = _run_intraday_demo(cfg, LimitedEngine(), ["600519.SH"], date(2024, 6, 28), days=5)
    assert result["execution_status"] == "completed"
    assert result["rows"] == 0
    assert result["fallback"] == [{"reason_codes": ["capability_limit"]}]


@pytest.mark.parametrize("research", [False, True])
def test_demo_enables_sina_only_for_the_research_view(tmp_path, monkeypatch, research):
    """`--research` derives hfq factors from Sina; the source gate must let it through."""
    from cnequity.adapters.throttle import SourceRateLimiters
    from cnequity.cli import demo as demo_mod

    seen = {}

    def stop_at_probe(cfg):
        seen["cfg"] = cfg
        raise RuntimeError("stop before the network")

    monkeypatch.setattr(demo_mod, "_probe_tdx", stop_at_probe)
    with pytest.raises(click.ClickException):
        demo_mod.run_demo(
            symbols=["600519.SH"],
            days=10,
            data_root=tmp_path / "lake",
            config_out=tmp_path / "demo.toml",
            research=research,
        )
    cfg = seen["cfg"]
    assert cfg.sources["eastmoney"] is False
    limiters = SourceRateLimiters(cfg)
    if research:
        with limiters.request("sina"):
            pass
    else:
        with pytest.raises(RuntimeError, match="source disabled"), limiters.request("sina"):
            pass


@pytest.mark.usefixtures("chinese_output")
def test_research_demo_keeps_bars_when_no_factor_source_is_available(monkeypatch):
    from types import SimpleNamespace

    from cnequity.cli.demo import _run_research_demo

    monkeypatch.setattr(
        "cnequity.derive.adj_factors.compute_adj_factors",
        lambda *a, **k: SimpleNamespace(
            failed=["600519.SH:hfq"],
            findings=[{"severity": "error", "check": "adj_factor_fetch_failed"}],
        ),
    )
    result = _run_research_demo(None, ["600519.SH"], date(2024, 6, 27), date(2024, 6, 28))
    assert result["execution_status"] == "completed"
    assert result["rows"] == 0
    assert "unadjusted daily bars" in result["fallback"]


@pytest.mark.usefixtures("chinese_output")
def test_research_demo_does_not_hide_factor_integrity_errors(monkeypatch):
    from types import SimpleNamespace

    from cnequity.cli.demo import _run_research_demo

    monkeypatch.setattr(
        "cnequity.derive.adj_factors.compute_adj_factors",
        lambda *a, **k: SimpleNamespace(
            failed=[], findings=[{"severity": "error", "check": "adj_factor_continuity"}]
        ),
    )
    with pytest.raises(click.ClickException, match="因子校验失败"):
        _run_research_demo(None, ["600519.SH"], date(2024, 6, 27), date(2024, 6, 28))


@pytest.mark.parametrize("missing", ["bars", "factors"])
def test_research_demo_uses_a_later_symbol_with_exact_inputs(monkeypatch, missing):
    from types import SimpleNamespace

    from cnequity.cli.demo import _run_research_demo
    from cnequity.query.reader import MissingAdjustmentError

    monkeypatch.setattr(
        "cnequity.derive.adj_factors.compute_adj_factors",
        lambda *a, **k: SimpleNamespace(
            failed=[],
            findings=[
                {
                    "severity": "warning",
                    "check": "adj_factor_source_unavailable",
                    "symbol": "600519.SH",
                }
            ],
        ),
    )
    raw = pl.DataFrame({"trade_date": [date(2024, 6, 28)], "close": [10.0]})
    adjusted = raw.with_columns(pl.lit(20.0).alias("adj_close"), pl.lit(True).alias("adj_is_exact"))

    def load(dataset, **kwargs):
        if kwargs["symbols"] == ["600519.SH"]:
            if missing == "bars":
                return pl.DataFrame()
            if kwargs.get("adjust"):
                raise MissingAdjustmentError("missing factors for first symbol")
        return adjusted if kwargs.get("adjust") else raw

    monkeypatch.setattr("cnequity.query.reader.load", load)
    result = _run_research_demo(
        None, ["600519.SH", "000001.SZ"], date(2024, 6, 27), date(2024, 6, 28)
    )
    assert result["symbol"] == "000001.SZ"
    assert result["exact"] is True


@pytest.mark.usefixtures("chinese_output")
def test_research_demo_finishes_when_derived_factors_do_not_cover_requested_bars(monkeypatch):
    from types import SimpleNamespace

    from cnequity.cli.demo import _run_research_demo
    from cnequity.query.reader import MissingAdjustmentError

    monkeypatch.setattr(
        "cnequity.derive.adj_factors.compute_adj_factors",
        lambda *a, **k: SimpleNamespace(failed=[], findings=[]),
    )

    def load(dataset, **kwargs):
        if kwargs.get("adjust"):
            raise MissingAdjustmentError("missing factors for requested dates")
        return pl.DataFrame({"trade_date": [date(2024, 6, 28)], "close": [10.0]})

    monkeypatch.setattr("cnequity.query.reader.load", load)
    result = _run_research_demo(None, ["600519.SH"], date(2024, 6, 27), date(2024, 6, 28))
    assert result["execution_status"] == "completed"
    assert result["status"] == "degraded"
    assert "unadjusted daily bars" in result["fallback"]


def test_research_demo_does_not_hide_other_reader_errors(monkeypatch):
    from types import SimpleNamespace

    from cnequity.cli.demo import _run_research_demo
    from cnequity.query.reader import ReaderError

    monkeypatch.setattr(
        "cnequity.derive.adj_factors.compute_adj_factors",
        lambda *a, **k: SimpleNamespace(failed=[], findings=[]),
    )

    def load(dataset, **kwargs):
        raise ReaderError("invalid adjustment dataset")

    monkeypatch.setattr("cnequity.query.reader.load", load)
    with pytest.raises(ReaderError, match="invalid adjustment dataset"):
        _run_research_demo(None, ["600519.SH"], date(2024, 6, 27), date(2024, 6, 28))


def test_run_intraday_demo_displays_an_available_symbol_from_partial_scope(tmp_path, monkeypatch):
    from cnequity.cli.demo import _run_intraday_demo
    from cnequity.config import Config

    cfg = Config(data_root=tmp_path / "lake")

    class LimitedEngine:
        def run_job(self, *a, **k):
            return {"status": "degraded", "execution_status": "completed"}

    monkeypatch.setattr(
        "cnequity.query.reader.load",
        lambda *a, **k: _minute_frame(["000001.SZ"], date(2024, 6, 28)),
    )
    result = _run_intraday_demo(
        cfg, LimitedEngine(), ["600519.SH", "000001.SZ"], date(2024, 6, 28), days=5
    )
    assert result["rows"] == 240
    assert result["full_sessions"] == 1


def test_the_demo_window_stops_before_a_forming_bar(tmp_path, monkeypatch):
    """`cne init --profile demo` is the quickest real-data trial.

    Run during a session it asked for today's bar, hit the finality guard, and
    died reporting a TDX connectivity problem that did not exist.
    """
    from datetime import date, datetime, timezone

    from cnequity.cli.demo import _last_trading_day
    from cnequity.config import Config
    from cnequity.domain.market_time import shanghai_now

    cfg = Config(data_root=tmp_path / "lake")
    sessions = [date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12)]
    monkeypatch.setattr(
        "cnequity.steps.common.list_trading_dates",
        lambda c, s, e: [d for d in sessions if s <= d <= e],
    )
    monkeypatch.setattr("cnequity.steps.bars.is_trading_day", lambda c, d: d in sessions)

    # 06:59 UTC is 14:59 in Shanghai — the 2026-08-12 bar is still forming.
    monkeypatch.setattr(
        "cnequity.steps.bars.shanghai_now",
        lambda now=None: shanghai_now(datetime(2026, 8, 12, 6, 59, tzinfo=timezone.utc)),
    )
    assert _last_trading_day(cfg, date(2026, 8, 12)) == date(2026, 8, 11)

    # 07:05 UTC is 15:05 — settled, so the demo may have today.
    monkeypatch.setattr(
        "cnequity.steps.bars.shanghai_now",
        lambda now=None: shanghai_now(datetime(2026, 8, 12, 7, 5, tzinfo=timezone.utc)),
    )
    assert _last_trading_day(cfg, date(2026, 8, 12)) == date(2026, 8, 12)
