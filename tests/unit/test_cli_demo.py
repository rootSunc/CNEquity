"""Offline coverage for `cml demo` (real network is mocked)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import click
import polars as pl
import pytest
from click.testing import CliRunner

from cn_market_lake.cli.main import cli
from cn_market_lake.domain.schemas import validate_dataframe, with_provenance
from cn_market_lake.orchestrator.registry import STEP_REGISTRY, StepEntry


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


def test_cml_demo_offline(tmp_path, monkeypatch):
    symbols = ["600519.SH", "000001.SZ"]
    monkeypatch.setattr("cn_market_lake.cli.demo._probe_tdx", lambda cfg: None)
    monkeypatch.setattr(
        "cn_market_lake.adapters.tdx_protocol.client.fetch_instruments",
        lambda **kwargs: _inst_frame(symbols),
    )
    monkeypatch.setattr(
        "cn_market_lake.adapters.tdx_protocol.client.normalize_with_source",
        lambda df: df,
    )

    def fake_calendar(config, trade_date, run_id, context):
        from cn_market_lake.storage.atomic import write_parquet_atomic

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
        for (year,), group in (
            df.with_columns(pl.col("trade_date").dt.year().alias("_y"))
            .partition_by("_y", as_dict=True)
            .items()
        ):
            out = config.curated_root / "trading_calendar" / f"trade_date={year}"
            out.mkdir(parents=True, exist_ok=True)
            write_parquet_atomic(out / "part-000.parquet", group.drop("_y"))
        return {"rows_read": df.height, "rows_written": df.height}

    def fake_daily_bars(config, trade_date, run_id, context):
        from cn_market_lake.storage import StagingWriter

        start = getattr(config, "_backfill_start", date(2024, 6, 1))
        end = getattr(config, "_backfill_end", trade_date)
        df = validate_dataframe(_bars_frame(symbols, start, end), "daily_bars")
        StagingWriter(config.staging_root).write_batch("daily_bars", run_id, "batch-0", df)
        return {"rows_read": df.height, "rows_written": df.height}

    def fake_compact(config, trade_date, run_id, context):
        from cn_market_lake.storage.parquet import compact_dataset
        from cn_market_lake.storage.state import StateStore

        n = compact_dataset(config.staging_root, config.curated_root, "daily_bars", run_id)
        StateStore(config.meta_root).set_date("daily_bars", trade_date)
        return {"rows_read": n, "rows_written": n}

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
        assert "Probe TDX" in result.output
        assert "600519.SH" in result.output
        assert config_out.exists()
        assert (data_root / "curated" / "instruments" / "part-merged.parquet").exists()
        assert list((data_root / "curated" / "daily_bars").glob("**/*.parquet"))
    finally:
        STEP_REGISTRY.update(originals)


def test_demo_help_lists_command():
    result = CliRunner().invoke(cli, ["demo", "--help"])
    assert result.exit_code == 0
    assert "--symbols" in result.output
    assert "--days" in result.output
    assert "--research" in result.output


def test_return_summary_compares_raw_and_adjusted_series():
    from cn_market_lake.cli.demo import _return_summary

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
    from cn_market_lake.cli.demo import _run_research_demo

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

    monkeypatch.setattr("cn_market_lake.derive.adj_factors.compute_adj_factors", fake_compute)
    monkeypatch.setattr("cn_market_lake.query.reader.load", fake_load)

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
    """Bare ``C:\\Users\\…`` is invalid TOML; follow-up ``cml query`` would fail."""
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib

    from cn_market_lake.cli.demo import _write_demo_toml

    out = tmp_path / "demo.toml"
    # Simulate a native Windows resolve() result even on Unix CI.
    data_root = tmp_path / "Users" / "测试" / "lake"
    data_root.mkdir(parents=True)
    _write_demo_toml(out, data_root)

    payload = tomllib.loads(out.read_text(encoding="utf-8"))
    assert "\\" not in payload["data"]["root"]
    assert Path(payload["data"]["root"]).name == "lake"


def test_probe_tdx_closes_its_client():
    """Probe must close the client even though the heartbeat is now a daemon.

    Before the close, an abandoned non-daemon thread kept the interpreter
    alive after all six demo steps had already printed.
    """
    from unittest.mock import patch

    from cn_market_lake.cli import demo as demo_mod

    client = object()
    with (
        patch.object(demo_mod, "_quotes_client", create=True),
        patch(
            "cn_market_lake.adapters.tdx_protocol.client._quotes_client",
            return_value=client,
        ),
        patch("cn_market_lake.adapters.tdx_protocol.session.close_quotes_client") as closer,
    ):
        demo_mod._probe_tdx(object())

    closer.assert_called_once_with(client)


def _minute_frame(symbols: list[str], day: date, bars: int = 240) -> pl.DataFrame:
    """A synthetic full session, right-labelled like the real source."""
    from datetime import datetime, timedelta

    from cn_market_lake.adapters.tdx_protocol.minute_bars import in_session

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


def test_cml_demo_intraday_offline(tmp_path, monkeypatch):
    """`cml demo --intraday` adds a 7th step and prints a real session."""
    symbols = ["600519.SH", "000001.SZ"]
    monkeypatch.setattr("cn_market_lake.cli.demo._probe_tdx", lambda cfg: None)
    monkeypatch.setattr(
        "cn_market_lake.adapters.tdx_protocol.client.fetch_instruments",
        lambda **kwargs: _inst_frame(symbols),
    )
    monkeypatch.setattr(
        "cn_market_lake.adapters.tdx_protocol.client.normalize_with_source",
        lambda df: df,
    )

    def fake_calendar(config, trade_date, run_id, context):
        from cn_market_lake.storage.atomic import write_parquet_atomic

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
        from cn_market_lake.storage import StagingWriter

        start = getattr(config, "_backfill_start", date(2024, 6, 1))
        end = getattr(config, "_backfill_end", trade_date)
        df = validate_dataframe(_bars_frame(symbols, start, end), "daily_bars")
        StagingWriter(config.staging_root).write_batch("daily_bars", run_id, "batch-0", df)
        return {"rows_read": df.height, "rows_written": df.height}

    def fake_minute_bars(config, trade_date, run_id, context):
        from cn_market_lake.storage import StagingWriter

        # The demo must have configured the watchlist scope before we ran.
        assert config.minute_bars_enabled
        assert config.minute_bars_scope == "watchlist"
        df = validate_dataframe(_minute_frame(symbols, date(2024, 6, 28)), "minute_bars")
        StagingWriter(config.staging_root).write_batch("minute_bars", run_id, "batch-0", df)
        return {"rows_read": df.height, "rows_written": df.height}

    def fake_compact(config, trade_date, run_id, context):
        from cn_market_lake.storage.parquet import compact_dataset

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
        assert "[7/7] minute_bars" in result.output
        assert "hold a full 240-bar session" in result.output
        assert "bar_time is the CLOSING minute" in result.output
        assert 'load("minute_bars"' in result.output
        assert list((data_root / "curated" / "minute_bars").glob("**/*.parquet"))
    finally:
        STEP_REGISTRY.update(originals)


def test_cml_demo_without_intraday_stays_six_steps(tmp_path, monkeypatch):
    """The default demo must not fetch intraday bars or mention them."""
    from cn_market_lake.cli import demo as demo_mod

    called = []
    monkeypatch.setattr(demo_mod, "_run_intraday_demo", lambda *a, **k: called.append(1) or {})
    assert demo_mod._intraday_hint(None, None, "600519.SH") == ""
    assert called == []


def test_run_intraday_demo_raises_when_the_step_fails(tmp_path):
    from cn_market_lake.cli.demo import _run_intraday_demo
    from cn_market_lake.config import Config

    cfg = Config(data_root=tmp_path / "lake")
    for sub in ("staging", "curated", "meta", "derived"):
        (cfg.data_root / sub).mkdir(parents=True, exist_ok=True)

    class FailingEngine:
        def run_job(self, *a, **k):
            return {"status": "failed"}

    with pytest.raises(click.ClickException, match="minute_bars failed"):
        _run_intraday_demo(cfg, FailingEngine(), ["600519.SH"], date(2024, 6, 28), days=5)


def test_run_intraday_demo_raises_when_no_rows_come_back(tmp_path, monkeypatch):
    from cn_market_lake.cli.demo import _run_intraday_demo
    from cn_market_lake.config import Config

    cfg = Config(data_root=tmp_path / "lake")
    for sub in ("staging", "curated", "meta", "derived"):
        (cfg.data_root / sub).mkdir(parents=True, exist_ok=True)

    class SucceedingEngine:
        def run_job(self, *a, **k):
            return {"status": "success"}

    monkeypatch.setattr(
        "cn_market_lake.query.reader.load", lambda *a, **k: pl.DataFrame({"symbol": []})
    )
    with pytest.raises(click.ClickException, match="returned no rows"):
        _run_intraday_demo(cfg, SucceedingEngine(), ["600519.SH"], date(2024, 6, 28), days=5)
