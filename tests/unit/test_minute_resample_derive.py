import os
from datetime import date, datetime, timedelta

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.derive.minute_resample import (
    derive_minute_resample,
    resample_session,
    stale_sessions,
)
from cnequity.storage.state import StateStore

OLD, NEW = date(2026, 9, 14), date(2026, 9, 15)


def _bars(symbol: str, day: date, step: int, count: int, volume: int = 100) -> pl.DataFrame:
    start = datetime.combine(day, datetime.min.time()) + timedelta(hours=9, minutes=30 + step)
    return pl.DataFrame(
        {
            "symbol": [symbol] * count,
            "trade_date": [day] * count,
            "bar_time": [start + timedelta(minutes=i * step) for i in range(count)],
            "frequency": [f"{step}m"] * count,
            **{k: [10.0] * count for k in ["open", "high", "low", "close"]},
            "volume": [volume] * count,
            "amount": [volume * 10.0] * count,
            "source": ["tdx_protocol"] * count,
            "data_version": ["v1"] * count,
            "fetched_at": [datetime(2026, 9, 16)] * count,
        }
    ).with_columns(pl.col("fetched_at").dt.replace_time_zone("UTC"))


def _write(config: Config, dataset: str, day: date, frame: pl.DataFrame) -> None:
    part = config.curated_root / dataset / f"trade_date={day.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(part / "part-000.parquet")


def _lake(tmp_path) -> Config:
    config = Config(data_root=tmp_path / "data")
    _write(config, "minute_bars_5m", OLD, _bars("603869.SH", OLD, 5, 6))
    _write(config, "minute_bars_5m", NEW, _bars("603869.SH", NEW, 5, 6))
    _write(config, "minute_bars", NEW, _bars("603869.SH", NEW, 1, 30, volume=7))
    return config


def _age(path, seconds: float) -> None:
    for file in path.rglob("*.parquet"):
        stat = file.stat()
        os.utime(file, (stat.st_atime - seconds, stat.st_mtime - seconds))


def test_session_skips_incomplete_symbol_days_without_falling_back():
    halted = _bars("000016.SZ", NEW, 1, 14, volume=0)
    # Resumed at 09:35 after an intraday suspension: unbroken, just late.
    resumed = _bars("600519.SH", NEW, 1, 15).slice(4)
    # A hole between bars of a traded day is what a missing fetch looks like.
    hole = _bars("600000.SH", NEW, 1, 15).filter(pl.col("bar_time").dt.minute() != 38)
    bars, skipped = resample_session(
        pl.concat([_bars("603869.SH", NEW, 1, 15), halted, resumed, hole]),
        pl.concat(
            [_bars(s, NEW, 5, 3) for s in ("603869.SH", "000016.SZ", "600519.SH", "600000.SH")]
        ),
        "15m",
    )
    assert bars.select("symbol", "resampled_from").rows() == [("603869.SH", "1m")]
    assert skipped.sort("symbol").rows() == [
        ("000016.SZ", NEW, "1m", "halted"),
        ("600000.SH", NEW, "1m", "incomplete"),
        ("600519.SH", NEW, "1m", "partial_session"),
    ]


def test_session_isolates_a_rejected_symbol_day_and_keeps_the_rest():
    off_session = _bars("000016.SZ", NEW, 1, 15).with_columns(
        pl.col("bar_time").dt.offset_by("150m")  # 12:01-12:15, inside the lunch break
    )
    bars, skipped = resample_session(
        pl.concat([_bars("603869.SH", NEW, 1, 15), off_session]),
        _bars("x", NEW, 5, 1).clear(),
        "15m",
    )
    assert bars["symbol"].to_list() == ["603869.SH"]
    assert skipped.select("symbol", "input").rows() == [("000016.SZ", "1m")]
    assert "session" in skipped["reason"][0]


def test_derive_writes_each_session_from_its_best_input(tmp_path):
    config = _lake(tmp_path)
    summary = derive_minute_resample(config, "minute_bars_30m")
    assert summary["sessions"] == 2
    assert summary["symbol_days_from_1m"] == 1
    assert summary["symbol_days_from_5m"] == 1
    out = pl.read_parquet(config.derived_root / "minute_bars_30m" / "**" / "*.parquet")
    assert out.sort("trade_date").select("trade_date", "resampled_from", "volume").rows() == [
        (OLD, "5m", 600),
        (NEW, "1m", 210),
    ]
    assert set(out["frequency"]) == {"30m"}
    assert set(out["source"]) == {"derived"}
    # Unscheduled, so no watermark that could only ever read as stale.
    assert StateStore(config.meta_root).get_date("minute_bars_30m") is None


def test_later_one_minute_backfill_makes_the_session_stale(tmp_path):
    config = _lake(tmp_path)
    derive_minute_resample(config, "minute_bars_15m")
    assert stale_sessions(config, "minute_bars_15m") == []
    assert derive_minute_resample(config, "minute_bars_15m")["rows"] == 0
    _write(config, "minute_bars", OLD, _bars("603869.SH", OLD, 1, 30))
    assert stale_sessions(config, "minute_bars_15m") == [OLD]
    summary = derive_minute_resample(config, "minute_bars_15m")
    assert summary["sessions"] == 1
    assert summary["symbol_days_from_1m"] == 1


def test_restored_input_with_an_older_mtime_is_still_detected(tmp_path):
    config = _lake(tmp_path)
    derive_minute_resample(config, "minute_bars_15m")
    # A rollback or backup restore: different content, preserved older mtime.
    _write(config, "minute_bars_5m", OLD, _bars("603869.SH", OLD, 5, 6, volume=50))
    _age(config.curated_root / "minute_bars_5m" / f"trade_date={OLD.isoformat()}", 3600)
    assert stale_sessions(config, "minute_bars_15m") == [OLD]


def test_changed_output_or_build_code_makes_sessions_stale(tmp_path, monkeypatch):
    import cnequity.derive.minute_resample as module

    config = _lake(tmp_path)
    derive_minute_resample(config, "minute_bars_15m")
    part = config.derived_root / "minute_bars_15m" / f"trade_date={NEW.isoformat()}"
    pl.read_parquet(part / "part-000.parquet").head(1).write_parquet(part / "part-000.parquet")
    assert stale_sessions(config, "minute_bars_15m") == [NEW]
    monkeypatch.setattr(module, "_derivation_identity", lambda: "new rules")
    assert stale_sessions(config, "minute_bars_15m") == [OLD, NEW]


def test_rebuild_that_leaves_nothing_removes_the_old_session(tmp_path):
    config = _lake(tmp_path)
    derive_minute_resample(config, "minute_bars_15m")
    _write(config, "minute_bars", NEW, _bars("603869.SH", NEW, 1, 29))
    summary = derive_minute_resample(config, "minute_bars_15m")
    assert (summary["sessions_cleared"], summary["skipped_partial_session_1m"]) == (1, 1)
    part = config.derived_root / "minute_bars_15m" / f"trade_date={NEW.isoformat()}"
    assert pl.read_parquet(part / "part-000.parquet").is_empty()
    assert stale_sessions(config, "minute_bars_15m") == []


def test_explicit_window_rebuilds_only_that_window(tmp_path):
    config = _lake(tmp_path)
    derive_minute_resample(config, "minute_bars_15m")
    summary = derive_minute_resample(config, "minute_bars_15m", start=OLD, end=OLD)
    assert (summary["sessions"], summary["first"], summary["last"]) == (1, str(OLD), str(OLD))


def test_sessions_without_a_full_interval_are_counted_not_written(tmp_path):
    config = _lake(tmp_path)
    summary = derive_minute_resample(config, "minute_bars_60m", full=True)
    assert summary["rows"] == 0
    assert (summary["skipped_partial_session_1m"], summary["skipped_partial_session_5m"]) == (1, 1)
    assert not (config.derived_root / "minute_bars_60m").exists()


def _cli_config(tmp_path) -> str:
    from cnequity.config.bootstrap import path_for_toml

    path = tmp_path / "cne.toml"
    path.write_text(f'[data]\nroot = "{path_for_toml(tmp_path / "data")}"\n')
    return str(path)


def test_cli_derive_publishes_a_readable_dataset(tmp_path):
    from click.testing import CliRunner

    from cnequity.cli.main import cli
    from cnequity.query import load

    _lake(tmp_path)
    cfg_path = _cli_config(tmp_path)
    result = CliRunner().invoke(cli, ["derive", "minute_bars_15m", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert '"symbol_days_from_1m": 1' in result.output
    frame = load("minute_bars_15m", data_root=tmp_path / "data")
    assert frame.sort("trade_date")["resampled_from"].to_list() == ["5m", "5m", "1m", "1m"]


def test_cli_derive_without_minute_input_says_what_to_fetch(tmp_path):
    from click.testing import CliRunner

    from cnequity.cli.main import cli

    cfg_path = _cli_config(tmp_path)
    result = CliRunner().invoke(cli, ["derive", "minute_bars_30m", "--config", cfg_path])
    assert result.exit_code != 0
    assert "minute_bars_5m" in str(result.exception) + result.output


def test_cli_derive_is_current_after_publishing_and_drops_emptied_sessions(tmp_path):
    from click.testing import CliRunner

    from cnequity.cli.main import cli
    from cnequity.config import load_config
    from cnequity.query import load

    _lake(tmp_path)
    cfg_path = _cli_config(tmp_path)
    run = lambda: CliRunner().invoke(cli, ["derive", "minute_bars_15m", "--config", cfg_path])  # noqa: E731
    assert run().exit_code == 0
    # The receipt's output digest must equal the published revision's.
    config = load_config(cfg_path)
    assert stale_sessions(config, "minute_bars_15m") == []
    assert '"rows": 0' in run().output
    # A backfill publishes the shorter 1m session as a new input revision.
    from cnequity.storage.revisions import RevisionStore

    config = load_config(cfg_path)
    _write(config, "minute_bars", NEW, _bars("603869.SH", NEW, 1, 29))
    RevisionStore(config.meta_root, config.curated_root).commit(
        "minute_bars",
        run_id="backfill",
        changed_files=[
            config.curated_root
            / "minute_bars"
            / f"trade_date={NEW.isoformat()}"
            / "part-000.parquet"
        ],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert stale_sessions(config, "minute_bars_15m") == [NEW]
    result = run()
    assert result.exit_code == 0, result.output
    assert '"sessions_cleared": 1' in result.output
    frame = load("minute_bars_15m", data_root=tmp_path / "data")
    assert frame["trade_date"].unique().to_list() == [OLD]
    assert stale_sessions(load_config(cfg_path), "minute_bars_15m") == []


@pytest.mark.usefixtures("chinese_output")
def test_backlog_reaches_the_panel_and_status(tmp_path):
    from click.testing import CliRunner
    from fastapi.testclient import TestClient

    from cnequity.cli.main import cli
    from cnequity.config import load_config
    from cnequity.serve.app import create_app

    _lake(tmp_path)
    cfg_path = _cli_config(tmp_path)

    def detail():
        app = create_app(load_config(cfg_path), read_only=True)
        return TestClient(app).get("/api/datasets/minute_bars_15m").json()

    before = detail()
    assert before["resample"] == {"pending": 2, "input_sessions": 2, "derived": False}
    assert before["commands"][0]["why"] == "默认不计算；从 1m / 5m 重采样后入湖"
    CliRunner().invoke(cli, ["derive", "minute_bars_15m", "--config", cfg_path])
    current = detail()
    assert current["resample"]["pending"] == 0
    assert current["commands"][0]["why"] == "已与 1m / 5m 一致"
    # A newly published 1m session the derive has not seen yet.
    from cnequity.storage.revisions import RevisionStore

    config = load_config(cfg_path)
    _write(config, "minute_bars", OLD, _bars("603869.SH", OLD, 1, 30))
    RevisionStore(config.meta_root, config.curated_root).commit(
        "minute_bars",
        run_id="backfill",
        changed_files=[
            config.curated_root
            / "minute_bars"
            / f"trade_date={OLD.isoformat()}"
            / "part-000.parquet"
        ],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert detail()["commands"][0]["why"] == "输入有 1 个交易日尚未重算"
    # Resamples are on demand, so status and check no longer nag about them;
    # the dataset page above is where the backlog shows.
    status = CliRunner().invoke(cli, ["status", "--datasets", "--config", cfg_path])
    assert "尚未重算" not in status.output
