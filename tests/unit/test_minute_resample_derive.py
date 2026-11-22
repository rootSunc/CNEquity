import os
from dataclasses import replace
from datetime import date, datetime, timedelta

import polars as pl
import pytest
from tests.catalog_seed import commit_tree, derive_into_lake, publish_rows

from cnequity.config import Config
from cnequity.derive.minute_resample import (
    derive_minute_resample,
    resample_session,
    stale_sessions,
)
from cnequity.query.parquet_scan import root_parquet_files
from cnequity.storage.revisions import RevisionStore
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
    commit_tree(
        RevisionStore(config.meta_root, config.curated_root, config.derived_root),
        dataset,
        run_id=f"seed-{day}",
        changed_files=[part / "part-000.parquet"],
        schema_version=1,
        contract_fingerprint="seed",
    )


def _lake(tmp_path) -> Config:
    config = Config(data_root=tmp_path / "data")
    _write(config, "minute_bars_5m", OLD, _bars("603869.SH", OLD, 5, 6))
    _write(config, "minute_bars_5m", NEW, _bars("603869.SH", NEW, 5, 6))
    _write(config, "minute_bars", NEW, _bars("603869.SH", NEW, 1, 30, volume=7))
    return config


def _derive(config: Config, dataset: str, **kwargs) -> dict:
    return derive_into_lake(config, dataset, derive_minute_resample, dataset, **kwargs)


def _published(config: Config, dataset: str) -> pl.DataFrame:
    """Every row the lake publishes for *dataset* (a derived resample)."""
    files = root_parquet_files(config.derived_root / dataset)
    return pl.concat([pl.read_parquet(path) for path in files], how="diagonal_relaxed")


def _age(files, seconds: float) -> None:
    for file in files:
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
    summary = _derive(config, "minute_bars_30m")
    assert summary["sessions"] == 2
    assert summary["symbol_days_from_1m"] == 1
    assert summary["symbol_days_from_5m"] == 1
    out = _published(config, "minute_bars_30m")
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
    _derive(config, "minute_bars_15m")
    assert stale_sessions(config, "minute_bars_15m") == []
    assert _derive(config, "minute_bars_15m")["rows"] == 0
    _write(config, "minute_bars", OLD, _bars("603869.SH", OLD, 1, 30))
    assert stale_sessions(config, "minute_bars_15m") == [OLD]
    summary = _derive(config, "minute_bars_15m")
    assert summary["sessions"] == 1
    assert summary["symbol_days_from_1m"] == 1


def test_restored_input_with_an_older_mtime_is_still_detected(tmp_path):
    config = _lake(tmp_path)
    _derive(config, "minute_bars_15m")
    # A rollback or backup restore: different content, preserved older mtime.
    _write(config, "minute_bars_5m", OLD, _bars("603869.SH", OLD, 5, 6, volume=50))
    _age(root_parquet_files(config.curated_root / "minute_bars_5m"), 3600)
    assert stale_sessions(config, "minute_bars_15m") == [OLD]


def test_changed_output_or_build_code_makes_sessions_stale(tmp_path, monkeypatch):
    import cnequity.derive.minute_resample as module

    config = _lake(tmp_path)
    _derive(config, "minute_bars_15m")
    session = _published(config, "minute_bars_15m").filter(pl.col("trade_date") == NEW)
    assert session.height > 1
    publish_rows(config, "minute_bars_15m", session.head(1), run_id="edit-output")
    assert stale_sessions(config, "minute_bars_15m") == [NEW]
    monkeypatch.setattr(module, "_derivation_identity", lambda: "new rules")
    assert stale_sessions(config, "minute_bars_15m") == [OLD, NEW]


def test_rebuild_that_leaves_nothing_removes_the_old_session(tmp_path):
    config = _lake(tmp_path)
    _derive(config, "minute_bars_15m")
    _write(config, "minute_bars", NEW, _bars("603869.SH", NEW, 1, 29))
    summary = _derive(config, "minute_bars_15m")
    assert (summary["sessions_cleared"], summary["skipped_partial_session_1m"]) == (1, 1)
    held = _published(config, "minute_bars_15m")
    assert held.filter(pl.col("trade_date") == NEW).is_empty()
    assert not held.filter(pl.col("trade_date") == OLD).is_empty()
    assert stale_sessions(config, "minute_bars_15m") == []


def test_explicit_window_rebuilds_only_that_window(tmp_path):
    config = _lake(tmp_path)
    _derive(config, "minute_bars_15m")
    summary = _derive(config, "minute_bars_15m", start=OLD, end=OLD)
    assert (summary["sessions"], summary["first"], summary["last"]) == (1, str(OLD), str(OLD))


def test_sessions_without_a_full_interval_are_counted_not_written(tmp_path):
    config = _lake(tmp_path)
    summary = _derive(config, "minute_bars_60m", full=True)
    assert summary["rows"] == 0
    assert (summary["skipped_partial_session_1m"], summary["skipped_partial_session_5m"]) == (1, 1)
    assert root_parquet_files(config.derived_root / "minute_bars_60m") == []


@pytest.fixture
def inputs_covered(monkeypatch):
    """The one-session test lake: the planner owes the minute inputs nothing else."""
    import cnequity.steps.finalize as finalize

    monkeypatch.setattr(finalize, "_planner_owes", lambda _config, _dataset, **_kw: False)


def _cli_config(tmp_path, *, intraday: bool = False) -> str:
    from cnequity.config.bootstrap import path_for_toml

    path = tmp_path / "cne.toml"
    content = f'[data]\nroot = "{path_for_toml(tmp_path / "data")}"\n'
    if intraday:
        content = f'extends = "builtin"\n{content}\n[minute_bars]\nenabled = true\n'
    path.write_text(content)
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
    # An explicit rebuild writes the same facts, so the catalog keeps the
    # published timestamp. Its receipt must keep matching that partition.
    rebuilt = CliRunner().invoke(
        cli, ["derive", "minute_bars_15m", "--config", cfg_path, "--start", str(OLD)]
    )
    assert rebuilt.exit_code == 0, rebuilt.output
    assert stale_sessions(config, "minute_bars_15m") == []
    # A backfill publishes the shorter 1m session as a new input revision.
    from cnequity.storage.revisions import RevisionStore

    config = load_config(cfg_path)
    _write(config, "minute_bars", NEW, _bars("603869.SH", NEW, 1, 29))
    commit_tree(
        RevisionStore(config.meta_root, config.curated_root),
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


def test_intraday_compacts_before_reconciling_already_stored_resamples(
    tmp_path, monkeypatch, inputs_covered
):
    from click.testing import CliRunner

    from cnequity.cli.main import cli
    from cnequity.config import load_config
    from cnequity.orchestrator.engine import JobEngine
    from cnequity.orchestrator.registry import STEP_REGISTRY
    from cnequity.query import load
    from cnequity.storage import StagingWriter

    cfg_path = _cli_config(tmp_path, intraday=True)
    config = load_config(cfg_path)
    _write(config, "minute_bars_5m", OLD, _bars("603869.SH", OLD, 5, 12))
    for dataset in ("minute_bars_15m", "minute_bars_30m"):
        activated = CliRunner().invoke(cli, ["derive", dataset, "--config", cfg_path])
        assert activated.exit_code == 0, activated.output

    def stage(config, trade_date, run_id, context):
        frame = _bars("603869.SH", trade_date, 1, 60)
        StagingWriter(config.staging_root).write_batch("minute_bars", run_id, "test", frame)
        return {"rows_written": frame.height}

    monkeypatch.setitem(
        STEP_REGISTRY,
        "minute_bars",
        replace(STEP_REGISTRY["minute_bars"], fn=stage, requires_workers=False),
    )
    monkeypatch.setitem(
        STEP_REGISTRY,
        "minute_bars_5m",
        replace(STEP_REGISTRY["minute_bars_5m"], fn=lambda *_: {}, requires_workers=False),
    )
    engine = JobEngine(config)
    steps = config.schedule_groups["intraday"].steps
    out = engine.run_job("daily:intraday", NEW, steps=steps, backfill=True)
    assert out["status"] == "success", out
    for dataset in ("minute_bars_15m", "minute_bars_30m"):
        assert load(dataset, config=config)["trade_date"].unique().sort().to_list() == [OLD, NEW]
        assert stale_sessions(config, dataset) == []
        assert engine.manifest.get_dataset_result(out["run_id"], dataset, "derive")["status"] == (
            "success"
        )
    assert (
        RevisionStore(config.meta_root, config.curated_root).current_pointer("minute_bars_60m")
        is None
    )

    repeat = engine.run_job(
        "derive:minute_bars", NEW, steps=steps[steps.index("compact") + 1 :], backfill=True
    )
    assert repeat["status"] == "success"
    assert not repeat["rows_written"]


def test_minute_backfill_reconciles_stored_resamples_after_publish(
    tmp_path, monkeypatch, inputs_covered
):
    import json

    from click.testing import CliRunner

    from cnequity.cli import backfill_cmds
    from cnequity.cli.main import cli
    from cnequity.config import load_config
    from cnequity.query import load

    cfg_path = _cli_config(tmp_path, intraday=True)
    config = load_config(cfg_path)
    _write(config, "minute_bars_5m", OLD, _bars("603869.SH", OLD, 5, 6))
    activated = CliRunner().invoke(cli, ["derive", "minute_bars_30m", "--config", cfg_path])
    assert activated.exit_code == 0, activated.output

    def backfill(config, dataset, start, end, chunk_symbols):
        assert (dataset, start, end) == ("minute_bars", OLD, OLD)
        _write(config, dataset, OLD, _bars("603869.SH", OLD, 1, 30))
        return {"status": "success", "rows_written": 30}

    monkeypatch.setattr(backfill_cmds, "_backfill_symbol_chunked", backfill)
    finished = CliRunner().invoke(
        cli,
        ["backfill", "minute_bars", "--config", cfg_path, "--start", str(OLD), "--end", str(OLD)],
    )
    assert finished.exit_code == 0, finished.output
    assert json.loads(finished.stdout)["followup"]["status"] == "success"
    assert load("minute_bars_30m", config=config)["resampled_from"].to_list() == ["1m"]
    assert stale_sessions(config, "minute_bars_30m") == []


def test_rejected_resample_publication_is_still_pending(tmp_path, monkeypatch, inputs_covered):
    from click.testing import CliRunner

    from cnequity.cli.main import cli
    from cnequity.config import load_config
    from cnequity.orchestrator.engine import JobEngine
    from cnequity.orchestrator.outcomes import InputChangedError
    from cnequity.steps import minute_resamples

    cfg_path = _cli_config(tmp_path, intraday=True)
    config = load_config(cfg_path)
    dataset = "minute_bars_15m"
    _write(config, "minute_bars_5m", OLD, _bars("603869.SH", OLD, 5, 6))
    activated = CliRunner().invoke(cli, ["derive", dataset, "--config", cfg_path])
    assert activated.exit_code == 0, activated.output
    _write(config, "minute_bars", NEW, _bars("603869.SH", NEW, 1, 30))

    def reject(*args, **kwargs):
        raise InputChangedError("input changed during calculation")

    engine = JobEngine(config)
    steps = [f"derive_{dataset}"]
    with monkeypatch.context() as patch:
        patch.setattr(minute_resamples, "publish_derived_revision", reject)
        failed = engine.run_job("derive:minute_bars", NEW, steps=steps, backfill=True)
    receipt = engine.manifest.get_dataset_result(failed["run_id"], dataset, "derive")
    assert receipt["execution_status"] == "failed"
    assert stale_sessions(config, dataset) == [NEW]

    retried = engine.run_job("derive:minute_bars", NEW, steps=steps, backfill=True)
    assert retried["status"] == "success"
    assert stale_sessions(config, dataset) == []


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
        return (
            TestClient(app, base_url="http://127.0.0.1").get("/api/datasets/minute_bars_15m").json()
        )

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
    commit_tree(
        RevisionStore(config.meta_root, config.curated_root),
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
