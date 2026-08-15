"""Coverage verification.

`cml audit` asks whether the data that landed is correct; this asks whether the
data that should have landed did. Every defect that ran unnoticed for weeks this
session was the second kind — a step raising on contact, the run recording a
failed batch, and nothing summing those into "this has not succeeded since the
3rd".

The load-bearing distinction is which gaps are faults. A `by_date` daily dataset
missing a session is one; a snapshot dataset missing one is its shape, and no
backfill can honestly fill it. Getting that backwards would either hide real
holes or propose repairs that can never work.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.domain.datasets import DATASETS
from cn_market_lake.quality.verify import verify_dataset, verify_lake

ANCHOR = date(2026, 8, 7)


def _meta():
    return {"source": "t", "data_version": "v1", "fetched_at": None}


def _calendar(cfg: Config, days: list[date]) -> None:
    root = cfg.curated_root / "trading_calendar" / "trade_date=2026"
    root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([{"trade_date": d, "is_trading": True, **_meta()} for d in days]).write_parquet(
        root / "part-0.parquet"
    )


def _write_days(cfg: Config, dataset: str, days: list[date], *, layer: str = "curated") -> None:
    base = cfg.derived_root if layer == "derived" else cfg.curated_root
    for d in days:
        part = base / dataset / f"trade_date={d.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [d]}).write_parquet(
            part / "part-0.parquet"
        )


def test_required_dataset_with_nothing_in_it_is_a_gap(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    cfg.curated_root.mkdir(parents=True, exist_ok=True)
    gaps = verify_dataset(cfg, DATASETS["daily_bars"], anchor=ANCHOR, watermark=None)
    assert [g.kind for g in gaps] == ["empty"]
    assert gaps[0].repairable is True


def test_optional_empty_dataset_is_not_reported(tmp_path):
    """minute_bars is off by default; saying so every run is how a report dies."""
    cfg = Config(data_root=tmp_path / "lake")
    cfg.curated_root.mkdir(parents=True, exist_ok=True)
    assert DATASETS["minute_bars"].required is False
    assert verify_dataset(cfg, DATASETS["minute_bars"], anchor=ANCHOR, watermark=None) == []


def test_interior_hole_in_a_daily_by_date_dataset_is_a_fault(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    sessions = [date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5), date(2026, 8, 6)]
    _calendar(cfg, sessions)
    # 08-05 never landed.
    _write_days(cfg, "daily_bars", [date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 6)])

    gaps = verify_dataset(
        cfg, DATASETS["daily_bars"], anchor=date(2026, 8, 6), watermark=date(2026, 8, 6)
    )
    interior = [g for g in gaps if g.kind == "interior"]
    assert len(interior) == 1
    gap = interior[0]
    assert gap.missing_days == 1
    assert gap.sample == (date(2026, 8, 5),)
    assert gap.start == gap.end == date(2026, 8, 5)
    assert gap.repairable is True


def test_snapshot_only_dataset_is_never_told_to_backfill(tmp_path):
    """fund_flow is snapshot with no backfill_source — a missing day cannot be
    filled, and proposing it would be proposing forged rows."""
    spec = DATASETS["fund_flow"]
    assert spec.fetch_semantics == "snapshot" and spec.backfill_source is None

    cfg = Config(data_root=tmp_path / "lake")
    cfg.curated_root.mkdir(parents=True, exist_ok=True)
    gaps = verify_dataset(cfg, spec, anchor=ANCHOR, watermark=None)
    assert [g.kind for g in gaps] == ["empty"]
    assert gaps[0].repairable is False
    assert gaps[0].repair_command("cfg.toml") is None


def test_stale_head_is_measured_against_the_datasets_own_tolerance(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    sessions = [date(2026, 8, 6), date(2026, 8, 7)]
    _calendar(cfg, sessions)
    _write_days(cfg, "daily_bars", sessions)

    # Watermark two months back, well past daily_bars' 1-day tolerance.
    gaps = verify_dataset(cfg, DATASETS["daily_bars"], anchor=ANCHOR, watermark=date(2026, 6, 1))
    stale = [g for g in gaps if g.kind == "stale"]
    assert len(stale) == 1
    assert stale[0].start == date(2026, 6, 1)
    assert stale[0].end == ANCHOR


def test_a_dataset_current_to_the_anchor_reports_nothing(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    sessions = [date(2026, 8, 6), date(2026, 8, 7)]
    _calendar(cfg, sessions)
    _write_days(cfg, "daily_bars", sessions)
    assert verify_dataset(cfg, DATASETS["daily_bars"], anchor=ANCHOR, watermark=ANCHOR) == []


def test_repair_command_carries_the_window(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    cfg.curated_root.mkdir(parents=True, exist_ok=True)
    gap = verify_dataset(cfg, DATASETS["daily_bars"], anchor=ANCHOR, watermark=None)[0]
    assert gap.repair_command("my.toml") == "cml backfill daily_bars --config my.toml"


def test_verify_lake_skips_unknown_dataset_names(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    cfg.curated_root.mkdir(parents=True, exist_ok=True)
    assert verify_lake(cfg, anchor=ANCHOR, datasets=["not_a_dataset"]) == []


# --- retired sources ---------------------------------------------------------
# A feed that stopped publishing is not a broken pipeline. Without this the two
# are indistinguishable: the watermark freezes, is_stale says stale forever, and
# verify offers a backfill that runs the whole window, writes zero rows, and
# leaves the identical gap. northbound_flows is the real case — the exchanges
# stopped publishing daily net flow after 2024-08-16.


def test_a_retired_source_caught_up_to_its_last_session_is_not_a_gap(tmp_path):
    spec = DATASETS["northbound_flows"]
    assert spec.source_retired_date == date(2024, 8, 16)

    cfg = Config(data_root=tmp_path / "lake")
    part = cfg.curated_root / "northbound_flows" / "trade_date=2024"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"trade_date": [date(2024, 8, 16)]}).write_parquet(part / "part-0.parquet")

    gaps = verify_dataset(cfg, spec, anchor=ANCHOR, watermark=date(2024, 8, 16))
    assert gaps == [], "the lake holds everything that exists"


def test_a_retired_source_short_of_its_last_session_is_still_a_gap(tmp_path):
    """Retirement must not blanket-silence the dataset — only the part past it."""
    spec = DATASETS["northbound_flows"]
    cfg = Config(data_root=tmp_path / "lake")
    part = cfg.curated_root / "northbound_flows" / "trade_date=2024"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"trade_date": [date(2024, 1, 5)]}).write_parquet(part / "part-0.parquet")

    gaps = verify_dataset(cfg, spec, anchor=ANCHOR, watermark=date(2024, 1, 5))
    stale = [g for g in gaps if g.kind == "stale"]
    assert len(stale) == 1
    # Repair window ends at retirement, not today — the months after it are empty.
    assert stale[0].end == date(2024, 8, 16)


def test_is_stale_respects_retirement():
    from cn_market_lake.domain.datasets import is_stale

    retired = date(2024, 8, 16)
    assert is_stale("northbound_flows", retired, ANCHOR) is False
    assert is_stale("northbound_flows", date(2024, 1, 5), ANCHOR) is True
    # A live dataset is unaffected.
    assert is_stale("daily_bars", date(2026, 6, 1), ANCHOR) is True


# --- the CLI repair loop -----------------------------------------------------


def _cli_lake(tmp_path):
    """A config + lake whose daily_bars is missing one session."""
    from cn_market_lake.config import load_config
    from cn_market_lake.config.bootstrap import path_for_toml

    cfg_path = tmp_path / "cfg.toml"
    cfg_path.write_text(
        f"""
[data]
root = "{path_for_toml(tmp_path / "lake")}"

[orchestrator]
workers = 1

[[job.daily.waves]]
name = "core"
parallel = false
steps = ["daily_bars"]
""",
        encoding="utf-8",
    )
    cfg = load_config(cfg_path)
    sessions = [date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5), date(2026, 8, 6)]
    _calendar(cfg, sessions)
    _write_days(cfg, "daily_bars", [date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 6)])
    return cfg_path


def test_cli_reports_the_gap_and_exits_nonzero(tmp_path):
    from click.testing import CliRunner

    from cn_market_lake.cli.main import cli

    res = CliRunner().invoke(
        cli, ["verify", "--config", str(_cli_lake(tmp_path)), "--dataset", "daily_bars"]
    )
    assert res.exit_code == 1, "a gap must be scriptable as a failure"
    assert "2026-08-05" in res.output
    assert "cml backfill daily_bars" in res.output


def test_cli_repair_does_not_claim_success_when_the_step_failed(tmp_path, monkeypatch):
    """Regression: the engine records a failed step and returns status="failed"
    rather than raising, so an exception-only check printed a traceback and then
    said 全部修复完成 immediately under it."""
    from click.testing import CliRunner

    from cn_market_lake.cli import main as cli_main

    monkeypatch.setattr(
        cli_main,
        "_run_backfill",
        lambda cfg, ds, start, end: {"status": "failed", "rows_written": 0},
    )
    res = CliRunner().invoke(
        cli_main.cli,
        ["verify", "--config", str(_cli_lake(tmp_path)), "--dataset", "daily_bars", "--repair"],
    )
    assert res.exit_code == 1
    assert "status=failed" in res.output
    assert "全部修复完成" not in res.output


def test_cli_repair_says_so_when_the_window_is_genuinely_empty(tmp_path, monkeypatch):
    """Succeeded but wrote nothing is not a repair — re-running will not help."""
    from click.testing import CliRunner

    from cn_market_lake.cli import main as cli_main

    monkeypatch.setattr(
        cli_main,
        "_run_backfill",
        lambda cfg, ds, start, end: {"status": "success", "rows_written": 0},
    )
    res = CliRunner().invoke(
        cli_main.cli,
        ["verify", "--config", str(_cli_lake(tmp_path)), "--dataset", "daily_bars", "--repair"],
    )
    assert res.exit_code == 0
    assert "源在该区间没有数据" in res.output
