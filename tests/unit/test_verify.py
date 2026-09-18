"""Coverage verification.

`cne audit` asks whether the data that landed is correct; this asks whether the
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

from dataclasses import replace
from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.domain.datasets import DATASETS
from cnequity.quality.verify import last_contiguous_dense_date, verify_dataset, verify_lake

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
        if dataset == "market_breadth":
            metrics = (
                "advance_count",
                "decline_count",
                "flat_count",
                "limit_up_count",
                "limit_down_count",
                "advance_ratio",
                "total_count",
            )
            pl.DataFrame(
                {
                    "trade_date": [d] * len(metrics),
                    "metric_id": list(metrics),
                    "value": [1.0] * len(metrics),
                }
            ).write_parquet(part / "part-0.parquet")
        elif dataset == "industry_index":
            pl.DataFrame(
                {
                    "trade_date": [d, d],
                    "industry_code": ["2403", "2403"],
                    "level": ["L2", "L2"],
                    "weighting": ["equal", "amount"],
                    "n_members": [1, 1],
                    "n_priced": [1, 1],
                    "n_excluded": [0, 0],
                }
            ).write_parquet(part / "part-0.parquet")
        else:
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


def test_disabled_optional_dataset_with_old_rows_is_not_reported(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    part = cfg.curated_root / "trade_ticks" / "trade_date=2026-07-01"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [date(2026, 7, 1)]}).write_parquet(
        part / "part-0.parquet"
    )

    assert verify_lake(cfg, anchor=ANCHOR, datasets=["trade_ticks"]) == []


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


def test_daily_bars_placeholder_only_partition_is_not_covered(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    sessions = [date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5)]
    _calendar(cfg, sessions)
    for day, volume in ((sessions[0], 100), (sessions[1], 0), (sessions[2], 100)):
        part = cfg.curated_root / "daily_bars" / f"trade_date={day.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {"symbol": ["600519.SH"], "trade_date": [day], "volume": [volume]}
        ).write_parquet(part / "part-0.parquet")

    gaps = verify_dataset(cfg, DATASETS["daily_bars"], anchor=sessions[-1], watermark=sessions[-1])

    interior = [gap for gap in gaps if gap.kind == "interior"]
    assert len(interior) == 1
    assert interior[0].sample == (sessions[1],)
    assert last_contiguous_dense_date(cfg, DATASETS["daily_bars"]) == sessions[0]


def test_dense_watermark_can_start_at_operational_baseline(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    sessions = [date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5)]
    _calendar(cfg, sessions)
    _write_days(cfg, "daily_bars", [sessions[0], sessions[2]])

    assert last_contiguous_dense_date(cfg, DATASETS["daily_bars"]) == sessions[0]
    assert last_contiguous_dense_date(cfg, DATASETS["daily_bars"], start=sessions[2]) == sessions[2]


def test_empty_day_partition_is_not_covered_for_dense_non_bar_dataset(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    sessions = [date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5)]
    _calendar(cfg, sessions)
    _write_days(cfg, "adj_factors", [sessions[0], sessions[-1]], layer="derived")
    empty = cfg.derived_root / "adj_factors" / f"trade_date={sessions[1].isoformat()}"
    empty.mkdir(parents=True)
    pl.DataFrame(schema={"symbol": pl.String, "trade_date": pl.Date}).write_parquet(
        empty / "part-empty.parquet"
    )

    gaps = verify_dataset(
        cfg,
        DATASETS["adj_factors"],
        anchor=sessions[-1],
        watermark=sessions[-1],
    )

    interior = [gap for gap in gaps if gap.kind == "interior"]
    assert len(interior) == 1
    assert interior[0].sample == (sessions[1],)


def test_nonempty_partition_survives_a_corrupt_sibling_file(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    sessions = [date(2026, 8, 3), date(2026, 8, 4)]
    _calendar(cfg, sessions)
    _write_days(cfg, "adj_factors", sessions, layer="derived")
    (cfg.derived_root / "adj_factors" / f"trade_date={sessions[0].isoformat()}").joinpath(
        "part-broken.parquet"
    ).write_bytes(b"not a parquet file")

    assert last_contiguous_dense_date(cfg, DATASETS["adj_factors"]) == sessions[-1]


def test_verify_reports_unreadable_dense_dataset_instead_of_crashing(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    root = cfg.curated_root / "daily_bars" / "trade_date=2026-08-03"
    root.mkdir(parents=True)
    (root / "broken.parquet").write_bytes(b"not a parquet file")

    gaps = verify_dataset(cfg, DATASETS["daily_bars"], anchor=ANCHOR, watermark=None)

    assert len(gaps) == 1
    assert gaps[0].kind == "unreadable"
    assert gaps[0].repairable is False


def test_market_breadth_interior_hole_is_a_fault(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    sessions = [date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5)]
    _calendar(cfg, sessions)
    _write_days(
        cfg,
        "market_breadth",
        [sessions[0], sessions[-1]],
    )

    gaps = verify_dataset(
        cfg,
        DATASETS["market_breadth"],
        anchor=sessions[-1],
        watermark=sessions[-1],
    )

    interior = [gap for gap in gaps if gap.kind == "interior"]
    assert len(interior) == 1
    assert interior[0].sample == (sessions[1],)


def test_partial_market_breadth_day_does_not_advance_dense_watermark(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    sessions = [date(2026, 8, 3), date(2026, 8, 4)]
    _calendar(cfg, sessions)
    _write_days(cfg, "market_breadth", [sessions[0]])
    part = cfg.derived_root / "market_breadth" / f"trade_date={sessions[1].isoformat()}"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "trade_date": [sessions[1]],
            "metric_id": ["advance_count"],
            "value": [1.0],
        }
    ).write_parquet(part / "partial.parquet")

    assert last_contiguous_dense_date(cfg, DATASETS["market_breadth"]) == sessions[0]


def test_industry_index_interior_hole_is_a_fault(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    sessions = [date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5)]
    _calendar(cfg, sessions)
    _write_days(
        cfg,
        "industry_index",
        [sessions[0], sessions[-1]],
        layer="derived",
    )

    gaps = verify_dataset(
        cfg,
        DATASETS["industry_index"],
        anchor=sessions[-1],
        watermark=sessions[-1],
    )

    interior = [gap for gap in gaps if gap.kind == "interior"]
    assert len(interior) == 1
    assert interior[0].sample == (sessions[1],)


def test_sparse_by_date_feed_is_not_treated_as_session_dense(tmp_path):
    """By-date querying is not a promise that an event exists every session."""
    cfg = Config(data_root=tmp_path / "lake")
    sessions = [date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5), date(2026, 8, 6)]
    _calendar(cfg, sessions)
    sparse = replace(
        DATASETS["daily_bars"],
        name="sparse_events",
        coverage_mode="sparse",
    )
    _write_days(cfg, sparse.name, [sessions[0], sessions[-1]])

    gaps = verify_dataset(cfg, sparse, anchor=sessions[-1], watermark=sessions[-1])

    assert [gap for gap in gaps if gap.kind == "interior"] == []


def test_dense_coarse_partition_scans_date_column_for_interior_holes(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    sessions = [date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5), date(2026, 8, 6)]
    _calendar(cfg, sessions)
    part = cfg.curated_root / "index_bars" / "trade_date=2026"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["000300.SH"] * 3,
            "trade_date": [sessions[0], sessions[1], sessions[-1]],
        }
    ).write_parquet(part / "part-0.parquet")

    gaps = verify_dataset(
        cfg,
        DATASETS["index_bars"],
        anchor=sessions[-1],
        watermark=sessions[-1],
    )

    interior = [gap for gap in gaps if gap.kind == "interior"]
    assert len(interior) == 1
    assert interior[0].sample == (date(2026, 8, 5),)


def test_dense_day_partitions_include_root_legacy_files_in_mixed_layout(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    sessions = [date(2026, 8, 3), date(2026, 8, 4)]
    _calendar(cfg, sessions)
    _write_days(cfg, "daily_bars", [sessions[0]])
    pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [sessions[1]]}).write_parquet(
        cfg.curated_root / "daily_bars" / "part-legacy.parquet"
    )

    assert last_contiguous_dense_date(cfg, DATASETS["daily_bars"]) == sessions[-1]


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
    assert gap.repair_command("my.toml") == "cne backfill daily_bars --config my.toml"


def test_repair_command_routes_derived_datasets_to_derive(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    cfg.derived_root.mkdir(parents=True, exist_ok=True)

    adj = verify_dataset(cfg, DATASETS["adj_factors"], anchor=ANCHOR, watermark=None)[0]
    industry = verify_dataset(cfg, DATASETS["industry_index"], anchor=ANCHOR, watermark=None)[0]

    assert adj.repair_command("my.toml") == "cne derive adj_factors --config my.toml"
    assert industry.repair_command("my.toml") == "cne derive industry_index --config my.toml"


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
    from cnequity.domain.datasets import is_stale

    retired = date(2024, 8, 16)
    assert is_stale("northbound_flows", retired, ANCHOR) is False
    assert is_stale("northbound_flows", date(2024, 1, 5), ANCHOR) is True
    # A live dataset is unaffected.
    assert is_stale("daily_bars", date(2026, 6, 1), ANCHOR) is True


# --- the CLI repair loop -----------------------------------------------------


def _cli_lake(tmp_path):
    """A config + lake whose daily_bars is missing one session."""
    from cnequity.config import load_config
    from cnequity.config.bootstrap import path_for_toml

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

    from cnequity.cli.main import cli

    res = CliRunner().invoke(
        cli, ["verify", "--config", str(_cli_lake(tmp_path)), "--dataset", "daily_bars"]
    )
    assert res.exit_code == 1, "a gap must be scriptable as a failure"
    assert "2026-08-05" in res.output
    assert "cne backfill daily_bars" in res.output


def test_cli_repair_does_not_claim_success_when_the_step_failed(tmp_path, monkeypatch):
    """Regression: the engine records a failed step and returns status="failed"
    rather than raising, so an exception-only check printed a traceback and then
    said 全部修复完成 immediately under it."""
    from click.testing import CliRunner

    from cnequity.cli import quality_cmds
    from cnequity.cli.main import cli

    monkeypatch.setattr(
        quality_cmds,
        "_run_backfill",
        lambda cfg, ds, start, end: {"status": "failed", "rows_written": 0},
    )
    res = CliRunner().invoke(
        cli,
        ["verify", "--config", str(_cli_lake(tmp_path)), "--dataset", "daily_bars", "--repair"],
    )
    assert res.exit_code == 1
    assert "status=failed" in res.output
    assert "全部修复完成" not in res.output


def test_cli_repair_says_so_when_the_window_is_genuinely_empty(tmp_path, monkeypatch):
    """Succeeded but wrote nothing is not a repair — re-running will not help."""
    from click.testing import CliRunner

    from cnequity.cli import quality_cmds
    from cnequity.cli.main import cli

    monkeypatch.setattr(
        quality_cmds,
        "_run_backfill",
        lambda cfg, ds, start, end: {"status": "success", "rows_written": 0},
    )
    res = CliRunner().invoke(
        cli,
        ["verify", "--config", str(_cli_lake(tmp_path)), "--dataset", "daily_bars", "--repair"],
    )
    assert res.exit_code == 0
    assert "源在该区间没有数据" in res.output


# --- demo lakes ---------------------------------------------------------------
# `cne init --profile demo|sample` builds a handful of symbols and two or three
# datasets on purpose. Measured against the whole registry, the first health
# check a new user runs reported 35 gaps and exited 1.


def test_verify_on_a_demo_lake_checks_only_what_the_lake_holds(tmp_path):
    from click.testing import CliRunner

    from cnequity.cli.main import cli

    lake = tmp_path / "lake"
    part = lake / "curated" / "daily_bars" / "trade_date=2026-09-18"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {"symbol": ["600519.SH"], "trade_date": [date(2026, 9, 18)], "close": [1.0]}
    ).write_parquet(part / "part-0.parquet")
    cfg = tmp_path / "cnequity.demo.toml"
    cfg.write_text(
        f'[data]\nroot = "{lake.as_posix()}"\nprofile = "demo"\n',
        encoding="utf-8",
    )

    result = CliRunner().invoke(cli, ["verify", "--config", str(cfg)])
    assert "demo 湖（demo）" in result.output
    assert "daily_bars" in result.output
    # The forty datasets it never ingested are the profile, not a gap.
    assert "financial_statement_items" not in result.output


def test_verify_on_a_sample_lake_does_not_offer_to_repair_synthetic_rows(tmp_path):
    """Its rows are dated by the generator, so it is stale the moment it exists.

    The offered repair would fetch real bars into a lake whose every row says
    `source=mock` — the one thing this profile exists to prevent.
    """
    from click.testing import CliRunner

    from cnequity.cli.main import cli

    lake = tmp_path / "lake"
    part = lake / "curated" / "daily_bars" / "trade_date=2024-06-28"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {"symbol": ["600519.SH"], "trade_date": [date(2024, 6, 28)], "close": [1.0]}
    ).write_parquet(part / "part-0.parquet")
    cfg = tmp_path / "cnequity.demo.toml"
    cfg.write_text(
        f'[data]\nroot = "{lake.as_posix()}"\nprofile = "sample"\n',
        encoding="utf-8",
    )

    result = CliRunner().invoke(cli, ["verify", "--config", str(cfg)])
    assert result.exit_code == 0, result.output
    assert "不对 1 个数据集判新鲜度" in result.output
    assert "cne backfill daily_bars" not in result.output
