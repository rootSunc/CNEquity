"""Stray directories under the curated layer must not go unnoticed."""

from datetime import date, timedelta

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.quality.audit import _unregistered_curated_dirs
from cn_market_lake.quality.dataset_checks import (
    check_mixed_partition_granularity,
    check_partition_fragmentation,
)


def _mkdataset(cfg: Config, name: str) -> None:
    part = cfg.curated_root / name / "trade_date=2026-07-21"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["600519.SH"]}).write_parquet(part / "part-merged.parquet")


def test_clean_curated_layer_has_no_findings(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _mkdataset(cfg, "daily_bars")
    _mkdataset(cfg, "corporate_actions")

    assert _unregistered_curated_dirs(cfg) == []


def test_missing_curated_root_is_not_a_finding(tmp_path):
    assert _unregistered_curated_dirs(Config(data_root=tmp_path / "data")) == []


def test_flags_a_backup_directory_left_inside_curated(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _mkdataset(cfg, "corporate_actions")
    _mkdataset(cfg, "corporate_actions.bak.20260709T122646Z")

    findings = _unregistered_curated_dirs(cfg)

    assert len(findings) == 1
    assert findings[0]["check"] == "unregistered_curated_dir"
    assert findings[0]["severity"] == "warning"
    assert findings[0]["stray_dirs"] == ["corporate_actions.bak.20260709T122646Z"]


def test_lists_every_stray_dir_and_counts_them(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _mkdataset(cfg, "daily_bars")
    for name in ("old_bars", "scratch", "daily_bars.old"):
        _mkdataset(cfg, name)

    finding = _unregistered_curated_dirs(cfg)[0]

    assert finding["stray_count"] == 3
    assert set(finding["stray_dirs"]) == {"old_bars", "scratch", "daily_bars.old"}


# --- partition fragmentation ------------------------------------------------


def _write_days(cfg, dataset, col, dates, rows_per_day=1):
    for d in dates:
        part = cfg.curated_root / dataset / f"{col}={d.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"symbol": ["A"] * rows_per_day, col: [d] * rows_per_day}).write_parquet(
            part / "part-merged.parquet"
        )


def test_flags_a_dataset_that_is_almost_all_parquet_footer(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    days = [date(2024, 1, 1) + timedelta(days=i) for i in range(60)]
    _write_days(cfg, "trading_calendar", "trade_date", days, rows_per_day=1)

    finding = check_partition_fragmentation(
        "trading_calendar", "trade_date", cfg.curated_root / "trading_calendar"
    )

    assert finding is not None
    assert finding["check"] == "partition_fragmentation"
    assert finding["partitions"] == 60
    assert finding["rows_per_partition"] == 1.0
    assert "cml repartition trading_calendar" in finding["message"]


def test_a_well_filled_dataset_is_not_flagged(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    days = [date(2024, 1, 1) + timedelta(days=i) for i in range(60)]
    _write_days(cfg, "daily_bars", "trade_date", days, rows_per_day=100)

    assert (
        check_partition_fragmentation("daily_bars", "trade_date", cfg.curated_root / "daily_bars")
        is None
    )


def test_too_few_partitions_to_judge(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    days = [date(2024, 1, 1) + timedelta(days=i) for i in range(5)]
    _write_days(cfg, "trading_calendar", "trade_date", days, rows_per_day=1)

    assert (
        check_partition_fragmentation(
            "trading_calendar", "trade_date", cfg.curated_root / "trading_calendar"
        )
        is None
    )


# --- mixed partition granularity --------------------------------------------


def _calendar_part(root, value: str, d: date, *, fetched_at: str = "2026-07-01T00:00:00+00:00"):
    part = root / f"trade_date={value}"
    part.mkdir(parents=True, exist_ok=True)
    (
        pl.DataFrame(
            {
                "trade_date": [d],
                "is_trading": [True],
                "source": ["seed"],
                "data_version": ["v1"],
                "fetched_at": [fetched_at],
            }
        )
        .with_columns(pl.col("fetched_at").str.to_datetime(time_unit="us", time_zone="UTC"))
        .write_parquet(part / "part-merged.parquet")
    )


def test_mixed_day_and_year_partitions_are_an_error(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    root = cfg.curated_root / "trading_calendar"
    day = date(2024, 6, 3)
    _calendar_part(root, "2024", day)
    _calendar_part(root, day.isoformat(), day, fetched_at="2026-01-01T00:00:00+00:00")

    finding = check_mixed_partition_granularity("trading_calendar", "trade_date", root)

    assert finding is not None
    assert finding["check"] == "mixed_partition_granularity"
    assert finding["severity"] == "error"
    assert finding["configured_granularity"] == "year"
    assert finding["stale_partitions"] == 1
    assert finding["pk_duplicate_rows"] == 1
    assert "cml repartition trading_calendar" in finding["message"]


def test_year_only_layout_matching_registry_is_clean(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    root = cfg.curated_root / "trading_calendar"
    _calendar_part(root, "2024", date(2024, 6, 3))
    _calendar_part(root, "2025", date(2025, 1, 6))

    assert check_mixed_partition_granularity("trading_calendar", "trade_date", root) is None


def test_day_granularity_dataset_is_not_flagged_for_day_dirs(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    root = cfg.curated_root / "daily_bars"
    day = date(2024, 6, 3)
    part = root / f"trade_date={day.isoformat()}"
    part.mkdir(parents=True)
    pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [day]}).write_parquet(
        part / "part-merged.parquet"
    )

    assert check_mixed_partition_granularity("daily_bars", "trade_date", root) is None
