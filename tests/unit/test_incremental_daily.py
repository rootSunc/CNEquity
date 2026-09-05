from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.steps import capital as cap
from cnequity.steps import http_common
from cnequity.steps.common import (
    SnapshotBackfillError,
    empty_day_is_expected,
    fetch_incremental_daily,
    incremental_trade_dates,
    is_trading_day,
    list_trading_dates,
)
from cnequity.storage.state import StateStore


def _seed_trading_calendar(cfg: Config, start: date, end: date) -> None:
    rows = []
    d = start
    while d <= end:
        rows.append({"trade_date": d, "is_trading": d.weekday() < 5})
        d = date.fromordinal(d.toordinal() + 1)
    path = cfg.curated_root / "trading_calendar" / "part-merged.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)


def test_incremental_trade_dates_uses_watermark(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _seed_trading_calendar(cfg, date(2024, 6, 24), date(2024, 6, 28))
    StateStore(cfg.meta_root).set_date("fund_flow", date(2024, 6, 25))

    dates = incremental_trade_dates(cfg, "fund_flow", date(2024, 6, 28))
    assert dates == [date(2024, 6, 26), date(2024, 6, 27), date(2024, 6, 28)]


def test_list_trading_dates_skips_weekends_without_calendar(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    dates = list_trading_dates(cfg, date(2024, 6, 28), date(2024, 6, 30))
    assert dates == [date(2024, 6, 28)]


def test_list_trading_dates_merges_nested_staging_calendar_fragments(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    root = cfg.staging_root / "trading_calendar" / "run_id=recovered" / ".fragments"
    root.mkdir(parents=True)
    base = {
        "source": "exchange_calendar",
        "data_version": "v1",
    }
    pl.DataFrame(
        [
            {
                **base,
                "trade_date": date(2024, 6, 24),
                "is_trading": True,
                "fetched_at": datetime(2024, 6, 1, tzinfo=timezone.utc),
            },
            {
                **base,
                "trade_date": date(2024, 6, 26),
                "is_trading": False,
                "fetched_at": datetime(2024, 6, 1, tzinfo=timezone.utc),
            },
        ]
    ).write_parquet(root / "part-0.parquet")
    pl.DataFrame(
        [
            {
                **base,
                "trade_date": date(2024, 6, 25),
                "is_trading": True,
                "fetched_at": datetime(2024, 6, 2, tzinfo=timezone.utc),
            },
            {
                **base,
                "trade_date": date(2024, 6, 26),
                "is_trading": True,
                "fetched_at": datetime(2024, 6, 2, tzinfo=timezone.utc),
            },
        ]
    ).write_parquet(root / "part-1.parquet")

    assert list_trading_dates(cfg, date(2024, 6, 24), date(2024, 6, 26)) == [
        date(2024, 6, 24),
        date(2024, 6, 25),
        date(2024, 6, 26),
    ]


def test_list_trading_dates_rebuilds_when_curated_calendar_is_partial(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    root = cfg.curated_root / "trading_calendar"
    root.mkdir(parents=True)
    # A partially promoted calendar must not make the missing sessions look
    # like holidays and silently narrow a backfill window.
    pl.DataFrame([{"trade_date": date(2024, 6, 26), "is_trading": True}]).write_parquet(
        root / "part-merged.parquet"
    )

    assert list_trading_dates(cfg, date(2024, 6, 24), date(2024, 6, 28)) == [
        date(2024, 6, 24),
        date(2024, 6, 25),
        date(2024, 6, 26),
        date(2024, 6, 27),
        date(2024, 6, 28),
    ]


def test_list_trading_dates_salvages_valid_file_when_curated_calendar_is_corrupt(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    root = cfg.curated_root / "trading_calendar"
    root.mkdir(parents=True)
    pl.DataFrame([{"trade_date": date(2024, 6, 27), "is_trading": True}]).write_parquet(
        root / "valid.parquet"
    )
    (root / "broken.parquet").write_bytes(b"not a parquet file")

    assert list_trading_dates(cfg, date(2024, 6, 27), date(2024, 6, 27)) == [date(2024, 6, 27)]


def test_is_trading_day_prefers_latest_curated_calendar_fragment(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    root = cfg.curated_root / "trading_calendar"
    root.mkdir(parents=True)
    base = {"trade_date": date(2024, 6, 26), "source": "calendar", "data_version": "v1"}
    pl.DataFrame(
        [{**base, "is_trading": False, "fetched_at": datetime(2024, 6, 1, tzinfo=timezone.utc)}]
    ).write_parquet(root / "part-merged.parquet")
    nested = root / ".old-fragments"
    nested.mkdir()
    pl.DataFrame(
        [{**base, "is_trading": True, "fetched_at": datetime(2024, 6, 2, tzinfo=timezone.utc)}]
    ).write_parquet(nested / "part-old.parquet")

    assert is_trading_day(cfg, date(2024, 6, 26)) is True


def test_fetch_incremental_daily_loops_gap_days_for_by_date(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _seed_trading_calendar(cfg, date(2024, 6, 24), date(2024, 6, 28))
    StateStore(cfg.meta_root).set_date("margin_trading", date(2024, 6, 25))
    fetched: list[date] = []

    def _fetch(day: date) -> pl.DataFrame:
        fetched.append(day)
        return pl.DataFrame({"trade_date": [day], "symbol": ["600519.SH"], "value": [1.0]})

    df, findings = fetch_incremental_daily(cfg, "margin_trading", date(2024, 6, 28), _fetch)
    assert fetched == [date(2024, 6, 26), date(2024, 6, 27), date(2024, 6, 28)]
    assert df.height == 3
    assert findings == []


def test_fetch_incremental_daily_rejects_stale_snapshot_date(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    requested = date(2024, 6, 28)

    def _fetch(day: date) -> pl.DataFrame:
        assert day == requested
        return pl.DataFrame(
            {"trade_date": [date(2024, 6, 27)], "symbol": ["600519.SH"], "value": [1.0]}
        )

    with pytest.raises(
        RuntimeError,
        match=r"hot_rank: fetch for 2024-06-28 returned 1 row\(s\) with a different",
    ):
        fetch_incremental_daily(
            cfg,
            "hot_rank",
            requested,
            _fetch,
            date_col="trade_date",
        )


def test_allowed_empty_response_is_reported_for_dense_dataset(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _seed_trading_calendar(cfg, date(2024, 6, 24), date(2024, 6, 28))
    StateStore(cfg.meta_root).set_date("market_breadth", date(2024, 6, 25))

    def _fetch(day: date) -> pl.DataFrame:
        if day == date(2024, 6, 27):
            return pl.DataFrame()
        return pl.DataFrame({"trade_date": [day], "metric_id": ["advance_count"], "value": [1.0]})

    df, findings = fetch_incremental_daily(
        cfg,
        "market_breadth",
        date(2024, 6, 28),
        _fetch,
        allow_empty=True,
    )

    assert df.height == 2
    assert findings[0]["check"] == "session_dense_empty_days"
    assert findings[0]["sample_dates"] == ["2024-06-27"]


def test_run_incremental_fetched_marks_dense_empty_day_retryable(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _seed_trading_calendar(cfg, date(2024, 6, 24), date(2024, 6, 28))
    StateStore(cfg.meta_root).set_date("market_breadth", date(2024, 6, 25))

    def _fetch(day: date) -> pl.DataFrame:
        if day == date(2024, 6, 27):
            return pl.DataFrame()
        return pl.DataFrame({"trade_date": [day], "metric_id": ["advance_count"], "value": [1.0]})

    result = http_common.run_incremental_fetched(
        cfg,
        date(2024, 6, 28),
        "run-dense-gap",
        "market_breadth",
        _fetch,
        source="derived",
        allow_empty=True,
    )

    assert result["status"] == "warning"
    assert result["context_updates"]["audit_findings"][0]["check"] == ("session_dense_empty_days")


def test_market_breadth_backfill_does_not_skip_partial_existing_day(tmp_path, monkeypatch):
    from cnequity.steps.macro_risk import step_market_breadth

    cfg = Config(data_root=tmp_path / "data")
    cfg._backfill = True
    cfg._backfill_start = date(2024, 6, 27)
    cfg._backfill_end = date(2024, 6, 27)
    partial = cfg.curated_root / "market_breadth" / "trade_date=2024"
    partial.mkdir(parents=True)
    pl.DataFrame(
        {
            "trade_date": [date(2024, 6, 27)],
            "metric_id": ["advance_count"],
            "value": [1.0],
        }
    ).write_parquet(partial / "partial.parquet")
    metrics = [
        "advance_count",
        "decline_count",
        "flat_count",
        "limit_up_count",
        "limit_down_count",
        "advance_ratio",
        "total_count",
    ]
    monkeypatch.setattr(
        "cnequity.steps.macro_risk.compute_market_breadth",
        lambda _config, day: pl.DataFrame(
            {
                "trade_date": [day] * len(metrics),
                "metric_id": metrics,
                "value": [1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0],
            }
        ),
    )

    result = step_market_breadth(cfg, date(2024, 6, 27), "run-breadth-retry", {})

    assert result["rows_written"] == len(metrics)
    assert result["days_skipped"] == 0


def test_market_breadth_backfill_does_not_skip_duplicate_metric_day(tmp_path, monkeypatch):
    from cnequity.steps.macro_risk import step_market_breadth

    cfg = Config(data_root=tmp_path / "data")
    cfg._backfill = True
    cfg._backfill_start = date(2024, 6, 27)
    cfg._backfill_end = date(2024, 6, 27)
    metrics = [
        "advance_count",
        "decline_count",
        "flat_count",
        "limit_up_count",
        "limit_down_count",
        "advance_ratio",
        "total_count",
    ]
    part = cfg.curated_root / "market_breadth" / "trade_date=2024"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "trade_date": [date(2024, 6, 27)] * 8,
            "metric_id": metrics + [metrics[0]],
            "value": [1.0] * 8,
        }
    ).write_parquet(part / "duplicate.parquet")
    monkeypatch.setattr(
        "cnequity.steps.macro_risk.compute_market_breadth",
        lambda _config, day: pl.DataFrame(
            {
                "trade_date": [day] * len(metrics),
                "metric_id": metrics,
                "value": [1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0],
            }
        ),
    )

    result = step_market_breadth(cfg, date(2024, 6, 27), "run-breadth-duplicate", {})

    assert result["rows_written"] == len(metrics)
    assert result["days_skipped"] == 0


def test_market_breadth_backfill_retries_zero_count_snapshot(tmp_path, monkeypatch):
    from cnequity.steps.macro_risk import step_market_breadth

    cfg = Config(data_root=tmp_path / "data")
    cfg._backfill = True
    cfg._backfill_start = date(2024, 6, 27)
    cfg._backfill_end = date(2024, 6, 27)
    metrics = [
        "advance_count",
        "decline_count",
        "flat_count",
        "limit_up_count",
        "limit_down_count",
        "advance_ratio",
        "total_count",
    ]
    part = cfg.curated_root / "market_breadth" / "trade_date=2024"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "trade_date": [date(2024, 6, 27)] * len(metrics),
            "metric_id": metrics,
            "value": [0.0] * len(metrics),
        }
    ).write_parquet(part / "zero-snapshot.parquet")
    monkeypatch.setattr(
        "cnequity.steps.macro_risk.compute_market_breadth",
        lambda _config, day: pl.DataFrame(
            {
                "trade_date": [day] * len(metrics),
                "metric_id": metrics,
                "value": [1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0],
            }
        ),
    )

    result = step_market_breadth(cfg, date(2024, 6, 27), "run-breadth-zero", {})

    assert result["rows_written"] == len(metrics)
    assert result["days_skipped"] == 0


def test_market_breadth_daily_rejects_partial_derived_snapshot(tmp_path, monkeypatch):
    from cnequity.steps.macro_risk import step_market_breadth

    cfg = Config(data_root=tmp_path / "data")
    monkeypatch.setattr(
        "cnequity.steps.macro_risk.compute_market_breadth",
        lambda _config, day: pl.DataFrame(
            {"trade_date": [day], "metric_id": ["advance_count"], "value": [1.0]}
        ),
    )

    with pytest.raises(RuntimeError, match="incomplete derived snapshot"):
        step_market_breadth(cfg, date(2024, 6, 28), "run-breadth-partial", {})


def test_fetch_incremental_daily_rejects_rows_from_a_different_trade_date(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _seed_trading_calendar(cfg, date(2024, 6, 28), date(2024, 6, 28))

    with pytest.raises(RuntimeError, match="different or invalid trade_date"):
        fetch_incremental_daily(
            cfg,
            "margin_trading",
            date(2024, 6, 28),
            lambda d: pl.DataFrame(
                {
                    "trade_date": [date(2024, 6, 27)],
                    "symbol": ["600519.SH"],
                    "value": [1.0],
                }
            ),
        )


def test_fetch_incremental_daily_validates_explicit_non_trade_date_column(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _seed_trading_calendar(cfg, date(2024, 6, 28), date(2024, 6, 28))

    with pytest.raises(RuntimeError, match="different or invalid publish_date"):
        fetch_incremental_daily(
            cfg,
            "news_headlines",
            date(2024, 6, 28),
            lambda d: pl.DataFrame(
                {
                    "publish_date": [date(2024, 6, 27)],
                    "news_id": ["n-1"],
                    "title": ["wrong day"],
                }
            ),
            date_col="publish_date",
        )


def test_fetch_incremental_daily_snapshot_only_fetches_run_day(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _seed_trading_calendar(cfg, date(2024, 6, 24), date(2024, 6, 28))
    StateStore(cfg.meta_root).set_date("fund_flow", date(2024, 6, 25))
    fetched: list[date] = []

    def _fetch(day: date) -> pl.DataFrame:
        fetched.append(day)
        return pl.DataFrame({"trade_date": [day], "symbol": ["600519.SH"], "value": [1.0]})

    df, findings = fetch_incremental_daily(cfg, "fund_flow", date(2024, 6, 28), _fetch)
    assert fetched == [date(2024, 6, 28)]
    assert df.height == 1
    assert len(findings) == 1
    assert findings[0]["check"] == "coverage_gap"
    assert findings[0]["gap_dates"] == ["2024-06-26", "2024-06-27"]


def test_trading_status_snapshot_does_not_replay_live_labels_into_gap_days(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _seed_trading_calendar(cfg, date(2024, 6, 24), date(2024, 6, 28))
    StateStore(cfg.meta_root).set_date("trading_status", date(2024, 6, 25))
    fetched: list[date] = []

    def _fetch(day: date) -> pl.DataFrame:
        fetched.append(day)
        return pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [day],
                "is_trading": [True],
                "status": ["normal"],
            }
        )

    df, findings = fetch_incremental_daily(cfg, "trading_status", date(2024, 6, 28), _fetch)

    assert fetched == [date(2024, 6, 28)]
    assert df["trade_date"].to_list() == [date(2024, 6, 28)]
    assert findings[0]["check"] == "coverage_gap"
    assert findings[0]["gap_dates"] == ["2024-06-26", "2024-06-27"]


def test_rolling_snapshot_without_watermark_ignores_legacy_future_state(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _seed_trading_calendar(cfg, date(2024, 6, 24), date(2024, 6, 28))
    StateStore(cfg.meta_root).set_date("share_unlock_schedule", date(2027, 2, 4))
    fetched: list[date] = []

    def _fetch(day: date) -> pl.DataFrame:
        fetched.append(day)
        return pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "unlock_date": [date(2024, 7, 1)],
                "unlock_shares": [1.0],
                "unlock_ratio": [0.01],
                "unlock_type": ["restricted"],
            }
        )

    df, findings = fetch_incremental_daily(cfg, "share_unlock_schedule", date(2024, 6, 28), _fetch)

    assert fetched == [date(2024, 6, 28)]
    assert df["unlock_date"].to_list() == [date(2024, 7, 1)]
    assert findings == []


def test_fetch_incremental_daily_snapshot_rejects_backfill(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    cfg._backfill = True

    with pytest.raises(SnapshotBackfillError, match="fund_flow"):
        fetch_incremental_daily(
            cfg,
            "fund_flow",
            date(2024, 6, 28),
            lambda d: pl.DataFrame(),
        )


def test_fetch_incremental_daily_backfill_rejects_empty_when_not_allowed(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    cfg._backfill = True

    with pytest.raises(RuntimeError, match="margin_trading: no rows returned"):
        fetch_incremental_daily(
            cfg,
            "margin_trading",
            date(2024, 6, 28),
            lambda d: pl.DataFrame(),
            allow_empty=False,
        )


def test_fetch_incremental_daily_backfill_rejects_wrong_trade_date(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    cfg._backfill = True

    with pytest.raises(RuntimeError, match="different or invalid trade_date"):
        fetch_incremental_daily(
            cfg,
            "margin_trading",
            date(2024, 6, 28),
            lambda d: pl.DataFrame(
                {
                    "trade_date": [date(2024, 6, 27)],
                    "symbol": ["600519.SH"],
                    "value": [1.0],
                }
            ),
        )


def test_run_incremental_fetched_rejects_universe_with_no_overlap(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    source = pl.DataFrame(
        {
            "symbol": ["000001.SZ"],
            "trade_date": [date(2024, 6, 28)],
            "value": [1.0],
        }
    )
    monkeypatch.setattr(
        http_common,
        "fetch_incremental_daily",
        lambda *args, **kwargs: (source, []),
    )

    with pytest.raises(RuntimeError, match="none matched the reconciled universe"):
        http_common.run_incremental_fetched(
            cfg,
            date(2024, 6, 28),
            "run-no-overlap",
            "valuation_metrics",
            lambda d: source,
            source="eastmoney",
            universe={"600519.SH"},
        )


def test_step_fund_flow_snapshot_gap_only_fetches_run_day(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data", raw_archive_enabled=False)
    _seed_trading_calendar(cfg, date(2024, 6, 24), date(2024, 6, 28))
    StateStore(cfg.meta_root).set_date("fund_flow", date(2024, 6, 25))
    fetched: list[date] = []

    def fake_fetch(trade_date, **kwargs):
        fetched.append(trade_date)
        return pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [trade_date],
                "main_net_inflow": [1.0],
                "super_large_net_inflow": [0.0],
                "large_net_inflow": [0.0],
                "medium_net_inflow": [0.0],
                "small_net_inflow": [0.0],
            }
        )

    monkeypatch.setattr(cap, "fetch_fund_flow", fake_fetch)
    cfg.staging_root.mkdir(parents=True)
    result = cap.step_fund_flow(cfg, date(2024, 6, 28), "run-gap", {})
    assert fetched == [date(2024, 6, 28)]
    assert result["rows_written"] == 1
    findings = result["context_updates"]["audit_findings"]
    assert findings[0]["check"] == "coverage_gap"


def test_step_fund_flow_single_day_when_caught_up(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data", raw_archive_enabled=False)
    StateStore(cfg.meta_root).set_date("fund_flow", date(2024, 6, 27))
    fetched: list[date] = []

    def fake_fetch(trade_date, **kwargs):
        fetched.append(trade_date)
        return pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [trade_date],
                "main_net_inflow": [1.0],
                "super_large_net_inflow": [0.0],
                "large_net_inflow": [0.0],
                "medium_net_inflow": [0.0],
                "small_net_inflow": [0.0],
            }
        )

    monkeypatch.setattr(cap, "fetch_fund_flow", fake_fetch)
    cfg.staging_root.mkdir(parents=True)
    result = cap.step_fund_flow(cfg, date(2024, 6, 28), "run-1", {})
    assert fetched == [date(2024, 6, 28)]
    assert result["rows_written"] == 1
    assert "context_updates" not in result


def _announcement_row(day: date) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "announcement_id": [f"A-{day.isoformat()}"],
            "symbol": ["600519.SH"],
            "title": ["公告"],
            "announce_date": [day],
            "category": ["其他"],
            "url": ["/a.pdf"],
        }
    )


def test_one_unreadable_day_no_longer_discards_the_rest_of_the_window(tmp_path):
    """A 30-day reconciliation tail must not go blind over one bad day.

    `announcement_index` re-reads the last 30 days on every run, so a day the
    source refuses used to fail the step — and with it every other day in that
    window — until the bad day rolled out of the tail.
    """
    cfg = Config(data_root=tmp_path / "data")
    broken = date(2024, 6, 26)
    StateStore(cfg.meta_root).set_date("announcement_index", date(2024, 6, 25))

    def _fetch(day: date) -> pl.DataFrame:
        if day == broken:
            raise RuntimeError("CNINFO announcement pagination failed for szse page 1")
        return _announcement_row(day)

    df, findings = fetch_incremental_daily(
        cfg, "announcement_index", date(2024, 6, 28), _fetch, date_col="announce_date"
    )

    assert broken not in df["announce_date"].to_list()
    assert df.height >= 1
    failed = next(f for f in findings if f["check"] == "fetch_failed_days")
    assert failed["severity"] == "error"
    assert failed["sample_dates"] == [broken.isoformat()]
    assert "pagination failed" in failed["errors"][0]


def test_a_failed_day_publishes_the_others_and_reports_degraded(tmp_path):
    cfg = Config(data_root=tmp_path / "data", raw_archive_enabled=False)
    StateStore(cfg.meta_root).set_date("announcement_index", date(2024, 6, 25))

    def _fetch(day: date) -> pl.DataFrame:
        if day == date(2024, 6, 26):
            raise RuntimeError("source refused the day")
        return _announcement_row(day)

    result = http_common.run_incremental_fetched(
        cfg,
        date(2024, 6, 28),
        "run-partial-window",
        "announcement_index",
        _fetch,
        source="cninfo",
        date_col="announce_date",
    )

    # `degraded`, not `warning`: the complete days publish, because holding
    # them back is what turned one bad day into a blind dataset.
    assert result["status"] == "degraded"
    assert result["rows_written"] >= 1
    checks = {f["check"] for f in result["context_updates"]["audit_findings"]}
    assert "fetch_failed_days" in checks


def test_a_window_that_fails_entirely_still_fails_loud(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    StateStore(cfg.meta_root).set_date("announcement_index", date(2024, 6, 25))

    def _fetch(day: date) -> pl.DataFrame:
        raise RuntimeError("cninfo down")

    with pytest.raises(RuntimeError, match="cninfo down"):
        fetch_incremental_daily(
            cfg, "announcement_index", date(2024, 6, 28), _fetch, date_col="announce_date"
        )


def test_a_dataset_without_a_reconciliation_tail_keeps_failing_loud(tmp_path):
    """Nothing would come back for the hole, so the window stays all-or-nothing."""
    cfg = Config(data_root=tmp_path / "data")
    _seed_trading_calendar(cfg, date(2024, 6, 24), date(2024, 6, 28))
    StateStore(cfg.meta_root).set_date("margin_trading", date(2024, 6, 25))

    def _fetch(day: date) -> pl.DataFrame:
        if day == date(2024, 6, 27):
            raise RuntimeError("source refused the day")
        return pl.DataFrame({"trade_date": [day], "symbol": ["600519.SH"], "value": [1.0]})

    with pytest.raises(RuntimeError, match="source refused the day"):
        fetch_incremental_daily(cfg, "margin_trading", date(2024, 6, 28), _fetch)


def test_a_closed_day_with_no_disclosures_is_not_a_failed_fetch(tmp_path):
    """A weekend is walked (companies do file on Saturdays) but may be empty.

    `announcement_index` steps through calendar days, so its window contains
    days the exchanges never opened. Market-wide zero disclosures on such a day
    is the ordinary case — failing the step there took `capital` down with it
    and blocked every dependent step for the day.
    """
    cfg = Config(data_root=tmp_path / "data")
    _seed_trading_calendar(cfg, date(2024, 6, 24), date(2024, 7, 1))
    StateStore(cfg.meta_root).set_date("announcement_index", date(2024, 6, 28))
    saturday, sunday, monday = date(2024, 6, 29), date(2024, 6, 30), date(2024, 7, 1)
    fetched: list[date] = []

    def _fetch(day: date) -> pl.DataFrame:
        fetched.append(day)
        return pl.DataFrame() if day == sunday else _announcement_row(day)

    df, findings = fetch_incremental_daily(
        cfg, "announcement_index", monday, _fetch, date_col="announce_date"
    )

    # The tail is walked day by day, weekend included.
    assert fetched[-3:] == [saturday, sunday, monday]
    published = df["announce_date"].to_list()
    assert saturday in published and monday in published
    assert sunday not in published
    # Not a coverage gap and not a dense-empty day: nothing was missed.
    assert findings == []


def test_a_session_with_no_disclosures_still_fails_loud(tmp_path):
    """The empty-day gate stays where it catches a broken source."""
    cfg = Config(data_root=tmp_path / "data")
    _seed_trading_calendar(cfg, date(2024, 6, 24), date(2024, 6, 28))
    StateStore(cfg.meta_root).set_date("announcement_index", date(2024, 6, 26))

    def _fetch(day: date) -> pl.DataFrame:
        return pl.DataFrame() if day == date(2024, 6, 27) else _announcement_row(day)

    with pytest.raises(RuntimeError, match="no rows returned for 2024-06-27"):
        fetch_incremental_daily(
            cfg, "announcement_index", date(2024, 6, 28), _fetch, date_col="announce_date"
        )


def test_a_session_scoped_dataset_never_tolerates_an_empty_day(tmp_path):
    """The tolerance is declared per dataset, not inherited by every feed."""
    cfg = Config(data_root=tmp_path / "data")
    _seed_trading_calendar(cfg, date(2024, 6, 24), date(2024, 6, 28))
    StateStore(cfg.meta_root).set_date("margin_trading", date(2024, 6, 27))
    assert not empty_day_is_expected(cfg, "margin_trading", date(2024, 6, 30))

    def _fetch(day: date) -> pl.DataFrame:
        return pl.DataFrame()

    with pytest.raises(RuntimeError, match="margin_trading: no rows returned"):
        fetch_incremental_daily(cfg, "margin_trading", date(2024, 6, 28), _fetch)


def test_a_live_page_that_comes_back_empty_still_fails_loud(tmp_path):
    """`flash_news_wire` is calendar-scoped too, but it is one live page.

    Zero items on that page says something about the page, not about the
    calendar, so the tolerance above must not reach it — an empty success would
    leave the dataset unregistered in curated and quietly fail lake health.
    """
    cfg = Config(data_root=tmp_path / "data")
    _seed_trading_calendar(cfg, date(2024, 6, 24), date(2024, 7, 1))
    sunday = date(2024, 6, 30)
    assert not empty_day_is_expected(cfg, "flash_news_wire", sunday)

    with pytest.raises(RuntimeError, match="flash_news_wire: no rows returned"):
        fetch_incremental_daily(
            cfg,
            "flash_news_wire",
            sunday,
            lambda day: pl.DataFrame(),
            date_col="publish_date",
        )
