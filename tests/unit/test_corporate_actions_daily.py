from datetime import date
from unittest.mock import patch

import polars as pl
import pytest

import cnequity.steps  # noqa: F401
from cnequity.config import Config, FailoverDatasetSpec
from cnequity.run_scope import set_backfill
from cnequity.steps.events import step_corporate_actions
from cnequity.storage.raw_archive import RawArchiveError


def test_corporate_actions_daily_uses_eastmoney(tmp_path):
    cfg = Config(
        data_root=tmp_path / "data", sources={"eastmoney": True}, raw_archive_enabled=False
    )
    em_df = pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "ex_date": [date(2024, 6, 28)],
            "action_type": ["cash_dividend"],
            "cash_dividend": [10.0],
            "bonus_ratio": [0.0],
            "transfer_ratio": [0.0],
            "allotment_ratio": [None],
            "allotment_price": [None],
        }
    )
    with patch(
        "cnequity.steps.events.fetch_corporate_actions_eastmoney",
        side_effect=lambda d, **_kwargs: em_df.with_columns(pl.lit(d).alias("ex_date")),
    ):
        result = step_corporate_actions(cfg, date(2024, 6, 28), "run-1", {})

    assert result["context_updates"]["symbols_to_rebackfill"] == ["600519.SH"]
    staged = list((cfg.staging_root / "corporate_actions").glob("**/*.parquet"))
    assert staged
    df = pl.read_parquet(staged[0])
    assert df["source"][0] == "eastmoney"


def test_corporate_actions_daily_empty_is_ok(tmp_path):
    cfg = Config(
        data_root=tmp_path / "data", sources={"eastmoney": True}, raw_archive_enabled=False
    )
    with patch(
        "cnequity.steps.events.fetch_corporate_actions_eastmoney",
        return_value=pl.DataFrame(),
    ):
        result = step_corporate_actions(cfg, date(2024, 6, 28), "run-1", {})

    assert result["context_updates"]["symbols_to_rebackfill"] == []
    assert result["rows_written"] == 0


def test_clean_corporate_actions_day_captures_daily_tdx_peer(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        sources={"eastmoney": True, "tdx_protocol": True},
        tdx_enabled=True,
        raw_archive_enabled=False,
        failover_enabled=True,
        failover_datasets=[
            FailoverDatasetSpec(
                name="corporate_actions",
                primary="eastmoney",
                backup="tdx_protocol",
                snapshot_cadence="daily",
            )
        ],
    )
    calls = []
    monkeypatch.setattr(
        "cnequity.steps.events.snapshot_corporate_actions_tdx_backup",
        lambda *args, **kwargs: calls.append(kwargs),
    )
    with patch(
        "cnequity.steps.events.fetch_corporate_actions_eastmoney",
        return_value=pl.DataFrame(),
    ):
        result = step_corporate_actions(
            cfg, date(2024, 6, 28), "run-clean-peer", {"symbols": ["600519.SH"]}
        )

    assert result["rows_written"] == 0
    assert calls and calls[0]["symbols"] == ["600519.SH"]
    finding = next(
        item
        for item in result["context_updates"]["audit_findings"]
        if item["check"] == "backup_snapshot_unavailable"
    )
    assert finding["peer_unavailable"] is True
    assert finding["retryable"] is True


def test_corporate_actions_peer_failure_is_warning_not_primary_error(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        sources={"eastmoney": True, "tdx_protocol": True},
        tdx_enabled=True,
        raw_archive_enabled=False,
        failover_enabled=True,
        failover_datasets=[
            FailoverDatasetSpec(
                name="corporate_actions",
                primary="eastmoney",
                backup="tdx_protocol",
                snapshot_cadence="daily",
            )
        ],
    )
    monkeypatch.setattr(
        "cnequity.steps.events.snapshot_corporate_actions_tdx_backup",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("peer down")),
    )
    with patch(
        "cnequity.steps.events.fetch_corporate_actions_eastmoney",
        return_value=pl.DataFrame(),
    ):
        result = step_corporate_actions(
            cfg, date(2024, 6, 28), "run-peer-down", {"symbols": ["600519.SH"]}
        )

    finding = next(
        item
        for item in result["context_updates"]["audit_findings"]
        if item["check"] == "backup_snapshot_unavailable"
    )
    assert finding["severity"] == "warning"
    assert finding["peer_unavailable"] is True
    assert finding["retryable"] is True


def test_corporate_actions_daily_preserves_incremental_findings(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data", sources={"eastmoney": True}, raw_archive_enabled=False
    )
    row = pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "ex_date": [date(2024, 6, 28)],
            "action_type": ["cash_dividend"],
            "cash_dividend": [1.0],
            "bonus_ratio": [0.0],
            "transfer_ratio": [0.0],
            "allotment_ratio": [None],
            "allotment_price": [None],
        }
    )
    finding = {"dataset": "corporate_actions", "check": "coverage_gap"}
    monkeypatch.setattr(
        "cnequity.steps.events.fetch_incremental_daily",
        lambda *args, **kwargs: (row, [finding]),
    )

    result = step_corporate_actions(cfg, date(2024, 6, 28), "run-1", {})

    assert result["context_updates"]["audit_findings"] == [finding]


def test_corporate_actions_backfill_rejects_source_rows_outside_requested_window(
    tmp_path, monkeypatch
):
    cfg = Config(
        data_root=tmp_path / "data", sources={"eastmoney": True}, raw_archive_enabled=False
    )
    set_backfill(cfg, active=True, start=date(2024, 1, 1), end=date(2024, 6, 30))
    rows = pl.DataFrame(
        {
            "symbol": ["600519.SH", "600519.SH", "600519.SH"],
            "ex_date": [date(2023, 12, 31), date(2024, 6, 30), date(2024, 7, 1)],
            "action_type": ["cash_dividend"] * 3,
            "cash_dividend": [1.0] * 3,
            "bonus_ratio": [0.0] * 3,
            "transfer_ratio": [0.0] * 3,
            "allotment_ratio": [None] * 3,
            "allotment_price": [None] * 3,
        },
        schema_overrides={"allotment_ratio": pl.Float64, "allotment_price": pl.Float64},
    )
    monkeypatch.setattr("cnequity.steps.events.load_symbols", lambda _config: ["600519.SH"])
    monkeypatch.setattr("cnequity.steps.events.fetch_corporate_actions", lambda *a, **k: rows)
    with pytest.raises(RuntimeError, match="outside requested window"):
        step_corporate_actions(cfg, date(2024, 6, 30), "run-bounded", {})
    assert not list((cfg.staging_root / "corporate_actions").glob("**/*.parquet"))


def test_corporate_actions_backfill_default_reaches_research_floor(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data", raw_archive_enabled=False)
    set_backfill(cfg, active=True)
    monkeypatch.setattr("cnequity.steps.events.load_symbols", lambda _cfg: ["600849.SH"])
    rows = pl.DataFrame(
        {
            "symbol": ["600849.SH"],
            "ex_date": [date(2005, 8, 19)],
            "action_type": ["cash_dividend"],
            "cash_dividend": [0.08],
            "bonus_ratio": [0.0],
            "transfer_ratio": [0.0],
            "allotment_ratio": [None],
            "allotment_price": [None],
        },
        schema_overrides={"allotment_ratio": pl.Float64, "allotment_price": pl.Float64},
    )
    monkeypatch.setattr("cnequity.steps.events.fetch_corporate_actions", lambda *a, **k: rows)

    result = step_corporate_actions(cfg, date(2026, 8, 21), "run-floor", {})

    assert result["rows_written"] == 1


def test_corporate_actions_captureless_backfill_fails_before_staging(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, active=True, start=date(2024, 1, 1), end=date(2024, 6, 28))
    monkeypatch.setattr("cnequity.steps.events.load_symbols", lambda _cfg: ["600519.SH"])
    monkeypatch.setattr(
        "cnequity.steps.events.fetch_corporate_actions",
        lambda *args, **kwargs: pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "ex_date": [date(2024, 6, 28)],
                "action_type": ["cash_dividend"],
                "cash_dividend": [1.0],
                "bonus_ratio": [0.0],
                "transfer_ratio": [0.0],
                "allotment_ratio": [None],
                "allotment_price": [None],
            }
        ),
    )

    with pytest.raises(RawArchiveError, match="(no exact wire observation|capture is not active)"):
        step_corporate_actions(cfg, date(2024, 6, 28), "run-captureless", {})
    assert not (cfg.staging_root / "corporate_actions").exists()


def test_parse_row_maps_current_eastmoney_columns():
    """Guards against EM column drift (EX_DIVIDEND_DATE/PRETAX_BONUS_RMB/IT_RATIO)."""
    from cnequity.adapters.eastmoney.corporate_actions import _parse_row

    cash = _parse_row(
        {
            "SECUCODE": "605009.SH",
            "SECURITY_CODE": "605009",
            "EX_DIVIDEND_DATE": "2026-07-06 00:00:00",
            "PRETAX_BONUS_RMB": 8.5,
            "IMPL_PLAN_PROFILE": "10派8.50元(含税,扣税后7.65元)",
        }
    )
    # per-share contract: EM "10派8.50元" (8.5 per 10 shares) → 0.85 per share
    assert cash == {
        "symbol": "605009.SH",
        "ex_date": date(2026, 7, 6),
        "action_type": "cash_dividend",
        "cash_dividend": 0.85,
        "bonus_ratio": 0.0,
        "transfer_ratio": 0.0,
        "allotment_ratio": None,
        "allotment_price": None,
    }

    transfer = _parse_row(
        {
            "SECUCODE": "000001.SZ",
            "SECURITY_CODE": "000001",
            "EX_DIVIDEND_DATE": "2026-05-20 00:00:00",
            "IT_RATIO": 4.0,
            "IMPL_PLAN_PROFILE": "10转4.00股",
        }
    )
    # per-share contract: EM "10转4.00股" (4.0 per 10 shares) → 0.4 per share
    assert transfer["action_type"] == "transfer"
    assert transfer["transfer_ratio"] == 0.4
    assert transfer["symbol"] == "000001.SZ"

    # no ex-date → skipped (not yet ex-dividend)
    assert _parse_row({"SECUCODE": "600000.SH", "IMPL_PLAN_PROFILE": "10派1元"}) is None
