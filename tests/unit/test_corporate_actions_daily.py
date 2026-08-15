from datetime import date
from unittest.mock import patch

import polars as pl

import cn_market_lake.steps  # noqa: F401
from cn_market_lake.config import Config
from cn_market_lake.steps.events import step_corporate_actions


def test_corporate_actions_daily_uses_eastmoney(tmp_path):
    cfg = Config(data_root=tmp_path / "data", sources={"eastmoney": True})
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
        "cn_market_lake.steps.events.fetch_corporate_actions_eastmoney",
        return_value=em_df,
    ):
        result = step_corporate_actions(cfg, date(2024, 6, 28), "run-1", {})

    assert result["context_updates"]["symbols_to_rebackfill"] == ["600519.SH"]
    staged = list((cfg.staging_root / "corporate_actions").glob("**/*.parquet"))
    assert staged
    df = pl.read_parquet(staged[0])
    assert df["source"][0] == "eastmoney"


def test_corporate_actions_daily_empty_is_ok(tmp_path):
    cfg = Config(data_root=tmp_path / "data", sources={"eastmoney": True})
    with patch(
        "cn_market_lake.steps.events.fetch_corporate_actions_eastmoney",
        return_value=pl.DataFrame(),
    ):
        result = step_corporate_actions(cfg, date(2024, 6, 28), "run-1", {})

    assert result["context_updates"]["symbols_to_rebackfill"] == []
    assert result["rows_written"] == 0


def test_parse_row_maps_current_eastmoney_columns():
    """Guards against EM column drift (EX_DIVIDEND_DATE/PRETAX_BONUS_RMB/IT_RATIO)."""
    from cn_market_lake.adapters.eastmoney.corporate_actions import _parse_row

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
