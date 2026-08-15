from datetime import date, datetime, timezone

import polars as pl
import pytest

from cn_market_lake.domain.schemas import (
    FETCHED_AT_DTYPE,
    SchemaValidationError,
    validate_dataframe,
)


def test_validate_empty_returns_typed_empty_frame():
    df = validate_dataframe(pl.DataFrame(), "corporate_actions")
    assert df.is_empty()
    assert "symbol" in df.columns
    assert "ex_date" in df.columns


def test_validate_casts_and_selects_schema_columns():
    raw = pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": ["2024-06-28"],
            "open": [1.0],
            "high": [2.0],
            "low": [0.5],
            "close": [1.5],
            "volume": [1000],
            "amount": [1500.0],
            "source": ["tdx_protocol"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-28T00:00:00+00:00"],
            "extra_col": ["drop-me"],
        }
    )
    out = validate_dataframe(raw, "daily_bars")
    assert "extra_col" not in out.columns
    assert out["trade_date"][0] == date(2024, 6, 28)
    assert out.schema["fetched_at"] == FETCHED_AT_DTYPE
    assert out["fetched_at"][0] == datetime(2024, 6, 28, tzinfo=timezone.utc)


def test_validate_missing_column_raises():
    df = pl.DataFrame({"symbol": ["600519.SH"]})
    with pytest.raises(SchemaValidationError, match="missing columns"):
        validate_dataframe(df, "daily_bars")
