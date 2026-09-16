from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.domain.schemas import (
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


def test_validate_rejects_unparseable_required_market_value():
    raw = pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": ["2024-06-28"],
            "open": ["bad"],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
            "volume": [1000],
            "amount": [10500.0],
            "source": ["eastmoney"],
            "data_version": ["v2"],
            "fetched_at": ["2024-06-28T00:00:00+00:00"],
        }
    )
    with pytest.raises(SchemaValidationError, match="required columns.*open"):
        validate_dataframe(raw, "daily_bars")


def test_validate_rejects_impossible_ohlc():
    raw = pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 28)],
            "open": [10.0],
            "high": [9.0],
            "low": [8.0],
            "close": [8.5],
            "volume": [1000],
            "amount": [8500.0],
            "source": ["eastmoney"],
            "data_version": ["v2"],
            "fetched_at": [datetime(2024, 6, 28, tzinfo=timezone.utc)],
        }
    )
    with pytest.raises(SchemaValidationError, match="numeric market-data invariants"):
        validate_dataframe(raw, "daily_bars")


def test_validate_allows_zero_volume_carried_forward_ohlc():
    raw = pl.DataFrame(
        {
            "symbol": ["519116.SH"],
            "trade_date": [date(2026, 3, 12)],
            "bar_time": [datetime(2026, 3, 12, 15, 0)],
            "frequency": ["1m"],
            "open": [140.61],
            "high": [140.61],
            "low": [140.61],
            "close": [141.19],
            "volume": [0],
            "amount": [0.0],
            "source": ["tdx_protocol"],
            "data_version": ["v1"],
            "fetched_at": [datetime(2026, 3, 12, tzinfo=timezone.utc)],
        }
    )

    out = validate_dataframe(raw, "minute_bars")

    assert out.height == 1


def test_validate_rejects_non_finite_optional_numeric_values():
    raw = pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 28)],
            "main_net_inflow": [float("nan")],
            "super_large_net_inflow": [None],
            "large_net_inflow": [None],
            "medium_net_inflow": [None],
            "small_net_inflow": [None],
            "source": ["eastmoney"],
            "data_version": ["v1"],
            "fetched_at": [datetime(2024, 6, 28, tzinfo=timezone.utc)],
        }
    )
    with pytest.raises(SchemaValidationError, match="non-finite"):
        validate_dataframe(raw, "fund_flow")


def test_validate_rejects_blank_required_provenance():
    raw = pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 28)],
            "main_net_inflow": [1.0],
            "super_large_net_inflow": [0.0],
            "large_net_inflow": [0.0],
            "medium_net_inflow": [0.0],
            "small_net_inflow": [0.0],
            "source": ["  "],
            "data_version": ["v1"],
            "fetched_at": [datetime(2024, 6, 28, tzinfo=timezone.utc)],
        }
    )
    with pytest.raises(SchemaValidationError, match="required string columns.*source"):
        validate_dataframe(raw, "fund_flow")


@pytest.mark.parametrize(
    ("dataset", "row", "missing"),
    [
        (
            "trading_calendar",
            {
                "trade_date": date(2024, 6, 28),
                "is_trading": None,
                "source": "seed",
                "data_version": "v1",
                "fetched_at": datetime(2024, 6, 28, tzinfo=timezone.utc),
            },
            "is_trading",
        ),
        (
            "trading_status",
            {
                "symbol": "600519.SH",
                "trade_date": date(2024, 6, 28),
                "is_trading": True,
                "status": None,
                # Nullable by design: a derived bar-gap row has no ST evidence.
                "risk_warning": None,
                "source": "eastmoney",
                "data_version": "v1",
                "fetched_at": datetime(2024, 6, 28, tzinfo=timezone.utc),
            },
            "status",
        ),
    ],
)
def test_validate_rejects_null_core_semantic_fields(dataset, row, missing):
    with pytest.raises(SchemaValidationError, match=f"required columns contain null.*{missing}"):
        validate_dataframe(pl.DataFrame([row]), dataset)


def test_validate_requires_trade_tick_time_and_direction():
    row = {
        "symbol": "600519.SH",
        "trade_date": date(2024, 6, 28),
        "tick_seq": 0,
        "trade_time": None,
        "price": 100.0,
        "volume": 100,
        "direction": None,
        "source": "tdx_protocol",
        "data_version": "v1",
        "fetched_at": datetime(2024, 6, 28, tzinfo=timezone.utc),
    }
    with pytest.raises(SchemaValidationError, match="required columns contain null"):
        validate_dataframe(pl.DataFrame([row]), "trade_ticks")


def test_validate_wraps_engine_error_for_invalid_boolean_cast():
    row = {
        "trade_date": date(2024, 6, 28),
        "is_trading": "unknown",
        "source": "seed",
        "data_version": "v1",
        "fetched_at": datetime(2024, 6, 28, tzinfo=timezone.utc),
    }
    with pytest.raises(SchemaValidationError, match="values cannot be cast") as exc_info:
        validate_dataframe(pl.DataFrame([row]), "trading_calendar")
    assert "is_trading" in str(exc_info.value)


def _fund_action(**kwargs):
    from cnequity.domain.schemas import with_provenance

    row = {
        "symbol": "159327.SZ",
        "ex_date": date(2026, 7, 20),
        "action_type": "cash_dividend",
        "cash_dividend": 0.1,
        "bonus_ratio": 0.0,
        "transfer_ratio": 0.0,
        "allotment_ratio": 0.0,
        "allotment_price": 0.0,
        **kwargs,
    }
    return with_provenance(pl.DataFrame([row]), "issuer", "v2")


def test_legacy_action_has_neutral_unit_multiplier():
    assert validate_dataframe(_fund_action(), "corporate_actions")["split_factor"][0] == 1.0


@pytest.mark.parametrize("factor", [None, 0.0, -1.0, float("inf"), 1.0])
def test_unit_split_requires_positive_non_neutral_factor(factor):
    frame = _fund_action(action_type="unit_split", cash_dividend=0.0, split_factor=factor)
    with pytest.raises(SchemaValidationError, match="split"):
        validate_dataframe(frame, "corporate_actions")


def test_unit_split_does_not_masquerade_as_bonus():
    with pytest.raises(SchemaValidationError, match="split"):
        validate_dataframe(_fund_action(split_factor=3.0), "corporate_actions")
    frame = validate_dataframe(
        _fund_action(action_type="unit_split", cash_dividend=0.0, split_factor=3.0),
        "corporate_actions",
    )
    assert frame["bonus_ratio"][0] == 0 and frame["transfer_ratio"][0] == 0
    assert frame["split_factor"][0] == 3


def test_unit_split_does_not_double_count_a_stock_distribution():
    frame = _fund_action(
        action_type="unit_split", split_factor=3.0, cash_dividend=0.0, bonus_ratio=2.0
    )
    with pytest.raises(SchemaValidationError, match="stock distribution"):
        validate_dataframe(frame, "corporate_actions")
