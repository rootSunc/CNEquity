"""Offline tests for limit_prices (Tushare stk_limit)."""

from __future__ import annotations

from datetime import date

import httpx
import polars as pl

from cnequity.adapters.tushare import limit_prices as lp
from cnequity.config import Config
from cnequity.domain.schemas import validate_dataframe, with_provenance

FIELDS = lp._FIELDS.split(",")


def test_normalize_maps_legacy_beijing_codes_and_drops_incomplete_rows():
    frame = lp.normalize(
        FIELDS,
        [
            ["20240603", "430017.BJ", 11.0, 7.0],
            ["20240603", "600519.SH", 1800.0, 1500.0],
            ["20240603", "600000.SH", None, 9.0],
        ],
    )
    assert frame.select("symbol", "up_limit").rows() == [("920017.BJ", 11.0), ("600519.SH", 1800.0)]
    assert frame["trade_date"][0] == date(2024, 6, 3)
    validate_dataframe(with_provenance(frame, source="tushare", data_version="v1"), "limit_prices")


def test_the_step_sends_nothing_without_a_token(tmp_path, monkeypatch):
    from cnequity.steps.reference import step_limit_prices

    def boom(*_a, **_k):  # pragma: no cover - must not be called
        raise AssertionError("no request without a token")

    monkeypatch.setattr(httpx.Client, "post", boom)
    result = step_limit_prices(Config(data_root=tmp_path), date(2026, 11, 4), "run-1", {})
    assert result["status"] == "skipped"


def test_fund_flow_measure_converts_units_and_keeps_its_own_columns():
    from cnequity.adapters.tushare import fund_flow_measure as fm

    names = fm._FIELDS.split(",")
    row = ["430017.BJ", "20240603", *[1.0] * (len(names) - 2)]
    frame = fm.normalize(names, [row])
    assert frame["symbol"][0] == "920017.BJ"
    assert frame["buy_small_volume"][0] == 100.0
    assert frame["net_amount"][0] == 10_000.0
    validate_dataframe(
        with_provenance(frame, source="tushare", data_version="v1"), "fund_flow_tushare"
    )


def test_exchange_reference_rows_carry_a_reference_price_and_nothing_else_may():
    import pytest

    from cnequity.domain.schemas import SchemaValidationError

    row = {
        "symbol": "600080.SH",
        "ex_date": date(2006, 6, 14),
        "action_type": "exchange_reference",
        "cash_dividend": 0.0,
        "bonus_ratio": 0.0,
        "transfer_ratio": 0.0,
        "allotment_ratio": None,
        "allotment_price": None,
        "reference_price": 3.63,
    }
    validate_dataframe(
        with_provenance(
            pl.DataFrame(
                [row],
                schema_overrides={"allotment_ratio": pl.Float64, "allotment_price": pl.Float64},
            ),
            source="tushare",
            data_version="v1",
        ),
        "corporate_actions",
    )
    with pytest.raises(SchemaValidationError):
        validate_dataframe(
            with_provenance(
                pl.DataFrame(
                    [{**row, "action_type": "cash_dividend"}],
                    schema_overrides={"allotment_ratio": pl.Float64, "allotment_price": pl.Float64},
                ),
                source="tushare",
                data_version="v1",
            ),
            "corporate_actions",
        )
