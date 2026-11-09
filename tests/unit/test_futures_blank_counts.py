"""A blank volume or open-interest cell is a malformed cell, never a zero.

Every exchange file and Sina feed prints ``0`` for no trade or no position, so
the adapters keep a blank count as None and the derivative quarantine sets the
row aside with a warning. Read as 0, a blank on a traded contract would have
nulled its prices (``traded`` is ``volume > 0``) and stored a fake idle row.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from cnequity.adapters.futures_exchange import cffex, czce, dce, gfex, shfe
from cnequity.adapters.futures_exchange.common import FuturesPayloadError
from cnequity.adapters.sina.dce_futures import _history_rows
from cnequity.adapters.sina.futures_minute import parse_minutes
from cnequity.steps.derivatives import _quarantine

DAY = date(2026, 9, 24)
FIXTURES = Path(__file__).parents[1] / "fixtures" / "futures"
_IF2610 = "IF2610,4495,4495,4426,25746,3441381.894,52944,2522,"


def _cffex(text: str):
    return cffex.parse_daily_csv(text.encode("gbk"), DAY)


def _cffex_fixture() -> str:
    return (FIXTURES / "cffex_20260924.csv").read_bytes().decode("gbk")


def test_cffex_keeps_a_blank_oi_change_as_null():
    text = _cffex_fixture()
    assert _IF2610 in text
    day = _cffex(text.replace(_IF2610, "IF2610,4495,4495,4426,25746,3441381.894,52944,,", 1))

    row = day.futures.filter(pl.col("exchange_code") == "IF2610").row(0, named=True)
    assert row["oi_change"] is None
    assert row["volume"] == 25746


def test_cffex_refuses_a_traded_contract_whose_volume_is_blank():
    text = _cffex_fixture().replace(_IF2610, "IF2610,4495,4495,4426,,3441381.894,52944,2522,", 1)

    with pytest.raises(FuturesPayloadError, match="小计"):
        _cffex(text)


BLANK_BARS = [
    pytest.param(
        lambda v: gfex._bar(
            {"volumn": v, "openInterest": v, "diffI": v, "clearPrice": "100"}, "a2611", DAY
        ),
        id="gfex",
    ),
    pytest.param(
        lambda v: dce._bar(
            {"volumn": v, "openInterest": v, "diffI": v, "clearPrice": "100"}, "m2611", DAY
        ),
        id="dce",
    ),
    pytest.param(
        lambda v: czce._bar(
            {"code": "SR611", "volume": v, "open_interest": v, "oi_change": v, "settle": "100"},
            DAY,
        ),
        id="czce",
    ),
    pytest.param(
        lambda v: shfe._bar(
            {"VOLUME": v, "OPENINTEREST": v, "OPENINTERESTCHG": v, "SETTLEMENTPRICE": "100"},
            exchange="SHF",
            trade_date=DAY,
        ),
        id="shfe",
    ),
]


@pytest.mark.parametrize("bar", BLANK_BARS)
def test_a_blank_count_stays_null_and_a_printed_zero_stays_zero(bar):
    blank, zero = bar(""), bar("0")

    assert (blank["volume"], blank["open_interest"], blank["oi_change"]) == (None, None, None)
    assert (zero["volume"], zero["open_interest"], zero["oi_change"]) == (0, 0, 0)
    assert blank["close"] is None and zero["close"] is None


def test_sina_minutes_keep_a_blank_count_as_null():
    frame = parse_minutes(
        [
            {"d": "2026-09-22 14:59:00", "c": "110690", "v": "", "p": ""},
            {"d": "2026-09-22 15:00:00", "c": "110750", "v": "0", "p": "160082"},
        ],
        symbol="CU2611.SHF",
        sessions=[date(2026, 9, 22)],
    )

    assert frame["volume"].to_list() == [None, 0]
    assert frame["open_interest"].to_list() == [None, 160082]


def test_sina_dce_history_keeps_a_blank_count_as_null():
    rows = _history_rows(
        "M2611",
        [
            {"d": "2026-09-24", "c": "3005", "s": "3001", "v": "", "p": ""},
            {"d": "2026-09-25", "c": "3005", "s": "3001", "v": "0", "p": "7"},
        ],
    )

    assert [(r["volume"], r["open_interest"]) for r in rows.values()] == [(None, None), (0, 7)]


def test_a_row_with_a_missing_count_is_quarantined_and_the_session_kept():
    frame = pl.DataFrame(
        {
            "symbol": ["A2611.DCE", "M2611.DCE", "Y2611.DCE"],
            "open": [None, None, None],
            "high": [None, None, None],
            "low": [None, None, None],
            "close": [None, None, None],
            "settle": [100.0, 200.0, 300.0],
            "pre_settle": [100.0, 200.0, 300.0],
            "volume": [None, 0, 0],
            "open_interest": [10, None, 30],
            "amount": [None, None, None],
        },
        schema_overrides={c: pl.Float64 for c in ("open", "high", "low", "close", "amount")},
    )
    findings: list[dict] = []

    kept = _quarantine(frame, "futures_bars", "DCE", DAY, findings)

    assert kept["symbol"].to_list() == ["Y2611.DCE"]
    assert [f["check"] for f in findings] == ["futures_row_rejected"]
