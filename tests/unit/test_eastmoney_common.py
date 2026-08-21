import pytest

from cnequity.adapters.eastmoney.clist import (
    clist_rows_to_symbols,
    clist_rows_to_symbols_tolerant,
)
from cnequity.adapters.eastmoney.common import (
    _to_int,
    exchange_from_datacenter,
    symbol_from_clist,
    symbol_from_em,
    symbol_from_secucode,
)


def test_to_int_rejects_fractional_and_out_of_range_values():
    assert _to_int("12.0") == 12
    assert _to_int("12.5") is None
    assert _to_int("1e300") is None
    assert _to_int("inf") is None


def test_to_int_applies_business_bounds():
    assert _to_int(0, minimum=1) is None
    assert _to_int(3, maximum=2) is None


def test_clist_rows_infer_exchange_for_invalid_market_id():
    rows = clist_rows_to_symbols([{"f12": "600519", "f13": float("inf")}])
    assert rows == [("600519.SH", {"f12": "600519", "f13": float("inf")})]


def test_clist_rows_to_symbols_tolerant_skips_reserved_band():
    rows = clist_rows_to_symbols_tolerant(
        [{"f12": "600519", "f13": 1}, {"f12": "810011", "f13": 0}],
        dataset="fund_flow",
    )
    assert rows == [("600519.SH", {"f12": "600519", "f13": 1})]


def test_common_symbol_helpers_infer_legacy_beijing_codes():
    assert symbol_from_clist("830001", 0) == "830001.BJ"
    assert exchange_from_datacenter({"SECURITY_CODE": "830001"}) == "BJ"


@pytest.mark.parametrize("value", ["abc", "0000001", "60051x"])
def test_symbol_helpers_reject_non_numeric_or_overlong_codes(value):
    assert symbol_from_secucode(f"{value}.SZ") is None
    assert symbol_from_em(value, 0) is None
    assert symbol_from_clist(value, 0) is None
