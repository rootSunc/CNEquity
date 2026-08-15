"""EastMoney datacenter report→columns contract inventory (offline)."""

from __future__ import annotations

import pytest

from cn_market_lake.adapters.eastmoney.datacenter import (
    EastMoneyDatacenterError,
    fetch_datacenter,
)
from cn_market_lake.adapters.eastmoney.datacenter_contracts import (
    datacenter_contracts,
    required_datacenter_contracts,
)
from cn_market_lake.adapters.eastmoney.share_unlock import _UNLOCK_COLUMNS, _UNLOCK_REPORT

# Floor: every fetch_datacenter call site must appear here. Add new reports
# when a new adapter lands — the live probe iterates the same list.
_EXPECTED_REQUIRED_REPORTS = frozenset(
    {
        "RPT_SHAREBONUS_DET",
        "RPT_LIFT_STAGE",
        "RPTA_WEB_RZRQ_GGMX",
        "RPT_MUTUAL_HOLDSTOCKNORTH_STA",
        "RPT_DAILYBILLBOARD_DETAILS",
        "RPT_BLOCKTRADE_STA",
        "RPT_MAIN_ORGHOLD",
        "RPT_WEB_RESPREDICT",
        "RPT_PUBLIC_BS_APPOIN",
        "RPT_INDEX_CONSTITUENT",
        "RPT_BOARD_CONSTITUENT",
        "RPTA_WEB_TREASURYYIELD",
        "RPT_IMP_INTRESTRATEN",
        "RPTA_WEB_RATE",
        "RPT_LICO_FN_CPD",
        "RPT_DMSK_FN_BALANCE",
        "RPT_DMSK_FN_INCOME",
        "RPT_DMSK_FN_CASHFLOW",
    }
)


class FakeClient:
    def __init__(self, responses: list[Exception | dict]):
        self.responses = responses
        self.calls = 0

    def get(self, url: str, **kwargs):
        if self.calls >= len(self.responses):
            raise RuntimeError("unexpected call")
        item = self.responses[self.calls]
        self.calls += 1
        if isinstance(item, Exception):
            raise item

        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return item

        return Resp()


def test_registry_has_nonempty_report_and_columns():
    contracts = datacenter_contracts()
    assert contracts
    for c in contracts:
        assert c.name.strip()
        assert c.report.strip()
        assert c.column_set()


def test_required_reports_match_known_call_sites():
    required = required_datacenter_contracts()
    reports = {c.report for c in required}
    assert _EXPECTED_REQUIRED_REPORTS <= reports
    assert "RPT_ECONOMICCALENDAR" not in reports
    retired = [c for c in datacenter_contracts() if not c.required]
    assert any(c.report == "RPT_ECONOMICCALENDAR" for c in retired)


def test_no_duplicate_required_name_or_report_columns_pair():
    required = required_datacenter_contracts()
    names = [c.name for c in required]
    assert len(names) == len(set(names))
    pairs = [(c.report, c.columns) for c in required]
    assert len(pairs) == len(set(pairs))


def test_schema_rejection_names_report_in_error():
    client = FakeClient([{"success": False, "message": "EX_DIV_DATE列不存在", "code": 9501}])
    with pytest.raises(
        EastMoneyDatacenterError,
        match=r"RPT_SHAREBONUS_DET rejected schema: EX_DIV_DATE列不存在 \(code=9501\)",
    ):
        fetch_datacenter(
            client,
            "RPT_SHAREBONUS_DET",
            "EX_DIV_DATE",
            max_retries=1,
            retry_backoff_seconds=0,
        )


def test_share_unlock_contract_columns_parse():
    """Guards against EM column drift on RPT_LIFT_STAGE (same spirit as CA)."""
    cols = set(_UNLOCK_COLUMNS.split(","))
    assert _UNLOCK_REPORT == "RPT_LIFT_STAGE"
    assert {
        "SECURITY_CODE",
        "FREE_DATE",
        "ABLE_FREE_SHARES",
        "FREE_RATIO",
        "FREE_SHARES_TYPE",
        "CURRENT_FREE_SHARES",
    } <= cols
    row = {c: ("600519" if c == "SECURITY_CODE" else "2026-08-01") for c in cols}
    row["ABLE_FREE_SHARES"] = 1e6
    row["FREE_RATIO"] = 0.05
    row["CURRENT_FREE_SHARES"] = 5e5
    # Registry must track the same CSV the adapter uses.
    contract = next(c for c in datacenter_contracts() if c.name == "share_unlock")
    assert contract.columns == _UNLOCK_COLUMNS
    assert contract.column_set() == frozenset(cols)
    assert all(k in row for k in contract.column_set())
