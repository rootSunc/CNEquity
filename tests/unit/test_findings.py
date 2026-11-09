"""Audit findings have one shape and one severity scale (`domain.findings`)."""

from __future__ import annotations

import pytest

from cnequity.derive.adj_factors import _factor_finding
from cnequity.domain.findings import SEVERITIES, FindingError, checked_findings, finding


def test_finding_refuses_what_the_gate_cannot_judge():
    built = finding(dataset="daily_bars", check="x", severity="warning", message="m", rows=3)
    assert built == {
        "dataset": "daily_bars",
        "check": "x",
        "severity": "warning",
        "message": "m",
        "rows": 3,
    }
    for bad in (
        {"severity": "critical", "check": "x", "message": "m"},
        {"severity": "error", "check": "", "message": "m"},
        {"severity": "error", "check": "x", "message": ""},
    ):
        with pytest.raises(FindingError):
            finding(dataset="daily_bars", **bad)
    assert SEVERITIES == ("error", "warning", "info")


def test_the_audit_boundary_raises_on_a_bad_scale_and_names_a_silent_finding():
    with pytest.raises(FindingError, match="severity"):
        checked_findings([{"dataset": "d", "check": "x", "severity": "fatal", "message": "m"}])
    with pytest.raises(FindingError, match="check name"):
        checked_findings([{"dataset": "d", "severity": "info", "message": "m"}])
    (filled,) = checked_findings([{"dataset": "d", "check": "x", "severity": "info"}])
    assert filled["message"] == "x"


def test_factor_findings_say_what_happened():
    """They had no message at all and printed as blank audit lines."""
    kept = _factor_finding(
        "stock_event_factor",
        {"symbol": "600000.SH", "trade_date": "2024-06-03", "reason": "ambiguous event reference"},
    )
    assert kept["severity"] == "warning"
    assert (
        kept["message"]
        == "600000.SH 2024-06-03: ambiguous event reference; vendor factor left as is"
    )
    fixed = _factor_finding(
        "fund_share_factor", {"symbol": "510300.SH", "trade_date": "2024-06-03", "correction": 2.0}
    )
    assert fixed["severity"] == "info" and fixed["correction"] == 2.0
    assert fixed["message"] == "510300.SH 2024-06-03: vendor step corrected by x2"
    confirmed = _factor_finding("stock_event_factor", {"confirmed": "vendor step matches"})
    assert confirmed["message"] == "vendor step matches"
