"""One rule for which audit errors fail a gate (`cnequity.quality.gate`)."""

from __future__ import annotations

from cnequity.quality.gate import audit_verdict, contradictions, coverage_only, is_contradiction

GAP = {"severity": "error", "dataset": "daily_bars", "check": "daily_bars_calendar_missing_day"}
WRONG = {"severity": "error", "dataset": "daily_bars", "check": "duplicate_keys"}
ADVISORY = {"severity": "error", "dataset": "adj_factors", "check": "adj_factor_continuity"}
NON_CORE = {"severity": "error", "dataset": "futures_bars", "check": "futures_exchange_missing"}
WARNING = {"severity": "warning", "dataset": "daily_bars", "check": "duplicate_keys"}


def test_publication_counts_contradictions_only():
    assert coverage_only(GAP)
    assert not is_contradiction(GAP)
    assert not is_contradiction(ADVISORY)
    assert contradictions([GAP, WRONG, ADVISORY, WARNING]) == [WRONG]


def test_a_lake_gate_also_fails_on_a_coverage_gap():
    verdict = audit_verdict([GAP, ADVISORY, WARNING])

    assert not verdict.passed
    assert verdict.errors == [GAP]


def test_core_only_keeps_what_it_leaves_out():
    verdict = audit_verdict([NON_CORE, GAP], core_only=True)

    assert verdict.errors == [GAP]
    assert verdict.outside_scope == [NON_CORE]
    assert audit_verdict([NON_CORE], core_only=True).passed
    assert not audit_verdict([NON_CORE]).passed
