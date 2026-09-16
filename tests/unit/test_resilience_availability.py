"""Concentration alone does not decide a routing question.

`build_dependency_report` says which datasets share a failure domain. It is
deliberately a pure function of the registry, so it cannot say whether a domain
is reachable *from here* — and that is the other half of the choice. Measured
on the reference host over 30 days, the EastMoney domain carried 29 datasets
(4 critical) with its worst probe at 0.0%, while `tdx` carried 9 (6 critical)
at 100.0%.
"""

from __future__ import annotations

from cnequity.diagnostics.source_resilience import (
    annotate_measured_availability,
    build_dependency_report,
)


def _slo(*results) -> dict:
    return {"window_days": 30, "results": list(results)}


def _probe(key, availability, *, observations=17, passed=True, vantage="local", target=0.99):
    return {
        "key": key,
        "availability": availability,
        "observations": observations,
        "passed": passed,
        "vantage": vantage,
        "target": target,
    }


def test_measured_availability_lands_on_the_matching_failure_domain():
    payload = build_dependency_report().to_dict()

    out = annotate_measured_availability(payload, _slo(_probe("tdx_protocol", 1.0)))

    tdx = next(r for r in out["blast_radii"] if r["failure_domain"] == "tdx")
    assert tdx["measured_availability"]["availability"] == 1.0
    assert tdx["measured_availability"]["probe"] == "tdx_protocol"


def test_a_domain_takes_its_worst_probe():
    """A feed is down when any endpoint it needs is down."""
    payload = build_dependency_report().to_dict()

    out = annotate_measured_availability(
        payload,
        _slo(
            _probe("eastmoney_push2", 0.588),
            _probe("eastmoney_push2his", 0.0, passed=False),
            _probe("eastmoney_datacenter", 0.588),
        ),
    )

    em = next(r for r in out["blast_radii"] if r["failure_domain"] == "eastmoney")
    assert em["measured_availability"]["availability"] == 0.0
    assert em["measured_availability"]["probe"] == "eastmoney_push2his"


def test_a_probe_with_no_observations_is_not_a_zero():
    """ "Never measured" and "measured at zero" are different answers."""
    payload = build_dependency_report().to_dict()

    out = annotate_measured_availability(
        payload, _slo(_probe("tdx_protocol", None, observations=0))
    )

    tdx = next(r for r in out["blast_radii"] if r["failure_domain"] == "tdx")
    assert "measured_availability" not in tdx
    assert out["measured_availability"]["by_failure_domain"] == {}


def test_the_declared_report_is_not_modified():
    """The pure report stays pure; the join returns a new payload."""
    payload = build_dependency_report().to_dict()

    out = annotate_measured_availability(payload, _slo(_probe("tdx_protocol", 1.0)))

    assert "measured_availability" not in payload
    assert all("measured_availability" not in r for r in payload["blast_radii"])
    assert out is not payload


def test_an_unprobed_domain_is_left_unannotated():
    payload = build_dependency_report().to_dict()

    out = annotate_measured_availability(payload, _slo(_probe("tdx_protocol", 1.0)))

    unprobed = [r for r in out["blast_radii"] if "measured_availability" not in r]
    assert unprobed, "expected domains with no probe of their own"
    assert all(r["failure_domain"] != "tdx" for r in unprobed)
