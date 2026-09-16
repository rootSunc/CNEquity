"""Availability is a property of (source, vantage), not of the source.

Measured over 30 days: from a mainland egress every source clears 99%; from an
overseas egress EastMoney measured 0-58.8% — not degraded, simply not served
there — while tdx, the SSE quote host, ths and pboc sat at 100%. One flat 99%
target made the gate unpassable from overseas, and a gate that cannot pass
stops being read.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from cnequity.diagnostics.source_health import HealthReport, ProbeResult, ProbeStatus
from cnequity.diagnostics.source_slo import (
    evaluate_source_slo,
    targets_for_vantage,
    vantage_class,
)

NOW = datetime(2026, 9, 15, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        ("cn", "cn"),
        ("cn-sh", "cn"),
        ("cn_aliyun", "cn"),
        ("overseas", "overseas"),
        ("overseas-eu", "overseas"),
        ("OVERSEAS_AWS", "overseas"),
        # The city is never the point.
        ("frankfurt", "unknown"),
        ("local", "unknown"),
        ("", "unknown"),
        (None, "unknown"),
    ],
)
def test_vantage_class_reads_the_prefix_not_a_place_name(label, expected):
    assert vantage_class(label) == expected


def test_an_undeclared_vantage_takes_the_strict_targets():
    """A gate must not hand out a discount to a vantage nobody declared."""
    assert targets_for_vantage("local") == targets_for_vantage("cn")
    assert targets_for_vantage("overseas") < targets_for_vantage("cn")


def _history(vantage: str, key: str, *, ok: int, down: int) -> list[HealthReport]:
    reports = []
    for index in range(ok + down):
        status = ProbeStatus.OK.value if index < ok else ProbeStatus.DOWN.value
        reports.append(
            HealthReport(
                generated_at=(NOW - timedelta(days=index)).isoformat(),
                vantage=vantage,
                version="1",
                results=[
                    ProbeResult(
                        key=key,
                        label=key,
                        host="example.invalid",
                        powers=["daily_bars"],
                        status=status,
                        latency_ms=10,
                        detail="",
                    )
                ],
            )
        )
    return reports


def test_the_same_numbers_pass_overseas_and_fail_from_the_mainland():
    """94% is a healthy overseas baostock and a broken mainland one."""
    history = _history("overseas-eu", "baostock", ok=16, down=1)
    overseas = evaluate_source_slo(history, now=NOW, minimum_observations=10)

    mainland = evaluate_source_slo(
        _history("cn-sh", "baostock", ok=16, down=1), now=NOW, minimum_observations=10
    )

    assert overseas.results[0].passed is True
    assert overseas.results[0].target == 0.85
    assert mainland.results[0].passed is False
    assert mainland.results[0].target == 0.99


def test_a_source_that_is_simply_not_served_there_still_fails():
    """Lowering the bar must not turn unreachable into acceptable."""
    report = evaluate_source_slo(
        _history("overseas-eu", "eastmoney_push2", ok=10, down=7),
        now=NOW,
        minimum_observations=10,
    )

    assert report.results[0].availability < 0.6
    assert report.results[0].passed is False


def test_an_explicit_target_pair_overrides_every_vantage():
    """Release evidence pins its own numbers; they must not drift with a label."""
    report = evaluate_source_slo(
        _history("overseas-eu", "baostock", ok=16, down=1),
        now=NOW,
        minimum_observations=10,
        core_target=0.99,
        other_target=0.95,
    )

    assert report.results[0].target == 0.99
    assert report.results[0].passed is False


def test_an_unreachable_source_is_reported_but_not_gated():
    """A source the network cannot reach is absent, not degraded.

    Holding the gate open on one means it never closes; disabling the gate for
    the whole vantage would lose the regression it exists to catch.
    """
    history = _history("overseas-eu", "eastmoney_push2", ok=0, down=17)

    gated = evaluate_source_slo(history, now=NOW, minimum_observations=10)
    declared = evaluate_source_slo(
        history, now=NOW, minimum_observations=10, unreachable={"eastmoney_push2"}
    )

    assert gated.passed is False
    # Still measured and still visible — just not a verdict on this deployment.
    assert declared.not_applicable[0].key == "eastmoney_push2"
    assert declared.not_applicable[0].availability == 0.0
    assert all(r.key != "eastmoney_push2" for r in declared.results)


def test_declaring_one_source_unreachable_does_not_excuse_the_others():
    history = _history("overseas-eu", "eastmoney_push2", ok=0, down=17)
    history += _history("overseas-eu", "tdx_protocol", ok=8, down=9)

    report = evaluate_source_slo(
        history, now=NOW, minimum_observations=10, unreachable={"eastmoney_push2"}
    )

    assert report.passed is False, "a working source that regressed must still fail"
    tdx = next(r for r in report.results if r.key == "tdx_protocol")
    assert tdx.passed is False
