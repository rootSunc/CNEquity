from __future__ import annotations

from datetime import datetime, timedelta, timezone

from cnequity.diagnostics.source_health import HealthReport, ProbeResult
from cnequity.diagnostics.source_resilience import (
    build_dependency_report,
    dependency_fingerprint,
    evaluate_backup_coverage,
    source_failure_domains,
)
from cnequity.diagnostics.source_slo import build_source_incidents
from cnequity.domain.datasets import DatasetSpec


def test_eastmoney_hosts_share_one_failure_domain():
    assert source_failure_domains("eastmoney_push2+eastmoney_datacenter") == ("eastmoney",)


def test_backup_gate_requires_disjoint_source_domain():
    shared = DatasetSpec("x", "L0", primary_source="eastmoney", backup_source="eastmoney_push2")
    independent = DatasetSpec("y", "L0", primary_source="tdx_protocol", backup_source="eastmoney")

    failed = evaluate_backup_coverage({"x": shared}, critical_datasets=["x"])
    passed = evaluate_backup_coverage({"y": independent}, critical_datasets=["y"])

    assert failed.passed is False
    assert failed.issues[0].reason == "backup_shares_failure_domain"
    assert passed.passed is True


def test_registry_report_is_deterministic_except_timestamp():
    first = build_dependency_report(generated_at="2026-08-29T00:00:00+00:00")
    second = build_dependency_report(generated_at="2026-08-30T00:00:00+00:00")

    assert dependency_fingerprint(first) == dependency_fingerprint(second)
    adj = next(item for item in first.datasets if item["dataset"] == "adj_factors")
    assert adj["impact"]["single_source_primary"] is True


def test_trading_status_has_a_backup_in_a_different_failure_domain():
    """This gate was green once before, for the wrong reason.

    `trading_status` declared `tdx_protocol` as primary and `eastmoney` as
    backup, which looked like two domains and was one:
    `tdx_protocol.client.fetch_trading_status` only forwards to
    `fetch_trading_status_eastmoney`. Correcting the primary made the gate fail
    honestly; it passes again now because the exchanges themselves turned out to
    publish both facts this dataset needs — the halt in a zeroed open/high/low
    beside a reference close, the ST designation in 证券简称.
    """
    report = build_dependency_report(generated_at="2026-08-29T00:00:00+00:00")

    assert report.backup_gate.passed is True
    record = next(r for r in report.datasets if r["dataset"] == "trading_status")
    assert record["primary"] == "eastmoney"
    assert record["backup"] == "exchange"
    assert record["backup_coverage"]["independent"] is True
    # Which is the whole point: EastMoney being unreachable must not take the
    # backup with it.
    assert not set(record["backup_coverage"]["primary_domains"]) & set(
        record["backup_coverage"]["backup_domains"]
    )


def _report(at: datetime, status: str) -> HealthReport:
    return HealthReport(
        generated_at=at.isoformat(),
        vantage="cn",
        version="1",
        results=[
            ProbeResult(
                key="sina_adj",
                label="Sina adjustment factor",
                host="sina.com.cn",
                status=status,
                detail=f"status={status}",
                latency_ms=10,
                powers=["adj_factors"],
            ),
        ],
    )


def test_repeated_source_failure_has_stable_dedupe_key_and_success_resets():
    now = datetime(2026, 8, 29, tzinfo=timezone.utc)
    failed = [_report(now + timedelta(hours=i), "down") for i in range(3)]
    first = build_source_incidents(failed)
    second = build_source_incidents([*failed, _report(now + timedelta(hours=3), "down")])

    assert first["open_count"] == 1
    assert first["open_incidents"][0]["dedupe_key"] == second["open_incidents"][0]["dedupe_key"]
    assert (
        build_source_incidents([*failed, _report(now + timedelta(hours=4), "ok")])["open_count"]
        == 0
    )
