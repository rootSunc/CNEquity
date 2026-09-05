from datetime import datetime, timedelta, timezone

from cnequity.diagnostics import source_health
from cnequity.diagnostics.source_health import HealthReport, ProbeResult
from cnequity.diagnostics.source_slo import (
    critical_probe_keys,
    evaluate_source_slo,
    load_health_history,
    store_health_report,
)


def _report(when: datetime, status: str = "ok", *, vantage: str = "cn") -> HealthReport:
    return HealthReport(
        vantage=vantage,
        generated_at=when.isoformat(),
        version="test",
        results=[
            ProbeResult(
                key="tdx_protocol",
                label="TDX",
                host="example.test",
                powers=["daily_bars"],
                status=status,
                latency_ms=10,
                detail="test",
            )
        ],
    )


def _full_report(when: datetime, status: str = "ok", *, vantage: str = "cn") -> HealthReport:
    return HealthReport(
        vantage=vantage,
        generated_at=when.isoformat(),
        version="test",
        results=[
            ProbeResult(
                key=key,
                label=source_health.PROBES_BY_KEY[key].label,
                host=source_health.PROBES_BY_KEY[key].host,
                powers=list(source_health.PROBES_BY_KEY[key].powers),
                status=status,
                latency_ms=10,
                detail="test",
            )
            for key in sorted(critical_probe_keys())
        ],
    )


def test_store_keeps_latest_and_immutable_history(tmp_path):
    now = datetime(2026, 8, 29, 10, tzinfo=timezone.utc)
    latest, historical = store_health_report(tmp_path, _report(now))
    assert latest.exists() and historical.exists()
    assert historical.parent.name == "cn"
    loaded = load_health_history(tmp_path)
    assert len(loaded) == 1
    assert loaded[0].generated_at == now.isoformat()


def test_slo_requires_enough_fresh_observations():
    now = datetime(2026, 8, 29, 10, tzinfo=timezone.utc)
    reports = [_full_report(now - timedelta(days=offset)) for offset in range(10)]
    result = evaluate_source_slo(reports, now=now, minimum_observations=10)
    assert result.passed
    tdx = next(item for item in result.results if item.key == "tdx_protocol")
    assert tdx.availability == 1.0
    assert tdx.critical


def test_slo_missing_critical_probe_fails_closed_per_vantage():
    now = datetime(2026, 8, 29, 10, tzinfo=timezone.utc)
    reports = [_report(now - timedelta(days=offset), vantage="cn") for offset in range(10)]
    result = evaluate_source_slo(reports, now=now, minimum_observations=10)

    assert not result.passed
    missing = {
        item.key for item in result.results if item.vantage == "cn" and item.observations == 0
    }
    assert missing == critical_probe_keys() - {"tdx_protocol"}


def test_slo_empty_report_vantage_fails_closed():
    now = datetime(2026, 8, 29, 10, tzinfo=timezone.utc)
    empty = HealthReport(vantage="overseas", generated_at=now.isoformat(), version="test")

    result = evaluate_source_slo([empty], now=now, minimum_observations=1)

    assert not result.passed
    missing = {
        item.key for item in result.results if item.vantage == "overseas" and item.observations == 0
    }
    assert missing == critical_probe_keys()


def test_slo_all_skipped_report_vantage_fails_closed():
    now = datetime(2026, 8, 29, 10, tzinfo=timezone.utc)
    skipped = _full_report(now, status="skipped", vantage="overseas")

    result = evaluate_source_slo([skipped], now=now, minimum_observations=1)

    assert not result.passed
    missing = {
        item.key for item in result.results if item.vantage == "overseas" and item.observations == 0
    }
    assert missing == critical_probe_keys()


def test_critical_slo_fails_on_availability_or_stale_latest():
    now = datetime(2026, 8, 29, 10, tzinfo=timezone.utc)
    low = [_report(now - timedelta(days=i), "down" if i == 0 else "ok") for i in range(10)]
    assert not evaluate_source_slo(low, now=now, minimum_observations=10).passed

    stale = [_report(now - timedelta(days=3 + i)) for i in range(10)]
    report = evaluate_source_slo(stale, now=now, minimum_observations=10)
    assert not report.passed
    assert not report.results[0].fresh


def test_skipped_samples_do_not_depress_availability():
    now = datetime(2026, 8, 29, 10, tzinfo=timezone.utc)
    reports = [_report(now, "ok"), _report(now - timedelta(hours=1), "skipped")]
    item = evaluate_source_slo(reports, now=now, minimum_observations=1).results[0]
    assert item.observations == 1
    assert item.availability == 1.0


def test_slo_counts_at_most_one_observation_per_utc_day():
    day = datetime(2026, 8, 28, 12, tzinfo=timezone.utc)
    reports = [
        _report(day, status="down"),
        _report(day + timedelta(hours=1), status="ok"),
    ]

    item = evaluate_source_slo(
        reports,
        now=day + timedelta(hours=2),
        minimum_observations=2,
    ).results[0]

    assert item.observations == 1
    assert item.successes == 1
    assert item.latest_status == "ok"
    assert item.passed is False
