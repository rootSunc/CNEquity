from __future__ import annotations

import datetime as dt
import json

from cnequity.diagnostics import source_health
from cnequity.diagnostics.release_evidence import validate_release_evidence
from cnequity.diagnostics.source_health import HealthReport, ProbeResult
from cnequity.diagnostics.source_slo import (
    build_source_incidents,
    critical_probe_keys,
    evaluate_source_slo,
)

NOW = dt.datetime(2026, 8, 31, 12, tzinfo=dt.timezone.utc)


def _write_reports(root, *, passed=True, generated_at="2026-08-31T10:00:00+00:00"):
    root.mkdir()
    days = []
    day = dt.date(2026, 8, 3)
    while len(days) < 20:
        if day.weekday() < 5:
            days.append(
                {
                    "trade_date": day.isoformat(),
                    "run_id": f"run-{day.isoformat()}",
                    "status": "success" if passed else "failed",
                    "dataset_results": 1,
                    "passed": passed,
                    "reason": "run succeeded" if passed else "core dataset stage failed",
                }
            )
        day += dt.timedelta(days=1)
    (root / "stability-20d.json").write_text(
        json.dumps(
            {
                "generated_at": generated_at,
                "job_name": "daily:core",
                "calendar_days_available": len(days),
                "passed": passed,
                "required_days": 20,
                "consecutive_passed": 20 if passed else 0,
                "days": days,
            }
        )
    )
    latest_status = "ok" if passed else "down"
    (root / "source-slo-30d.json").write_text(
        json.dumps(
            {
                "generated_at": generated_at,
                "passed": passed,
                "window_days": 30,
                "minimum_observations": 10,
                "results": [
                    {
                        "key": key,
                        "vantage": "cn",
                        "critical": True,
                        "observations": 10,
                        "successes": 10 if passed else 0,
                        "availability": 1.0 if passed else 0.0,
                        "target": 0.99,
                        "latest_status": latest_status,
                        "latest_generated_at": generated_at,
                        "fresh": passed,
                        "passed": passed,
                    }
                    for key in sorted(critical_probe_keys())
                ],
                "incidents": {
                    "format": "cnequity.source-incidents",
                    "version": 1,
                    "threshold": 3,
                    "open_incidents": [] if passed else [{"probe": "tdx_protocol"}],
                    "open_count": 0 if passed else 1,
                },
            }
        )
    )


def _write_native_slo_report(root, *, offsets=None):
    if offsets is None:
        offsets = range(31)
    reports = [
        HealthReport(
            vantage="cn",
            generated_at=(NOW - dt.timedelta(days=offset)).isoformat(),
            version="test",
            results=[
                ProbeResult(
                    key=key,
                    label=source_health.PROBES_BY_KEY[key].label,
                    host=source_health.PROBES_BY_KEY[key].host,
                    powers=list(source_health.PROBES_BY_KEY[key].powers),
                    status="ok",
                    latency_ms=10,
                    detail="test",
                )
                for key in sorted(critical_probe_keys())
            ],
        )
        for offset in offsets
    ]
    native = evaluate_source_slo(reports, now=NOW, minimum_observations=10)
    payload = native.to_dict()
    payload["incidents"] = build_source_incidents(reports)
    (root / "source-slo-30d.json").write_text(json.dumps(payload))
    return native


def test_release_evidence_accepts_current_passing_production_reports(tmp_path):
    root = tmp_path / "v0.8.0"
    _write_reports(root)
    assert validate_release_evidence(root, now=NOW) == []


def test_release_evidence_rejects_failed_reports(tmp_path):
    root = tmp_path / "v0.8.0"
    _write_reports(root, passed=False)
    errors = validate_release_evidence(root, now=NOW)
    assert any("passed must be true" in error for error in errors)
    assert any("critical source must pass" in error for error in errors)
    assert any("open source incidents" in error for error in errors)


def test_release_evidence_recomputes_native_stability_and_critical_coverage(tmp_path):
    root = tmp_path / "v0.8.0"
    _write_reports(root)

    stability_path = root / "stability-20d.json"
    stability = json.loads(stability_path.read_text())
    stability["job_name"] = "daily:research"
    stability["consecutive_passed"] = 19
    stability_path.write_text(json.dumps(stability))

    slo_path = root / "source-slo-30d.json"
    slo = json.loads(slo_path.read_text())
    slo["results"].pop()
    slo_path.write_text(json.dumps(slo))

    errors = validate_release_evidence(root, now=NOW)
    assert any("job_name must be 'daily:core'" in error for error in errors)
    assert any("does not match tail count" in error for error in errors)
    assert any("missing critical probes" in error for error in errors)


def test_release_evidence_rejects_missing_or_stale_reports(tmp_path):
    missing = validate_release_evidence(tmp_path / "missing", now=NOW)
    assert len(missing) == 2

    root = tmp_path / "v0.8.0"
    _write_reports(root, generated_at="2026-08-20T10:00:00+00:00")
    stale = validate_release_evidence(root, now=NOW)
    assert sum("older than 7 days" in error for error in stale) == 2


def test_release_evidence_requires_timezone_aware_generated_at(tmp_path):
    root = tmp_path / "v0.8.0"
    _write_reports(root, generated_at="2026-08-31T10:00:00")

    errors = validate_release_evidence(root, now=NOW)

    assert sum("generated_at must include a timezone" in error for error in errors) == 2


def test_release_evidence_rejects_weak_critical_target(tmp_path):
    root = tmp_path / "v0.8.0"
    _write_reports(root)
    path = root / "source-slo-30d.json"
    payload = json.loads(path.read_text())
    payload["results"][0]["target"] = 0
    path.write_text(json.dumps(payload))

    errors = validate_release_evidence(root, now=NOW)

    assert any("critical target must equal native core target" in error for error in errors)


def test_release_evidence_requires_a_30_day_slo_window(tmp_path):
    root = tmp_path / "v0.8.0"
    _write_reports(root)
    path = root / "source-slo-30d.json"
    payload = json.loads(path.read_text())
    payload["window_days"] = 365
    path.write_text(json.dumps(payload))

    errors = validate_release_evidence(root, now=NOW)

    assert any("window_days must equal 30" in error for error in errors)


def test_release_evidence_rejects_observations_beyond_slo_window(tmp_path):
    root = tmp_path / "v0.8.0"
    _write_reports(root)
    path = root / "source-slo-30d.json"
    payload = json.loads(path.read_text())
    payload["results"][0]["observations"] = payload["window_days"] + 2
    payload["results"][0]["successes"] = payload["results"][0]["observations"]
    path.write_text(json.dumps(payload))

    errors = validate_release_evidence(root, now=NOW)

    assert any("observations cannot exceed window_days + 1" in error for error in errors)


def test_release_evidence_accepts_native_closed_30_day_slo_window(tmp_path):
    root = tmp_path / "v0.8.0"
    _write_reports(root)
    native = _write_native_slo_report(root)

    assert {item.observations for item in native.results} == {31}
    assert validate_release_evidence(root, now=NOW) == []


def test_release_evidence_rejects_native_32nd_observation(tmp_path):
    root = tmp_path / "v0.8.0"
    _write_reports(root)
    _write_native_slo_report(root)
    path = root / "source-slo-30d.json"
    payload = json.loads(path.read_text())
    payload["results"][0]["observations"] = 32
    payload["results"][0]["successes"] = 32
    path.write_text(json.dumps(payload))

    errors = validate_release_evidence(root, now=NOW)

    assert any("observations cannot exceed window_days + 1" in error for error in errors)


def test_release_evidence_rejects_unknown_vantage(tmp_path):
    root = tmp_path / "v0.8.0"
    _write_reports(root)
    path = root / "source-slo-30d.json"
    payload = json.loads(path.read_text())
    payload["results"][0]["vantage"] = "unknown"
    path.write_text(json.dumps(payload))

    errors = validate_release_evidence(root, now=NOW)

    assert any("vantage must identify a real probe origin" in error for error in errors)


def test_release_evidence_rejects_stability_with_old_tail_trade_date(tmp_path):
    root = tmp_path / "v0.8.0"
    _write_reports(root)
    path = root / "stability-20d.json"
    payload = json.loads(path.read_text())
    old_days = []
    day = dt.date(2026, 7, 20)
    while len(old_days) < 20:
        if day.weekday() < 5:
            old_days.append(day)
        day += dt.timedelta(days=1)
    for row, old_day in zip(payload["days"], old_days, strict=True):
        row["trade_date"] = old_day.isoformat()
    path.write_text(json.dumps(payload))

    errors = validate_release_evidence(root, now=NOW)

    assert any("latest trade_date" in error and "more than 7 days" in error for error in errors)


def test_release_evidence_rejects_future_stability_trade_date(tmp_path):
    root = tmp_path / "v0.8.0"
    _write_reports(root)
    path = root / "stability-20d.json"
    payload = json.loads(path.read_text())
    payload["days"][-1]["trade_date"] = "2026-09-01"
    path.write_text(json.dumps(payload))

    errors = validate_release_evidence(root, now=NOW)

    assert any("trade_date must not be later than now" in error for error in errors)
