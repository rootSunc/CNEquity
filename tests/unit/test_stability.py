from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from cnequity.diagnostics.stability import evaluate_stability, store_stability_report
from cnequity.orchestrator.manifest import Manifest


def _days(n: int) -> list[date]:
    start = date(2026, 7, 1)
    return [start + timedelta(days=index) for index in range(n)]


def _run(manifest: Manifest, day: date, status: str = "success") -> str:
    run_id = manifest.start_run("daily:core", {"trade_date": day.isoformat()})
    manifest.finish_run(run_id, status)
    return run_id


def test_twenty_consecutive_successes_pass(tmp_path):
    manifest = Manifest(tmp_path / "manifest.db")
    days = _days(20)
    for day in days:
        _run(manifest, day)

    report = evaluate_stability(manifest, days, required_days=20, as_of=days[-1])

    assert report.passed is True
    assert report.consecutive_passed == 20


def test_missing_day_fails_without_shortening_window(tmp_path):
    manifest = Manifest(tmp_path / "manifest.db")
    days = _days(20)
    for day in days:
        if day != days[-3]:
            _run(manifest, day)

    report = evaluate_stability(manifest, days, required_days=20, as_of=days[-1])

    assert report.passed is False
    assert report.days[-3].status == "missing"
    assert report.consecutive_passed == 2


def test_degraded_non_core_passes_but_core_failure_does_not(tmp_path):
    manifest = Manifest(tmp_path / "manifest.db")
    days = _days(2)
    degraded = _run(manifest, days[0], "degraded")
    manifest.record_dataset_result(
        degraded,
        "adj_factors",
        "derive",
        "warning",
        criticality="research",
    )
    failed = _run(manifest, days[1], "degraded")
    manifest.record_dataset_result(
        failed,
        "daily_bars",
        "compact",
        "failed",
        criticality="core",
    )

    report = evaluate_stability(manifest, days, required_days=2, as_of=days[-1])

    assert report.days[0].passed is True
    assert report.days[1].passed is False
    assert report.passed is False


def test_legacy_warning_without_receipts_is_not_evidence(tmp_path):
    manifest = Manifest(tmp_path / "manifest.db")
    day = _days(1)[0]
    _run(manifest, day, "warning")

    report = evaluate_stability(manifest, [day], required_days=1, as_of=day)

    assert report.passed is False
    assert "unproven" in report.days[0].reason


def test_latest_attempt_wins_and_report_is_persisted(tmp_path):
    manifest = Manifest(tmp_path / "manifest.db")
    day = _days(1)[0]
    _run(manifest, day, "failed")
    _run(manifest, day, "success")
    now = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)

    report = evaluate_stability(
        manifest,
        [day],
        required_days=1,
        as_of=day,
        now=now,
    )
    latest, historical = store_stability_report(tmp_path / "meta", report)

    assert report.passed is True
    assert latest.is_file()
    assert historical.is_file()
    assert latest.read_bytes() == historical.read_bytes()


def _group_run(manifest: Manifest, day: date, job: str, status: str, receipts) -> str:
    run_id = manifest.start_run(job, {"trade_date": day.isoformat()})
    for dataset, stage, outcome in receipts:
        manifest.record_dataset_result(run_id, dataset, stage, outcome)
    manifest.finish_run(run_id, status)
    return run_id


def test_a_core_dataset_fails_the_day_from_any_group(tmp_path):
    manifest = Manifest(tmp_path / "manifest.db")
    day = _days(1)[0]
    _group_run(manifest, day, "daily:core", "success", [("daily_bars", "fetch", "success")])
    _group_run(
        manifest,
        day,
        "daily:fundamentals",
        "failed",
        [("financial_statement_items", "fetch", "failed")],
    )

    report = evaluate_stability(manifest, [day], required_days=1, as_of=day)

    assert report.job_name == "core_datasets"
    assert report.days[0].passed is False
    assert "financial_statement_items" in report.days[0].reason


def test_a_non_core_failure_in_the_core_group_does_not(tmp_path):
    manifest = Manifest(tmp_path / "manifest.db")
    day = _days(1)[0]
    _group_run(
        manifest,
        day,
        "daily:core",
        "failed",
        [("daily_bars", "fetch", "success"), ("industry_index", "derive", "failed")],
    )

    report = evaluate_stability(manifest, [day], required_days=1, as_of=day)

    assert report.days[0].passed is True
    assert report.days[0].status == "degraded"


def test_a_later_run_repairs_a_core_failure(tmp_path):
    manifest = Manifest(tmp_path / "manifest.db")
    day = _days(1)[0]
    _group_run(manifest, day, "daily:core", "failed", [("adj_factors", "derive", "failed")])
    _group_run(manifest, day, "daily:stale", "success", [("adj_factors", "derive", "success")])

    report = evaluate_stability(manifest, [day], required_days=1, as_of=day)

    assert report.days[0].passed is True
