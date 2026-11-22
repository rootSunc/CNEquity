import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from cnequity.diagnostics.metrics import (
    check_offline_benchmark,
    persist_offline_benchmark,
    run_offline_benchmark,
)
from cnequity.orchestrator.manifest import Manifest


def test_record_stage_metrics_merges_concurrent_manifest_instances_atomically(tmp_path):
    """Two stage writers must retain both stages and sum their requests."""
    db = tmp_path / "meta" / "manifest.db"
    first = Manifest(db)
    second = Manifest(db)
    run_id = first.start_run("metrics")
    barrier = Barrier(2)

    def _record(manifest, stage, requests):
        barrier.wait()
        manifest.record_stage_metrics(run_id, stage, 0.01, {"requests": requests})

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(_record, first, "stage_a", 1),
            pool.submit(_record, second, "stage_b", 2),
        ]
        for future in futures:
            future.result()

    metrics = first.get_run_metadata(run_id)["metrics"]
    assert set(metrics["stages"]) == {"stage_a", "stage_b"}
    assert metrics["requests"] == 3


def test_record_performance_metrics_and_stage_metrics_merge_concurrently(tmp_path):
    """Different metadata writers retain each other's top-level keys."""
    db = tmp_path / "meta" / "manifest.db"
    manifests = [Manifest(db) for _ in range(4)]
    run_id = manifests[0].start_run("metrics", {"unknown": {"keep": True}})
    barrier = Barrier(4)

    def _record(index):
        barrier.wait()
        if index % 2:
            manifests[index].record_performance_metrics(
                run_id, f"source_{index}", {"requests": index}
            )
        else:
            manifests[index].record_stage_metrics(
                run_id, f"stage_{index}", 0.01, {"requests": index}
            )

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(_record, index) for index in range(4)]
        for future in futures:
            future.result()

    metadata = manifests[0].get_run_metadata(run_id)
    assert set(metadata["performance"]) == {"source_1", "source_3"}
    assert set(metadata["metrics"]["stages"]) == {"stage_0", "stage_2"}
    assert metadata["metrics"]["requests"] == 2
    assert metadata["unknown"] == {"keep": True}


def test_stale_performance_read_then_stage_commit_keeps_both_payloads(tmp_path, monkeypatch):
    """A stale performance snapshot cannot overwrite a later stage commit."""
    db = tmp_path / "meta" / "manifest.db"
    stage_manifest = Manifest(db)
    performance_manifest = Manifest(db)
    run_id = stage_manifest.start_run("metrics")
    original_get = performance_manifest.get_run_metadata
    stale_metadata = original_get(run_id)
    read_old = Barrier(2)
    stage_committed = Barrier(2)
    first_read = True

    def _stale_get(_run_id):
        nonlocal first_read
        if first_read:
            first_read = False
            read_old.wait()
            stage_committed.wait()
        return stale_metadata

    # The explicit read models the old split get→update writer.  Keeping this
    # hook stale makes the regression fail against that implementation while
    # the atomic record_performance_metrics path ignores it.
    monkeypatch.setattr(performance_manifest, "get_run_metadata", _stale_get)

    def _record_stage():
        read_old.wait()
        stage_manifest.record_stage_metrics(run_id, "stage", 0.01, {"requests": 2})
        stage_committed.wait()

    def _record_performance():
        performance_manifest.get_run_metadata(run_id)  # read old metadata first
        performance_manifest.record_performance_metrics(
            run_id, "source", {"requests": 5, "pages": 1}
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(_record_stage), pool.submit(_record_performance)]
        for future in futures:
            future.result()

    metadata = stage_manifest.get_run_metadata(run_id)
    assert metadata["performance"]["source"] == {"requests": 5, "pages": 1}
    assert metadata["metrics"]["stages"]["stage"]["requests"] == 2


def test_record_stage_metrics_after_performance_commit_keeps_both_payloads(tmp_path):
    """The opposite commit order is also serialized by the same primitive."""
    db = tmp_path / "meta" / "manifest.db"
    stage_manifest = Manifest(db)
    performance_manifest = Manifest(db)
    run_id = stage_manifest.start_run("metrics")
    performance_committed = Barrier(2)

    def _record_performance():
        performance_manifest.record_performance_metrics(
            run_id, "source", {"requests": 5, "pages": 1}
        )
        performance_committed.wait()

    def _record_stage():
        performance_committed.wait()
        stage_manifest.record_stage_metrics(run_id, "stage", 0.01, {"requests": 2})

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(_record_performance), pool.submit(_record_stage)]
        for future in futures:
            future.result()

    metadata = stage_manifest.get_run_metadata(run_id)
    assert metadata["performance"]["source"] == {"requests": 5, "pages": 1}
    assert metadata["metrics"]["stages"]["stage"]["requests"] == 2


def test_offline_benchmark_check_reports_parameterized_threshold_breach():
    result = run_offline_benchmark(
        sources=("fixture",),
        requests_per_source=2,
        concurrency_limit=1,
        latency_seconds=0.001,
    )

    assert check_offline_benchmark(result, max_elapsed_seconds=0)
    assert check_offline_benchmark(result, max_concurrency=1) == []
    assert result["mode"] == "offline_fixture"


def test_offline_benchmark_records_each_source_and_respects_fixture_cap():
    result = run_offline_benchmark(
        sources=("tdx_protocol", "eastmoney", "cninfo"),
        requests_per_source=6,
        concurrency_limit=2,
        payload_bytes=64,
        latency_seconds=0.001,
        retry_every=2,
    )

    assert result["mode"] == "offline_fixture"
    assert set(result["sources"]) == {"tdx_protocol", "eastmoney", "cninfo"}
    for source in result["sources"].values():
        assert source["logical_requests"] == 6
        assert source["requests"] == 9
        assert source["retries"] == 3
        assert source["failed_requests"] == 3
        assert source["bytes_read"] == 6 * 64
        assert source["bytes_attempted"] == 9 * 64
        assert source["concurrency_peak"] <= 2
        assert source["throughput_requests_per_second"] > 0
    assert result["totals"]["requests"] == 27
    assert result["totals"]["retries"] == 9
    assert result["totals"]["failed_requests"] == 9
    assert result["totals"]["bytes_read"] == 3 * 6 * 64
    assert result["totals"]["concurrency_peak"] <= 2
    assert result["totals"]["throughput_requests_per_second"] > 0
    assert (
        result["totals"]["elapsed_seconds"] < result["ci_thresholds"]["max_fixture_elapsed_seconds"]
    )


def test_offline_benchmark_manifest_persistence_keeps_full_source_breakdown(tmp_path):
    manifest = Manifest(tmp_path / "meta" / "manifest.db")
    run_id = manifest.start_run("offline-benchmark")
    result = run_offline_benchmark(
        sources=("tdx_protocol", "eastmoney"),
        requests_per_source=2,
        concurrency_limit=1,
        payload_bytes=8,
        latency_seconds=0.0005,
        retry_every=2,
    )

    persist_offline_benchmark(manifest, run_id, result)
    metadata = manifest.get_run_metadata(run_id)
    assert metadata["performance"]["offline_benchmark"] == result
    stage = metadata["metrics"]["stages"]["offline_benchmark"]
    assert stage["requests"] == result["totals"]["requests"]
    assert stage["bytes_read"] == result["totals"]["bytes_read"]
    assert stage["retries"] == result["totals"]["retries"]
    assert stage["concurrency_peak"] == 1


@pytest.mark.skipif(sys.platform != "win32", reason="Windows GetProcessMemoryInfo")
def test_windows_reports_peak_memory():
    from cnequity.diagnostics.metrics import _rss_bytes

    assert _rss_bytes() > 10 * 1024 * 1024
