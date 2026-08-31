"""Small, dependency-free ingestion metrics helpers.

Metrics are deliberately descriptive rather than performance promises.  A
benchmark can compare two runs made against the same fixture, while a live
run can expose the source and storage work that actually happened without
claiming a particular wall-clock speed.
"""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

try:
    import resource
except ImportError:  # pragma: no cover - Windows has no resource module
    resource = None  # type: ignore[assignment]

COUNTER_KEYS = (
    "requests",
    "pages",
    "cache_hits",
    "fallback_requests",
    "retries",
    # Explicit spelling for adapter/request retries. ``retries`` remains in
    # the public payload for compatibility; the manifest stores this field on
    # each batch separately from the orchestrator retry budget.
    "request_retries",
    "failed_requests",
    "rows_read",
    "rows_written",
    "bytes_read",
    "bytes_written",
    "changed_partitions",
)

# These are observations rather than additive counters.  They are kept out of
# ``COUNTER_KEYS`` because combining two parallel stages must take the maximum
# in-flight peak and sum elapsed wire time, not blindly apply one operation to
# every field.
OBSERVATION_KEYS = (
    "request_seconds",
    "concurrency_wait_seconds",
    "concurrency_peak",
    "throughput_requests_per_second",
)

# The fixture benchmark intentionally names every configured upstream lane.
# It is a local transport contract, not a claim about any vendor's production
# speed or availability.
OFFLINE_BENCHMARK_SOURCES = (
    "tdx_protocol",
    "eastmoney",
    "cninfo",
    "ths",
    "ths_pages",
    "ths_bonus",
    "sina",
    "sina_bars",
    "bse",
    "baostock",
    "tushare",
    "pboc",
    "nbs",
    "exchange",
)


def new_metrics() -> dict[str, Any]:
    """Return a JSON-safe metrics payload with stable counter names."""

    return {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": 0.0,
        "peak_memory_bytes": 0,
        **{key: 0 for key in COUNTER_KEYS},
        "request_seconds": 0.0,
        "concurrency_wait_seconds": 0.0,
        "concurrency_peak": 0,
        "throughput_requests_per_second": 0.0,
        "source_metrics": {},
        "stages": {},
    }


def _rss_bytes() -> int:
    """Return process peak RSS in bytes on macOS and Linux."""

    if resource is None:
        return 0
    try:
        value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    except (AttributeError, OSError, ValueError):
        return 0
    # macOS reports bytes; Linux reports KiB.  Windows may not expose
    # ``resource`` at all, in which case the caller receives zero.
    return value if os.sys.platform == "darwin" else value * 1024


def finish_metrics(metrics: Mapping[str, Any], started: float) -> dict[str, Any]:
    """Finalize a metrics mapping without mutating a caller-owned object."""

    out = deepcopy(dict(metrics))
    out["elapsed_seconds"] = round(max(0.0, time.perf_counter() - started), 6)
    out["peak_memory_bytes"] = max(int(out.get("peak_memory_bytes", 0) or 0), _rss_bytes())
    for key in COUNTER_KEYS:
        try:
            value = int(out.get(key, 0) or 0)
        except (TypeError, ValueError):
            value = 0
        out[key] = max(0, value)
    for key in ("request_seconds", "concurrency_wait_seconds", "throughput_requests_per_second"):
        try:
            out[key] = max(0.0, float(out.get(key, 0.0) or 0.0))
        except (TypeError, ValueError):
            out[key] = 0.0
    try:
        out["concurrency_peak"] = max(0, int(out.get("concurrency_peak", 0) or 0))
    except (TypeError, ValueError):
        out["concurrency_peak"] = 0
    return out


def add_metrics(target: dict[str, Any], source: Mapping[str, Any] | None) -> None:
    """Accumulate counters and stage records from *source* into *target*."""

    if not source:
        return
    for key in COUNTER_KEYS:
        raw = source.get(key, 0)
        try:
            target[key] = int(target.get(key, 0) or 0) + int(raw or 0)
        except (TypeError, ValueError):
            continue
    target["peak_memory_bytes"] = max(
        int(target.get("peak_memory_bytes", 0) or 0),
        int(source.get("peak_memory_bytes", 0) or 0),
    )
    for key in ("request_seconds", "concurrency_wait_seconds"):
        try:
            target[key] = float(target.get(key, 0.0) or 0.0) + float(source.get(key, 0.0) or 0.0)
        except (TypeError, ValueError):
            continue
    try:
        target["concurrency_peak"] = max(
            int(target.get("concurrency_peak", 0) or 0),
            int(source.get("concurrency_peak", 0) or 0),
        )
    except (TypeError, ValueError):
        pass
    source_metrics = source.get("source_metrics")
    if isinstance(source_metrics, Mapping):
        target_metrics = target.setdefault("source_metrics", {})
        if isinstance(target_metrics, dict):
            for name, item in source_metrics.items():
                if isinstance(item, Mapping):
                    target_metrics[str(name)] = deepcopy(dict(item))
    stages = source.get("stages")
    if isinstance(stages, Mapping):
        target_stages = target.setdefault("stages", {})
        if isinstance(target_stages, dict):
            for stage, item in stages.items():
                if not isinstance(item, Mapping):
                    continue
                current = target_stages.setdefault(str(stage), {})
                if not isinstance(current, dict):
                    current = {}
                    target_stages[str(stage)] = current
                for key, value in item.items():
                    if key in COUNTER_KEYS:
                        try:
                            current[key] = int(current.get(key, 0) or 0) + int(value or 0)
                        except (TypeError, ValueError):
                            continue
                    elif key == "elapsed_seconds":
                        try:
                            current[key] = float(current.get(key, 0.0) or 0.0) + float(value or 0.0)
                        except (TypeError, ValueError):
                            continue
                    elif key in {"request_seconds", "concurrency_wait_seconds"}:
                        try:
                            current[key] = float(current.get(key, 0.0) or 0.0) + float(value or 0.0)
                        except (TypeError, ValueError):
                            continue
                    elif key == "concurrency_peak":
                        try:
                            current[key] = max(int(current.get(key, 0) or 0), int(value or 0))
                        except (TypeError, ValueError):
                            continue
                    else:
                        current[key] = value


def stage_metrics(
    stage: str,
    *,
    elapsed_seconds: float,
    metrics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one stage record suitable for a run manifest or benchmark file."""

    item = {"elapsed_seconds": round(max(0.0, float(elapsed_seconds)), 6)}
    if metrics:
        for key in COUNTER_KEYS:
            if key in metrics:
                try:
                    item[key] = max(0, int(metrics[key] or 0))
                except (TypeError, ValueError):
                    pass
        # Existing adapters emit ``retries`` for request attempts. Preserve
        # that contract while making the batch/manifest hand-off explicit for
        # new callers. Do not infer retries from request counts or failures.
        if "request_retries" not in metrics and "retries" in metrics:
            try:
                item["request_retries"] = max(0, int(metrics["retries"] or 0))
            except (TypeError, ValueError):
                pass
        if "peak_memory_bytes" in metrics:
            try:
                item["peak_memory_bytes"] = max(0, int(metrics["peak_memory_bytes"] or 0))
            except (TypeError, ValueError):
                pass
        for key in (
            "request_seconds",
            "concurrency_wait_seconds",
            "throughput_requests_per_second",
        ):
            if key in metrics:
                try:
                    item[key] = max(0.0, float(metrics[key] or 0.0))
                except (TypeError, ValueError):
                    pass
        if "concurrency_peak" in metrics:
            try:
                item["concurrency_peak"] = max(0, int(metrics["concurrency_peak"] or 0))
            except (TypeError, ValueError):
                pass
        if isinstance(metrics.get("source_metrics"), Mapping):
            item["source_metrics"] = deepcopy(dict(metrics["source_metrics"]))
    return {"stages": {stage: item}, **item}


def run_offline_benchmark(
    *,
    sources: Iterable[str] = OFFLINE_BENCHMARK_SOURCES,
    requests_per_source: int = 4,
    concurrency_limit: int = 2,
    payload_bytes: int = 256,
    latency_seconds: float = 0.001,
    retry_every: int = 0,
    max_elapsed_seconds: float = 10.0,
    max_concurrency: int | None = None,
    min_throughput_requests_per_second: float = 0.0,
) -> dict[str, Any]:
    """Run a repeatable, network-free transport benchmark fixture.

    Each logical request sleeps for a caller-selected fixture latency and
    returns a fixed-size payload.  ``retry_every`` makes the first attempt of
    every Nth logical request fail, exercising retry accounting without
    relying on a live service.  Results report observations by source and are
    suitable for manifest persistence; no value is presented as a supplier's
    real-world speed.
    """
    names = tuple(dict.fromkeys(str(source) for source in sources if str(source).strip()))
    if not names:
        raise ValueError("offline benchmark requires at least one source")
    if requests_per_source < 1:
        raise ValueError("requests_per_source must be >= 1")
    if concurrency_limit < 1:
        raise ValueError("concurrency_limit must be >= 1")
    if payload_bytes < 0:
        raise ValueError("payload_bytes must be >= 0")
    if latency_seconds < 0:
        raise ValueError("latency_seconds must be >= 0")
    if retry_every < 0:
        raise ValueError("retry_every must be >= 0")
    if max_elapsed_seconds < 0:
        raise ValueError("max_elapsed_seconds must be >= 0")
    if max_concurrency is not None and max_concurrency < 0:
        raise ValueError("max_concurrency must be >= 0")
    if min_throughput_requests_per_second < 0:
        raise ValueError("min_throughput_requests_per_second must be >= 0")
    configured_max_concurrency = concurrency_limit if max_concurrency is None else max_concurrency

    started = time.perf_counter()
    per_source: dict[str, dict[str, Any]] = {}
    source_lock = threading.Lock()

    def _run_source(source: str) -> tuple[str, dict[str, Any]]:
        source_started = time.perf_counter()
        semaphore = threading.BoundedSemaphore(concurrency_limit)
        active = 0
        peak = 0
        counters = {
            "logical_requests": requests_per_source,
            "requests": 0,
            "bytes_read": 0,
            "retries": 0,
            "request_retries": 0,
            "failed_requests": 0,
        }
        active_lock = threading.Lock()

        def _request(index: int) -> None:
            nonlocal active, peak
            attempt = 0
            while True:
                with semaphore:
                    with active_lock:
                        active += 1
                        peak = max(peak, active)
                        counters["requests"] += 1
                    try:
                        if latency_seconds:
                            time.sleep(latency_seconds)
                        should_retry = retry_every and index % retry_every == 0 and attempt == 0
                        if should_retry:
                            with active_lock:
                                counters["retries"] += 1
                                counters["request_retries"] += 1
                                counters["failed_requests"] += 1
                            attempt += 1
                            continue
                        with active_lock:
                            counters["bytes_read"] += payload_bytes
                        return
                    finally:
                        with active_lock:
                            active -= 1

        with ThreadPoolExecutor(max_workers=concurrency_limit * 2) as pool:
            futures = [pool.submit(_request, index) for index in range(requests_per_source)]
            for future in as_completed(futures):
                future.result()
        elapsed = max(time.perf_counter() - source_started, 1e-9)
        result = {
            **counters,
            "bytes_attempted": counters["requests"] * payload_bytes,
            "elapsed_seconds": round(elapsed, 6),
            "request_seconds": round(counters["requests"] * latency_seconds, 6),
            "concurrency_peak": peak,
            "throughput_requests_per_second": round(counters["logical_requests"] / elapsed, 3),
            "fixture": {
                "payload_bytes": payload_bytes,
                "latency_seconds": latency_seconds,
                "concurrency_limit": concurrency_limit,
                "retry_every": retry_every,
            },
        }
        with source_lock:
            per_source[source] = result
        return source, result

    with ThreadPoolExecutor(max_workers=len(names)) as pool:
        list(pool.map(_run_source, names))

    total_elapsed = max(time.perf_counter() - started, 1e-9)
    totals = {
        "logical_requests": sum(item["logical_requests"] for item in per_source.values()),
        "requests": sum(item["requests"] for item in per_source.values()),
        "bytes_read": sum(item["bytes_read"] for item in per_source.values()),
        "bytes_attempted": sum(item["bytes_attempted"] for item in per_source.values()),
        "retries": sum(item["retries"] for item in per_source.values()),
        "request_retries": sum(item["request_retries"] for item in per_source.values()),
        "failed_requests": sum(item["failed_requests"] for item in per_source.values()),
        "elapsed_seconds": round(total_elapsed, 6),
        "request_seconds": round(sum(item["request_seconds"] for item in per_source.values()), 6),
        "concurrency_peak": max(item["concurrency_peak"] for item in per_source.values()),
        "throughput_requests_per_second": round(
            sum(item["logical_requests"] for item in per_source.values()) / total_elapsed, 3
        ),
    }
    return {
        "schema_version": 1,
        "mode": "offline_fixture",
        "description": "Synthetic transport measurements; not vendor speed claims.",
        "sources": per_source,
        "totals": totals,
        "ci_thresholds": {
            "max_concurrency": configured_max_concurrency,
            "max_fixture_elapsed_seconds": max_elapsed_seconds,
            "min_throughput_requests_per_second": min_throughput_requests_per_second,
        },
    }


def check_offline_benchmark(
    result: Mapping[str, Any],
    *,
    max_elapsed_seconds: float | None = None,
    max_concurrency: int | None = None,
    min_throughput_requests_per_second: float | None = None,
) -> list[str]:
    """Return CI-gate failures for an offline fixture result.

    The check is deliberately explicit about ``offline_fixture`` mode.  It is
    a deterministic regression gate for limiter/telemetry plumbing, never a
    claim about a vendor's network performance or availability.
    """
    if result.get("mode") != "offline_fixture":
        return ["benchmark result is not an offline_fixture measurement"]

    thresholds = result.get("ci_thresholds")
    if not isinstance(thresholds, Mapping):
        thresholds = {}
    if max_elapsed_seconds is None:
        max_elapsed_seconds = thresholds.get("max_fixture_elapsed_seconds", 10.0)
    if max_concurrency is None:
        max_concurrency = thresholds.get("max_concurrency")
    if min_throughput_requests_per_second is None:
        min_throughput_requests_per_second = thresholds.get(
            "min_throughput_requests_per_second", 0.0
        )

    failures: list[str] = []
    try:
        elapsed_limit = float(max_elapsed_seconds)
    except (TypeError, ValueError):
        return [f"max elapsed threshold is not numeric: {max_elapsed_seconds!r}"]
    if elapsed_limit < 0:
        failures.append("max elapsed threshold must be >= 0")
    totals = result.get("totals")
    if not isinstance(totals, Mapping):
        return [*failures, "benchmark result has no totals"]
    try:
        elapsed = float(totals.get("elapsed_seconds", 0.0) or 0.0)
    except (TypeError, ValueError):
        failures.append("benchmark elapsed_seconds is not numeric")
        elapsed = 0.0
    if elapsed_limit >= 0 and elapsed > elapsed_limit:
        failures.append(f"offline fixture elapsed {elapsed:.6f}s exceeds {elapsed_limit:.6f}s")

    if max_concurrency is not None:
        try:
            concurrency_limit = int(max_concurrency)
        except (TypeError, ValueError):
            failures.append(f"max concurrency threshold is not an integer: {max_concurrency!r}")
        else:
            if concurrency_limit < 1:
                failures.append("max concurrency threshold must be >= 1")
            else:
                observed = [totals.get("concurrency_peak", 0)]
                sources = result.get("sources")
                if isinstance(sources, Mapping):
                    observed.extend(
                        item.get("concurrency_peak", 0)
                        for item in sources.values()
                        if isinstance(item, Mapping)
                    )
                try:
                    peak = max(int(value or 0) for value in observed)
                except (TypeError, ValueError):
                    failures.append("benchmark concurrency_peak is not an integer")
                else:
                    if peak > concurrency_limit:
                        failures.append(
                            f"offline fixture concurrency peak {peak} exceeds {concurrency_limit}"
                        )

    try:
        throughput_limit = float(min_throughput_requests_per_second or 0.0)
    except (TypeError, ValueError):
        failures.append(
            f"minimum throughput threshold is not numeric: {min_throughput_requests_per_second!r}"
        )
    else:
        try:
            throughput = float(totals.get("throughput_requests_per_second", 0.0) or 0.0)
        except (TypeError, ValueError):
            failures.append("benchmark throughput_requests_per_second is not numeric")
            throughput = 0.0
        if throughput_limit < 0:
            failures.append("minimum throughput threshold must be >= 0")
        elif throughput < throughput_limit:
            failures.append(
                f"offline fixture throughput {throughput:.3f} requests/s is below "
                f"{throughput_limit:.3f} requests/s"
            )
    return failures


def persist_offline_benchmark(manifest: Any, run_id: str, result: Mapping[str, Any]) -> None:
    """Persist the complete fixture result alongside regular run telemetry."""
    manifest.record_performance_metrics(run_id, "offline_benchmark", dict(result))
    totals = result.get("totals")
    if isinstance(totals, Mapping):
        manifest.record_stage_metrics(
            run_id,
            "offline_benchmark",
            float(totals.get("elapsed_seconds", 0.0) or 0.0),
            {
                "requests": totals.get("requests", 0),
                "retries": totals.get("retries", 0),
                "request_retries": totals.get("request_retries", totals.get("retries", 0)),
                "failed_requests": totals.get("failed_requests", 0),
                "bytes_read": totals.get("bytes_read", 0),
                "request_seconds": totals.get("request_seconds", 0.0),
                "concurrency_peak": totals.get("concurrency_peak", 0),
                "throughput_requests_per_second": totals.get("throughput_requests_per_second", 0.0),
            },
        )
