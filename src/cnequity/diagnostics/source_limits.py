"""Read-only view of the current egress and lake request protections."""

from __future__ import annotations

import json
import math
import sqlite3
import time
from contextlib import closing

from cnequity.adapters.eastmoney import host_guard, news_pause
from cnequity.diagnostics.source_health import PROBES
from cnequity.domain.datasets import DATASETS
from cnequity.domain.http_policy import cooldown_status, source_family
from cnequity.domain.rate_limit import (
    DEFAULT_SOURCE_CONCURRENCY,
    DEFAULT_SOURCE_INTERVALS,
    DEFAULT_UNKNOWN_SOURCE_INTERVAL_SECONDS,
    _policy_day,
    _read_json,
    _safe_source_name,
)
from cnequity.sqlite_readonly import connect_readonly


def effective_source_policy(config, source: str, aliases: set[str] | None = None) -> dict:
    """Read the configured and same-day shared limits without reserving a slot."""
    family = source_family(source)
    intervals = {**DEFAULT_SOURCE_INTERVALS, **config.source_intervals}
    names = {family, source}
    names.update(name for name in (aliases or set()) if source_family(name) == family)
    names.update(
        name
        for mapping in (
            intervals,
            config.source_concurrency,
            config.http_workers,
            config.source_workers,
        )
        for name in mapping
        if source_family(name) == family
    )
    configured_caps = [
        int(mapping[name])
        for mapping in (config.source_concurrency, config.http_workers, config.source_workers)
        for name in names
        if name in mapping
    ]
    if configured_caps:
        configured_cap = min(configured_caps)
    elif family == "tdx_protocol" and config.tdx_enabled:
        # Match the direct wire adapter and the shared source-request helper.
        # A macOS process-pool budget of one does not imply one TDX socket.
        configured_cap = config.tdx_rate_limit_spec().concurrency_limit
    elif family.startswith("futures_exchange_"):
        configured_cap = DEFAULT_SOURCE_CONCURRENCY["futures_exchange"]
    else:
        configured_cap = DEFAULT_SOURCE_CONCURRENCY.get(family, int(config.workers))
    if family == "baostock":
        from cnequity.adapters.baostock.access import MAX_CONCURRENT_CONNECTIONS

        configured_cap = min(int(configured_cap), MAX_CONCURRENT_CONNECTIONS)
    day = _policy_day(time.time())
    concurrency = _read_json(
        config.rate_limit_root / f"concurrency-{_safe_source_name(family)}.json"
    )
    saved_cap = concurrency.get("limit") if concurrency.get("policy_day") == day else None
    effective_cap = (
        min(configured_cap, saved_cap)
        if type(saved_cap) is int and saved_cap > 0
        else configured_cap
    )
    pacing = {}
    for name in sorted(names):
        configured = intervals.get(name)
        if name == family and configured is None:
            family_intervals = [
                value for alias, value in intervals.items() if source_family(alias) == family
            ]
            if family_intervals:
                configured = max(family_intervals)
        if name == "tdx_protocol" and config.tdx_enabled:
            configured = config.tdx_rate_limit_spec().min_interval
        elif configured is None and name.startswith("futures_exchange_"):
            configured = intervals["futures_exchange"]
        elif configured is None and name != family:
            configured = intervals.get(family)
        if configured is None:
            configured = DEFAULT_UNKNOWN_SOURCE_INTERVAL_SECONDS
        saved = _read_json(config.rate_limit_root / f"{_safe_source_name(name)}.json")
        saved_interval = saved.get("min_interval") if saved.get("policy_day") == day else None
        effective = (
            max(configured or 0.0, saved_interval)
            if type(saved_interval) in (int, float)
            and math.isfinite(saved_interval)
            and saved_interval >= 0
            else configured
        )
        pacing[name] = {"configured_seconds": configured, "effective_seconds": effective}
    return {
        "configured_max_concurrency": configured_cap,
        "effective_max_concurrency": effective_cap,
        "pacing_seconds": pacing,
    }


def _metered_attempts_today(config, family: str) -> dict:
    """Read admitted request scopes without mistaking policy refusals for sends."""
    payload = _read_json(config.rate_limit_root / f"meter-{_safe_source_name(family)}.json")
    if payload.get("policy_day") != _policy_day(time.time()):
        return {"count": 0, "aliases": {}}
    aliases = payload.get("aliases")
    aliases = (
        {str(name): value for name, value in aliases.items() if type(value) is int and value >= 0}
        if isinstance(aliases, dict)
        else {}
    )
    return {"count": sum(aliases.values()), "aliases": aliases}


def _wire_responses_today(config, family: str) -> dict:
    """Read response receipts; the body size is decoded application bytes."""
    payload = _read_json(config.rate_limit_root / f"wire-{_safe_source_name(family)}.json")
    if payload.get("policy_day") != _policy_day(time.time()):
        return {"responses": 0, "body_bytes": 0, "statuses": {}, "endpoints": {}}
    return {
        "responses": payload.get("responses", 0),
        "body_bytes": payload.get("body_bytes", 0),
        "statuses": payload.get("statuses", {}),
        "endpoints": payload.get("endpoints", {}),
    }


def _cache_reuse_today(config, family: str) -> dict:
    payload = _read_json(config.rate_limit_root / f"reuse-{_safe_source_name(family)}.json")
    if payload.get("policy_day") != _policy_day(time.time()):
        return {"hits": 0, "caches": {}}
    return {"hits": payload.get("hits", 0), "caches": payload.get("caches", {})}


def _request_events_today(config, family: str) -> dict:
    payload = _read_json(config.rate_limit_root / f"events-{_safe_source_name(family)}.json")
    if payload.get("policy_day") != _policy_day(time.time()):
        return {"retry": 0, "fallback": 0}
    return {"retry": payload.get("retry", 0), "fallback": payload.get("fallback", 0)}


def _latest_run_metrics(config) -> dict | None:
    path = config.manifest_path
    if not path.is_file():
        return None
    try:
        with closing(connect_readonly(path)) as conn:
            row = conn.execute(
                "SELECT run_id, metadata_json FROM ingestion_runs ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        metadata = json.loads(row[1] or "{}")
        metrics = metadata.get("metrics", {}) if isinstance(metadata, dict) else {}
        if not isinstance(metrics, dict):
            metrics = {}
        source_metrics = metrics.get("source_metrics", {})
        return {
            "run_id": row[0],
            "requests": metrics.get("requests"),
            "request_retries": metrics.get("request_retries"),
            "source_metrics": source_metrics if isinstance(source_metrics, dict) else {},
        }
    except (OSError, sqlite3.Error, ValueError, TypeError):
        return None


def build_source_limits(config) -> dict:
    """Report known limits and repair ranges without creating lake files."""
    names = (
        set(config.sources)
        | set(DEFAULT_SOURCE_INTERVALS)
        | set(config.source_intervals)
        | set(config.source_concurrency)
        | set(config.http_workers)
        | set(config.source_workers)
    )
    names.update({"tdx_protocol", "eastmoney_push2", "eastmoney_dc"})
    # Futures files pace and meter per exchange host, not under the generic
    # config lane. Include those effective lanes so a one-file check is visible
    # in `sources limits` instead of disappearing from the request accounting.
    names.update(
        f"futures_exchange_{probe.host}"
        for probe in PROBES
        if probe.config_key == "futures_exchange"
    )
    for spec in DATASETS.values():
        names.update(
            name
            for name in (
                spec.primary_source,
                spec.backup_source,
                spec.backfill_source,
                *spec.supplementary_sources,
                *spec.repair_sources,
            )
            if name
        )
    families = sorted({source_family(name) for name in names})
    sources = {}
    for family in families:
        aliases = sorted(name for name in names if source_family(name) == family)
        sources[family] = {
            "enabled": any(config.sources.get(name, True) for name in aliases)
            and config.sources.get(family, True)
            and (not family.startswith("eastmoney") or config.sources.get("eastmoney", True))
            and (
                not family.startswith("futures_exchange_")
                or config.sources.get("futures_exchange", True)
            )
            and (family != "tdx_protocol" or config.tdx_enabled),
            "alias_enabled": {name: config.sources.get(name, True) for name in aliases},
            **effective_source_policy(config, family, set(aliases)),
            **cooldown_status(config.rate_limit_root, family),
            "metered_attempts_today": _metered_attempts_today(config, family),
            "wire_responses_today": _wire_responses_today(config, family),
            "cache_reuse_today": _cache_reuse_today(config, family),
            "request_events_today": _request_events_today(config, family),
        }
    outstanding = {}
    for dataset in DATASETS:
        state = _read_json(config.meta_root / "state" / f"{dataset}.json")
        rows = state.get("outstanding_keys")
        if isinstance(rows, list) and rows:
            outstanding[dataset] = {
                "keys": len(rows),
                "repair_command": f"cne backfill {dataset} --outstanding",
            }
    guard = host_guard.status(config)
    from cnequity.adapters.baostock.access import status as baostock_status

    return {
        "rate_limit_root": str(config.rate_limit_root),
        "sources": sources,
        "eastmoney": {
            **{name: guard[name] for name in ("vendor", "push2", "datacenter")},
            "news": news_pause.status(config),
        },
        "baostock": baostock_status(config),
        "latest_run_metrics": _latest_run_metrics(config),
        "outstanding": outstanding,
        "note": (
            "metered_attempts_today counts entries into a network scope that passed the shared "
            "rate limit, including handshakes and pages that are wired in; one scope may send "
            "several underlying requests. wire_responses_today counts only responses returned "
            "through a wired response hook, with their decoded body size; the endpoint is the "
            "host plus a hash of the path, so query parameters are never stored, and requests "
            "that got no response are not included. cache_reuse_today counts local reuses of "
            "a wired response cache, which are not treated as new source observations. "
            "request_events_today counts only the tagged extra retries and direct-connection "
            "fallbacks, so its coverage is incomplete. The latest run's requests/retries are "
            "still the telemetry adapters recorded, not the total requests sent to all sources."
        ),
    }
