"""Non-secret effective settings suitable for a run receipt."""

import json
from dataclasses import asdict
from hashlib import sha256

from cnequity import __version__
from cnequity.config.logging import logging_settings
from cnequity.config.resources import DEFAULT_RESOURCES, resources_for
from cnequity.config.retention import revision_keep
from cnequity.domain.rate_limit import DEFAULT_SOURCE_INTERVALS

_RECORDED_SOURCES = frozenset(DEFAULT_SOURCE_INTERVALS) | {
    "baostock",
    "eastmoney",
    "eastmoney_dc",
    "eastmoney_push2",
    "tdx_protocol",
    "ths_official",
    "tushare",
}


def run_configuration(config) -> dict:
    """Capture a fixed allowlist of operational settings at run creation."""
    path = getattr(config, "config_path", None)
    resources = {
        name: {
            key: str(value) if key == "temp_directory" else value
            for key, value in resources_for(config, name).options().items()
        }
        for name in DEFAULT_RESOURCES
    }
    effective = {
        "workers": getattr(config, "workers", None),
        "batch_size": getattr(config, "batch_size", None),
        "max_retries": getattr(config, "max_retries", None),
        "retry_backoff_seconds": getattr(config, "retry_backoff_seconds", None),
        "batch_stale_seconds": getattr(config, "batch_stale_seconds", None),
        "audit_gate": getattr(config, "audit_gate", None),
        "publication_gate": getattr(config, "publication_gate", None),
        "run_failure_window_hours": getattr(config, "run_failure_window_hours", None),
        "run_failure_streak": getattr(config, "run_failure_streak", None),
        "margin_trading_source": getattr(config, "margin_trading_source", None),
        "adj_factors_source": getattr(config, "adj_factors_source", None),
        "sources": {
            name: bool(enabled)
            for name, enabled in getattr(config, "sources", {}).items()
            if name in _RECORDED_SOURCES
        },
        "source_intervals": {
            name: seconds
            for name, seconds in getattr(config, "source_intervals", {}).items()
            if name in _RECORDED_SOURCES
        },
        "tdx_enabled": bool(getattr(config, "tdx_enabled", False)),
        "tushare_configured": bool(getattr(config, "tushare_token", None)),
        "eastmoney_push2_paused": bool(getattr(config, "eastmoney_push2_paused", False)),
        "keep_generations": revision_keep(config),
        "duckdb": resources,
        "query_scan_budget_rows": getattr(config, "query_scan_budget_rows", None),
        "logging": asdict(logging_settings(config)),
    }
    encoded = json.dumps(effective, sort_keys=True, separators=(",", ":")).encode()
    return {
        "schema_version": 1,
        "version": __version__,
        "config_path": str(path) if path is not None else None,
        "digest": sha256(encoded).hexdigest(),
        "settings": effective,
    }
