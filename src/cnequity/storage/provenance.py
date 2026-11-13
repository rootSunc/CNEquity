"""Safe runtime identity attached to dataset revision receipts."""

from __future__ import annotations

import functools
import hashlib
import json
import os
import subprocess
from dataclasses import fields
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from cnequity.config import Config
from cnequity.run_scope import LEGACY_RUN_FLAG_ATTRS

#: Fingerprinted as "configured or not" under these older payload names.
_LEGACY_SECRET_KEYS = {
    "tushare_token": "tushare_configured",
    "eastmoney_proxy": "eastmoney_proxy_configured",
}
_LOCATION_FIELDS = frozenset({"data_root", "config_path", "duckdb_path"})
#: Runtime plumbing and diagnostics, not data-affecting settings.
#: Run flags that were declared Config fields, hashed under those names.
_FINGERPRINTED_FLAGS = (
    "_corporate_actions_baostock_repair",
    "_corporate_actions_ths_repair",
    "_bse_tip_repair",
    "_tdx_amount_repair",
    "_turnover_repair",
    "_turnover_peer_review",
    "_sector_bars_force",
)
_NOT_DATA_FIELDS = frozenset(
    {
        "_rate_limiters",
        "inline_credentials",
        "backfill_window",
        "run_flags",
        "eastmoney_proxy_hosts",
    }
)


def _json_value(value: Any) -> Any:
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        # Sorted: str() of a set follows per-process hash order, which made
        # the same config hash differently on every run.
        return sorted(_json_value(item) for item in value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def config_fingerprint(config: Config) -> str:
    """Hash data-affecting configuration without serialising credentials or paths."""
    payload: dict[str, Any] = {}
    for item in fields(config):
        name = item.name
        value = getattr(config, name)
        if name in _NOT_DATA_FIELDS or name in _LOCATION_FIELDS:
            continue
        if name in _LEGACY_SECRET_KEYS:
            payload[_LEGACY_SECRET_KEYS[name]] = bool(value)
            continue
        if not item.repr:
            # Any field hidden from repr is a credential: hash whether it is
            # set, never its value, so rotating a key keeps the fingerprint.
            # None stays None, which keeps fingerprints of unset keys unchanged.
            payload[name] = "configured" if value else None
            continue
        payload[name] = _json_value(value)
    # These two were Config fields before the backfill window became one
    # value. Hash them under their old names, exactly as before, so a config
    # keeps its fingerprint and nothing reads as changed.
    window = getattr(config, "backfill_window", None)
    if window is not None:
        payload["_backfill"] = window.active
        payload["_backfill_symbols"] = window.symbol_list()
    # Likewise the run flags that used to be declared Config fields; the
    # others were never fields and never hashed.
    flags = getattr(config, "run_flags", None)
    if flags is not None:
        for name in _FINGERPRINTED_FLAGS:
            payload[name] = getattr(flags, LEGACY_RUN_FLAG_ATTRS[name])
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _package_version() -> str:
    try:
        return version("cnequity")
    except PackageNotFoundError:
        return "unknown"


@functools.lru_cache(maxsize=1)
def _git_identity() -> tuple[str | None, bool | None]:
    # The checkout's HEAD and working-tree state do not change within a single
    # CLI invocation, and every caller (compaction, snapshot creation) is a
    # short-lived process. Caching collapses repeated lineage stamps in one
    # process to a single pair of subprocesses.
    override = os.getenv("CNEQUITY_GIT_COMMIT")
    if override:
        return override, None
    checkout = Path(__file__).resolve().parents[3]
    try:
        commit = subprocess.run(
            ["git", "-C", str(checkout), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "-C", str(checkout), "status", "--porcelain", "--untracked-files=no"],
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return None, None
    return commit or None, bool(status.strip())


def runtime_lineage(config: Config) -> dict[str, Any]:
    """Return non-secret code and configuration identity for a run receipt."""
    commit, dirty = _git_identity()
    return {
        "package_version": _package_version(),
        "git_commit": commit,
        "git_dirty": dirty,
        "config_fingerprint": config_fingerprint(config),
    }
