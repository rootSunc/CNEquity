"""The last key check for each paid source, kept beside the lake's metadata.

A check sends one small request with the configured credential and records
whether the provider accepted it. The record holds a short digest of the
credential, never the credential, so a changed key reads as unchecked rather
than inheriting the old key's verdict.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from cnequity.file_replace import replace_with_retry

# The credential attribute on ``Config`` for each paid source.
KEY_ATTRS = {"tushare": "tushare_token", "ths_official": "ths_official_api_key"}
# ok: accepted. limited: the key is valid but the probe endpoint is outside its
# plan. rejected: the provider refused the key. error: no verdict (network,
# rate limit, unexpected reply).
STATES = ("ok", "limited", "rejected", "error")


def _path(config) -> Path:
    return Path(config.meta_root) / "sources" / "key-checks.json"


def fingerprint(key: str) -> str:
    return hashlib.sha256(key.strip().encode("utf-8")).hexdigest()[:12]


def configured_key(config, source: str) -> str | None:
    key = getattr(config, KEY_ATTRS[source], None) if config is not None else None
    return str(key).strip() or None if key else None


def _load(config) -> dict:
    try:
        with open(_path(config), encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def last_check(config, source: str) -> dict | None:
    """The record for the key configured now, or None when it was never checked."""
    key = configured_key(config, source)
    if key is None:
        return None
    record = _load(config).get(source)
    if not isinstance(record, dict) or record.get("fingerprint") != fingerprint(key):
        return None
    if record.get("state") not in STATES:
        return None
    return {k: record.get(k) for k in ("state", "message", "checked_at")}


def save_check(config, source: str, state: str, message: str, checked_at: str) -> dict:
    key = configured_key(config, source)
    if key is None or state not in STATES:
        raise ValueError(f"cannot record a {source} key check")
    data = _load(config)
    data[source] = {
        "fingerprint": fingerprint(key),
        "state": state,
        "message": message,
        "checked_at": checked_at,
    }
    path = _path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    replace_with_retry(tmp, path)
    return {"state": state, "message": message, "checked_at": checked_at}


def key_rejected(config, source: str) -> bool:
    """Whether the last check of the configured key was refused by the provider."""
    record = last_check(config, source)
    return bool(record and record["state"] == "rejected")
