"""Check a paid source's key with one small request and record the verdict.

The check runs whether or not the source is switched on, so a key can be
confirmed before it is enabled. It writes nothing to the lake. The request
counts against the source's own pacing like any other call.
"""

from __future__ import annotations

import copy
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

from cnequity.config.keyed_sources import KEYED_SOURCES
from cnequity.file_replace import replace_with_retry
from cnequity.i18n import _
from cnequity.storage.key_checks import configured_key, last_check, save_check

PAID_SOURCES = tuple(KEYED_SOURCES)


def _probe_tushare(config) -> tuple[str, str]:
    from cnequity.adapters.tushare.client import (
        TushareClient,
        TusharePermissionDenied,
        TushareQuotaExhausted,
        TushareRefused,
    )

    probe = copy.copy(config)
    probe.sources = {**(config.sources or {}), "tushare": True}
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    try:
        with TushareClient(probe) as client:
            client.rows(
                "trade_cal", {"exchange": "SSE", "start_date": today, "end_date": today}, "cal_date"
            )
    except TusharePermissionDenied as exc:
        return "limited", str(exc)
    except TushareQuotaExhausted as exc:
        return "limited", str(exc)
    except TushareRefused as exc:
        return "rejected", str(exc)
    return "ok", ""


def _probe_ths_official(config) -> tuple[str, str]:
    from cnequity.adapters.ths_official.client import ThsOfficialAuthError, ThsOfficialClient

    try:
        with ThsOfficialClient(configured_key(config, "ths_official"), config=config) as client:
            client.get("/api/a-share/valuations/snapshot", thscodes="600000.SH")
    except ThsOfficialAuthError as exc:
        return ("limited" if exc.granted_elsewhere else "rejected"), str(exc)
    return "ok", ""


_PROBES = {"tushare": _probe_tushare, "ths_official": _probe_ths_official}
assert set(_PROBES) == set(KEYED_SOURCES), "every keyed source needs a probe"


def check_key(config, source: str) -> dict:
    """Probe *source* with its configured key; returns and saves the verdict."""
    if source not in _PROBES:
        raise ValueError(f"{source} is not a paid source")
    if configured_key(config, source) is None:
        raise ValueError(f"{source} has no key configured")
    try:
        state, message = _PROBES[source](config)
    except (httpx.HTTPError, OSError, RuntimeError, ValueError) as exc:
        state, message = "error", str(exc) or type(exc).__name__
    checked_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return save_check(config, source, state, message[:300], checked_at)


def ensure_recent_check(
    config, source: str, *, max_age: timedelta = timedelta(hours=12)
) -> dict | None:
    """Check *source*'s key again when the last verdict is older than *max_age*.

    The daily run calls this before it lets Tushare in: a relay key lapses
    without warning, and a verdict from last week says nothing about today.
    One small request at most, paced like any other.
    """
    if configured_key(config, source) is None:
        return None
    record = last_check(config, source)
    if record is not None:
        try:
            checked = datetime.fromisoformat(str(record.get("checked_at")))
        except ValueError:
            checked = None
        if checked is not None and datetime.now(timezone.utc) - checked < max_age:
            return record
    return check_key(config, source)


# A credential is one printable token: no spaces, quotes or line breaks, so it
# can never break out of the file it is written to or the TOML that names it.
_KEY_SHAPE = re.compile(r"^[\x21-\x7e]{8,512}$")


def save_key(config_path: Path, source: str, key: str) -> dict:
    """Write *key* to the source's key file (0600) and point the config at that file.

    The key itself never enters the config file: ``[sources.<id>].<key>_file``
    names the file, which is what scheduled runs read (launchd does not see a
    shell's exported variables). Returns where it went and what still overrides it.
    """
    from cnequity.ops.catalog import OpsError

    keyed = KEYED_SOURCES.get(source)
    if keyed is None:
        raise ValueError(f"{source} is not a paid source")
    key = key.strip()
    if not _KEY_SHAPE.fullmatch(key):
        raise OpsError(_("The key must be 8–512 printable characters with no spaces."))
    target = keyed.default_key_file()
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(target.parent, 0o700)
    except OSError:
        pass
    tmp = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, (key + "\n").encode("utf-8"))
    finally:
        os.close(fd)
    replace_with_retry(tmp, target)
    _edit_config(config_path, keyed, json.dumps(str(target)))
    return _where(keyed, target)


def remove_key(config_path: Path, source: str) -> dict:
    """Forget the key the dashboard wrote: drop ``<key>_file`` and delete that file.

    A key given some other way (environment variable, inline value, a file the
    user named) is left alone and reported, since the page did not write it.
    """
    keyed = KEYED_SOURCES.get(source)
    if keyed is None:
        raise ValueError(f"{source} is not a paid source")
    target = keyed.default_key_file()
    _edit_config(config_path, keyed, None, only_if=str(target))
    try:
        target.unlink()
    except FileNotFoundError:
        pass
    return _where(keyed, None)


def _where(keyed, target: Path | None) -> dict:
    return {
        "source": keyed.id,
        "key_file": str(target) if target else None,
        # An exported variable wins over any file; say so rather than let the
        # page claim a key it is not using.
        "overridden_by_env": bool(os.environ.get(keyed.env, "").strip()),
        "env": keyed.env,
    }


def _edit_config(
    config_path: Path, keyed, literal: str | None, *, only_if: str | None = None
) -> None:
    """Set (or, with ``literal=None``, remove) ``[sources.<id>].<key>_file`` in place.

    Same discipline as the settings page: an exclusive lock, a backup, a
    reload of the written file, and the original restored if anything fails.
    """
    import shutil

    from cnequity.config import load_config
    from cnequity.config.upgrade import _table_span, tomllib
    from cnequity.file_lock import LockUnavailable, exclusive_lock
    from cnequity.ops.catalog import OpsError
    from cnequity.ops.scheduler import replace_config, state_dir
    from cnequity.ops.settings import _assignment, _backup, _read, _set_assignment

    path = Path(config_path)
    name = f"{keyed.key}_file"
    table = f"sources.{keyed.id}"
    try:
        lock_root = state_dir(load_config(path))
    except (OSError, ValueError) as exc:
        raise OpsError(_("The config file cannot be read.")) from exc
    try:
        with exclusive_lock(lock_root / "control.lock", blocking=False):
            text = _read(path)
            if literal is not None:
                edited = _set_assignment(text, table, name, literal)
            else:
                current = (tomllib.loads(text).get("sources") or {}).get(keyed.id) or {}
                if not current.get(name) or (
                    only_if is not None and Path(str(current[name])).expanduser() != Path(only_if)
                ):
                    return
                lines = text.splitlines(keepends=True)
                span = _table_span(lines, table)
                pattern = _assignment(name)
                edited = text
                if span is not None:
                    start, end = span
                    kept = [
                        line
                        for index, line in enumerate(lines)
                        if not (start < index < end and pattern.match(line))
                    ]
                    edited = "".join(kept)
            if edited == text:
                return
            backup = _backup(path)
            try:
                replace_config(path, edited)
                load_config(path)
            except Exception as exc:
                shutil.copy2(backup, path)
                raise OpsError(
                    _("Writing the config failed; the original file was restored.")
                ) from exc
    except LockUnavailable as exc:
        raise OpsError(_("The config is being changed; try again shortly.")) from exc
