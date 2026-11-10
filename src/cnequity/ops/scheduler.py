"""Preview and apply current-user scheduling, with persistent fail-closed state."""

from __future__ import annotations

import copy
import os
import re
import secrets
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from cnequity.config import load_config
from cnequity.config.upgrade import _table_span, tomllib
from cnequity.file_lock import LockUnavailable, exclusive_lock
from cnequity.i18n import _, current_language, normalize_language
from cnequity.ops.catalog import OpsError, config_file
from cnequity.ops.records import atomic_write, read_record
from cnequity.ops.scheduler_backend import SchedulerBackend
from cnequity.orchestrator.scheduler_lock import lock_directory
from cnequity.storage.atomic import replace_with_retry


def state_dir(config) -> Path:
    return Path(config.meta_root) / "state" / "web_scheduler"


def settings(config) -> dict:
    path = state_dir(config) / "settings.json"
    if not path.exists():
        return {"daily": False, "stale": False}
    value = read_record(path)
    if not isinstance(value, dict) or any(
        key in value and not isinstance(value[key], bool)
        for key in ("daily", "stale", "backup", "events")
    ):
        raise OpsError(_("The scheduled task settings are invalid; check settings.json."))
    if value.get("config_path") != str(config_file(config)):
        raise OpsError(
            _(
                "This lake's web schedule is bound to another config; manage it with the original config."
            )
        )
    interval = value.get("events_interval_minutes", 60)
    if isinstance(interval, bool) or not isinstance(interval, int) or not 1 <= interval <= 1440:
        raise OpsError(_("The event-stream run interval is invalid; set it again."))
    if value.get("backup") and (
        not isinstance(value.get("backup_run_at"), str)
        or not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", value["backup_run_at"])
        or not isinstance(value.get("backup_root"), str)
        or not Path(value["backup_root"]).is_absolute()
        or not isinstance(value.get("backup_datasets"), list)
        or not value["backup_datasets"]
        or not all(isinstance(item, str) for item in value["backup_datasets"])
    ):
        raise OpsError(_("The automatic backup settings are invalid; set them again."))
    if (
        value.get("events")
        and not getattr(config, "event_schedules", None)
        and not isinstance(value.get("events_group"), str)
    ):
        raise OpsError(_("The scheduled event groups are invalid; set them again."))
    language = value.get("language")
    if language is not None and normalize_language(language) is None:
        raise OpsError(_("The scheduled task settings are invalid; check settings.json."))
    packs = value.get("daily_packs", [])
    if not isinstance(packs, list) or not all(isinstance(item, str) for item in packs):
        raise OpsError(_("The research pack settings are invalid; set them again."))
    if packs:
        from cnequity.research.packs import normalize_packs

        try:
            normalize_packs(packs)
        except ValueError as exc:
            raise OpsError(str(exc)) from exc
    return value


def _settings_bytes(config) -> bytes:
    path = state_dir(config) / "settings.json"
    return path.read_bytes() if path.exists() else b""


def edit_times(text: str, daily: str, stale: str) -> str:
    """Preserve comments/other values and verify the complete TOML semantic diff."""
    for value in (daily, stale):
        if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", value):
            raise OpsError(_("The run time must be HH:MM, Beijing time."))
    original = tomllib.loads(text)
    expected = copy.deepcopy(original)
    for name, value in (("daily", daily), ("stale", stale)):
        expected.setdefault("job", {}).setdefault(name, {})["run_at"] = value
        lines = text.splitlines(keepends=True)
        span = _table_span(lines, f"job.{name}")
        if span is None:
            text = text.rstrip() + f'\n\n[job.{name}]\nrun_at = "{value}"\n'
            continue
        start, end = span
        for index in range(start + 1, end):
            match = re.match(
                r"""^(\s*(?:run_at|"run_at"|'run_at')\s*=\s*)(["'])([^"']*)\2(\s*(?:#.*)?)(\r?\n)?$""",
                lines[index],
            )
            if match:
                lines[index] = f'{match[1]}"{value}"{match[4]}{match[5] or chr(10)}'
                break
        else:
            lines.insert(start + 1, f'run_at = "{value}"\n')
        text = "".join(lines)
    try:
        parsed = tomllib.loads(text)
    except ValueError as exc:
        raise OpsError(
            _(
                "Cannot change this config's times automatically; put run_at inside the [job.daily] and [job.stale] tables."
            )
        ) from exc
    if parsed != expected:
        raise OpsError(
            _(
                "The config structure does not support changing times automatically; use standard TOML tables."
            )
        )
    return text


def replace_config(path: Path, text: str) -> None:
    # Keep the existing file's permissions, including private credentials.
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix=".cne-schedule-")
    tmp = Path(temporary)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
        tmp.chmod(path.stat().st_mode)
        replace_with_retry(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


class ScheduleService:
    def __init__(self, config):
        path = config_file(config)
        if path is None:
            raise OpsError(_("Generate the config before setting up scheduled tasks."))
        self.config = config
        self.path = path
        saved = settings(config)
        self.backend = SchedulerBackend(
            path,
            Path(config.meta_root),
            working_directory=saved.get("working_directory"),
            scheduler_lock_dir=str(lock_directory(config)),
        )
        self.previews: dict[str, dict] = {}

    def _current(self):
        config = load_config(self.path)
        if Path(config.meta_root) != Path(self.config.meta_root):
            raise OpsError(
                _(
                    "The configured data directory has changed; restart serve before setting up scheduled tasks."
                )
            )
        return config

    def home(self) -> dict:
        from cnequity.ops.backups import stored_datasets

        # Show what the timer will actually load, including external edits.
        config = self._current()
        state = settings(config)
        native = self.backend.observe()
        heartbeat = state_dir(config) / "last_tick.json"
        try:
            last_tick = read_record(heartbeat) if heartbeat.exists() else None
        except (OSError, ValueError):
            last_tick = None
        active = bool(native["active"] and native["matches"])
        enabled = active and any(state.get(key) for key in ("daily", "stale", "backup", "events"))
        timer_health = "paused"
        if enabled:
            stamp = (last_tick or {}).get("at") or state.get("updated_at")
            try:
                age = (datetime.now(timezone.utc) - datetime.fromisoformat(stamp)).total_seconds()
            except (TypeError, ValueError):
                age = float("inf")
            timer_health = "stale" if age > 20 * 60 else (last_tick or {}).get("status", "not_seen")
        return {
            "backend": self.backend.kind,
            "native": native,
            "daily": bool(state.get("daily")),
            "stale": bool(state.get("stale")),
            "daily_run_at": config.daily_run_at,
            "stale_run_at": config.stale_run_at,
            "backup": bool(state.get("backup")),
            "backup_run_at": state.get("backup_run_at", "23:00"),
            "backup_datasets": state.get("backup_datasets", []),
            "backup_choices": stored_datasets(config),
            "backup_root": state.get("backup_root") or str(Path(config.meta_root) / "snapshots"),
            "events": bool(state.get("events")),
            "events_group": state.get("events_group"),
            "events_interval_minutes": state.get("events_interval_minutes", 60),
            "events_groups": sorted(config.events_groups),
            # [[job.events.schedules]] from the config; when present they replace
            # the single group and interval above, and the page shows them read-only.
            "event_schedules": [
                {
                    "group": spec.group,
                    "every_minutes": spec.every_minutes,
                    "jitter_seconds": spec.jitter_seconds,
                    "quiet_hours": spec.quiet_hours,
                    "quiet_every_minutes": spec.quiet_every_minutes,
                }
                for spec in config.event_schedules
            ],
            "daily_packs": list(state.get("daily_packs") or []),
            "enabled": enabled,
            "timer_health": timer_health,
            "last_tick": last_tick,
            "warning": state.get("warning"),
            "note": (
                _(
                    "Checked once a minute. The daily update and catch-up each try once per trading day; the catch-up only handles snapshots and waits for the daily update to have run. Backups try once per calendar day once their time comes, and event streams run at their own frequency, weekends and holidays included. When a task is busy it waits and the next check decides again. It keeps working after the web page or serve is closed. Windows and macOS need the current user logged in; Linux needs the cron service running. Nothing runs during sleep, and it decides again after waking; the daily update and catch-up go no later than 09:15 on the next trading day, backups only make up the same day, and event streams do not accumulate missed runs. Only tasks created from the web are managed here; check existing scripts and other system tasks first to avoid double scheduling."
                )
            ),
            "note_en": (
                "Checked once a minute. The daily update and the catch-up are each tried once per trading "
                "day; the catch-up handles snapshots only and waits until the daily update has run. A "
                "backup is tried once per calendar day after its time; the event stream runs at its own "
                "interval, weekends and holidays included. A busy lake means wait for the next check. "
                "Schedules keep working after you close the page or stop serve. Windows and macOS need "
                "the current user logged in; Linux needs the cron service running. Nothing runs while "
                "the machine sleeps; on wake the check decides again. The daily update and catch-up are "
                "retried until 09:15 on the next trading day, a backup only on its own day, and missed "
                "event-stream runs do not pile up. Only schedules created on this page are managed "
                "here; check existing scripts and other system jobs first to avoid running twice."
            ),
        }

    def preview(
        self,
        *,
        daily: bool,
        stale: bool,
        daily_run_at: str,
        stale_run_at: str,
        backup: bool = False,
        backup_run_at: str = "23:00",
        backup_datasets: list[str] | None = None,
        backup_root: str | None = None,
        events: bool = False,
        events_group: str | None = None,
        events_interval_minutes: int = 60,
        daily_packs: list[str] | None = None,
    ) -> dict:
        if stale and not daily:
            raise OpsError(_("Enabling the closing catch-up needs the daily update enabled too."))
        native = self.backend.observe()
        from cnequity.ops.catalog import normalize, spec_for

        config = self._current()
        if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", backup_run_at):
            raise OpsError(_("The backup time must be HH:MM, Beijing time."))
        if (
            isinstance(events_interval_minutes, bool)
            or not isinstance(events_interval_minutes, int)
            or not 1 <= events_interval_minutes <= 1440
        ):
            raise OpsError(_("The event-stream interval must be 1–1440 minutes."))
        if events and not config.event_schedules and events_group not in config.events_groups:
            raise OpsError(_("Select valid event groups."))
        datasets = sorted(set(backup_datasets or []))
        backup_root = backup_root or str(Path(config.meta_root) / "snapshots")
        if backup:
            normalize(
                spec_for("snapshot.create"),
                {"name": "timer-preview", "datasets": datasets, "snapshot_root": backup_root},
                config,
            )
        elif backup_root:
            # Validate a saved directory even if automatic backup is disabled.
            from cnequity.storage.snapshots import _reject_symlink_path

            root = Path(backup_root).expanduser()
            if not root.is_absolute():
                raise OpsError(_("The backup directory must be an absolute path."))
            _reject_symlink_path(root, label="snapshot root")
            if root.exists() and not root.is_dir():
                raise OpsError(_("The backup directory is not a directory."))
        packs = list(daily_packs or [])
        if packs:
            from cnequity.research.packs import normalize_packs

            try:
                packs = list(normalize_packs(packs))
            except ValueError as exc:
                raise OpsError(str(exc)) from exc
        options = {
            "backup": backup,
            "backup_run_at": backup_run_at,
            "backup_datasets": datasets,
            "backup_root": str(Path(backup_root).expanduser())
            if backup_root
            else str(Path(config.meta_root) / "snapshots"),
            "events": events,
            "events_group": events_group,
            "events_interval_minutes": events_interval_minutes,
            "daily_packs": packs,
        }
        enabled = daily or stale or backup or events
        if enabled and native["error"]:
            raise OpsError(native["error"])
        if enabled and native["installed"] and not native.get("owned", native["matches"]):
            raise OpsError(
                _(
                    "A system task with the same name was modified; check it in the system scheduler first."
                )
            )
        raw = self.path.read_bytes()
        text = edit_times(raw.decode("utf-8"), daily_run_at, stale_run_at)
        artifact = self.backend.render() if enabled else ""
        # Binding is checked even on a paused schedule.
        settings(self._current())
        token = secrets.token_urlsafe(32)
        self.previews = {
            key: value
            for key, value in self.previews.items()
            if value["expires"] > time.monotonic()
        }
        if len(self.previews) >= 16:
            self.previews.pop(next(iter(self.previews)))
        self.previews[token] = {
            "raw": raw,
            "text": text,
            "artifact": artifact,
            "native": native["fingerprint"],
            "state": _settings_bytes(self.config),
            "daily": daily,
            "stale": stale,
            "options": options,
            "enabled": enabled,
            "expires": time.monotonic() + 600,
        }
        return {
            "token": token,
            "backend": self.backend.kind,
            "action": _("Enable / update") if enabled else _("Pause"),
            "action_en": "Enable / update" if enabled else "Pause",
            "daily": daily,
            "stale": stale,
            "daily_run_at": daily_run_at,
            "stale_run_at": stale_run_at,
            **options,
            "artifact": artifact,
            "confirmation": (
                _(
                    "The system will fetch or copy backups automatically for the selected scope (backups keep using disk and are not deleted automatically); it keeps running after the web service is closed."
                )
                + (
                    _(" The daily update only runs the schedule groups of research packs ")
                    + _(", ").join(options["daily_packs"])
                    + _(
                        ", without event streams. A snapshot missed for a day cannot be filled back by date."
                    )
                    if options.get("daily_packs")
                    else ""
                )
            )
            if enabled
            else _("Stops future scheduled triggers; tasks already running continue."),
            "confirmation_en": (
                "The system will fetch or copy backups on schedule for the selected scope (backups keep "
                "using disk space and are never deleted automatically); this continues after the web "
                "service stops."
                + (
                    " The daily update runs only the schedule groups of research packs "
                    + ", ".join(options["daily_packs"])
                    + ", without the event stream. A missed snapshot day cannot be filled later."
                    if options.get("daily_packs")
                    else ""
                )
            )
            if enabled
            else "Stops future scheduled triggers; a job already running continues.",
        }

    def apply(self, token: str, *, acknowledged: bool, requested_by: str) -> dict:
        if not acknowledged:
            raise OpsError(_("First confirm the scope the scheduled tasks will run."))
        try:
            with exclusive_lock(state_dir(self.config) / "control.lock", blocking=False):
                plan = self.previews.pop(token, None)
                if plan is None or plan["expires"] <= time.monotonic():
                    raise OpsError(_("The schedule settings preview has expired; preview again."))
                if (
                    self.path.read_bytes() != plan["raw"]
                    or self.backend.observe()["fingerprint"] != plan["native"]
                    or _settings_bytes(self.config) != plan["state"]
                ):
                    raise OpsError(_("The config or system tasks have changed; preview again."))
                state = {
                    "schema_version": 1,
                    "config_path": str(self.path),
                    "daily": False,
                    "stale": False,
                    "requested_by": requested_by,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "working_directory": self.backend.working_directory,
                    # A launchd, cron or Task Scheduler job has no LANG of its
                    # own, so the language it reports in is the installer's.
                    "language": current_language(),
                    **plan["options"],
                    "backup": False,
                    "events": False,
                }
                destination = state_dir(self.config) / "settings.json"
                # Fail closed before unregistering/reloading a timer. A failed
                # OS command must never leave background ingestion enabled.
                atomic_write(destination, state)
                if plan["text"].encode() != plan["raw"]:
                    backup = self.path.with_name(self.path.name + ".schedule.bak")
                    try:
                        descriptor = os.open(
                            backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL, self.path.stat().st_mode
                        )
                    except FileExistsError:
                        pass
                    else:
                        with os.fdopen(descriptor, "wb") as handle:
                            handle.write(plan["raw"])
                    replace_config(self.path, plan["text"])
                try:
                    self.backend.apply(plan["enabled"], plan["artifact"])
                    native = self.backend.observe()
                    if plan["enabled"] and not (native["active"] and native["matches"]):
                        raise OpsError(
                            _("The system task was registered but could not be confirmed active.")
                        )
                except OpsError as exc:
                    if plan["enabled"]:
                        raise OpsError(
                            _(
                                "Scheduled runs stay paused; the time settings were saved. {exc}"
                            ).format(exc=exc)
                        ) from exc
                    # The worker checks this state even if the OS task cannot
                    # be removed (permission change, offline scheduler, drift).
                    state["warning"] = _(
                        "Automatic fetching is paused, but the system trigger was not removed: {exc}"
                    ).format(exc=exc)
                state.update(daily=plan["daily"], stale=plan["stale"], **plan["options"])
                atomic_write(destination, state)
        except LockUnavailable as exc:
            raise OpsError(
                _("Another page is changing the schedule settings; try again shortly.")
            ) from exc
        return self.home()
