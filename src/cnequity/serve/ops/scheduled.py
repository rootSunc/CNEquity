"""Packaged OS timer entry; no running web server or checkout is required."""

from __future__ import annotations

import argparse
import os
import secrets
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cnequity.config import load_config
from cnequity.fd_limit import raise_open_file_limit
from cnequity.file_lock import LockUnavailable, exclusive_lock
from cnequity.i18n import _, use_language
from cnequity.orchestrator.run_window import CST, is_done, mark_done, parse_run_at, pending_session
from cnequity.serve.ops.catalog import OpsError
from cnequity.serve.ops.records import atomic_write, read_record
from cnequity.serve.ops.runner import OpsService
from cnequity.serve.ops.scheduler import settings, state_dir


def _daily_params(saved: dict, session) -> dict:
    params = {"trade_date": session.isoformat()}
    packs = saved.get("daily_packs") or []
    if packs:
        params["pack"] = list(packs)
    return params


def _stale_params(saved: dict) -> dict | None:
    """Snapshot retry for the saved packs. ``None`` means this tick has nothing to retry."""
    params: dict = {"snapshots_only": True}
    packs = saved.get("daily_packs") or []
    if not packs:
        return params
    from cnequity.research.packs import groups_for

    groups = groups_for(packs)
    if not groups:
        return None
    params["groups"] = groups
    return params


def backup_session(config, saved: dict, now: datetime | None = None):
    local = (now or datetime.now(timezone.utc)).astimezone(CST)
    if local.time().replace(tzinfo=None) < parse_run_at(saved.get("backup_run_at", "23:00")):
        return None
    return None if is_done(config.meta_root, "backup", local.date()) else local.date()


def events_due(config, saved: dict, now: datetime | None = None) -> bool:
    path = state_dir(config) / "events_last.json"
    if not path.exists():
        return True
    last = read_record(path)
    if not isinstance(last, dict) or not isinstance(last.get("at"), str):
        raise OpsError(
            _("The event-stream check record is invalid, so the run interval cannot be judged.")
        )
    if last.get("group") != saved.get("events_group"):
        return True
    stamp = datetime.fromisoformat(last["at"])
    if stamp.tzinfo is None:
        raise OpsError(
            _("The event-stream check time has no time zone, so the run interval cannot be judged.")
        )
    return (now or datetime.now(timezone.utc)) >= stamp + timedelta(
        minutes=saved.get("events_interval_minutes", 60)
    )


#: A job switched on by another job's setting: maintenance follows the daily update.
_SETTING = {"maintain": "daily"}


def claim_session(config, scheduled: dict) -> None:
    """Recheck under both the child's scheduler lock and the control lock."""
    job = scheduled.get("job")
    if job not in {"daily", "maintain", "stale", "backup", "events"}:
        raise OpsError(_("Invalid scheduled task type."))
    with exclusive_lock(state_dir(config) / "control.lock", blocking=False):
        saved = settings(config)
        if not saved.get(_SETTING.get(job, job)):
            raise OpsError(_("The scheduled task is paused, so no fetch started."))
        if job == "events" and scheduled.get("schedule"):
            from cnequity.serve.ops import event_schedules

            spec = event_schedules.find(config, scheduled["schedule"])
            if spec is None or not event_schedules.is_due(spec, state_dir(config)):
                raise OpsError(
                    _(
                        "The event-stream settings or run interval changed, so nothing was fetched twice."
                    )
                )
            event_schedules.record_attempt(spec, state_dir(config))
            return
        if job == "events":
            if (
                saved.get("events_group") != scheduled.get("group")
                or saved.get("events_interval_minutes", 60) != scheduled.get("interval_minutes")
                or not events_due(config, saved)
            ):
                raise OpsError(
                    _(
                        "The event-stream settings or run interval changed, so nothing was fetched twice."
                    )
                )
            atomic_write(
                state_dir(config) / "events_last.json",
                {
                    "at": datetime.now(timezone.utc).isoformat(),
                    "group": saved["events_group"],
                },
            )
            return
        if job == "backup":
            if saved.get("backup_datasets", []) != scheduled.get("datasets") or saved.get(
                "backup_root"
            ) != scheduled.get("root"):
                raise OpsError(
                    _("The backup scope changed before starting; wait for the next check.")
                )
            session = backup_session(config, saved)
        else:
            session = pending_session(config, job)
        if session is None or session.isoformat() != scheduled.get("session"):
            raise OpsError(
                _(
                    "This trading day already ran or is outside the current run window, so nothing was fetched twice."
                )
            )
        mark_done(config.meta_root, job, session)


def _start_event_schedule(config, service, now, report) -> bool:
    """Start the first due ``[[job.events.schedules]]`` entry; True when one started."""
    from cnequity.serve.ops import event_schedules

    for spec in config.event_schedules:
        if not event_schedules.is_due(spec, state_dir(config), now):
            continue
        params = {"group": spec.group} if spec.group else {}
        scheduled = {
            "job": "events",
            "schedule": spec.id,
            "session": (now or datetime.now(timezone.utc)).isoformat(),
        }
        preview = service.preview("events.run", params)
        if not preview["launch_token"]:
            report.update(status="waiting", message=" ".join(preview["blockers"]))
            return True
        record = service.start(
            preview["preview_id"],
            preview["launch_token"],
            [item["id"] for item in preview["acknowledgements"]],
            requested_by="system-scheduler",
            scheduled=scheduled,
        )
        report["jobs"].append(
            {"job": "events", "session": scheduled["session"], "job_id": record["job_id"]}
        )
        report["status"] = "started"
        return True
    return False


def tick(config, *, now: datetime | None = None) -> dict:
    report = {"at": datetime.now(timezone.utc).isoformat(), "status": "idle", "jobs": []}
    directory = state_dir(config)
    try:
        with exclusive_lock(directory / "tick.lock", blocking=False):
            try:
                saved = settings(config)
                service = OpsService(config, storage_busy=lambda: False)
                for job, operation in (
                    ("daily", "daily.full"),
                    # After the day's run; waits on its lock while it is going.
                    ("maintain", "daily.maintain"),
                    ("stale", "daily.stale"),
                    ("backup", "snapshot.create"),
                    ("events", "events.run"),
                ):
                    if not saved.get(_SETTING.get(job, job)):
                        continue
                    if job == "events" and config.event_schedules:
                        # Config schedules replace the single dashboard one.
                        started = _start_event_schedule(config, service, now, report)
                        if started:
                            break
                        continue
                    if job == "events":
                        if not events_due(config, saved, now):
                            continue
                        group = saved.get("events_group")
                        if group not in config.events_groups:
                            raise OpsError(
                                _(
                                    "The configured scheduled event groups no longer exist; set them up again."
                                )
                            )
                        params = {"group": group}
                        scheduled = {
                            "job": job,
                            "session": (now or datetime.now(timezone.utc)).isoformat(),
                            "group": group,
                            "interval_minutes": saved.get("events_interval_minutes", 60),
                        }
                    else:
                        session = (
                            backup_session(config, saved, now)
                            if job == "backup"
                            else pending_session(config, job, now)
                        )
                        if session is None:
                            continue
                        scheduled = {"job": job, "session": session.isoformat()}
                        if job == "backup":
                            params = {
                                "name": f"web-auto-{session.isoformat()}-{secrets.token_hex(4)}",
                                "datasets": saved.get("backup_datasets", []),
                                "snapshot_root": saved.get("backup_root"),
                            }
                            scheduled.update(
                                datasets=params["datasets"], root=params["snapshot_root"]
                            )
                        elif job == "maintain":
                            params = {"notify": True} if sys.platform == "darwin" else {}
                        else:
                            params = (
                                _daily_params(saved, session)
                                if job == "daily"
                                else _stale_params(saved)
                            )
                            if params is None:
                                continue
                    preview = service.preview(operation, params)
                    if not preview["launch_token"]:
                        report.update(status="waiting", message=" ".join(preview["blockers"]))
                        break
                    record = service.start(
                        preview["preview_id"],
                        preview["launch_token"],
                        [item["id"] for item in preview["acknowledgements"]],
                        requested_by="system-scheduler",
                        scheduled=scheduled,
                    )
                    report["jobs"].append(
                        {"job": job, "session": scheduled["session"], "job_id": record["job_id"]}
                    )
                    report["status"] = "started"
                    # Wait until the next tick for the late catch-up; the daily
                    # child may still be running, and owns the same lake slot.
                    break
            except (OpsError, OSError, ValueError) as exc:
                report.update(status="error", message=str(exc))
            atomic_write(directory / "last_tick.json", report)
    except LockUnavailable:
        return {**report, "status": "busy"}
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--scheduler-lock-dir", type=Path)
    args = parser.parse_args()
    # launchd starts this at a 256-descriptor soft limit; the jobs it spawns
    # inherit whatever this process holds.
    raise_open_file_limit()
    if args.scheduler_lock_dir is not None:
        if not args.scheduler_lock_dir.is_absolute():
            parser.error("--scheduler-lock-dir must be absolute")
        os.environ["CNE_SCHEDULER_LOCK_DIR"] = str(args.scheduler_lock_dir)
    try:
        config = load_config(args.config)
    except (OSError, ValueError) as exc:
        print(f"scheduler: cannot load config ({type(exc).__name__})")
        return 1
    # Run in the language the schedule was set up in; a bad settings file is
    # reported by `tick` itself.
    try:
        language = settings(config).get("language")
    except (OpsError, OSError, ValueError):
        language = None
    with use_language(language):
        report = tick(config)
    return int(report["status"] == "error")


if __name__ == "__main__":
    raise SystemExit(main())
