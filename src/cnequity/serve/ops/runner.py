"""Previews, the one-shot launch token, and the process that runs a job.

The token lives in this process. Restarting serve invalidates every preview,
which is what a 10-minute confirmation is for. The job record does not: it is
the file the child keeps updating after this process is gone.
"""

from __future__ import annotations

import os
import secrets
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from cnequity.i18n import ENV_VAR, _, carry_language, current_language
from cnequity.serve.ops.catalog import OpsError, command_line, config_file, prepared, spec_for
from cnequity.serve.ops.environment import command_environment
from cnequity.serve.ops.job import launch_runs, launch_stats, same_job
from cnequity.serve.ops.plan import describe_backfill
from cnequity.serve.ops.preflight import blockers, config_drift, drift_blocker, hints, occupancy
from cnequity.serve.ops.records import (
    atomic_write,
    cancel_path,
    jobs_dir,
    list_records,
    logs_dir,
    public_record,
    read_record,
    record_path,
    slot_path,
)

PREVIEW_SECONDS = 600
IS_WINDOWS = sys.platform == "win32"


class OpsRejected(OpsError):
    """The child started and then refused the locks. The record says why."""


@dataclass
class Preview:
    preview_id: str
    token: str
    op: str
    params: dict
    argv: list[str]
    command: str
    config_path: str | None
    lock_class: str
    result_kind: str
    confirm: str
    acknowledgements: list[dict[str, str]]
    title: str
    target_run_id: str | None
    expires: float
    snapshot_digest: str | None = None
    used: bool = False


def _spawn_kwargs() -> dict:
    if IS_WINDOWS:
        flags = subprocess.CREATE_NEW_PROCESS_GROUP
        flags |= getattr(subprocess, "CREATE_NO_WINDOW", 0)
        return {"creationflags": flags}
    return {"start_new_session": True}


class OpsService:
    def __init__(self, config, *, storage_busy):
        self.config = config
        self.storage_busy = storage_busy
        self._previews: dict[str, Preview] = {}
        self._guard = threading.Lock()

    def bind(self, config) -> None:
        self.config = config

    def _identity(self) -> str | None:
        path = config_file(self.config)
        return str(path) if path else None

    def home(self) -> dict:
        from cnequity.serve.ops.catalog import describe

        return {
            "operations": describe(self.config, setup=self.config is None),
            "occupancy": occupancy(self.config, storage_busy=self.storage_busy()),
        }

    def preview(self, op_id: str, raw: dict | None) -> dict:
        spec = spec_for(op_id)
        built = prepared(spec, raw, self.config)
        found = blockers(
            self.config,
            spec,
            storage_busy=self.storage_busy(),
            lock_class=built["lock_class"],
        )
        plan = None
        snapshot_digest = None
        if spec.id.startswith("snapshot."):
            from cnequity.serve.ops.backups import describe as describe_snapshot

            try:
                plan, snapshot_digest = describe_snapshot(self.config, spec.id, built["params"])
            except (OSError, ValueError, RuntimeError, KeyError) as exc:
                raise OpsError(str(exc)) from exc
        if spec.id == "backfill.run" and not found:
            try:
                plan = describe_backfill(built["argv"])
            except OpsError as exc:
                found.append(str(exc))
        token = None
        preview_id = None
        if not found:
            preview_id = secrets.token_hex(16)
            token = secrets.token_urlsafe(32)
            preview = Preview(
                preview_id=preview_id,
                token=token,
                op=spec.id,
                params=built["params"],
                argv=built["argv"],
                command=built["command"],
                config_path=built["config_path"],
                lock_class=built["lock_class"],
                result_kind=built["result"],
                confirm=built["confirm"],
                acknowledgements=built["acknowledgements"],
                title=built["title"],
                target_run_id=built["target_run_id"],
                expires=time.monotonic() + PREVIEW_SECONDS,
                snapshot_digest=snapshot_digest,
            )
            with self._guard:
                self._expire()
                self._previews[preview_id] = preview
        return {
            "preview_id": preview_id,
            "launch_token": token,
            "expires_in_seconds": PREVIEW_SECONDS if token else None,
            "command": built["command"],
            "params": built["params"],
            "blockers": found,
            "hints": hints(self.config, spec, built["params"]),
            "acknowledgements": built["acknowledgements"] if not found else [],
            "confirm": built["confirm"],
            "plan": plan,
        }

    def _expire(self) -> None:
        now = time.monotonic()
        stale = [
            key for key, preview in self._previews.items() if preview.expires <= now or preview.used
        ]
        for key in stale:
            del self._previews[key]
        if len(self._previews) > 32:
            for key in list(self._previews)[: len(self._previews) - 32]:
                del self._previews[key]

    def _take(self, preview_id: str, token: str, acknowledged: list[str]) -> Preview:
        with self._guard:
            self._expire()
            preview = self._previews.get(preview_id)
            if preview is None or preview.used or time.monotonic() > preview.expires:
                raise OpsError(_("This confirmation has expired; preview again."))
            if len(token) != len(preview.token) or not secrets.compare_digest(token, preview.token):
                raise OpsError(_("The confirmation token does not match; preview again."))
            if preview.config_path != self._identity():
                raise OpsError(_("The config has changed; preview again."))
            drift = config_drift(self.config)
            if drift:
                # Unused, like the maintenance wait: valid again once the file
                # and the server agree.
                raise OpsError(drift_blocker(drift))
            if self.storage_busy() and preview.lock_class != "read":
                # Leave the token unused so the same preview can start once
                # maintenance finishes.
                raise OpsError(
                    _("Storage maintenance is running; wait for it to finish before starting.")
                )
            required = {item["id"] for item in preview.acknowledgements}
            if not required.issubset(set(acknowledged)):
                raise OpsError(_("The confirmation is not ticked yet."))
            if preview.op.startswith("snapshot."):
                from cnequity.serve.ops.backups import check_manifest

                check_manifest(self.config, preview.params, preview.snapshot_digest)
            preview.used = True
            return preview

    def start(
        self,
        preview_id: str,
        token: str,
        acknowledged: list[str],
        *,
        requested_by: str,
        scheduled: dict | None = None,
    ) -> dict:
        preview = self._take(preview_id, token, acknowledged)
        job_id = secrets.token_hex(16)
        stamp = time.strftime("%Y%m%d-%H%M%S")
        out = logs_dir(self.config) / f"cne-serve-job-{stamp}-{job_id}.out.log"
        err = logs_dir(self.config) / f"cne-serve-job-{stamp}-{job_id}.err.log"
        record = {
            "schema_version": 1,
            "job_id": job_id,
            "op": preview.op,
            "title": preview.title,
            "params": preview.params,
            "argv": preview.argv,
            "command": preview.command or command_line(preview.argv),
            "config_path": preview.config_path,
            "lock_class": preview.lock_class,
            "result_kind": preview.result_kind,
            "target_run_id": preview.target_run_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "requested_by": requested_by,
            "scheduled": scheduled,
            "snapshot_digest": preview.snapshot_digest,
            "state": "starting",
            "started_at": None,
            "finished_at": None,
            "pid": None,
            "process_created_at": None,
            "exit_code": None,
            "outcome": None,
            "result": None,
            "slot_lock": str(slot_path(self.config)),
            "logs": {"out": str(out), "err": str(err)},
        }
        path = jobs_dir(self.config) / f"{job_id}.json"
        atomic_write(path, record)
        env = command_environment()
        # The job speaks the language of the page that started it: its record
        # messages, and the `cne` command it runs.
        env[ENV_VAR] = current_language()
        env["CNE_LAUNCH_ID"] = job_id
        out_handle = open(out, "ab")
        err_handle = open(err, "ab")
        try:
            try:
                child = subprocess.Popen(
                    [sys.executable, "-m", "cnequity.serve.ops.job", str(path)],
                    stdin=subprocess.DEVNULL,
                    stdout=out_handle,
                    stderr=err_handle,
                    env=env,
                    cwd=os.getcwd(),
                    **_spawn_kwargs(),
                )
            except OSError as exc:
                # The child never started, so it cannot write the record.
                record.update(
                    state="rejected",
                    finished_at=datetime.now(timezone.utc).isoformat(),
                    outcome={
                        "kind": "rejected",
                        "message": _("Could not start the task process ({exc}).").format(exc=exc),
                    },
                )
                atomic_write(path, record)
                raise OpsError(record["outcome"]["message"]) from exc
        finally:
            out_handle.close()
            err_handle.close()
        # Reap the job the moment it exits. Unwaited, it lingers as a zombie,
        # which `kill -0` still reports alive; Linux can tell from /proc, but
        # macOS cannot, so a cancelled job stayed "running" in the panel until
        # some unrelated subprocess call happened to trigger Python's cleanup.
        threading.Thread(target=child.wait, name="ops-reap", daemon=True).start()
        deadline = time.monotonic() + 5
        current = read_record(path)
        while current.get("state") == "starting" and time.monotonic() < deadline:
            time.sleep(0.05)
            current = read_record(path)
        shown = public_record(self.config, current)
        if shown["state"] == "rejected":
            message = (shown.get("outcome") or {}).get("message") or _("The task was rejected.")
            raise OpsRejected(message)
        shown["runs"] = launch_runs(self.config, job_id, preview.target_run_id)
        return shown

    def list_jobs(
        self,
        limit: int = 50,
        *,
        since: datetime | None = None,
        ops: tuple[str, ...] = (),
        exclude_ops: tuple[str, ...] = (),
    ) -> list[dict]:
        """Newest jobs first. ``since`` keeps jobs created at or after it."""
        chosen = []
        for record in list_records(self.config):
            if since is not None and not _created_since(record, since):
                break
            op = record.get("op")
            if (ops and op not in ops) or op in exclude_ops:
                continue
            chosen.append(public_record(self.config, record))
            if len(chosen) >= limit:
                break
        _attach_run_stats(self.config, chosen)
        return chosen

    def get_job(self, job_id: str) -> dict:
        try:
            path = record_path(self.config, job_id)
        except KeyError as exc:
            raise OpsError(_("No such task.")) from exc
        if not path.exists():
            raise OpsError(_("No such task."))
        shown = public_record(self.config, read_record(path))
        shown["runs"] = launch_runs(self.config, job_id, shown.get("target_run_id"))
        return shown

    def cancel(self, job_id: str, *, requested_by: str) -> dict:
        shown = self.get_job(job_id)
        if shown["state"] not in {"starting", "running"}:
            raise OpsError(_("The task has already finished."))
        if (
            IS_WINDOWS
            and shown.get("pid")
            and not same_job(int(shown["pid"]), job_id, shown.get("process_created_at"))
        ):
            raise OpsError(
                _(
                    "Could not verify the task process identity, so no stop signal was sent. Check the system task manager."
                )
            )

        cancel = cancel_path(self.config, job_id)
        atomic_write(cancel, {"at": datetime.now(timezone.utc).isoformat(), "by": requested_by})
        threading.Thread(
            target=carry_language(_escalate),
            args=(job_id, record_path(self.config, job_id)),
            name="ops-cancel",
            daemon=True,
        ).start()
        return self.get_job(job_id)


def _escalate(job_id: str, record_file: Path) -> None:
    """SIGINT, then TERM, then KILL. Windows ends the tree immediately."""
    pid = None
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            current = read_record(record_file)
        except PermissionError:
            # Still locked after read_record's retries: keep trying until the
            # deadline rather than silently dropping the user's cancel.
            time.sleep(0.1)
            continue
        except (OSError, ValueError):
            return
        if current.get("state") not in {"starting", "running"}:
            return
        if current.get("pid"):
            pid = int(current["pid"])
            break
        time.sleep(0.1)
    if not pid:
        return
    if IS_WINDOWS:
        from cnequity.windows_process import verified_process

        # Hold the handle through taskkill, closing the check-to-kill race.
        with verified_process(pid, current.get("process_created_at")) as matched:
            if matched:
                subprocess.run(
                    ["taskkill", "/T", "/F", "/PID", str(pid)],
                    capture_output=True,
                    check=False,
                    timeout=10,
                )
        return
    if not same_job(pid, job_id):
        return
    import signal

    _signal(pid, signal.SIGINT)
    if _wait_dead(pid, job_id, 60):
        return
    if same_job(pid, job_id):
        _signal(pid, signal.SIGTERM)
    if _wait_dead(pid, job_id, 30):
        return
    if same_job(pid, job_id):
        _signal(pid, signal.SIGKILL)


def _signal(pid: int, sig: int) -> None:
    try:
        os.killpg(pid, sig)
    except OSError:
        try:
            os.kill(pid, sig)
        except OSError:
            pass


def _wait_dead(pid: int, job_id: str, seconds: float) -> bool:
    from cnequity.orchestrator.scheduler_lock import pid_alive

    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if not pid_alive(pid) or not same_job(pid, job_id):
            return True
        time.sleep(0.2)
    return not pid_alive(pid)


def requested_by(host: str | None, user_agent: str | None) -> str:
    agent = (user_agent or "").replace("\n", " ")[:160]
    return f"{host or '-'} {agent}".strip()


def _created_since(record: dict, since: datetime) -> bool:
    """Whether a job was created at or after ``since``. An undated record is old."""
    raw = record.get("created_at")
    if not raw:
        return False
    try:
        created = datetime.fromisoformat(raw)
    except ValueError:
        return False
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    if since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)
    return created >= since


def _attach_run_stats(config, jobs: list[dict]) -> None:
    """Add ``rows_written`` and ``failed_datasets`` from each job's manifest runs."""

    def run_ids(job: dict) -> list[str]:
        result = job.get("result")
        ids = result.get("run_ids") if isinstance(result, dict) else None
        return [str(item) for item in ids or [] if item]

    stats = launch_stats(config, [run_id for job in jobs for run_id in run_ids(job)])
    for job in jobs:
        ids = run_ids(job)
        if not ids:
            continue
        found = [stats[run_id] for run_id in ids if run_id in stats]
        job["rows_written"] = sum(item["rows_written"] for item in found) if found else None
        failed: dict[str, set[str]] = {}
        for item in found:
            for batch in item["failed"]:
                codes = failed.setdefault(batch["dataset"], set())
                if batch["reason_code"]:
                    codes.add(batch["reason_code"])
        job["failed_datasets"] = [
            {"dataset": dataset, "reason_codes": sorted(codes)}
            for dataset, codes in sorted(failed.items())
        ]
