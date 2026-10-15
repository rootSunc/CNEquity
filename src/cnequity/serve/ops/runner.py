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
from pathlib import Path

from cnequity.serve.ops.catalog import OpsError, command_line, config_file, prepared, spec_for
from cnequity.serve.ops.environment import command_environment
from cnequity.serve.ops.job import launch_runs, same_job
from cnequity.serve.ops.plan import describe_backfill
from cnequity.serve.ops.preflight import blockers, hints, occupancy
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
        with self._guard:
            # 配置变更会改变预览的目标和来源开关，旧确认令牌必须失效。
            self._previews.clear()
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
                raise OpsError("这次确认已失效，请重新预览。")
            if len(token) != len(preview.token) or not secrets.compare_digest(token, preview.token):
                raise OpsError("确认凭据不匹配，请重新预览。")
            if preview.config_path != self._identity():
                raise OpsError("配置已经变了，请重新预览。")
            if self.storage_busy() and preview.lock_class != "read":
                # Leave the token unused so the same preview can start once
                # maintenance finishes.
                raise OpsError("存储维护正在执行，请等它结束后再启动。")
            required = {item["id"] for item in preview.acknowledgements}
            if not required.issubset(set(acknowledged)):
                raise OpsError("还没有勾选确认。")
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
        from datetime import datetime, timezone

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
        env["CNE_LAUNCH_ID"] = job_id
        out_handle = open(out, "ab")
        err_handle = open(err, "ab")
        try:
            try:
                subprocess.Popen(
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
                    outcome={"kind": "rejected", "message": f"无法启动任务进程（{exc}）。"},
                )
                atomic_write(path, record)
                raise OpsError(record["outcome"]["message"]) from exc
        finally:
            out_handle.close()
            err_handle.close()
        deadline = time.monotonic() + 5
        current = read_record(path)
        while current.get("state") == "starting" and time.monotonic() < deadline:
            time.sleep(0.05)
            current = read_record(path)
        shown = public_record(self.config, current)
        if shown["state"] == "rejected":
            message = (shown.get("outcome") or {}).get("message") or "任务被拒绝。"
            raise OpsRejected(message)
        shown["runs"] = launch_runs(self.config, job_id, preview.target_run_id)
        return shown

    def list_jobs(self, limit: int = 50) -> list[dict]:
        return [public_record(self.config, record) for record in list_records(self.config)[:limit]]

    def get_job(self, job_id: str) -> dict:
        try:
            path = record_path(self.config, job_id)
        except KeyError as exc:
            raise OpsError("没有这个任务。") from exc
        if not path.exists():
            raise OpsError("没有这个任务。")
        shown = public_record(self.config, read_record(path))
        shown["runs"] = launch_runs(self.config, job_id, shown.get("target_run_id"))
        return shown

    def cancel(self, job_id: str, *, requested_by: str) -> dict:
        shown = self.get_job(job_id)
        if shown["state"] not in {"starting", "running"}:
            raise OpsError("任务已经结束。")
        if (
            IS_WINDOWS
            and shown.get("pid")
            and not same_job(int(shown["pid"]), job_id, shown.get("process_created_at"))
        ):
            raise OpsError("无法核对任务进程身份，未发送终止信号。请检查系统任务管理器。")
        from datetime import datetime, timezone

        cancel = cancel_path(self.config, job_id)
        atomic_write(cancel, {"at": datetime.now(timezone.utc).isoformat(), "by": requested_by})
        threading.Thread(
            target=_escalate,
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
