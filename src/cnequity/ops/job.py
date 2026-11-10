"""One dashboard operation, in a process that outlives ``cne serve``.

The process holds the panel slot itself, so the lock disappears when the
process does — including when it is killed. It then calls the same Click
command a terminal would, with the argv the preview built. ``CNE_OPS_HANDLER``
replaces that call in tests; the argv still comes from the record the serve
process wrote, so the variable is not a way to run an arbitrary command.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import traceback
from contextlib import ExitStack, closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cnequity.file_lock import LockUnavailable, exclusive_lock
from cnequity.i18n import _
from cnequity.ops.preflight import engine_blockers, mark_due_daily_session, scheduler_names
from cnequity.ops.records import read_record, update_record
from cnequity.orchestrator.scheduler_lock import SchedulerLockError, lock_directory, scheduler_lock

IS_WINDOWS = sys.platform == "win32"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_config(record: dict):
    path = record.get("config_path")
    if not path:
        return None
    from cnequity.config import load_config

    return load_config(path)


def _reject(path: Path, message: str) -> None:
    update_record(
        path,
        state="rejected",
        finished_at=_now(),
        outcome={"kind": "rejected", "message": message},
    )


def _finish(path: Path, *, state: str, message: str, exit_code: int | None, result: dict) -> None:
    update_record(
        path,
        state=state,
        finished_at=_now(),
        exit_code=exit_code,
        outcome={"kind": state, "message": message},
        result=result,
    )


def command_line_of(pid: int) -> str:
    if IS_WINDOWS:
        return ""
    proc_cmd = Path(f"/proc/{pid}/cmdline")
    if proc_cmd.exists():
        return proc_cmd.read_bytes().replace(b"\x00", b" ").decode("utf-8", "replace")
    import subprocess

    completed = subprocess.run(
        ["ps", "-p", str(pid), "-o", "command="],
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.stdout or ""


def same_job(pid: int, job_id: str, process_created_at: str | None = None) -> bool:
    """Whether *pid* is still the process started for *job_id*.

    The record path is on the command line and contains the id. A recycled pid
    must not receive the cancel signal meant for the old job. Windows matches
    the exact process creation FILETIME saved by the child before it runs.
    """
    from cnequity.orchestrator.scheduler_lock import pid_alive

    if not pid_alive(pid):
        return False
    if IS_WINDOWS:
        from cnequity.windows_process import verified_process

        with verified_process(pid, process_created_at) as matched:
            return matched
    try:
        return job_id in command_line_of(pid)
    except OSError:
        # It may exit between the liveness check and reading /proc or ps.
        return False


def _map_status(status: str) -> str:
    if status in {"failed", "error"}:
        return "failed"
    if status in {"warning", "degraded"}:
        return "partial"
    if status == "success":
        return "succeeded"
    if status == "nothing_stale" or status.startswith("skipped"):
        return "skipped"
    return "failed"


def aggregate(statuses: list[str]) -> str | None:
    if not statuses:
        return None
    mapped = [_map_status(status) for status in statuses]
    if "failed" in mapped:
        return "failed"
    if "partial" in mapped:
        return "partial"
    if "succeeded" in mapped:
        return "succeeded"
    if "skipped" in mapped:
        return "skipped"
    return "failed"


def last_json(text: str) -> dict | None:
    decoder = json.JSONDecoder()
    found = None
    index = 0
    while True:
        start = text.find("{", index)
        if start < 0:
            return found
        try:
            value, end = decoder.raw_decode(text, start)
        except json.JSONDecodeError:
            index = start + 1
            continue
        if isinstance(value, dict):
            found = value
        index = end


def _stdout_state(payload: dict) -> str | None:
    status = payload.get("status")
    if isinstance(status, str):
        return _map_status(status)
    statuses: list[str] = []
    groups = payload.get("groups")
    if isinstance(groups, list):
        statuses.extend(
            item["status"] for item in groups if isinstance(item, dict) and item.get("status")
        )
    events = payload.get("events")
    if isinstance(events, dict) and events.get("status"):
        statuses.append(str(events["status"]))
    return aggregate(statuses)


def launch_runs(config, launch_id: str, target_run_id: str | None) -> list[dict]:
    if config is None or not Path(config.manifest_path).exists():
        return []
    import sqlite3

    query = (
        "SELECT run_id, job_name, status, started_at, finished_at, rows_written, error_message "
        "FROM ingestion_runs WHERE json_extract(metadata_json, '$.launch.id') = ?"
    )
    try:
        with closing(sqlite3.connect(f"file:{config.manifest_path}?mode=ro", uri=True)) as conn:
            conn.row_factory = sqlite3.Row
            rows = [dict(row) for row in conn.execute(query, (launch_id,))]
            if target_run_id and all(row["run_id"] != target_run_id for row in rows):
                target = conn.execute(
                    "SELECT run_id, job_name, status, started_at, finished_at, "
                    "rows_written, error_message FROM ingestion_runs WHERE run_id = ?",
                    (target_run_id,),
                ).fetchone()
                if target is not None:
                    rows.append(dict(target))
    except sqlite3.Error:
        return []
    return rows


def launch_stats(config, run_ids: list[str]) -> dict[str, dict]:
    """Rows written and still-failed batches of these runs, keyed by run id.

    One read of the manifest for a whole job list. A failed batch keeps its
    dataset and reason code, so a job whose result names no fallback (a stale
    catch-up, say) can still say what it could not fetch.
    """
    if config is None or not run_ids or not Path(config.manifest_path).exists():
        return {}
    import sqlite3

    stats: dict[str, dict] = {}
    unique = list(dict.fromkeys(run_ids))
    try:
        with closing(sqlite3.connect(f"file:{config.manifest_path}?mode=ro", uri=True)) as conn:
            for start in range(0, len(unique), 500):
                chunk = unique[start : start + 500]
                marks = ",".join("?" * len(chunk))
                for run_id, rows in conn.execute(
                    f"SELECT run_id, rows_written FROM ingestion_runs WHERE run_id IN ({marks})",
                    chunk,
                ):
                    stats.setdefault(run_id, {"rows_written": 0, "failed": []})
                    stats[run_id]["rows_written"] = int(rows or 0)
                for run_id, dataset, reason in conn.execute(
                    f"SELECT run_id, dataset, reason_code FROM ingestion_batches "
                    f"WHERE run_id IN ({marks}) AND status = 'failed'",
                    chunk,
                ):
                    entry = stats.setdefault(run_id, {"rows_written": 0, "failed": []})
                    entry["failed"].append({"dataset": dataset, "reason_code": reason})
    except sqlite3.Error:
        return {}
    return stats


def classify(result_kind: str, exit_code: int, runs: list[dict], stdout: str) -> tuple[str, str]:
    """``(state, message)`` after a command returned. Exceptions are handled earlier."""
    if result_kind == "gate":
        if exit_code == 0:
            return "complete", _("The check passed.")
        if exit_code == 1:
            return "findings", _(
                "The check did not pass. These are findings, not a failed command."
            )
        return "failed", _("The check exited with code {exit_code}.").format(exit_code=exit_code)
    if result_kind == "run_json":
        state = aggregate([str(run["status"]) for run in runs if run.get("status")])
        if state is None:
            payload = last_json(stdout)
            state = _stdout_state(payload) if payload else None
        if state is None:
            state = "complete" if exit_code == 0 else "failed"
        if state == "failed" and exit_code == 0:
            # An init whose bars published but which still owes a non-blocking
            # step (`lake_usable`) is a partial delivery, not a failed build.
            payload = last_json(stdout)
            if payload and payload.get("lake_usable"):
                state = "partial"
        message = {
            "succeeded": _("Succeeded."),
            "partial": _("Partly delivered; gaps or degradation remain."),
            "failed": _("Failed."),
            "skipped": _("Nothing to run, or the day is not a trading day."),
            "complete": _("Completed."),
        }[state]
        return state, message
    if exit_code == 0:
        return "complete", _("Completed.")
    return "failed", _("Exit code {exit_code}.").format(exit_code=exit_code)


def _invoke(argv: list[str]) -> int:
    handler = os.environ.get("CNE_OPS_HANDLER")
    if handler:
        module_name, func_name = handler.rsplit(":", 1)
        function = getattr(importlib.import_module(module_name), func_name)
        return int(function(argv) or 0)
    import click

    from cnequity.cli.main import cli

    try:
        cli.main(args=argv, prog_name="cne", standalone_mode=False)
    except click.exceptions.Exit as exc:
        return int(exc.exit_code or 0)
    return 0


def _read_log(path: str | None, limit: int = 200_000) -> str:
    if not path:
        return ""
    file = Path(path)
    if not file.exists():
        return ""
    data = file.read_bytes()
    if len(data) > limit:
        data = data[-limit:]
    return data.decode("utf-8", "replace")


def _result(state: str, runs: list[dict], stdout: str) -> dict[str, Any]:
    payload = last_json(stdout)
    return {
        "status": state,
        "run_ids": [run["run_id"] for run in runs],
        "summary": payload if payload is not None else stdout[-4000:],
    }


def _run(argv: list[str]) -> tuple[int | None, str | None, str]:
    """``(exit_code, forced_state, message)``. A forced state skips classification."""
    import click

    try:
        return _invoke(argv), None, ""
    except (KeyboardInterrupt, click.Abort):
        return None, "interrupted", _("Cancelled.")
    except click.ClickException as exc:
        exc.show()
        return None, "error", exc.format_message()
    except SystemExit as exc:
        code = exc.code
        if code is None or code == 0:
            return 0, None, ""
        if isinstance(code, int):
            return code, None, ""
        return 1, "error", str(code)
    except Exception as exc:  # noqa: BLE001 — the record has to outlive the traceback
        traceback.print_exc()
        return None, "error", f"{type(exc).__name__}: {exc}"


def execute(path: Path) -> None:
    record = read_record(path)
    os.environ["CNE_LAUNCH_ID"] = str(record["job_id"])
    slot = Path(record["slot_lock"])
    if slot.name != "serve_ops.lock":
        _reject(path, _("The lock path in the task record is invalid."))
        return
    try:
        config = _load_config(record)
    except Exception as exc:  # noqa: BLE001 — a bad config must become a record
        _finish(
            path,
            state="error",
            message=_("Could not load the config: {name}").format(name=type(exc).__name__),
            exit_code=None,
            result={},
        )
        return
    print(_("Running: {value}").format(value=record.get("command")), file=sys.stderr, flush=True)
    with ExitStack() as stack:
        try:
            stack.enter_context(exclusive_lock(slot, blocking=False))
        except LockUnavailable:
            _reject(path, _("The dashboard is already running another task."))
            return
        process_created_at = None
        if IS_WINDOWS:
            from cnequity.windows_process import creation_time

            process_created_at = creation_time(os.getpid())
            if process_created_at is None:
                _reject(path, _("Could not verify the task process identity, so it did not start."))
                return
        update_record(
            path,
            state="running",
            pid=os.getpid(),
            process_created_at=process_created_at,
            started_at=_now(),
        )
        names = scheduler_names(record["lock_class"])
        if names:
            if config is None:
                _reject(path, _("No config, so the schedule lock cannot be taken."))
                return
            try:
                for name in names:
                    stack.enter_context(scheduler_lock(lock_directory(config), name))
            except SchedulerLockError as exc:
                _reject(path, str(exc))
                return
        blocked = engine_blockers(config, record["lock_class"])
        if blocked:
            _reject(path, " ".join(blocked))
            return
        if path.with_name(f"{record['job_id']}.cancel").exists():
            _finish(
                path,
                state="interrupted",
                message=_("Cancelled."),
                exit_code=None,
                result={"status": "interrupted", "run_ids": [], "summary": None},
            )
            return
        if record["op"].startswith("snapshot."):
            from cnequity.ops.backups import check_manifest, create_root, restore_target

            try:
                check_manifest(config, record["params"], record.get("snapshot_digest"))
                if record["op"] == "snapshot.create":
                    create_root(config, record["params"].get("snapshot_root"))
                if record["op"] == "snapshot.restore":
                    restore_target(
                        config, record["params"]["target"], record["params"].get("snapshot_root")
                    )
            except (OSError, ValueError, RuntimeError) as exc:
                _reject(path, str(exc))
                return
        if record.get("scheduled"):
            from cnequity.ops.catalog import OpsError
            from cnequity.ops.scheduled import claim_session

            try:
                claim_session(config, record["scheduled"])
            except (OpsError, LockUnavailable, OSError, ValueError) as exc:
                _reject(path, str(exc))
                return
        elif config is not None:
            mark_due_daily_session(config, record["op"], record.get("params") or {})
        exit_code, forced, message = _run(record["argv"])
        runs = launch_runs(config, record["job_id"], record.get("target_run_id"))
        stdout = _read_log((record.get("logs") or {}).get("out"))
        if forced is None:
            assert exit_code is not None
            state, message = classify(record["result_kind"], exit_code, runs, stdout)
        else:
            state = forced
        # Write the ending before the locks drop. A reader that sees the lock
        # gone and the record still "running" would report the job as killed.
        _finish(
            path,
            state=state,
            message=message,
            exit_code=exit_code,
            result=_result(state, runs, stdout),
        )


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 1:
        print("usage: python -m cnequity.ops.job RECORD", file=sys.stderr)
        return 2
    path = Path(args[0])
    try:
        execute(path)
    except Exception:
        traceback.print_exc()
        try:
            _finish(
                path,
                state="error",
                message=_("The task process exited abnormally."),
                exit_code=1,
                result={},
            )
        except Exception:
            pass
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
