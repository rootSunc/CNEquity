"""HTTP for the operations page. GET never starts a command."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field
from starlette.concurrency import run_in_threadpool

from cnequity.serve.guard import LOOPBACK_HOSTS, check_browser, client_host, ops_enabled
from cnequity.serve.ops.catalog import OpsError, UnknownOp
from cnequity.serve.ops.runner import OpsRejected, OpsService, requested_by

_FRAME = 64 * 1024


class PreviewBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    op: str
    params: dict[str, Any] = Field(default_factory=dict)


class StartBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    preview_id: str
    launch_token: str
    acknowledged: list[str] = Field(default_factory=list)


class SetupBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    data_root: str


class ScheduleBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    daily: bool
    stale: bool
    daily_run_at: str
    stale_run_at: str
    backup: bool = False
    backup_run_at: str = "23:00"
    backup_datasets: list[str] = Field(default_factory=list)
    events: bool = False
    events_group: str | None = None
    events_interval_minutes: int = Field(default=60, ge=1, le=1440)
    backup_root: str | None = None
    daily_packs: list[str] = Field(default_factory=list)


class ScheduleApplyBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    token: str
    acknowledged: bool = False


class SettingsBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    values: dict[str, Any]


class SettingsApplyBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    token: str
    acknowledged: bool = False


def mode_payload(request: Request) -> dict:
    app = request.app.state
    host = client_host(request)
    if app.read_only:
        label = "只读"
    elif app.setup:
        label = "首次配置"
    elif host not in LOOPBACK_HOSTS and not app.allow_remote_ops:
        label = "远程浏览"
    else:
        label = "可发起取数"
    return {
        "setup": bool(app.setup),
        "read_only": bool(app.read_only),
        "allow_remote_ops": bool(app.allow_remote_ops),
        "ops_enabled": ops_enabled(request),
        "label": label,
        "suggested_data_root": (str(Path("./data/cnequity").resolve()) if app.setup else None),
        "config_path": str(app.config_path) if app.config_path else None,
        "data_root": str(app.config.data_root) if app.config is not None else None,
        "config_notice": getattr(getattr(app, "config_runtime", None), "notice", None),
    }


def _service(request: Request) -> OpsService:
    return request.app.state.ops


def _settings_service(request: Request):
    from cnequity.serve.ops.settings import SettingsService

    state = request.app.state
    config = state.config
    if config is None:
        raise OpsError("先完成首次配置，再修改取数设置。")
    service = getattr(state, "settings_service", None)
    if service is None or service.config is not config:
        service = SettingsService(config)
        state.settings_service = service
    return service


def _schedule_service(request: Request):
    from cnequity.serve.ops.scheduler import ScheduleService

    state = request.app.state
    config = state.config
    if config is None:
        raise OpsError("先完成首次配置，再设置定时任务。")
    service = getattr(state, "schedule_service", None)
    if service is None or service.config is not config:
        service = ScheduleService(config)
        state.schedule_service = service
    return service


def _fail(exc: OpsError) -> HTTPException:
    if isinstance(exc, UnknownOp):
        return HTTPException(404, str(exc))
    if isinstance(exc, OpsRejected):
        return HTTPException(409, str(exc))
    return HTTPException(422, str(exc))


def accept_data_root(text: str) -> tuple[Path, str | None]:
    path = Path(text).expanduser()
    if not path.is_absolute():
        raise OpsError("数据目录必须是绝对路径。")
    try:
        resolved = path.resolve()
    except OSError as exc:
        raise OpsError(f"无法解析数据目录（{exc}）。") from exc
    if resolved.exists() and not resolved.is_dir():
        raise OpsError("数据目录不能是已有文件。")
    parent = resolved.parent
    while not parent.exists():
        parent = parent.parent
    if not parent.is_dir():
        raise OpsError("数据目录的上级路径不是目录。")
    if not os.access(resolved if resolved.exists() else parent, os.W_OK):
        raise OpsError("数据目录或它的上级目录不可写。")
    hint = None
    if (resolved / "curated").exists() or (resolved / "meta" / "manifest.db").exists():
        hint = "这个目录里已经有数据湖，生成配置后会接管它，不会清空。"
    return resolved, hint


def install_ops_routes(app: FastAPI, *, mutations: bool) -> None:
    @app.get("/api/ops")
    def ops_home(request: Request) -> dict:
        check_browser(request, mutation=False, scope="ops")
        payload = _service(request).home()
        payload["mode"] = mode_payload(request)
        payload["csrf_token"] = request.app.state.csrf
        return payload

    @app.get("/api/ops/readiness")
    def ops_readiness(request: Request) -> dict:
        check_browser(request, mutation=False, scope="ops")
        from cnequity.research.packs import readiness_payload

        return readiness_payload(request.app.state.config)

    @app.get("/api/ops/settings")
    def ops_settings(request: Request) -> dict:
        check_browser(request, mutation=False, scope="ops")
        try:
            return _settings_service(request).home()
        except (OpsError, OSError, ValueError) as exc:
            raise HTTPException(422, str(exc)) from exc

    @app.get("/api/ops/schedule")
    def ops_schedule(request: Request) -> dict:
        check_browser(request, mutation=False, scope="ops")
        try:
            return _schedule_service(request).home()
        except (OpsError, OSError, ValueError) as exc:
            raise HTTPException(422, str(exc)) from exc

    @app.get("/api/ops/backups")
    def ops_backups(request: Request, root: str | None = None) -> dict:
        check_browser(request, mutation=False, scope="ops")
        from cnequity.serve.ops.backups import inventory

        try:
            if root is not None:
                from cnequity.storage.snapshots import _reject_symlink_path

                path = Path(root).expanduser()
                if not path.is_absolute():
                    raise OpsError("备份目录必须是绝对路径。")
                _reject_symlink_path(path, label="snapshot root")
            return inventory(request.app.state.config, root)
        except (OSError, ValueError, RuntimeError) as exc:
            raise HTTPException(422, str(exc)) from exc

    @app.get("/api/ops/jobs")
    def ops_jobs(request: Request, limit: int = Query(default=50, ge=1, le=50)) -> list[dict]:
        check_browser(request, mutation=False, scope="ops")
        return _service(request).list_jobs(limit)

    @app.get("/api/ops/jobs/{job_id}")
    def ops_job(job_id: str, request: Request) -> dict:
        check_browser(request, mutation=False, scope="ops")
        try:
            return _service(request).get_job(job_id)
        except OpsError as exc:
            raise _fail(exc) from exc

    @app.get("/api/stream/ops/jobs/{job_id}")
    async def ops_job_stream(
        job_id: str,
        request: Request,
        stream: str = Query(default="err", pattern="^(err|out)$"),
        offset: int = Query(default=0, ge=0),
    ):
        check_browser(request, mutation=False, scope="ops")
        try:
            shown = _service(request).get_job(job_id)
        except OpsError as exc:
            raise _fail(exc) from exc
        log_path = Path((shown.get("logs") or {}).get(stream) or "")

        async def events():
            pos = offset
            while True:
                if await request.is_disconnected():
                    return
                chunk = await run_in_threadpool(_read_chunk, log_path, pos)
                if chunk:
                    pos += len(chunk)
                    text = chunk.decode("utf-8", "replace")
                    yield _frame({"offset": pos, "text": text})
                    continue
                current = await run_in_threadpool(_service(request).get_job, job_id)
                if current["state"] not in {"starting", "running"}:
                    yield _frame(
                        {"offset": pos, "text": "", "done": True, "state": current["state"]}
                    )
                    return
                yield ": keepalive\n\n"
                await asyncio.sleep(0.5)

        return StreamingResponse(
            events(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    if not mutations:
        return

    @app.post("/api/ops/settings/preview")
    def settings_preview(body: SettingsBody, request: Request) -> dict:
        check_browser(request, mutation=True, scope="ops")
        try:
            return _settings_service(request).preview(body.values)
        except (OpsError, OSError, ValueError) as exc:
            raise HTTPException(422, str(exc)) from exc

    @app.post("/api/ops/settings/apply")
    def settings_apply(body: SettingsApplyBody, request: Request) -> dict:
        check_browser(request, mutation=True, scope="ops")
        try:
            result = _settings_service(request).apply(body.token, acknowledged=body.acknowledged)
            from cnequity.serve.app import reload_lake_config

            reload_lake_config(request.app)
            return result
        except (OpsError, OSError, ValueError) as exc:
            raise HTTPException(422, str(exc)) from exc

    @app.post("/api/ops/schedule/preview")
    def schedule_preview(body: ScheduleBody, request: Request) -> dict:
        check_browser(request, mutation=True, scope="ops")
        try:
            return _schedule_service(request).preview(**body.model_dump())
        except (OpsError, OSError, ValueError) as exc:
            raise HTTPException(422, str(exc)) from exc

    @app.post("/api/ops/schedule/apply")
    def schedule_apply(body: ScheduleApplyBody, request: Request) -> dict:
        check_browser(request, mutation=True, scope="ops")
        try:
            service = _schedule_service(request)
            try:
                result = service.apply(
                    body.token,
                    acknowledged=body.acknowledged,
                    requested_by=requested_by(
                        request.client.host if request.client else None,
                        request.headers.get("user-agent"),
                    ),
                )
            finally:
                # Time changes may be saved even when OS registration fails.
                # Keep the existing view and active maintenance gate.
                fresh = service._current()
                request.app.state.config.daily_run_at = fresh.daily_run_at
                request.app.state.config.stale_run_at = fresh.stale_run_at
            return result
        except (OpsError, OSError, ValueError) as exc:
            raise HTTPException(422, str(exc)) from exc

    @app.post("/api/ops/preview")
    def ops_preview(body: PreviewBody, request: Request) -> dict:
        check_browser(request, mutation=True, scope="ops")
        try:
            return _service(request).preview(body.op, body.params)
        except OpsError as exc:
            raise _fail(exc) from exc

    @app.post("/api/ops/jobs", status_code=202)
    def ops_start(body: StartBody, request: Request) -> dict:
        check_browser(request, mutation=True, scope="ops")
        try:
            return _service(request).start(
                body.preview_id,
                body.launch_token,
                body.acknowledged,
                requested_by=requested_by(
                    request.client.host if request.client else None,
                    request.headers.get("user-agent"),
                ),
            )
        except OpsError as exc:
            raise _fail(exc) from exc

    @app.post("/api/ops/jobs/{job_id}/cancel", status_code=202)
    def ops_cancel(job_id: str, request: Request) -> dict:
        check_browser(request, mutation=True, scope="ops")
        try:
            return _service(request).cancel(
                job_id,
                requested_by=requested_by(
                    request.client.host if request.client else None,
                    request.headers.get("user-agent"),
                ),
            )
        except OpsError as exc:
            raise _fail(exc) from exc

    @app.post("/api/setup/config")
    def setup_config(body: SetupBody, request: Request) -> dict:
        check_browser(request, mutation=True, scope="ops")
        if not request.app.state.setup:
            raise HTTPException(409, "已经有配置。")
        target = request.app.state.config_path
        if target is None:
            raise HTTPException(409, "没有可写入的配置路径。")
        try:
            data_root, hint = accept_data_root(body.data_root)
            from cnequity.config import load_config
            from cnequity.config.bootstrap import write_user_config
            from cnequity.serve.app import activate_lake

            data_root.mkdir(parents=True, exist_ok=True)
            write_user_config(Path(target), data_root=str(data_root))
            config = load_config(target)
            activate_lake(request.app, config)
        except FileExistsError as exc:
            raise HTTPException(409, "配置文件已经存在。") from exc
        except OpsError as exc:
            raise _fail(exc) from exc
        except (OSError, ValueError) as exc:
            raise HTTPException(422, f"无法生成配置（{type(exc).__name__}）。") from exc
        return {
            "config_path": str(Path(target).resolve()),
            "data_root": str(data_root),
            "hint": hint,
            "mode": mode_payload(request),
        }


def _read_chunk(path: Path, offset: int) -> bytes:
    if not path.is_file():
        return b""
    with path.open("rb") as handle:
        handle.seek(offset)
        return handle.read(_FRAME)


def _frame(payload: dict) -> str:
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
