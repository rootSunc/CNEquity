"""Same-origin browser confirmation endpoints for lifecycle maintenance."""

from __future__ import annotations

from typing import Literal

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, ConfigDict, StrictBool

from cnequity.file_lock import LockUnavailable
from cnequity.i18n import _
from cnequity.serve.guard import check_browser
from cnequity.serve.storage import StorageMaintenance
from cnequity.storage.lifecycle import LifecycleError
from cnequity.storage.revisions import RevisionConsistencyError


class ReviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["revisions", "staging"]
    phase: Literal["purge"] = "purge"
    resume_plan_id: str | None = None
    object_ids: list[str] | None = None


class Confirmation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    review_id: str
    confirmation_token: str
    confirmed: StrictBool
    maintenance_confirmed: StrictBool = False


def _service(request: Request) -> StorageMaintenance:
    service = request.app.state.storage_maintenance
    if request.app.state.setup or service is None:
        raise HTTPException(409, _("Not configured yet"))
    return service


def install_storage_routes(app: FastAPI, *, mutations: bool = True) -> None:
    @app.get("/api/storage")
    def storage_summary(request: Request) -> dict:
        check_browser(request, mutation=False, scope="storage")
        service = _service(request)
        try:
            return service.summary()
        except (LifecycleError, RevisionConsistencyError, OSError, ValueError) as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get("/api/storage/jobs/{job_id}")
    def storage_job(job_id: str, request: Request) -> dict:
        check_browser(request, mutation=False, scope="storage")
        service = _service(request)
        try:
            return service.job(job_id)
        except KeyError as exc:
            raise HTTPException(404, _("The check record has expired; check again.")) from exc

    if not mutations:
        return

    @app.post("/api/storage/reviews", status_code=202)
    def storage_review(body: ReviewRequest, request: Request) -> dict:
        check_browser(request, mutation=True, scope="storage")
        service = _service(request)
        try:
            return service.review(body.kind, body.phase, body.resume_plan_id, body.object_ids)
        except (LifecycleError, LockUnavailable) as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/api/storage/confirm", status_code=202)
    def storage_confirm(body: Confirmation, request: Request) -> dict:
        check_browser(request, mutation=True, scope="storage")
        service = _service(request)
        try:
            return service.confirm(
                body.review_id,
                body.confirmation_token,
                confirmed=body.confirmed,
                maintenance=body.maintenance_confirmed,
            )
        except (LifecycleError, LockUnavailable) as exc:
            raise HTTPException(409, str(exc)) from exc
