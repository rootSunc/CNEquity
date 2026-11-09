"""Storage notices and explicit, short-lived browser approvals.

GETs never plan or delete. Only an approved review of chosen versions can invoke
the lifecycle executor. External readers still require operator maintenance consent.
"""

from __future__ import annotations

import copy
import secrets
import shutil
import threading
import time
from datetime import datetime, timezone
from typing import Literal

from cnequity.config import Config
from cnequity.i18n import _, carry_language
from cnequity.storage.lifecycle import LifecycleError, LifecycleStore
from cnequity.storage.lifecycle.purge import unfinished
from cnequity.storage.lifecycle.staging import StagingRetirement

Kind = Literal["revisions", "staging"]
Phase = Literal["purge"]
APPROVAL_SECONDS = 600


class MaintenanceGate:
    """Drain this server's requests and background scans before deletion."""

    def __init__(self):
        self.condition = threading.Condition()
        self.readers = 0
        self.closed = False

    def enter(self) -> bool:
        with self.condition:
            if self.closed:
                return False
            self.readers += 1
            return True

    def leave(self) -> None:
        with self.condition:
            self.readers -= 1
            self.condition.notify_all()

    def close(self) -> None:
        with self.condition:
            self.closed = True

    def drain(self, timeout: float = 30) -> None:
        with self.condition:
            if not self.condition.wait_for(lambda: self.readers == 0, timeout=timeout):
                raise LifecycleError(
                    _(
                        "The dashboard still has reads in progress; wait for them to finish, then confirm again."
                    )
                )

    def reopen(self) -> None:
        with self.condition:
            self.closed = False
            self.condition.notify_all()


class StorageMaintenance:
    def __init__(self, config: Config, *, invalidate=lambda: None, csrf: str | None = None):
        self.config = config
        self.store = LifecycleStore(config.meta_root)
        self.gate = MaintenanceGate()
        self.csrf = csrf or secrets.token_urlsafe(32)
        self.lock = threading.RLock()
        self.jobs: dict[str, dict] = {}
        self.busy = False
        self.invalidate = invalidate
        self.last_summary: dict | None = None

    def summary(self) -> dict:
        # A page reload during deletion must show progress without scanning a
        # directory tree that the executor is currently moving/removing.
        if not self.gate.enter():
            with self.lock:
                if self.last_summary is None:
                    raise LifecycleError(
                        _("Maintenance is running; follow its progress in the operation records.")
                    )
                return copy.deepcopy(self.last_summary) | {
                    "active_jobs": [
                        self._public(j)
                        for j in self.jobs.values()
                        if j["status"] in {"checking", "executing"}
                    ],
                    "inventory_paused": True,
                }
        try:
            result = self._summary()
            with self.lock:
                self.last_summary = copy.deepcopy(result)
            return result
        finally:
            self.gate.leave()

    def _summary(self) -> dict:
        now = datetime.now(timezone.utc)
        # The same count backfill and the daily run keep; 0 means they keep all.
        configured = getattr(self.config, "revision_keep_generations", 5)
        automatic = (
            isinstance(configured, int) and not isinstance(configured, bool) and configured > 0
        )
        keep = configured if automatic else 5
        report = self.store.inspect(keep=keep)
        registry = self.store.registry()
        objects = []
        for obj in report["objects"]:
            # Unprotected history is deleted by the next backfill or daily run
            # when automatic retention is on.
            status = (
                "protected" if obj["blocked_reasons"] else "scheduled" if automatic else "deletable"
            )
            objects.append(
                {
                    "object_id": obj["object_id"],
                    "kind": "revisions",
                    "current": bool(obj["current"]),
                    "label": _("{dataset} · revision {revision}").format(
                        dataset=obj["dataset"], revision=obj["revision"]
                    ),
                    "status": status,
                    "logical_bytes": obj["logical_bytes"],
                    "reasons": obj["blocked_reasons"],
                }
            )
        objects.extend(StagingRetirement(self.config).inspect())
        events = [
            {
                "kind": "staging" if p.parent.name == "staging-purges" else "revisions",
                "plan_id": p.stem,
            }
            for p in unfinished(self.store)
        ]
        root = self.config.data_root
        while not root.exists() and root != root.parent:
            root = root.parent
        usage = shutil.disk_usage(root)
        with self.lock:
            active = [
                self._public(j)
                for j in self.jobs.values()
                if j["status"] in {"checking", "executing"}
            ]
        return {
            "mode": "web_confirmation_only",
            "registered": registry is not None,
            "csrf_token": self.csrf,
            "objects": objects,
            "unfinished": events,
            "active_jobs": active,
            "disk_free_bytes": usage.free,
            "disk_total_bytes": usage.total,
            "keep": keep,
            "automatic_retention": automatic,
            "generated_at": now.isoformat(),
        }

    @staticmethod
    def _public(job: dict) -> dict:
        return copy.deepcopy({k: v for k, v in job.items() if not k.startswith("_")})

    def job(self, job_id: str) -> dict:
        with self.lock:
            if job_id not in self.jobs:
                raise KeyError(job_id)
            return self._public(self.jobs[job_id])

    def _start(self, state: dict, work) -> dict:
        with self.lock:
            if self.busy:
                raise LifecycleError(
                    _("A storage check or cleanup is already running; wait for it to finish.")
                )
            # Completed reviews are short lived and never become an unbounded queue.
            for key, job in list(self.jobs.items()):
                if time.monotonic() - job["_created"] > APPROVAL_SECONDS and job["status"] not in {
                    "checking",
                    "executing",
                }:
                    del self.jobs[key]
            if len(self.jobs) >= 64:
                raise LifecycleError(_("Too many check requests; try again shortly."))
            job_id = secrets.token_hex(16)
            job = {"job_id": job_id, "_created": time.monotonic(), **state}
            self.jobs[job_id] = job
            self.busy = True
            initial = self._public(job)

        def run():
            try:
                result = work()
                with self.lock:
                    job.update(result)
            except Exception as exc:
                with self.lock:
                    job.update(
                        status="error",
                        error=str(exc),
                        message=_(
                            "The operation stopped. If it failed while executing, some content may already be deleted; check the records and review the original plan again."
                        )
                        if state["status"] == "executing"
                        else _("The check did not pass, so nothing was deleted."),
                    )
            finally:
                with self.lock:
                    try:
                        if state["status"] == "executing":
                            self.invalidate()
                    finally:
                        self.gate.reopen()
                        self.busy = False

        try:
            threading.Thread(
                target=carry_language(run), name="storage-maintenance", daemon=True
            ).start()
        except BaseException:
            with self.lock:
                self.busy = False
                del self.jobs[job_id]
            self.gate.reopen()
            raise
        return initial

    def review(
        self,
        kind: Kind,
        phase: Phase,
        resume_plan_id: str | None = None,
        object_ids: list[str] | None = None,
    ) -> dict:
        def prepare():
            if kind == "staging":
                retirement = StagingRetirement(self.config)
                if phase != "purge" or (resume_plan_id and object_ids):
                    raise LifecycleError("Staging only supports a deletion review")
                if object_ids is not None and (not object_ids or len(object_ids) > 200):
                    raise LifecycleError(_("Select 1 to 200 staging runs."))
                if resume_plan_id:
                    if not any(
                        p.stem == resume_plan_id and p.parent.name == "staging-purges"
                        for p in unfinished(self.store)
                    ):
                        raise LifecycleError(
                            _("Only the same unfinished deletion record can be reviewed again.")
                        )
                    plan = retirement.read_plan(resume_plan_id)
                else:
                    plan = retirement.plan(phase=phase, selected_ids=object_ids)
            elif resume_plan_id:
                if phase != "purge" or not any(
                    p.stem == resume_plan_id and p.parent.name == "purges"
                    for p in unfinished(self.store)
                ):
                    raise LifecycleError(
                        _("Only the same unfinished deletion record can be reviewed again.")
                    )
                plan = self.store.read_plan(resume_plan_id)
                if plan["phase"] != "purge":
                    raise LifecycleError(_("Invalid restore plan."))
                plan = {**plan, "plan_id": resume_plan_id}
            elif object_ids:
                if phase != "purge" or resume_plan_id:
                    raise LifecycleError(
                        _("A selection starts a new deletion; it cannot resume another one.")
                    )
                if len(object_ids) > 200 or len(object_ids) != len(set(object_ids)):
                    raise LifecycleError(_("Select 1 to 200 distinct historical versions."))
                if any(
                    not oid or len(oid) > 300 or any(ch.isspace() for ch in oid)
                    for oid in object_ids
                ):
                    raise LifecycleError(_("Invalid version number."))
                plan = self.store.plan(selected_ids=object_ids)
            else:
                raise LifecycleError(_("Choose the versions to delete."))
            if kind == "revisions":
                selected = set(plan["purge_ids"])
                objects = [o for o in plan["objects"] if o["object_id"] in selected]
            else:
                objects = plan["objects"]
            return {
                "status": "ready",
                "plan_id": plan["plan_id"],
                "objects": [
                    {
                        "object_id": o["object_id"],
                        "label": _("{dataset} · revision {revision}").format(
                            dataset=o["dataset"], revision=o["revision"]
                        )
                        if kind == "revisions"
                        else o["label"],
                        "logical_bytes": o["logical_bytes"],
                    }
                    for o in objects
                ],
                "logical_bytes": sum(o["logical_bytes"] for o in objects),
                "confirmation_token": secrets.token_urlsafe(32),
                "_expires": time.monotonic() + APPROVAL_SECONDS,
                "expires_in_seconds": APPROVAL_SECONDS,
                "resuming": bool(resume_plan_id),
            }

        return self._start({"status": "checking", "kind": kind, "phase": phase}, prepare)

    def confirm(
        self, review_id: str, confirmation_token: str, *, confirmed: bool, maintenance: bool
    ) -> dict:
        with self.lock:
            review = self.jobs.get(review_id)
            if not review or review["status"] != "ready" or review.get("_used"):
                raise LifecycleError(
                    _("The check has expired or was already confirmed; check again.")
                )
            if not confirmed or not secrets.compare_digest(
                confirmation_token, review["confirmation_token"]
            ):
                raise LifecycleError(_("No web confirmation matches this check."))
            if time.monotonic() > review["_expires"]:
                raise LifecycleError(_("The confirmation has expired; check again."))
            if not review["objects"]:
                raise LifecycleError(_("Nothing to execute this time."))
            if review["phase"] == "purge" and not maintenance:
                raise LifecycleError(
                    _("First confirm that external queries, services and schedules have stopped.")
                )
            if review["phase"] == "purge":
                from cnequity.serve.ops.records import purge_blocked_reason

                reason = purge_blocked_reason(self.config)
                if reason:
                    raise LifecycleError(reason)
            if self.busy:
                raise LifecycleError(_("A storage operation is already running."))
            review["_used"] = True
            self.gate.close()

            def execute():
                self.gate.drain()
                plan_id = review["plan_id"]
                if review["kind"] == "revisions":
                    result = self.store.purge(
                        plan_id, maintenance=True, manifest=self.config.manifest_path
                    )
                else:
                    result = StagingRetirement(self.config).apply(plan_id, maintenance=maintenance)
                return {"status": "complete", "result": result}

            try:
                return self._start(
                    {
                        "status": "executing",
                        "kind": review["kind"],
                        "phase": review["phase"],
                        "plan_id": review["plan_id"],
                    },
                    execute,
                )
            except BaseException:
                review["_used"] = False
                self.gate.reopen()
                raise
