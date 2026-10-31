"""Reviewed reclamation of compacted staging, with resumable quarantine receipts."""

from __future__ import annotations

import re
import shutil
from contextlib import ExitStack
from pathlib import Path

from cnequity.config import Config
from cnequity.file_lock import exclusive_lock, lake_mutation_lock
from cnequity.orchestrator.manifest import Manifest
from cnequity.storage.atomic import replace_with_retry, write_json_atomic
from cnequity.storage.lifecycle import (
    LifecycleError,
    LifecycleStore,
    _read,
    _tree_signature,
    digest,
)
from cnequity.storage.lifecycle.purge import check_idle, unfinished
from cnequity.storage.lifecycle.resources import resource_holds
from cnequity.storage.revisions import _reject_symlink_path, sha256_file
from cnequity.storage.staging_cleanup import (
    clean_staging,
    run_ready_for_staging_cleanup,
    staging_run_paths,
)


class StagingRetirement:
    """Never retire incomplete batches or infer supersession from age alone."""

    def __init__(self, config: Config):
        self.config = config
        self.store = LifecycleStore(config.meta_root)
        self.staging = config.staging_root.absolute()

    def inspect(self) -> list[dict]:
        if not self.staging.exists():
            return []
        inventory = clean_staging(self.config, dry_run=True).inventory
        return [
            {
                "object_id": f"staging/{row['run_id']}",
                "kind": "staging",
                "current": False,
                "label": row["run_id"],
                "logical_bytes": row["bytes"],
                "status": "unmarked" if row["decision"] == "compacted_candidate" else "protected",
                "reasons": [] if row["decision"] == "compacted_candidate" else [row["decision"]],
            }
            for row in inventory
        ]

    @staticmethod
    def _capture(path: Path) -> dict:
        signature = _tree_signature(path)
        info = path.stat()
        files = {
            p.relative_to(path).as_posix(): {"size": p.stat().st_size, "sha256": sha256_file(p)}
            for p in sorted(path.rglob("*"))
            if p.is_file()
        }
        if _tree_signature(path) != signature:
            raise LifecycleError("Staging changed during review")
        return {"signature": signature, "identity": [info.st_dev, info.st_ino], "files": files}

    def plan(self, *, phase="purge", selected_ids=None, require_registry: bool = True) -> dict:
        if phase != "purge":
            raise LifecycleError("Staging supports deletion review only")
        with lake_mutation_lock(self.store.meta, blocking=False):
            registry = self.store.registry(required=require_registry)
            if registry is None:
                from cnequity.storage.lifecycle.snapshot import read_dependencies

                if read_dependencies(self.store.meta) is not None:
                    raise LifecycleError("Rebind imported lifecycle dependencies before cleanup")
            if unfinished(self.store):
                raise LifecycleError("Resume the unfinished purge first")
            available = {o["object_id"]: o for o in self.inspect() if not o["reasons"]}
            ids = sorted(available) if selected_ids is None else selected_ids
            if len(ids) != len(set(ids)) or any(oid not in available for oid in ids):
                raise LifecycleError("Staging selection is no longer eligible")
            objects = []
            for oid in ids:
                obj = dict(available[oid])
                paths = staging_run_paths(self.staging, oid.split("/", 1)[1])
                if not paths:
                    raise LifecycleError("Staging disappeared during review")
                obj["paths"] = {
                    p.relative_to(self.staging).as_posix(): self._capture(p) for p in paths
                }
                obj["logical_bytes"] = sum(
                    f["size"] for item in obj["paths"].values() for f in item["files"].values()
                )
                objects.append(obj)
            plan = {
                "schema_version": 1,
                "phase": "purge",
                "meta_root": str(self.store.meta),
                "lake_id": None if registry is None else registry["lake_id"],
                "staging_root": str(self.staging),
                "objects": objects,
            }
            plan_id = digest(plan)
            path = self.store.root / "staging-plans" / f"{plan_id}.json"
            _reject_symlink_path(path)
            write_json_atomic(path, plan)
            return plan | {"plan_id": plan_id}

    def read_plan(self, plan_id: str) -> dict:
        if not re.fullmatch(r"[0-9a-f]{64}", plan_id):
            raise LifecycleError("Invalid staging plan ID")
        plan = _read(self.store.root / "staging-plans" / f"{plan_id}.json")
        if (
            digest(plan) != plan_id
            or plan.get("schema_version") != 1
            or plan.get("phase") != "purge"
            or plan.get("meta_root") != str(self.store.meta)
            or plan.get("staging_root") != str(self.staging)
        ):
            raise LifecycleError("Staging plan changed or belongs to another lake")
        ids = set()
        for obj in plan["objects"]:
            oid = obj["object_id"]
            if not re.fullmatch(r"staging/[\w-]+", oid) or oid in ids or not obj["paths"]:
                raise LifecycleError("Invalid staging object identity")
            ids.add(oid)
            for relative in obj["paths"]:
                path = Path(relative)
                if (
                    path.is_absolute()
                    or len(path.parts) != 2
                    or path.parts[0] in {".", ".."}
                    or path.parts[1] != f"run_id={oid.split('/', 1)[1]}"
                ):
                    raise LifecycleError("Staging plan escapes its registered run")
        return plan | {"plan_id": plan_id}

    @staticmethod
    def _verify(path: Path, item: dict, *, partial=False, moved=False) -> None:
        _tree_signature(path)  # Reject links, special files and unreadable trees.
        info = path.stat()
        if [info.st_dev, info.st_ino] != item["identity"]:
            raise LifecycleError("Staging directory identity changed")
        actual = {
            p.relative_to(path).as_posix(): {"size": p.stat().st_size, "sha256": sha256_file(p)}
            for p in path.rglob("*")
            if p.is_file()
        }
        if any(item["files"].get(k) != v for k, v in actual.items()) or (
            not partial and actual != item["files"]
        ):
            raise LifecycleError("Staging content changed")
        if not moved and _tree_signature(path) != item["signature"]:
            raise LifecycleError("Staging file identities changed")

    def apply(self, plan_id: str, *, maintenance: bool) -> dict:
        if not maintenance:
            raise LifecycleError("Staging purge requires a confirmed maintenance window")
        plan = self.read_plan(plan_id)
        event_path = self.store.root / "staging-purges" / f"{plan_id}.json"
        _reject_symlink_path(event_path)
        with lake_mutation_lock(self.store.meta, blocking=False), ExitStack() as locks:
            check_idle(self.config.manifest_path)
            registry = self.store.registry()
            planned_lake = plan.get("lake_id")
            if planned_lake is None:
                if registry is not None:
                    raise LifecycleError("Staging plan belongs to another lake")
            elif registry is None or registry["lake_id"] != planned_lake:
                raise LifecycleError("Staging plan belongs to another lake")
            if any(p != event_path for p in unfinished(self.store)):
                raise LifecycleError("Resume the other unfinished purge first")
            manifest = Manifest(self.config.manifest_path)
            journal = (
                _read(event_path)
                if event_path.exists()
                else {
                    "plan_id": plan_id,
                    "status": "prepared",
                    "items": {},
                    "logical_bytes_deleted": 0,
                    "filesystem_free_bytes_before": shutil.disk_usage(self.staging).free,
                }
            )
            expected = {p for o in plan["objects"] for p in o["paths"]}
            if (
                journal.get("plan_id") != plan_id
                or journal.get("status") not in {"prepared", "error", "complete"}
                or not isinstance(journal.get("items"), dict)
                or not set(journal["items"]).issubset(expected)
                or any(
                    v not in {"moving", "deleting", "deleted"} for v in journal["items"].values()
                )
            ):
                raise LifecycleError("Invalid staging purge journal")
            if journal["status"] == "complete":
                if (
                    set(journal["items"]) != expected
                    or set(journal["items"].values()) != {"deleted"}
                    or journal.get("logical_bytes_deleted")
                    != sum(o["logical_bytes"] for o in plan["objects"])
                ):
                    raise LifecycleError("Invalid completed staging purge")
                return journal
            with resource_holds(self.store.meta) as holds:
                # Lock and verify the entire selection before the first irreversible operation.
                for obj in plan["objects"]:
                    run_id = obj["object_id"].split("/", 1)[1]
                    locks.enter_context(
                        exclusive_lock(self.store.meta / "locks" / f"{run_id}.lock", blocking=False)
                    )
                    if obj["object_id"] in holds or not run_ready_for_staging_cleanup(
                        manifest, run_id
                    ):
                        raise LifecycleError("Staging is held or no longer compacted")
                    current = {
                        p.relative_to(self.staging).as_posix()
                        for p in staging_run_paths(self.staging, run_id)
                    }
                    if not current.issubset(obj["paths"]):
                        raise LifecycleError("New staging paths appeared after review")
                    for relative, item in obj["paths"].items():
                        source = self.staging / relative
                        target = self.store.root / "staging-trash" / plan_id / digest(relative)
                        _reject_symlink_path(source)
                        _reject_symlink_path(target)
                        state = journal["items"].get(relative)
                        if source.exists():
                            if target.exists() or state in {"deleting", "deleted"}:
                                raise LifecycleError("Conflicting staging quarantine state")
                            self._verify(source, item)
                        elif target.exists():
                            if state not in {"moving", "deleting"}:
                                raise LifecycleError("Unrecorded staging quarantine")
                            self._verify(target, item, partial=state == "deleting", moved=True)
                        elif state not in {"deleting", "deleted"}:
                            raise LifecycleError("Staging disappeared outside the purge")
                try:
                    for obj in plan["objects"]:
                        for relative in obj["paths"]:
                            if journal["items"].get(relative) == "deleted":
                                continue
                            source = self.staging / relative
                            target = self.store.root / "staging-trash" / plan_id / digest(relative)
                            if source.exists():
                                journal["items"][relative] = "moving"
                                write_json_atomic(event_path, journal)
                                target.parent.mkdir(parents=True, exist_ok=True)
                                replace_with_retry(source, target)
                            journal["items"][relative] = "deleting"
                            write_json_atomic(event_path, journal)
                            if target.exists():
                                shutil.rmtree(target)
                            journal["items"][relative] = "deleted"
                            journal["logical_bytes_deleted"] = sum(
                                f["size"]
                                for o in plan["objects"]
                                for p, item in o["paths"].items()
                                if journal["items"].get(p) == "deleted"
                                for f in item["files"].values()
                            )
                            write_json_atomic(event_path, journal)
                    journal.update(
                        status="complete",
                        filesystem_free_bytes_after=shutil.disk_usage(self.store.meta).free,
                    )
                    write_json_atomic(event_path, journal)
                except Exception:
                    journal["status"] = "error"
                    write_json_atomic(event_path, journal)
                    raise
                return journal
