"""Revision retention: registered holds, immutable plans and an observation period."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from cnequity.file_lock import lake_mutation_lock
from cnequity.i18n import _
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.revisions import (
    RevisionStore,
    _exclusive_tree_bytes,
    _reject_symlink_path,
    sha256_file,
)

GRACE_DAYS = 7
TEXT_SUFFIXES = {".json", ".jsonl", ".md", ".py", ".toml", ".yaml", ".yml"}
_TOKEN = re.compile(r"[a-zA-Z0-9_-]+\Z")


class LifecycleError(RuntimeError):
    """Incomplete protection information or a stale lifecycle operation."""


def digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def case_holds(cases: list[dict]) -> dict[str, list[dict]]:
    """Expand reviewed byte dependencies, without retaining provenance ancestors."""
    holds: dict[str, list[dict]] = {}
    for case in cases:
        if not isinstance(case, dict) or not isinstance(case.get("case_id"), str):
            raise LifecycleError("Invalid case manifest")
        roots, edges = case.get("roots"), case.get("dependencies", [])
        if not isinstance(roots, list) or not roots or not isinstance(edges, list):
            raise LifecycleError("Case manifest requires explicit retention roots")
        if not all(isinstance(oid, str) and oid for oid in roots):
            raise LifecycleError("Invalid case root")
        graph: dict[str, set[str]] = {}
        for edge in edges:
            if (
                not isinstance(edge, dict)
                or edge.get("kind") not in ("requires_bytes", "provenance")
                or not isinstance(edge.get("from"), str)
                or not isinstance(edge.get("to"), str)
            ):
                raise LifecycleError("Invalid dependency edge")
            if edge["kind"] == "requires_bytes":
                graph.setdefault(edge["from"], set()).add(edge["to"])
        reached, todo = set(), list(roots)
        while todo:
            oid = todo.pop()
            if oid in reached:
                continue
            reached.add(oid)
            todo.extend(graph.get(oid, ()))
        for oid in sorted(reached):
            holds.setdefault(oid, []).append(
                {
                    "kind": "case_dependency",
                    "case_id": case["case_id"],
                    "case_digest": digest(case),
                }
            )
    return holds


def _read(path: Path) -> dict:
    _reject_symlink_path(path)
    try:
        if not stat.S_ISREG(path.lstat().st_mode):
            raise ValueError("not a regular file")
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("not an object")
        return value
    except (OSError, ValueError) as exc:
        raise LifecycleError(f"Cannot read lifecycle metadata: {path}") from exc


#: What the lake rewrites under its own ``meta`` on every run: source caches,
#: watermarks and current-pointer mirrors, checkpoints, locks, health and
#: quality reports, and the derivatives capture records. None of it is a
#: reference a reviewer registered, and scanning it made every run invalidate
#: the import, so cleanup refused from the first run after one.
RUNTIME_META_PATHS = (
    "adj_factor_arbitration_cache",
    "checkpoints",
    "derivations",
    "derivatives/annual_archives",
    "derivatives/futures_bars",
    "derivatives/greeks_dependencies",
    "derivatives/http_cache",
    "derivatives/option_bars",
    "instruments_absence_streak.json",
    "lifecycle",
    "locks",
    "payment_date_repairs",
    "quality",
    "rate_limits",
    "raw",
    "raw_storage.json",
    "repairs",
    "revisions",
    "source_cache",
    "source_health",
    "source_snapshots",
    "stability",
    "state",
    "stats",
)


def reference_files(roots: list[dict], *, meta_root: Path | None = None) -> list[Path]:
    """Enumerate registered evidence roots, including newly added reports.

    Exclusions are explicit root-relative paths, saved in the import report.
    A root that is the lake's own *meta_root* also skips
    :data:`RUNTIME_META_PATHS`. Missing roots, unreadable files and links never
    mean 'no references'.
    """
    found: set[Path] = set()
    lake_meta = Path(meta_root).resolve() if meta_root is not None else None
    for spec in roots:
        root = Path(spec["path"])
        _reject_symlink_path(root)
        if not root.is_absolute() or not root.is_dir():
            raise LifecycleError(f"Reference root unavailable: {root}")
        excluded = [Path(p) for p in spec.get("exclude", [])]
        if any(p.is_absolute() or ".." in p.parts or not p.parts for p in excluded):
            raise LifecycleError("Invalid reference exclusion")
        if lake_meta is not None and root.resolve() == lake_meta:
            excluded += [Path(p) for p in RUNTIME_META_PATHS]

        def onerror(error: OSError) -> None:
            raise LifecycleError(f"Reference scan failed: {error}") from error

        for directory, dirs, names in os.walk(root, followlinks=False, onerror=onerror):
            parent = Path(directory)
            for name in list(dirs) + names:
                path = parent / name
                relative = path.relative_to(root)
                if any(relative == p or p in relative.parents for p in excluded):
                    if name in dirs:
                        dirs.remove(name)
                    continue
                if path.is_symlink():
                    raise LifecycleError(f"Reference scan contains a link: {path}")
                if name in names and path.suffix in TEXT_SUFFIXES:
                    found.add(path)
    return sorted(found)


def reference_fingerprint(roots: list[dict], *, meta_root: Path | None = None) -> str:
    return digest([(str(p), sha256_file(p)) for p in reference_files(roots, meta_root=meta_root)])


def _tree_signature(root: Path) -> str:
    """Bind a marking plan to the exact directory and file identities."""
    records = []
    _reject_symlink_path(root)

    def onerror(error: OSError) -> None:
        raise LifecycleError(f"Generation scan failed: {error}") from error

    for directory, dirs, names in os.walk(root, followlinks=False, onerror=onerror):
        for p in [Path(directory), *(Path(directory) / n for n in names)]:
            info = p.lstat()
            if not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
                raise LifecycleError(f"Unsupported generation entry: {p}")
            records.append(
                (
                    str(p.relative_to(root)),
                    info.st_dev,
                    info.st_ino,
                    info.st_size,
                    info.st_mtime_ns,
                    info.st_ctime_ns,
                )
            )
        for name in dirs:
            if (Path(directory) / name).is_symlink():
                raise LifecycleError(f"Generation contains a link: {Path(directory) / name}")
    return digest(sorted(records))


class LifecycleStore:
    def __init__(self, meta_root: Path | str):
        supplied = Path(meta_root).expanduser().absolute()
        _reject_symlink_path(supplied)
        self.meta = supplied.resolve()
        self.root = self.meta / "lifecycle"
        _reject_symlink_path(self.root)
        self.registry_path = self.root / "registry.json"

    def registry(self, *, required: bool = False) -> dict | None:
        if not self.registry_path.exists() and not self.registry_path.is_symlink():
            if required:
                raise LifecycleError(
                    "Import a reviewed reference manifest before marking revisions"
                )
            return None
        value = _read(self.registry_path)
        if (
            value.get("schema_version") != 1
            or value.get("meta_root") != str(self.meta)
            or not isinstance(value.get("lake_id"), str)
            or not isinstance(value.get("holds"), dict)
            or not isinstance(value.get("pending"), dict)
            or not isinstance(value.get("experiments"), list)
            or not isinstance(value.get("reference_roots"), list)
            or not isinstance(value.get("reference_fingerprint"), str)
        ):
            raise LifecycleError("Invalid lifecycle registry or copied lake identity")
        if not isinstance(value.get("artifacts", {}), dict) or not isinstance(
            value.get("experiment_pending", {}), dict
        ):
            raise LifecycleError("Invalid artifact registry or experiment observation periods")
        for oid, reasons in value["holds"].items():
            if not isinstance(oid, str) or not isinstance(reasons, list) or not reasons:
                raise LifecycleError("Invalid lifecycle hold")
        cases = value.get("cases", [])
        if not isinstance(cases, list):
            raise LifecycleError("Invalid case manifests")
        for oid, reasons in case_holds(cases).items():
            if any(reason not in value["holds"].get(oid, []) for reason in reasons):
                raise LifecycleError("Case dependencies are missing registered holds")
        return value

    def _save(self, value: dict) -> None:
        _reject_symlink_path(self.registry_path)
        write_json_atomic(self.registry_path, value, indent=2, ensure_ascii=False)

    def import_manifest(self, manifest: dict) -> dict:
        """Merge protection; imports never silently release an earlier hold."""
        if (
            manifest.get("schema_version") != 1
            or manifest.get("meta_root") != str(self.meta)
            or not isinstance(manifest.get("holds"), dict)
            or not isinstance(manifest.get("experiments", []), list)
            or not isinstance(manifest.get("reference_roots"), list)
        ):
            raise LifecycleError("Invalid reference import manifest")
        with lake_mutation_lock(self.meta):
            fingerprint = reference_fingerprint(manifest["reference_roots"], meta_root=self.meta)
            if fingerprint != manifest.get("reference_fingerprint"):
                raise LifecycleError("References changed since inventory; refresh the import")
            from cnequity.storage.lifecycle.snapshot import read_dependencies

            restored = read_dependencies(self.meta)
            inherited = restored["protection"] if restored else {}
            value = self.registry() or {
                "schema_version": 1,
                "lake_id": uuid.uuid4().hex,
                "meta_root": str(self.meta),
                "holds": inherited.get("holds", {}),
                "cases": inherited.get("cases", []),
                "pending": {},
                "experiments": [],
            }
            cases = {case["case_id"]: case for case in value.get("cases", [])}
            incoming_cases = manifest.get("cases", [])
            if not isinstance(incoming_cases, list):
                raise LifecycleError("Invalid imported cases")
            case_holds(incoming_cases)
            for case in incoming_cases:
                if case["case_id"] in cases and cases[case["case_id"]] != case:
                    raise LifecycleError("Case manifests are immutable; use a new case ID")
                cases[case["case_id"]] = case
            imported_holds = {
                oid: list(reasons)
                for oid, reasons in manifest["holds"].items()
                if isinstance(reasons, list)
            }
            if len(imported_holds) != len(manifest["holds"]):
                raise LifecycleError("Invalid imported holds")
            for oid, reasons in case_holds(list(cases.values())).items():
                imported_holds.setdefault(oid, []).extend(reasons)
            # Validate every hold before the single atomic registry replacement.
            for oid, reasons in imported_holds.items():
                if not isinstance(oid, str) or not isinstance(reasons, list) or not reasons:
                    raise LifecycleError("Invalid imported hold")
                value["holds"].setdefault(oid, [])
                for reason in reasons:
                    if reason not in value["holds"][oid]:
                        value["holds"][oid].append(reason)
                value["pending"].pop(oid, None)
                value.get("experiment_pending", {}).pop(oid, None)
            experiments = {item["object_id"]: item for item in value["experiments"]}
            for item in manifest.get("experiments", []):
                experiments[item["object_id"]] = item
                if item.get("references") or item.get("status") == "active":
                    value.get("experiment_pending", {}).pop(item["object_id"], None)
            value.update(
                {
                    "reference_roots": manifest["reference_roots"],
                    "reference_fingerprint": fingerprint,
                    "experiments": list(experiments.values()),
                    "cases": list(cases.values()),
                    "imported_at": datetime.now(timezone.utc).isoformat(),
                    "import_digest": digest(manifest),
                }
            )
            self._save(value)
            return value

    def hold(self, object_id: str, reason: str) -> None:
        if not reason.strip():
            raise LifecycleError("A hold requires a reason")
        with lake_mutation_lock(self.meta):
            value = self.registry(required=True)
            assert value is not None
            known = {o["object_id"] for o in self.inventory()}
            from cnequity.storage.lifecycle.resources import resource_exists

            known.update(item["object_id"] for item in value["experiments"])
            known.update(value.get("artifacts", {}))
            if object_id not in known and not resource_exists(self.meta, object_id):
                raise LifecycleError(f"Unknown retained object: {object_id}")
            value["holds"].setdefault(object_id, []).append(
                {
                    "reason": reason,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            value["pending"].pop(object_id, None)
            value.get("experiment_pending", {}).pop(object_id, None)
            self._save(value)

    def register_case(self, case: dict) -> None:
        """Add an immutable, evidence-backed case without weakening any hold."""
        additions = case_holds([case])
        evidence = case.get("evidence")
        if not isinstance(evidence, list) or not evidence:
            raise LifecycleError("Case registration requires source evidence")
        with lake_mutation_lock(self.meta, timeout=30):
            for item in evidence:
                path = Path(item["path"])
                _reject_symlink_path(path)
                if (
                    not path.is_absolute()
                    or not path.is_file()
                    or sha256_file(path) != item["sha256"]
                ):
                    raise LifecycleError("Case evidence changed or is unavailable")
            value = self.registry(required=True)
            cases = {item["case_id"]: item for item in value.get("cases", [])}
            if case["case_id"] in cases and cases[case["case_id"]] != case:
                raise LifecycleError("Case manifests are immutable; use a new case ID")
            cases[case["case_id"]] = case
            for oid, reasons in additions.items():
                existing = value["holds"].setdefault(oid, [])
                existing.extend(reason for reason in reasons if reason not in existing)
                value["pending"].pop(oid, None)
                value.get("experiment_pending", {}).pop(oid, None)
            value["cases"] = list(cases.values())
            self._save(value)

    def inventory(self) -> list[dict]:
        root = self.meta / "revisions"
        _reject_symlink_path(root)
        if not root.exists():
            return []
        store = RevisionStore(self.meta, self.meta.parent / "curated", create=False)
        data = root / "data"
        _reject_symlink_path(data)
        datasets = {p.name for p in root.iterdir() if p.name != "data" and p.is_dir()}
        if data.exists():
            datasets.update(p.name for p in data.iterdir() if p.is_dir())
        result = []
        for dataset in sorted(datasets):
            if not _TOKEN.fullmatch(dataset):
                raise LifecycleError(f"Invalid dataset directory: {dataset}")
            _reject_symlink_path(root / dataset)
            generation_root = data / dataset
            _reject_symlink_path(generation_root)
            pointer = store.current_pointer(dataset)
            receipts = {}
            for path in sorted((root / dataset).glob("*.json")):
                if path.name == "current.json":
                    continue
                record = _read(path)
                rid, number = record.get("revision_id"), record.get("revision")
                if (
                    not isinstance(rid, str)
                    or not _TOKEN.fullmatch(rid)
                    or type(number) is not int
                    or number < 1
                    or record.get("dataset") != dataset
                    or path.name != f"{number:08d}-{rid}.json"
                ):
                    raise LifecycleError(f"Invalid revision receipt: {path}")
                # Receipts without retained bytes remain provenance only.
                receipts[rid] = (record, sha256_file(path))
            if not generation_root.exists():
                continue
            for path in sorted(generation_root.iterdir()):
                _reject_symlink_path(path)
                if not path.is_dir():
                    raise LifecycleError(f"Unexpected generation entry: {path}")
                rid = path.name
                record, receipt_digest = receipts.get(rid, ({}, None))
                relative = path.relative_to(self.meta).as_posix()
                reasons = []
                if not pointer:
                    reasons.append("missing_current_pointer")
                if not record:
                    reasons.append("unreceipted_generation")
                elif record.get("generation_path") != relative:
                    raise LifecycleError(f"Generation path disagrees with receipt: {path}")
                files = record.get("generation_files")
                if not isinstance(files, list):
                    reasons.append("missing_file_manifest")
                    files = []
                for item in files:
                    file_path = Path(item["path"])
                    if (
                        file_path.is_absolute()
                        or ".." in file_path.parts
                        or len(file_path.parts) < 2
                        or file_path.parts[0] != dataset
                        or type(item.get("size_bytes")) is not int
                        or item["size_bytes"] < 0
                        or not re.fullmatch(r"[0-9a-f]{64}", item.get("sha256", ""))
                    ):
                        raise LifecycleError(f"Invalid generation file manifest: {path}")
                info = path.stat()
                result.append(
                    {
                        "object_id": f"revision/{dataset}/{rid}",
                        "dataset": dataset,
                        "revision_id": rid,
                        "revision": record.get("revision", 0),
                        "path": relative,
                        "receipt_digest": receipt_digest,
                        "directory_identity": [info.st_dev, info.st_ino, info.st_mtime_ns],
                        "logical_bytes": sum(f["size_bytes"] for f in files),
                        "exclusive_bytes": _exclusive_tree_bytes(path),
                        "current": bool(pointer and pointer["revision_id"] == rid),
                        "pointer_digest": digest(pointer),
                        "blocked_reasons": reasons,
                    }
                )
        return result

    def inspect(self, *, keep: int = 5) -> dict:
        if keep < 1:
            raise LifecycleError("keep must be at least 1")
        registry = self.registry()
        objects = self.inventory()
        for dataset in {o["dataset"] for o in objects}:
            valid = sorted(
                (o for o in objects if o["dataset"] == dataset and not o["blocked_reasons"]),
                key=lambda o: o["revision"],
            )
            for obj in valid[-keep:]:
                obj["blocked_reasons"].append("recent_generation")
        for obj in objects:
            oid = obj["object_id"]
            if obj["current"]:
                obj["blocked_reasons"].append("current_generation")
            obj["holds"] = registry["holds"].get(oid, []) if registry else []
            if obj["holds"]:
                obj["blocked_reasons"].append("explicit_hold")
            obj["pending"] = registry["pending"].get(oid) if registry else None
        return {
            "schema_version": 1,
            "meta_root": str(self.meta),
            "keep": keep,
            "lake_id": registry["lake_id"] if registry else None,
            "registry_digest": digest(registry),
            "objects": objects,
            # Experiment and artifact entries an older version registered stay
            # in the registry for compatibility, but nothing acts on them now.
            "resource_holds": {
                k: v
                for k, v in registry["holds"].items()
                if not k.startswith(("revision/", "experiment/", "artifact/"))
            }
            if registry
            else {},
            "purge_available": True,
        }

    def _plan(self, keep: int) -> dict:
        registry = self.registry(required=True)
        assert registry is not None
        if (
            reference_fingerprint(registry["reference_roots"], meta_root=self.meta)
            != registry["reference_fingerprint"]
        ):
            raise LifecycleError("Reference inventory changed; refresh holds before planning")
        report = self.inspect(keep=keep)
        candidates = [o for o in report["objects"] if not o["blocked_reasons"]]
        for obj in candidates:
            obj["tree_signature"] = _tree_signature(self.meta / obj["path"])
        report["candidate_ids"] = [o["object_id"] for o in candidates]
        report["logical_bytes_selected"] = sum(o["logical_bytes"] for o in candidates)
        report["grace_days"] = GRACE_DAYS
        return report

    def plan(
        self, *, keep: int = 5, phase: str = "mark", selected_ids: list[str] | None = None
    ) -> dict:
        from cnequity.storage.lifecycle.purge import prepare, prepare_selected, unfinished

        if phase not in ("mark", "purge"):
            raise LifecycleError("Invalid lifecycle plan phase")
        if selected_ids is not None and phase != "purge":
            raise LifecycleError(_("A manual selection can only be used for deletion."))
        with lake_mutation_lock(self.meta):
            if unfinished(self):
                raise LifecycleError("Resume unfinished purge before creating another plan")
            report = self._plan(keep)
            if selected_ids is not None:
                report = prepare_selected(self, report, selected_ids)
            elif phase == "purge":
                report = prepare(self, report)
            report["phase"] = phase
            report["created_at"] = datetime.now(timezone.utc).isoformat()
            plan_id = digest(report)
            path = self.root / "plans" / f"{plan_id}.json"
            _reject_symlink_path(path)
            write_json_atomic(path, report, indent=2, ensure_ascii=False)
            return {"plan_id": plan_id, **report}

    def read_plan(self, plan_id: str) -> dict:
        if not re.fullmatch(r"[0-9a-f]{64}", plan_id):
            raise LifecycleError("Invalid plan ID")
        plan = _read(self.root / "plans" / f"{plan_id}.json")
        if digest(plan) != plan_id:
            raise LifecycleError("Plan was modified")
        return plan

    def purge(
        self, plan_id: str, *, maintenance: bool = False, manifest: Path | None = None
    ) -> dict:
        from cnequity.storage.lifecycle.purge import execute

        return execute(self, plan_id, maintenance=maintenance, manifest=manifest)

    def mark(self, plan_id: str) -> dict:
        from cnequity.storage.lifecycle.purge import unfinished

        with lake_mutation_lock(self.meta):
            if unfinished(self):
                raise LifecycleError("Resume unfinished purge before marking revisions")
            plan = self.read_plan(plan_id)
            if plan.get("phase", "mark") != "mark":
                raise LifecycleError("A mark plan is required")
            expected = {k: v for k, v in plan.items() if k not in ("created_at", "phase")}
            if self._plan(plan["keep"]) != expected:
                raise LifecycleError("Plan is stale; refresh the plan")
            value = self.registry(required=True)
            assert value is not None
            now = datetime.now(timezone.utc)
            candidates = set(plan["candidate_ids"])
            # A new protection or changed content restarts the observation period.
            pending = {}
            for obj in plan["objects"]:
                oid = obj["object_id"]
                if oid not in candidates:
                    continue
                previous = value["pending"].get(oid)
                if previous and previous.get("tree_signature") == obj["tree_signature"]:
                    pending[oid] = previous
                else:
                    pending[oid] = {
                        "marked_at": now.isoformat(),
                        "not_before": (now + timedelta(days=GRACE_DAYS)).isoformat(),
                        "tree_signature": obj["tree_signature"],
                        "plan_id": plan_id,
                    }
            value["pending"] = pending
            self._save(value)
            return {
                "plan_id": plan_id,
                "marked": len(pending),
                "logical_bytes_selected": plan["logical_bytes_selected"],
                "logical_bytes_deleted": 0,
                "purge_available": True,
                "earliest_not_before": min(
                    (p["not_before"] for p in pending.values()),
                    default=None,
                ),
            }
