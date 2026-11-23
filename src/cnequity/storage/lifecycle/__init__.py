"""Revision retention: registered holds, references and reviewed deletion.

One retention model: keep the newest N generations per dataset
(``[storage.retention].keep_generations``). Backfill and the daily run delete
the rest right away (``revisions.retain_revision_generations``); a person can
also delete chosen history earlier, through an immutable plan and a journalled
purge on the dashboard. Current versions and holds always stay.

A hold is placed on purpose: ``cne storage hold`` (with an optional last day,
after which it lapses) or a reviewed case's dependency. Releases before 0.25.0
also held every revision whose id appeared in any text file under ``meta/`` or
the operator's notes; those holds never lapsed, pinned most of the lake's old
files, and are released by the ``explicit-holds`` migration.

A seven-day mark-and-wait phase used to sit beside this. Automatic retention
deleted the marked generations before their wait ended, so it protected
nothing and was removed; registries still carrying its ``pending`` marks load
without them.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from cnequity.config.retention import DEFAULT_KEEP_GENERATIONS, revision_keep
from cnequity.file_lock import lake_mutation_lock
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.revisions import (
    RevisionStore,
    _exclusive_tree_bytes,
    _reject_symlink_path,
    sha256_file,
)

_TOKEN = re.compile(r"[a-zA-Z0-9_-]+\Z")


class LifecycleError(RuntimeError):
    """Incomplete protection information or a stale lifecycle operation."""


def digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


#: Hold kinds releases before 0.25.0 derived from a revision id seen in a file.
#: They protect until the ``explicit-holds`` migration releases them.
RETIRED_HOLD_KINDS = frozenset({"reference", "unresolved_legacy_reference"})


def retired_hold(reason) -> bool:
    """Whether *reason* is one of :data:`RETIRED_HOLD_KINDS`."""
    return isinstance(reason, dict) and reason.get("kind") in RETIRED_HOLD_KINDS


def _releasable(reason: dict) -> bool:
    """A manual hold, or one of the retired kinds: anything but a case's dependency."""
    if not isinstance(reason, dict):
        return False
    if "reason" in reason and "kind" not in reason:
        return True
    return reason.get("kind") in RETIRED_HOLD_KINDS


def _lapsed(reason: dict, today: date) -> bool:
    """Whether a manual hold's last day (``expires_at``) has passed."""
    until = reason.get("expires_at") if isinstance(reason, dict) else None
    if until is None:
        return False
    try:
        return date.fromisoformat(until) < today
    except (TypeError, ValueError) as exc:
        raise LifecycleError(f"Invalid hold expiry: {until!r}") from exc


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
                    "Import a reviewed reference manifest before planning a deletion"
                )
            return None
        value = _read(self.registry_path)
        if (
            value.get("schema_version") != 1
            or not isinstance(value.get("meta_root"), str)
            or not isinstance(value.get("lake_id"), str)
            or not isinstance(value.get("holds"), dict)
            or not isinstance(value.get("pending", {}), dict)
        ):
            raise LifecycleError("Invalid lifecycle registry or copied lake identity")
        if value["meta_root"] != str(self.meta):
            raise LifecycleError(
                f"Copied lake identity: {self.registry_path} records "
                f"meta_root={value['meta_root']!r}, not {str(self.meta)!r}; "
                "update meta_root there if you moved or copied this lake on purpose"
            )
        for oid, reasons in value["holds"].items():
            if not isinstance(oid, str) or not isinstance(reasons, list) or not reasons:
                raise LifecycleError("Invalid lifecycle hold")
        cases = value.get("cases", [])
        if not isinstance(cases, list):
            raise LifecycleError("Invalid case manifests")
        for oid, reasons in case_holds(cases).items():
            if any(reason not in value["holds"].get(oid, []) for reason in reasons):
                raise LifecycleError("Case dependencies are missing registered holds")
        # Marks of the retired mark-and-wait phase (see the module docstring),
        # and the experiment and artifact registries an older version kept:
        # nothing acted on them, and their paths were temporary. The next
        # save drops them. Holds a case placed on artifact/ ids stay, since
        # case manifests are immutable and still name them.
        for retired in ("pending", "experiments", "artifacts", "experiment_pending"):
            value.pop(retired, None)
        # A hold past its last day protects nothing; the next save records its release.
        today = date.today()
        for oid in list(value["holds"]):
            lapsed = [r for r in value["holds"][oid] if _lapsed(r, today)]
            if lapsed:
                self._drop(value, oid, "hold expired", lambda r, gone=lapsed: r in gone)
        return value

    def _new_registry(self) -> dict:
        """An empty registry, carrying the protection a restored snapshot brought.

        Of a snapshot taken before 0.25.0, only manual holds and cases carry over:
        the retired reference holds named files of the lake the snapshot came from.
        """
        from cnequity.storage.lifecycle.snapshot import read_dependencies

        restored = read_dependencies(self.meta)
        inherited = restored["protection"] if restored else {}
        holds = {}
        for oid, reasons in inherited.get("holds", {}).items():
            kept = [r for r in reasons if not retired_hold(r)]
            if kept:
                holds[oid] = kept
        return {
            "schema_version": 1,
            "lake_id": uuid.uuid4().hex,
            "meta_root": str(self.meta),
            "holds": holds,
            "cases": inherited.get("cases", []),
        }

    def _save(self, value: dict) -> None:
        _reject_symlink_path(self.registry_path)
        write_json_atomic(self.registry_path, value, indent=2, ensure_ascii=False)

    def import_manifest(self, manifest: dict) -> dict:
        """Merge reviewed holds and cases; an import never releases an earlier hold."""
        if (
            manifest.get("schema_version") != 1
            or manifest.get("meta_root") != str(self.meta)
            or not isinstance(manifest.get("holds"), dict)
        ):
            raise LifecycleError("Invalid hold import manifest")
        if manifest.get("reference_roots"):
            raise LifecycleError(
                "Reference roots are retired: hold each revision with `cne storage hold`"
            )
        with lake_mutation_lock(self.meta):
            value = self.registry() or self._new_registry()
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
                if any(
                    not isinstance(r, dict) or r.get("kind") in RETIRED_HOLD_KINDS for r in reasons
                ):
                    raise LifecycleError("Reference holds are retired; import explicit holds")
                value["holds"].setdefault(oid, [])
                for reason in reasons:
                    if reason not in value["holds"][oid]:
                        value["holds"][oid].append(reason)
            value.update(
                {
                    "cases": list(cases.values()),
                    "imported_at": datetime.now(timezone.utc).isoformat(),
                    "import_digest": digest(manifest),
                }
            )
            self._save(value)
            return value

    def _catalog_revisions(self) -> set[tuple[str, str]]:
        """Revisions the lake catalog records and still holds: they have no receipt file."""
        from cnequity.storage.revision_records import records_for

        records = records_for(self.meta)
        return set() if records is None else records.revision_ids()

    def hold(self, object_id: str, reason: str, *, until: date | None = None) -> None:
        """Keep *object_id* from retention, until the end of *until* if given."""
        if not reason.strip():
            raise LifecycleError("A hold requires a reason")
        if until is not None and until < date.today():
            raise LifecycleError("A hold's last day cannot be in the past")
        with lake_mutation_lock(self.meta):
            value = self.registry() or self._new_registry()
            known = {o["object_id"] for o in self.inventory(measure=False)}
            known |= {f"revision/{d}/{rid}" for d, rid in self._catalog_revisions()}
            from cnequity.storage.lifecycle.resources import resource_exists

            if object_id not in known and not resource_exists(self.meta, object_id):
                raise LifecycleError(f"Unknown retained object: {object_id}")
            entry = {"reason": reason, "created_at": datetime.now(timezone.utc).isoformat()}
            if until is not None:
                entry["expires_at"] = until.isoformat()
            value["holds"].setdefault(object_id, []).append(entry)
            self._save(value)

    def release(self, object_id: str, note: str) -> dict:
        """Drop the manual holds on one object, and any retired reference holds.

        Case dependencies stay; they are reported so the caller sees why the
        object is still kept. Each release is logged in the registry.
        """
        if not note.strip():
            raise LifecycleError("A release requires a note")
        with lake_mutation_lock(self.meta):
            value = self.registry(required=True)
            assert value is not None
            if not value["holds"].get(object_id):
                raise LifecycleError(f"No hold on {object_id}")
            kept, dropped = self._drop(value, object_id, note, _releasable)
            self._save(value)
        return {"object_id": object_id, "dropped": len(dropped), "still_held_by": kept}

    @staticmethod
    def _drop(value: dict, object_id: str, note: str, releasable) -> tuple[list, list]:
        kept, dropped = [], []
        for reason in value["holds"][object_id]:
            (dropped if releasable(reason) else kept).append(reason)
        if kept:
            value["holds"][object_id] = kept
        else:
            value["holds"].pop(object_id)
        value.setdefault("releases", []).append(
            {
                "object_id": object_id,
                "note": note,
                "released_at": datetime.now(timezone.utc).isoformat(),
                "dropped": dropped,
            }
        )
        return kept, dropped

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
            value["cases"] = list(cases.values())
            self._save(value)

    def _catalog_inventory(self, seen: set[tuple[str, str]]) -> list[dict]:
        """Live revisions stored in the lake catalog (not legacy generation directories)."""
        from cnequity.meta_layout import LAKE_DIR
        from cnequity.storage import revision_records
        from cnequity.storage.catalog_bridge import revision_read_snapshots
        from equitylake_core.catalog import ALIAS, CATALOG_FILE, Catalog
        from equitylake_core.lock import lake_read

        if not revision_records.enabled(self.meta):
            return []
        lake_dir = self.meta.parent / LAKE_DIR
        if not (lake_dir / CATALOG_FILE).is_file():
            return []
        records = revision_records.records_for(self.meta)
        if records is None:
            return []
        with lake_read(lake_dir), Catalog(lake_dir, read_only=True) as catalog:
            live_snaps = {
                int(row[0])
                for row in catalog.execute(
                    f"SELECT snapshot_id FROM ducklake_snapshots('{ALIAS}')"
                ).fetchall()
            }
            snap_by_rev: dict[tuple[str, str], int] = {}
            for dataset, rid, snap, _unique in revision_read_snapshots(catalog):
                if int(snap) in live_snaps:
                    snap_by_rev[(dataset, rid)] = int(snap)
        items: list[dict] = []
        for dataset in sorted({ds for ds, _ in snap_by_rev}):
            current = records.current.get(dataset)
            pointer = current.pointer() if current is not None else None
            history_by_id = {rec.revision_id: rec for rec in records.history(dataset)}
            for (ds, rid), snap in sorted(
                ((k, v) for k, v in snap_by_rev.items() if k[0] == dataset),
                key=lambda kv: (
                    history_by_id[kv[0][1]].revision if kv[0][1] in history_by_id else 0,
                    kv[1],
                ),
            ):
                if (ds, rid) in seen:
                    continue
                rec = history_by_id.get(rid)
                meta = (rec.metadata or {}) if rec is not None else {}
                items.append(
                    {
                        "object_id": f"revision/{ds}/{rid}",
                        "dataset": ds,
                        "revision_id": rid,
                        "revision": rec.revision if rec is not None else 0,
                        "storage": "catalog",
                        "snapshot": snap,
                        "path": f"lake/catalog.sqlite@{snap}",
                        "receipt_digest": None,
                        "directory_identity": None,
                        "logical_bytes": int(meta.get("total_bytes") or 0),
                        "exclusive_bytes": 0,
                        "current": bool(current and current.revision_id == rid),
                        "content_changed": meta.get("content_changed") is not False,
                        "pointer_digest": digest(pointer),
                        "blocked_reasons": [],
                    }
                )
        return items

    def inventory(self, *, measure: bool = True) -> list[dict]:
        """Every retained revision; *measure* False leaves ``exclusive_bytes`` as None.

        Measuring walks every file of every legacy generation directory (a
        third of a storage page's build on a real lake) for a figure only the
        raw ``cne storage inspect`` report shows; plans measure it again.
        """
        root = self.meta / "revisions"
        _reject_symlink_path(root)
        result = []
        seen: set[tuple[str, str]] = set()
        if root.exists():
            store = RevisionStore(self.meta, self.meta.parent / "curated", create=False)
            data = root / "data"
            _reject_symlink_path(data)
            datasets = {p.name for p in root.iterdir() if p.name != "data" and p.is_dir()}
            if data.exists():
                datasets.update(p.name for p in data.iterdir() if p.is_dir())
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
                    seen.add((dataset, rid))
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
                            "exclusive_bytes": _exclusive_tree_bytes(path) if measure else None,
                            "current": bool(pointer and pointer["revision_id"] == rid),
                            # A merge that rewrote files without changing a row
                            # says so in its receipt; see `inspect`.
                            "content_changed": (record.get("metadata") or {}).get("content_changed")
                            is not False,
                            "pointer_digest": digest(pointer),
                            "blocked_reasons": reasons,
                        }
                    )
        result.extend(self._catalog_inventory(seen))
        return result

    def inspect(self, *, keep: int = DEFAULT_KEEP_GENERATIONS, measure: bool = True) -> dict:
        keep = revision_keep(None, keep)
        registry = self.registry()
        objects = self.inventory(measure=measure)
        for dataset in {o["dataset"] for o in objects}:
            valid = sorted(
                (o for o in objects if o["dataset"] == dataset and not o["blocked_reasons"]),
                key=lambda o: o["revision"],
            )
            if keep:
                for obj in _recent(valid, keep):
                    obj["blocked_reasons"].append("recent_generation")
        for obj in objects:
            oid = obj["object_id"]
            if obj["current"]:
                obj["blocked_reasons"].append("current_generation")
            obj["holds"] = registry["holds"].get(oid, []) if registry else []
            if obj["holds"]:
                obj["blocked_reasons"].append("explicit_hold")
        return {
            "schema_version": 1,
            "meta_root": str(self.meta),
            "keep": keep,
            "lake_id": registry["lake_id"] if registry else None,
            "registry_digest": digest(registry),
            "objects": objects,
            # Only cleaner-owned kinds are resource holds. Revisions have their
            # own retention, and experiment/ or artifact/ ids are names an
            # older version's cases still hold; nothing here acts on them.
            "resource_holds": {
                k: v
                for k, v in registry["holds"].items()
                if not k.startswith(("revision/", "experiment/", "artifact/"))
            }
            if registry
            else {},
            "purge_available": True,
        }

    def retention_registry(self) -> dict | None:
        """The holds an expiry plan keeps; refused while a purge is unfinished."""
        from cnequity.storage.lifecycle.purge import unfinished

        if unfinished(self):
            raise LifecycleError("Resume unfinished purge before revision retention")
        return self.registry()

    def _plan(self, keep: int) -> dict:
        report = self.inspect(keep=keep)
        candidates = [o for o in report["objects"] if not o["blocked_reasons"]]
        for obj in candidates:
            if obj.get("storage") == "catalog":
                obj["tree_signature"] = f"catalog-snapshot:{obj['snapshot']}"
            else:
                obj["tree_signature"] = _tree_signature(self.meta / obj["path"])
        report["candidate_ids"] = [o["object_id"] for o in candidates]
        report["logical_bytes_selected"] = sum(o["logical_bytes"] for o in candidates)
        return report

    def plan(self, *, selected_ids: list[str], keep: int = DEFAULT_KEEP_GENERATIONS) -> dict:
        """Save an immutable plan to delete exactly *selected_ids*; deletes nothing."""
        from cnequity.storage.lifecycle.purge import prepare_selected, unfinished

        with lake_mutation_lock(self.meta):
            if unfinished(self):
                raise LifecycleError("Resume unfinished purge before creating another plan")
            if self.registry() is None:
                # No hold was ever placed: an empty registry names the lake the plan binds to.
                self._save(self._new_registry())
            report = prepare_selected(self, self._plan(keep), selected_ids)
            report["phase"] = "purge"
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


def _recent(valid: list[dict], keep: int) -> list[dict]:
    """The generations *keep* protects, oldest first.

    The newest *keep* generations that changed content, and every generation
    after the oldest of them. A merge that only rewrote files is not history of
    its own, so it does not use up a place; it stays while it sits between
    kept generations, and goes with them when they age out.
    """
    counted = [obj for obj in valid if obj.get("content_changed", True)]
    if len(counted) <= keep:
        return valid
    floor = counted[-keep]["revision"]
    return [obj for obj in valid if obj["revision"] >= floor]
