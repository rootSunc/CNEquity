"""Revision retention: registered holds, references and reviewed deletion.

One retention model: keep the newest N generations per dataset
(``[storage.retention].keep_generations``). Backfill and the daily run delete
the rest right away (``revisions.retain_revision_generations``); a person can
also delete chosen history earlier, through an immutable plan and a journalled
purge on the dashboard. Current versions, holds and references always stay.

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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cnequity.config.retention import DEFAULT_KEEP_GENERATIONS, revision_keep
from cnequity.file_lock import lake_mutation_lock
from cnequity.file_replace import replace_with_retry
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.revisions import (
    RevisionStore,
    _exclusive_tree_bytes,
    _reject_symlink_path,
    sha256_file,
)

TEXT_SUFFIXES = {".json", ".jsonl", ".md", ".py", ".toml", ".yaml", ".yml"}
_TOKEN = re.compile(r"[a-zA-Z0-9_-]+\Z")


class LifecycleError(RuntimeError):
    """Incomplete protection information or a stale lifecycle operation."""


def digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def _releasable(reason: dict) -> bool:
    """A manual hold, or a reference whose citing file no longer cites it."""
    if "reason" in reason and "kind" not in reason:
        return True
    if reason.get("kind") != "reference":
        return False
    source = Path(reason.get("source", ""))
    try:
        text = source.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return True
    return reason.get("revision_id") not in set(_REVISION_ID.findall(text))


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
_REVISION_ID = re.compile(r"(?<![a-f0-9])[a-f0-9]{32}(?![a-f0-9])")

RUNTIME_META_PATHS = (
    "adj_factor_arbitration_cache",
    "checkpoints",
    "derivations",
    "derivatives/annual_archives",
    "derivatives/futures_bars",
    "derivatives/greeks_dependencies",
    "derivatives/http_cache",
    "derivatives/option_bars",
    # Contract reference data and rejected rows, rewritten by each capture.
    "derivatives/quarantine",
    "derivatives/references",
    "instruments_absence_streak.json",
    # The migration ledger `cne upgrade` keeps.
    "lake.json",
    "lifecycle",
    "locks",
    "payment_date_repairs",
    "quality",
    "rate_limits",
    "raw",
    "raw_storage.json",
    "repairs",
    "revisions",
    # The dashboard timer's tick and schedule records (every minute), and the
    # daily run's markers and streaks.
    "scheduler",
    "source_cache",
    "source_health",
    "source_snapshots",
    # Key-check verdicts; the key itself is never written here.
    "sources",
    "stability",
    "state",
    "stats",
    # Tushare's per-interface permission cache, rewritten by the peer audit.
    "tushare",
)


def _snapshot_runtime() -> tuple[str, ...]:
    return tuple(Path(p).as_posix() for p in RUNTIME_META_PATHS if p != "revisions")


def _in_snapshot_runtime(relative: Path) -> bool:
    """Whether *relative* (to the lake's meta) is a runtime path inside a research snapshot.

    A snapshot under ``meta/snapshots/<name>/meta/`` carries a copy of the
    lake's metadata, raw archive included: hundreds of thousands of files the
    lake itself is never scanned for. Its runtime paths are skipped the same
    way, except its revision records, which name the revisions it captured.
    """
    parts = relative.parts
    if len(parts) < 4 or parts[0] != "snapshots" or parts[2] != "meta":
        return False
    return _under("/".join(parts[3:]), _snapshot_runtime())


def _under(relative: str, prefixes: tuple[str, ...]) -> bool:
    """Whether the POSIX path *relative* is one of *prefixes* or inside one.

    String tests rather than ``Path.parents``: a reference scan visits several
    hundred thousand entries, and building every ancestor as a ``Path`` made
    it take most of a minute.
    """
    return any(relative == p or relative.startswith(p + "/") for p in prefixes)


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
        lake_root_meta = lake_meta is not None and root.resolve() == lake_meta
        if lake_root_meta:
            excluded += [Path(p) for p in RUNTIME_META_PATHS]
        skipped = tuple(p.as_posix() for p in excluded)

        # os.scandir rather than os.walk: the entry's type comes with the
        # listing, so the several hundred thousand entries need no lstat each.
        pending = [(str(root), "")]
        while pending:
            directory, prefix = pending.pop()
            try:
                with os.scandir(directory) as entries:
                    listed = list(entries)
            except OSError as error:
                raise LifecycleError(f"Reference scan failed: {error}") from error
            for entry in listed:
                relative = prefix + entry.name
                if lake_root_meta and _in_snapshot_runtime(Path(relative)):
                    continue
                if _under(relative, skipped):
                    continue
                try:
                    if entry.is_symlink():
                        raise LifecycleError(f"Reference scan contains a link: {entry.path}")
                    is_dir = entry.is_dir(follow_symlinks=False)
                    is_file = not is_dir and entry.is_file(follow_symlinks=False)
                except OSError as error:
                    raise LifecycleError(f"Reference scan failed: {error}") from error
                if is_dir:
                    pending.append((entry.path, relative + "/"))
                elif is_file and Path(entry.name).suffix in TEXT_SUFFIXES:
                    found.add(Path(entry.path))
    return sorted(found)


def reference_fingerprint(roots: list[dict], *, meta_root: Path | None = None) -> str:
    files = reference_files(roots, meta_root=meta_root)
    memo = _HashMemo.for_meta(meta_root)
    try:
        return digest([(str(p), memo.sha256(p)) for p in files])
    finally:
        memo.save()


class _HashMemo:
    """File digests remembered by file identity, so an unchanged tree is not read again.

    A reference inventory is hundreds of megabytes (snapshot manifests, audit
    evidence); hashing it all took about a minute on every storage page view
    and every retention run. A file whose device, inode, size, mtime and ctime
    are unchanged has the same bytes: ctime cannot be set back by a writer, so
    this does not trust a preserved mtime. Kept in the user's cache directory,
    never in the lake or the reference roots it fingerprints.
    """

    _memos: dict[str, _HashMemo] = {}

    def __init__(self, path: Path | None):
        self.path = path
        self.entries: dict[str, list] = {}
        self.dirty = False
        if path is not None:
            try:
                loaded = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    self.entries = loaded
            except (OSError, ValueError):
                pass

    @classmethod
    def for_meta(cls, meta_root: Path | None) -> _HashMemo:
        key = str(Path(meta_root).resolve()) if meta_root is not None else ""
        memo = cls._memos.get(key)
        if memo is None:
            path = None
            if meta_root is not None:
                base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
                name = hashlib.sha256(key.encode()).hexdigest()[:16]
                path = Path(base).expanduser() / "cnequity" / "reference-hashes" / f"{name}.json"
            memo = cls._memos[key] = cls(path)
        return memo

    def sha256(self, path: Path) -> str:
        info = path.stat()
        identity = [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns]
        key = str(path)
        hit = self.entries.get(key)
        if isinstance(hit, list) and hit[:-1] == identity:
            return hit[-1]
        value = sha256_file(path)
        self.entries[key] = [*identity, value]
        self.dirty = True
        return value

    def save(self) -> None:
        if not self.dirty or self.path is None:
            return
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(f".{os.getpid()}.tmp")
            tmp.write_text(json.dumps(self.entries), encoding="utf-8")
            replace_with_retry(tmp, self.path, sync_dir=False)
            self.dirty = False
        except OSError:
            pass


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
            or value.get("meta_root") != str(self.meta)
            or not isinstance(value.get("lake_id"), str)
            or not isinstance(value.get("holds"), dict)
            or not isinstance(value.get("pending", {}), dict)
            or not isinstance(value.get("reference_roots"), list)
            or not isinstance(value.get("reference_fingerprint"), str)
        ):
            raise LifecycleError("Invalid lifecycle registry or copied lake identity")
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
            value.update(
                {
                    "reference_roots": manifest["reference_roots"],
                    "reference_fingerprint": fingerprint,
                    "cases": list(cases.values()),
                    "imported_at": datetime.now(timezone.utc).isoformat(),
                    "import_digest": digest(manifest),
                }
            )
            self._save(value)
            return value

    def refresh_references(self) -> dict | None:
        """Re-scan the registered reference roots and hold what they now cite.

        Every retained revision whose 32-hex id appears in a reference file is
        held, exactly as a reviewed import would hold it, and the inventory
        fingerprint is brought up to date. Imports only add protection, so
        doing this automatically releases nothing: a revision a new note
        cites becomes protected, and one a note stopped citing stays held
        until someone releases it. Returns the summary, or None when no
        reference inventory has ever been imported.

        Without this, retention refused to run from the first edit to any
        file under the roots — a note in ``private/``, a snapshot manifest —
        until someone re-ran a manual import, and old generations piled up.
        """
        registry = self.registry()
        if registry is None:
            return None
        known: dict[str, set[str]] = {}
        revisions = self.meta / "revisions"
        for path in revisions.glob("*/*.json") if revisions.is_dir() else ():
            if path.name == "current.json":
                continue
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if isinstance(record, dict) and record.get("revision_id") and record.get("dataset"):
                known.setdefault(record["revision_id"], set()).add(
                    f"revision/{record['dataset']}/{record['revision_id']}"
                )
        for dataset, revision_id in self._catalog_revisions():
            known.setdefault(revision_id, set()).add(f"revision/{dataset}/{revision_id}")
        roots = registry["reference_roots"]
        holds: dict[str, list[dict]] = {}
        fingerprints = []
        for path in reference_files(roots, meta_root=self.meta):
            raw = path.read_bytes()
            fingerprints.append((str(path), hashlib.sha256(raw).hexdigest()))
            cited = set(_REVISION_ID.findall(raw.decode("utf-8", errors="replace")))
            for revision_id in sorted(cited & known.keys()):
                # Stable across edits of the citing file, so a re-scan after
                # an unrelated edit adds no duplicate reason.
                reason = {"kind": "reference", "source": str(path), "revision_id": revision_id}
                for object_id in known[revision_id]:
                    holds.setdefault(object_id, []).append(reason)
        before = set(registry["holds"])
        self.import_manifest(
            {
                "schema_version": 1,
                "meta_root": str(self.meta),
                "reference_roots": roots,
                "reference_fingerprint": digest(fingerprints),
                "holds": holds,
            }
        )
        return {
            "reference_files": len(fingerprints),
            "cited_revisions": len(holds),
            "new_holds": sorted(set(holds) - before),
        }

    def _catalog_revisions(self) -> set[tuple[str, str]]:
        """Revisions the lake catalog records and still holds: they have no receipt file."""
        from cnequity.storage.revision_records import records_for

        records = records_for(self.meta)
        return set() if records is None else records.revision_ids()

    def hold(self, object_id: str, reason: str) -> None:
        if not reason.strip():
            raise LifecycleError("A hold requires a reason")
        with lake_mutation_lock(self.meta):
            value = self.registry()
            if value is None:
                from cnequity.storage.lifecycle.snapshot import read_dependencies

                restored = read_dependencies(self.meta)
                inherited = restored["protection"] if restored else {}
                value = {
                    "schema_version": 1,
                    "lake_id": uuid.uuid4().hex,
                    "meta_root": str(self.meta),
                    "holds": inherited.get("holds", {}),
                    "cases": inherited.get("cases", []),
                    "reference_roots": [],
                    "reference_fingerprint": digest([]),
                }
            known = {o["object_id"] for o in self.inventory()}
            known |= {f"revision/{d}/{rid}" for d, rid in self._catalog_revisions()}
            from cnequity.storage.lifecycle.resources import resource_exists

            if object_id not in known and not resource_exists(self.meta, object_id):
                raise LifecycleError(f"Unknown retained object: {object_id}")
            value["holds"].setdefault(object_id, []).append(
                {
                    "reason": reason,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            self._save(value)

    def release(self, object_id: str, note: str) -> dict:
        """Drop the holds on one object that no longer protect anything real.

        Removes every manual hold, and every reference whose source file is
        gone or no longer cites the revision. Case dependencies and live
        references stay; they are reported so the caller sees why the object
        is still kept. Each release is logged in the registry.
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

    def release_references_under(self, root: Path, note: str) -> list[dict]:
        """Drop stale references whose citing file lay below ``root``.

        Used after a snapshot directory is deleted: only the references it
        contributed go, so manual holds and other citations are untouched.
        """
        root = Path(root).absolute()

        def stale_below(reason: dict) -> bool:
            if reason.get("kind") != "reference" or "reason" in reason:
                return False
            source = Path(reason.get("source", ""))
            return source.is_relative_to(root) and _releasable(reason)

        with lake_mutation_lock(self.meta):
            value = self.registry()
            if value is None:
                return []
            results = []
            for object_id in sorted(value["holds"]):
                if any(stale_below(r) for r in value["holds"][object_id]):
                    kept, dropped = self._drop(value, object_id, note, stale_below)
                    results.append(
                        {"object_id": object_id, "dropped": len(dropped), "still_held": bool(kept)}
                    )
            if results:
                self._save(value)
        return results

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
        from cnequity.storage.catalog_bridge import revision_snapshots
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
            for dataset, rid, snap, _unique in revision_snapshots(catalog):
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

    def inventory(self) -> list[dict]:
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
                            "exclusive_bytes": _exclusive_tree_bytes(path),
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

    def inspect(self, *, keep: int = DEFAULT_KEEP_GENERATIONS) -> dict:
        keep = revision_keep(None, keep)
        registry = self.registry()
        objects = self.inventory()
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

    def retention_registry(self, *, refresh: bool = False) -> dict | None:
        """Protection for an expiry plan, with optional refresh under the mutation lock."""
        from cnequity.storage.lifecycle.purge import unfinished

        if unfinished(self):
            raise LifecycleError("Resume unfinished purge before revision retention")
        return self._checked_reference_registry(refresh=refresh)

    def _checked_reference_registry(
        self, *, refresh: bool = False, required: bool = False
    ) -> dict | None:
        registry = self.registry(required=required)
        if registry is not None and (
            reference_fingerprint(registry["reference_roots"], meta_root=self.meta)
            != registry["reference_fingerprint"]
        ):
            if not refresh:
                if required:
                    raise LifecycleError(
                        "Reference inventory changed; refresh holds before planning"
                    )
                raise LifecycleError(
                    "References changed; refresh the lifecycle import before cleanup"
                )
            self.refresh_references()
            registry = self.registry(required=True)
        return registry

    def _plan(self, keep: int) -> dict:
        self._checked_reference_registry(required=True)
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
