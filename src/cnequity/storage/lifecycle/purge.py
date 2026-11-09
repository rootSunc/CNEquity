"""Maintenance-window deletion of chosen revisions, with durable, resumable receipts.

External file readers do not participate in the lake lock. The caller must
stop them and new jobs before explicitly confirming the maintenance window.
"""

from __future__ import annotations

import shutil
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from cnequity.file_lock import lake_mutation_lock
from cnequity.i18n import _
from cnequity.storage.atomic import replace_with_retry, write_json_atomic
from cnequity.storage.lifecycle import LifecycleError, _read, _tree_signature, digest
from cnequity.storage.revisions import _reject_symlink_path, sha256_file


def check_idle(manifest: Path) -> None:
    _reject_symlink_path(manifest)
    if not manifest.exists():
        return
    try:
        with closing(
            sqlite3.connect(manifest.resolve().as_uri() + "?mode=ro", uri=True)
        ) as connection:
            active = connection.execute(
                "SELECT run_id FROM ingestion_runs WHERE status = 'running' LIMIT 1"
            ).fetchone()
    except sqlite3.Error as exc:
        raise LifecycleError("Cannot verify that ingestion is idle") from exc
    if active:
        raise LifecycleError(f"Ingestion is running: {active[0]}")


def unfinished(store) -> list[Path]:
    found = []
    for name in ("purges", "staging-purges"):
        events = store.root / name
        _reject_symlink_path(events)
        found.extend(
            p for p in sorted(events.glob("*.json")) if _read(p).get("status") != "complete"
        )
    return found


def verify_content(store, obj: dict, root: Path, *, partial: bool = False) -> int:
    """Check every remaining byte against the immutable generation receipt."""
    _reject_symlink_path(root)
    if not root.is_dir():
        raise LifecycleError(f"Missing generation: {root}")
    info = root.stat()
    if [info.st_dev, info.st_ino] != obj["directory_identity"][:2]:
        raise LifecycleError(f"Generation identity changed: {root}")
    receipt_path = (
        store.meta
        / "revisions"
        / obj["dataset"]
        / (f"{obj['revision']:08d}-{obj['revision_id']}.json")
    )
    if sha256_file(receipt_path) != obj["receipt_digest"]:
        raise LifecycleError(f"Receipt changed: {receipt_path}")
    receipt = _read(receipt_path)
    expected = {}
    directories = set()
    for item in receipt["generation_files"]:
        relative = Path(item["path"]).relative_to(obj["dataset"])
        if relative.is_absolute() or ".." in relative.parts or relative in expected:
            raise LifecycleError("Invalid or duplicate file in receipt")
        expected[relative] = item
        directories.update(p for p in relative.parents if p != Path("."))
    actual = set()
    # The strict walker rejects links and special files before reading contents.
    _tree_signature(root)
    for path in root.rglob("*"):
        relative = path.relative_to(root)
        if path.is_dir():
            if relative not in directories:
                raise LifecycleError(f"Unreceipted directory: {path}")
            continue
        actual.add(relative)
        item = expected.get(relative)
        if (
            item is None
            or path.stat().st_size != item["size_bytes"]
            or sha256_file(path) != item["sha256"]
        ):
            raise LifecycleError(f"Generation content differs from receipt: {path}")
    if not partial and actual != set(expected):
        raise LifecycleError(f"Generation file list differs from receipt: {root}")
    return sum(expected[p]["size_bytes"] for p in actual)


def explicit_deletable(obj: dict) -> bool:
    """Operator-chosen history. Current, holds, references and broken receipts stay."""
    if obj.get("current"):
        return False
    return set(obj.get("blocked_reasons") or []) <= {"recent_generation"}


def prepare_selected(store, report: dict, object_ids: list[str]) -> dict:
    """Purge exactly the chosen historical generations, including ones still among the newest N."""
    if unfinished(store):
        raise LifecycleError("An unfinished purge must be resumed before creating another plan")
    if not object_ids or len(object_ids) != len(set(object_ids)):
        raise LifecycleError(_("Choose historical versions without duplicates."))
    by_id = {o["object_id"]: o for o in report["objects"]}
    selected = []
    for oid in object_ids:
        obj = by_id.get(oid)
        if obj is None or not explicit_deletable(obj):
            raise LifecycleError(
                _(
                    "Cannot delete {oid}: it is the current version, or is still referenced or "
                    "retained, or has no receipt."
                ).format(oid=oid)
            )
        verify_content(store, obj, store.meta / obj["path"])
        selected.append(obj)
    report["purge_ids"] = [o["object_id"] for o in selected]
    report["logical_bytes_to_purge"] = sum(o["logical_bytes"] for o in selected)
    report["explicit_selection"] = True
    return report


def execute(store, plan_id: str, *, maintenance: bool, manifest: Path | None = None) -> dict:
    if not maintenance:
        raise LifecycleError(
            "Purge requires a confirmed maintenance window with all readers/jobs stopped"
        )
    plan = store.read_plan(plan_id)
    if plan.get("phase") != "purge":
        raise LifecycleError("A deletion plan is required")
    event_path = store.root / "purges" / f"{plan_id}.json"
    _reject_symlink_path(event_path)
    with lake_mutation_lock(store.meta, blocking=False):
        check_idle(manifest or store.meta / "manifest.db")
        registry = store.registry(required=True)
        if plan["lake_id"] != registry["lake_id"] or plan["meta_root"] != str(store.meta):
            raise LifecycleError("Purge belongs to another lake")
        journal = _read(event_path) if event_path.exists() else None
        if journal and (
            journal.get("plan_id") != plan_id or journal.get("lake_id") != plan["lake_id"]
        ):
            raise LifecycleError("Invalid purge journal identity")
        if any(p != event_path for p in unfinished(store)):
            raise LifecycleError("Resume the other unfinished purge first")
        objects = {o["object_id"]: o for o in plan["objects"]}
        selected = [objects[oid] for oid in plan["purge_ids"]]
        if journal is None:
            journal = {
                "schema_version": 1,
                "plan_id": plan_id,
                "lake_id": plan["lake_id"],
                "status": "prepared",
                "items": {},
                "logical_bytes_deleted": 0,
                "filesystem_free_bytes_before": shutil.disk_usage(store.meta).free,
            }
        if not isinstance(journal.get("items"), dict) or not set(journal["items"]).issubset(
            plan["purge_ids"]
        ):
            raise LifecycleError("Invalid purge journal items")
        if journal.get("schema_version") != 1 or journal.get("status") not in (
            "prepared",
            "error",
            "complete",
        ):
            raise LifecycleError("Invalid purge journal schema or state")
        if journal["status"] == "complete":
            if (
                set(journal["items"]) != set(plan["purge_ids"])
                or any(state != "deleted" for state in journal["items"].values())
                or journal.get("logical_bytes_deleted") != plan["logical_bytes_to_purge"]
            ):
                raise LifecycleError("Invalid completed purge journal")
            return journal
        # Quarantine paths are derived, never accepted from a journal or CLI.
        trash = store.root / "trash" / plan_id
        _reject_symlink_path(trash)
        removed_from_source = set()
        for obj in selected:
            oid = obj["object_id"]
            source = store.meta / obj["path"]
            target = trash / digest(oid)
            state = journal["items"].get(oid)
            _reject_symlink_path(source)
            _reject_symlink_path(target)
            if state not in (None, "moving", "deleting", "deleted"):
                raise LifecycleError("Invalid purge item state")
            if target.exists():
                if source.exists() or state not in ("moving", "deleting"):
                    raise LifecycleError("Conflicting quarantine/source state")
                verify_content(store, obj, target, partial=state == "deleting")
                removed_from_source.add(oid)
            elif not source.exists():
                if state not in ("deleting", "deleted"):
                    raise LifecycleError("Generation disappeared outside the purge")
                removed_from_source.add(oid)
            elif state in ("deleting", "deleted"):
                raise LifecycleError("Deleted generation path was reused")
        actual = store._plan(plan["keep"])
        expected = {
            k: v
            for k, v in plan.items()
            if k
            not in (
                "created_at",
                "phase",
                "purge_ids",
                "logical_bytes_to_purge",
                "explicit_selection",
            )
        }
        expected["objects"] = [
            o for o in expected["objects"] if o["object_id"] not in removed_from_source
        ]
        expected["candidate_ids"] = [
            oid for oid in expected["candidate_ids"] if oid not in removed_from_source
        ]
        expected["logical_bytes_selected"] = sum(
            o["logical_bytes"]
            for o in expected["objects"]
            if o["object_id"] in expected["candidate_ids"]
        )
        if actual != expected:
            raise LifecycleError("Purge plan is stale; protections or contents changed")
        # A plan of the retired mark-and-wait phase selected generations with
        # no protection at all, so the same check covers it.
        for obj in selected:
            if not explicit_deletable(obj):
                raise LifecycleError(
                    _("Cannot delete {oid}: its protection has changed.").format(
                        oid=obj["object_id"]
                    )
                )
            if obj["object_id"] not in removed_from_source:
                verify_content(store, obj, store.meta / obj["path"])
        # All candidates are validated before the first destructive operation.
        trash.mkdir(parents=True, exist_ok=True)
        if trash.stat().st_dev != store.meta.stat().st_dev:
            raise LifecycleError("Quarantine must be on the same filesystem")
        if any(o["directory_identity"][0] != trash.stat().st_dev for o in selected):
            raise LifecycleError("All selected generations must share the quarantine filesystem")

        def save() -> None:
            _reject_symlink_path(event_path)
            write_json_atomic(event_path, journal, indent=2)

        save()
        try:
            for obj in selected:
                oid = obj["object_id"]
                source, target = store.meta / obj["path"], trash / digest(oid)
                if journal["items"].get(oid) == "deleted":
                    continue
                if source.exists():
                    journal["items"][oid] = "moving"
                    save()
                    replace_with_retry(source, target)
                journal["items"][oid] = "deleting"
                save()
                if target.exists():
                    shutil.rmtree(target)
                journal["items"][oid] = "deleted"
                journal["logical_bytes_deleted"] = sum(
                    objects[key]["logical_bytes"]
                    for key, status in journal["items"].items()
                    if status == "deleted"
                )
                save()
            journal["status"] = "complete"
            journal.pop("error", None)
            journal.pop("partial_deletion_possible", None)
            journal["filesystem_free_bytes_after"] = shutil.disk_usage(store.meta).free
            journal["completed_at"] = datetime.now(timezone.utc).isoformat()
            save()
        except Exception as exc:
            journal["status"] = "error"
            journal["partial_deletion_possible"] = True
            journal["error"] = f"{type(exc).__name__}: {exc}"
            save()
            raise
        # The journal is authoritative; inventory no longer exposes physically
        # removed generations.
        return journal
