"""Read-only backup inventory and bounded previews for portable lake snapshots."""

from __future__ import annotations

import hashlib
from pathlib import Path

from cnequity.domain.datasets import DATASETS
from cnequity.i18n import _
from cnequity.ops.catalog import OpsError
from cnequity.storage.locator import layer_root
from cnequity.storage.snapshots import SnapshotStore, _reject_symlink_path


def store_for(config, root: str | None = None) -> SnapshotStore:
    if root is None:
        from cnequity.ops.scheduler import settings

        root = settings(config).get("backup_root")
    return SnapshotStore(config, Path(root) if root else None)


def snapshot_names(config) -> list[str]:
    return [item["name"] for item in inventory(config)["snapshots"] if not item.get("error")]


def create_root(config, root: str | None = None) -> Path:
    path = store_for(config, root).root
    _reject_symlink_path(path, label="snapshot root")
    resolved = path.resolve()
    lake = Path(config.data_root).resolve()
    if resolved == lake or lake in resolved.parents:
        allowed = [Path(config.meta_root).resolve() / "snapshots", lake / "backups"]
        if not any(resolved == base or base in resolved.parents for base in allowed):
            raise OpsError(
                _(
                    "Backups inside the lake may only go under meta/snapshots or backups, so they never mix with business data; a directory outside the lake also works."
                )
            )
    if path.exists() and not path.is_dir():
        raise OpsError(_("The backup directory is not a directory."))
    return path


def stored_datasets(config) -> list[str]:
    """Use publication pointers, not a scan of parquet contents."""
    from cnequity.storage.revisions import resolve_committed_root

    result = []
    for name, spec in DATASETS.items():
        base = layer_root(config, spec)
        try:
            path = resolve_committed_root(base / name, dataset=name, meta_root=config.meta_root)
        except (OSError, ValueError, RuntimeError):
            continue
        if path.is_dir():
            result.append(name)
    return sorted(result)


def inventory(config, root: str | None = None) -> dict:
    if config is None:
        return {"root": None, "snapshots": [], "datasets": []}
    store = store_for(config, root)
    _reject_symlink_path(store.root, label="snapshot root")
    if store.root.exists() and not store.root.is_dir():
        raise OpsError(_("The backup directory is not a directory."))
    rows = []
    if store.root.is_dir():
        for path in sorted(store.root.iterdir(), key=lambda p: p.name, reverse=True):
            if path.name.startswith("."):
                continue
            try:
                _path, manifest = store._manifest(path.name)
                files = manifest.get("files")
                datasets = manifest.get("datasets")
                if not isinstance(datasets, list) or not all(
                    isinstance(item, str) for item in datasets
                ):
                    raise ValueError("invalid snapshot datasets")
                if not isinstance(files, list) or not all(isinstance(item, dict) for item in files):
                    raise ValueError("invalid snapshot file inventory")
                rows.append(
                    {
                        "name": path.name,
                        "created_at": manifest.get("created_at"),
                        "datasets": datasets,
                        "files": len(files),
                        "bytes": sum(int(item.get("size_bytes", 0)) for item in files),
                        "verified": False,
                    }
                )
            except (OSError, ValueError, RuntimeError, KeyError, TypeError) as exc:
                rows.append({"name": path.name, "error": str(exc)})
    return {"root": str(store.root), "snapshots": rows, "datasets": stored_datasets(config)}


def restore_target(config, text: str, snapshot_root: str | None = None) -> Path:
    path = Path(text).expanduser()
    if not path.is_absolute():
        raise OpsError(_("The restore target must be an absolute path."))
    _reject_symlink_path(path, label="restore target")
    resolved = path.resolve()
    for forbidden in (
        Path(config.data_root).resolve(),
        store_for(config, snapshot_root).root.resolve(),
    ):
        if resolved == forbidden or forbidden in resolved.parents or resolved in forbidden.parents:
            raise OpsError(
                _("The restore target must be outside the current lake and the backup directory.")
            )
    if resolved.exists() and (not resolved.is_dir() or any(resolved.iterdir())):
        raise OpsError(_("The restore target must be new or an empty directory."))
    return path


def describe(config, op: str, params: dict) -> tuple[str, str | None]:
    store = store_for(config, params.get("snapshot_root"))
    if op == "snapshot.create":
        if store.path(params["name"]).exists():
            raise OpsError(_("This backup name already exists; enter a new name."))
        return (
            _(
                "Data backup: {value}\nTarget: {value2}\nCopies the selected published data, state, contracts and lineage; does not contact data sources. This is not a system image and includes no original config, credentials or full run database."
            ).format(value=", ".join(params["datasets"]), value2=store.path(params["name"])),
            None,
        )
    path, manifest = store._manifest(params["name"])
    if (
        not isinstance(manifest.get("datasets"), list)
        or not all(isinstance(item, str) for item in manifest["datasets"])
        or not isinstance(manifest.get("files"), list)
    ):
        raise OpsError(_("The backup manifest format is invalid."))
    digest = hashlib.sha256((path / "manifest.json").read_bytes()).hexdigest()
    if op == "snapshot.restore":
        restore_target(config, params["target"], params.get("snapshot_root"))
    text = _("Backup: {name}\nDatasets: {value}\nFiles: {value2}").format(
        name=params["name"],
        value=", ".join(manifest.get("datasets", [])),
        value2=len(manifest.get("files", [])),
    )
    if op == "snapshot.restore":
        text += _(
            "\nRestore target: {target}\nDigests are fully verified when it runs, and nothing is restored if verification fails; afterwards the current service still uses the original lake."
        ).format(target=params["target"])
    else:
        text += _(
            "\nRecomputes every file digest; the verification result is saved in the task output."
        )
    return text, digest


def check_manifest(config, params: dict, expected: str | None) -> None:
    if expected is None:
        return
    try:
        path = store_for(config, params.get("snapshot_root")).path(params["name"]) / "manifest.json"
        matched = hashlib.sha256(path.read_bytes()).hexdigest() == expected
    except (OSError, ValueError) as exc:
        raise OpsError(_("The backup manifest cannot be read; preview again.")) from exc
    if not matched:
        raise OpsError(_("The backup manifest changed after the preview; preview again."))
