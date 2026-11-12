"""Inspect revision retention: what is kept, why, and what retention would delete next."""

from __future__ import annotations

import json
from pathlib import Path

import click

from cnequity.cli._root import cli
from cnequity.cli._shared import _cfg, config_option
from cnequity.file_lock import LockUnavailable
from cnequity.i18n import N_, _
from cnequity.storage.lifecycle import LifecycleError, LifecycleStore
from cnequity.storage.raw_archive import RawArchiveError, RawPayloadArchive
from cnequity.storage.revisions import RevisionConsistencyError


class _LifecycleGroup(click.Group):
    def invoke(self, ctx: click.Context):
        try:
            return super().invoke(ctx)
        except (
            LifecycleError,
            RevisionConsistencyError,
            RawArchiveError,
            LockUnavailable,
            OSError,
            ValueError,
        ) as exc:
            raise click.ClickException(str(exc)) from exc


def _store(config_path: str) -> LifecycleStore:
    return LifecycleStore(_cfg(config_path).meta_root)


def _emit(value: dict) -> None:
    click.echo(json.dumps(value, indent=2, ensure_ascii=False))


def _keep(config_path: str, keep: int | None) -> int:
    """``--keep``, else the count backfill and the daily run keep (5 when they keep all)."""
    if keep is not None:
        return keep
    configured = getattr(_cfg(config_path), "revision_keep_generations", 5)
    return configured if isinstance(configured, int) and configured >= 1 else 5


_KEEP_HELP = N_(
    "Generations to keep per dataset; defaults to [storage.retention].keep_generations."
)


@cli.group(cls=_LifecycleGroup)
def storage():
    """Version retention: holds, references and a preview of what retention deletes.

    \b
    Backfill and the daily run keep the newest [storage.retention].keep_generations
    generations of each dataset and delete older ones. Deleting chosen history
    earlier is confirmed on the storage page of `cne serve`.
    """


@storage.command("raw-external")
@config_option
@click.option("--dataset", required=True)
@click.option("--source", required=True)
@click.option(
    "--root", type=click.Path(exists=True, file_okay=False, path_type=Path), required=True
)
@click.option("--format", "formats", multiple=True, default=("pdf",), show_default=True)
def raw_external(config_path: str, dataset: str, source: str, root: Path, formats: tuple[str, ...]):
    """Register external payload storage; retains local sidecars and moves/deletes no files."""
    cfg = _cfg(config_path)
    _emit(
        RawPayloadArchive(cfg.meta_root).register_external(dataset, source, root, formats=formats)
    )


@storage.command("inspect")
@config_option
@click.option("--keep", type=click.IntRange(min=1), default=None, help=_KEEP_HELP)
def inspect(config_path: str, keep: int | None):
    """List versions and their retention reasons, read-only."""
    _emit(_store(config_path).inspect(keep=_keep(config_path, keep)))


@storage.command("explain")
@config_option
@click.argument("object_id")
def explain(config_path: str, object_id: str):
    """Show why one version is retained."""
    report = _store(config_path).inspect()
    for obj in report["objects"]:
        if obj["object_id"] == object_id:
            _emit(obj)
            return
    reasons = report["resource_holds"].get(object_id)
    if reasons:
        _emit({"object_id": object_id, "holds": reasons})
        return
    raise click.ClickException(f"Unknown retained object: {object_id}")


@storage.command("import")
@config_option
@click.option(
    "--manifest", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True
)
def import_references(config_path: str, manifest: Path):
    """Import a reviewed reference list; only adds protection and never releases old holds automatically."""
    value = _store(config_path).import_manifest(json.loads(manifest.read_text(encoding="utf-8")))
    _emit(
        {
            "lake_id": value["lake_id"],
            "held_objects": len(value["holds"]),
        }
    )


@storage.command("refresh")
@config_option
def refresh_references(config_path: str):
    """Re-scan the registered reference roots and hold every version they now cite; never releases a hold."""
    summary = _store(config_path).refresh_references()
    if summary is None:
        raise click.ClickException("No reference inventory has been imported for this lake.")
    _emit(summary)


@storage.command("sync-catalog")
@config_option
@click.option(
    "--create",
    is_flag=True,
    help=N_(
        "Create the lake catalog when it does not exist yet; the first run copies every dataset."
    ),
)
def sync_catalog(config_path: str, create: bool):
    """Bring the lake catalog to the committed revision of every dataset; reads the revision store only."""
    from cnequity.storage.catalog_bridge import lake_root, sync_all, sync_summary
    from equitylake_core.catalog import CATALOG_FILE, Catalog

    config = _cfg(config_path)
    root = lake_root(config)
    if not create and not (root / CATALOG_FILE).exists():
        raise click.ClickException(
            _("No lake catalog at {root}; run again with --create to build it.").format(root=root)
        )
    with Catalog(root, create=create, memory_limit="8GB", temp_directory=root / "tmp") as catalog:
        _emit(sync_summary(sync_all(config, catalog)))


@storage.command("hold")
@config_option
@click.argument("object_id")
@click.option("--reason", required=True)
def hold(config_path: str, object_id: str, reason: str):
    """Retain a version or an existing cleanup resource; retention then never deletes it."""
    _store(config_path).hold(object_id, reason)
    _emit({"object_id": object_id, "held": True})


@storage.command("release")
@config_option
@click.argument("object_ids", nargs=-1, required=True)
@click.option("--note", required=True, help=N_("Why the protection is no longer needed."))
def release(config_path: str, object_ids: tuple[str, ...], note: str):
    """Drop manual holds and stale references on versions; case dependencies and live references stay."""
    store = _store(config_path)
    _emit({"released": [store.release(object_id, note) for object_id in object_ids]})


@storage.command("plan")
@config_option
@click.option("--keep", type=click.IntRange(min=1), default=None, help=_KEEP_HELP)
@click.option("--phase", type=click.Choice(["mark", "purge"]), default=None, hidden=True)
def plan(config_path: str, keep: int | None, phase: str | None):
    """Preview which generations retention would delete now; deletes nothing."""
    from cnequity.storage.revisions import preview_revision_retention

    if phase is not None:
        click.echo(
            _("--phase is no longer used: there is no mark phase, and this only previews."),
            err=True,
        )
    keep = _keep(config_path, keep)
    preview = preview_revision_retention(_cfg(config_path).meta_root, keep=keep)
    _emit(
        {
            "keep": keep,
            "candidate_ids": [
                f"revision/{item.dataset}/{rid}"
                for item in preview
                for rid in item.candidate_revision_ids
            ],
            "logical_bytes_selected": sum(item.logical_bytes_selected for item in preview),
        }
    )


@storage.command("apply", hidden=True, context_settings={"ignore_unknown_options": True})
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def apply(args: tuple[str, ...]):
    """Removed: marking versions for a waiting period."""
    raise click.ClickException(
        _(
            "`cne storage apply` was removed with the mark-and-wait phase: backfill and the daily run already delete generations beyond [storage.retention].keep_generations. Keep a version with `cne storage hold`; delete chosen history earlier on the storage page of `cne serve`."
        )
    )


@storage.command("merge-files")
@config_option
@click.argument("datasets", nargs=-1)
@click.option(
    "--min-files",
    type=click.IntRange(min=2),
    default=None,
    help=N_("Merge partitions with at least this many files (default 4)."),
)
@click.option(
    "--apply", is_flag=True, help=N_("Publish the merge; without it, only report what would merge.")
)
def merge_files(config_path: str, datasets: tuple[str, ...], min_files: int | None, apply: bool):
    """Merge partitions that incremental writes left in several files.

    \b
    New keys are written as a file of their own instead of rewriting the partition, so
    partitions gather files over time. `cne run maintain` merges them after each daily
    update; run this to merge now or to see what would merge.
    """
    from cnequity.storage.merge_files import MERGE_AT_FILES, merge_all, merge_dataset_files

    cfg = _cfg(config_path)
    threshold = min_files or MERGE_AT_FILES
    if datasets:
        reports = [
            merge_dataset_files(cfg, name, apply=apply, min_files=threshold) for name in datasets
        ]
    else:
        reports = merge_all(cfg, apply=apply, min_files=threshold)
    _emit({"applied": apply, "min_files": threshold, "datasets": reports})
