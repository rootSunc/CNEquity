"""Inspect revision protection and begin a non-destructive observation period."""

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


@cli.group(cls=_LifecycleGroup)
def storage():
    """Version retention holds and observation periods; physical deletion must be confirmed on the serve web page."""


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
@click.option("--keep", type=click.IntRange(min=1), default=5, show_default=True)
def inspect(config_path: str, keep: int):
    """List versions and their retention reasons, read-only."""
    _emit(_store(config_path).inspect(keep=keep))


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


@storage.command("hold")
@config_option
@click.argument("object_id")
@click.option("--reason", required=True)
def hold(config_path: str, object_id: str, reason: str):
    """Retain a version or an existing cleanup resource, and cancel its pending-deletion mark."""
    _store(config_path).hold(object_id, reason)
    _emit({"object_id": object_id, "held": True})


@storage.command("plan")
@config_option
@click.option("--keep", type=click.IntRange(min=1), default=5, show_default=True)
@click.option("--phase", type=click.Choice(["mark", "purge"]), default="mark", show_default=True)
def plan(config_path: str, keep: int, phase: str):
    """Check references and save an immutable version cleanup plan; deletes no data."""
    result = _store(config_path).plan(keep=keep, phase=phase)
    summary = {
        k: result[k]
        for k in (
            "plan_id",
            "logical_bytes_selected",
            "candidate_ids",
            "grace_days",
            "purge_available",
            "phase",
        )
    }
    if phase == "purge":
        summary.update({k: result[k] for k in ("purge_ids", "logical_bytes_to_purge")})
    _emit(summary)


@storage.command("apply")
@config_option
@click.argument("plan_id")
@click.option(
    "--phase",
    type=click.Choice(["mark", "purge"]),
    default="mark",
    show_default=True,
    help=N_(
        "mark marks in place; purge is disabled, use the serve storage operations page instead."
    ),
)
@click.option(
    "--maintenance-window",
    is_flag=True,
    help=N_("Compatibility option; cannot bypass the web delete confirmation."),
)
def apply(config_path: str, plan_id: str, phase: str, maintenance_window: bool):
    """Mark after reviewing the plan; the CLI does not allow physical deletion."""
    if phase == "purge":
        raise click.ClickException(
            _(
                "Physical deletion must be reviewed and confirmed on the storage operations page of cne serve; the CLI only allows previewing and marking."
            )
        )
    else:
        _emit(_store(config_path).mark(plan_id))


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
