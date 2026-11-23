"""Inspect revision retention: what is kept, why, and what retention would delete next."""

from __future__ import annotations

import json
from pathlib import Path

import click

from cnequity.cli._root import cli
from cnequity.cli._shared import _cfg, attach_log_file, config_option
from cnequity.config.retention import revision_keep
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
    return revision_keep(_cfg(config_path) if keep is None else None, keep)


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
@click.option("--keep", type=click.IntRange(min=0), default=None, help=_KEEP_HELP)
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
def import_holds(config_path: str, manifest: Path):
    """Import reviewed holds and cases; only adds protection and never releases old holds."""
    value = _store(config_path).import_manifest(json.loads(manifest.read_text(encoding="utf-8")))
    _emit(
        {
            "lake_id": value["lake_id"],
            "held_objects": len(value["holds"]),
        }
    )


@storage.command("catalog-retention")
@config_option
@click.option(
    "--apply",
    is_flag=True,
    help=N_(
        "Expire old catalog snapshots and delete files past the grace period; without it, only report."
    ),
)
def catalog_retention_cmd(config_path: str, apply: bool):
    """Keep each dataset's recent and held revisions in the lake catalog; expire the rest."""
    from cnequity.storage.catalog_bridge import catalog_retention

    config = _cfg(config_path)
    attach_log_file(config, "storage-catalog-retention")
    result = catalog_retention(config, apply=apply)
    if result is None:
        raise click.ClickException(_("This lake has no lake catalog."))
    _emit(result)


#: Parts under this size share one line of the table; ``--json`` lists every part.
_SMALL_PART = 10_000_000


def _gb(value: int) -> str:
    return f"{value / 1e9:,.2f} GB"


@storage.command("usage")
@config_option
@click.option("--json", "as_json", is_flag=True, help=N_("Output machine-readable JSON."))
def usage_cmd(config_path: str, as_json: bool):
    """Show where the lake's disk space goes and what deleting each part would free; reads only."""
    from cnequity.storage.usage import storage_usage

    report = storage_usage(_cfg(config_path))
    if as_json:
        _emit(report)
        return
    say = click.echo
    disk = report["disk"]
    say(
        _("Disk: {free} free of {total}").format(
            free=_gb(disk["free_bytes"]), total=_gb(disk["total_bytes"])
        )
    )
    total = report["total"]
    say(
        _("{root}: {files} files, {allocated} on disk").format(
            root=report["data_root"],
            files=f"{total['files']:,}",
            allocated=_gb(total["allocated_bytes"]),
        )
    )
    if not report["clone_aware"]:
        say(_("Shared clone blocks cannot be seen here; what deletion frees may be less."))
    say("")
    part, files, on_disk, frees = _("Part"), _("Files"), _("On disk"), _("Frees at least")
    say(f"{part:<36} {files:>10} {on_disk:>12} {frees:>15}")
    small = [p for p in report["parts"].values() if p["allocated_bytes"] < _SMALL_PART]
    for name, part in report["parts"].items():
        if part["allocated_bytes"] >= _SMALL_PART:
            say(
                f"{name:<36} {part['files']:>10,} {_gb(part['allocated_bytes']):>12} "
                f"{_gb(part['freeable_bytes']):>15}"
            )
    if small:
        label = _("{count} smaller parts").format(count=len(small))
        files = sum(p["files"] for p in small)
        allocated = sum(p["allocated_bytes"] for p in small)
        freeable = sum(p["freeable_bytes"] for p in small)
        say(f"{label:<36} {files:>10,} {_gb(allocated):>12} {_gb(freeable):>15}")
    lake = report.get("lake_files")
    if lake:
        say("")
        say(_("Lake data files by catalog state:"))
        labels = {
            "current": _("in the current snapshot"),
            "history": _("kept for older revisions"),
            "expired": _("expired, deleted by retention after the grace period"),
            "unregistered": _("registered nowhere"),
        }
        for state, label in labels.items():
            say(
                f"  {label:<54} {lake[state]['files']:>8,} {_gb(lake[state]['allocated_bytes']):>12}"
            )
        first, last = lake["expired_between"]
        if first:
            say(
                _(
                    "Expired between {first} and {last}; `cne storage catalog-retention --apply` "
                    "deletes each file once {hours} hours have passed since it expired."
                ).format(hours=lake["grace_hours"], first=first, last=last)
            )


@storage.command("export")
@config_option
@click.argument("datasets", nargs=-1, required=True)
@click.option(
    "--out",
    "out",
    required=True,
    type=click.Path(file_okay=False),
    help=N_("Directory to write to; each dataset goes into its own empty subdirectory."),
)
@click.option(
    "--start",
    type=click.DateTime(["%Y-%m-%d"]),
    default=None,
    help=N_("First date to export (inclusive), by the dataset's date column."),
)
@click.option(
    "--end",
    type=click.DateTime(["%Y-%m-%d"]),
    default=None,
    help=N_("Last date to export (inclusive), by the dataset's date column."),
)
def export_cmd(config_path: str, datasets: tuple[str, ...], out: str, start, end):
    """Export published datasets from the lake catalog to plain Parquet for tools that read files directly."""
    from pathlib import Path

    from cnequity.storage.catalog_bridge import ExportRefused, export_dataset

    config = _cfg(config_path)
    attach_log_file(config, "storage-export")
    exported = []
    for dataset in datasets:
        try:
            exported.append(
                export_dataset(
                    config,
                    dataset.lower(),
                    Path(out),
                    start=start.date() if start else None,
                    end=end.date() if end else None,
                )
            )
        except ExportRefused as exc:
            _emit({"exported": exported})
            raise click.ClickException(str(exc)) from exc
    _emit({"exported": exported})


@storage.command("verify-catalog")
@config_option
@click.option("--dataset", default=None, help=N_("Check only this dataset's revisions."))
def verify_catalog(config_path: str, dataset: str | None):
    """Check that each revision published into the lake catalog still holds the rows recorded for it."""
    from cnequity.config.resources import resources_for
    from cnequity.storage.catalog_bridge import lake_root
    from cnequity.storage.lake_write import verify_revisions
    from equitylake_core.catalog import CATALOG_FILE, Catalog
    from equitylake_core.lock import lake_read

    config = _cfg(config_path)
    attach_log_file(config, "storage-verify-catalog")
    root = lake_root(config)
    if not (root / CATALOG_FILE).exists():
        raise click.ClickException(_("This lake has no lake catalog."))
    with (
        lake_read(root),
        Catalog(
            root,
            read_only=True,
            **resources_for(config, "maintenance").options(scratch=root / "tmp"),
        ) as catalog,
    ):
        checks = verify_revisions(catalog, dataset)
    counts: dict[str, int] = {}
    for check in checks:
        counts[check.status] = counts.get(check.status, 0) + 1
    mismatched = [
        {"dataset": c.dataset, "revision_id": c.revision_id, "snapshot": c.snapshot}
        for c in checks
        if c.status == "mismatch"
    ]
    _emit({"revisions": len(checks), "status": counts, "mismatched": mismatched})
    if mismatched:
        raise SystemExit(1)


@storage.command("hold")
@config_option
@click.argument("object_id")
@click.option("--reason", required=True)
@click.option(
    "--until",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help=N_("Last day the hold protects (YYYY-MM-DD); without it the hold stays until released."),
)
def hold(config_path: str, object_id: str, reason: str, until):
    """Retain a version or an existing cleanup resource; retention does not delete it while held."""
    last_day = until.date() if until is not None else None
    _store(config_path).hold(object_id, reason, until=last_day)
    _emit(
        {
            "object_id": object_id,
            "held": True,
            "until": last_day.isoformat() if last_day else None,
        }
    )


@storage.command("release")
@config_option
@click.argument("object_ids", nargs=-1, required=True)
@click.option("--note", required=True, help=N_("Why the protection is no longer needed."))
def release(config_path: str, object_ids: tuple[str, ...], note: str):
    """Drop manual holds on versions; case dependencies stay."""
    store = _store(config_path)
    _emit({"released": [store.release(object_id, note) for object_id in object_ids]})


@storage.command("plan")
@config_option
@click.option("--keep", type=click.IntRange(min=0), default=None, help=_KEEP_HELP)
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
