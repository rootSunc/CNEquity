"""Contracts, profiles and snapshots — the reproducibility surface.

These are what let a published result name exactly the data it used: a
fingerprinted dataset contract, a versioned research universe, and an immutable
checksummed copy of the bytes.
"""

from __future__ import annotations

import json
import tarfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import click

from cnequity.cli._root import cli
from cnequity.cli._shared import (
    _cfg,
    attach_log_file,
    config_option,
)
from cnequity.i18n import N_, _


def _profile_list_payload(include_compatibility: bool) -> list[dict]:
    from cnequity.domain.universe_profiles import list_universe_profiles

    return list_universe_profiles(include_compatibility=include_compatibility)


def _profile_show_payload(name: str, symbols: tuple[str, ...]) -> dict:
    from cnequity.domain.universe_profiles import (
        resolve_universe_profile,
        show_universe_profile,
    )

    try:
        payload = show_universe_profile(name)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    if symbols:
        payload["concrete_scope_hash"] = resolve_universe_profile(name).symbol_scope_hash(symbols)
        payload["symbols"] = sorted(
            {symbol.strip().upper() for symbol in symbols if symbol.strip()}
        )
    return payload


@cli.group("profile")
def profile_grp():
    """View versioned research universe profiles."""


@profile_grp.command("list")
@click.option(
    "--include-compatibility/--official-only",
    default=True,
    show_default=True,
    help=N_("Include legacy universe aliases in the list."),
)
def profile_list(include_compatibility: bool):
    """List profile registry records (machine-readable)."""

    click.echo(
        json.dumps(_profile_list_payload(include_compatibility), ensure_ascii=False, indent=2)
    )


@profile_grp.command("show")
@click.argument("name")
@click.option(
    "--symbol",
    "symbols",
    multiple=True,
    help=N_("Bind the profile to concrete symbols and include concrete_scope_hash."),
)
def profile_show(name: str, symbols: tuple[str, ...]):
    """Show one versioned profile and its stable scope hash."""

    click.echo(json.dumps(_profile_show_payload(name, symbols), ensure_ascii=False, indent=2))


@cli.group("contract")
def contract_grp():
    """View and validate registered dataset data contracts."""


@contract_grp.command("show")
@click.argument("dataset", required=False)
@click.option(
    "--dataset",
    "dataset_option",
    default=None,
    help=N_("Dataset name (can also be passed as an argument). Omit to print the full contract."),
)
@click.option(
    "--out",
    "--output",
    "--path",
    "output_path",
    default="-",
    show_default=True,
    help=N_("Write the JSON to this path instead of stdout; '-' means print."),
)
@click.option("--json", "as_json", is_flag=True, help=N_("Output machine-readable JSON (default)."))
def contract_show(dataset: str | None, dataset_option: str | None, output_path: str, as_json: bool):
    """Show the contract of one dataset, or of the whole registry.

    \b
    `--out PATH` writes a file instead of printing: that file is the contract snapshot committed with a release,
    and exactly what `cne contract diff` reads back to judge whether a later registry is compatible or breaking.

    """
    from cnequity.domain.contracts import (
        build_contract,
        contract_json,
        dataset_contract,
        export_contract,
    )

    # Registry names are lower case, and command names are already
    # case-insensitive; a dataset typed in caps should resolve the same way.
    name = (dataset_option or dataset or "").lower() or None
    try:
        payload = dataset_contract(name) if name else build_contract()
    except KeyError as exc:
        raise click.ClickException(str(exc)) from exc
    # ``--json`` is intentionally a no-op today: JSON is the stable output
    # shape for this command. Keeping the option makes scripts explicit and
    # leaves room for a future human table without changing their invocation.
    del as_json

    if output_path == "-":
        click.echo(contract_json(payload))
        return
    if name:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(contract_json(payload) + "\n", encoding="utf-8")
    else:
        export_contract(output_path)
    click.echo(_("Written to {output_path}").format(output_path=output_path))


@contract_grp.command("diff")
@click.argument("old_contract", required=False)
@click.argument("new_contract", required=False)
@click.option("--old", "old_option", default=None, help=N_("Path to the baseline contract file."))
@click.option("--new", "new_option", default=None, help=N_("Path to the candidate contract file."))
@click.option("--from", "from_option", default=None, help=N_("Alias for --old."))
@click.option("--to", "to_option", default=None, help=N_("Alias for --new."))
@click.option("--json", "as_json", is_flag=True, help=N_("Output machine-readable JSON."))
@click.option(
    "--allow-breaking",
    is_flag=True,
    help=N_("Exit with code 0 even when breaking changes are found."),
)
def contract_diff(
    old_contract: str | None,
    new_contract: str | None,
    old_option: str | None,
    new_option: str | None,
    from_option: str | None,
    to_option: str | None,
    as_json: bool,
    allow_breaking: bool,
):
    """Compare OLD_CONTRACT with NEW_CONTRACT (the current registry by default)."""
    from cnequity.domain.contracts import contract_json, diff_contracts, format_contract_diff

    old_path = old_option or from_option or old_contract
    new_path = new_option or to_option or new_contract
    if old_path is None:
        raise click.UsageError(_("Give OLD_CONTRACT or --old/--from"))
    try:
        diff = diff_contracts(old_path, new_path)
    except (OSError, TypeError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    if as_json:
        click.echo(contract_json(diff))
    else:
        click.echo(format_contract_diff(diff))
    if diff["is_breaking"] and not allow_breaking:
        raise SystemExit(1)


@contract_grp.command("validate")
@click.argument("contract_path", required=False)
@click.option(
    "--path",
    "path_option",
    default=None,
    help=N_("Path to the contract JSON (can also be passed as an argument)."),
)
@click.option("--json", "as_json", is_flag=True, help=N_("Output machine-readable JSON."))
@click.option(
    "--against-registry",
    is_flag=True,
    help=N_(
        "Require the contract in the file to match the current DATASETS / SCHEMAS / PRIMARY_KEYS exactly."
    ),
)
def contract_validate(
    contract_path: str | None,
    path_option: str | None,
    as_json: bool,
    against_registry: bool,
):
    """Validate a contract file; without one, validate the current registry."""
    from cnequity.domain.contracts import contract_json, validate_contract

    contract_path = path_option or contract_path
    errors = validate_contract(
        contract_path,
        against_registry=True if (contract_path is None or against_registry) else False,
    )
    if as_json:
        click.echo(contract_json({"valid": not errors, "errors": errors}))
    elif errors:
        for error in errors:
            click.echo(f"ERROR: {error}", err=True)
    else:
        click.echo(_("Contract check passed"))
    if errors:
        raise SystemExit(1)


@contextmanager
def _snapshot_operator_errors() -> Iterator[None]:
    """Surface snapshot input/data problems as Click errors, not tracebacks.

    A snapshot that is missing, corrupt or fails verification is operator
    input, and the store signals it with a plain ``FileNotFoundError`` or
    ``ValueError``. ``export`` and ``import`` already made this call; the rest
    of the group printed a Python traceback for the identical class of
    failure, so naming a snapshot that does not exist looked like a crash.
    """
    try:
        yield
    except (OSError, ValueError, RuntimeError, KeyError, tarfile.TarError) as exc:
        raise click.ClickException(str(exc)) from exc


@cli.group("snapshot")
def snapshot_grp():
    """Create, verify and safely restore portable lake snapshots."""


@snapshot_grp.command("create")
@click.argument("name")
@click.option(
    "--research",
    is_flag=True,
    help=N_(
        "Include prices, factors, identity, status, calendar and research coverage evidence; errors if dependencies are missing."
    ),
)
@click.option(
    "--dataset",
    "datasets",
    multiple=True,
    required=True,
    help=N_(
        "Dataset to include (repeatable). Snapshots are always explicit, never the whole lake."
    ),
)
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help=N_("Where to put the snapshot; defaults to meta/snapshots under the data root."),
)
def snapshot_create(
    name: str,
    datasets: tuple[str, ...],
    config_path: str,
    snapshot_root: Path | None,
    research: bool,
):
    """Freeze the given datasets into a new, immutable snapshot.

    \b
    The manifest records each Parquet file's size and SHA-256, together with dataset state, contract fingerprint and run lineage:
    enough for someone later to prove that a published result used exactly these bytes. The command prints the manifest path.

    """
    cfg = _cfg(config_path)
    attach_log_file(cfg, "snapshot-create")
    from cnequity.storage.snapshots import SnapshotStore

    with _snapshot_operator_errors():
        manifest = SnapshotStore(cfg, snapshot_root).create(name, list(datasets), research=research)
    click.echo(str(manifest))


@snapshot_grp.command("verify")
@click.argument("name")
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help=N_("Where to put the snapshot; defaults to meta/snapshots under the data root."),
)
def snapshot_verify(name: str, config_path: str, snapshot_root: Path | None):
    """Re-hash every file in a snapshot and reconcile it with the manifest.

    \b
    Exits 1 as soon as a size or digest mismatches, so it can serve directly as a gate in a scheduled job.
    Run it before trusting a snapshot you did not just create:
    bit rot and a truncated copy look identical until the hashes disagree.

    """
    cfg = _cfg(config_path)
    attach_log_file(cfg, "snapshot-verify")
    from dataclasses import asdict

    from cnequity.storage.snapshots import SnapshotStore

    with _snapshot_operator_errors():
        result = SnapshotStore(cfg, snapshot_root).verify(name)
    click.echo(json.dumps(asdict(result), indent=2, ensure_ascii=False))
    if not result.passed:
        raise SystemExit(1)


@snapshot_grp.command("delete")
@click.argument("name")
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help=N_("Where to put the snapshot; defaults to meta/snapshots under the data root."),
)
@click.option("--yes", is_flag=True, help=N_("Delete without asking for confirmation."))
def snapshot_delete(name: str, config_path: str, snapshot_root: Path | None, yes: bool):
    """Delete a snapshot and release the retention references it contributed.

    \b
    Manual holds and references from anywhere else stay. Old versions the snapshot
    kept become reclaimable by the next retention run; current data is untouched.
    """
    cfg = _cfg(config_path)
    from cnequity.storage.snapshots import SnapshotStore

    store = SnapshotStore(cfg, snapshot_root)
    with _snapshot_operator_errors():
        if not (store.path(name) / "manifest.json").is_file():
            raise FileNotFoundError(f"no snapshot named {name!r}")
        if not yes:
            click.confirm(_("Delete snapshot %s permanently?") % name, abort=True)
        result = store.delete(name)
    click.echo(json.dumps(result, indent=2, ensure_ascii=False))


@snapshot_grp.command("restore")
@click.argument("name")
@click.argument("target", type=click.Path(path_type=Path))
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help=N_("Where to put the snapshot; defaults to meta/snapshots under the data root."),
)
def snapshot_restore(name: str, target: Path, config_path: str, snapshot_root: Path | None):
    """Restore a snapshot into TARGET, which must be new or an empty directory.

    \b
    The active lake root is refused, and existing files are never overwritten:
    restoring is for looking at an old version beside the current one, not for rolling back the live lake.
    Before pointing anything at it, create a separate config pointing at TARGET, then check the result with
    `cne status --datasets --config PATH`.

    """
    cfg = _cfg(config_path)
    attach_log_file(cfg, "snapshot-restore")
    from cnequity.storage.snapshots import SnapshotStore

    with _snapshot_operator_errors():
        restored = SnapshotStore(cfg, snapshot_root).restore(name, target)
    click.echo(str(restored))


@snapshot_grp.command("export")
@click.argument("name")
@click.argument("destination", type=click.Path(path_type=Path), required=False)
@click.option(
    "--compression",
    type=click.Choice(["auto", "zstd", "gzip", "none"]),
    default="auto",
    show_default=True,
    help=N_("Archive encoding; auto prefers tar.zst and falls back to tar.gz."),
)
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help=N_("Where to put the snapshot; defaults to meta/snapshots under the data root."),
)
def snapshot_export(
    name: str,
    destination: Path | None,
    compression: str,
    config_path: str,
    snapshot_root: Path | None,
):
    """Stream snapshot NAME into one portable tar archive."""
    cfg = _cfg(config_path)
    attach_log_file(cfg, "snapshot-export")
    from cnequity.storage.snapshots import SnapshotStore

    try:
        archive = SnapshotStore(cfg, snapshot_root).export_archive(
            name,
            destination,
            compression=compression,
        )
    except (OSError, ValueError, RuntimeError, KeyError, tarfile.TarError) as exc:
        # Snapshot validation failures are operator input/data errors.  Keep
        # Click's normal one-line error surface; a Python traceback is not
        # useful when an archive is missing, corrupt or fails verification.
        raise click.ClickException(str(exc)) from exc
    click.echo(str(archive))


@snapshot_grp.command("import")
@click.argument("archive", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option(
    "--name",
    default=None,
    help=N_("Name of the imported snapshot; defaults to the archive file name."),
)
@click.option(
    "--overwrite",
    is_flag=True,
    help=N_(
        "Replace an existing snapshot with the same name only after the archive passes verification."
    ),
)
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help=N_("Where to put the snapshot; defaults to meta/snapshots under the data root."),
)
def snapshot_import(
    archive: Path,
    name: str | None,
    overwrite: bool,
    config_path: str,
    snapshot_root: Path | None,
):
    """Verify ARCHIVE and import it into the snapshot store atomically."""
    cfg = _cfg(config_path)
    attach_log_file(cfg, "snapshot-import")
    from cnequity.storage.snapshots import SnapshotStore

    try:
        restored = SnapshotStore(cfg, snapshot_root).import_archive(
            archive,
            name=name,
            overwrite=overwrite,
        )
    except (OSError, ValueError, RuntimeError, KeyError, tarfile.TarError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(str(restored))


@snapshot_grp.group("delta")
def snapshot_delta_grp():
    """Create, verify and apply portable delta packages."""


def _snapshot_delta_create(
    name: str,
    baseline: Path | None,
    target: Path | None,
    from_revision: int | None,
    datasets: tuple[str, ...],
    config_path: str,
    snapshot_root: Path | None,
) -> None:
    from cnequity.storage.snapshots import SnapshotStore

    # ``--from 12`` was used by an early command draft before the explicit
    # ``--from-revision`` spelling existed.  Accept it when it is not a real
    # path, while preserving normal two-root paths named with digits.
    if from_revision is None and baseline is not None and not baseline.exists():
        raw = str(baseline)
        if raw.isdigit():
            from_revision = int(raw)
            baseline = None
    if from_revision is None and baseline is None:
        raise click.UsageError(_("Give --from BASELINE or --from-revision REVISION"))
    cfg = _cfg(config_path)
    attach_log_file(cfg, "snapshot-delta-create")
    with _snapshot_operator_errors():
        if from_revision is not None:
            manifest = SnapshotStore(cfg, snapshot_root).create_delta(
                name,
                datasets=list(datasets),
                target=target,
                from_revision=from_revision,
            )
        else:
            manifest = SnapshotStore(cfg, snapshot_root).create_delta(
                name,
                baseline=baseline,
                target=target,
                datasets=list(datasets) if datasets else None,
            )
    click.echo(str(manifest))


@snapshot_delta_grp.command("create")
@click.argument("name")
@click.option(
    "--from",
    "baseline",
    type=click.Path(path_type=Path),
    default=None,
    help=N_("Baseline lake root. The target root is compared against it byte for byte."),
)
@click.option(
    "--to",
    "target",
    type=click.Path(path_type=Path),
    default=None,
    help=N_("Target lake root; defaults to the currently active root in the config."),
)
@click.option(
    "--from-revision",
    type=int,
    default=None,
    help=N_("Use the revision committed in the target as the baseline precondition."),
)
@click.option(
    "--dataset",
    "datasets",
    multiple=True,
    help=N_(
        "Dataset to include (repeatable). If omitted, auto-discover datasets present in both roots."
    ),
)
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help=N_("Where to put the delta package; defaults to meta/snapshots under the data root."),
)
def snapshot_delta_create(
    name: str,
    baseline: Path | None,
    target: Path | None,
    from_revision: int | None,
    datasets: tuple[str, ...],
    config_path: str,
    snapshot_root: Path | None,
):
    """Create NAME as an immutable add/replace/delete package."""

    _snapshot_delta_create(
        name, baseline, target, from_revision, datasets, config_path, snapshot_root
    )


@snapshot_delta_grp.command("verify")
@click.argument("name")
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help=N_("Where to put the delta package; defaults to meta/snapshots under the data root."),
)
def snapshot_delta_verify(name: str, config_path: str, snapshot_root: Path | None):
    """Verify the hashes and change semantics of every add/replace payload."""
    cfg = _cfg(config_path)
    attach_log_file(cfg, "snapshot-delta-verify")

    from dataclasses import asdict

    from cnequity.storage.snapshots import SnapshotStore

    with _snapshot_operator_errors():
        result = SnapshotStore(cfg, snapshot_root).verify_delta(name)
    click.echo(json.dumps(asdict(result), indent=2, ensure_ascii=False))
    if not result.passed:
        raise SystemExit(1)


@snapshot_delta_grp.command("apply")
@click.argument("name")
@click.argument("target", type=click.Path(path_type=Path))
@click.option(
    "--dry-run", is_flag=True, help=N_("Only validate preconditions; do not modify TARGET.")
)
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help=N_("Where to put the delta package; defaults to meta/snapshots under the data root."),
)
def snapshot_delta_apply(
    name: str,
    target: Path,
    dry_run: bool,
    config_path: str,
    snapshot_root: Path | None,
):
    """Apply NAME safely onto the non-empty TARGET lake root."""
    cfg = _cfg(config_path)
    attach_log_file(cfg, "snapshot-delta-apply")

    from cnequity.storage.snapshots import SnapshotStore

    with _snapshot_operator_errors():
        applied = SnapshotStore(cfg, snapshot_root).apply_delta(name, target, dry_run=dry_run)
    click.echo(str(applied))
