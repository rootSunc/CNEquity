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
    config_option,
)


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
    """Inspect versioned research universe profiles."""


@profile_grp.command("list")
@click.option(
    "--include-compatibility/--official-only",
    default=True,
    show_default=True,
    help="Include legacy universe aliases in the registry listing.",
)
def profile_list(include_compatibility: bool):
    """List machine-readable profile registry records."""

    click.echo(
        json.dumps(_profile_list_payload(include_compatibility), ensure_ascii=False, indent=2)
    )


@profile_grp.command("show")
@click.argument("name")
@click.option(
    "--symbol",
    "symbols",
    multiple=True,
    help="Bind the profile to concrete symbols and include concrete_scope_hash.",
)
def profile_show(name: str, symbols: tuple[str, ...]):
    """Show one versioned profile and its stable scope hash."""

    click.echo(json.dumps(_profile_show_payload(name, symbols), ensure_ascii=False, indent=2))


@cli.group("contract")
def contract_grp():
    """Inspect and validate the registered dataset data contract."""


@contract_grp.command("show")
@click.argument("dataset", required=False)
@click.option(
    "--dataset",
    "dataset_option",
    default=None,
    help="Dataset name (an argument is also accepted). Omit for the full contract.",
)
@click.option(
    "--out",
    "--output",
    "--path",
    "output_path",
    default="-",
    show_default=True,
    help="Write the JSON to this path instead of stdout; '-' prints.",
)
@click.option("--json", "as_json", is_flag=True, help="Machine-readable JSON (the default).")
def contract_show(dataset: str | None, dataset_option: str | None, output_path: str, as_json: bool):
    """Show one dataset contract, or the complete registry contract.

    `--out PATH` writes it instead of printing: that file is a contract vintage
    to commit beside a release, and it is what `cne contract diff` reads back to
    classify a later registry as compatible or breaking.
    """
    from cnequity.domain.contracts import (
        build_contract,
        contract_json,
        dataset_contract,
        export_contract,
    )

    name = dataset_option or dataset
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
    click.echo(f"Wrote {output_path}")


@contract_grp.command("diff")
@click.argument("old_contract", required=False)
@click.argument("new_contract", required=False)
@click.option("--old", "old_option", default=None, help="Baseline contract path.")
@click.option("--new", "new_option", default=None, help="Candidate contract path.")
@click.option("--from", "from_option", default=None, help="Alias for --old.")
@click.option("--to", "to_option", default=None, help="Alias for --new.")
@click.option("--json", "as_json", is_flag=True, help="Machine-readable JSON output.")
@click.option(
    "--allow-breaking",
    is_flag=True,
    help="Return exit code 0 even when breaking changes are found.",
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
    """Compare OLD_CONTRACT with NEW_CONTRACT (default: current registry)."""
    from cnequity.domain.contracts import contract_json, diff_contracts, format_contract_diff

    old_path = old_option or from_option or old_contract
    new_path = new_option or to_option or new_contract
    if old_path is None:
        raise click.UsageError("provide OLD_CONTRACT or --old/--from")
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
    "--path", "path_option", default=None, help="Contract JSON path (an argument is also accepted)."
)
@click.option("--json", "as_json", is_flag=True, help="Machine-readable JSON output.")
@click.option(
    "--against-registry",
    is_flag=True,
    help="Require a file contract to match the current DATASETS/SCHEMAS/PRIMARY_KEYS exactly.",
)
def contract_validate(
    contract_path: str | None,
    path_option: str | None,
    as_json: bool,
    against_registry: bool,
):
    """Validate a contract file, or the current registry when omitted."""
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
        click.echo("Contract OK")
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
    "--dataset",
    "datasets",
    multiple=True,
    required=True,
    help="Dataset to include (repeatable). A snapshot is explicit, never the whole lake.",
)
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help="Where snapshots live; default is meta/snapshots under the data root.",
)
def snapshot_create(
    name: str, datasets: tuple[str, ...], config_path: str, snapshot_root: Path | None
):
    """Freeze the named datasets into a new immutable snapshot.

    The manifest records every Parquet file's size and SHA-256 alongside the
    dataset state, the contract fingerprint and the run lineage — enough for a
    reader to prove later that a published result used exactly these bytes.
    Prints the manifest path.
    """
    from cnequity.storage.snapshots import SnapshotStore

    with _snapshot_operator_errors():
        manifest = SnapshotStore(_cfg(config_path), snapshot_root).create(name, list(datasets))
    click.echo(str(manifest))


@snapshot_grp.command("verify")
@click.argument("name")
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help="Where snapshots live; default is meta/snapshots under the data root.",
)
def snapshot_verify(name: str, config_path: str, snapshot_root: Path | None):
    """Re-hash every file in the snapshot against its manifest.

    Exits 1 on the first size or digest mismatch, so it works as a gate in a
    scheduled job. Run it before trusting a snapshot you did not just create —
    bit rot and a truncated copy look identical until the hashes disagree.
    """
    from dataclasses import asdict

    from cnequity.storage.snapshots import SnapshotStore

    with _snapshot_operator_errors():
        result = SnapshotStore(_cfg(config_path), snapshot_root).verify(name)
    click.echo(json.dumps(asdict(result), indent=2, ensure_ascii=False))
    if not result.passed:
        raise SystemExit(1)


@snapshot_grp.command("restore")
@click.argument("name")
@click.argument("target", type=click.Path(path_type=Path))
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help="Where snapshots live; default is meta/snapshots under the data root.",
)
def snapshot_restore(name: str, target: Path, config_path: str, snapshot_root: Path | None):
    """Restore a snapshot into TARGET, which must be new or empty.

    An active lake root is refused and an existing file is never overwritten:
    restoring is how you inspect an old vintage beside the current one, not how
    you roll the live lake back. Check the result with
    `cne status --datasets` against TARGET before pointing anything at it.
    """
    from cnequity.storage.snapshots import SnapshotStore

    with _snapshot_operator_errors():
        restored = SnapshotStore(_cfg(config_path), snapshot_root).restore(name, target)
    click.echo(str(restored))


@snapshot_grp.command("export")
@click.argument("name")
@click.argument("destination", type=click.Path(path_type=Path), required=False)
@click.option(
    "--compression",
    type=click.Choice(["auto", "zstd", "gzip", "none"]),
    default="auto",
    show_default=True,
    help="Archive codec; auto prefers tar.zst and falls back to tar.gz.",
)
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help="Where snapshots live; default is meta/snapshots under the data root.",
)
def snapshot_export(
    name: str,
    destination: Path | None,
    compression: str,
    config_path: str,
    snapshot_root: Path | None,
):
    """Stream snapshot NAME to one portable tar archive."""
    from cnequity.storage.snapshots import SnapshotStore

    try:
        archive = SnapshotStore(_cfg(config_path), snapshot_root).export_archive(
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
@click.option("--name", default=None, help="Imported snapshot name; defaults to the archive stem.")
@click.option(
    "--overwrite",
    is_flag=True,
    help="Replace an existing snapshot only after the archive passes verification.",
)
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help="Where snapshots live; default is meta/snapshots under the data root.",
)
def snapshot_import(
    archive: Path,
    name: str | None,
    overwrite: bool,
    config_path: str,
    snapshot_root: Path | None,
):
    """Verify ARCHIVE and atomically import it into the snapshot store."""
    from cnequity.storage.snapshots import SnapshotStore

    try:
        restored = SnapshotStore(_cfg(config_path), snapshot_root).import_archive(
            archive,
            name=name,
            overwrite=overwrite,
        )
    except (OSError, ValueError, RuntimeError, KeyError, tarfile.TarError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(str(restored))


@snapshot_grp.group("delta")
def snapshot_delta_grp():
    """Create, verify and apply portable incremental lake packages."""


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
        raise click.UsageError("provide --from BASELINE or --from-revision REVISION")
    with _snapshot_operator_errors():
        if from_revision is not None:
            manifest = SnapshotStore(_cfg(config_path), snapshot_root).create_delta(
                name,
                datasets=list(datasets),
                target=target,
                from_revision=from_revision,
            )
        else:
            manifest = SnapshotStore(_cfg(config_path), snapshot_root).create_delta(
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
    help="Baseline lake root. The target root is compared byte-for-byte against it.",
)
@click.option(
    "--to",
    "target",
    type=click.Path(path_type=Path),
    default=None,
    help="Target lake root; defaults to the configured active root.",
)
@click.option(
    "--from-revision",
    type=int,
    default=None,
    help="Use committed revision(s) in the target as the baseline precondition.",
)
@click.option(
    "--dataset",
    "datasets",
    multiple=True,
    help="Dataset to include (repeatable). Omit to discover datasets in both roots.",
)
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help="Where delta packages live; default is meta/snapshots under the data root.",
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
    help="Where delta packages live; default is meta/snapshots under the data root.",
)
def snapshot_delta_verify(name: str, config_path: str, snapshot_root: Path | None):
    """Verify all add/replace payload hashes and change semantics."""

    from dataclasses import asdict

    from cnequity.storage.snapshots import SnapshotStore

    with _snapshot_operator_errors():
        result = SnapshotStore(_cfg(config_path), snapshot_root).verify_delta(name)
    click.echo(json.dumps(asdict(result), indent=2, ensure_ascii=False))
    if not result.passed:
        raise SystemExit(1)


@snapshot_delta_grp.command("apply")
@click.argument("name")
@click.argument("target", type=click.Path(path_type=Path))
@click.option("--dry-run", is_flag=True, help="Validate preconditions without changing TARGET.")
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help="Where delta packages live; default is meta/snapshots under the data root.",
)
def snapshot_delta_apply(
    name: str,
    target: Path,
    dry_run: bool,
    config_path: str,
    snapshot_root: Path | None,
):
    """Safely apply NAME to the non-empty TARGET lake root."""

    from cnequity.storage.snapshots import SnapshotStore

    with _snapshot_operator_errors():
        applied = SnapshotStore(_cfg(config_path), snapshot_root).apply_delta(
            name, target, dry_run=dry_run
        )
    click.echo(str(applied))


# Flat aliases keep scripts written against the initial command proposal
# working while the nested ``snapshot delta ...`` form remains discoverable.
@snapshot_grp.command("delta-create")
@click.argument("name")
@click.option("--from", "baseline", type=click.Path(path_type=Path), default=None)
@click.option("--to", "target", type=click.Path(path_type=Path), default=None)
@click.option("--from-revision", type=int, default=None)
@click.option("--dataset", "datasets", multiple=True)
@config_option
@click.option("--snapshot-root", type=click.Path(path_type=Path), default=None)
def snapshot_delta_create_alias(
    name: str,
    baseline: Path | None,
    target: Path | None,
    from_revision: int | None,
    datasets: tuple[str, ...],
    config_path: str,
    snapshot_root: Path | None,
):
    """Compatibility alias for ``snapshot delta create``."""

    _snapshot_delta_create(
        name, baseline, target, from_revision, datasets, config_path, snapshot_root
    )
