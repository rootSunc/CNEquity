"""Lake maintenance: `run compact`, `run clean`, `derive`, `stats`.

What you run against a lake that already exists, to keep its shape rather than
to change what it holds.
"""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import date

import click
import polars as pl

from cnequity.cli._root import cli, run
from cnequity.cli._shared import (
    _cfg,
    _run_status_exit_code,
    attach_log_file,
    config_option,
    parse_date_option,
)
from cnequity.derive.adj_factors import compute_adj_factors
from cnequity.domain.datasets import RESAMPLED_MINUTE_DATASETS
from cnequity.domain.market_time import shanghai_today
from cnequity.i18n import N_, _
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.manifest import Manifest
from cnequity.query.parquet_scan import scan_parquet_files
from cnequity.storage.revisions import prune_revision_generations, retain_revision_generations
from cnequity.storage.source_snapshots import (
    DEFAULT_SNAPSHOT_RETENTION_DAYS,
    clean_source_snapshots,
)
from cnequity.storage.staging_cleanup import (
    DEFAULT_LOG_RETENTION_DAYS,
    clean_run_logs,
    clean_staging,
)


@run.command("compact")
@config_option
@click.option(
    "--run-id",
    default=None,
    help=N_("Publish only this run; by default handles all runs pending publication."),
)
def compact(config_path: str, run_id: str | None):
    """Compact runs in staging that have finished but are not yet published into curated.

    \b
    Without --run-id it handles every such run in turn: the process has ended, it has staged
    files, and it has no successful compact yet. Runs still going, and init runs (which
    `cne init` resumes itself), are left alone; each dataset is still guarded by the compact
    gate, and datasets with unfinished batches are left for `cne run retry`.

    """
    cfg = _cfg(config_path)
    attach_log_file(cfg, "run-compact")
    engine = JobEngine(cfg)
    if run_id:
        out = engine.run_step("compact", shanghai_today(), run_id)
        click.echo(
            json.dumps(
                {"run_id": run_id, "rows_written": out.get("rows_written", 0), **out},
                indent=2,
                default=str,
            )
        )
        return

    pending = _unpublished_runs(cfg, engine)
    if not pending:
        click.echo(_("Nothing in staging to publish: every finished run has been compacted."))
        return
    results = []
    worst = 0
    for rid in pending:
        out = engine.run_step("compact", shanghai_today(), rid)
        status = str(out.get("status", "success"))
        click.echo(_("compact {rid}: {status}").format(rid=rid, status=status), err=True)
        results.append({"run_id": rid, "rows_written": out.get("rows_written", 0), **out})
        exit_code = _run_status_exit_code(status)
        if exit_code == 1 or worst == 0:
            worst = exit_code
    click.echo(json.dumps({"runs": results}, indent=2, default=str))
    if worst:
        raise SystemExit(worst)


def _unpublished_runs(cfg, engine: JobEngine) -> list[str]:
    """Finished runs with staged files and no successful compact, oldest first.

    These used to be found by hand — the runbook said to look for stranded
    runs and compact them one `--run-id` at a time, while a bare
    `cne run compact` only ever touched the latest run.
    """
    from cnequity.storage.staging_cleanup import list_staging_run_ids

    # A killed process leaves its row `running`; close those first so their
    # staged facts are not mistaken for live work forever.
    engine.manifest.reconcile_orphaned_runs(
        stale_after_seconds=cfg.batch_stale_seconds, locks_root=cfg.meta_root
    )
    staged = list_staging_run_ids(cfg.staging_root)
    pending = []
    for record in reversed(engine.manifest.list_runs()):
        rid = str(record["run_id"])
        # Name the in-flight states, not the terminal ones, so a new terminal
        # status is never silently skipped. Init publishes through its phases.
        if record["status"] in ("running", "stale") or record["job_name"] == "init":
            continue
        if rid not in staged:
            continue
        batches = engine.manifest.get_batches_for_run(rid)
        if any(b["dataset"] == "compact" and b["status"] == "success" for b in batches):
            continue
        pending.append(rid)
    return pending


def _derive_trading_status(cfg, *, start: date | None, end: date | None) -> dict:
    """Run the derive step and publish it, as the daily job would.

    The rows have to reach a committed revision to be worth anything: a
    consumer reading the lake — including `daily_bars`'s own interior-gap
    check — reads the committed generation, not the mutable curated directory
    a direct write would land in. So this is a one-step run through the
    engine, followed by the same compact the daily job ends with.
    """
    engine = JobEngine(cfg)
    trade_date = shanghai_today()
    run_id = engine.manifest.start_run(
        "derive_trading_status",
        {
            "trade_date": trade_date.isoformat(),
            "derive_start": start.isoformat() if start else None,
            "derive_end": end.isoformat() if end else None,
        },
    )
    context = {"derive_start": start, "derive_end": end, "derive_full": True}
    summary: dict = {"run_id": run_id}
    try:
        derived = engine.run_step("trading_status_derive", trade_date, run_id, context)
        summary["rows_staged"] = derived.get("rows_written", 0)
        summary["compact"] = engine.run_step("compact", trade_date, run_id)
    except Exception as exc:
        engine.manifest.finish_run(run_id, "failed", error_message=str(exc))
        raise
    step_statuses = {
        str(derived.get("status", "success")),
        str(summary["compact"].get("status", "success")),
    }
    if step_statuses.intersection({"failed", "blocked"}):
        status = "failed"
    elif step_statuses.intersection({"warning", "degraded"}):
        status = "degraded"
    else:
        status = "success"
    engine.manifest.finish_run(
        run_id,
        status,
        rows_written=summary["rows_staged"],
    )
    persisted = engine.manifest.get_run(run_id)
    summary["status"] = str(persisted["status"]) if persisted is not None else status
    summary.update(engine._public_outcome(run_id))
    return summary


def _require_minute_input(cfg, dataset: str) -> None:
    """Either minute dataset will do; `_published_derive` can only demand all."""
    from cnequity.orchestrator.outcomes import InputUnavailableError
    from cnequity.query.parquet_scan import dataset_has_parquet
    from cnequity.storage.read_context import read_root

    if not any(
        dataset_has_parquet(read_root(cfg, name)) for name in ("minute_bars", "minute_bars_5m")
    ):
        raise InputUnavailableError(
            _(
                "{dataset}: the lake has no minute_bars or minute_bars_5m; enable [minute_bars] and backfill minute bars first"
            ).format(dataset=dataset)
        )


@contextmanager
def _published_derive(cfg, dataset: str):
    """Make CLI derives visible to revision-aware readers under the writer lock."""
    from cnequity.file_lock import lake_mutation_lock
    from cnequity.orchestrator.outcomes import (
        InputUnavailableError,
        step_outcome,
    )
    from cnequity.orchestrator.run_lock import run_lock
    from cnequity.query.parquet_scan import dataset_has_parquet
    from cnequity.steps.finalize import (
        _capture_derive_inputs,
        _layer_file_identity,
        _publish_derived_revision,
        _record_dataset_result,
    )
    from cnequity.storage.revisions import RevisionStore

    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run(f"derive:{dataset}", {"dataset": dataset})
    outcome = {"status": "success", "rows_written": 0}
    try:
        with run_lock(cfg.meta_root, run_id), lake_mutation_lock(cfg.meta_root):
            store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
            store.ensure_current(dataset)
            store.materialize_current(dataset)
            before = _layer_file_identity(cfg.derived_root / dataset)
            inputs = _capture_derive_inputs(cfg, dataset)
            required = {
                "adj_factors": ("daily_bars",),
                "industry_index": ("daily_bars", "industry_members"),
                "futures_continuous": ("futures_bars",),
                "option_greeks": ("option_bars", "futures_bars"),
            }
            missing = [
                name
                for name in required.get(dataset, ())
                if not dataset_has_parquet(store.current_root(name))
            ]
            if missing:
                from cnequity.research.packs import next_command_for_dataset

                remedies = ", ".join(
                    f"`{next_command_for_dataset(name) or f'cne backfill {name}'}`"
                    for name in missing
                )
                raise InputUnavailableError(
                    f"{dataset}: missing input {', '.join(missing)}; build it first with {remedies}"
                )
            yield outcome
            revision = _publish_derived_revision(
                cfg,
                dataset,
                run_id,
                shanghai_today(),
                before,
                input_revisions=inputs,
                coverage_status="partial"
                if outcome["status"] in {"warning", "degraded"}
                else "unknown",
            )
            if revision:
                if revision["coverage_status"] == "partial":
                    outcome["status"] = "degraded"
                _record_dataset_result(
                    cfg,
                    run_id,
                    dataset,
                    "publish_revision",
                    "success",
                    criticality="research",
                    revision_id=revision["revision_id"],
                    rows_written=outcome["rows_written"],
                    coverage_status=revision["coverage_status"],
                    publication_status=revision["publication_status"],
                )
            _record_dataset_result(
                cfg,
                run_id,
                dataset,
                "derive",
                outcome["status"],
                criticality="research",
                revision_id=revision["revision_id"] if revision else None,
                rows_written=outcome["rows_written"],
            )
            manifest.finish_run(run_id, outcome["status"], rows_written=outcome["rows_written"])
            row = manifest.get_run(run_id)
            outcome.update(
                {
                    key: row[key]
                    for key in (
                        "status",
                        "execution_status",
                        "coverage_status",
                        "publication_status",
                        "result_schema_version",
                    )
                }
            )
            from cnequity.orchestrator.recovery import fallback_options

            outcome.update(run_id=run_id, fallback=fallback_options(cfg, manifest, run_id))
    except Exception as exc:
        axes = step_outcome("failed", error=exc)
        fields = axes.to_dict()
        fields.pop("result_schema_version")
        manifest.record_dataset_result(
            run_id,
            dataset,
            "derive",
            "skipped" if axes.execution_status == "skipped" else "failed",
            criticality="research",
            error_code=type(exc).__name__,
            error_message=str(exc),
            **fields,
        )
        manifest.finish_run(run_id, "failed", error_message=str(exc))
        raise


@cli.command()
@click.argument("name", default="adj_factors")
@config_option
@click.option(
    "--full",
    is_flag=True,
    default=False,
    help=(
        N_(
            "Rewrite all partitions of adj_factors / industry_index / option_greeks / minute_bars_15m|30m|60m (default: incremental only)."
        )
    ),
)
@click.option(
    "--start",
    "start_str",
    default=None,
    help=(
        N_(
            "industry_index / trading_status / option_greeks / minute_bars_15m|30m|60m: derive only this date (YYYY-MM-DD) and later."
        )
    ),
)
@click.option(
    "--end",
    "end_str",
    default=None,
    help=(
        N_(
            "industry_index / trading_status / option_greeks / minute_bars_15m|30m|60m: derive only this date (YYYY-MM-DD) and earlier."
        )
    ),
)
@click.option(
    "--apply",
    "apply_changes",
    is_flag=True,
    help=(
        N_(
            "adj_factor_source: write per-security source overrides and recompute factors for those securities (default: report only)."
        )
    ),
)
def derive(
    name: str,
    config_path: str,
    full: bool,
    start_str: str | None,
    end_str: str | None,
    apply_changes: bool,
):
    """Derive computed datasets.

    \b
    `adj_factors` and `industry_index` are already steps in the daily update (`derive_adj_factors`,
    `derive_industry_index`); the suspension derivation for `trading_status` runs automatically after init
    and after `cne backfill daily_bars`, and `cne backfill corporate_actions` also recomputes factors for the affected securities.
    So running them here is a repair or covers an earlier window, not part of a normal day.
    `sector_routing` and `sector_code_map` are run by no schedule and only by hand.
    `industry_taxonomy` writes one stable identity per industry (`system:version:code`, e.g. `sw:2021:480101`) to meta, also by hand.
    `industry_crosswalk` adds member-overlap evidence between Shenwan and EastMoney industries for the latest day both have a full
    snapshot (or the latest on or before `--end`); nothing is merged.
    `adj_factor_source` arbitrates conflicts between factors and corporate actions with Baostock; securities
    where Sina is shown wrong and Baostock agrees with the other events switch their whole factor to Baostock with `--apply`.
    `futures_continuous` and `option_greeks` update daily in the `derivatives` group.
    `futures_continuous` is recomputed in full every time; `option_greeks` detects changes in quotes, contracts, rates and model dependencies.
    After a derivatives backfill, contracts and derived data update automatically; `--full` recomputes everything explicitly.
    `minute_bars_15m`, `minute_bars_30m` and `minute_bars_60m` are not computed by default and are only derived here:
    a stock's day uses 1m when it has 1m, otherwise 5m. Without a window, only sessions not yet computed, or whose minute inputs changed, are recomputed.

    """
    # Derive targets are lower case in the registry, and command names are
    # already case-insensitive; a target typed in caps should resolve the same.
    name = name.lower()
    cfg = _cfg(config_path)
    attach_log_file(cfg, "derive")
    start = parse_date_option(start_str, "--start")
    end = parse_date_option(end_str, "--end")
    if start and end and start > end:
        raise click.ClickException(_("--start must be on or before --end"))
    if name == "adj_factors":
        with _published_derive(cfg, name) as outcome:
            result = compute_adj_factors(cfg, full=full)
            outcome["rows_written"] = result.rows
            if result.failed:
                outcome["status"] = "degraded"
        click.echo(_("Derived {name}: {rows} rows").format(name=name, rows=result.rows))
        if result.failed:
            click.echo(
                _("Warning: {value} symbol×type fetches failed ({fail_ratio:.1%})").format(
                    value=len(result.failed), fail_ratio=result.fail_ratio
                ),
                err=True,
            )
            click.echo(json.dumps(outcome, indent=2, default=str, ensure_ascii=False))
    elif name == "adj_factor_source":
        from cnequity.derive.factor_arbitration import arbitrate_factor_sources
        from cnequity.storage.repairs.adj_factors import repair_adj_factors

        report = arbitrate_factor_sources(cfg)
        if apply_changes:
            repair_result = repair_adj_factors(cfg, apply=True)
            report["repair"] = repair_result
            report["applied"] = repair_result["applied"]
            report["rows_rewritten"] = repair_result["rows"]
        else:
            report["applied"] = False
        report.pop("switch_detail", None)
        click.echo(json.dumps(report, indent=2, default=str, ensure_ascii=False))
    elif name == "industry_index":
        from cnequity.derive.industry_index import derive_industry_index

        with _published_derive(cfg, name) as outcome:
            summary = derive_industry_index(cfg, start=start, end=end, full=full)
            outcome["rows_written"] = summary.get("rows", 0)
        click.echo(json.dumps(summary, indent=2, default=str))
    elif name == "futures_continuous":
        if start or end:
            raise click.ClickException(
                _(
                    "futures_continuous must be recomputed in full and does not support --start / --end"
                )
            )
        from cnequity.derive.futures_continuous import derive_futures_continuous

        with _published_derive(cfg, name) as outcome:
            summary = derive_futures_continuous(cfg)
            outcome["rows_written"] = summary.get("rows", 0)
        click.echo(json.dumps(summary, indent=2, default=str))
    elif name == "option_greeks":
        from cnequity.derive.option_greeks import derive_option_greeks

        with _published_derive(cfg, name) as outcome:
            summary = derive_option_greeks(cfg, start=start, end=end, full=full)
            outcome["rows_written"] = summary.get("rows", 0)
        click.echo(json.dumps(summary, indent=2, default=str))
    elif name in RESAMPLED_MINUTE_DATASETS:
        from cnequity.derive.minute_resample import derive_minute_resample

        with _published_derive(cfg, name) as outcome:
            _require_minute_input(cfg, name)
            summary = derive_minute_resample(cfg, name, start=start, end=end, full=full)
            outcome["rows_written"] = summary.get("rows", 0)
            # Halts are expected gaps; a gap in a traded day, or input the
            # resampler rejects, is a data problem.
            if any(
                summary.get(key)
                for key in ("skipped_incomplete_1m", "skipped_incomplete_5m", "skipped_invalid")
            ):
                outcome["status"] = "degraded"
        click.echo(json.dumps(summary, indent=2, default=str, ensure_ascii=False))
    elif name == "trading_status":
        summary = _derive_trading_status(cfg, start=start, end=end)
        click.echo(json.dumps(summary, indent=2, default=str))
        exit_code = _run_status_exit_code(str(summary.get("status", "failed")))
        if exit_code:
            raise SystemExit(exit_code)
    elif name == "sector_routing":
        from cnequity.derive.sector_routing import derive_sector_routing

        try:
            summary = derive_sector_routing(cfg)
        except RuntimeError as exc:  # missing input or source: the message says what to do
            raise click.ClickException(str(exc)) from exc
        click.echo(json.dumps(summary, indent=2, default=str))
    elif name == "sector_code_map":
        from cnequity.derive.sector_code_map import derive_sector_code_map

        try:
            summary = derive_sector_code_map(cfg)
        except RuntimeError as exc:  # missing input or source: the message says what to do
            raise click.ClickException(str(exc)) from exc
        click.echo(json.dumps(summary, indent=2, default=str))
    elif name == "industry_taxonomy":
        from cnequity.derive.industry_taxonomy import derive_industry_taxonomy

        try:
            summary = derive_industry_taxonomy(cfg)
        except RuntimeError as exc:  # missing input: the message says what to do
            raise click.ClickException(str(exc)) from exc
        click.echo(json.dumps(summary, indent=2, default=str))
    elif name == "industry_crosswalk":
        from cnequity.derive.industry_crosswalk import derive_industry_crosswalk

        try:
            summary = derive_industry_crosswalk(cfg, as_of=end)
        except RuntimeError as exc:  # no same-day evidence yet: the message says what to run
            raise click.ClickException(str(exc)) from exc
        click.echo(json.dumps(summary, indent=2, default=str, ensure_ascii=False))
    else:
        raise click.ClickException(_("Unknown derive target: {name}").format(name=name))


@run.command("clean")
@config_option
@click.option(
    "--dry-run",
    is_flag=True,
    help=N_("Print the preview and do not ask to delete."),
)
@click.option(
    "--orphan-retention-days",
    default=7,
    show_default=True,
    help=N_(
        "Preview orphan staging older than this many days that is not recorded in the manifest."
    ),
)
@click.option(
    "--snapshot-retention-days",
    default=DEFAULT_SNAPSHOT_RETENTION_DAYS,
    show_default=True,
    help=(
        N_(
            "Preview meta/source_snapshots run_id directories older than this many days (the latest one per dataset / source is always kept)."
        )
    ),
)
@click.option(
    "--force",
    is_flag=True,
    help=N_(
        "Also include unfinished or uncompacted staging in the preview; deletes no files and does not downgrade batches."
    ),
)
@click.option(
    "--keep-revision-generations",
    default=5,
    show_default=True,
    type=int,
    help=(
        N_(
            "When a person confirms deletion, keep the latest this many generations plus the current version and holds. 0 skips version handling."
        )
    ),
)
@click.option(
    "--log-retention-days",
    default=DEFAULT_LOG_RETENTION_DAYS,
    show_default=True,
    type=int,
    help=N_("Preview `logs/cne-*.log` older than this many days, without deleting; 0 skips."),
)
@click.option(
    "--reconcile-runs",
    is_flag=True,
    help=N_("Before cleanup, mark runs stuck in 'running' (crashed worker) as failed."),
)
@click.option(
    "--reconcile-after-seconds",
    default=None,
    type=float,
    help=(
        N_(
            "Apply the reconciliation above only to runs silent for more than this many seconds (defaults to [orchestrator].batch_stale_seconds)."
        )
    ),
)
def clean(
    config_path: str,
    dry_run: bool,
    orphan_retention_days: int,
    snapshot_retention_days: int,
    keep_revision_generations: int,
    log_retention_days: int,
    force: bool,
    reconcile_runs: bool,
    reconcile_after_seconds: float | None,
):
    """Preview expired data, then ask before deleting historical generations or compacted staging.

    \b
    A terminal and a scheduler see the same preview. Only an interactive terminal is asked,
    and the default answer is no. Historical generations and staging are separate questions.
    --dry-run never asks. Scheduled runs therefore stay preview-only.
    Staging that is running, locked, unfinished, uncompacted, orphaned or held is never
    deleted, even when the answer is yes. Source snapshots and logs are never deleted either.
    Current versions, holds and generations without a usable receipt are kept.
    Backfill and the daily run never delete staging or the generation currently in use.

    """
    if dry_run and reconcile_runs:
        raise click.UsageError(
            _(
                "--dry-run cannot be combined with --reconcile-runs: reconciling changes run state. Drop --reconcile-runs to see the cleanup preview."
            )
        )
    # The preview executors never delete. A person at a terminal may confirm
    # afterwards; a scheduler has no terminal and is not asked.
    preview_only = dry_run
    dry_run = True
    cfg = _cfg(config_path)
    attach_log_file(cfg, "run-clean")
    # Preview the candidate inventory; no cleanup executor receives write authority.
    generations = (
        prune_revision_generations(cfg.meta_root, keep=keep_revision_generations, dry_run=dry_run)
        if keep_revision_generations > 0
        else []
    )
    reconciled: dict[str, int] | None = None
    if reconcile_runs:
        manifest = Manifest(cfg.manifest_path)
        stale_after = (
            float(reconcile_after_seconds)
            if reconcile_after_seconds is not None
            else cfg.batch_stale_seconds
        )
        reconciled = manifest.reconcile_orphaned_runs(
            stale_after_seconds=stale_after,
            locks_root=cfg.meta_root,
        )
    result = clean_staging(
        cfg,
        dry_run=dry_run,
        orphan_retention_days=orphan_retention_days,
        force=force,
    )
    snaps = clean_source_snapshots(
        cfg.meta_root,
        retention_days=snapshot_retention_days,
        dry_run=dry_run,
    )
    logs = clean_run_logs(cfg.data_root, retention_days=log_retention_days, dry_run=dry_run)
    generation_candidates = sum(len(item.candidate_revision_ids) for item in generations)
    staging_run_ids = list(result.removed_run_ids)
    will_ask = (
        not preview_only
        and _stdin_is_tty()
        and (generation_candidates > 0 or bool(staging_run_ids))
    )
    payload = {
        "dry_run": True,
        "confirmation_required": "interactive" if will_ask else "none",
        "reconciled": reconciled,
        "removed_run_ids": [],
        "candidate_run_ids": result.removed_run_ids,
        "orphan_run_ids": result.orphan_run_ids,
        "force_removed_run_ids": [],
        "force_candidate_run_ids": result.force_removed_run_ids,
        "skipped_run_ids": result.skipped_run_ids,
        "skipped_reasons": result.skipped_reasons,
        "staging_inventory": result.inventory,
        "staging_run_count": len(result.inventory),
        "bytes_freed": 0,
        "logical_bytes_selected": (
            result.bytes_freed
            + snaps.bytes_freed
            + logs.bytes_freed
            + sum(item.logical_bytes_selected for item in generations)
        ),
        "source_snapshots": {
            "removed_run_dirs": [],
            "candidate_run_dirs": snaps.removed_run_dirs,
            "kept_run_dirs": snaps.kept_run_dirs,
            "bytes_freed": 0,
            "logical_bytes_selected": snaps.bytes_freed,
        },
        "run_logs": {
            "removed": 0,
            "candidates": len(logs.removed),
            "kept": logs.kept,
            "bytes_freed": 0,
            "logical_bytes_selected": logs.bytes_freed,
        },
        "revision_generations": [
            {
                "dataset": item.dataset,
                "removed": len(item.removed_revision_ids),
                "kept": len(item.kept_revision_ids),
                "bytes_freed": item.freed_bytes,
                "candidates": len(item.candidate_revision_ids),
                "marked": len(item.marked_revision_ids),
                "logical_bytes_selected": item.logical_bytes_selected,
            }
            for item in generations
        ],
    }
    click.echo(json.dumps(payload, indent=2))
    if not will_ask:
        return
    _delete_preview_selection(
        cfg,
        keep_revision_generations=keep_revision_generations,
        generation_candidates=generation_candidates,
        staging_run_ids=staging_run_ids,
    )


def _stdin_is_tty() -> bool:
    return sys.stdin.isatty()


def _confirm(text: str) -> bool:
    try:
        return click.confirm(text, default=False, err=True)
    except click.Abort:
        return False


def _staging_in_use_reason(cfg, run_id: str, holds: set[str]) -> str | None:
    """Why this staging run is still part of the live lake, or None if it is redundant."""
    from cnequity.file_lock import is_locked
    from cnequity.orchestrator.manifest import Manifest
    from cnequity.storage.staging_cleanup import run_ready_for_staging_cleanup

    if f"staging/{run_id}" in holds:
        return "hold"
    if is_locked(cfg.meta_root / "locks" / f"{run_id}.lock"):
        return "active_run_lock"
    manifest = Manifest(cfg.manifest_path)
    run = manifest.get_run(run_id)
    if run is None:
        return "orphan"
    if run["status"] == "running":
        return "running"
    if not run_ready_for_staging_cleanup(manifest, run_id):
        return "unfinished_or_not_compacted"
    return None


def _delete_preview_selection(
    cfg,
    *,
    keep_revision_generations: int,
    generation_candidates: int,
    staging_run_ids: list[str],
) -> None:
    """Ask separately, then delete only what the person accepted.

    The preview is already on stdout. Questions go to stderr. Staging that is
    still in use is left even when the staging answer is yes. Declining either
    question deletes nothing from that group.
    """
    from cnequity.storage.lifecycle.resources import resource_holds

    deleted: dict = {}
    with resource_holds(cfg.meta_root, dry_run=True) as holds:
        idle = []
        for run_id in staging_run_ids:
            reason = _staging_in_use_reason(cfg, run_id, holds)
            if reason is None:
                idle.append(run_id)
                continue
            click.echo(
                _(
                    "Left staging run {run_id} in place because it is still in use ({reason})."
                ).format(run_id=run_id, reason=reason),
                err=True,
            )
    if generation_candidates and keep_revision_generations > 0:
        agreed = _confirm(
            _(
                "Delete {generations} historical generations from this preview? The generation "
                "currently in use, holds, generations with an unusable receipt, and all staging "
                "stay."
            ).format(generations=generation_candidates)
        )
        if agreed:
            removed = retain_revision_generations(cfg.meta_root, keep=keep_revision_generations)
            deleted["revision_generations"] = [
                {
                    "dataset": item.dataset,
                    "removed_revision_ids": list(item.removed_revision_ids),
                    "freed_bytes": item.freed_bytes,
                }
                for item in removed
            ]
    if idle:
        agreed = _confirm(
            _(
                "Delete {staging} compacted staging runs that are not in use? Running, locked, "
                "unfinished, uncompacted, orphaned and held staging stays."
            ).format(staging=len(idle))
        )
        if agreed:
            from cnequity.storage.lifecycle.staging import StagingRetirement

            retirement = StagingRetirement(cfg)
            plan = retirement.plan(
                phase="purge",
                selected_ids=[f"staging/{run_id}" for run_id in idle],
                require_registry=False,
            )
            journal = retirement.apply(plan["plan_id"], maintenance=True)
            deleted["staging"] = {
                "removed_run_ids": idle,
                "logical_bytes_deleted": journal.get("logical_bytes_deleted", 0),
            }
    if deleted:
        click.echo(json.dumps({"deleted": True, **deleted}, indent=2))


@cli.group()
def stats():
    """Lake metrics under meta/stats (row counts, bytes, source mix)."""


def _stats_rebuild_if_stale(cfg, *, as_json: bool) -> None:
    """The former `cne stats refresh`: rebuild only when the lake has moved on."""
    from cnequity.storage.stats import refresh_stats_if_stale, stats_freshness

    freshness = stats_freshness(cfg)
    result = refresh_stats_if_stale(cfg)
    if result is None:
        reason = (
            "stale, but another rebuild holds the lock — nothing to do"
            if freshness.stale
            else f"current as of run {freshness.latest_run_id} — nothing to do"
        )
        click.echo(json.dumps({"rebuilt": False, "reason": reason}) if as_json else reason)
        return
    if as_json:
        click.echo(json.dumps({"rebuilt": True, **result.as_dict()}, indent=2, default=str))
        return
    click.echo(
        _(
            "Recomputed ({value}): {value2} datasets, {rows:,} rows in {elapsed_seconds:.1f}s"
        ).format(
            value=freshness.reason or "stale",
            value2=len(result.datasets),
            rows=result.rows,
            elapsed_seconds=result.elapsed_seconds,
        )
    )


@stats.command("rebuild")
@config_option
@click.option(
    "--dataset",
    "dataset_names",
    multiple=True,
    help=N_("Recompute only these datasets (repeatable). Rows for other datasets stay unchanged."),
)
@click.option(
    "--if-stale",
    is_flag=True,
    help=N_(
        "Do nothing unless collection has run since the stats were built. Safe to put on a timer."
    ),
)
@click.option("--json", "as_json", is_flag=True, help=N_("Print the result as JSON."))
def stats_rebuild(config_path: str, dataset_names: tuple[str, ...], if_stale: bool, as_json: bool):
    """Recompute partition_stats / provenance_stats from curated and derived.

    \b
    Recomputes unconditionally by default. For a timer use `--if-stale`: it returns at once when nothing changed,
    and steps aside rather than queueing when a concurrent recompute holds the lock;
    a dashboard request stuck behind a full scan is worse than numbers one run behind.

    \b
    Staleness is judged by run id, not by the clock. Only ingestion changes this lake,
    so stats built after the last run are current, however old they look.

    """
    # Registry names are lower case; `--dataset` should not care about case.
    dataset_names = tuple(n.lower() for n in dataset_names)
    from cnequity.storage.stats import rebuild_stats

    cfg = _cfg(config_path)
    attach_log_file(cfg, "stats-rebuild")

    if if_stale:
        if dataset_names:
            raise click.UsageError(
                _("--if-stale recomputes the whole lake; drop --dataset, or drop --if-stale")
            )
        _stats_rebuild_if_stale(cfg, as_json=as_json)
        return

    try:
        result = rebuild_stats(cfg, datasets=list(dataset_names) or None)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc

    if as_json:
        click.echo(json.dumps(result.as_dict(), indent=2, default=str))
        return
    click.echo(
        _(
            "{value} datasets, {partitions} partitions, {rows:,} rows, {files} files, {value2:.1f}MB in {elapsed_seconds:.1f}s"
        ).format(
            value=len(result.datasets),
            partitions=result.partitions,
            rows=result.rows,
            files=result.files,
            value2=result.bytes / 1e6,
            elapsed_seconds=result.elapsed_seconds,
        )
    )
    if result.empty:
        # A first lake holds a few of the registered datasets; the other fifty
        # names are noise, and `cne status --datasets` already lists them.
        click.echo(
            _(
                "{value} registered datasets have no parquet yet (see `cne status --datasets`)"
            ).format(value=len(result.empty))
        )


def _scan_curated_datasets(cfg) -> list[dict]:
    """Count curated Parquet on the spot — the former `cne catalog`.

    Every call walks the whole tree, which is why `cne stats rebuild` exists.
    It stays as the answer for a lake that has never built its stats tables:
    "what is in here" should not require a build step first.
    """
    entries = []
    curated = cfg.curated_root
    if not curated.exists():
        return entries
    for ds_dir in sorted(curated.iterdir()):
        if not ds_dir.is_dir():
            continue
        files = list(ds_dir.glob("**/*.parquet"))
        # lazy count(*) resolves from parquet metadata without decoding data
        # pages — cheap even on a 10-year lake.
        rows = int(scan_parquet_files(files).select(pl.len()).collect().item()) if files else 0
        entries.append({"dataset": ds_dir.name, "files": len(files), "rows": rows})
    return entries


@stats.command("show")
@config_option
@click.option("--dataset", default=None, help=N_("Per-partition detail for one dataset."))
@click.option("--by-source", is_flag=True, help=N_("Group by source / data_version instead."))
@click.option("--json", "as_json", is_flag=True, help=N_("Output machine-readable JSON."))
def stats_show(config_path: str, dataset: str | None, by_source: bool, as_json: bool):
    """Summarize the stats tables; recompute them first when they are stale.

    \b
    When the stats lag the latest ingestion, they are recomputed by the rules of `cne stats rebuild --if-stale` before showing;
    stats that `--dataset` / `--by-source` need but that do not exist yet are generated first as well.
    Otherwise a lake that never built stats falls back to counting curated Parquet on the spot: thinner
    (no byte totals, no source mix, no per-partition detail) but able to answer
    "what is in this lake" without building anything first. That fallback is the former `cne catalog`, and `--json` is its output.

    """
    dataset = dataset.lower() if dataset else None
    from cnequity.storage.stats import (
        load_partition_stats,
        load_provenance_stats,
        load_summary,
        refresh_stats_if_stale,
        stats_freshness,
    )

    cfg = _cfg(config_path)
    summary = load_summary(cfg)
    # Refresh instead of telling the reader to: a stale table, or a missing one
    # a detail view needs. A lake with no tables and a plain view keeps the
    # cheap scan below, whose --json shape scripts already read.
    if (summary is not None and stats_freshness(cfg).stale) or (
        summary is None and (dataset or by_source)
    ):
        click.echo(
            (
                _("Stats are stale; recomputing (time depends on the lake's size)…")
                if summary is not None
                else _("Stats not generated yet; computing (time depends on the lake's size)…")
            ),
            err=True,
        )
        refresh_stats_if_stale(cfg)
        summary = load_summary(cfg)
    if summary is None:
        if dataset or by_source:
            raise click.ClickException(
                _(
                    "Another stats recompute is in progress, and the stats `--dataset` / `--by-source` need do not exist yet; try again shortly"
                )
            )
        entries = _scan_curated_datasets(cfg)
        if as_json:
            click.echo(json.dumps(entries, indent=2))
            return
        click.echo(
            _(
                "No stats tables: scanned curated directly; adding --dataset or --by-source generates the stats automatically"
            )
        )
        click.echo(
            pl.DataFrame(
                entries, schema={"dataset": pl.String, "files": pl.Int64, "rows": pl.Int64}
            )
        )
        return
    freshness = stats_freshness(cfg)

    df = load_provenance_stats(cfg) if by_source else load_partition_stats(cfg)
    if dataset:
        df = df.filter(pl.col("dataset") == dataset)
        if df.is_empty():
            raise click.ClickException(
                _("Dataset {dataset!r} has no stats rows").format(dataset=dataset)
            )
    elif by_source:
        df = df.group_by(["dataset", "source", "data_version"]).agg(
            pl.col("row_count").sum(),
            pl.col("fetched_at_min").min(),
            pl.col("fetched_at_max").max(),
        )
    else:
        df = df.group_by("dataset").agg(
            pl.len().alias("partitions"),
            pl.col("row_count").sum(),
            pl.col("file_count").sum().alias("files"),
            pl.col("bytes").sum(),
            pl.col("period_start").min(),
            pl.col("period_end").max(),
        )

    # Still stale only when another rebuild held the lock just now.
    stale_note = (
        f"  STALE — {freshness.reason}; another rebuild is in progress" if freshness.stale else ""
    )
    click.echo(
        _("Generated: {value}  run: {value2}{stale_note}").format(
            value=summary.get("generated_at"),
            value2=summary.get("latest_run_id"),
            stale_note=stale_note,
        )
    )
    if as_json:
        click.echo(json.dumps(df.sort(df.columns[:2]).to_dicts(), indent=2, default=str))
        return
    with pl.Config(tbl_rows=-1, tbl_cols=-1, fmt_str_lengths=32):
        click.echo(df.sort(df.columns[:2]))
