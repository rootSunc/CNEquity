"""Lake maintenance: `compact`, `derive`, `clean`, `stats`.

What you run against a lake that already exists, to keep its shape rather than
to change what it holds.
"""

from __future__ import annotations

import json
from datetime import date

import click
import polars as pl

from cnequity.cli._root import cli
from cnequity.cli._shared import (
    _cfg,
    _run_status_exit_code,
    config_option,
    parse_date_option,
)
from cnequity.derive.adj_factors import compute_adj_factors
from cnequity.domain.market_time import shanghai_today
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.manifest import Manifest
from cnequity.query.parquet_scan import scan_parquet_files
from cnequity.storage.source_snapshots import (
    DEFAULT_SNAPSHOT_RETENTION_DAYS,
    clean_source_snapshots,
)
from cnequity.storage.staging_cleanup import clean_staging


@cli.command()
@config_option
@click.option("--run-id", default=None)
def compact(config_path: str, run_id: str | None):
    """Compact staging into curated for all datasets staged in the run."""
    cfg = _cfg(config_path)
    manifest = Manifest(cfg.manifest_path)
    if not run_id:
        latest = manifest.latest_run()
        if not latest:
            raise click.ClickException("No runs found")
        run_id = latest["run_id"]

    out = JobEngine(cfg).run_step("compact", shanghai_today(), run_id)
    click.echo(
        json.dumps(
            {"run_id": run_id, "rows_written": out.get("rows_written", 0), **out},
            indent=2,
            default=str,
        )
    )


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
    return summary


@cli.command()
@click.argument("name", default="adj_factors")
@config_option
@click.option(
    "--full",
    is_flag=True,
    default=False,
    help="Rewrite all adj_factors partitions (default: append-only since watermark).",
)
@click.option(
    "--start",
    "start_str",
    default=None,
    help="industry_index / trading_status: only derive on/after this date (YYYY-MM-DD).",
)
@click.option(
    "--end",
    "end_str",
    default=None,
    help="industry_index / trading_status: only derive on/before this date (YYYY-MM-DD).",
)
def derive(name: str, config_path: str, full: bool, start_str: str | None, end_str: str | None):
    """Derive computed datasets."""
    cfg = _cfg(config_path)
    start = parse_date_option(start_str, "--start")
    end = parse_date_option(end_str, "--end")
    if start and end and start > end:
        raise click.ClickException("--start must be on or before --end")
    if name == "adj_factors":
        result = compute_adj_factors(cfg, full=full)
        click.echo(f"Derived {name}: {result.rows} rows")
        if result.failed:
            click.echo(
                f"Warnings: {len(result.failed)} symbol×type fetch failures "
                f"({result.fail_ratio:.1%})",
                err=True,
            )
    elif name == "industry_index":
        from cnequity.derive.industry_index import derive_industry_index

        summary = derive_industry_index(cfg, start=start, end=end, full=full)
        click.echo(json.dumps(summary, indent=2, default=str))
    elif name == "trading_status":
        summary = _derive_trading_status(cfg, start=start, end=end)
        click.echo(json.dumps(summary, indent=2, default=str))
        exit_code = _run_status_exit_code(str(summary.get("status", "failed")))
        if exit_code:
            raise SystemExit(exit_code)
    elif name == "sector_routing":
        from cnequity.derive.sector_routing import derive_sector_routing

        summary = derive_sector_routing(cfg)
        click.echo(json.dumps(summary, indent=2, default=str))
    elif name == "sector_code_map":
        from cnequity.derive.sector_code_map import derive_sector_code_map

        summary = derive_sector_code_map(cfg)
        click.echo(json.dumps(summary, indent=2, default=str))
    elif name == "valuation_orphans":
        from cnequity.storage.valuation_orphans import purge_valuation_orphan_symbols

        summary = purge_valuation_orphan_symbols(cfg)
        click.echo(json.dumps(summary, indent=2, default=str))
    else:
        raise click.ClickException(f"Unknown derive target: {name}")


@cli.command()
@config_option
@click.option("--dry-run", is_flag=True, help="Report removable staging without deleting.")
@click.option(
    "--orphan-retention-days",
    default=7,
    show_default=True,
    help="Delete manifest-less orphan staging older than this many days.",
)
@click.option(
    "--snapshot-retention-days",
    default=DEFAULT_SNAPSHOT_RETENTION_DAYS,
    show_default=True,
    help="Delete meta/source_snapshots run_id dirs older than this many days "
    "(always keeps the newest per dataset/source).",
)
@click.option(
    "--force",
    is_flag=True,
    help=(
        "Also delete staging that is not yet cleanup-ready (incomplete batches "
        "and/or no compact). Success fetch batches are demoted to failed so "
        "`cne retry` refetches them (data is refetched, not lost, but the retry "
        "becomes a full re-run). Do not use on success-without-compact runs — "
        "run `cne compact --run-id` first."
    ),
)
@click.option(
    "--reconcile-runs",
    is_flag=True,
    help="Mark runs stuck in 'running' (crashed workers) as failed before cleanup.",
)
@click.option(
    "--reconcile-after-seconds",
    default=None,
    type=float,
    help="Only reconcile runs idle longer than this many seconds "
    "(default: [orchestrator].batch_stale_seconds).",
)
def clean(
    config_path: str,
    dry_run: bool,
    orphan_retention_days: int,
    snapshot_retention_days: int,
    force: bool,
    reconcile_runs: bool,
    reconcile_after_seconds: float | None,
):
    """Remove staging for compacted terminal runs and aged orphans.

    Ready means: run is terminal (success/warning/failed), all batches settled,
    and a successful compact batch was recorded. Incomplete or never-compacted
    staging is kept for retry unless --force is given. Also prunes aged
    ``meta/source_snapshots`` run_id dirs.
    """
    cfg = _cfg(config_path)
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
    click.echo(
        json.dumps(
            {
                "dry_run": dry_run,
                "reconciled": reconciled,
                "removed_run_ids": result.removed_run_ids,
                "orphan_run_ids": result.orphan_run_ids,
                "force_removed_run_ids": result.force_removed_run_ids,
                "skipped_run_ids": result.skipped_run_ids,
                "bytes_freed": result.bytes_freed + snaps.bytes_freed,
                "source_snapshots": {
                    "removed_run_dirs": snaps.removed_run_dirs,
                    "kept_run_dirs": snaps.kept_run_dirs,
                    "bytes_freed": snaps.bytes_freed,
                },
            },
            indent=2,
        )
    )


@cli.group()
def stats():
    """Lake measurement tables under meta/stats (rows, bytes, source mix)."""


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
        f"rebuilt ({freshness.reason or 'stale'}): "
        f"{len(result.datasets)} dataset(s), {result.rows:,} row(s) "
        f"in {result.elapsed_seconds:.1f}s"
    )


@stats.command("rebuild")
@config_option
@click.option(
    "--dataset",
    "dataset_names",
    multiple=True,
    help="Rebuild only these datasets (repeatable). Other datasets keep their rows.",
)
@click.option(
    "--if-stale",
    is_flag=True,
    help="No-op unless ingestion has run since the stats were built. Safe on a timer.",
)
@click.option("--json", "as_json", is_flag=True, help="Print the result as JSON.")
def stats_rebuild(config_path: str, dataset_names: tuple[str, ...], if_stale: bool, as_json: bool):
    """Recompute partition_stats / provenance_stats from curated and derived.

    Unconditional by default. `--if-stale` is the form to put on a timer: it
    returns without work when nothing has changed, and stands down rather than
    queueing when a concurrent rebuild already holds the lock — a dashboard
    request blocked behind a full scan is worse than numbers one run old.

    Staleness is judged by run id, not by the clock. Only ingestion moves the
    lake, so stats built after the last run are current however old they look.
    """
    from cnequity.storage.stats import rebuild_stats

    cfg = _cfg(config_path)

    if if_stale:
        if dataset_names:
            raise click.UsageError(
                "--if-stale rebuilds the whole lake; drop --dataset or drop --if-stale"
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
        f"{len(result.datasets)} dataset(s), {result.partitions} partition(s), "
        f"{result.rows:,} row(s), {result.files} file(s), "
        f"{result.bytes / 1e6:.1f}MB in {result.elapsed_seconds:.1f}s"
    )
    if result.empty:
        click.echo(f"no parquet yet: {', '.join(sorted(result.empty))}")


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
@click.option("--dataset", default=None, help="Per-partition detail for one dataset.")
@click.option("--by-source", is_flag=True, help="Group by source / data_version instead.")
@click.option("--json", "as_json", is_flag=True, help="Machine-readable JSON.")
def stats_show(config_path: str, dataset: str | None, by_source: bool, as_json: bool):
    """Summarise the stats tables, scanning curated directly when there are none.

    `cne stats rebuild` builds the tables this reads. Without them the command
    falls back to counting curated Parquet on the spot: slower, and thinner —
    no byte totals, no source mix, no per-partition detail — but it answers
    "what is in this lake" on a clone that has never built anything. That
    fallback is the former `cne catalog`, and `--json` is its output.
    """
    from cnequity.storage.stats import (
        load_partition_stats,
        load_provenance_stats,
        load_summary,
        stats_freshness,
    )

    cfg = _cfg(config_path)
    summary = load_summary(cfg)
    if summary is None:
        if dataset or by_source:
            raise click.ClickException(
                "no stats yet — `--dataset` / `--by-source` need `cne stats rebuild`"
            )
        entries = _scan_curated_datasets(cfg)
        if as_json:
            click.echo(json.dumps(entries, indent=2))
            return
        click.echo("no stats tables — scanned curated directly; `cne stats rebuild` for the rest")
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
            raise click.ClickException(f"no stats rows for dataset {dataset!r}")
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

    stale_note = (
        f"  STALE — {freshness.reason}; run `cne stats rebuild --if-stale`"
        if freshness.stale
        else ""
    )
    click.echo(
        f"generated_at: {summary.get('generated_at')}  "
        f"run: {summary.get('latest_run_id')}{stale_note}"
    )
    if as_json:
        click.echo(json.dumps(df.sort(df.columns[:2]).to_dicts(), indent=2, default=str))
        return
    with pl.Config(tbl_rows=-1, tbl_cols=-1, fmt_str_lengths=32):
        click.echo(df.sort(df.columns[:2]))
