"""The scheduled path: `run daily` and `retry`.

Composition of these into a day's worth of work lives in
`scripts/daily_pipeline.sh`, not here — the CLI runs one job, the script decides
which jobs a day needs.
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import date
from pathlib import Path

import click

from cnequity.cli._root import cli
from cnequity.cli._shared import (
    _cfg,
    _progress_logging,
    _run_status_exit_code,
    config_option,
)
from cnequity.cli.quality_cmds import _last_trading_day
from cnequity.config import WaveConfig
from cnequity.domain.market_time import shanghai_today
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.run_lock import RunLockError


@cli.group()
def run():
    """Run scheduled jobs."""


def _stale_priority(spec, row: dict, anchor: date) -> tuple[int, int, int, str]:
    """Order stale work by loss deadline, then estimated operational cost.

    Snapshot-only feeds have no honest historical replay: if today's window
    is missed, the observation is gone. They therefore run first. Intraday and
    known wide sweeps are deliberately deferred so a multi-minute/multi-page
    task cannot hold the urgent same-day snapshots behind it.
    """
    from cnequity.domain.datasets import history_mode_for

    snapshot_live = not spec.watermark and spec.fetch_semantics == "snapshot"
    if snapshot_live:
        mark = row.get("snapshot_date") or row.get("coverage_end")
    else:
        mark = row.get("watermark") or row.get("coverage_end")
    lag = (anchor - mark).days if mark is not None else 0
    mode = history_mode_for(spec)
    urgent = 0 if snapshot_live or mode == "snapshot_only" else 1
    if spec.intraday_frequency or spec.row_grain:
        cost_class = 3
    elif spec.name in {
        "daily_bars",
        "financial_statement_items",
        "top_holders",
        "trade_ticks",
        "minute_bars",
        "minute_bars_5m",
    }:
        cost_class = 2
    else:
        cost_class = 1
    # A larger lag is more urgent within the same semantic/cost class. The
    # negative sign keeps the key ascending while favouring older losses.
    return (urgent, cost_class, -lag, spec.name)


def stale_fetch_plan(cfg, anchor: date) -> list[dict]:
    """Return stale fetch steps with deterministic deadline/cost metadata.

    Registered fetch steps whose dataset is still behind *anchor*.

    Freshness is judged exactly as ``cne status --datasets`` judges it, so the
    two cannot disagree about what is behind.

    Derived datasets are excluded: they are recomputed by ``cne derive`` from
    curated inputs, and re-fetching is not what they need. Datasets with no
    registered step are excluded because there is nothing to run.
    """
    # Steps are registered by the module-level `import cnequity.steps`.
    from cnequity.domain.datasets import DATASETS, history_mode_for, is_dataset_enabled, is_stale
    from cnequity.orchestrator.registry import STEP_REGISTRY
    from cnequity.query.reader import list_datasets

    def has_failed_attempt(dataset: str) -> bool:
        """Distinguish a never-started empty dataset from a failed snapshot."""
        manifest_path = getattr(cfg, "manifest_path", None)
        if manifest_path is None or not Path(manifest_path).exists():
            return False
        from cnequity.orchestrator.manifest import Manifest

        manifest = Manifest(manifest_path)
        for run in manifest.list_runs():
            for receipt in manifest.get_dataset_results(run["run_id"], dataset=dataset):
                if receipt["status"] in {"failed", "warning", "degraded", "blocked"}:
                    return True
            for batch in manifest.get_batches_for_run(run["run_id"]):
                if batch["dataset"] == dataset and batch["status"] in {
                    "failed",
                    "warning",
                    "stale",
                }:
                    return True
        return False

    out: list[dict] = []
    for row in list_datasets(config=cfg).iter_rows(named=True):
        name = row["dataset"]
        spec = DATASETS[name]
        if spec.layer == "derived" or name not in STEP_REGISTRY:
            continue
        if not is_dataset_enabled(name, cfg):
            continue
        mode = history_mode_for(spec)
        snapshot_live = not spec.watermark and spec.fetch_semantics == "snapshot"
        if snapshot_live:
            # There is no honest PIT watermark for a rolling live window.  A
            # separate capture marker is written by the fetch step; missing
            # marker/data is itself stale so a skipped first capture can be
            # recovered on the next scheduler pass.  Unlike a normal EOD
            # feed, a live snapshot has a same-day deadline: yesterday's
            # marker must still be retried today even though the generic
            # freshness tolerance allows one day of lag. ``coverage_end``
            # keeps older lakes (before the marker was introduced) schedulable.
            mark = row.get("snapshot_date") or row.get("coverage_end")
            stale = mark is None or mark < anchor
        elif (
            spec.required
            and spec.watermark
            and spec.fetch_semantics == "snapshot"
            and (not row.get("has_data") or not row.get("watermarked"))
            and has_failed_attempt(name)
        ):
            # The first failed required snapshot has no watermark to compare,
            # but it is still an actionable stale obligation.  Without this
            # branch the scheduler treats an empty/unwatermarked row as
            # "never started" and permanently skips its retry.
            mark = row.get("watermark") or row.get("coverage_end")
            stale = True
        else:
            if not row["has_data"] or not row["watermarked"]:
                continue
            mark = row["watermark"] or row["coverage_end"]
            stale = is_stale(name, mark, anchor)
        if stale:
            priority = _stale_priority(spec, row, anchor)
            out.append(
                {
                    "dataset": name,
                    "priority": priority[0],
                    "cost_class": priority[1],
                    "estimated_cost": priority[1],
                    "lag_days": max(0, (anchor - mark).days) if mark is not None else 0,
                    "history_mode": mode,
                    "deadline": ("same_day" if priority[0] == 0 else "next_available_window"),
                }
            )
    # Reconstruct the exact key from the plan metadata; plan output is public
    # and intentionally JSON-serialisable, so callers need not know registry
    # internals to display or persist the schedule.
    out.sort(
        key=lambda item: (
            item["priority"],
            item["cost_class"],
            -item["lag_days"],
            item["dataset"],
        )
    )
    return out


def stale_fetch_steps(cfg, anchor: date) -> list[str]:
    """Registered fetch steps still behind *anchor*, ordered by urgency/cost."""
    return [item["dataset"] for item in stale_fetch_plan(cfg, anchor)]


def _repairable_gaps(cfg, anchor: date) -> list:
    """Read repairable coverage gaps, excluding snapshot-only pseudo-repairs."""
    from cnequity.domain.datasets import DATASETS, history_mode_for
    from cnequity.quality.verify import verify_lake

    return [
        gap
        for gap in verify_lake(cfg, anchor=anchor)
        if gap.repairable and history_mode_for(DATASETS.get(gap.dataset)) != "snapshot_only"
    ]


def _auto_repair_gaps(cfg, anchor: date) -> list[dict]:
    """Best-effort repair of verified gaps for an explicitly requested run."""
    from cnequity.cli.backfill_cmds import _run_backfill

    results: list[dict] = []
    for gap in _repairable_gaps(cfg, anchor):
        try:
            result = _run_backfill(cfg, gap.dataset, gap.start, gap.end)
            results.append(
                {
                    "dataset": gap.dataset,
                    "kind": gap.kind,
                    "status": result.get("status", "unknown"),
                    "rows_written": result.get("rows_written", 0),
                }
            )
        except Exception as exc:  # noqa: BLE001 — current-day ingest must continue
            results.append(
                {
                    "dataset": gap.dataset,
                    "kind": gap.kind,
                    "status": "failed",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
    return results


def _run_stale_only(
    cfg,
    engine,
    trade_date: date | None,
    *,
    backfill: bool,
    repair_gaps: bool = False,
) -> None:
    """Second attempt, same day, for whatever the first attempt did not land.

    The gap this closes: a ``snapshot`` dataset fetches only the run day, so a
    source outage during the one scheduled window loses that day permanently —
    ``valuation_metrics`` lost 2026-07-30 and 07-31 to a push2 clist outage and
    no later run could have recovered them. Per-host retries already exist and
    were exhausted; what was missing was a second window.
    """
    anchor = _last_trading_day(cfg, trade_date or shanghai_today())
    repairs = _auto_repair_gaps(cfg, anchor) if repair_gaps else []
    plan = stale_fetch_plan(cfg, anchor)
    steps = [item["dataset"] for item in plan]
    if not plan:
        click.echo(f"nothing stale as of {anchor.isoformat()}")
        click.echo(
            json.dumps(
                {"anchor": anchor.isoformat(), "repairs": repairs, "status": "nothing_stale"},
                indent=2,
            )
        )
        return
    click.echo(f"stale as of {anchor.isoformat()}: {', '.join(steps)}", err=True)
    # Run same-day snapshot-only feeds in an earlier wave. A large intraday or
    # historical sweep can still run in parallel with peers of its own class,
    # but it cannot delay the finite snapshot capture window.
    urgent = [item["dataset"] for item in plan if item["priority"] == 0]
    deferred = [item["dataset"] for item in plan if item["priority"] != 0]
    waves: list[WaveConfig] = []
    if urgent:
        waves.append(WaveConfig(name="stale:snapshot", parallel=True, steps=urgent))
    if deferred:
        waves.append(WaveConfig(name="stale:deferred", parallel=True, steps=deferred))
    waves.append(WaveConfig(name="stale:compact", parallel=True, steps=["compact"]))
    # ``--backfill`` is a historical replay mode, but a stale-only retry must
    # still capture snapshot-only feeds on today's window. Never pass the
    # replay flag when that urgent class is present; otherwise the step would
    # correctly reject the call as an attempt to manufacture PIT values.
    effective_backfill = bool(backfill and not urgent)
    try:
        result = engine.run_job(
            "daily:stale",
            # ``trade_date`` can be a weekend/holiday supplied by a timer.
            # Fetches and snapshot markers must use the same executable
            # trading-day anchor that produced the stale plan.
            trade_date=anchor,
            waves=waves,
            backfill=effective_backfill,
        )
    except RunLockError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        json.dumps(
            {
                "run_id": result["run_id"],
                "status": result["status"],
                "stale_plan": plan,
                "repairs": repairs,
                "backfill_applied": effective_backfill,
            },
            indent=2,
        )
    )
    exit_code = _run_status_exit_code(result["status"])
    if exit_code:
        raise SystemExit(exit_code)


@run.command("daily")
@config_option
@click.option(
    "--group",
    "group_name",
    default=None,
    help=(
        "Schedule group: core, capital, signals, fundamentals, macro_risk, research, "
        "intraday, ticks"
    ),
)
@click.option(
    "--trade-date",
    "trade_date_str",
    default=None,
    help="As-of trade date YYYY-MM-DD (default: today). Use to catch up on weekends/holidays.",
)
@click.option("--backfill", is_flag=True)
@click.option(
    "--ignore-calendar",
    is_flag=True,
    help="Bypass trading calendar check (execute even on weekends/holidays).",
)
@click.option(
    "--repair-gaps",
    is_flag=True,
    help="Before the daily/stale run, repair verified historical gaps that have an honest source.",
)
@click.option("--quiet", is_flag=True, help="Only warnings and errors; no per-step progress.")
@click.option(
    "--stale-only",
    is_flag=True,
    help="Re-fetch only the datasets still behind the last trading day. "
    "Schedule a few hours after the main pipeline: a snapshot dataset that lost "
    "its window to a source outage cannot be replayed tomorrow.",
)
def run_daily(
    config_path: str,
    group_name: str | None,
    trade_date_str: str | None,
    backfill: bool,
    ignore_calendar: bool,
    repair_gaps: bool,
    stale_only: bool,
    quiet: bool,
):
    """Run daily ingestion job (Wave DAG or schedule group)."""
    _progress_logging(quiet)
    try:
        cfg = _cfg(config_path)
        # Validate the safety-critical source caps before constructing an
        # engine.  A malformed cap must fail closed even when the selected
        # group happens not to touch that source; surface it as a one-line CLI
        # error rather than a worker traceback or a silent global fallback.
        cfg._validate_source_limits()
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    engine = JobEngine(cfg)
    td = date.fromisoformat(trade_date_str) if trade_date_str else None
    if stale_only:
        if group_name:
            raise click.ClickException("--stale-only picks its own steps; drop --group.")
        _run_stale_only(cfg, engine, td, backfill=backfill, repair_gaps=repair_gaps)
        return
    repairs = []
    if repair_gaps:
        repairs = _auto_repair_gaps(cfg, _last_trading_day(cfg, td or shanghai_today()))
    try:
        if group_name:
            group = cfg.schedule_groups.get(group_name)
            if not group:
                raise click.ClickException(f"Unknown group: {group_name}")
            result = engine.run_job(
                f"daily:{group_name}",
                trade_date=td,
                waves=[
                    WaveConfig(
                        name=f"group:{group_name}",
                        parallel=getattr(group, "parallel", True),
                        steps=group.steps,
                    )
                ],
                backfill=backfill,
                ignore_calendar=ignore_calendar,
            )
        else:
            result = engine.run_job(
                "daily",
                trade_date=td,
                backfill=backfill,
                ignore_calendar=ignore_calendar,
            )
    except RunLockError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        json.dumps(
            {"run_id": result["run_id"], "status": result["status"], "repairs": repairs},
            indent=2,
        )
    )
    # Exit non-zero on failure so schedulers (launchd/cron) and the daily
    # pipeline can detect it; a non-trading-day skip is a success (exit 0).
    exit_code = _run_status_exit_code(result["status"])
    if exit_code:
        raise SystemExit(exit_code)


@run.command("events")
@config_option
@click.option(
    "--group",
    "group_name",
    default=None,
    help="Event group: corporate_events, news_wire",
)
@click.option(
    "--trade-date",
    "trade_date_str",
    default=None,
    help="As-of calendar date YYYY-MM-DD (default: today).",
)
@click.option("--backfill", is_flag=True)
@click.option("--quiet", is_flag=True, help="Only warnings and errors; no per-step progress.")
def run_events(
    config_path: str,
    group_name: str | None,
    trade_date_str: str | None,
    backfill: bool,
    quiet: bool,
):
    """Run event-driven ingestion job (e.g. corporate_events, news_wire)."""
    _progress_logging(quiet)
    try:
        cfg = _cfg(config_path)
        cfg._validate_source_limits()
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    engine = JobEngine(cfg)
    td = date.fromisoformat(trade_date_str) if trade_date_str else None
    try:
        if group_name:
            group = cfg.event_groups.get(group_name)
            if not group:
                valid = ", ".join(cfg.event_groups.keys()) or "none configured"
                raise click.ClickException(
                    f"Unknown event group: {group_name}. Configured: {valid}"
                )
            result = engine.run_job(
                f"events:{group_name}",
                trade_date=td,
                waves=[
                    WaveConfig(
                        name=f"group:{group_name}",
                        parallel=getattr(group, "parallel", True),
                        steps=group.steps,
                    )
                ],
                backfill=backfill,
                ignore_calendar=True,
            )
        else:
            if not cfg.event_groups:
                raise click.ClickException("No event groups configured under [job.events.groups]")
            waves = [
                WaveConfig(
                    name=f"group:{name}",
                    parallel=getattr(grp, "parallel", True),
                    steps=grp.steps,
                )
                for name, grp in cfg.event_groups.items()
            ]
            result = engine.run_job(
                "events",
                trade_date=td,
                waves=waves,
                backfill=backfill,
                ignore_calendar=True,
            )
    except RunLockError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        json.dumps(
            {"run_id": result["run_id"], "status": result["status"]},
            indent=2,
        )
    )
    exit_code = _run_status_exit_code(result["status"])
    if exit_code:
        raise SystemExit(exit_code)


def _retry_single_run(engine: JobEngine, run_id: str) -> dict:
    """Retry one run, print its result, and return it to the CLI caller."""
    run = engine.manifest.get_run(run_id)
    if run is None:
        raise click.ClickException(f"Unknown run_id: {run_id}")
    try:
        if run["job_name"] == "init":
            result = engine.resume_init(run_id=run_id)
        else:
            result = engine.run_job("retry", retry_failed_only=True, run_id=run_id)
    except RunLockError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(result, indent=2, default=str))
    return result


def _failed_daily_group_runs(engine: JobEngine) -> list[dict]:
    """Return the latest failed run of each ``daily:*`` group."""
    latest: dict[str, dict] = {}
    for row in engine.manifest.list_runs():
        run = dict(row)
        job_name = str(run["job_name"])
        if job_name.startswith("daily:"):
            # Manifest order is newest first; an older failure must not be
            # replayed once a newer run for that group has succeeded.
            latest.setdefault(job_name, run)
    return [latest[name] for name in sorted(latest) if latest[name]["status"] == "failed"]


@cli.command()
@config_option
@click.option("--run-id", default=None, help="Retry a specific run.")
@click.option(
    "--failed-groups",
    is_flag=True,
    help="Retry the latest failed run of each daily group.",
)
def retry(config_path: str, run_id: str | None, failed_groups: bool):
    """Retry one run or every latest failed daily group."""
    cfg = _cfg(config_path)
    engine = JobEngine(cfg)
    if failed_groups:
        if run_id:
            raise click.ClickException("use either --run-id or --failed-groups, not both")
        runs = _failed_daily_group_runs(engine)
        if not runs:
            click.echo("No failed daily group run to retry.")
            return
        failed = False
        for run in runs:
            click.echo(f"Retrying failed daily group run {run['run_id']} ({run['job_name']})")
            # Heavy groups retain sizeable Polars/Python arenas. A fresh child
            # process per group releases that memory before the next retry.
            proc = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "from cnequity.cli.main import cli; cli.main()",
                    "retry",
                    "--config",
                    config_path,
                    "--run-id",
                    str(run["run_id"]),
                ],
                check=False,
            )
            if proc.returncode != 0:
                failed = True
        if failed:
            raise SystemExit(1)
        return
    if not run_id:
        raise click.ClickException("provide --run-id or --failed-groups")
    result = _retry_single_run(engine, run_id)
    exit_code = _run_status_exit_code(str(result.get("status", "failed")))
    if exit_code:
        raise SystemExit(exit_code)
