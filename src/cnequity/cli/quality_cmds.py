"""Is the lake right, and can it prove it: `audit`, `verify`, `status`,
`stability`, `sources`, `source`.

`audit` asks whether what landed is correct; `verify` asks whether what should
have landed, landed. Different failures, so different commands.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import click
import polars as pl

from cnequity.cli._root import cli
from cnequity.cli._shared import (
    _cfg,
    _progress_logging,
    attach_log_file,
    config_option,
    parse_date_option,
    resolve_config_path,
)
from cnequity.cli.backfill_cmds import _require_known_dataset, _run_backfill
from cnequity.domain.market_time import is_session_final, shanghai_today
from cnequity.orchestrator.manifest import Manifest
from cnequity.quality.audit import run_audit
from cnequity.storage.atomic import write_json_atomic


def _gate_groups(raw: str | None) -> set[str] | None:
    """Schedule groups the caller actually runs, or None for "gate on all"."""
    if raw is None:
        return None
    names = {part for part in raw.replace(",", " ").split() if part}
    return names or None


def stale_datasets_by_group(cfg, datasets: list[str]) -> dict[str, list[str]]:
    """Map stale dataset names to the schedule group that would fetch them.

    Datasets owned by no group land under ``(unscheduled)``: nothing routine
    fetches them, which is a different problem from a group that failed.
    """
    from cnequity.domain.datasets import DATASETS

    owner: dict[str, str] = {}
    for scope in ("schedule_groups", "events_groups"):
        for name, group in (getattr(cfg, scope, {}) or {}).items():
            for step in group.steps:
                dataset = step.removeprefix("derive_")
                owner.setdefault(dataset if dataset in DATASETS else step, name)
    out: dict[str, list[str]] = {}
    for dataset in datasets:
        out.setdefault(owner.get(dataset, "(unscheduled)"), []).append(dataset)
    return out


@cli.command()
@config_option
@click.option("--run-id", default=None)
@click.option(
    "--full",
    "full",
    is_flag=True,
    help="Whole-lake health snapshot (current state + freshness), not a per-run file.",
)
@click.option(
    "--research-start",
    default=None,
    help="Strictly validate a research window starting here (requires --full).",
)
@click.option(
    "--quality-only",
    is_flag=True,
    help="With --full, gate on quality errors; check scheduled freshness separately with status.",
)
@click.option(
    "--research-end",
    default=None,
    help="Research window end (default: latest daily_bars; requires --research-start).",
)
@click.option(
    "--research-universe",
    type=click.Choice(["all_a", "all_a_sh_sz"]),
    default="all_a",
    show_default=True,
    help="Historical research universe checked by --full.",
)
def audit(
    config_path: str,
    run_id: str | None,
    full: bool,
    research_start: str | None,
    research_end: str | None,
    research_universe: str,
    quality_only: bool = False,
):
    """Run quality audit, or --full for a current whole-lake health snapshot.

    The per-run audit is already the last step of the daily `finalize` wave, so
    running it here re-audits a run the job has audited. `--full` is the one
    that is not scheduled: it judges the lake as it stands now rather than what
    one run wrote, and it is what the health check and the dashboard read.
    """
    cfg = _cfg(config_path)
    attach_log_file(cfg, "audit")

    if quality_only and not full:
        raise click.ClickException("--quality-only requires --full")

    if research_start and not full:
        raise click.ClickException("--research-start requires --full")
    if research_end and not research_start:
        raise click.ClickException("--research-end requires --research-start")

    if full:
        from cnequity.quality.audit import lake_health

        start_date = parse_date_option(research_start, "--research-start")
        end_date = parse_date_option(research_end, "--research-end")
        if start_date and end_date and start_date > end_date:
            raise click.ClickException("--research-start must be on or before --research-end")
        health = lake_health(
            cfg,
            shanghai_today(),
            research_start=start_date,
            research_end=end_date,
            research_universe=research_universe,
        )
        sev = health["findings_by_severity"]
        click.echo(f"Lake health @ last trading day {health['last_trading_day']}")
        click.echo(
            f"  findings: {sev.get('error', 0)} error, "
            f"{sev.get('warning', 0)} warning, {sev.get('info', 0)} info"
        )
        if health["empty_datasets"]:
            click.echo(f"  empty datasets: {', '.join(health['empty_datasets'])}")
        if health.get("expected_empty_datasets"):
            click.echo(f"  expected empty datasets: {', '.join(health['expected_empty_datasets'])}")
        if health["stale_datasets"]:
            click.echo(f"  STALE datasets: {', '.join(health['stale_datasets'])}")
        for f in health["error_findings"]:
            click.echo(f"  [error]   {f.get('dataset', ''):22} {f.get('message', '')}")
        for f in health["warning_findings"]:
            click.echo(f"  [warning] {f.get('dataset', ''):22} {f.get('message', '')}")
        for f in health.get("info_findings", []):
            if f.get("source_limited"):
                click.echo(f"  [info]    {f.get('dataset', ''):22} {f.get('message', '')}")
        validity = health["historical_universe_validity"]
        research_state = "READY" if validity["universe_ready"] else "BLOCKED"
        universe_label = (
            "all-A"
            if validity.get("universe", research_universe) == "all_a"
            else validity.get("universe", research_universe)
        )
        click.echo(
            f"  historical {universe_label} "
            f"{validity['window']['start']}.."
            f"{validity['window']['end']}: {research_state}"
        )
        for blocker in validity["blockers"]:
            click.echo(f"  [research] {blocker['message']}")
            if blocker.get("remediation"):
                click.echo(f"              remediation: {blocker['remediation']}")
        if not health["healthy"]:
            click.echo("UNHEALTHY")
        elif research_start and not validity["universe_ready"]:
            # Operational freshness and research readiness are separate
            # contracts. Keep the former visible, but never let a green lake
            # label hide the strict research gate printed immediately above.
            click.echo("HEALTHY (operational; research BLOCKED)")
        else:
            click.echo("HEALTHY")
        failed = bool(sev.get("error", 0)) if quality_only else not health["healthy"]
        if quality_only:
            click.echo(
                "Quality gate: FAILED" if failed else "Quality gate: OK (freshness separate)"
            )
        if failed or (research_start and not validity["universe_ready"]):
            raise SystemExit(1)
        return

    manifest = Manifest(cfg.manifest_path)
    latest = manifest.latest_run() if not run_id else None
    rid = run_id or (latest["run_id"] if latest else "manual")

    severities: dict = {}
    n = run_audit(cfg, rid, shanghai_today(), severities)
    by_severity = severities.get("audit_by_severity", {})
    errors = int(by_severity.get("error", 0))
    warnings = int(by_severity.get("warning", 0))
    click.echo(f"Audit complete: {n} findings written ({errors} error, {warnings} warning)")
    # Exit like `--full` does. Callers use this as a gate, and a mode that
    # records errors and still reports success is a gate that never fires —
    # the daily health check had to shell out and re-read the findings file to
    # learn what the command already knew.
    if errors:
        for finding in severities.get("audit_error_findings", []):
            click.echo(f"  [error] {finding.get('dataset', ''):22} {finding.get('message', '')}")
        raise SystemExit(1)


def _last_trading_day(cfg, today: date) -> date:

    from cnequity.steps.common import is_trading_day

    d = today if is_session_final(today) else today - timedelta(days=1)
    for _ in range(15):
        if is_trading_day(cfg, d):
            return d
        d -= timedelta(days=1)
    return today


_GAP_LABELS = {
    "empty": "空",
    "stale": "陈旧",
    "interior": "区间内缺口",
    "shallow": "历史偏浅",
}


# `cne verify --runs` default. Four trading weeks: long enough that one bad
# evening cannot pass the gate, short enough to recover within a month.
DEFAULT_STABILITY_DAYS = 20


def _verify_bars(cfg, start: str | None, end: str | None) -> None:
    """Securities × sessions, including securities with no rows in the window."""
    from cnequity.quality.bar_coverage import daily_bar_coverage

    if not start:
        raise click.UsageError("--bars needs --start")
    start_date = parse_date_option(start, "--start")
    end_date = parse_date_option(end, "--end") or _last_trading_day(cfg, shanghai_today())
    if start_date > end_date:
        raise click.ClickException("--start must be on or before --end")
    result = daily_bar_coverage(cfg, start_date, end_date)
    click.echo(json.dumps(result, ensure_ascii=False, indent=2))
    if not result["complete"]:
        raise SystemExit(1)


def _verify_runs(cfg, days: int | None, as_of: str | None, enforce: bool) -> None:
    """Consecutive trading-day run evidence, without filling any gap it finds."""
    from cnequity.diagnostics.stability import evaluate_stability, store_stability_report
    from cnequity.query.parquet_scan import collect_parquet_root

    try:
        calendar = collect_parquet_root(
            cfg.curated_root / "trading_calendar", partition_col="trade_date"
        )
    except FileNotFoundError as exc:
        raise click.ClickException("curated trading_calendar is required") from exc
    trading_days = (
        calendar.filter(pl.col("is_trading"))["trade_date"].drop_nulls().unique().to_list()
    )
    report = evaluate_stability(
        Manifest(cfg.manifest_path),
        trading_days,
        required_days=days or DEFAULT_STABILITY_DAYS,
        as_of=parse_date_option(as_of, "--as-of"),
    )
    latest, historical = store_stability_report(cfg.meta_root, report)
    payload = report.to_dict()
    payload["latest_path"] = str(latest)
    payload["historical_path"] = str(historical)
    click.echo(json.dumps(payload, indent=2, ensure_ascii=False))
    if enforce and not report.passed:
        raise SystemExit(1)


# Which mode owns each option, so a flag from the wrong one is refused by name
# instead of being silently ignored. `--bars` and `--runs` were `cne verify-bars`
# and `cne stability`: three top-level commands, two of them a hyphen apart,
# all three answering "did what should have landed, land?".
_VERIFY_MODE_OPTIONS = {
    "datasets": (("--dataset", "only"), ("--repair", "repair"), ("--kind", "kinds")),
    "bars": (("--start", "start"), ("--end", "end")),
    "runs": (("--days", "days"), ("--as-of", "as_of"), ("--enforce", "enforce")),
}


def _verify_mode(bars: bool, runs: bool, given: dict) -> str:
    """Pick the mode, and refuse options belonging to the other two."""
    if bars and runs:
        raise click.UsageError("use either --bars or --runs, not both")
    mode = "bars" if bars else "runs" if runs else "datasets"
    for other, options in _VERIFY_MODE_OPTIONS.items():
        if other == mode:
            continue
        for flag, key in options:
            if given.get(key):
                hint = {
                    "datasets": "the default dataset-coverage mode",
                    "bars": "--bars",
                    "runs": "--runs",
                }[other]
                raise click.UsageError(f"{flag} belongs to {hint}")
    return mode


@cli.command()
@config_option
@click.option(
    "--bars",
    is_flag=True,
    help="Instead check securities × sessions, including securities with no rows "
    "in the window. Needs --start.",
)
@click.option(
    "--runs",
    is_flag=True,
    help="Instead check consecutive trading-day run evidence, without filling gaps.",
)
@click.option(
    "--dataset",
    "only",
    default=None,
    help="Verify these datasets only (comma-separated); default is every registered one.",
)
@click.option(
    "--repair",
    is_flag=True,
    help="Run the backfills that would close the repairable gaps, newest dataset first.",
)
@click.option(
    "--kind",
    "kinds",
    default=None,
    help="Limit to these gap kinds: empty,stale,interior,shallow.",
)
@click.option("--start", default=None, help="With --bars: inclusive coverage window start.")
@click.option(
    "--end",
    default=None,
    help="With --bars: window end; defaults to the last completed trading day.",
)
@click.option(
    "--days",
    default=None,
    type=click.IntRange(min=1),
    help=f"With --runs: consecutive trading days required (default: {DEFAULT_STABILITY_DAYS}).",
)
@click.option("--as-of", default=None, help="With --runs: inclusive YYYY-MM-DD cutoff.")
@click.option(
    "--enforce",
    is_flag=True,
    help="With --runs: exit 1 until the consecutive-day gate passes.",
)
def verify(
    config_path: str,
    bars: bool,
    runs: bool,
    only: str | None,
    repair: bool,
    kinds: str | None,
    start: str | None,
    end: str | None,
    days: int | None,
    as_of: str | None,
    enforce: bool,
):
    """Check what the lake should hold against what it does.

    `cne audit` asks whether the data that landed is correct. This asks whether
    the data that should have landed, landed — a different failure, and the one
    a step that raises on contact produces. Without it a dataset can fail every
    run for weeks while each individual run merely records a failed batch.

    Gaps are separated by whether anything can be done about them: a `by_date`
    dataset missing a session is a fault, a snapshot dataset missing one is its
    shape and no backfill can honestly fill it. `--repair` only ever runs the
    former.

    `--bars` and `--runs` ask the same question at two other grains: one
    security × session rather than dataset × session, and one run per trading
    day rather than rows in the lake.
    """
    from cnequity.quality.verify import verify_lake

    mode = _verify_mode(
        bars,
        runs,
        {
            "only": only,
            "repair": repair,
            "kinds": kinds,
            "start": start,
            "end": end,
            "days": days,
            "as_of": as_of,
            "enforce": enforce,
        },
    )
    cfg = _cfg(config_path)
    attach_log_file(cfg, "verify")
    if mode == "bars":
        _verify_bars(cfg, start, end)
        return
    if mode == "runs":
        _verify_runs(cfg, days, as_of, enforce)
        return

    anchor = _last_trading_day(cfg, shanghai_today())
    names = [s.strip() for s in only.split(",") if s.strip()] if only else None
    if names:
        # `verify_lake` warns and skips an unknown name, which is right for a
        # library sweeping the whole registry and wrong for a name the caller
        # typed: a misspelt `--dataset` printed "覆盖完整" and exited 0, so a
        # typo read as proof the lake was fine.
        names = [_require_known_dataset(name) for name in names]
    wanted = {s.strip() for s in kinds.split(",") if s.strip()} if kinds else None

    gaps = verify_lake(cfg, anchor=anchor, datasets=names)
    if wanted:
        gaps = [g for g in gaps if g.kind in wanted]

    click.echo(f"Verify @ {anchor.isoformat()} — {len(gaps)} gap(s)")
    if not gaps:
        click.echo("覆盖完整：没有可修复的缺口。")
        return

    for gap in gaps:
        label = _GAP_LABELS.get(gap.kind, gap.kind)
        flag = "可修复" if gap.repairable else "源的形态，无法回填"
        click.echo(f"  [{label}] {gap.dataset:28} {gap.detail}  ({flag})")
        if gap.sample:
            shown = ", ".join(d.isoformat() for d in gap.sample)
            more = (
                f" … 还有 {gap.missing_days - len(gap.sample)} 天"
                if gap.missing_days > len(gap.sample)
                else ""
            )
            click.echo(f"      例：{shown}{more}")

    repairable = [g for g in gaps if g.repairable]
    if not repair:
        if repairable:
            click.echo(f"\n{len(repairable)} 个缺口可修复。加 --repair 执行，或手动跑：")
            for gap in repairable[:10]:
                click.echo(f"  {gap.repair_command(config_path)}")
        raise SystemExit(1)

    if not repairable:
        click.echo("\n没有可修复的缺口。")
        raise SystemExit(1)

    click.echo(f"\n修复 {len(repairable)} 个缺口…")
    failed: list[str] = []
    for gap in repairable:
        click.echo(f"  → {gap.dataset} ({_GAP_LABELS.get(gap.kind, gap.kind)})")
        try:
            result = _run_backfill(cfg, gap.dataset, gap.start, gap.end)
        except Exception as exc:  # noqa: BLE001 — one gap must not sink the rest
            failed.append(gap.dataset)
            click.echo(f"    失败：{type(exc).__name__}: {exc}", err=True)
            continue
        # A failing step does not raise: the engine records a failed batch and
        # hands back status="failed". Reading only exceptions here reported
        # "全部修复完成" directly under a printed traceback.
        status = (result or {}).get("status", "unknown")
        written = (result or {}).get("rows_written", 0)
        if status != "success":
            failed.append(gap.dataset)
            click.echo(f"    失败：status={status}", err=True)
        elif not written:
            # Succeeded and wrote nothing: the window is genuinely empty
            # upstream, so re-running will not change it. Say so rather than
            # claiming a repair.
            click.echo("    源在该区间没有数据，缺口未变（重跑也不会变）")
        else:
            click.echo(f"    写入 {written:,} 行")
    if failed:
        click.echo(f"\n{len(failed)} 个未能修复：{', '.join(failed)}", err=True)
        raise SystemExit(1)
    click.echo("\n修复流程结束。再跑一次 `cne verify` 确认。")


@cli.command()
@config_option
@click.option(
    "--run",
    "run_selector",
    default=None,
    help="Run id to inspect, or 'latest' (the default). Includes dataset stage results.",
)
@click.option(
    "--datasets",
    "show_datasets",
    is_flag=True,
    help="Per-dataset freshness: coverage, watermark, and staleness vs the last trading day.",
)
@click.option(
    "--all-columns",
    "all_columns",
    is_flag=True,
    help="With --datasets, print every column of the dataset inventory, not just freshness.",
)
@click.option(
    "--groups",
    "gate_groups",
    default=None,
    help=(
        "With --datasets, fail only on datasets owned by these schedule groups "
        "(space or comma separated). Datasets in any other group are still "
        "listed and still reported as a schedule gap, but do not fail the gate."
    ),
)
def status(
    config_path: str,
    run_selector: str | None,
    show_datasets: bool,
    all_columns: bool,
    gate_groups: str | None,
):
    """Show latest run status, or per-dataset freshness with --datasets."""
    cfg = _cfg(config_path)

    if all_columns and not show_datasets:
        raise click.UsageError("--all-columns only applies with --datasets")
    if gate_groups and not show_datasets:
        raise click.UsageError("--groups only applies with --datasets")

    if show_datasets:
        import polars as pl_mod

        from cnequity.domain.datasets import DATASETS, is_dataset_enabled, is_stale
        from cnequity.query.reader import list_datasets

        anchor = _last_trading_day(cfg, shanghai_today())
        df = list_datasets(config=cfg)

        def _freshness(row: dict) -> str:
            if not row["has_data"]:
                return "empty"
            if not is_dataset_enabled(row["dataset"], cfg):
                return "n/a"
            # Datasets keyed by report_period (no daily watermark) are not
            # judged on a daily cadence.
            if not row["watermarked"]:
                return "n/a"
            mark = row["watermark"] or row["coverage_end"]
            # A source the exchanges retired has nothing further to publish, so
            # it is not stale — but calling a watermark from 2024 "fresh" reads
            # as current data. Name it for what it is.
            spec = DATASETS.get(row["dataset"])
            retired = getattr(spec, "source_retired_date", None) if spec else None
            if retired is not None and mark is not None and mark >= retired:
                return "retired"
            # Per-dataset tolerance (T+1, quarterly …) — inherent lag is not STALE.
            return "STALE" if is_stale(row["dataset"], mark, anchor) else "fresh"

        df = df.with_columns(
            pl_mod.Series("freshness", [_freshness(r) for r in df.iter_rows(named=True)])
        )
        click.echo(f"last trading day: {anchor.isoformat()}")
        # This flag is the freshness probe the runbooks reach for, but
        # `list_datasets` has grown to twenty columns — contract fingerprints,
        # revision ids, PIT storage lists — and forcing all of them into a
        # terminal shredded every value into unreadable vertical slivers. Show
        # the three things the flag promises; `--all-columns` still prints the
        # whole inventory. Intersected with what is actually there, because the
        # frame is narrower in tests and on older lakes.
        freshness_columns = [
            "dataset",
            "layer",
            "freshness",
            "has_data",
            "coverage_start",
            "coverage_end",
            "watermark",
        ]
        view = df if all_columns else df.select([c for c in freshness_columns if c in df.columns])
        with pl_mod.Config(tbl_rows=-1, tbl_cols=-1, fmt_str_lengths=32):
            click.echo(view)
        # A tolerated gap is invisible in freshness: the watermark moved over
        # the hole, so the dataset reads FRESH while still owing keys. The
        # ledger is the only place that knows, and nobody reads a json file
        # they were not told about.
        _report_outstanding_keys(cfg, df["dataset"].to_list())
        stale_rows = df.filter(pl_mod.col("freshness") == "STALE")
        stale = stale_rows.height
        if stale:
            click.echo(
                f"\n{stale} dataset(s) STALE — check runs with `cne status` / `cne run retry`."
            )
            # Which schedule group each one belongs to. A lake where every
            # stale dataset sits in groups this host never runs is a schedule
            # gap, not a broken pipeline, and the flat count cannot tell those
            # apart — it reads as a total outage either way.
            by_group = stale_datasets_by_group(cfg, stale_rows["dataset"].to_list())
            if by_group:
                summary = ", ".join(
                    f"{group} {len(names)}" for group, names in sorted(by_group.items())
                )
                click.echo(f"by schedule group: {summary}")
                click.echo(
                    "a group you do not schedule is a schedule gap, not a failure — "
                    "run it with `cne run daily --group <name>`."
                )
            # ...and until this flag existed, the gate said exactly that and then
            # failed anyway. A host scheduling `core` alone has twenty-odd
            # datasets no job ever fetches, so the freshness gate failed every
            # single day — 21 to 25 stale on 2026-09-12/13/14 — and the daily
            # "数据异常" notification became something to dismiss. Three real
            # `UNHEALTHY` days sat inside that noise unread.
            wanted = _gate_groups(gate_groups)
            if wanted is not None:
                # Exempt only what a *known* group other than mine owns. A
                # dataset nothing schedules — `(unscheduled)`, or a config with
                # no groups at all — still fails: "I cannot tell who fetches
                # this" is not the same claim as "another host fetches it", and
                # reading it as one would let a malformed config silence the
                # gate completely.
                gating = sorted(
                    name
                    for group, names in by_group.items()
                    if group in wanted or group == "(unscheduled)"
                    for name in names
                )
                skipped = stale - len(gating)
                if skipped:
                    click.echo(
                        f"gating on {', '.join(sorted(wanted))}: "
                        f"{len(gating)} stale here, {skipped} in groups this host does not run."
                    )
                if not gating:
                    return
            raise SystemExit(1)
        return

    manifest = Manifest(cfg.manifest_path)
    selected = run_selector
    if selected and selected != "latest":
        latest = manifest.get_run(selected)
        if latest is None:
            raise click.ClickException(f"Unknown run_id: {selected}")
    else:
        latest = manifest.latest_run()
    if not latest:
        click.echo("No runs yet.")
        return
    summary = manifest.run_summary(latest["run_id"])
    # Keep the historical summary shape while making `cne status --run latest`
    # easy for shell callers to consume.  ``run_summary`` now carries the
    # complete dataset_results list and aggregate dataset_status.
    if isinstance(summary, dict) and "run" in summary and summary["run"]:
        run_payload = summary["run"]
        summary.setdefault("run_id", run_payload.get("run_id"))
        summary.setdefault("status", run_payload.get("status"))
        summary.setdefault("job_name", run_payload.get("job_name"))
    orphaned = manifest.count_stale_running_runs(
        stale_after_seconds=cfg.batch_stale_seconds,
        locks_root=cfg.meta_root,
    )
    if orphaned:
        summary["orphaned_running_runs"] = orphaned
        summary["orphaned_note"] = (
            f"{orphaned} run(s) still status=running with no activity for "
            f">={int(cfg.batch_stale_seconds)}s — next cne run reconciles them; "
            "or `cne run clean --reconcile-runs`"
        )
    click.echo(json.dumps(summary, indent=2, default=str))
    run_status = str(summary.get("dataset_status") or summary.get("status") or "success")
    if run_status == "degraded":
        raise SystemExit(2)
    if run_status == "failed":
        raise SystemExit(1)


@cli.group("sources")
def sources_grp():
    """Probe the sources this lake depends on, and check the evidence.

    `probe` is the only one that touches the network; the rest read stored probe
    history and the dataset registry.

    These were `cne sources` and `cne source <sub>` — two top-level entries one
    letter apart, where the group's own help had to explain which was which.
    """


@sources_grp.command("slo")
@config_option
@click.option("--window-days", default=30, show_default=True, type=click.IntRange(min=1))
@click.option("--minimum-observations", default=10, show_default=True, type=click.IntRange(min=1))
@click.option("--enforce", is_flag=True, help="Exit 1 when a critical source SLO is not met.")
def source_slo(config_path: str, window_days: int, minimum_observations: int, enforce: bool):
    """Evaluate historical source probes and emit incident payloads."""
    from cnequity.diagnostics.source_slo import (
        build_source_incidents,
        evaluate_source_slo,
        load_health_history,
        store_source_incidents,
    )

    cfg = _cfg(config_path)
    history = load_health_history(cfg.meta_root)
    report = evaluate_source_slo(
        history,
        window_days=window_days,
        minimum_observations=minimum_observations,
        unreachable=frozenset(cfg.slo_unreachable_sources),
    )
    incidents = build_source_incidents(history)
    incident_path = store_source_incidents(cfg.meta_root, incidents)
    payload = report.to_dict()
    payload["incidents"] = incidents
    payload["incident_path"] = str(incident_path)
    click.echo(json.dumps(payload, indent=2, ensure_ascii=False))
    if enforce and not report.passed:
        raise SystemExit(1)


@sources_grp.command("resilience")
@config_option
@click.option("--out", type=click.Path(path_type=Path), default=None)
@click.option(
    "--enforce", is_flag=True, help="Exit 1 when a core dataset lacks an independent backup."
)
@click.option(
    "--with-availability",
    is_flag=True,
    help="Join measured probe availability onto each failure domain (reads the lake).",
)
@click.option("--window-days", default=30, show_default=True, type=click.IntRange(min=1))
def source_resilience(
    config_path: str,
    out: Path | None,
    enforce: bool,
    with_availability: bool,
    window_days: int,
):
    """Show source concentration, blast radius and independent backup gate.

    Concentration alone does not decide a routing question. The largest domain
    carries most of the registry, and that is only a problem in proportion to
    how often it is unreachable — which is measured, not declared. So
    `--with-availability` joins the probe history this lake has already
    accumulated onto each failure domain.

    The report itself is computed from the registry, so it needs no lake and
    answers the same way everywhere. `--config` is read only for
    `--with-availability`; passing one explicitly still resolves it, so a typo
    is an error here rather than a silently ignored flag.
    """
    from cnequity.diagnostics.source_resilience import (
        annotate_measured_availability,
        build_dependency_report,
    )

    ctx = click.get_current_context(silent=True)
    explicit_config = (
        ctx is not None
        and ctx.get_parameter_source("config_path") is click.core.ParameterSource.COMMANDLINE
    )
    if explicit_config and not with_availability:
        resolve_config_path(config_path)

    report = build_dependency_report()
    payload = report.to_dict()
    if with_availability:
        from cnequity.diagnostics.source_slo import evaluate_source_slo, load_health_history

        cfg = _cfg(config_path)
        slo = evaluate_source_slo(
            load_health_history(cfg.meta_root), window_days=window_days
        ).to_dict()
        payload = annotate_measured_availability(payload, slo)
    if out is not None:
        write_json_atomic(out, payload, indent=2, ensure_ascii=False)
    click.echo(json.dumps(payload, indent=2, ensure_ascii=False))
    if enforce and not report.passed:
        raise SystemExit(1)


@sources_grp.command("policy")
@click.argument("source", required=False)
@click.option(
    "--profile",
    type=click.Choice(["personal", "commercial", "cache", "redistribution"]),
    default=None,
)
@click.option("--redistribution", is_flag=True)
def source_policy(source: str | None, profile: str | None, redistribution: bool):
    """Inspect source-use policy; unknown permission fails closed."""
    from cnequity.compliance.source_policy import load_source_policies, usage_profile

    policies = load_source_policies()
    if source is None:
        click.echo(
            json.dumps(
                {name: policy.as_dict() for name, policy in sorted(policies.items())},
                indent=2,
                ensure_ascii=False,
            )
        )
        return
    if source not in policies:
        raise click.ClickException(f"unknown source policy {source!r}")
    assessment = usage_profile(
        policies[source],
        profile=profile,
        redistribution=redistribution,
    )
    click.echo(json.dumps(assessment.as_dict(), indent=2, ensure_ascii=False))
    if not assessment.allowed:
        raise SystemExit(1)


@sources_grp.command("probe")
@config_option
@click.option(
    "--vantage",
    default="local",
    show_default=True,
    help="Where this probe ran from — 'cn', 'overseas', or any label you use. "
    "Several sources refuse non-mainland egress, so a result without this is "
    "not interpretable.",
)
@click.option("--only", default=None, help="Comma-separated probe keys; default is all of them.")
@click.option(
    "--out",
    default=None,
    help="Where to write the JSON report. Defaults to meta/source_health/<vantage>.json "
    "inside the lake, which is where `cne serve` reads it from.",
)
def sources_probe(config_path: str, vantage: str, only: str | None, out: str | None):
    """Probe the public sources this lake depends on.

    One request per source, serial and polite: these are the same hosts the
    daily pipeline uses, and a health check that trips a rate-limit ban would be
    causing the outage it is meant to observe.

    The report lands in the lake, and `cne serve` renders it at /source-health.
    Probing is a CLI action on purpose — the dashboard stays read-only, and an
    unauthenticated local service that can reach out to a dozen third parties
    is not something to leave listening.
    """
    from cnequity.diagnostics.source_health import STATUS_LABELS, ProbeStatus, run_probes

    _progress_logging(quiet=True)
    cfg = _cfg(config_path)
    keys = [k.strip() for k in only.split(",") if k.strip()] if only else None
    report = run_probes(cfg, vantage=vantage, only=keys)

    for result in report.results:
        latency = f"{result.latency_ms:>6}ms" if result.latency_ms is not None else "     \u2014"
        label = STATUS_LABELS[ProbeStatus(result.status)]
        click.echo(f"{result.status:<8}{label:<5}{latency}  {result.key:<22}{result.detail}")

    if out:
        path = Path(out)
        write_json_atomic(path, report.to_dict(), indent=2, ensure_ascii=False)
    else:
        from cnequity.diagnostics.source_slo import store_health_report

        path, historical = store_health_report(cfg.meta_root, report)
        click.echo(f"Historical sample: {historical}")
    click.echo(f"\nWrote {path}")
    click.echo("View it with: cne serve  \u2192  http://127.0.0.1:8787/source-health")


@sources_grp.command("substitutes")
@config_option
@click.option(
    "--vantage", default="local", show_default=True, help="Which vantage's report to read."
)
@click.option(
    "--probe/--no-probe",
    default=False,
    show_default=True,
    help="Measure now instead of reading the stored report. Same requests as `sources probe`.",
)
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output.")
def sources_substitutes(config_path: str, vantage: str, probe: bool, as_json: bool):
    """For every source that is down, what can still answer for its datasets.

    A probe report says what is up; this says what to do about what is not.
    Substitutes are ranked independent-first, because an endpoint that shares a
    blast radius with the one that failed is not a second opinion — EastMoney's
    history host cannot stand in for EastMoney's snapshot host.

    Exits non-zero when a dataset is stranded: something it needs is down and
    nothing reachable is left.
    """
    import json as json_mod

    from cnequity.diagnostics.source_health import HealthReport, run_probes
    from cnequity.diagnostics.substitutes import (
        render_substitutions,
        substitution_report,
        to_dict,
    )

    _progress_logging(quiet=True)
    cfg = _cfg(config_path)
    if probe:
        report = run_probes(cfg, vantage=vantage)
    else:
        path = cfg.meta_root / "source_health" / f"{vantage}.json"
        if not path.exists():
            raise click.ClickException(
                f"No probe report for vantage {vantage!r} at {path}. "
                "Run `cne sources probe` first, or pass --probe to measure now."
            )
        report = HealthReport.from_dict(json_mod.loads(path.read_text(encoding="utf-8")))

    entries = substitution_report(report)
    if as_json:
        click.echo(json.dumps(to_dict(entries), indent=2, ensure_ascii=False))
    else:
        click.echo(f"探测时间 {report.generated_at} · vantage {report.vantage}")
        for line in render_substitutions(entries):
            click.echo(line)
    if any(entry.stranded for entry in entries):
        raise SystemExit(1)


def _report_outstanding_keys(cfg, datasets: list[str]) -> None:
    """Name the datasets carrying a debt a tolerated gap left behind."""
    from cnequity.storage.state import StateStore

    store = StateStore(cfg.meta_root)
    owed = []
    for dataset in datasets:
        try:
            rows = store.get_outstanding_keys(dataset)
        except Exception:  # noqa: BLE001 — a missing/garbled state file is not a status failure
            continue
        if rows:
            owed.append((dataset, len(rows)))
    if not owed:
        return
    summary = ", ".join(f"{dataset} {count}" for dataset, count in sorted(owed))
    click.echo(
        f"\noutstanding keys from tolerated gaps: {summary}"
        "\nthese sessions are past the watermark, so no incremental run will ask for them — "
        "fill them with `cne backfill <dataset> --outstanding`."
    )
