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
    config_option,
    parse_date_option,
)
from cnequity.cli.backfill_cmds import _run_backfill
from cnequity.domain.market_time import is_session_final, shanghai_today
from cnequity.orchestrator.manifest import Manifest
from cnequity.quality.audit import run_audit
from cnequity.storage.atomic import write_json_atomic


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
):
    """Run quality audit, or --full for a current whole-lake health snapshot."""
    cfg = _cfg(config_path)

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
        if not health["healthy"] or (research_start and not validity["universe_ready"]):
            raise SystemExit(1)
        return

    manifest = Manifest(cfg.manifest_path)
    latest = manifest.latest_run() if not run_id else None
    rid = run_id or (latest["run_id"] if latest else "manual")

    n = run_audit(cfg, rid, shanghai_today())
    click.echo(f"Audit complete: {n} findings written")


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


@cli.command()
@config_option
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
def verify(config_path: str, only: str | None, repair: bool, kinds: str | None):
    """Check what the lake should hold against what it does.

    `cne audit` asks whether the data that landed is correct. This asks whether
    the data that should have landed, landed — a different failure, and the one
    a step that raises on contact produces. Without it a dataset can fail every
    run for weeks while each individual run merely records a failed batch.

    Gaps are separated by whether anything can be done about them: a `by_date`
    dataset missing a session is a fault, a snapshot dataset missing one is its
    shape and no backfill can honestly fill it. `--repair` only ever runs the
    former.
    """
    from cnequity.quality.verify import verify_lake

    cfg = _cfg(config_path)
    anchor = _last_trading_day(cfg, shanghai_today())
    names = [s.strip() for s in only.split(",") if s.strip()] if only else None
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
@click.option("--run-id", "run_id", default=None, help="Alias for --run with an explicit id.")
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
def status(
    config_path: str,
    run_selector: str | None,
    run_id: str | None,
    show_datasets: bool,
    all_columns: bool,
):
    """Show latest run status, or per-dataset freshness with --datasets."""
    cfg = _cfg(config_path)

    if run_selector and run_id:
        raise click.UsageError("use either --run or --run-id, not both")
    if all_columns and not show_datasets:
        raise click.UsageError("--all-columns only applies with --datasets")

    if show_datasets:
        import polars as pl_mod

        from cnequity.domain.datasets import is_dataset_enabled, is_stale
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
        stale = df.filter(pl_mod.col("freshness") == "STALE").height
        if stale:
            click.echo(f"\n{stale} dataset(s) STALE — check runs with `cne status` / `cne retry`.")
            raise SystemExit(1)
        return

    manifest = Manifest(cfg.manifest_path)
    selected = run_selector or run_id
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
            "or `cne clean --reconcile-runs`"
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


@cli.command("servers", hidden=True)
@click.argument("action", type=click.Choice(["test"]))
@config_option
def servers(action: str, config_path: str):
    """Deprecated alias for the legacy TDX payload probe."""

    import warnings

    from cnequity.diagnostics.source_health import PROBES_BY_KEY, ProbeStatus, run_probe

    message = (
        "`cne servers test` is deprecated; use `cne sources probe --only "
        "tdx_protocol` for a payload health check. The alias is planned for "
        "removal in 0.9.0."
    )
    warnings.warn(message, DeprecationWarning, stacklevel=2)
    click.echo(f"warning: {message}", err=True)

    result = run_probe(PROBES_BY_KEY["tdx_protocol"], _cfg(config_path))
    if result.status == ProbeStatus.OK.value:
        click.echo(f"TDX connection OK ({result.detail}, {result.latency_ms}ms)")
        return
    click.echo(f"TDX {result.status}: {result.detail}", err=True)
    raise SystemExit(1)


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
@click.option("--out", type=click.Path(path_type=Path), default=None)
@click.option(
    "--enforce", is_flag=True, help="Exit 1 when a core dataset lacks an independent backup."
)
def source_resilience(out: Path | None, enforce: bool):
    """Show source concentration, blast radius and independent backup gate."""
    from cnequity.diagnostics.source_resilience import build_dependency_report

    report = build_dependency_report()
    payload = report.to_dict()
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


@cli.command("stability")
@config_option
@click.option("--days", default=20, show_default=True, type=click.IntRange(min=1))
@click.option("--as-of", default=None, help="Inclusive YYYY-MM-DD cutoff.")
@click.option("--enforce", is_flag=True, help="Exit 1 until the consecutive-day gate passes.")
def stability(config_path: str, days: int, as_of: str | None, enforce: bool):
    """Verify consecutive trading-day run evidence without filling gaps."""
    from cnequity.diagnostics.stability import evaluate_stability, store_stability_report
    from cnequity.query.parquet_scan import collect_parquet_root

    cfg = _cfg(config_path)
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
        required_days=days,
        as_of=parse_date_option(as_of, "--as-of"),
    )
    latest, historical = store_stability_report(cfg.meta_root, report)
    payload = report.to_dict()
    payload["latest_path"] = str(latest)
    payload["historical_path"] = str(historical)
    click.echo(json.dumps(payload, indent=2, ensure_ascii=False))
    if enforce and not report.passed:
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
