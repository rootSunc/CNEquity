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
    help="整个湖的健康快照（当前状态 + 新鲜度），而不是某一次 run 的文件。",
)
@click.option(
    "--research-start",
    default=None,
    help="严格校验从这一天开始的研究窗口（需要 --full）。",
)
@click.option(
    "--quality-only",
    is_flag=True,
    help="配合 --full：只按质量 error 判门禁；调度新鲜度请另外用 status 检查。",
)
@click.option(
    "--research-end",
    default=None,
    help="研究窗口终点（默认取最新的 daily_bars；需要 --research-start）。",
)
@click.option(
    "--research-universe",
    type=click.Choice(["all_a", "all_a_sh_sz"]),
    default="all_a",
    show_default=True,
    help="--full 检查的历史研究 universe。",
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
    """跑质量审计；加 --full 则给出当前整个湖的健康快照。

    \b
    按 run 的审计本来就是日更 `finalize` wave 的最后一步，所以在这里跑等于把已经审过的 run 再审一遍。
    没有被调度的是 `--full`：它判的是此刻这个湖的状态，而不是某一次 run 写了什么，
    健康检查和面板读的也是它。
    """
    cfg = _cfg(config_path)
    attach_log_file(cfg, "audit")

    if quality_only and not full:
        raise click.ClickException("--quality-only 需要配合 --full")

    if research_start and not full:
        raise click.ClickException("--research-start 需要配合 --full")
    if research_end and not research_start:
        raise click.ClickException("--research-end 需要配合 --research-start")

    if full:
        from cnequity.quality.audit import lake_health

        start_date = parse_date_option(research_start, "--research-start")
        end_date = parse_date_option(research_end, "--research-end")
        if start_date and end_date and start_date > end_date:
            raise click.ClickException("--research-start 必须早于或等于 --research-end")
        health = lake_health(
            cfg,
            shanghai_today(),
            research_start=start_date,
            research_end=end_date,
            research_universe=research_universe,
        )
        sev = health["findings_by_severity"]
        click.echo(f"湖健康度 @ 最后交易日 {health['last_trading_day']}")
        click.echo(
            f"  findings：{sev.get('error', 0)} error、"
            f"{sev.get('warning', 0)} warning、{sev.get('info', 0)} info"
        )
        if health["empty_datasets"]:
            click.echo(f"  空数据集：{', '.join(health['empty_datasets'])}")
        if health.get("expected_empty_datasets"):
            click.echo(f"  预期就是空的数据集：{', '.join(health['expected_empty_datasets'])}")
        if health["stale_datasets"]:
            click.echo(f"  STALE 数据集：{', '.join(health['stale_datasets'])}")
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
            f"  历史 {universe_label} "
            f"{validity['window']['start']}.."
            f"{validity['window']['end']}：{research_state}"
        )
        for blocker in validity["blockers"]:
            click.echo(f"  [research] {blocker['message']}")
            if blocker.get("remediation"):
                click.echo(f"              修复建议：{blocker['remediation']}")
        if not health["healthy"]:
            click.echo("UNHEALTHY")
        elif research_start and not validity["universe_ready"]:
            # Operational freshness and research readiness are separate
            # contracts. Keep the former visible, but never let a green lake
            # label hide the strict research gate printed immediately above.
            click.echo("HEALTHY（运维层面；研究层面 BLOCKED）")
        else:
            click.echo("HEALTHY")
        failed = bool(sev.get("error", 0)) if quality_only else not health["healthy"]
        if quality_only:
            click.echo("Quality gate: FAILED" if failed else "Quality gate: OK（新鲜度另算）")
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
    click.echo(f"审计完成：写入 {n} 条 findings（{errors} error，{warnings} warning）")
    if getattr(cfg, "lake_profile", None) == "sample" and errors:
        # The fabricated-row check is doing its job here, loudly and correctly.
        # Say which lake it is looking at, so a first-time reader does not take
        # `cne init --profile sample` for a broken install.
        click.echo(
            "sample 湖：每一行都是刻意生成的合成数据（source=mock），所以下面那些"
            "「伪造行」findings 正是这个 profile 在正常工作，不是缺陷。"
            "要建真数据的湖，用 `cne init --profile demo`（或 `cne config create` + `cne init`）。"
        )
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


def _datasets_with_data(cfg) -> list[str]:
    """Registered datasets that have Parquet in this lake, in registry order."""
    from cnequity.domain.datasets import DATASETS
    from cnequity.query.parquet_scan import dataset_has_parquet

    out = []
    for name, spec in DATASETS.items():
        root = cfg.derived_root if spec.layer == "derived" else cfg.curated_root
        if dataset_has_parquet(root / name):
            out.append(name)
    return sorted(out)


def _verify_bars(cfg, start: str | None, end: str | None) -> None:
    """Securities × sessions, including securities with no rows in the window."""
    from cnequity.quality.bar_coverage import daily_bar_coverage

    if not start:
        raise click.UsageError("--bars 需要配合 --start")
    start_date = parse_date_option(start, "--start")
    end_date = parse_date_option(end, "--end") or _last_trading_day(cfg, shanghai_today())
    if start_date > end_date:
        raise click.ClickException("--start 必须早于或等于 --end")
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
        raise click.ClickException("需要 curated 里的 trading_calendar") from exc
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
        raise click.UsageError("--bars 和 --runs 只能用一个")
    mode = "bars" if bars else "runs" if runs else "datasets"
    for other, options in _VERIFY_MODE_OPTIONS.items():
        if other == mode:
            continue
        for flag, key in options:
            if given.get(key):
                hint = {
                    "datasets": "默认的数据集覆盖模式",
                    "bars": "--bars",
                    "runs": "--runs",
                }[other]
                raise click.UsageError(f"{flag} 属于 {hint}")
    return mode


@cli.command()
@config_option
@click.option(
    "--bars",
    is_flag=True,
    help="改为检查「证券 × 交易日」，含窗口内一行都没有的证券。需要 --start。",
)
@click.option(
    "--runs",
    is_flag=True,
    help="改为检查连续交易日的运行证据，不补任何缺口。",
)
@click.option(
    "--dataset",
    "only",
    default=None,
    help="只校验这些数据集（逗号分隔）；默认校验注册表里的全部。",
)
@click.option(
    "--repair",
    is_flag=True,
    help="把能补的缺口跑一遍回填，按数据集从新到旧。",
)
@click.option(
    "--kind",
    "kinds",
    default=None,
    help="只看这些缺口类型：empty,stale,interior,shallow。",
)
@click.option("--start", default=None, help="配合 --bars：覆盖窗口起点（含）。")
@click.option(
    "--end",
    default=None,
    help="配合 --bars：窗口终点，默认上一个完整交易日。",
)
@click.option(
    "--days",
    default=None,
    type=click.IntRange(min=1),
    help=f"配合 --runs：要求连续多少个交易日（默认 {DEFAULT_STABILITY_DAYS}）。",
)
@click.option("--as-of", default=None, help="配合 --runs：截止日期 YYYY-MM-DD（含）。")
@click.option(
    "--enforce",
    is_flag=True,
    help="配合 --runs：连续天数门禁没过就退出 1。",
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
    """拿这个湖应该有的东西，对一对它实际有的东西。

    \b
    `cne audit` 问的是落进来的数据对不对。这条问的是该落的有没有落 ——
    这是另一种失败，也是一碰就抛的 step 会造成的那种。没有它，一个数据集可以连着几周每次 run 都失败，
    而每一次 run 都只是记一条 failed batch。

    \b
    缺口按「能不能补」分开：`by_date` 数据集少一个交易日是故障，snapshot 数据集少一个是它本来的形态，
    任何回填都补不诚实。`--repair` 只会去跑前一种。

    \b
    `--bars` 和 `--runs` 在另外两个粒度上问同一个问题：一个是「证券 × 交易日」而不是「数据集 × 交易日」，
    另一个是「每个交易日一次 run」而不是湖里的行。
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

    if names is None and getattr(cfg, "lake_profile", None) in {"demo", "sample"}:
        # A demo lake holds a handful of symbols and two or three datasets on
        # purpose. Measured against the whole registry it reported 35 gaps and
        # exited 1, which reads — to someone who has just run their first
        # command — as a broken install. Judge what this lake actually holds.
        names = _datasets_with_data(cfg)
        click.echo(
            f"demo 湖（{cfg.lake_profile}）：只检查它实际持有的 {len(names)} 个数据集"
            f"（{', '.join(names) or '无'}）；其余的从来没有采集过，"
            "这是这个 profile 本来的样子，不是缺口。"
        )

    gaps = verify_lake(cfg, anchor=anchor, datasets=names)
    if wanted:
        gaps = [g for g in gaps if g.kind in wanted]
    if getattr(cfg, "lake_profile", None) == "sample" and not wanted:
        # Synthetic rows carry the dates the generator chose, so this lake is
        # stale the moment it is written and stays that way. Repairing it means
        # fetching real bars into a lake whose every row says `source=mock`,
        # which is the one thing the sample profile exists to prevent.
        stale = [gap for gap in gaps if gap.kind == "stale"]
        if stale:
            gaps = [gap for gap in gaps if gap.kind != "stale"]
            click.echo(
                f"sample 湖：不对 {len(stale)} 个数据集判新鲜度 —— "
                "这些行是合成的（source=mock），日期由生成器决定。"
            )

    click.echo(f"校验 @ {anchor.isoformat()} —— {len(gaps)} 个缺口")
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
    help="要看的 run id，或 'latest'（默认）。包含各数据集 stage 的结果。",
)
@click.option(
    "--datasets",
    "show_datasets",
    is_flag=True,
    help="逐数据集的新鲜度：覆盖区间、水位，以及相对最后交易日是否陈旧。",
)
@click.option(
    "--all-columns",
    "all_columns",
    is_flag=True,
    help="配合 --datasets：打印数据集清单的全部列，而不只是新鲜度。",
)
@click.option(
    "--groups",
    "gate_groups",
    default=None,
    help=(
        "配合 --datasets：只对这些调度组拥有的数据集判失败（空格或逗号分隔）。其它组的数据集照常列出、照常报为调度缺口，"
        "但不会让门禁失败。"
    ),
)
def status(
    config_path: str,
    run_selector: str | None,
    show_datasets: bool,
    all_columns: bool,
    gate_groups: str | None,
):
    """查看最近一次 run 的状态；加 --datasets 则看逐数据集的新鲜度。"""
    cfg = _cfg(config_path)

    if all_columns and not show_datasets:
        raise click.UsageError("--all-columns 只能配合 --datasets 使用")
    if gate_groups and not show_datasets:
        raise click.UsageError("--groups 只能配合 --datasets 使用")

    if show_datasets:
        import polars as pl_mod

        from cnequity.domain.datasets import (
            DATASETS,
            empty_freshness_label,
            is_dataset_enabled,
            is_stale,
        )
        from cnequity.query.reader import list_datasets

        anchor = _last_trading_day(cfg, shanghai_today())
        df = list_datasets(config=cfg)

        def _freshness(row: dict) -> str:
            if not row["has_data"]:
                # "empty" alone cannot say whether the dataset is waiting
                # for its first run or for a source that no longer exists.
                return empty_freshness_label(row["dataset"])
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
        click.echo(f"最后交易日：{anchor.isoformat()}")
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
                f"\n{stale} 个数据集 STALE —— 用 `cne status` / `cne run retry` 查一下相关 run。"
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
                click.echo(f"按调度组：{summary}")
                click.echo(
                    "你没有排期的调度组，是调度缺口而不是失败 —— "
                    "用 `cne run daily --group <名字>` 跑它。"
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
                        f"门禁只看 {', '.join(sorted(wanted))}："
                        f"其中 {len(gating)} 个 stale，另有 {skipped} 个属于这台机器不跑的组。"
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
            raise click.ClickException(f"未知 run_id：{selected}")
    else:
        latest = manifest.latest_run()
    if not latest:
        click.echo("还没有任何 run。")
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
    """探测这个湖依赖的数据源，并检查相关证据。

    \b
    只有 `probe` 会碰网络；其余都只读已存的探测历史和数据集注册表。

    \b
    它们以前是 `cne sources` 和 `cne source <子命令>` —— 两个只差一个字母的顶层命令，
    以至于这个组的帮助文本本身得先解释哪个是哪个。
    """


@sources_grp.command("slo")
@config_option
@click.option("--window-days", default=30, show_default=True, type=click.IntRange(min=1))
@click.option("--minimum-observations", default=10, show_default=True, type=click.IntRange(min=1))
@click.option("--enforce", is_flag=True, help="关键源的 SLO 未达标时退出 1。")
def source_slo(config_path: str, window_days: int, minimum_observations: int, enforce: bool):
    """评估历史源探测记录，并输出事件载荷。"""
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
@click.option("--enforce", is_flag=True, help="核心数据集没有独立备份时退出 1。")
@click.option(
    "--with-availability",
    is_flag=True,
    help="把实测的探测可用率接到每个故障域上（会读湖）。",
)
@click.option("--window-days", default=30, show_default=True, type=click.IntRange(min=1))
def source_resilience(
    config_path: str,
    out: Path | None,
    enforce: bool,
    with_availability: bool,
    window_days: int,
):
    """展示源的集中度、影响半径，以及独立备份门禁。

    \b
    只看集中度决定不了路由问题。最大的那个域承载了注册表里的大部分数据集，
    但这件事有多严重，取决于它多久不可达一次 —— 那是测出来的，不是声明出来的。
    所以 `--with-availability` 会把这个湖已经积累的探测历史，接到每一个故障域上。

    \b
    报告本身由注册表算出，因此不需要湖，在哪儿跑答案都一样。
    `--config` 只在 `--with-availability` 时才读；显式传了也仍然会解析，
    所以拼错路径在这里是报错，而不是被悄悄忽略。
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
    """查看源的使用政策；权限不明时按拒绝处理。"""
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
        raise click.ClickException(f"未知的源政策 {source!r}")
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
    help="这次探测是从哪儿跑的 —— 'cn'、'overseas'，或你自己用的任何标签。有几个源拒绝非大陆出口，所以没有这个标签的结果没法解读。",
)
@click.option("--only", default=None, help="要探测的 key，逗号分隔；默认全部。")
@click.option(
    "--out",
    default=None,
    help=(
        "JSON 报告写到哪。默认写到湖内的 meta/source_health/<vantage>.json，`cne serve` "
        "也从那里读。"
    ),
)
def sources_probe(config_path: str, vantage: str, only: str | None, out: str | None):
    """探测这个湖依赖的公开数据源。

    \b
    每个源一个请求，串行且克制：这些正是日更 pipeline 用的主机，
    一个把自己探到被限流封禁的健康检查，等于亲手制造它本要观测的故障。

    \b
    报告写进湖里，`cne serve` 在 /source-health 上渲染它。
    探测被有意做成 CLI 动作 —— 面板保持只读，
    而一个不需要认证、却能主动连出十几家第三方的本地服务，不适合一直挂在那里听。
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
        click.echo(f"历史抽样：{historical}")
    click.echo(f"\n已写入 {path}")
    click.echo("查看方式：cne serve  \u2192  http://127.0.0.1:8787/source-health")


@sources_grp.command("substitutes")
@config_option
@click.option(
    "--vantage", default="local", show_default=True, help="读哪个出口位置（vantage）的报告。"
)
@click.option(
    "--probe/--no-probe",
    default=False,
    show_default=True,
    help="现在实测，而不是读已存的报告。请求和 `sources probe` 相同。",
)
@click.option("--json", "as_json", is_flag=True, help="输出机器可读的 JSON。")
def sources_substitutes(config_path: str, vantage: str, probe: bool, as_json: bool):
    """对每个挂掉的源，还有谁能替它的数据集作答。

    \b
    探测报告说的是什么活着；这条说的是对死掉的那些该怎么办。
    候选按「是否独立」优先排序，因为和故障源共享影响半径的端点算不上第二个意见 ——
    东财的历史主机替不了东财的快照主机。

    \b
    当某个数据集被困住 —— 它需要的东西挂了，而可达的替代一个都不剩 —— 时非零退出。
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
                f"{path} 下没有 vantage {vantage!r} 的探测报告。"
                "先跑 `cne sources probe`，或者加 --probe 现在实测。"
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
            stubborn = sum(1 for row in rows if int(row.get("attempts", 0) or 0) >= 3)
            owed.append((dataset, len(rows), stubborn))
    if not owed:
        return
    summary = ", ".join(f"{dataset} {count}" for dataset, count, _ in sorted(owed))
    click.echo(
        f"\n被容忍缺口欠下的 key：{summary}"
        "\n这些交易日已经在水位之后，任何增量 run 都不会再去要它们 —— "
        "用 `cne backfill <数据集> --outstanding` 补上。"
    )
    # Separated because they need a different decision. A key three repairs
    # could not fill is not backlog, it is a key no configured source serves,
    # and re-running the repair will not change that.
    stuck = [(dataset, n) for dataset, _count, n in sorted(owed) if n]
    if stuck:
        detail = ", ".join(f"{dataset} {n}" for dataset, n in stuck)
        click.echo(
            f"其中修复尝试 3 次以上仍未补上的：{detail}"
            " —— 没有任何已配置的源提供它们；查一下 `cne sources probe`，"
            "或者接受这个缺口。"
        )
