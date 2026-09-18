"""The scheduled path, and the way back from a failed run: `run daily`,
`run events`, `run retry`.

`run daily --all-groups` composes one day's schedule groups, because the shell
pipeline that used to be the only way to run them all is not installed by the
package. Everything around a day — health check, source probe, metadata backup,
the late stale-only pass — still lives in `scripts/daily_pipeline.sh`.
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import date
from pathlib import Path

import click

from cnequity.cli._root import run
from cnequity.cli._shared import (
    _cfg,
    _progress_logging,
    _run_status_exit_code,
    attach_log_file,
    config_option,
    parse_date_option,
)
from cnequity.cli.quality_cmds import _last_trading_day
from cnequity.config import WaveConfig
from cnequity.domain.market_time import shanghai_today
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.run_lock import RunLockError


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


def stale_fetch_plan(cfg, anchor: date, *, groups: set[str] | None = None) -> list[dict]:
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
        for past in manifest.list_runs():
            for receipt in manifest.get_dataset_results(past["run_id"], dataset=dataset):
                if receipt["status"] in {"failed", "warning", "degraded", "blocked"}:
                    return True
            for batch in manifest.get_batches_for_run(past["run_id"]):
                if batch["dataset"] == dataset and batch["status"] in {
                    "failed",
                    "warning",
                    "stale",
                }:
                    return True
        return False

    # `getattr`: the plan is also driven from lightweight stand-in configs
    # (tests, tooling) that carry only the fields it reads.
    events_owned = {
        step for group in getattr(cfg, "events_groups", {}).values() for step in group.steps
    }
    allowed = None
    if groups is not None:
        known = getattr(cfg, "schedule_groups", {})
        unknown = groups - known.keys()
        if unknown:
            raise click.ClickException(f"未知的 stale 调度组：{', '.join(sorted(unknown))}")
        allowed = {step for name in groups for step in known[name].steps}

    out: list[dict] = []
    for row in list_datasets(config=cfg).iter_rows(named=True):
        name = row["dataset"]
        if allowed is not None and name not in allowed:
            continue
        spec = DATASETS[name]
        if spec.layer == "derived" or name not in STEP_REGISTRY:
            continue
        if not is_dataset_enabled(name, cfg):
            continue
        if name in events_owned:
            # The events job owns this feed and holds a different lock, so
            # re-fetching it from the daily pass would ingest it concurrently
            # with a sweep — and redundantly: every sweep re-reads its own
            # window, which is what a stale-only pass is for.
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


def _group_is_runnable(cfg, group) -> bool:
    """False when every dataset the group would fetch is disabled.

    `intraday` and `ticks` are configured but unscheduled: their datasets are
    opt-in and off by default. Running them from `--all-groups` would report a
    failure for work nobody asked for.
    """
    from cnequity.domain.datasets import DATASETS, is_dataset_enabled

    datasets = [step for step in group.steps if step in DATASETS]
    if not datasets:
        return True
    return any(is_dataset_enabled(name, cfg) for name in datasets)


def _run_all_groups(cfg, engine: JobEngine, td: date | None, *, backfill: bool, repairs: list):
    """Run every schedule group in config order, one at a time.

    A day's ingestion is six groups, and until now the only thing that ran all
    six was `scripts/daily_pipeline.sh` — which is not installed by the PyPI
    package. So the documented answer for anyone who had only `pip install`
    was six cron lines, and the single command they reached for instead
    (`cne run daily`) silently covered a third of the lake.

    One group failing does not stop the rest, exactly as the script does it:
    the point is to get as much of the day as the sources will give. The exit
    code is the worst of them.
    """
    if not cfg.schedule_groups:
        raise click.ClickException(
            f"{getattr(cfg, 'config_path', None) or '这份配置'} 里没有 [job.daily.groups] —— "
            "`cne config create` 生成的配置带有这些调度组。"
        )
    results: list[dict] = []
    worst = 0
    for name, group in cfg.schedule_groups.items():
        if not _group_is_runnable(cfg, group):
            click.echo(f"调度组 {name}：跳过（它要抓的数据集全部处于关闭状态）", err=True)
            results.append({"group": name, "status": "skipped_disabled"})
            continue
        try:
            result = engine.run_job(
                f"daily:{name}",
                trade_date=td,
                waves=[
                    WaveConfig(
                        name=f"group:{name}",
                        parallel=getattr(group, "parallel", True),
                        steps=group.steps,
                    )
                ],
                backfill=backfill,
            )
        except RunLockError as exc:
            # Another daily job holds the ingestion lock. That is a reason to
            # stop, not to walk the remaining groups into the same wall.
            raise click.ClickException(str(exc)) from exc
        except Exception as exc:  # noqa: BLE001 — one group must not sink the day
            click.echo(f"调度组 {name}：{type(exc).__name__}: {exc}", err=True)
            results.append({"group": name, "status": "failed", "error": str(exc)})
            worst = max(worst, 1)
            continue
        status = result["status"]
        results.append({"group": name, "run_id": result["run_id"], "status": status})
        click.echo(f"调度组 {name}：{status}", err=True)
        worst = max(worst, _run_status_exit_code(status))
    click.echo(json.dumps({"groups": results, "repairs": repairs}, indent=2))
    if worst:
        raise SystemExit(worst)


def _run_stale_only(
    cfg,
    engine,
    trade_date: date | None,
    *,
    backfill: bool,
    repair_gaps: bool = False,
    groups: set[str] | None = None,
) -> None:
    """Second attempt, same day, for whatever the first attempt did not land.

    The gap this closes: a ``snapshot`` dataset fetches only the run day, so a
    source outage during the one scheduled window loses that day permanently —
    ``valuation_metrics`` lost 2026-07-30 and 07-31 to a push2 clist outage and
    no later run could have recovered them. Per-host retries already exist and
    were exhausted; what was missing was a second window.
    """
    anchor = _last_trading_day(cfg, trade_date or shanghai_today())
    if groups is not None and repair_gaps:
        raise click.ClickException("限定了 --groups 就不能再用全湖范围的 --repair-gaps")
    repairs = _auto_repair_gaps(cfg, anchor) if repair_gaps else []
    plan = (
        stale_fetch_plan(cfg, anchor)
        if groups is None
        else stale_fetch_plan(cfg, anchor, groups=groups)
    )
    steps = [item["dataset"] for item in plan]
    if not plan:
        click.echo(f"截至 {anchor.isoformat()} 没有落后的数据集")
        click.echo(
            json.dumps(
                {"anchor": anchor.isoformat(), "repairs": repairs, "status": "nothing_stale"},
                indent=2,
            )
        )
        return
    click.echo(f"截至 {anchor.isoformat()} 落后的数据集：{', '.join(steps)}", err=True)
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
    # Repaired bars must reach the same derived outputs as the scheduled group.
    # Otherwise a successful core retry leaves factors/industry indexes behind.
    derived_steps = list(
        dict.fromkeys(
            step
            for name, group in getattr(cfg, "schedule_groups", {}).items()
            if (groups is None or name in groups) and set(steps).intersection(group.steps)
            for step in group.steps
            if step.startswith("derive_")
        )
    )
    if derived_steps:
        waves.append(WaveConfig(name="stale:derive", parallel=False, steps=derived_steps))
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


def datasets_outside_the_daily_waves(cfg) -> list[str]:
    """Enabled, fetchable datasets that a bare ``cne run daily`` never touches.

    ``cne run daily`` without ``--group`` runs ``[[job.daily.waves]]`` — the
    core spine — while two thirds of the registered datasets live in
    ``[job.daily.groups.*]``. Running only that one line builds a lake where
    most datasets are silently never updated and nothing ever fails, which is
    exactly what the README used to recommend.

    Events-owned feeds are excluded: ``cne run events`` owns them on the
    natural calendar, so they are not missing work for this job.
    """
    from cnequity.domain.datasets import DATASETS, is_dataset_enabled
    from cnequity.orchestrator.registry import STEP_REGISTRY

    covered = {step for wave in getattr(cfg, "daily_waves", []) or [] for step in wave.steps}
    events_owned = {
        step for group in getattr(cfg, "events_groups", {}).values() for step in group.steps
    }
    grouped = {
        step for group in getattr(cfg, "schedule_groups", {}).values() for step in group.steps
    }
    return sorted(
        step
        for step in grouped - covered - events_owned
        if step in STEP_REGISTRY and (step not in DATASETS or is_dataset_enabled(step, cfg))
    )


@run.command("daily")
@config_option
@click.option(
    "--groups",
    "stale_groups",
    default=None,
    help="把 --stale-only 限定在这些 daily 调度组内（逗号或空格分隔）。",
)
@click.option(
    "--group",
    "group_name",
    default=None,
    help=("调度组：core、capital、signals、fundamentals、macro_risk、research、intraday、ticks"),
)
@click.option(
    "--all-groups",
    "all_groups",
    is_flag=True,
    help=(
        "按配置顺序串行跑完全部调度组，某个组失败也继续往下跑。一条命令跑完一天，"
        "给没有仓库里 scripts/daily_pipeline.sh 的人用。数据集全部关闭的组会跳过。"
    ),
)
@click.option(
    "--trade-date",
    "trade_date_str",
    default=None,
    help="as-of 交易日 YYYY-MM-DD（默认今天）。周末 / 节假日补跑时用。",
)
@click.option(
    "--backfill",
    is_flag=True,
    help=(
        "用 backfill 语义跑：跳过交易日门禁和每个 step 自己的增量窗口，"
        "改为抓配置里 backfill scope 指定的窗口。用于补跑调度漏掉的某一天；"
        "日常调度从不带这个参数。"
    ),
)
@click.option(
    "--repair-gaps",
    is_flag=True,
    help="在 daily / stale 跑之前，先修复已验证、且有诚实来源的历史缺口。",
)
@click.option("--quiet", is_flag=True, help="只留 warning 及以上，不打逐步进度。")
@click.option(
    "--stale-only",
    is_flag=True,
    help="只重抓仍然落后于最后交易日的数据集。挂在主 pipeline 几小时之后跑："
    "snapshot 类数据集一旦因源端中断丢掉当天窗口，第二天就补不回来了。",
)
def run_daily(
    config_path: str,
    group_name: str | None,
    all_groups: bool,
    trade_date_str: str | None,
    backfill: bool,
    repair_gaps: bool,
    stale_only: bool,
    quiet: bool,
    stale_groups: str | None = None,
):
    """跑日更采集（Wave DAG 或指定调度组）。"""
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
    attach_log_file(cfg, "run-daily", quiet=quiet)
    engine = JobEngine(cfg)
    td = parse_date_option(trade_date_str, "--trade-date")
    if stale_groups is not None and not stale_only:
        raise click.ClickException("--groups 只能配合 --stale-only；普通的一次 run 请用 --group")
    if all_groups and group_name:
        raise click.ClickException("--all-groups 会跑全部调度组，请去掉 --group。")
    if all_groups and stale_only:
        raise click.ClickException("--stale-only 自己挑要跑的 step，请去掉 --all-groups。")
    if stale_only:
        if group_name:
            raise click.ClickException("--stale-only 自己挑要跑的 step，请去掉 --group。")
        groups = None
        if stale_groups is not None:
            groups = set(stale_groups.replace(",", " ").split())
            if not groups:
                raise click.ClickException("--groups 不能为空")
        _run_stale_only(cfg, engine, td, backfill=backfill, repair_gaps=repair_gaps, groups=groups)
        return
    repairs = []
    if repair_gaps:
        repairs = _auto_repair_gaps(cfg, _last_trading_day(cfg, td or shanghai_today()))
    if all_groups:
        _run_all_groups(cfg, engine, td, backfill=backfill, repairs=repairs)
        return
    try:
        if group_name:
            group = cfg.schedule_groups.get(group_name)
            if not group:
                known = ", ".join(sorted(cfg.schedule_groups))
                raise click.ClickException(
                    f"未知调度组：{group_name}（配置里有：{known}）"
                    if known
                    else f"未知调度组：{group_name} —— 这份配置里根本没有 [job.daily.groups]。"
                    f"`cne config create` 生成的配置带有它们；"
                    f"`cne init --profile demo|sample` 写的配置故意不带，"
                    f"因为 demo 湖不是一个市场。"
                )
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
            )
        else:
            if not (getattr(cfg, "daily_waves", None) or []):
                # An empty plan used to be a success: `planned_steps: []`,
                # `status: success`, exit 0, nothing fetched. That is the
                # answer a scheduler is least able to act on, and the one a
                # new user got by pointing `cne run daily` at the demo config.
                groups = ", ".join(sorted(cfg.schedule_groups))
                remedy = (
                    f"改跑调度组：`cne run daily --group <名字>`（有 {groups}）。"
                    if groups
                    else "`cne config create` 生成的配置两者都有。"
                )
                raise click.ClickException(
                    f"{config_path} 里没有 [[job.daily.waves]]：不带参数的 `cne run daily` "
                    f"跑的就是这张 wave DAG，而这份配置一条都没定义，"
                    f"跑起来会一行数据都没抓却报成功。{remedy}"
                )
            result = engine.run_job("daily", trade_date=td, backfill=backfill)
            # A skipped non-trading day ran nothing at all; listing what it did
            # not cover would put this note in every weekend cron mail.
            uncovered = (
                datasets_outside_the_daily_waves(cfg)
                if result["status"] != "skipped_non_trading_day"
                else []
            )
            if uncovered:
                preview = ", ".join(uncovered[:6])
                suffix = f", … (+{len(uncovered) - 6})" if len(uncovered) > 6 else ""
                click.echo(
                    f"提示：这一趟只跑了核心骨架；还有 {len(uncovered)} 个已启用的数据集属于"
                    f"调度组，这次没有更新：{preview}{suffix}。"
                    f"用 `cne run daily --group <名字>` 跑它们"
                    f"（有 {', '.join(sorted(cfg.schedule_groups))}）。",
                    err=True,
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
    help="[job.events.groups] 里的某一个组（默认按顺序跑全部）。",
)
@click.option(
    "--trade-date",
    "trade_date_str",
    default=None,
    help="as-of 自然日 YYYY-MM-DD（默认今天），含周末与节假日。",
)
@click.option("--quiet", is_flag=True, help="只留 warning 及以上，不打逐步进度。")
def run_events(config_path: str, group_name: str | None, trade_date_str: str | None, quiet: bool):
    """按自然日跑持续事件流（公告、新闻）。

    \b
    这些源在周末和节假日照常发布，所以这个 job 不受交易日门禁约束，并且拿自己的采集锁：
    它既不等晚间批次，也不会因为休市而被跳过。每个组通过自带的 `compact` 发布它 staging 的数据。
    """
    _progress_logging(quiet)
    try:
        cfg = _cfg(config_path)
        cfg._validate_source_limits()
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    attach_log_file(cfg, "run-events", quiet=quiet)
    if not cfg.events_groups:
        raise click.ClickException(
            "这份配置里没有 [job.events.groups] —— 参考 configs/cnequity.example.toml"
        )
    if group_name and group_name not in cfg.events_groups:
        known = ", ".join(sorted(cfg.events_groups))
        raise click.ClickException(f"未知 events 调度组：{group_name}（配置里有：{known}）")

    selected = (
        [(group_name, cfg.events_groups[group_name])]
        if group_name
        else list(cfg.events_groups.items())
    )
    engine = JobEngine(cfg)
    td = parse_date_option(trade_date_str, "--trade-date")
    try:
        result = engine.run_job(
            f"events:{group_name}" if group_name else "events",
            trade_date=td,
            waves=[
                WaveConfig(
                    name=f"events:{name}",
                    parallel=getattr(group, "parallel", True),
                    steps=group.steps,
                )
                for name, group in selected
            ],
        )
    except RunLockError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps({"run_id": result["run_id"], "status": result["status"]}, indent=2))
    exit_code = _run_status_exit_code(result["status"])
    if exit_code:
        raise SystemExit(exit_code)


def _retry_single_run(engine: JobEngine, run_id: str) -> dict:
    """Retry one run, print its result, and return it to the CLI caller."""
    record = engine.manifest.get_run(run_id)
    if record is None:
        raise click.ClickException(f"未知 run_id：{run_id}")
    try:
        if record["job_name"] == "init":
            result = engine.resume_init(run_id=run_id)
        else:
            result = engine.run_job("retry", retry_failed_only=True, run_id=run_id)
    except RunLockError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(result, indent=2, default=str))
    return result


def _failed_daily_group_runs(engine: JobEngine) -> list[dict]:
    """Return the latest failed run of each ``daily:*`` group.

    Reconcile first. A run killed mid-flight is still recorded as `running`
    until somebody closes it, and this selection keeps only rows that say
    `failed` — so the one command an operator reaches for after a crash used to
    answer "No failed daily group run to retry" about the crash they were
    looking at.
    """
    engine._reconcile_orphans()
    latest: dict[str, dict] = {}
    for row in engine.manifest.list_runs():
        record = dict(row)
        job_name = str(record["job_name"])
        if job_name.startswith("daily:"):
            # Manifest order is newest first; an older failure must not be
            # replayed once a newer run for that group has succeeded.
            latest.setdefault(job_name, record)
    return [latest[name] for name in sorted(latest) if latest[name]["status"] == "failed"]


@run.command("retry")
@config_option
@click.option("--run-id", default=None, help="重试指定的一次 run。")
@click.option(
    "--failed-groups",
    is_flag=True,
    help="重试每个 daily 调度组最近一次失败的 run。",
)
def retry(config_path: str, run_id: str | None, failed_groups: bool):
    """重试某一次 run，或每个 daily 调度组最近一次失败的 run。"""
    _progress_logging()
    cfg = _cfg(config_path)
    attach_log_file(cfg, "run-retry")
    engine = JobEngine(cfg)
    if failed_groups:
        if run_id:
            raise click.ClickException("--run-id 和 --failed-groups 只能用一个")
        runs = _failed_daily_group_runs(engine)
        if not runs:
            click.echo("没有需要重试的失败 daily 调度组 run。")
            return
        failed = False
        for record in runs:
            click.echo(f"重试失败的 daily 调度组 run {record['run_id']}（{record['job_name']}）")
            # Heavy groups retain sizeable Polars/Python arenas. A fresh child
            # process per group releases that memory before the next retry.
            proc = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "from cnequity.cli.main import cli; cli.main()",
                    "run",
                    "retry",
                    "--config",
                    config_path,
                    "--run-id",
                    str(record["run_id"]),
                ],
                check=False,
            )
            if proc.returncode != 0:
                failed = True
        if failed:
            raise SystemExit(1)
        return
    if not run_id:
        raise click.ClickException("请给出 --run-id 或 --failed-groups")
    result = _retry_single_run(engine, run_id)
    exit_code = _run_status_exit_code(str(result.get("status", "failed")))
    if exit_code:
        raise SystemExit(exit_code)
