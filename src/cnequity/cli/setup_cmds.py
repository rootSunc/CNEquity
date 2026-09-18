"""First-run commands: `init`, `config`, `doctor`.

Everything a fresh clone touches before it has a lake, in the order the
quickstart walks through them.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import click

from cnequity.cli._root import cli
from cnequity.cli._shared import (
    DEMO_CONFIG,
    _cfg,
    _progress_logging,
    _run_status_exit_code,
    attach_log_file,
    config_option,
    parse_date_option,
    resolve_config_path,
)
from cnequity.config import load_config, validate_config, write_user_config
from cnequity.domain.market_time import shanghai_today
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.run_lock import INIT_JOB_LOCK, is_run_locked
from cnequity.steps.common import BACKFILL_START
from cnequity.storage.layout import init_data_layout

# `cne init --profile quick`. Three years is the shortest window that still
# spans a full A-share cycle plus two annual report seasons, so the lake it
# builds can answer a real question rather than only prove the pipeline runs.
QUICK_PROFILE_YEARS = 3


def _init_history_start(profile: str, since_str: str | None, trade_date: date) -> date | None:
    """History floor for an init run, or None to use each step's own default."""
    if since_str:
        return parse_date_option(since_str, "--since")
    if profile == "quick":
        # Calendar arithmetic, not 365*N: a leap year in the window would
        # otherwise move the floor by a day for no reason anyone could explain.
        # Feb 29 has no counterpart three years back, so it lands on Mar 1.
        year = trade_date.year - QUICK_PROFILE_YEARS
        try:
            return trade_date.replace(year=year)
        except ValueError:
            return date(year, 3, 1)
    return None


# `cne init --profile demo|sample` used to be `cne demo`. It is the same
# decision as `quick` vs `full` — how much of the market to build — and asking a
# first-time user to choose between two commands before they have either was one
# fork too many. The option sets stay disjoint, so each side refuses the other's
# flags rather than accepting and ignoring them.
DEMO_PROFILES = ("demo", "sample")
_DEMO_ONLY = ("symbols", "days", "data_root", "config_out", "intraday", "research")
_LAKE_ONLY = ("config_path", "layout_only", "resume", "resume_run_id", "keep_going", "since_str")
_FLAG_NAMES = {
    "config_path": "--config",
    "data_root": "--data-root",
    "config_out": "--config-out",
    "resume_run_id": "--run-id",
    "since_str": "--since",
}


def _reject_foreign_options(profile: str, names: tuple[str, ...]) -> None:
    """Fail on an option the chosen --profile has no meaning for."""
    ctx = click.get_current_context(silent=True)
    if ctx is None:
        return
    for name in names:
        if ctx.get_parameter_source(name) is not click.core.ParameterSource.COMMANDLINE:
            continue
        flag = _FLAG_NAMES.get(name, "--" + name.replace("_", "-"))
        raise click.UsageError(f"{flag} 对 --profile {profile} 不适用")


@cli.command()
@config_option
@click.option(
    "--profile",
    type=click.Choice(["demo", "sample", "quick", "full"]),
    default="quick",
    show_default=True,
    help="建多大。demo = 用真实数据源抓几只票，sample = 同样的形状但离线且确定 —— "
    "两者都不是一个市场。"
    f"quick = 全市场标的、最近 {QUICK_PROFILE_YEARS} 年；"
    f"full = 全市场标的、从 {BACKFILL_START.isoformat()} 起（实测约 3 倍时间）。"
    "以后可以用 `cne backfill daily_bars` 补深。",
)
@click.option(
    "--symbols",
    default=",".join(("600519.SH", "000001.SZ", "000858.SZ", "300750.SZ", "601318.SH")),
    show_default=True,
    help="demo/sample：要抓的标的，逗号分隔（有意保持很少）。",
)
@click.option(
    "--days",
    default=30,
    show_default=True,
    help="demo/sample：daily_bars 大致抓最近多少个交易日。",
)
@click.option(
    "--data-root",
    default="data/cnequity-demo",
    show_default=True,
    help="demo/sample：独立的湖根目录（不要拿去跑全市场 init）。",
)
@click.option(
    "--config-out",
    default=DEMO_CONFIG,
    show_default=True,
    help="demo/sample：把那份小配置写到哪，供后续 `cne query` 使用。",
)
@click.option(
    "--intraday",
    is_flag=True,
    help="demo/sample：同一批标的额外抓 1 分钟线（最多 5 个交易日）并打印一个交易日，"
    "让 bar_time 的口径看得见。",
)
@click.option(
    "--research",
    is_flag=True,
    help="demo/sample：额外用 Sina 派生 hfq 复权因子，并打印未复权 / 复权收益对照"
    "（较慢；需要访问 Sina）。",
)
@click.option(
    "--layout-only",
    is_flag=True,
    help="只建目录、manifest 和 DuckDB 视图，跳过 init 各阶段。",
)
@click.option(
    "--trade-date",
    default=None,
    help="init 各阶段的 as-of 交易日（YYYY-MM-DD）；默认今天。",
)
@click.option(
    "--resume",
    is_flag=True,
    help="续跑最近一次没跑完的 init run（重试失败批次 + 补缺失阶段）。",
)
@click.option(
    "--run-id",
    "resume_run_id",
    default=None,
    help="续跑指定的 init run_id（隐含 --resume）。",
)
@click.option(
    "--keep-going",
    is_flag=True,
    help="某个阶段失败后继续往下跑，而不是停下来。",
)
@click.option(
    "--since",
    "since_str",
    default=None,
    help="显式指定历史起点（YYYY-MM-DD）；覆盖 --profile。",
)
@click.option("--quiet", is_flag=True, help="只留 warning 及以上，不打逐批进度。")
def init(
    config_path: str,
    profile: str,
    symbols: str,
    days: int,
    data_root: str,
    config_out: str,
    intraday: bool,
    research: bool,
    layout_only: bool,
    trade_date: str | None,
    resume: bool,
    resume_run_id: str | None,
    keep_going: bool,
    since_str: str | None,
    quiet: bool,
):
    """初始化数据湖，并按配置跑完 init 各阶段。

    \b
    默认 `--profile quick`：最近几年、全市场标的。也就是说**浅**，但绝不**窄**。
    砍标的会把这个湖存在的意义 —— 避免幸存者偏差 —— 直接砍掉，而浅是诚实的：
    `coverage_start` 会如实记下湖有多深，少一个标的却会看起来像这只票从未交易过。

    \b
    为什么 quick 是默认：单连接每 10 只标的实测，3 年约 4.8 秒，而 2001 年至今约 15.1 秒 ——
    全市场就是一小时和几小时的差别。再浅几乎买不到什么（1 年实测约 3.9 秒，窗口一短，
    每个标的的往返开销就占主导），却会丢掉多数因子研究要用的多年窗口。
    所以：第一次就跑出一个能用的湖，需要多深再补多深。

    \b
    不用重跑 init 也能补深：

      cne backfill daily_bars --start 2016-01-01 --end <你的 coverage_start>

    或者一开始就全量：`--profile full`。

    \b
    `--profile demo` 把几只标的建到独立的 `--data-root` 里，一分钟内就能看到进度和查询结果；
    `--profile sample` 做同样的事，但离线且确定。两者都不是一个市场，也都不碰 `--config` ——
    它们把自己的配置写到 `--config-out`。
    """
    if profile in DEMO_PROFILES:
        _reject_foreign_options(profile, _LAKE_ONLY)
        from cnequity.cli.demo import run_demo, run_sample_demo

        runner = run_sample_demo if profile == "sample" else run_demo
        runner(
            symbols=[s.strip() for s in symbols.split(",") if s.strip()],
            days=days,
            data_root=Path(data_root),
            trade_date=parse_date_option(trade_date, "--trade-date"),
            config_out=Path(config_out),
            intraday=intraday,
            research=research,
        )
        return

    _reject_foreign_options(profile, _DEMO_ONLY)
    _progress_logging(quiet)
    cfg = _cfg(config_path)
    init_data_layout(cfg)
    if layout_only:
        click.echo(f"已在 {cfg.data_root} 建好目录结构")
        return

    # After the layout-only exit: a command that finishes in a second has
    # nothing to leave behind but an empty file.
    attach_log_file(cfg, "init", quiet=quiet)

    td = parse_date_option(trade_date, "--trade-date") or shanghai_today()

    history_start = _init_history_start(profile, since_str, td)
    if history_start is not None:
        cfg._backfill_start = history_start
        click.echo(
            f"历史窗口：{history_start.isoformat()} .. {td.isoformat()}"
            f"（全市场标的，{profile if not since_str else 'custom'} 深度）。"
            "以后可以用 `cne backfill daily_bars --start <更早的日期>` 补深。"
        )

    engine = JobEngine(cfg)

    if not resume and not resume_run_id:
        incomplete = engine.manifest.latest_incomplete_init_run()
        if incomplete is not None:
            # An init that looked hung, got killed, and was started again used
            # to be refused here — the one command the operator had left,
            # answering "no" to the one thing they were trying to do. The
            # refusal is right for a *live* peer and wrong for a dead one, and
            # the init lock tells them apart: a killed process releases it as
            # the kernel reaps it, while a running one holds it throughout.
            if is_run_locked(cfg.meta_root, INIT_JOB_LOCK):
                raise click.ClickException(
                    f"已经有一个 init 在跑（run {incomplete['run_id']}）。"
                    "等它跑完，或者先停掉它再开新的。"
                )
            click.echo(
                f"发现一个没跑完的 init run {incomplete['run_id']}，它的进程已经不在了 "
                "—— 直接续跑而不是从头再来（已完成的批次会保留）。",
                err=True,
            )
            resume = True
            resume_run_id = str(incomplete["run_id"])

    result = engine.run_init_phases(
        trade_date=td,
        resume=resume or bool(resume_run_id),
        resume_run_id=resume_run_id,
        keep_going=keep_going,
    )
    click.echo(json.dumps(result, indent=2, default=str))
    exit_code = _run_status_exit_code(str(result.get("status", "failed")))
    if exit_code:
        raise SystemExit(exit_code)


#: What `cne config` does, and the spellings that used to mean one of them.
#: `cnequity.cli._root.MOVED` answers a moved *command*; an argument is not a
#: command, so it needs its own map — and this is the one people hit first.
CONFIG_ACTIONS: tuple[str, ...] = ("validate", "create", "diff")
CONFIG_ACTIONS_MOVED: dict[str, str] = {"init": "cne config create"}


@cli.command("config")
# Free-form rather than a Choice, so the body can answer a moved spelling.
# Click's own rejection — "'init' is not one of 'validate', 'create', 'diff'" —
# names everything except what the caller needs, and `cne config init` is the
# first command a new lake ever runs: the people most likely to type it are the
# ones with the least context to decode that.
@click.argument("action")
@config_option
@click.option(
    "--force",
    is_flag=True,
    help="action=create 时覆盖已存在的配置文件。",
)
@click.option(
    "--data-root",
    default=None,
    help="action=create 时设置 [data].root（默认把 ./data/cnequity 解析成绝对路径）。",
)
def config_cmd(action: str, config_path: str, force: bool, data_root: str | None):
    """校验、生成或对比配置。

    \b
    `cne config create` 写出随包携带的示例 TOML（不需要 clone 仓库）—— 它以前叫
    `cne config init`，和会建整个湖的 `cne init` 只差一个词，而后者误跑的代价大得多。
    在 macOS 上它还会强制 `orchestrator.workers = 1`。
    `cne config validate` 校验一份已有的配置。
    `cne config diff` 报告示例配置里有、而这份配置没有的东西 —— 最要紧的是新版本往调度组里
    加的 step：一份写完就再没更新过的配置永远不会跑到它们。
    """
    # A free-form argument bypasses `token_normalize_func`, which is what makes
    # every other name in this CLI case-insensitive; normalise it here so
    # `cne config CREATE` behaves like `cne run DAILY`.
    action = action.lower()
    moved = CONFIG_ACTIONS_MOVED.get(action)
    if moved:
        raise click.ClickException(f"`cne config {action}` 已改名，请改用 `{moved}`。")
    if action not in CONFIG_ACTIONS:
        raise click.BadParameter(
            f"{action!r} 不是 {', '.join(repr(a) for a in CONFIG_ACTIONS)} 之一",
            param_hint="'{" + "|".join(CONFIG_ACTIONS) + "}'",
        )
    if action == "diff":
        from cnequity.config.drift import config_drift, render_drift

        path = resolve_config_path(config_path)
        drift = config_drift(path)
        for line in render_drift(drift, path):
            click.echo(line)
        # Unscheduled steps are the case that silently loses data, so they are
        # the only drift that fails: missing keys merely take their defaults.
        if drift.unscheduled_steps:
            raise SystemExit(1)
        return

    if action == "create":
        out = Path(config_path)
        try:
            write_user_config(out, data_root=data_root, force=force)
        except FileExistsError as exc:
            raise click.ClickException(str(exc)) from exc
        click.echo(f"已写入 {out}")
        click.echo("data.root 是绝对路径，按需修改后执行：cne config validate && cne init")
        return

    cfg = _cfg(config_path)
    errors = validate_config(cfg)
    if errors:
        for e in errors:
            click.echo(f"ERROR: {e}", err=True)
        raise SystemExit(1)
    click.echo("配置检查通过")


@cli.command()
@config_option
@click.option("--json", "as_json", is_flag=True, help="输出机器可读的 JSON。")
def doctor(config_path: str, as_json: bool):
    """体检环境、可选依赖和配置，找出会悄悄坏掉的地方。

    \b
    不需要配置（刚装完就能跑），也不需要网络。只有确实会丢数据时才非零退出 ——
    最典型的是配置里启用了某个源、但它背后的包没装，这件事别的命令都不会说。
    """
    from cnequity.diagnostics.render import render_text, to_dict
    from cnequity.diagnostics.report import build_report

    cfg = None
    resolved: Path | None = None
    path = Path(config_path)
    if path.exists():
        try:
            cfg = load_config(path)
            resolved = path
        except Exception as exc:  # config errors must not hide the dependency report
            click.echo(f"WARN: 配置解析失败 {path}: {exc}", err=True)

    report = build_report(config=cfg, config_path=resolved)

    if as_json:
        click.echo(json.dumps(to_dict(report), indent=2, default=str))
    else:
        for line in render_text(report):
            click.echo(line)

    if not report.ok:
        raise SystemExit(1)
