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
        raise click.UsageError(f"{flag} does not apply to --profile {profile}")


@cli.command()
@config_option
@click.option(
    "--profile",
    type=click.Choice(["demo", "sample", "quick", "full"]),
    default="quick",
    show_default=True,
    help="How much to build. demo = a handful of symbols against the real source, "
    "sample = the same shape offline and deterministic — neither is a market. "
    f"quick = every symbol, last {QUICK_PROFILE_YEARS} years; "
    f"full = every symbol from {BACKFILL_START.isoformat()} (measured ~3x longer). "
    "Deepen later with `cne backfill daily_bars`.",
)
@click.option(
    "--symbols",
    default=",".join(("600519.SH", "000001.SZ", "000858.SZ", "300750.SZ", "601318.SH")),
    show_default=True,
    help="demo/sample: comma-separated symbols to fetch (kept small on purpose).",
)
@click.option(
    "--days",
    default=30,
    show_default=True,
    help="demo/sample: approx. number of recent trading days of daily_bars.",
)
@click.option(
    "--data-root",
    default="data/cnequity-demo",
    show_default=True,
    help="demo/sample: separate lake root (do not reuse for a full-market init).",
)
@click.option(
    "--config-out",
    default=DEMO_CONFIG,
    show_default=True,
    help="demo/sample: where to write the tiny config for follow-up `cne query`.",
)
@click.option(
    "--intraday",
    is_flag=True,
    help="demo/sample: also capture 1-minute bars for the same symbols (up to 5 "
    "sessions) and print a session, so the bar_time convention is visible.",
)
@click.option(
    "--research",
    is_flag=True,
    help="demo/sample: also derive Sina hfq factors and print a raw-vs-adjusted "
    "return (slower; needs Sina).",
)
@click.option(
    "--layout-only",
    is_flag=True,
    help="Only create directories, manifest, and DuckDB views (skip init phases).",
)
@click.option(
    "--trade-date",
    default=None,
    help="As-of trade date for init phases (YYYY-MM-DD); default today.",
)
@click.option(
    "--resume",
    is_flag=True,
    help="Resume the latest incomplete init run (retry failed batches + missing phases).",
)
@click.option(
    "--run-id",
    "resume_run_id",
    default=None,
    help="Resume a specific init run_id (implies --resume).",
)
@click.option(
    "--keep-going",
    is_flag=True,
    help="Continue init phases after a phase failure instead of stopping.",
)
@click.option(
    "--since",
    "since_str",
    default=None,
    help="Explicit history start (YYYY-MM-DD); overrides --profile.",
)
@click.option("--quiet", is_flag=True, help="Only warnings and errors; no per-batch progress.")
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
    """Initialize the data lake and run the configured init phases.

    Defaults to `--profile quick`: the last few years, every symbol. That is
    SHALLOWER, never NARROWER. Dropping symbols instead would build the
    survivorship bias this lake exists to avoid straight into it, and
    `coverage_start` records a shallow lake honestly where a missing name would
    look like a name that never traded.

    Why quick is the default: measured per 10 symbols on one connection,
    3 years costs ~4.8s against ~15.1s for everything from 2001 — roughly an
    hour versus several for a full market. Going shallower still buys very
    little (1 year measured ~3.9s, because the per-symbol round trip dominates
    once the window is short) while costing the multi-year windows that most
    factor work needs. So: a usable lake on the first run, deepened on demand.

    Deepen later without re-running init:

      cne backfill daily_bars --start 2016-01-01 --end <your coverage_start>

    Or take everything up front with `--profile full`.

    `--profile demo` builds a handful of symbols into a separate `--data-root`
    so you can watch progress and query a result in a minute; `--profile sample`
    does the same offline and deterministically. Neither is a market, and
    neither touches `--config` — they write their own at `--config-out`.
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
        click.echo(f"Initialized layout at {cfg.data_root}")
        return

    # After the layout-only exit: a command that finishes in a second has
    # nothing to leave behind but an empty file.
    attach_log_file(cfg, "init", quiet=quiet)

    td = parse_date_option(trade_date, "--trade-date") or shanghai_today()

    history_start = _init_history_start(profile, since_str, td)
    if history_start is not None:
        cfg._backfill_start = history_start
        click.echo(
            f"History window: {history_start.isoformat()} .. {td.isoformat()} "
            f"(full universe, {profile if not since_str else 'custom'} depth). "
            "Deepen later with `cne backfill daily_bars --start <earlier>`."
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
                    f"Another init is running now (run {incomplete['run_id']}). "
                    "Wait for it, or stop it before starting another."
                )
            click.echo(
                f"Found an unfinished init run {incomplete['run_id']} whose process is gone "
                "— resuming it instead of starting over (completed batches are kept).",
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
    help="Overwrite an existing config when action=create.",
)
@click.option(
    "--data-root",
    default=None,
    help="Set [data].root when action=create (default: resolve ./data/cnequity to an absolute path).",
)
def config_cmd(action: str, config_path: str, force: bool, data_root: str | None):
    """Validate, bootstrap, or diff configuration.

    ``cne config create`` writes the packaged example TOML (no repo checkout
    needed) — it was ``cne config init``, one word away from ``cne init``, which
    builds a lake and is the far more expensive of the two to run by mistake.
    On macOS it also forces ``orchestrator.workers = 1``.
    ``cne config validate`` checks an existing file.
    ``cne config diff`` reports what the packaged example has that this file does
    not — most importantly steps added to a schedule group by a later release,
    which a config written once and never updated will never run.
    """
    # A free-form argument bypasses `token_normalize_func`, which is what makes
    # every other name in this CLI case-insensitive; normalise it here so
    # `cne config CREATE` behaves like `cne run DAILY`.
    action = action.lower()
    moved = CONFIG_ACTIONS_MOVED.get(action)
    if moved:
        raise click.ClickException(f"`cne config {action}` has moved. Use `{moved}` instead.")
    if action not in CONFIG_ACTIONS:
        raise click.BadParameter(
            f"{action!r} is not one of {', '.join(repr(a) for a in CONFIG_ACTIONS)}",
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
        click.echo(f"Wrote {out}")
        click.echo("data.root is absolute; edit if needed, then: cne config validate && cne init")
        return

    cfg = _cfg(config_path)
    errors = validate_config(cfg)
    if errors:
        for e in errors:
            click.echo(f"ERROR: {e}", err=True)
        raise SystemExit(1)
    click.echo("Configuration OK")


@cli.command()
@config_option
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output.")
def doctor(config_path: str, as_json: bool):
    """Check environment, optional dependencies, and config for silent breakage.

    Runs without a config (fresh install) and without network. Exits non-zero
    when something will actually lose data — notably a source that is enabled in
    config but has no package behind it, which no other command surfaces.
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
