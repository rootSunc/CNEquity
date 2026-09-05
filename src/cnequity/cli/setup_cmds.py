"""First-run commands: `demo`, `init`, `config`, `doctor`.

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
    _cfg,
    _progress_logging,
    _run_status_exit_code,
    config_option,
    parse_date_option,
)
from cnequity.config import load_config, validate_config, write_user_config
from cnequity.domain.market_time import shanghai_today
from cnequity.orchestrator.engine import JobEngine
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


@cli.command("demo")
@click.option(
    "--symbols",
    default=",".join(
        (
            "600519.SH",
            "000001.SZ",
            "000858.SZ",
            "300750.SZ",
            "601318.SH",
        )
    ),
    show_default=True,
    help="Comma-separated symbols to fetch (kept small on purpose).",
)
@click.option(
    "--days",
    default=30,
    show_default=True,
    help="Approx. number of recent trading days of daily_bars.",
)
@click.option(
    "--data-root",
    default="data/cnequity-demo",
    show_default=True,
    help="Separate demo lake root (do not reuse for full-market init).",
)
@click.option(
    "--trade-date",
    "trade_date_str",
    default=None,
    help="As-of date YYYY-MM-DD (default: today / last trading day).",
)
@click.option(
    "--config-out",
    default="configs/cnequity.demo.toml",
    show_default=True,
    help="Where to write the tiny demo config for follow-up `cne query`.",
)
@click.option(
    "--intraday",
    is_flag=True,
    help="Also capture 1-minute bars for the same symbols (up to 5 sessions) "
    "and print a session, so the bar_time convention is visible.",
)
@click.option(
    "--research",
    is_flag=True,
    help="Also derive Sina hfq factors and print a raw-vs-adjusted return (slower; needs Sina).",
)
@click.option(
    "--sample",
    is_flag=True,
    help="Write a deterministic synthetic sample lake without network access.",
)
def demo_cmd(
    symbols: str,
    days: int,
    data_root: str,
    trade_date_str: str | None,
    config_out: str,
    intraday: bool,
    research: bool,
    sample: bool,
):
    """Create a tiny lake so you can see progress and results quickly.

    The default fetches real TDX data; --sample is deterministic and offline.
    Neither is a full-market backfill — use `cne init` for that.
    """
    from cnequity.cli.demo import run_demo, run_sample_demo

    td = parse_date_option(trade_date_str, "--trade-date")
    runner = run_sample_demo if sample else run_demo
    runner(
        symbols=[s.strip() for s in symbols.split(",") if s.strip()],
        days=days,
        data_root=Path(data_root),
        trade_date=td,
        config_out=Path(config_out),
        intraday=intraday,
        research=research,
    )


@cli.command()
@config_option
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
    "--profile",
    type=click.Choice(["full", "quick"]),
    default="quick",
    show_default=True,
    help=f"How much history to fetch. quick = the last {QUICK_PROFILE_YEARS} years; "
    f"full = everything from {BACKFILL_START.isoformat()} (measured ~3x longer). "
    "Both fetch every symbol — deepen later with `cne backfill daily_bars`.",
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
    layout_only: bool,
    trade_date: str | None,
    resume: bool,
    resume_run_id: str | None,
    keep_going: bool,
    profile: str,
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
    """
    _progress_logging(quiet)
    cfg = _cfg(config_path)
    init_data_layout(cfg)
    if layout_only:
        click.echo(f"Initialized layout at {cfg.data_root}")
        return

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
            raise click.ClickException(
                f"Incomplete init run {incomplete['run_id']} exists "
                f"(status={incomplete['status']}). "
                "Use `cne init --resume` or `cne retry --run-id "
                f"{incomplete['run_id']}` — do not start a new full init."
            )

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


@cli.command("config")
@click.argument("action", type=click.Choice(["validate", "init"]))
@config_option
@click.option(
    "--force",
    is_flag=True,
    help="Overwrite an existing config when action=init.",
)
@click.option(
    "--data-root",
    default=None,
    help="Set [data].root when action=init (default: resolve ./data/cnequity to an absolute path).",
)
def config_cmd(action: str, config_path: str, force: bool, data_root: str | None):
    """Validate or bootstrap configuration.

    ``cne config init`` writes the packaged example TOML (no repo checkout needed).
    On macOS it also forces ``orchestrator.workers = 1``.
    ``cne config validate`` checks an existing file.
    """
    if action == "init":
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
