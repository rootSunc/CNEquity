from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from pathlib import Path

import click
import polars as pl

import cnequity.steps  # noqa: F401 — register steps
from cnequity.config import WaveConfig, load_config, validate_config, write_user_config
from cnequity.derive.adj_factors import compute_adj_factors
from cnequity.domain.datasets import (
    fetch_semantics,
    get_dataset,
)
from cnequity.domain.market_time import is_session_final, shanghai_today
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.manifest import Manifest
from cnequity.orchestrator.run_lock import RunLockError
from cnequity.quality.audit import run_audit
from cnequity.query.on_demand import OnDemandService
from cnequity.query.parquet_scan import scan_parquet_files
from cnequity.query.views import ensure_duckdb_views
from cnequity.steps.common import BACKFILL_START
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.layout import init_data_layout
from cnequity.storage.source_snapshots import (
    DEFAULT_SNAPSHOT_RETENTION_DAYS,
    clean_source_snapshots,
)
from cnequity.storage.staging_cleanup import clean_staging

USER_CONFIG = "configs/cnequity.toml"
EXAMPLE_CONFIG = "configs/cnequity.example.toml"
DEFAULT_CONFIG = USER_CONFIG

# `cne init --profile quick`. Three years is the shortest window that still
# spans a full A-share cycle plus two annual report seasons, so the lake it
# builds can answer a real question rather than only prove the pipeline runs.
QUICK_PROFILE_YEARS = 3


def _init_history_start(profile: str, since_str: str | None, trade_date: date) -> date | None:
    """History floor for an init run, or None to use each step's own default."""
    if since_str:
        return date.fromisoformat(since_str)
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


def _progress_logging(quiet: bool = False) -> None:
    """Send the pipeline's own INFO records to the terminal.

    Long fetches were silent until they finished: `cne init` runs for hours and
    printed nothing until the closing JSON, which is indistinguishable from
    hung — and a process that looks hung gets killed. The steps and the worker
    pool already log their progress; nothing was listening.

    Third-party loggers stay at WARNING. httpx logs a line per request, which
    on a full-market sweep is hundreds of thousands of lines and buries exactly
    the progress this exists to surface.
    """
    import logging

    logging.basicConfig(
        level=logging.WARNING if quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    for noisy in ("httpx", "httpcore", "urllib3", "curl_cffi"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def resolve_config_path(config_path: str) -> Path:
    path = Path(config_path)
    if config_path == USER_CONFIG and not path.exists():
        raise click.ClickException(
            f"Config not found: {USER_CONFIG}. "
            "Run `cne config init` to write one from the packaged example "
            f"(or copy {EXAMPLE_CONFIG} if you have the repo checkout)."
        )
    if not path.exists():
        raise click.ClickException(f"Config not found: {path}")
    return path


def _cfg(config: str):
    return load_config(resolve_config_path(config))


@click.group()
@click.version_option(package_name="cnequity")
def cli():
    """cnequity — A-share data ingestion CLI."""


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
def demo_cmd(
    symbols: str,
    days: int,
    data_root: str,
    trade_date_str: str | None,
    config_out: str,
    intraday: bool,
    research: bool,
):
    """Fetch a tiny real-source lake so you can see progress and results quickly.

    Not a full-market backfill — use `cne init` for that. Requires network access
    to TDX hosts (mainland egress is more reliable overseas).
    """
    from cnequity.cli.demo import run_demo

    td = date.fromisoformat(trade_date_str) if trade_date_str else None
    run_demo(
        symbols=[s.strip() for s in symbols.split(",") if s.strip()],
        days=days,
        data_root=Path(data_root),
        trade_date=td,
        config_out=Path(config_out),
        intraday=intraday,
        research=research,
    )


@cli.command()
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
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

    td = date.fromisoformat(trade_date) if trade_date else shanghai_today()

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
    if result.get("status") != "success":
        raise SystemExit(1)


@cli.command("config")
@click.argument("action", type=click.Choice(["validate", "init"]))
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
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
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
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


@cli.group()
def run():
    """Run scheduled jobs."""


def stale_fetch_steps(cfg, anchor: date) -> list[str]:
    """Registered fetch steps whose dataset is still behind *anchor*.

    Freshness is judged exactly as ``cne status --datasets`` judges it, so the
    two cannot disagree about what is behind.

    Derived datasets are excluded: they are recomputed by ``cne derive`` from
    curated inputs, and re-fetching is not what they need. Datasets with no
    registered step are excluded because there is nothing to run.
    """
    # Steps are registered by the module-level `import cnequity.steps`.
    from cnequity.domain.datasets import DATASETS, is_dataset_enabled, is_stale
    from cnequity.orchestrator.registry import STEP_REGISTRY
    from cnequity.query.reader import list_datasets

    out: list[str] = []
    for row in list_datasets(config=cfg).iter_rows(named=True):
        name = row["dataset"]
        spec = DATASETS[name]
        if spec.layer == "derived" or name not in STEP_REGISTRY:
            continue
        if not is_dataset_enabled(name, cfg):
            continue
        if not row["has_data"] or not row["watermarked"]:
            continue
        mark = row["watermark"] or row["coverage_end"]
        if is_stale(name, mark, anchor):
            out.append(name)
    return out


def _run_stale_only(cfg, engine, trade_date: date | None, *, backfill: bool) -> None:
    """Second attempt, same day, for whatever the first attempt did not land.

    The gap this closes: a ``snapshot`` dataset fetches only the run day, so a
    source outage during the one scheduled window loses that day permanently —
    ``valuation_metrics`` lost 2026-07-30 and 07-31 to a push2 clist outage and
    no later run could have recovered them. Per-host retries already exist and
    were exhausted; what was missing was a second window.
    """
    anchor = _last_trading_day(cfg, trade_date or shanghai_today())
    steps = stale_fetch_steps(cfg, anchor)
    if not steps:
        click.echo(f"nothing stale as of {anchor.isoformat()}")
        return
    click.echo(f"stale as of {anchor.isoformat()}: {', '.join(steps)}", err=True)
    try:
        result = engine.run_job(
            "daily:stale",
            trade_date=trade_date,
            waves=[WaveConfig(name="stale", parallel=False, steps=[*steps, "compact"])],
            backfill=backfill,
        )
    except RunLockError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps({"run_id": result["run_id"], "status": result["status"]}, indent=2))
    if result["status"] not in ("success", "skipped_non_trading_day"):
        raise SystemExit(1)


@run.command("daily")
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
@click.option(
    "--group",
    "group_name",
    default=None,
    help=("Schedule group: core, capital, signals, fundamentals, macro_risk, research, intraday"),
)
@click.option(
    "--trade-date",
    "trade_date_str",
    default=None,
    help="As-of trade date YYYY-MM-DD (default: today). Use to catch up on weekends/holidays.",
)
@click.option("--backfill", is_flag=True)
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
    stale_only: bool,
    quiet: bool,
):
    """Run daily ingestion job (Wave DAG or schedule group)."""
    _progress_logging(quiet)
    cfg = _cfg(config_path)
    engine = JobEngine(cfg)
    td = date.fromisoformat(trade_date_str) if trade_date_str else None
    if stale_only:
        if group_name:
            raise click.ClickException("--stale-only picks its own steps; drop --group.")
        _run_stale_only(cfg, engine, td, backfill=backfill)
        return
    try:
        if group_name:
            group = cfg.schedule_groups.get(group_name)
            if not group:
                raise click.ClickException(f"Unknown group: {group_name}")
            result = engine.run_job(
                f"daily:{group_name}",
                trade_date=td,
                waves=[WaveConfig(name=f"group:{group_name}", parallel=False, steps=group.steps)],
                backfill=backfill,
            )
        else:
            result = engine.run_job("daily", trade_date=td, backfill=backfill)
    except RunLockError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps({"run_id": result["run_id"], "status": result["status"]}, indent=2))
    # Exit non-zero on failure so schedulers (launchd/cron) and the daily
    # pipeline can detect it; a non-trading-day skip is a success (exit 0).
    if result["status"] not in ("success", "skipped_non_trading_day"):
        raise SystemExit(1)


_CATCHUP_EXTRA_DEFAULT = (
    "capital",
    "signals",
    "fundamentals",
    "macro_risk",
    "research",
)


def _dataset_watermark(cfg, dataset: str):
    """Latest success date for a gate dataset (StateStore or hive max for adj)."""
    from cnequity.query.parquet_scan import list_hive_partition_dates
    from cnequity.storage.state import StateStore

    state = StateStore(cfg.meta_root)
    wm = state.get_date(dataset)
    if wm is not None:
        return wm
    if dataset == "adj_factors":
        parts = list_hive_partition_dates(cfg.derived_root / "adj_factors", "trade_date")
        return parts[-1] if parts else None
    return None


def _gate_fresh_for_catchup(cfg, trade_date: date, *, core_only: bool) -> dict[str, bool]:
    """Which gate pieces are already at/above ``trade_date``."""

    def _ok(name: str) -> bool:
        wm = _dataset_watermark(cfg, name)
        return wm is not None and wm >= trade_date

    bars_ok = _ok("daily_bars")
    adj_ok = _ok("adj_factors")
    breadth_ok = True if core_only else _ok("market_breadth")
    return {
        "daily_bars": bars_ok,
        "adj_factors": adj_ok,
        "market_breadth": breadth_ok,
        "core": bars_ok and adj_ok,
        "all": bars_ok and adj_ok and breadth_ok,
    }


@run.command("catchup")
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
@click.option(
    "--trade-date",
    "trade_date_str",
    default=None,
    help="Target trading day YYYY-MM-DD (default: latest trading day on/before today).",
)
@click.option(
    "--core-only",
    is_flag=True,
    help="Skip market_breadth (gate bars/adj only).",
)
@click.option(
    "--extra-group",
    "extra_groups",
    multiple=True,
    help=(
        "Also run this schedule group after the gate catchup (repeatable). "
        "Best-effort: failures are reported but do not fail the command. "
        "EM-heavy groups usually need a mainland egress."
    ),
)
@click.option(
    "--all-groups",
    is_flag=True,
    help=f"After gate catchup, best-effort run: {' '.join(_CATCHUP_EXTRA_DEFAULT)}.",
)
def run_catchup(
    config_path: str,
    trade_date_str: str | None,
    core_only: bool,
    extra_groups: tuple[str, ...],
    all_groups: bool,
):
    """Catch up core gate datasets after a missed/weekend skip.

    Runs ``daily:core`` for the target date, then ``market_breadth`` + ``compact``
    (unless ``--core-only``). Does **not** pass ``--backfill`` (full CA scan is
    fragile overseas). Optional ``--extra-group`` / ``--all-groups`` continue past
    EastMoney failures so a mainland box can refresh capital/research in one shot.
    """
    from cnequity.steps.common import is_trading_day, list_trading_dates

    cfg = _cfg(config_path)
    if trade_date_str:
        td = date.fromisoformat(trade_date_str)
        if not is_trading_day(cfg, td):
            raise click.ClickException(f"{td.isoformat()} is not a trading day")
    else:
        # Walk back up to ~3 weeks for long holidays.
        end = shanghai_today()
        start = date.fromordinal(end.toordinal() - 21)
        days = list_trading_dates(cfg, start, end)
        if not days:
            raise click.ClickException("no trading day found in the last 21 calendar days")
        td = days[-1]

    extras: list[str] = []
    if all_groups:
        extras.extend(_CATCHUP_EXTRA_DEFAULT)
    extras.extend(extra_groups)
    # Preserve order, drop dupes / core (already handled).
    seen: set[str] = set()
    extras_ordered: list[str] = []
    for name in extras:
        if name == "core" or name in seen:
            continue
        seen.add(name)
        extras_ordered.append(name)

    bars_wm = _dataset_watermark(cfg, "daily_bars")
    adj_wm = _dataset_watermark(cfg, "adj_factors")
    breadth_wm = _dataset_watermark(cfg, "market_breadth")
    fresh = _gate_fresh_for_catchup(cfg, td, core_only=core_only)
    click.echo(
        json.dumps(
            {
                "trade_date": td.isoformat(),
                "daily_bars_watermark": bars_wm.isoformat() if bars_wm else None,
                "adj_factors_watermark": adj_wm.isoformat() if adj_wm else None,
                "market_breadth_watermark": breadth_wm.isoformat() if breadth_wm else None,
                "core_only": core_only,
                "extra_groups": extras_ordered,
                "already_fresh": fresh,
            },
            indent=2,
        )
    )

    engine = JobEngine(cfg)
    group = cfg.schedule_groups.get("core")
    if not group:
        raise click.ClickException("schedule group 'core' missing from config")

    results: dict[str, dict[str, str]] = {}
    try:
        if fresh["core"]:
            results["core"] = {"run_id": "", "status": "skipped_already_fresh"}
        else:
            core = engine.run_job(
                "daily:core",
                trade_date=td,
                waves=[WaveConfig(name="group:core", parallel=False, steps=group.steps)],
                backfill=False,
            )
            results["core"] = {"run_id": core["run_id"], "status": core["status"]}
            if core["status"] not in ("success", "skipped_non_trading_day"):
                click.echo(json.dumps(results, indent=2))
                raise SystemExit(1)

        if not core_only:
            if fresh["market_breadth"]:
                results["market_breadth"] = {
                    "run_id": "",
                    "status": "skipped_already_fresh",
                }
            else:
                breadth = engine.run_job(
                    "daily:market_breadth",
                    trade_date=td,
                    waves=[
                        WaveConfig(
                            name="breadth",
                            parallel=False,
                            steps=["market_breadth", "compact"],
                        )
                    ],
                    backfill=False,
                )
                results["market_breadth"] = {
                    "run_id": breadth["run_id"],
                    "status": breadth["status"],
                }
                if breadth["status"] not in ("success", "skipped_non_trading_day"):
                    click.echo(json.dumps(results, indent=2))
                    raise SystemExit(1)

        for name in extras_ordered:
            g = cfg.schedule_groups.get(name)
            if not g:
                results[name] = {"run_id": "", "status": "unknown_group"}
                continue
            out = engine.run_job(
                f"daily:{name}",
                trade_date=td,
                waves=[WaveConfig(name=f"group:{name}", parallel=False, steps=g.steps)],
                backfill=False,
            )
            results[name] = {"run_id": out["run_id"], "status": out["status"]}
    except RunLockError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(json.dumps(results, indent=2))
    # Gate path already validated; extra-group failures are advisory.
    if results["core"]["status"] not in (
        "success",
        "skipped_non_trading_day",
        "skipped_already_fresh",
    ):
        raise SystemExit(1)
    mb = results.get("market_breadth")
    if mb and mb["status"] not in (
        "success",
        "skipped_non_trading_day",
        "skipped_already_fresh",
    ):
        raise SystemExit(1)


@cli.command()
@click.argument("dataset")
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
@click.option(
    "--retry-failed",
    is_flag=True,
    help="Resume sector_bars backfill (skip boards already written to checkpoint).",
)
@click.option(
    "--force",
    is_flag=True,
    help="Clear sector_bars backfill checkpoint and re-fetch all boards.",
)
@click.option(
    "--start",
    "start_str",
    default=None,
    help="Range start (YYYY-MM-DD) for date-walking backfills (margin_trading, "
    "financial_statement_items period walk, minute_bars) and to narrow the "
    "sector_bars kline window (default: 400 days back). Horizon-limited "
    "datasets refuse a start older than what their source still serves.",
)
@click.option(
    "--end",
    "end_str",
    default=None,
    help="Range end (YYYY-MM-DD) for date-walking backfills (margin_trading, "
    "financial_statement_items period walk) and sector_bars (default: today).",
)
@click.option(
    "--symbols",
    "symbols_str",
    default=None,
    help="Comma-separated symbols for a scoped intraday or trading_status "
    "backfill. The trading_status checkpoint and coverage evidence retain the "
    "exact scope; other datasets use their configured watchlist block.",
)
@click.option(
    "--workers",
    default=1,
    show_default=True,
    help="Concurrent fetch workers for date-walking backfills; each worker is "
    "throttled to 1 req/s (aggregate up to N req/s, bypassing the source limiter).",
)
def backfill(
    dataset: str,
    config_path: str,
    retry_failed: bool,
    force: bool,
    start_str: str | None,
    end_str: str | None,
    symbols_str: str | None,
    workers: int,
):
    """Backfill a dataset."""
    _progress_logging()
    if fetch_semantics(dataset) == "snapshot" and not get_dataset(dataset).backfill_source:
        raise click.ClickException(
            f"{dataset}: backfill not supported — fetch semantics are snapshot "
            "(live page stamped with trade_date; historical values unavailable). "
            "Run daily ingestion on trading days instead."
        )
    cfg = _cfg(config_path)
    if dataset == "sector_bars":
        if retry_failed and force:
            raise click.ClickException("Use either --retry-failed or --force, not both.")
        cfg._sector_bars_force = force
    start_d = date.fromisoformat(start_str) if start_str else None
    end_d = date.fromisoformat(end_str) if end_str else None
    _guard_history_horizon(dataset, start_d)
    if symbols_str:
        symbols = [s.strip().upper() for s in symbols_str.split(",") if s.strip()]
        if dataset == "trading_status":
            cfg._backfill_symbols = symbols
        else:
            _override_scope(cfg, dataset, symbols)
        click.echo(f"[{dataset}] scope overridden for this run: {len(symbols)} symbol(s)", err=True)
    if start_d:
        cfg._backfill_start = start_d
    if end_d:
        cfg._backfill_end = end_d
    cfg._backfill_workers = workers

    spec = get_dataset(dataset)
    # Tip-paged sources (intraday) must chunk by symbol, not by date: the wire
    # always walks tip → start, so date slices re-fetch every newer page.
    if spec.backfill_chunk_symbols and start_d and end_d:
        result = _backfill_symbol_chunked(cfg, dataset, start_d, end_d, spec.backfill_chunk_symbols)
    elif spec.backfill_chunk_days and start_d and end_d:
        result = _backfill_chunked(cfg, dataset, start_d, end_d, spec.backfill_chunk_days)
    else:
        result = _backfill_once(cfg, dataset)
    click.echo(json.dumps(result, indent=2, default=str))
    if result["status"] != "success":
        raise SystemExit(1)


# Datasets whose universe comes from a config block rather than from
# `instruments`, and the block that holds it. `cne backfill --symbols` and the
# horizon guard both need to name the right one — telling a trade_ticks user to
# narrow `[minute_bars].scope` sends them to edit a setting that does nothing.
SCOPED_DATASETS: dict[str, str] = {
    "minute_bars": "minute_bars",
    "minute_bars_5m": "minute_bars",
    "trade_ticks": "trade_ticks",
}


def _override_scope(cfg, dataset: str, symbols: list[str]) -> None:
    """Point *dataset* at exactly *symbols* for this run only.

    Enabling as well as scoping: a one-off `--symbols` pull should not also
    require flipping the config's `enabled` flag first, and the capture steps
    return early when it is false.
    """
    block = SCOPED_DATASETS.get(dataset)
    if block is None:
        raise click.ClickException(
            f"--symbols only applies to datasets with a configured scope "
            f"({', '.join(sorted(SCOPED_DATASETS))}); {dataset} takes its "
            "universe from instruments."
        )
    setattr(cfg, f"{block}_enabled", True)
    setattr(cfg, f"{block}_scope", "watchlist")
    setattr(cfg, f"{block}_symbols", symbols)
    # The ceiling exists to stop an unnoticed full-market sweep, not to second
    # guess a list the user just typed out by hand.
    if block == "trade_ticks":
        cfg.trade_ticks_max_symbols = max(cfg.trade_ticks_max_symbols, len(symbols))
    frequency = get_dataset(dataset).intraday_frequency
    if frequency and frequency not in cfg.minute_bars_frequencies:
        cfg.minute_bars_frequencies = [*cfg.minute_bars_frequencies, frequency]


def _guard_history_horizon(dataset: str, start: date | None) -> None:
    """Refuse a window the source cannot serve, instead of sweeping into nothing.

    A horizon-limited source does not return *less* data for an older window,
    it returns none — so without this an ``cne backfill minute_bars --start
    2016-01-01`` spends hours producing an empty lake and reads as a bug in the
    lake rather than a limit of the vendor.
    """
    spec = get_dataset(dataset)
    earliest = spec.earliest_available(shanghai_today())
    if earliest is None or start is None or start >= earliest:
        return
    if spec.history_floor_date is not None:
        # A fixed floor, not a per-symbol budget: no symbol reaches further
        # back, so there is no narrower scope that would help.
        raise click.ClickException(
            f"{dataset}: --start {start} is before the source's history floor. "
            f"The vendor serves nothing earlier than {earliest} for any symbol, "
            f"and no backfill source extends it. Re-run with --start {earliest} "
            "or later."
        )
    block = SCOPED_DATASETS.get(dataset, "minute_bars")
    raise click.ClickException(
        f"{dataset}: --start {start} is older than the source horizon. "
        f"The vendor caps history per symbol at about {spec.history_horizon_days} "
        f"trading days for an instrument quoted every session (back to about "
        f"{earliest}), and no backfill source extends it. Re-run with "
        f"--start {earliest} or later. "
        "(A barely-traded instrument holds bars on fewer days and so reaches "
        f"further back. To pull those, narrow [{block}].scope to a watchlist "
        "first — a full sweep at that start would spend hours on symbols that "
        "have nothing there.)"
    )


def _finish_backfill_run(engine, result: dict) -> dict:
    """Compact this run's staging, then close the run out."""
    run_id = result["run_id"]
    # Compact partial sweeps too, including failed ones. `compact` only ever
    # drains the *current* run's staging, so skipping it here would strand
    # every row the sweep did fetch before the failure — measured in
    # production: a walk_day_backfill window that flushed 21 clean days to
    # staging before an exception on day 22 still lost all 21, because this
    # used to skip compact on status=="failed". A run with nothing staged
    # compacts to a no-op (`step_compact` only touches datasets with files
    # under this run_id), so there is no cost to always trying.
    # Through the engine, not step_compact directly: the recorded compact
    # batch is what later lets `cne clean` release this run's staging.
    result["compact"] = engine.run_step("compact", shanghai_today(), run_id)
    compact_status = result["compact"].get("status", "success")
    if compact_status == "failed" or result["status"] == "failed":
        result["status"] = "failed"
    elif compact_status == "warning" or result["status"] == "warning":
        result["status"] = "warning"
    engine.manifest.finish_run(
        run_id,
        result["status"],
        rows_read=result.get("rows_read", 0),
        rows_written=result.get("rows_written", 0),
        error_message="one or more steps failed" if result["status"] == "failed" else None,
    )
    return result


def _recover_compactable_backfill_staging(engine: JobEngine, dataset: str) -> list[str]:
    """Compact staged rows left by an interrupted terminal backfill run.

    A process killed after a step flushed a batch has no chance to execute the
    normal ``_finish_backfill_run`` path. The next invocation used to start a
    fresh run while leaving those rows invisible in staging, so checkpointed
    positive facts were fetched again and the old run became a permanent
    staging leak. Terminal runs with staged files are safe to compact here: the
    regular compact gate still protects incomplete worker batches, and coverage
    receipts remain gated by their versioned checkpoint.
    """
    from cnequity.storage import StagingWriter

    config = getattr(engine, "config", None)
    if config is None:  # lightweight engine doubles in CLI/unit tests
        return []
    writer = StagingWriter(config.staging_root)
    recovered: list[str] = []
    for run in engine.manifest.list_runs("backfill"):
        run_id = str(run["run_id"])
        if run["status"] not in ("success", "warning", "failed"):
            continue
        batches = engine.manifest.get_batches_for_run(run_id)
        if any(batch["dataset"] == "compact" and batch["status"] == "success" for batch in batches):
            continue
        if not writer.list_run_files(dataset, run_id):
            continue
        result = engine.run_step("compact", shanghai_today(), run_id)
        if result.get("status") == "success":
            recovered.append(run_id)
            logging.getLogger(__name__).info(
                "Recovered staged %s from interrupted backfill run %s before retry",
                dataset,
                run_id,
            )
    return recovered


def _run_backfill(cfg, dataset: str, start: date | None, end: date | None) -> dict:
    """Backfill one window, dispatching exactly as `cne backfill` does.

    Shared so `cne verify --repair` cannot drift into a second, subtly
    different backfill path — the chunking rules below are not incidental
    (see `_backfill_symbol_chunked`).
    """
    if start is not None:
        cfg._backfill_start = start
    if end is not None:
        cfg._backfill_end = end
    spec = get_dataset(dataset)
    if spec.backfill_chunk_symbols and start and end:
        return _backfill_symbol_chunked(cfg, dataset, start, end, spec.backfill_chunk_symbols)
    if spec.backfill_chunk_days and start and end:
        return _backfill_chunked(cfg, dataset, start, end, spec.backfill_chunk_days)
    return _backfill_once(cfg, dataset)


def _backfill_once(cfg, dataset: str) -> dict:
    engine = JobEngine(cfg)
    _recover_compactable_backfill_staging(engine, dataset)
    # Do not finish_run until after compact — otherwise a kill between the two
    # leaves status=success with no compact batch, and `cne clean` cannot reclaim
    # staging that never reached curated (same ordering as delisted CLI).
    result = engine.run_job("backfill", steps=[dataset], backfill=True, finalize_run=False)
    return _finish_backfill_run(engine, result)


def _backfill_symbol_chunked(cfg, dataset: str, start: date, end: date, chunk_symbols: int) -> dict:
    """Backfill a tip-paged dataset as compacted symbol slices over [start, end].

    TDX intraday pages backwards from the live tip. A date-sliced sweep of the
    same window therefore re-walks tip → each slice_start for every symbol —
    measured ~8× the wire traffic of one tip→horizon walk on CSI300 1m. Chunking
    by symbol keeps one walk per name, bounds compact memory, and makes a kill
    cost only the current symbol batch.
    """
    from cnequity.steps.intraday import resolve_scope

    symbols = resolve_scope(cfg)
    if not symbols:
        raise click.ClickException(
            f"{dataset}: scope resolved to zero symbols — check [minute_bars].scope"
        )

    engine = JobEngine(cfg)
    _recover_compactable_backfill_staging(engine, dataset)
    chunks: list[dict] = []
    status = "success"
    rows_read = rows_written = 0
    original_scope = cfg.minute_bars_scope
    original_symbols = list(cfg.minute_bars_symbols)
    cfg._backfill_start, cfg._backfill_end = start, end
    try:
        for index in range(0, len(symbols), chunk_symbols):
            chunk = symbols[index : index + chunk_symbols]
            cfg.minute_bars_scope = "watchlist"
            cfg.minute_bars_symbols = chunk
            click.echo(
                f"[{dataset}] symbols {index + 1}..{index + len(chunk)}/"
                f"{len(symbols)} ({chunk[0]}..{chunk[-1]}) window {start}..{end}",
                err=True,
            )
            result = engine.run_job("backfill", steps=[dataset], backfill=True, finalize_run=False)
            result = _finish_backfill_run(engine, result)
            rows_read += int(result.get("rows_read", 0))
            rows_written += int(result.get("rows_written", 0))
            chunks.append(
                {
                    "symbols_from": index + 1,
                    "symbols_to": index + len(chunk),
                    "first_symbol": chunk[0],
                    "last_symbol": chunk[-1],
                    "start": start,
                    "end": end,
                    "status": result["status"],
                    "rows_written": result.get("rows_written", 0),
                }
            )
            if result["status"] == "failed":
                status = "failed"
                break
            if result["status"] == "warning":
                status = "warning" if status == "success" else status
    finally:
        cfg.minute_bars_scope = original_scope
        cfg.minute_bars_symbols = original_symbols

    return {
        "dataset": dataset,
        "status": status,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "chunks": chunks,
        "resume_from_symbol": (
            chunks[-1]["first_symbol"] if status == "failed" and chunks else None
        ),
    }


def _backfill_chunked(cfg, dataset: str, start: date, end: date, chunk_days: int) -> dict:
    """Run the backfill as a sequence of compacted date slices.

    One run for the whole window would stage more than compact can hold in
    memory (it reads every staging file of a run into one frame). Slicing also
    means a kill costs the current slice rather than the whole sweep: every
    earlier slice is already in curated.

    Do **not** use this for tip-paged intraday sources — see
    ``_backfill_symbol_chunked``.
    """
    engine = JobEngine(cfg)
    _recover_compactable_backfill_staging(engine, dataset)
    slices: list[dict] = []
    status = "success"
    rows_read = rows_written = 0
    cursor = start
    while cursor <= end:
        slice_end = min(cursor + timedelta(days=chunk_days - 1), end)
        cfg._backfill_start, cfg._backfill_end = cursor, slice_end
        click.echo(f"[{dataset}] slice {cursor}..{slice_end}", err=True)
        result = engine.run_job("backfill", steps=[dataset], backfill=True, finalize_run=False)
        result = _finish_backfill_run(engine, result)
        rows_read += int(result.get("rows_read", 0))
        rows_written += int(result.get("rows_written", 0))
        slices.append(
            {
                "start": cursor,
                "end": slice_end,
                "status": result["status"],
                "rows_written": result.get("rows_written", 0),
            }
        )
        if result["status"] == "failed":
            # Stop rather than press on: the slices already compacted are kept,
            # and the window to resume from is the one printed here.
            status = "failed"
            break
        if result["status"] == "warning":
            status = "warning" if status == "success" else status
        cursor = slice_end + timedelta(days=1)
    return {
        "dataset": dataset,
        "status": status,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "slices": slices,
        "resume_from": slices[-1]["start"] if status == "failed" and slices else None,
    }


@cli.command()
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
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


@cli.command()
@click.argument("dataset", required=False)
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
@click.option("--all", "do_all", is_flag=True, help="Repartition every dataset that needs it.")
@click.option("--dry-run", is_flag=True, help="Report the effect without swapping anything.")
def repartition(config_path: str, dataset: str | None, do_all: bool, dry_run: bool):
    """Rewrite a dataset's partitions at its configured granularity.

    Reads work whatever period the directories span, so this only reclaims the
    space and file opens a too-fine partitioning wastes. With no argument, lists
    the datasets whose layout does not match the registry.
    """
    from cnequity.storage.repartition import (
        RepartitionError,
        repartition_candidates,
        repartition_dataset,
    )

    cfg = _cfg(config_path)
    if dataset and do_all:
        raise click.ClickException("Pass a dataset or --all, not both.")

    candidates = repartition_candidates(cfg)
    if not dataset and not do_all:
        click.echo(json.dumps({"needs_repartition": candidates}, indent=2))
        return

    targets = [dataset] if dataset else candidates
    results = []
    for name in targets:
        try:
            res = repartition_dataset(cfg, name, dry_run=dry_run)
        except RepartitionError as exc:
            raise click.ClickException(str(exc)) from exc
        results.append(
            {
                "dataset": res.dataset,
                "changed": res.changed,
                "rows": res.rows,
                "files": f"{res.files_before} -> {res.files_after}",
                "partitions": f"{res.partitions_before} -> {res.partitions_after}",
                "mb": f"{res.bytes_before / 1e6:.1f} -> {res.bytes_after / 1e6:.1f}",
                "mb_saved": round(res.bytes_saved / 1e6, 1),
            }
        )
    click.echo(json.dumps({"dry_run": dry_run, "results": results}, indent=2))


@cli.command()
@click.argument("name", default="adj_factors")
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
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
    start = date.fromisoformat(start_str) if start_str else None
    end = date.fromisoformat(end_str) if end_str else None
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
        if result.best_effort_failed:
            click.echo(
                f"Best-effort BJ factors unavailable: "
                f"{len(result.best_effort_failed)} symbol×type fetch failure(s)",
                err=True,
            )
    elif name == "industry_index":
        from cnequity.derive.industry_index import derive_industry_index

        summary = derive_industry_index(cfg, start=start, end=end, full=full)
        click.echo(json.dumps(summary, indent=2, default=str))
    elif name == "trading_status":
        from cnequity.derive.trading_status_history import derive_suspension_history

        rows = derive_suspension_history(cfg, start=start, end=end)
        click.echo(f"Derived historical suspension: {rows} rows into trading_status")
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
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
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
    help="Strictly validate an all-A research window starting here (requires --full).",
)
@click.option(
    "--research-end",
    default=None,
    help="Research window end (default: latest daily_bars; requires --research-start).",
)
def audit(
    config_path: str,
    run_id: str | None,
    full: bool,
    research_start: str | None,
    research_end: str | None,
):
    """Run quality audit, or --full for a current whole-lake health snapshot."""
    cfg = _cfg(config_path)

    if research_start and not full:
        raise click.ClickException("--research-start requires --full")
    if research_end and not research_start:
        raise click.ClickException("--research-end requires --research-start")

    if full:
        from cnequity.quality.audit import lake_health

        start_date = date.fromisoformat(research_start) if research_start else None
        end_date = date.fromisoformat(research_end) if research_end else None
        if start_date and end_date and start_date > end_date:
            raise click.ClickException("--research-start must be on or before --research-end")
        health = lake_health(
            cfg,
            shanghai_today(),
            research_start=start_date,
            research_end=end_date,
        )
        sev = health["findings_by_severity"]
        click.echo(f"Lake health @ last trading day {health['last_trading_day']}")
        click.echo(
            f"  findings: {sev.get('error', 0)} error, "
            f"{sev.get('warning', 0)} warning, {sev.get('info', 0)} info"
        )
        if health["empty_datasets"]:
            click.echo(f"  empty datasets: {', '.join(health['empty_datasets'])}")
        if health["stale_datasets"]:
            click.echo(f"  STALE datasets: {', '.join(health['stale_datasets'])}")
        for f in health["error_findings"]:
            click.echo(f"  [error]   {f.get('dataset', ''):22} {f.get('message', '')}")
        for f in health["warning_findings"]:
            click.echo(f"  [warning] {f.get('dataset', ''):22} {f.get('message', '')}")
        validity = health["historical_universe_validity"]
        research_state = "READY" if validity["universe_ready"] else "BLOCKED"
        click.echo(
            f"  historical all-A {validity['window']['start']}.."
            f"{validity['window']['end']}: {research_state}"
        )
        for blocker in validity["blockers"]:
            click.echo(f"  [research] {blocker['message']}")
        click.echo("HEALTHY" if health["healthy"] else "UNHEALTHY")
        if not health["healthy"] or (research_start and not validity["universe_ready"]):
            raise SystemExit(1)
        return

    manifest = Manifest(cfg.manifest_path)
    latest = manifest.latest_run() if not run_id else None
    rid = run_id or (latest["run_id"] if latest else "manual")

    n = run_audit(cfg, rid, shanghai_today())
    click.echo(f"Audit complete: {n} findings written")


def _last_trading_day(cfg, today: date) -> date:
    from datetime import timedelta

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
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
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
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
@click.option(
    "--datasets",
    "show_datasets",
    is_flag=True,
    help="Per-dataset freshness: coverage, watermark, and staleness vs the last trading day.",
)
def status(config_path: str, show_datasets: bool):
    """Show latest run status, or per-dataset freshness with --datasets."""
    cfg = _cfg(config_path)

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
        with pl_mod.Config(tbl_rows=-1, tbl_cols=-1, fmt_str_lengths=32):
            click.echo(df)
        stale = df.filter(pl_mod.col("freshness") == "STALE").height
        if stale:
            click.echo(f"\n{stale} dataset(s) STALE — check runs with `cne status` / `cne retry`.")
            raise SystemExit(1)
        return

    manifest = Manifest(cfg.manifest_path)
    latest = manifest.latest_run()
    if not latest:
        click.echo("No runs yet.")
        return
    summary = manifest.run_summary(latest["run_id"])
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


@cli.command()
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
@click.option("--run-id", required=True)
def retry(config_path: str, run_id: str):
    """Retry failed batches and missing init steps for a run."""
    cfg = _cfg(config_path)
    engine = JobEngine(cfg)
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
    if result.get("status") not in ("success",):
        raise SystemExit(1)


@cli.command()
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
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


@cli.command()
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
def catalog(config_path: str):
    """List datasets and latest partition info."""
    cfg = _cfg(config_path)
    curated = cfg.curated_root
    entries = []
    if curated.exists():
        for ds_dir in sorted(curated.iterdir()):
            if ds_dir.is_dir():
                files = list(ds_dir.glob("**/*.parquet"))
                # lazy count(*) resolves from parquet metadata without
                # decoding data pages — cheap even on a 10-year lake.
                rows = (
                    int(scan_parquet_files(files).select(pl.len()).collect().item()) if files else 0
                )
                entries.append({"dataset": ds_dir.name, "files": len(files), "rows": rows})
    click.echo(json.dumps(entries, indent=2))


_LOOPBACK = {"127.0.0.1", "::1", "localhost"}


@cli.command()
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--port", default=8787, show_default=True)
@click.option(
    "--token",
    default=None,
    help="Require this bearer token (or ?token=). Mandatory for a non-loopback --host.",
)
def serve(config_path: str, host: str, port: int, token: str | None):
    """Serve the read-only lake dashboard.

    Shows coverage, freshness and source mix. Nothing here writes to the lake —
    running, retrying and cleaning stay with the CLI.
    """
    import uvicorn

    from cnequity.serve.app import create_app

    # Checked before the config is even loaded: a typo in --config must not
    # mask the bind guard by failing first. The service has no other access
    # control, and a lake holds a full market history plus the paths and
    # sources that built it.
    if host not in _LOOPBACK and not token:
        raise click.ClickException(
            f"--host {host} would expose the dashboard beyond this machine; "
            "pass --token to require one, or leave --host at 127.0.0.1."
        )

    cfg = _cfg(config_path)
    click.echo(f"lake:      {cfg.data_root}")
    click.echo(f"dashboard: http://{host}:{port}/" + (f"?token={token}" if token else ""))
    click.echo(f"api docs:  http://{host}:{port}/api/docs")
    click.echo(
        f"sources:   http://{host}:{port}/source-health" + (f"?token={token}" if token else "")
    )
    uvicorn.run(create_app(cfg, token=token), host=host, port=port, log_level="info")


@cli.group()
def stats():
    """Lake measurement tables under meta/stats (rows, bytes, source mix)."""


@stats.command("rebuild")
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
@click.option(
    "--dataset",
    "dataset_names",
    multiple=True,
    help="Rebuild only these datasets (repeatable). Other datasets keep their rows.",
)
@click.option("--json", "as_json", is_flag=True, help="Print the result as JSON.")
def stats_rebuild(config_path: str, dataset_names: tuple[str, ...], as_json: bool):
    """Recompute partition_stats / provenance_stats from curated and derived."""
    from cnequity.storage.stats import rebuild_stats

    cfg = _cfg(config_path)
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


@stats.command("refresh")
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
@click.option("--force", is_flag=True, help="Rebuild even when the stats are current.")
def stats_refresh(config_path: str, force: bool):
    """Rebuild only if ingestion has run since the stats were built.

    Safe to schedule on a timer: it is a no-op when nothing has changed, and a
    concurrent rebuild elsewhere makes it exit rather than queue behind one.
    """
    from cnequity.storage.stats import refresh_stats_if_stale, stats_freshness

    cfg = _cfg(config_path)
    freshness = stats_freshness(cfg)
    result = refresh_stats_if_stale(cfg, force=force)
    if result is None:
        if freshness.stale:
            click.echo("stale, but another rebuild holds the lock — nothing to do")
        else:
            click.echo(f"current as of run {freshness.latest_run_id} — nothing to do")
        return
    click.echo(
        f"rebuilt ({freshness.reason or 'forced'}): "
        f"{len(result.datasets)} dataset(s), {result.rows:,} row(s) "
        f"in {result.elapsed_seconds:.1f}s"
    )


@stats.command("show")
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
@click.option("--dataset", default=None, help="Per-partition detail for one dataset.")
@click.option("--by-source", is_flag=True, help="Group by source / data_version instead.")
def stats_show(config_path: str, dataset: str | None, by_source: bool):
    """Summarise the stats tables. Run `cne stats rebuild` first."""
    from cnequity.storage.stats import (
        load_partition_stats,
        load_provenance_stats,
        load_summary,
        stats_freshness,
    )

    cfg = _cfg(config_path)
    summary = load_summary(cfg)
    if summary is None:
        raise click.ClickException("no stats yet — run `cne stats rebuild`")
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

    stale_note = f"  STALE — {freshness.reason}; run `cne stats refresh`" if freshness.stale else ""
    click.echo(
        f"generated_at: {summary.get('generated_at')}  "
        f"run: {summary.get('latest_run_id')}{stale_note}"
    )
    with pl.Config(tbl_rows=-1, tbl_cols=-1, fmt_str_lengths=32):
        click.echo(df.sort(df.columns[:2]))


@cli.command()
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
@click.option("--sql", default="SELECT COUNT(*) AS n FROM daily_bars")
@click.option("--dataset", default=None, help="On-demand dataset name")
@click.option("--symbol", default=None, help="Symbol for on-demand fetch")
def query(config_path: str, sql: str, dataset: str | None, symbol: str | None):
    """Run DuckDB SQL or on-demand dataset fetch."""
    cfg = _cfg(config_path)
    if dataset and symbol:
        svc = OnDemandService(cfg)
        data = svc.fetch(dataset, symbol)
        click.echo(json.dumps(data, indent=2, ensure_ascii=False, default=str))
        return
    db_path = ensure_duckdb_views(cfg)
    import duckdb

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        df = con.execute(sql).pl()
        click.echo(df)
    finally:
        con.close()


@cli.command("mcp")
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
@click.option(
    "--live",
    is_flag=True,
    help="Where the lake holds nothing, fetch from the vendor on demand and do "
    "not store it. Serves symbol lookup and unadjusted daily bars only; every "
    "other tool refuses rather than answer without adjustment, universe or PIT.",
)
def mcp_cmd(config_path: str, live: bool):
    """Serve this lake to an AI agent over MCP (stdio).

    Not meant to be typed interactively: any MCP-compatible client spawns it
    and talks JSON-RPC on the pipe. The client-specific registration UI varies;
    the portable command and arguments are simply::

      cne mcp --config /path/to/cnequity.toml

    Use that command as the ``command``/``args`` entry in the client's MCP
    configuration. This implementation uses the standard stdio transport, not
    a vendor-specific Claude integration.

    Read-only, like `cne serve`. The tools query the lake; ingestion stays on
    the CLI, where a person runs it.
    """
    import logging
    import sys

    from cnequity.mcp_server import serve_stdio

    cfg = _cfg(config_path)
    # Opt-in, never inferred. A lake user whose lake is broken must get "no
    # parquet data" and go fix it, not a quietly different answer from a vendor.
    cfg._mcp_live = live
    if not live:
        _guard_mcp_data_root(cfg, config_path)

    # stdout is the JSON-RPC wire. Anything else written there is a parse error
    # on the client with no indication of where it came from, so every log
    # record — ours and every library's — goes to stderr, which MCP clients
    # capture as the server's log.
    logging.basicConfig(stream=sys.stderr, level=logging.WARNING)
    serve_stdio(cfg)


def _guard_mcp_data_root(cfg, config_path: str) -> None:
    """Refuse to serve a lake with nothing in it, and say why it is empty.

    A relative ``data.root`` resolves against the working directory, and this is
    the one entry point where the working directory belongs to somebody else —
    an MCP client spawns the process from wherever it happens to be. The lake
    then resolves to a path that does not exist, every tool answers "no parquet
    data", and the agent reports that the data is missing. Which is true of that
    path and false of the user's lake.

    Cheap enough to do on every start: one directory walk that stops at the
    first file.
    """
    curated = cfg.curated_root
    if curated.exists() and next(curated.rglob("*.parquet"), None) is not None:
        return
    raise click.ClickException(
        f"No curated data under {curated}.\n"
        f"  config:    {resolve_config_path(config_path).resolve()}\n"
        f"  data.root: {cfg.data_root}\n"
        "If that is not your lake, `data.root` is relative and resolved against "
        "the working directory the client started this process in. Make both "
        "`--config` and `[data].root` absolute paths.\n"
        "If it is your lake and it is genuinely empty: `cne init` builds one, "
        "`cne demo` makes a 5-symbol sample in 30 seconds, and `--live` serves "
        "symbol lookup and raw daily bars straight from the vendor without one."
    )


@cli.command("sources")
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
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
def sources(config_path: str, vantage: str, only: str | None, out: str | None):
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

    path = Path(out) if out else cfg.meta_root / "source_health" / f"{vantage}.json"
    write_json_atomic(
        path,
        report.to_dict(),
        indent=2,
        ensure_ascii=False,
    )
    click.echo(f"\nWrote {path}")
    click.echo("View it with: cne serve  \u2192  http://127.0.0.1:8787/source-health")

    # Exit 0 even when sources are down. A red source is this command's *output*,
    # not its failure.


@cli.command("servers", hidden=True)
@click.argument("action", type=click.Choice(["test"]))
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
def servers(action: str, config_path: str):
    """Deprecated alias for `cne sources --only tdx_protocol`.

    That probe is strictly the stronger check: it asserts that real bars came
    back, where this one only proved a socket opened. Kept working — it is in
    the quickstart and in people's runbooks — but off the top-level list.
    """
    from cnequity.diagnostics.source_health import PROBES_BY_KEY, ProbeStatus, run_probe

    click.echo("note: `cne servers test` is now `cne sources --only tdx_protocol`", err=True)
    result = run_probe(PROBES_BY_KEY["tdx_protocol"], _cfg(config_path))
    if result.status == ProbeStatus.OK.value:
        click.echo(f"TDX connection OK ({result.detail}, {result.latency_ms}ms)")
        return
    click.echo(f"TDX {result.status}: {result.detail}", err=True)
    raise SystemExit(1)


if __name__ == "__main__":
    cli()


@cli.group("delisted")
def delisted_grp():
    """Reconstruct the delisted universe (survivorship-bias repair)."""


@delisted_grp.command("discover")
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
@click.option("--limit", default=None, type=int, help="Probe at most N codes this run.")
def delisted_discover(config_path: str, limit: int | None):
    """Sweep the issued code space for codes that used to trade.

    Resumable: a re-run continues where the last one stopped. Codes whose probe
    failed stay pending rather than being filed as never-issued.
    """
    import logging

    from cnequity.steps.delisted import discover_delisted

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    result = discover_delisted(_cfg(config_path), limit=limit)
    click.echo(
        json.dumps(
            {
                "probed": result.probed,
                "delisted": result.delisted,
                "never_issued": result.never_issued,
                "failed": len(result.failed),
                "remaining": result.remaining,
                "complete": result.complete,
            },
            indent=2,
        )
    )


@delisted_grp.command("status")
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
@click.option("--since", default="2016-01-01", show_default=True, help="Lake window start.")
@click.option("--sample", default=15, show_default=True, help="Rows of detail to print.")
def delisted_status(config_path: str, since: str, sample: int):
    """Summarise the catalogue: how many, from when, and what is left to probe."""
    from collections import Counter

    from cnequity.steps.delisted import (
        LIVE_RECENCY_DAYS,
        classify_catalog,
        delisted_symbols_in_window,
        pending_codes,
    )

    cfg = _cfg(config_path)
    start = date.fromisoformat(since)
    catalog, live_missing = classify_catalog(cfg)
    in_window = {s: d for s, d in catalog.items() if d >= start}
    by_year = Counter(d.year for d in in_window.values())
    by_board = Counter(s.split(".")[1] for s in in_window)
    recent = sorted(in_window.items(), key=lambda kv: kv[1], reverse=True)[:sample]
    click.echo(
        json.dumps(
            {
                "delisted": len(catalog),
                "in_window": len(in_window),
                # Still quoting near the latest session: either a code the
                # instrument list is missing, or a delisting inside the recency
                # window that will reclassify on the next sweep.
                "live_or_recent": len(live_missing),
                "live_or_recent_by_exchange": dict(
                    sorted(Counter(s.split(".")[1] for s in live_missing).items())
                ),
                "live_recency_days": LIVE_RECENCY_DAYS,
                "window_start": since,
                "pending_probe": len(pending_codes(cfg)),
                "not_yet_ingested": len(delisted_symbols_in_window(cfg, start)),
                "by_year": dict(sorted(by_year.items())),
                "by_exchange": dict(sorted(by_board.items())),
                "most_recent": [{"symbol": s, "last_traded": d.isoformat()} for s, d in recent],
            },
            indent=2,
        )
    )


@delisted_grp.command("coverage")
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
@click.option("--start", default="2016-01-01", show_default=True, help="Research window start.")
@click.option("--end", default=None, help="Research window end (default: latest lake session).")
@click.option("--sample", default=15, show_default=True, type=click.IntRange(min=0))
def delisted_coverage(config_path: str, start: str, end: str | None, sample: int):
    """Fail unless the requested window has verified delisting coverage.

    Read-only. The JSON separates incomplete discovery, definite missing bars,
    uncertain overlap, terminal mismatches, and instruments identity gaps.
    """
    from cnequity.steps.delisted import delisted_coverage_report

    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end) if end else None
    if end_date is not None and start_date > end_date:
        raise click.ClickException("--start must be on or before --end")
    report = delisted_coverage_report(_cfg(config_path), start_date, end_date, sample=sample)
    click.echo(json.dumps(report, indent=2))
    if not report["verified"]:
        raise SystemExit(1)


@delisted_grp.command("reconcile")
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
@click.option("--apply", is_flag=True, help="Apply high-confidence corrections.")
@click.option("--sample", default=15, show_default=True, type=click.IntRange(min=0))
def delisted_reconcile(config_path: str, apply: bool, sample: int):
    """Audit catalogue terminals and optionally apply independently proven fixes.

    Dry-run by default. ``--apply`` refuses to run during any active ingestion,
    backs up the catalogue, and writes a quality receipt.
    """
    from cnequity.steps.delisted import (
        delisted_catalog_reconciliation_report,
        reconcile_delisted_catalog,
    )

    cfg = _cfg(config_path)
    try:
        report = (
            reconcile_delisted_catalog(cfg, sample=sample)
            if apply
            else delisted_catalog_reconciliation_report(cfg, sample=sample)
        )
    except RuntimeError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(report, indent=2))


@delisted_grp.command("repair")
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
@click.option(
    "--since",
    default=None,
    help="Only catalogued delistings on/after this date (default: all genuine).",
)
def delisted_repair(config_path: str, since: str | None):
    """Wire catalogued / orphan-bar delistings into instruments without re-fetching.

    Use this when daily_bars already holds the recovered series (e.g. from
    baostock) but instruments still has no delist_date — the gap that leaves
    ``universe=all_a`` selecting dead names. Also drops ``认购款`` stubs.
    """
    import logging

    from cnequity.steps.delisted import repair_delisted_instruments

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", force=True)
    cfg = _cfg(config_path)
    start = date.fromisoformat(since) if since else None
    engine = JobEngine(cfg)
    meta = {"since": since} if since else {}
    run_id = engine.manifest.start_run("delisted_repair", meta)
    result = repair_delisted_instruments(cfg, run_id, start=start)
    compact_out = engine.run_step("compact", shanghai_today(), run_id)
    # Compact can re-introduce nothing for placeholders; purge once more after
    # the merge in case an older curated copy still carried them.
    from cnequity.steps.delisted import purge_subscription_placeholders

    result["purged_placeholders_after_compact"] = purge_subscription_placeholders(cfg)
    source_status = result.get("status", "success")
    compact_status = compact_out.get("status", "success")
    if "failed" in (source_status, compact_status):
        run_status = "failed"
    elif "warning" in (source_status, compact_status):
        run_status = "warning"
    else:
        run_status = "success"
    engine.manifest.finish_run(
        run_id,
        run_status,
        rows_read=result.get("rows_read", 0),
        rows_written=result.get("rows_written", 0),
        error_message=None if run_status == "success" else "delisted repair is incomplete",
    )
    ensure_duckdb_views(cfg)
    click.echo(
        json.dumps(
            {"run_id": run_id, **result, "status": run_status, "compact": compact_out},
            indent=2,
            default=str,
        )
    )
    if run_status != "success":
        raise click.ClickException("delisted repair is incomplete; retry the missing scope")


@delisted_grp.command("backfill")
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
@click.option("--since", default="2016-01-01", show_default=True, help="Lake window start.")
def delisted_backfill(config_path: str, since: str):
    """Fetch price history for catalogued delistings and compact it into the lake."""
    import logging

    from cnequity.steps.delisted import backfill_delisted_bars

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    cfg = _cfg(config_path)
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run("delisted_backfill", {"since": since})
    result = backfill_delisted_bars(cfg, run_id, date.fromisoformat(since))
    compact_out = engine.run_step("compact", shanghai_today(), run_id)
    complete = (
        result.get("status", "success") == "success"
        and compact_out.get("status", "success") == "success"
    )
    run_status = "success" if complete else "warning"
    error_message = None if complete else "delisted recovery has unresolved targets"
    engine.manifest.finish_run(
        run_id,
        run_status,
        rows_read=result.get("rows_read", 0),
        rows_written=result.get("rows_written", 0),
        error_message=error_message,
    )
    click.echo(
        json.dumps(
            {"run_id": run_id, **result, "status": run_status, "compact": compact_out},
            indent=2,
            default=str,
        )
    )
    if not complete:
        raise click.ClickException(error_message)
