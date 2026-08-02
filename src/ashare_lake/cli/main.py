from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import click
import polars as pl

import ashare_lake.steps  # noqa: F401 — register steps
from ashare_lake.config import WaveConfig, load_config, validate_config, write_user_config
from ashare_lake.derive.adj_factors import compute_adj_factors
from ashare_lake.domain.datasets import (
    fetch_semantics,
    get_dataset,
)
from ashare_lake.orchestrator.engine import JobEngine
from ashare_lake.orchestrator.manifest import Manifest
from ashare_lake.orchestrator.run_lock import RunLockError
from ashare_lake.quality.audit import run_audit
from ashare_lake.query.on_demand import OnDemandService
from ashare_lake.query.views import ensure_duckdb_views
from ashare_lake.steps.common import BACKFILL_START
from ashare_lake.storage.layout import init_data_layout
from ashare_lake.storage.source_snapshots import (
    DEFAULT_SNAPSHOT_RETENTION_DAYS,
    clean_source_snapshots,
)
from ashare_lake.storage.staging_cleanup import clean_staging

USER_CONFIG = "configs/ashare-lake.toml"
EXAMPLE_CONFIG = "configs/ashare-lake.example.toml"
DEFAULT_CONFIG = USER_CONFIG

# `asl init --profile quick`. Three years is the shortest window that still
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


def resolve_config_path(config_path: str) -> Path:
    path = Path(config_path)
    if config_path == USER_CONFIG and not path.exists():
        raise click.ClickException(
            f"Config not found: {USER_CONFIG}. "
            "Run `asl config init` to write one from the packaged example "
            f"(or copy {EXAMPLE_CONFIG} if you have the repo checkout)."
        )
    if not path.exists():
        raise click.ClickException(f"Config not found: {path}")
    return path


def _cfg(config: str):
    return load_config(resolve_config_path(config))


@click.group()
@click.version_option(package_name="ashare-lake")
def cli():
    """ashare-lake — A-share data ingestion CLI."""


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
    default="data/ashare-lake-demo",
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
    default="configs/ashare-lake.demo.toml",
    show_default=True,
    help="Where to write the tiny demo config for follow-up `asl query`.",
)
@click.option(
    "--intraday",
    is_flag=True,
    help="Also capture 1-minute bars for the same symbols (up to 5 sessions) "
    "and print a session, so the bar_time convention is visible.",
)
def demo_cmd(
    symbols: str,
    days: int,
    data_root: str,
    trade_date_str: str | None,
    config_out: str,
    intraday: bool,
):
    """Fetch a tiny real-source lake so you can see progress and results quickly.

    Not a full-market backfill — use `asl init` for that. Requires network access
    to TDX hosts (mainland egress is more reliable overseas).
    """
    from ashare_lake.cli.demo import run_demo

    td = date.fromisoformat(trade_date_str) if trade_date_str else None
    run_demo(
        symbols=[s.strip() for s in symbols.split(",") if s.strip()],
        days=days,
        data_root=Path(data_root),
        trade_date=td,
        config_out=Path(config_out),
        intraday=intraday,
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
    default="full",
    show_default=True,
    help=f"How much history to fetch. quick = the last {QUICK_PROFILE_YEARS} years "
    f"instead of everything from {BACKFILL_START.isoformat()}.",
)
@click.option(
    "--since",
    "since_str",
    default=None,
    help="Explicit history start (YYYY-MM-DD); overrides --profile.",
)
def init(
    config_path: str,
    layout_only: bool,
    trade_date: str | None,
    resume: bool,
    resume_run_id: str | None,
    keep_going: bool,
    profile: str,
    since_str: str | None,
):
    """Initialize data lake and run configured init phases (first full backfill).

    `--profile quick` makes the first run SHALLOWER, never NARROWER: every
    symbol is still fetched, just fewer years each. Dropping symbols instead
    would build the survivorship bias this lake exists to avoid straight into
    it, and `coverage_start` records a shallow lake honestly where a missing
    name would look like a name that never traded.

    Deepen later without re-running init:

      asl backfill daily_bars --start 2016-01-01 --end <your coverage_start>
    """
    cfg = _cfg(config_path)
    init_data_layout(cfg)
    if layout_only:
        click.echo(f"Initialized layout at {cfg.data_root}")
        return

    td = date.fromisoformat(trade_date) if trade_date else date.today()

    history_start = _init_history_start(profile, since_str, td)
    if history_start is not None:
        cfg._backfill_start = history_start
        click.echo(
            f"History window: {history_start.isoformat()} .. {td.isoformat()} "
            f"(full universe, {profile if not since_str else 'custom'} depth). "
            "Deepen later with `asl backfill daily_bars --start <earlier>`."
        )

    engine = JobEngine(cfg)

    if not resume and not resume_run_id:
        incomplete = engine.manifest.latest_incomplete_init_run()
        if incomplete is not None:
            raise click.ClickException(
                f"Incomplete init run {incomplete['run_id']} exists "
                f"(status={incomplete['status']}). "
                "Use `asl init --resume` or `asl retry --run-id "
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
    help="Set [data].root when action=init (default: resolve ./data/ashare-lake to an absolute path).",
)
def config_cmd(action: str, config_path: str, force: bool, data_root: str | None):
    """Validate or bootstrap configuration.

    ``asl config init`` writes the packaged example TOML (no repo checkout needed).
    On macOS it also forces ``orchestrator.workers = 1``.
    ``asl config validate`` checks an existing file.
    """
    if action == "init":
        out = Path(config_path)
        try:
            write_user_config(out, data_root=data_root, force=force)
        except FileExistsError as exc:
            raise click.ClickException(str(exc)) from exc
        click.echo(f"Wrote {out}")
        click.echo("data.root is absolute; edit if needed, then: asl config validate && asl init")
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
    from ashare_lake.diagnostics.render import render_text, to_dict
    from ashare_lake.diagnostics.report import build_report

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

    Freshness is judged exactly as ``asl status --datasets`` judges it, so the
    two cannot disagree about what is behind.

    Derived datasets are excluded: they are recomputed by ``asl derive`` from
    curated inputs, and re-fetching is not what they need. Datasets with no
    registered step are excluded because there is nothing to run.
    """
    # Steps are registered by the module-level `import ashare_lake.steps`.
    from ashare_lake.domain.datasets import DATASETS, is_stale
    from ashare_lake.orchestrator.registry import STEP_REGISTRY
    from ashare_lake.query.reader import list_datasets

    out: list[str] = []
    for row in list_datasets(config=cfg).iter_rows(named=True):
        name = row["dataset"]
        spec = DATASETS[name]
        if spec.layer == "derived" or name not in STEP_REGISTRY:
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
    anchor = _last_trading_day(cfg, trade_date or date.today())
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
):
    """Run daily ingestion job (Wave DAG or schedule group)."""
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
    from ashare_lake.query.parquet_scan import list_hive_partition_dates
    from ashare_lake.storage.state import StateStore

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
    from ashare_lake.steps.common import is_trading_day, list_trading_dates

    cfg = _cfg(config_path)
    if trade_date_str:
        td = date.fromisoformat(trade_date_str)
        if not is_trading_day(cfg, td):
            raise click.ClickException(f"{td.isoformat()} is not a trading day")
    else:
        # Walk back up to ~3 weeks for long holidays.
        end = date.today()
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
    help="Comma-separated symbols to restrict an intraday backfill to "
    "(minute_bars, minute_bars_5m), overriding [minute_bars].scope for this "
    "run only. Use it for a one-off pull without editing the config.",
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
    # Multi-hour sweeps (baostock ST, EM sector kline) need visible progress on
    # stdout; adapters log at INFO. Keep WARNING+ for third-party noise.
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
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
# `instruments`, and the block that holds it. `asl backfill --symbols` and the
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
    it returns none — so without this an ``asl backfill minute_bars --start
    2016-01-01`` spends hours producing an empty lake and reads as a bug in the
    lake rather than a limit of the vendor.
    """
    spec = get_dataset(dataset)
    earliest = spec.earliest_available(date.today())
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
    # Compact partial sweeps too. `compact` only ever drains the *current* run's
    # staging, so skipping it on a warning would strand every row the sweep did
    # fetch — while its resume checkpoint already counts those boards as done,
    # which is how a partial backfill turns into a silent hole in curated.
    if result["status"] in ("success", "warning"):
        # Through the engine, not step_compact directly: the recorded compact
        # batch is what later lets `asl clean` release this run's staging.
        result["compact"] = engine.run_step("compact", date.today(), run_id)
    engine.manifest.finish_run(
        run_id,
        result["status"],
        rows_read=result.get("rows_read", 0),
        rows_written=result.get("rows_written", 0),
        error_message="one or more steps failed" if result["status"] == "failed" else None,
    )
    return result


def _backfill_once(cfg, dataset: str) -> dict:
    engine = JobEngine(cfg)
    # Do not finish_run until after compact — otherwise a kill between the two
    # leaves status=success with no compact batch, and `asl clean` cannot reclaim
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
    from ashare_lake.steps.intraday import resolve_scope

    symbols = resolve_scope(cfg)
    if not symbols:
        raise click.ClickException(
            f"{dataset}: scope resolved to zero symbols — check [minute_bars].scope"
        )

    engine = JobEngine(cfg)
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

    out = JobEngine(cfg).run_step("compact", date.today(), run_id)
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
    from ashare_lake.storage.repartition import (
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
    help="trading_status: only derive suspensions on/after this date (YYYY-MM-DD).",
)
@click.option(
    "--end",
    "end_str",
    default=None,
    help="trading_status: only derive suspensions on/before this date (YYYY-MM-DD).",
)
def derive(name: str, config_path: str, full: bool, start_str: str | None, end_str: str | None):
    """Derive computed datasets."""
    cfg = _cfg(config_path)
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
        from ashare_lake.derive.industry_index import derive_industry_index

        summary = derive_industry_index(cfg, full=full)
        click.echo(json.dumps(summary, indent=2, default=str))
    elif name == "trading_status":
        from ashare_lake.derive.trading_status_history import derive_suspension_history

        start = date.fromisoformat(start_str) if start_str else None
        end = date.fromisoformat(end_str) if end_str else None
        if start and end and start > end:
            raise click.ClickException("--start must be on or before --end")
        rows = derive_suspension_history(cfg, start=start, end=end)
        click.echo(f"Derived historical suspension: {rows} rows into trading_status")
    elif name == "sector_routing":
        from ashare_lake.derive.sector_routing import derive_sector_routing

        summary = derive_sector_routing(cfg)
        click.echo(json.dumps(summary, indent=2, default=str))
    elif name == "sector_code_map":
        from ashare_lake.derive.sector_code_map import derive_sector_code_map

        summary = derive_sector_code_map(cfg)
        click.echo(json.dumps(summary, indent=2, default=str))
    elif name == "valuation_orphans":
        from ashare_lake.storage.valuation_orphans import purge_valuation_orphan_symbols

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
def audit(config_path: str, run_id: str | None, full: bool):
    """Run quality audit, or --full for a current whole-lake health snapshot."""
    cfg = _cfg(config_path)

    if full:
        from ashare_lake.quality.audit import lake_health

        health = lake_health(cfg, date.today())
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
        click.echo("HEALTHY" if health["healthy"] else "UNHEALTHY")
        if not health["healthy"]:
            raise SystemExit(1)
        return

    manifest = Manifest(cfg.manifest_path)
    latest = manifest.latest_run() if not run_id else None
    rid = run_id or (latest["run_id"] if latest else "manual")

    n = run_audit(cfg, rid, date.today())
    click.echo(f"Audit complete: {n} findings written")


def _last_trading_day(cfg, today: date) -> date:
    from datetime import timedelta

    from ashare_lake.steps.common import is_trading_day

    d = today
    for _ in range(15):
        if is_trading_day(cfg, d):
            return d
        d -= timedelta(days=1)
    return today


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

        from ashare_lake.domain.datasets import is_stale
        from ashare_lake.query.reader import list_datasets

        anchor = _last_trading_day(cfg, date.today())
        df = list_datasets(config=cfg)

        def _freshness(row: dict) -> str:
            if not row["has_data"]:
                return "empty"
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
            click.echo(f"\n{stale} dataset(s) STALE — check runs with `asl status` / `asl retry`.")
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
            f">={int(cfg.batch_stale_seconds)}s — next asl run reconciles them; "
            "or `asl clean --reconcile-runs`"
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
        "`asl retry` refetches them (data is refetched, not lost, but the retry "
        "becomes a full re-run). Do not use on success-without-compact runs — "
        "run `asl compact --run-id` first."
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
                    int(pl.scan_parquet([str(f) for f in files]).select(pl.len()).collect().item())
                    if files
                    else 0
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

    from ashare_lake.serve.app import create_app

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
    from ashare_lake.storage.stats import rebuild_stats

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
    from ashare_lake.storage.stats import refresh_stats_if_stale, stats_freshness

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
    """Summarise the stats tables. Run `asl stats rebuild` first."""
    from ashare_lake.storage.stats import (
        load_partition_stats,
        load_provenance_stats,
        load_summary,
        stats_freshness,
    )

    cfg = _cfg(config_path)
    summary = load_summary(cfg)
    if summary is None:
        raise click.ClickException("no stats yet — run `asl stats rebuild`")
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

    stale_note = f"  STALE — {freshness.reason}; run `asl stats refresh`" if freshness.stale else ""
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
def mcp_cmd(config_path: str):
    """Serve this lake to an AI agent over MCP (stdio).

    Not meant to be typed: an MCP client spawns it and talks JSON-RPC on the
    pipe. Register it once, e.g.

      claude mcp add ashare-lake -- asl mcp --config /path/to/ashare-lake.toml

    Read-only, like `asl serve`. The tools query the lake; ingestion stays on
    the CLI, where a person runs it.
    """
    import logging
    import sys

    from ashare_lake.mcp_server import serve_stdio

    cfg = _cfg(config_path)
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
        "If it is your lake and it is genuinely empty, run `asl init` first."
    )


@cli.command("servers")
@click.argument("action", type=click.Choice(["test"]))
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
def servers(action: str, config_path: str):
    """Test TDX server connectivity."""
    try:
        from ashare_lake.adapters.tdx_protocol.client import _quotes_client

        cfg = _cfg(config_path)
        client = _quotes_client(cfg)
        _ = client
        click.echo("TDX connection OK")
    except ImportError:
        click.echo("TDX wire client unavailable — this is a bug, please report it")
    except Exception as exc:
        click.echo(f"TDX connection failed: {exc}", err=True)
        raise SystemExit(1) from exc


@cli.group("push2his")
def push2his_grp():
    """push2his CDN edge sticky / probe (sector_bars kline)."""


@push2his_grp.command("remember")
@click.argument("endpoint")
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
def push2his_remember(endpoint: str, config_path: str):
    """Save Chrome DevTools Remote Address as sticky CDN edge.

    Example: asl push2his remember 61.129.129.199:443
    """
    from ashare_lake.adapters.eastmoney.em_auth import remember_push2his_endpoint

    cfg = _cfg(config_path)
    remember_push2his_endpoint(endpoint, config=cfg)
    click.echo(f"sticky push2his edge → {endpoint.split(':')[0]}")


@push2his_grp.command("probe")
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
def push2his_probe(config_path: str):
    """Discover CDN edges and probe which ones answer kline (updates sticky on hit)."""
    from ashare_lake.adapters.eastmoney.em_auth import (
        EastMoneyClient,
        _candidate_ips,
        _sticky_path,
    )

    cfg = _cfg(config_path)
    sticky = _sticky_path(cfg)
    candidates = _candidate_ips("push2his.eastmoney.com", sticky, force_discover=True)
    click.echo(f"candidates ({len(candidates)}): {', '.join(candidates)}")
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": "90.BK1152",
        "fields1": "f1",
        "fields2": "f51",
        "klt": 101,
        "fqt": 1,
        "beg": 0,
        "end": "20500101",
        "lmt": 2,
    }
    try:
        with EastMoneyClient(config=cfg) as client:
            resp = client.get(url, params=params)
        code = int(getattr(resp, "status_code", 0) or 0)
        body = getattr(resp, "text", "") or ""
        click.echo(f"probe OK status={code} bytes={len(body.encode('utf-8', 'replace'))}")
        if sticky and sticky.exists():
            click.echo(f"sticky: {sticky.read_text(encoding='utf-8').strip()}")
    except Exception as exc:
        click.echo(f"probe FAILED: {exc}", err=True)
        raise SystemExit(1) from exc


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

    from ashare_lake.steps.delisted import discover_delisted

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

    from ashare_lake.steps.delisted import (
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

    from ashare_lake.steps.delisted import repair_delisted_instruments

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", force=True)
    cfg = _cfg(config_path)
    start = date.fromisoformat(since) if since else None
    engine = JobEngine(cfg)
    meta = {"since": since} if since else {}
    run_id = engine.manifest.start_run("delisted_repair", meta)
    result = repair_delisted_instruments(cfg, run_id, start=start)
    compact_out = engine.run_step("compact", date.today(), run_id)
    # Compact can re-introduce nothing for placeholders; purge once more after
    # the merge in case an older curated copy still carried them.
    from ashare_lake.steps.delisted import purge_subscription_placeholders

    result["purged_placeholders_after_compact"] = purge_subscription_placeholders(cfg)
    engine.manifest.finish_run(run_id, "success", rows_written=result.get("rows_written", 0))
    ensure_duckdb_views(cfg)
    click.echo(
        json.dumps({"run_id": run_id, **result, "compact": compact_out}, indent=2, default=str)
    )


@delisted_grp.command("backfill")
@click.option("--config", "config_path", default=DEFAULT_CONFIG, show_default=True)
@click.option("--since", default="2016-01-01", show_default=True, help="Lake window start.")
def delisted_backfill(config_path: str, since: str):
    """Fetch price history for catalogued delistings and compact it into the lake."""
    import logging

    from ashare_lake.steps.delisted import backfill_delisted_bars

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    cfg = _cfg(config_path)
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run("delisted_backfill", {"since": since})
    result = backfill_delisted_bars(cfg, run_id, date.fromisoformat(since))
    compact_out = engine.run_step("compact", date.today(), run_id)
    engine.manifest.finish_run(run_id, "success", rows_written=result.get("rows_written", 0))
    click.echo(
        json.dumps({"run_id": run_id, **result, "compact": compact_out}, indent=2, default=str)
    )
