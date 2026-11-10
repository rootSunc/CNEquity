"""First-run commands: `init`, `config`, `doctor`.

Everything a fresh clone touches before it has a lake, in the order the
quickstart walks through them.
"""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path

import click

from cnequity.cli._root import cli
from cnequity.cli._shared import (
    DEMO_CONFIG,
    DEMO_DATA_ROOT,
    SAMPLE_CONFIG,
    SAMPLE_DATA_ROOT,
    _cfg,
    _progress_logging,
    _run_status_exit_code,
    attach_log_file,
    comma_option,
    comma_values,
    config_is_implicit,
    config_option,
    ingest_scope_label,
    parse_date_option,
    resolve_config_path,
    star_hint_once,
)
from cnequity.config import validate_config, write_user_config
from cnequity.domain.market_time import shanghai_today
from cnequity.i18n import N_, _, localize
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.run_lock import INIT_JOB_LOCK, is_run_locked
from cnequity.run_scope import set_backfill
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


def _echo_init_plan(
    profile: str,
    *,
    since_str: str | None,
    history_start: date | None,
    trade_date: date,
    resume: bool,
    config_path: str,
    ingest_universe: str,
) -> None:
    """Explain the required backbone and optional history before fetching."""
    scope = ingest_scope_label(ingest_universe)
    if resume:
        override = (
            _(" This time the explicit --since={value} overrides the history start.").format(
                value=history_start.isoformat()
            )
            if since_str and history_start
            else _(" The original run's history range stays as it was.")
        )
        click.echo(
            _(
                "Resume plan: keep successful batches and handle only failed or missing phases; batch progress and the ETA tell the remaining time.{override}"
            ).format(override=override)
        )
    elif since_str:
        click.echo(
            _(
                "Init plan: {scope}, custom history window {value} .. {value2}; duration depends on the window length, see the live ETA."
            ).format(
                scope=scope,
                value=history_start.isoformat() if history_start else since_str,
                value2=trade_date.isoformat(),
            )
        )
    elif profile == "full":
        click.echo(
            _(
                "Init plan: full, {scope}, daily bars from {value}; deep history needs more paging; batch progress and the ETA tell the duration."
            ).format(scope=scope, value=BACKFILL_START.isoformat())
        )
    else:
        click.echo(
            _(
                "Init plan: quick, {scope}, the last {QUICK_PROFILE_YEARS} years; duration depends on source reachability, paging and existing caches; batch progress and the ETA tell it."
            ).format(scope=scope, QUICK_PROFILE_YEARS=QUICK_PROFILE_YEARS)
        )
    click.echo(
        _(
            "Init scope: current trading status and suspensions derived from daily bars; daily bars of symbols delisted inside the window are recovered automatically."
        )
    )
    click.echo(
        _(
            "Optional extra: the whole-market historical ST scan is not part of init and does not affect init completing; historical ST research needs a separate `cne backfill trading_status --config {config_path}`, whose coverage depends on the market and enabled sources."
        ).format(config_path=config_path)
    )


# `cne init --profile demo|sample` used to be `cne demo`. It is the same
# decision as `quick` vs `full` — how much of the market to build — and asking a
# first-time user to choose between two commands before they have either was one
# fork too many. The option sets stay disjoint, so each side refuses the other's
# flags rather than accepting and ignoring them.
DEMO_PROFILES = ("demo", "sample")
_DEMO_ONLY = ("symbols", "days", "data_root", "config_out", "intraday", "research", "force")
_LAKE_ONLY = (
    "config_path",
    "layout_only",
    "resume",
    "resume_run_id",
    "keep_going",
    "since_str",
    "pack_names",
    "schedule",
    "as_json",
    "no_preflight",
)
_FLAG_NAMES = {
    "config_path": "--config",
    "data_root": "--data-root",
    "config_out": "--config-out",
    "resume_run_id": "--run-id",
    "since_str": "--since",
    "pack_names": "--pack",
    "schedule": "--schedule",
    "as_json": "--json",
}


def _create_default_config_if_missing(config_path: str) -> bool:
    """Let a first `cne init` stand alone instead of needing `config create` first.

    Only the implicit default path is created: an explicit `--config` or
    `CNE_CONFIG` that does not exist is more likely a typo than a request for a
    new lake, and silently building one there would hide that.
    """
    ctx = click.get_current_context(silent=True)
    if ctx is None or ctx.get_parameter_source("config_path") is not (
        click.core.ParameterSource.DEFAULT
    ):
        return False
    path = Path(config_path).expanduser()
    if path.exists():
        return False
    write_user_config(path)
    return True


def _reject_foreign_options(profile: str, names: tuple[str, ...]) -> None:
    """Fail on an option the chosen --profile has no meaning for."""
    ctx = click.get_current_context(silent=True)
    if ctx is None:
        return
    for name in names:
        if ctx.get_parameter_source(name) is not click.core.ParameterSource.COMMANDLINE:
            continue
        flag = _FLAG_NAMES.get(name, "--" + name.replace("_", "-"))
        raise click.UsageError(
            _("{flag} does not apply to --profile {profile}").format(flag=flag, profile=profile)
        )


@cli.command()
@config_option
@click.option(
    "--profile",
    type=click.Choice(["demo", "sample", "quick", "full"]),
    default="quick",
    show_default=True,
    # The years and start date are QUICK_PROFILE_YEARS and BACKFILL_START, written
    # out so the catalog can hold the text; a test fails if either drifts.
    help=N_(
        "How big to build. demo = fetch a few symbols from real sources; sample = the same shape "
        "but offline and deterministic — neither is a market. quick = all symbols, last 3 years; "
        "full = all symbols since 2016-01-01 (usually needs more pages). You can deepen history "
        "later with `cne backfill daily_bars`."
    ),
)
@comma_option(
    "--symbols",
    default=",".join(("600519.SH", "000001.SZ", "000858.SZ", "300750.SZ", "601318.SH")),
    show_default=True,
    help=N_("demo/sample: symbols to fetch, comma-separated (deliberately few)."),
)
@click.option(
    "--days",
    default=30,
    show_default=True,
    help=N_("demo/sample: roughly how many recent trading days of daily_bars to fetch."),
)
@click.option(
    "--data-root",
    default=None,
    show_default="data/cnequity-demo; sample: data/cnequity-sample",
    help=N_("demo/sample: separate lake root (do not use it for a full-market init)."),
)
@click.option(
    "--config-out",
    default=None,
    show_default=f"{DEMO_CONFIG}; sample: {SAMPLE_CONFIG}",
    help=N_("demo/sample: where to write the small config for later `cne query` use."),
)
@click.option(
    "--force",
    is_flag=True,
    help=N_(
        "demo/sample: allow overwriting an existing --config-out with different content; by default refuses and keeps the original file."
    ),
)
@click.option(
    "--intraday",
    is_flag=True,
    help=N_(
        "demo/sample: also fetch 1-minute bars for the same symbols (up to 5 trading days) and print one trading day, so the bar_time convention is visible."
    ),
)
@click.option(
    "--research",
    is_flag=True,
    help=N_(
        "demo/sample: also derive hfq adjustment factors from Sina and print an unadjusted vs. adjusted return comparison (slower; needs access to Sina)."
    ),
)
@click.option(
    "--layout-only",
    is_flag=True,
    help=N_("Create only directories, the manifest and DuckDB views; skip the init stages."),
)
@click.option(
    "--trade-date",
    default=None,
    help=N_("As-of trading day (YYYY-MM-DD) for the init stages; defaults to today."),
)
@click.option(
    "--resume",
    is_flag=True,
    help=N_(
        "Resume the most recent unfinished init run (retry failed batches + fill missing stages)."
    ),
)
@click.option(
    "--run-id",
    "resume_run_id",
    default=None,
    help=N_("Resume the given init run_id (implies --resume)."),
)
@click.option(
    "--keep-going",
    is_flag=True,
    help=N_("Keep going after a stage fails instead of stopping."),
)
@click.option(
    "--since",
    "since_str",
    default=None,
    help=N_("Explicit history start (YYYY-MM-DD); overrides --profile."),
)
@click.option(
    "--quiet", is_flag=True, help=N_("Show only warnings and above; no per-batch progress.")
)
@click.option(
    "--pack",
    "pack_names",
    multiple=True,
    type=click.Choice(["market", "fundamentals", "universe"]),
    help=(
        N_(
            "Research pack: market (market data), fundamentals, universe (historical ST). Does not change what this init downloads. Decides which schedule groups stay updated afterward and the verdict of `cne check --pack`. Repeatable. If omitted, the saved selection is reused; the first time it is market."
        )
    ),
)
@click.option(
    "--schedule",
    is_flag=True,
    help=N_(
        "After init succeeds, install the scheduled daily update and closing catch-up for the selected research packs. Excludes announcements and news."
    ),
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    help=N_(
        "Print the full machine-readable result at the end instead of the summary; the full result is always saved to the log directory too."
    ),
)
@click.option(
    "--no-preflight",
    is_flag=True,
    help=N_(
        "Skip the network preflight (TDX, exchanges, EastMoney) of up to 8 seconds before the run starts. Setting CNE_SKIP_PREFLIGHT=1 does the same."
    ),
)
def init(
    config_path: str,
    profile: str,
    symbols: str,
    days: int,
    data_root: str | None,
    config_out: str | None,
    force: bool,
    intraday: bool,
    research: bool,
    layout_only: bool,
    trade_date: str | None,
    resume: bool,
    resume_run_id: str | None,
    keep_going: bool,
    since_str: str | None,
    quiet: bool,
    pack_names: tuple[str, ...],
    schedule: bool,
    as_json: bool,
    no_preflight: bool,
):
    """Initialize the lake, and run every init phase the config defines.

    \b
    The default is `--profile quick`: the last few years, every symbol in the market. Shallow, that is, but never **narrow**.
    Cutting symbols would cut the very reason this lake exists (avoiding survivorship bias), whereas shallow is honest:
    `coverage_start` records exactly how deep the lake is, while a missing symbol looks like one that never traded.

    \b
    Why quick is the default: the deeper the history window, the more paging and writing there usually is; but even with a
    shallow window each symbol still has connection and request overhead. The last 3 years are the default research window:
    build a usable lake first, and fill deeper history by its gaps when you need it. Actual duration depends on your network, source responses and caches.

    \b
    Go deeper without re-running init:

      cne backfill daily_bars --start 2016-01-01 --end <your coverage_start>

    or go full from the start: `--profile full`.

    On the first run, when no config is found (configs/cnequity.toml here, or the per-user ~/.config/cnequity/config.toml),
    the per-user one is generated from the bundled example
    (the same as `cne config create`). After an interruption or partial coverage,
    re-running the same `cne init` resumes it, and batches that succeeded are not fetched again.

    quick/full both scan the configured universe and recover daily bars of symbols delisted inside the window automatically.
    Trading status takes the current snapshot and derives historical suspensions; the historical ST scan is a separate, optional
    job run with `cne backfill trading_status`, and is not a condition for init to complete.

    \b
    `--profile demo` builds a few symbols into a separate `--data-root`, with progress and query results within a minute;
    `--profile sample` does the same offline and deterministically. Neither is a market, and neither touches `--config`:
    they write their own config to `--config-out`.

    """
    if profile in DEMO_PROFILES:
        _reject_foreign_options(profile, _LAKE_ONLY)
        from cnequity.cli.demo import run_demo, run_sample_demo

        # Separate defaults per profile: the synthetic sample and the real demo
        # sharing one config and one root meant the sample's own closing hint,
        # "run `cne init --profile demo`", failed on the config it had written.
        sample = profile == "sample"
        runner = run_sample_demo if sample else run_demo
        runner(
            symbols=comma_values(symbols, "--symbols"),
            days=days,
            data_root=Path(data_root or (SAMPLE_DATA_ROOT if sample else DEMO_DATA_ROOT)),
            trade_date=parse_date_option(trade_date, "--trade-date"),
            config_out=Path(config_out or (SAMPLE_CONFIG if sample else DEMO_CONFIG)),
            intraday=intraday,
            research=research,
            force=force,
        )
        return

    _reject_foreign_options(profile, _DEMO_ONLY)
    _progress_logging(quiet)
    created = _create_default_config_if_missing(config_path)
    cfg = _cfg(config_path)
    if created:
        click.echo(
            _(
                "No config found; wrote {config_path} with defaults (data.root = {data_root}). Edit it if needed, then re-run `cne init`."
            ).format(config_path=config_path, data_root=cfg.data_root)
        )
    init_data_layout(cfg)
    if layout_only:
        click.echo(_("Directory layout created at {data_root}").format(data_root=cfg.data_root))
        return

    # After the layout-only exit: a command that finishes in a second has
    # nothing to leave behind but an empty file.
    log_path = attach_log_file(cfg, "init", quiet=quiet)

    # On a weekend or holiday "today" has no session: the Beijing board and the
    # exchange status pages answer nothing for it, which left a lake built on a
    # Saturday without BJ and without trading status. Use the newest finished
    # session, as the demo does.
    from cnequity.cli.demo import _last_trading_day

    td = parse_date_option(trade_date, "--trade-date") or _last_trading_day(cfg, shanghai_today())

    history_start = _init_history_start(profile, since_str, td)

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
                    _(
                        "An init is already running (run {run_id}). Wait for it to finish, or stop it before starting a new one."
                    ).format(run_id=incomplete["run_id"])
                )
            click.echo(
                _(
                    "Found an unfinished init run {run_id} whose process is gone; resuming it rather than starting over (finished batches are kept)."
                ).format(run_id=incomplete["run_id"]),
                err=True,
            )
            resume = True
            resume_run_id = str(incomplete["run_id"])

    config_arg = (
        ""
        if click.get_current_context().get_parameter_source("config_path")
        is click.core.ParameterSource.DEFAULT
        else f" --config {config_path}"
    )
    _init_preflight(cfg, skip=no_preflight, config_arg=config_arg)

    is_resume = resume or bool(resume_run_id)
    # A default `--profile quick` is not an explicit request to shrink the
    # history of an older full/custom run. Let the engine restore that run's
    # recorded window. `--since` is the one deliberate resume-time override.
    if history_start is not None and (not is_resume or since_str):
        set_backfill(cfg, start=history_start)
        click.echo(
            _(
                "History window: {value} .. {value2} ({value3}, {value4} depth). Go deeper later with `cne backfill daily_bars --start <an earlier date>`."
            ).format(
                value=history_start.isoformat(),
                value2=td.isoformat(),
                value3=ingest_scope_label(cfg.ingest_universe),
                value4=profile if not since_str else "custom",
            )
        )

    _echo_init_plan(
        profile,
        since_str=since_str,
        history_start=history_start,
        trade_date=td,
        resume=is_resume,
        config_path=config_path,
        ingest_universe=cfg.ingest_universe,
    )
    from cnequity.research.packs import format_readiness, remember_packs

    chosen_packs = remember_packs(cfg, pack_names, override=bool(pack_names))

    from cnequity.keep_awake import keep_awake

    with keep_awake() as held:
        if held:
            click.echo(
                _(
                    "System idle sleep is blocked while init runs (closing the lid or sleeping by hand still works); set CNE_ALLOW_SLEEP=1 if you do not want this."
                ),
                err=True,
            )
        result = engine.run_init_phases(
            trade_date=td,
            resume=is_resume,
            resume_run_id=resume_run_id,
            keep_going=keep_going,
        )
    from cnequity.cli.init_summary import render_init_summary, save_init_result

    result_path = save_init_result(
        log_path.parent if log_path is not None else cfg.data_root / "logs", result
    )
    exit_code = _run_status_exit_code(str(result.get("status", "failed")))
    if exit_code and result.get("lake_usable"):
        # Bars are published; what is owed resumes with the next `cne init`.
        exit_code = 0
    click.echo(format_readiness(cfg, chosen_packs), err=True)
    if as_json:
        click.echo(json.dumps(result, indent=2, default=str))
    else:
        for line in render_init_summary(
            result,
            config_arg=config_arg,
            config_abs=Path(config_path).expanduser().resolve(),
            data_root=cfg.data_root,
            log_path=log_path,
            result_path=result_path,
        ):
            click.echo(line)
    if schedule and not exit_code:
        click.echo(_install_pack_schedule(cfg, chosen_packs), err=True)
    elif schedule:
        click.echo(
            _("Init did not finish successfully, so no scheduled task was installed."), err=True
        )
    if exit_code:
        raise SystemExit(exit_code)
    if not quiet:
        star_hint_once(cfg)


def _init_preflight(cfg, *, skip: bool, config_arg: str) -> None:
    """Fail in seconds when TDX is unreachable, rather than minutes into a step."""
    from cnequity.diagnostics.preflight import (
        PREFLIGHT_BUDGET_SECONDS,
        preflight_skip_reason,
        run_init_preflight,
    )
    from cnequity.diagnostics.source_health import STATUS_LABELS, ProbeStatus

    if preflight_skip_reason(cfg, requested_skip=skip):
        return
    click.echo(
        _("Network preflight (at most {PREFLIGHT_BUDGET_SECONDS:g} seconds):").format(
            PREFLIGHT_BUDGET_SECONDS=PREFLIGHT_BUDGET_SECONDS
        ),
        err=True,
    )
    outcome = run_init_preflight(cfg)
    for result in outcome.results:
        label = _(STATUS_LABELS[ProbeStatus(result.status)])
        latency = f"{result.latency_ms}ms" if result.latency_ms is not None else "—"
        line = f"  {label:<4} {_(result.label)}  {latency}"
        if result.status != ProbeStatus.OK.value:
            line += f"  {localize(result.detail)}"
            if result.key not in outcome.required:
                line += _(" (the related steps will degrade without blocking the build)")
        click.echo(line, err=True)
    if outcome.ok:
        return
    failed = outcome.failed_required[0]
    raise click.ClickException(
        _(
            "{label} is unreachable ({detail}). A whole-market init takes its security list and daily bars from it, so starting now would only fail a few minutes in.\n  Check the network or proxy first; networks outside mainland China usually cannot reach TDX.\n  See every data source: cne sources probe{config_arg}\n  If the network is fine and only this probe was slow, add --no-preflight to skip the preflight."
        ).format(label=_(failed.label), detail=localize(failed.detail), config_arg=config_arg)
    )


def _install_pack_schedule(cfg, packs: tuple[str, ...]) -> str:
    """Register the current-user timer for these packs. A failure leaves the lake."""
    from cnequity.ops.catalog import OpsError
    from cnequity.ops.scheduler import ScheduleService
    from cnequity.research.packs import SNAPSHOT_NOTE, groups_for

    try:
        service = ScheduleService(cfg)
        preview = service.preview(
            daily=True,
            stale=True,
            daily_run_at=cfg.daily_run_at,
            stale_run_at=cfg.stale_run_at,
            daily_packs=list(packs),
        )
        service.apply(preview["token"], acknowledged=True, requested_by="cne-init")
    except (OpsError, OSError, ValueError) as exc:
        return _("Scheduled task not installed: {exc}").format(exc=exc)
    groups = _(", ").join(groups_for(packs)) or _("none")
    return _(
        "Installed the scheduled daily update and closing catch-up, schedule groups: {groups}.{SNAPSHOT_NOTE}"
    ).format(groups=groups, SNAPSHOT_NOTE=_(SNAPSHOT_NOTE))


#: What `cne config` does, and the spellings that used to mean one of them.
#: `cnequity.cli._root.MOVED` answers a moved *command*; an argument is not a
#: command, so it needs its own map — and this is the one people hit first.
CONFIG_ACTIONS: tuple[str, ...] = ("validate", "create", "show", "diff", "upgrade", "path")
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
    help=N_("With action=create: overwrite an existing config file."),
)
@click.option(
    "--data-root",
    default=None,
    help=N_(
        "With action=create: set [data].root (defaults to ./data/cnequity resolved to an absolute path)."
    ),
)
@click.option(
    "--dry-run",
    is_flag=True,
    help=N_(
        "With action=upgrade: only list the steps and schedule groups to add; writes no files."
    ),
)
@click.option(
    "--minimize",
    is_flag=True,
    help=N_(
        "With action=upgrade: rewrite a full config to extend the built-in defaults, keeping only "
        "the settings that differ; lists every setting whose behaviour would change."
    ),
)
@click.option(
    "--defaults",
    "show_defaults",
    is_flag=True,
    help=N_("With action=show: print every setting with its built-in default and explanation."),
)
def config_cmd(
    action: str,
    config_path: str,
    force: bool,
    data_root: str | None,
    dry_run: bool = False,
    minimize: bool = False,
    show_defaults: bool = False,
):
    """Validate, generate or compare configs.

    \b
    `cne config create` writes a short config that extends the built-in defaults (`extends = "builtin"`) and holds only
    the data directory; later releases' new settings and schedule steps then apply without editing it. It used to be
    called `cne config init`, one word away from `cne init`, which builds the whole lake and costs far more when run by mistake.
    `cne config show` prints the effective configuration and marks what the file sets; `--defaults` prints every setting.
    `cne config validate` validates an existing config.
    `cne config path` shows which config a command would use, why, and the lake it points to.
    `cne config diff` reports what a full config lacks against the defaults; above all the steps new versions add
    to schedule groups, which a config written once and never updated will never run.
    `cne config upgrade` adds those steps and the missing schedule groups straight into the config (backing up the original first),
    leaving every other setting alone. `--minimize` instead rewrites a full config to extend the built-in defaults.

    """
    # A free-form argument bypasses `token_normalize_func`, which is what makes
    # every other name in this CLI case-insensitive; normalise it here so
    # `cne config CREATE` behaves like `cne run DAILY`.
    action = action.lower()
    moved = CONFIG_ACTIONS_MOVED.get(action)
    if moved:
        raise click.ClickException(
            _("`cne config {action}` has been renamed; use `{moved}` instead.").format(
                action=action, moved=moved
            )
        )
    if action not in CONFIG_ACTIONS:
        raise click.BadParameter(
            _("{action!r} is not one of {value}").format(
                action=action, value=", ".join(repr(a) for a in CONFIG_ACTIONS)
            ),
            param_hint="'{" + "|".join(CONFIG_ACTIONS) + "}'",
        )
    if dry_run and action != "upgrade":
        raise click.UsageError(_("--dry-run only applies to `cne config upgrade`"))
    if minimize and action != "upgrade":
        raise click.UsageError(_("--minimize only applies to `cne config upgrade`"))
    if show_defaults and action != "show":
        raise click.UsageError(_("--defaults only applies to `cne config show`"))
    if action == "show":
        _config_show(config_path, defaults=show_defaults)
        return
    if action in {"upgrade", "diff"}:
        # A demo or sample config deliberately schedules nothing; comparing it
        # with the full example would list every step as missing, and
        # upgrading it would turn five symbols into a whole-market schedule.
        try:
            profile = getattr(_cfg(config_path), "lake_profile", None)
        except (click.ClickException, ValueError, OSError):
            profile = None  # a config that does not load is the action's own error to report
        if profile in {"demo", "sample"}:
            click.echo(
                _(
                    "{path} belongs to a {profile} lake, which has no schedule to compare or upgrade; `cne init` builds a lake with a full config."
                ).format(path=resolve_config_path(config_path), profile=profile)
            )
            return
    if action == "upgrade":
        if minimize:
            _config_minimize(resolve_config_path(config_path), dry_run=dry_run)
        else:
            _config_upgrade(resolve_config_path(config_path), dry_run=dry_run)
        return
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

    if action == "path":
        _config_path_report(config_path)
        return

    if action == "create":
        out = Path(config_path).expanduser()
        try:
            write_user_config(out, data_root=data_root, force=force)
        except FileExistsError as exc:
            raise click.ClickException(str(exc)) from exc
        click.echo(_("Written to {out}").format(out=out))
        click.echo(
            _(
                "It extends the built-in defaults and sets only data.root (an absolute path); "
                "add only what you want to change. `cne config show --defaults` lists every setting. "
                "Then run: cne init"
            )
        )
        return

    cfg = _cfg(config_path)
    errors = validate_config(cfg)
    if errors:
        for e in errors:
            click.echo(f"ERROR: {e}", err=True)
        raise SystemExit(1)
    click.echo(_("Config check passed"))


def _config_path_report(config_path: str) -> None:
    """`cne config path`: the config in effect, how it was chosen, and its lake."""
    from cnequity.config.locate import config_candidates, describe_source

    ctx = click.get_current_context()
    source = ctx.get_parameter_source("config_path")
    path = Path(config_path).expanduser()
    if source is click.core.ParameterSource.COMMANDLINE:
        why = _("given with --config")
    elif source is click.core.ParameterSource.ENVIRONMENT:
        why = _("named by CNE_CONFIG")
    elif describe_source(path) == "project":
        why = _("found in the current directory")
    else:
        why = _("the per-user config")
    click.echo(_("Config: {path} ({why})").format(path=path.absolute(), why=why))
    if path.is_file():
        click.echo(_("Data root: {root}").format(root=_cfg(config_path).data_root))
    elif config_is_implicit():
        click.echo(_("Not created yet; `cne init` or `cne config create` writes it here."))
    else:
        raise click.ClickException(_("Config not found: {path}").format(path=path))
    click.echo(_("Lookup order without --config: CNE_CONFIG, then:"))
    for candidate in config_candidates():
        mark = "*" if candidate == path else " "
        click.echo(f"  {mark} {candidate}")


_SECRET_KEY = re.compile(r"(?:^|_)(?:token|api_key|secret|password)$")
_PROXY_USERINFO = re.compile(r"^([a-z][a-z0-9+.-]*://)[^@/]+@", re.I)


def _flat_keys(table: dict, prefix: str = "") -> set[str]:
    out: set[str] = set()
    for key, value in table.items():
        path = f"{prefix}{key}"
        out.add(path)
        if isinstance(value, dict):
            out |= _flat_keys(value, f"{path}.")
    return out


def _mask_secrets(table: dict) -> dict:
    out: dict = {}
    for key, value in table.items():
        if isinstance(value, dict):
            out[key] = _mask_secrets(value)
        elif isinstance(value, str) and _SECRET_KEY.search(key):
            out[key] = "***"
        elif isinstance(value, str) and key == "proxy":
            out[key] = _PROXY_USERINFO.sub(r"\1***@", value)
        else:
            out[key] = value
    return out


def _config_show(config_path: str, *, defaults: bool) -> None:
    """`cne config show`: the configuration the lake runs with, or every default."""
    import sys

    from cnequity.config.bootstrap import render_example_toml
    from cnequity.config.layers import EXTENDS_KEY, effective_raw, is_layered
    from cnequity.config.toml_out import dumps

    if defaults:
        click.echo(render_example_toml(platform=sys.platform), nl=False)
        return
    if sys.version_info >= (3, 11):
        import tomllib
    else:  # pragma: no cover - Python 3.10
        import tomli as tomllib

    path = resolve_config_path(config_path)
    cfg = _cfg(config_path)
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    effective = effective_raw(raw)
    effective.setdefault("data", {})["root"] = cfg.data_root.as_posix()
    layered = is_layered(raw)
    own = _flat_keys({k: v for k, v in raw.items() if k != EXTENDS_KEY})
    edited = {
        key.rsplit(".", 1)[0] + ".steps"
        for key in own
        if key.endswith((".steps_add", ".steps_remove"))
    }

    def note(key: str) -> str | None:
        if not layered:
            return None
        if key in edited:
            return _("edited by steps_add / steps_remove")
        return _("set in this file") if key in own else None

    click.echo(_("# Effective configuration of {path}").format(path=path))
    click.echo(
        _("# Built-in defaults of this version, with the settings marked below from the file.")
        if layered
        else _(
            "# A full config: every value is the file's own. "
            "`cne config upgrade --minimize` turns it into one that extends the built-in defaults."
        )
    )
    click.echo(_("# Secrets are masked."))
    click.echo("")
    click.echo(dumps(_mask_secrets(effective), annotate=note), nl=False)


def _config_minimize(path: Path, *, dry_run: bool) -> None:
    """`cne config upgrade --minimize`: keep only what differs from the built-in defaults."""
    import shutil
    from datetime import datetime

    from cnequity.config import load_config
    from cnequity.config.minimize import plan_minimize

    plan = plan_minimize(path)
    if plan.already_layered:
        click.echo(
            _("{path} already extends the built-in defaults; nothing to minimize.").format(
                path=path
            )
        )
        return
    click.echo(
        _(
            "Keeps {kept} of {total} settings; the rest follow the built-in defaults of each release."
        ).format(kept=plan.kept_keys, total=plan.total_keys)
    )
    for table, (added, removed) in plan.step_edits.items():
        click.echo(
            _("[{table}] steps_add = {added}; steps_remove = {removed}").format(
                table=table, added=added, removed=removed
            )
        )
    for table in plan.disabled_groups:
        click.echo(
            _("[{table}] enabled = false (this config does not schedule it today)").format(
                table=table
            )
        )
    if plan.behaviour_changes:
        click.echo(
            _(
                "These settings change: the old config left them to built-in fallbacks, and the "
                "defaults now supply them:"
            )
        )
        for name, before, after in plan.behaviour_changes:
            click.echo(f"  {name}: {before} → {after}")
    else:
        click.echo(_("Loading the new file gives the same settings as the old one."))
    if dry_run:
        return
    backup = path.with_name(f"{path.name}.bak-{datetime.now():%Y%m%d-%H%M%S}")
    shutil.copy2(path, backup)
    path.write_text(plan.text, encoding="utf-8")
    click.echo(
        _("Updated {path} (the original was backed up as {name})").format(
            path=path, name=backup.name
        )
    )
    errors = validate_config(load_config(path))
    for e in errors:
        click.echo(f"ERROR: {e}", err=True)
    if errors:
        raise SystemExit(1)


def _config_upgrade(path: Path, *, dry_run: bool) -> None:
    """Apply the schedule edits of `plan_upgrade`, keeping a timestamped backup."""
    import shutil
    from datetime import datetime

    from cnequity.config import load_config
    from cnequity.config.upgrade import plan_upgrade

    plan = plan_upgrade(path)
    if not plan.changed and not plan.manual_steps:
        click.echo(
            _(
                "The schedule in {path} already includes every step of this version; nothing to upgrade."
            ).format(path=path)
        )
        return
    for table, steps in plan.added_steps.items():
        label = (
            f"[[job.daily.waves]] {table.removeprefix('wave:')}"
            if table.startswith("wave:")
            else f"[{table}]"
        )
        click.echo(_("Add steps: {label} += {value}").format(label=label, value=", ".join(steps)))
    for table in plan.added_groups:
        click.echo(_("Add schedule group: [{table}]").format(table=table))
    if plan.pinned_root is not None:
        click.echo(
            _(
                "Pin [data].root: {written} → {absolute} (a relative root is now read against the config's directory)"
            ).format(written=plan.pinned_root[0], absolute=plan.pinned_root[1])
        )
    if plan.manual_steps:
        click.echo(
            _(
                "Could not place these automatically; add them by hand, following the example config: "
            )
            + ", ".join(plan.manual_steps),
            err=True,
        )
    if dry_run or not plan.changed:
        if plan.manual_steps:
            raise SystemExit(1)
        return
    backup = path.with_name(f"{path.name}.bak-{datetime.now():%Y%m%d-%H%M%S}")
    shutil.copy2(path, backup)
    path.write_text(plan.text, encoding="utf-8")
    click.echo(
        _("Updated {path} (the original was backed up as {name})").format(
            path=path, name=backup.name
        )
    )
    errors = validate_config(load_config(path))
    for e in errors:
        click.echo(f"ERROR: {e}", err=True)
    if errors or plan.manual_steps:
        raise SystemExit(1)


@cli.command()
@config_option
@click.option("--json", "as_json", is_flag=True, help=N_("Output machine-readable JSON."))
def doctor(config_path: str, as_json: bool):
    """Check the environment, optional dependencies and config for things that would quietly break.

    \b
    Needs no config (it runs right after installing) and no network. It exits non-zero only when data would really be lost;
    the typical case is a source enabled in the config whose package is not installed, which no other command mentions.

    """
    from cnequity.diagnostics.render import render_text, to_dict
    from cnequity.diagnostics.report import Finding, Severity, build_report

    cfg = None
    resolved: Path | None = None
    path = Path(config_path).expanduser()
    config_problem = None
    if path.exists():
        try:
            cfg = _cfg(str(path))
            resolved = path
        except Exception as exc:  # config errors must not hide the dependency report
            config_problem = _("Failed to load config: {path} ({name})").format(
                path=path, name=type(exc).__name__
            )
    elif click.get_current_context().get_parameter_source("config_path") in {
        click.core.ParameterSource.COMMANDLINE,
        click.core.ParameterSource.ENVIRONMENT,
    }:
        config_problem = _("The given config does not exist: {path}").format(path=path)

    report = build_report(config=cfg, config_path=resolved)
    if config_problem:
        report.findings.append(
            Finding(Severity.ERROR, config_problem, fix="cne config validate --config <path>")
        )

    if as_json:
        click.echo(json.dumps(to_dict(report), indent=2, default=str))
    else:
        for line in render_text(report):
            click.echo(line)

    if not report.ok:
        raise SystemExit(1)
