"""The scheduled path, and the way back from a failed run: `run daily`,
`run events`, `run retry`.

A bare `run daily` composes one day's schedule groups and then the event
stream, because the shell pipelines that used to be the only way to run them
all are not installed by the package. Everything around a day — health check, source probe, metadata backup,
the late stale-only pass — still lives in `scripts/scheduler/daily_pipeline.sh`.
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
    attach_revision_retention,
    config_option,
    parse_date_option,
)
from cnequity.cli.quality_cmds import _last_trading_day
from cnequity.config import WaveConfig
from cnequity.domain.market_time import is_session_final, shanghai_today
from cnequity.i18n import N_, _
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
            raise click.ClickException(
                _("Unknown stale schedule groups: {value}").format(value=", ".join(sorted(unknown)))
            )
        allowed = {step for name in groups for step in known[name].steps}

    out: list[dict] = []
    for row in list_datasets(config=cfg).iter_rows(named=True):
        name = row["dataset"]
        if allowed is not None and name not in allowed:
            continue
        spec = DATASETS[name]
        if name == "flash_news_wire":
            # A compatibility projection of news_headlines, not an independent
            # source or watermark. Its canonical fetch is scheduled once.
            continue
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
            stale = is_stale(name, mark, anchor, cfg)
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
    from cnequity.orchestrator.cadence import group_is_runnable

    return group_is_runnable(cfg, group)


def _warn_schedule_drift(config_path: str) -> None:
    """One stderr line when this config schedules less than the installed release.

    A release that adds a step to a group cannot reach a config written by an
    older one; the day still "succeeds" without it. Advisory only: a config
    that cannot be compared must not stop the day.
    """
    from cnequity.cli._shared import resolve_config_path
    from cnequity.config.drift import config_drift

    try:
        drift = config_drift(resolve_config_path(config_path))
    except Exception:  # noqa: BLE001 — never block the run on the advisory
        return
    missing = set(drift.unscheduled_steps).union(*drift.group_steps.values())
    if missing:
        click.echo(
            _(
                "Note: the config has {value} fewer schedule steps than this version ({value2}); they will not run this time. Run `cne config upgrade` to add them."
            ).format(value=len(missing), value2=", ".join(sorted(missing))),
            err=True,
        )


def _run_all_groups(
    cfg,
    engine: JobEngine,
    td: date | None,
    *,
    backfill: bool,
    repairs: list,
    include_events: bool = False,
    audit_after: bool = False,
    only: list[str] | None = None,
):
    """Run every schedule group in config order, one at a time.

    A day's ingestion is six groups, and until now the only thing that ran all
    six was `scripts/scheduler/daily_pipeline.sh` — which is not installed by the PyPI
    package. So the documented answer for anyone who had only `pip install`
    was six cron lines, and the single command they reached for instead
    (`cne run daily`) silently covered a third of the lake.

    One group failing does not stop the rest, exactly as the script does it:
    the point is to get as much of the day as the sources will give. The exit
    code is the worst of them.

    With *audit_after* the lake audit runs once after the groups — it reads
    the whole lake, so inside any one group it would judge data the later
    groups had not landed yet. With *include_events* the natural-calendar
    event stream runs last, so one command covers the whole day — on a weekend
    the groups skip and the events still run.
    """
    if not cfg.schedule_groups:
        raise click.ClickException(
            _(
                "{config} has no [job.daily.groups]; a config written by `cne config create` "
                "has these schedule groups."
            ).format(config=getattr(cfg, "config_path", None) or _("This config"))
        )
    results: list[dict] = []
    worst = 0
    from cnequity.orchestrator.cadence import due_steps

    session = _settled_session(cfg, td)
    schedule_date = session or _last_trading_day(cfg, shanghai_today())
    selected = (
        cfg.schedule_groups.items()
        if only is None
        else [(name, cfg.schedule_groups[name]) for name in only if name in cfg.schedule_groups]
    )
    if only is not None:
        missing = [name for name in only if name not in cfg.schedule_groups]
        if missing:
            click.echo(
                _("These schedule groups are not in the config: ") + _(", ").join(missing), err=True
            )
        if not selected:
            click.echo(json.dumps({"groups": [], "repairs": repairs}, indent=2))
            return
    for name, group in selected:
        if not _group_is_runnable(cfg, group):
            click.echo(
                _("Schedule group {name}: skipped (every dataset it fetches is turned off)").format(
                    name=name
                ),
                err=True,
            )
            results.append({"group": name, "status": "skipped_disabled"})
            continue
        steps = group.steps if backfill else due_steps(group, schedule_date)
        if not steps:
            results.append(
                {"group": name, "status": "skipped_not_scheduled", "cadence": group.cadence}
            )
            click.echo(
                _("Schedule group {name}: skipped (not yet a {cadence} collection day)").format(
                    name=name, cadence=group.cadence
                ),
                err=True,
            )
            continue
        try:
            result = engine.run_job(
                f"daily:{name}",
                trade_date=session,
                waves=[
                    WaveConfig(
                        name=f"group:{name}",
                        parallel=getattr(group, "parallel", True),
                        steps=steps,
                    )
                ],
                backfill=backfill,
            )
        except RunLockError as exc:
            # Another daily job holds the ingestion lock. That is a reason to
            # stop, not to walk the remaining groups into the same wall.
            raise click.ClickException(str(exc)) from exc
        except Exception as exc:  # noqa: BLE001 — one group must not sink the day
            click.echo(
                _("Schedule group {name}: {name2}: {exc}").format(
                    name=name, name2=type(exc).__name__, exc=exc
                ),
                err=True,
            )
            results.append({"group": name, "status": "failed", "error": str(exc)})
            worst = 1
            continue
        status = result["status"]
        results.append({"group": name, **result})
        click.echo(_("Schedule group {name}: {status}").format(name=name, status=status), err=True)
        exit_code = _run_status_exit_code(status)
        # Exit 1 is a terminal/core failure; 2 is usable but degraded.
        # Numeric max would hide a core failure behind a later advisory one.
        if exit_code == 1 or worst == 0:
            worst = exit_code
    payload: dict = {"groups": results, "repairs": repairs}
    ran = [r for r in results if not str(r.get("status", "")).startswith("skipped")]
    if audit_after and ran:
        try:
            audit = engine.run_job(
                "daily:audit",
                trade_date=session,
                waves=[WaveConfig(name="audit", parallel=False, steps=["audit"])],
                backfill=backfill,
            )
        except RunLockError as exc:
            raise click.ClickException(str(exc)) from exc
        except Exception as exc:  # noqa: BLE001 — the groups already ran
            click.echo(_("Audit: {name}: {exc}").format(name=type(exc).__name__, exc=exc), err=True)
            audit = {"status": "failed", "error": str(exc)}
        payload["audit"] = audit
        click.echo(_("Audit: {status}").format(status=audit["status"]), err=True)
        exit_code = _run_status_exit_code(audit["status"])
        if exit_code == 1 or worst == 0:
            worst = exit_code
    if include_events and cfg.events_groups:
        try:
            events = _run_events_job(cfg, engine, td)
        except RunLockError as exc:
            # A separately scheduled `cne run events` is already covering it.
            click.echo(_("Event streams: skipped ({exc})").format(exc=exc), err=True)
            events = {"status": "skipped_locked", "error": str(exc)}
        except Exception as exc:  # noqa: BLE001 — the groups already ran
            click.echo(
                _("Event streams: {name}: {exc}").format(name=type(exc).__name__, exc=exc), err=True
            )
            events = {"status": "failed", "error": str(exc)}
        payload["events"] = events
        click.echo(_("Event streams: {status}").format(status=events["status"]), err=True)
        exit_code = (
            0 if events["status"] == "skipped_locked" else _run_status_exit_code(events["status"])
        )
        if exit_code == 1 or worst == 0:
            worst = exit_code
    attach_revision_retention(cfg, payload)
    click.echo(json.dumps(payload, indent=2, default=str))
    if worst:
        raise SystemExit(worst)


def _settled_session(cfg, td: date | None) -> date | None:
    """The session the schedule groups update when no ``--trade-date`` is given.

    Today once its session is final, and before 15:05 Beijing time on a
    trading day the previous session. A manual run at 01:00 on a Monday used
    to target Monday itself: ``daily_bars`` refused the unfinished session with
    a traceback, while snapshot feeds stamped Friday's numbers with Monday's
    date. ``None`` keeps the engine's default of today, so a weekend or holiday
    is still skipped as a non-trading day.
    """
    if td is not None:
        return td
    from cnequity.steps.common import is_trading_day

    today = shanghai_today()
    if is_session_final(today) or not is_trading_day(cfg, today):
        return None
    settled = _last_trading_day(cfg, today)
    click.echo(
        _(
            "Today's session ({today}) is not final until 15:05 Beijing time; the schedule groups update through {settled}."
        ).format(today=today.isoformat(), settled=settled.isoformat()),
        err=True,
    )
    return settled


def _run_stale_only(
    cfg,
    engine,
    trade_date: date | None,
    *,
    backfill: bool,
    repair_gaps: bool = False,
    groups: set[str] | None = None,
    snapshots_only: bool = False,
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
        raise click.ClickException(
            _("With --groups set, the lake-wide --repair-gaps cannot be used")
        )
    repairs = _auto_repair_gaps(cfg, anchor) if repair_gaps else []
    plan = (
        stale_fetch_plan(cfg, anchor)
        if groups is None
        else stale_fetch_plan(cfg, anchor, groups=groups)
    )
    if snapshots_only:
        # History datasets catch up by date on the next daily run; only a
        # snapshot's missed session is lost for good, so only those are retried.
        from cnequity.domain.datasets import DATASETS

        plan = [item for item in plan if DATASETS[item["dataset"]].fetch_semantics == "snapshot"]
    steps = [item["dataset"] for item in plan]
    if not plan:
        click.echo(_("No datasets lag as of {value}").format(value=anchor.isoformat()))
        click.echo(
            json.dumps(
                {"anchor": anchor.isoformat(), "repairs": repairs, "status": "nothing_stale"},
                indent=2,
            )
        )
        return
    click.echo(
        _("Datasets lagging as of {value}: {value2}").format(
            value=anchor.isoformat(), value2=", ".join(steps)
        ),
        err=True,
    )
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
    payload = {
        "run_id": result["run_id"],
        "status": result["status"],
        "stale_plan": plan,
        "repairs": repairs,
        "backfill_applied": effective_backfill,
    }
    attach_revision_retention(cfg, payload)
    click.echo(json.dumps(payload, indent=2))
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
    help=N_("Restrict --stale-only to these daily schedule groups (comma- or space-separated)."),
)
@click.option(
    "--group",
    "group_name",
    default=None,
    help=(
        N_(
            "Schedule groups: core, capital, signals, fundamentals, macro_risk, research, intraday, ticks"
        )
    ),
)
@click.option(
    "--all-groups",
    "all_groups",
    is_flag=True,
    help=(
        N_(
            "Run all schedule groups only, without the event streams. Kept for old schedules that run `cne run events` separately; `cne run daily` without arguments already includes all schedule groups and event streams."
        )
    ),
)
@click.option(
    "--core-only",
    "core_only",
    is_flag=True,
    help=N_("Run only the [[job.daily.waves]] core skeleton (the old no-argument behavior)."),
)
@click.option(
    "--no-events",
    "no_events",
    is_flag=True,
    help=N_(
        "When run without arguments, skip the event streams (announcements, regulatory events, news)."
    ),
)
@click.option(
    "--trade-date",
    "trade_date_str",
    default=None,
    help=N_(
        "As-of trading day YYYY-MM-DD (default today). Use for catch-up runs on weekends / holidays."
    ),
)
@click.option(
    "--backfill",
    is_flag=True,
    help=(
        N_(
            "Run with backfill semantics: skip the trading-day gate and each step's own incremental window, and fetch the window given by the backfill scope in the config instead. Used to rerun a day the schedule missed; scheduled runs never pass it."
        )
    ),
)
@click.option(
    "--repair-gaps",
    is_flag=True,
    help=N_(
        "Before the daily / stale run, repair verified historical gaps that have an honest source."
    ),
)
@click.option(
    "--quiet", is_flag=True, help=N_("Show only warnings and above; no per-step progress.")
)
@click.option(
    "--stale-only",
    is_flag=True,
    help=N_(
        "Refetch only datasets still behind the last trading day. Run it a few hours after the main pipeline: once a snapshot dataset loses the day's window to a source outage, it cannot be recovered the next day."
    ),
)
@click.option(
    "--snapshots-only",
    is_flag=True,
    help=N_(
        "With --stale-only: refetch only snapshot datasets; historical datasets are filled by date in the next daily update."
    ),
)
@click.option(
    "--pack",
    "pack_names",
    multiple=True,
    type=click.Choice(["market", "fundamentals", "universe"]),
    help=(
        N_(
            "Run only the daily schedule groups for these research packs, without the event streams. If omitted, all schedule groups and event streams still run. Repeatable."
        )
    ),
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
    snapshots_only: bool = False,
    core_only: bool = False,
    no_events: bool = False,
    pack_names: tuple[str, ...] = (),
):
    """Run one day's update: every daily schedule group, then the event streams.

    \b
    With no arguments it runs every enabled schedule group in config order (continuing if a group fails), audits the whole lake once,
    then runs the event streams (announcements, regulatory events, news; weekends and holidays included). Schedule groups skip
    non-trading days automatically while event streams still run.
    `--group` runs one group, `--core-only` only the core backbone, `--no-events` skips the event streams.

    """
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
    if (
        getattr(cfg, "lake_profile", None) in {"demo", "sample"}
        and not (getattr(cfg, "daily_waves", None) or [])
        and not getattr(cfg, "schedule_groups", None)
    ):
        # Every mode is refused, --stale-only included: judged against the
        # whole registry it fetched market-wide snapshots into a 2-symbol lake.
        raise click.ClickException(
            _(
                "{config_path} belongs to a {profile} lake, which has no daily schedule: a few symbols are not worth updating. Build a lake `cne run daily` can keep current with `cne init`, which writes the per-user config."
            ).format(config_path=config_path, profile=cfg.lake_profile)
        )
    if snapshots_only and not stale_only:
        raise click.ClickException(_("--snapshots-only only works with --stale-only"))
    if stale_groups is not None and not stale_only:
        raise click.ClickException(
            _("--groups only works with --stale-only; for an ordinary run use --group")
        )
    if all_groups and group_name:
        raise click.ClickException(_("--all-groups runs every schedule group; drop --group."))
    if core_only and (group_name or all_groups or stale_only):
        raise click.ClickException(
            _(
                "--core-only runs only the core backbone and cannot be combined with --group / --all-groups / --stale-only."
            )
        )
    if pack_names and (group_name or all_groups or stale_only or core_only):
        raise click.ClickException(
            _(
                "--pack only selects research packs' schedule groups and cannot be combined with --group / --all-groups / --stale-only / --core-only."
            )
        )
    if no_events and (group_name or all_groups or stale_only or core_only):
        raise click.ClickException(
            _(
                "--no-events only applies to a full daily update without --group / --all-groups / --stale-only / --core-only."
            )
        )
    if all_groups and stale_only:
        raise click.ClickException(_("--stale-only picks its own steps; drop --all-groups."))
    if stale_only:
        if group_name:
            raise click.ClickException(_("--stale-only picks its own steps; drop --group."))
        groups = None
        if stale_groups is not None:
            groups = set(stale_groups.replace(",", " ").split())
            if not groups:
                raise click.ClickException(_("--groups cannot be empty"))
        _run_stale_only(
            cfg,
            engine,
            td,
            backfill=backfill,
            repair_gaps=repair_gaps,
            groups=groups,
            snapshots_only=snapshots_only,
        )
        return
    repairs = []
    if repair_gaps:
        repairs = _auto_repair_gaps(cfg, _last_trading_day(cfg, td or shanghai_today()))
    if all_groups:
        _run_all_groups(cfg, engine, td, backfill=backfill, repairs=repairs)
        return
    if pack_names:
        from cnequity.research.packs import SNAPSHOT_NOTE, groups_for, snapshot_steps

        selected_groups = groups_for(pack_names)
        click.echo(_(SNAPSHOT_NOTE), err=True)
        snapshots = snapshot_steps(cfg, selected_groups)
        if snapshots:
            click.echo(_("Lost for good if a day is missed: ") + _(", ").join(snapshots), err=True)
        if not selected_groups:
            click.echo(
                _(
                    "These research packs have no daily schedule groups. For historical ST use `cne backfill trading_status`."
                ),
                err=True,
            )
            click.echo(json.dumps({"groups": [], "repairs": repairs}, indent=2))
            return
        _run_all_groups(
            cfg,
            engine,
            td,
            backfill=backfill,
            repairs=repairs,
            include_events=False,
            audit_after=True,
            only=selected_groups,
        )
        return
    if not group_name and not core_only and cfg.schedule_groups:
        _warn_schedule_drift(config_path)
        _run_all_groups(
            cfg,
            engine,
            td,
            backfill=backfill,
            repairs=repairs,
            # A --backfill replay re-fetches one trading day; the event stream
            # follows the natural calendar and has no such replay.
            include_events=not (no_events or backfill),
            audit_after=True,
        )
        return
    session = _settled_session(cfg, td)
    try:
        if group_name:
            group = cfg.schedule_groups.get(group_name)
            if not group:
                known = ", ".join(sorted(cfg.schedule_groups))
                raise click.ClickException(
                    _("Unknown schedule group: {group_name} (the config has: {known})").format(
                        group_name=group_name, known=known
                    )
                    if known
                    else _(
                        "Unknown schedule group: {group_name}; this config has no [job.daily.groups] at all. A config written by `cne config create` has them; one written by `cne init --profile demo|sample` deliberately does not, because a demo lake is not a market."
                    ).format(group_name=group_name)
                )
            from cnequity.orchestrator.cadence import due_steps

            steps = group.steps
            if not backfill:
                steps = due_steps(group, session or _last_trading_day(cfg, shanghai_today()))
            if not steps:
                click.echo(
                    json.dumps(
                        {
                            "run_id": None,
                            "status": "skipped_not_scheduled",
                            "group": group_name,
                            "cadence": group.cadence,
                        },
                        indent=2,
                    )
                )
                return
            result = engine.run_job(
                f"daily:{group_name}",
                trade_date=session,
                waves=[
                    WaveConfig(
                        name=f"group:{group_name}",
                        parallel=getattr(group, "parallel", True),
                        steps=steps,
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
                    _(
                        "Run without --core-only, or run a schedule group instead: `cne run daily --group <name>` (available: {groups})."
                    ).format(groups=groups)
                    if groups
                    else _("A config written by `cne config create` has both.")
                )
                raise click.ClickException(
                    _(
                        "{config_path} has no [[job.daily.waves]]: without schedule groups `cne run daily` runs this wave DAG, and this config defines none, so a run would fetch nothing and still report success. {remedy}"
                    ).format(config_path=config_path, remedy=remedy)
                )
            result = engine.run_job("daily", trade_date=session, backfill=backfill)
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
                    _(
                        "Note: this pass ran only the core backbone; {value} more enabled datasets belong to schedule groups and were not updated this time: {preview}{suffix}. Drop --core-only to update them too (schedule groups: {value2})."
                    ).format(
                        value=len(uncovered),
                        preview=preview,
                        suffix=suffix,
                        value2=", ".join(sorted(cfg.schedule_groups)),
                    ),
                    err=True,
                )
    except RunLockError as exc:
        raise click.ClickException(str(exc)) from exc
    result = {**result, "repairs": repairs}
    attach_revision_retention(cfg, result)
    click.echo(json.dumps(result, indent=2, default=str))
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
    help=N_("One group from [job.events.groups] (default: run all in order)."),
)
@click.option(
    "--trade-date",
    "trade_date_str",
    default=None,
    help=N_("As-of calendar date YYYY-MM-DD (default today), including weekends and holidays."),
)
@click.option(
    "--quiet", is_flag=True, help=N_("Show only warnings and above; no per-step progress.")
)
def run_events(config_path: str, group_name: str | None, trade_date_str: str | None, quiet: bool):
    """Run the continuous event streams by calendar day (announcements, news).

    \b
    These sources publish on weekends and holidays too, so this job is not gated on trading days and takes its own ingest lock:
    it neither waits for the evening batch nor gets skipped because the market is closed. Each group publishes its staging data
    through its own `compact`. A bare `cne run daily` already ends with this step; run it alone to refresh a group more often.

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
            _("This config has no [job.events.groups]; see configs/cnequity.example.toml")
        )
    if group_name and group_name not in cfg.events_groups:
        known = ", ".join(sorted(cfg.events_groups))
        raise click.ClickException(
            _("Unknown events schedule group: {group_name} (the config has: {known})").format(
                group_name=group_name, known=known
            )
        )

    engine = JobEngine(cfg)
    td = parse_date_option(trade_date_str, "--trade-date")
    try:
        result = _run_events_job(cfg, engine, td, group_name)
    except RunLockError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(result, indent=2, default=str))
    exit_code = _run_status_exit_code(result["status"])
    if exit_code:
        raise SystemExit(exit_code)


def _run_events_job(cfg, engine: JobEngine, td: date | None, group_name: str | None = None) -> dict:
    """One events run over *group_name*, or every events group in config order."""
    selected = (
        [(group_name, cfg.events_groups[group_name])]
        if group_name
        else list(cfg.events_groups.items())
    )
    return engine.run_job(
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


def _retry_single_run(engine: JobEngine, run_id: str) -> dict:
    """Retry one run, print its result, and return it to the CLI caller."""
    record = engine.manifest.get_run(run_id)
    if record is None:
        raise click.ClickException(_("Unknown run_id: {run_id}").format(run_id=run_id))
    if dict(record).get("status") == "success":
        # Retrying re-runs the whole job and could only turn a finished run
        # into a failed one, so say there is nothing to do.
        click.echo(
            _("Run {run_id} ({job_name}) already succeeded; nothing to retry.").format(
                run_id=run_id, job_name=record["job_name"]
            )
        )
        return {"run_id": run_id, "status": "success", "retried": 0}
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
@click.option("--run-id", default=None, help=N_("Retry the given run."))
@click.option(
    "--failed-groups",
    is_flag=True,
    help=N_("Retry the most recent failed run of each daily schedule group."),
)
def retry(config_path: str, run_id: str | None, failed_groups: bool):
    """Retry one run, or the latest failed run of each daily schedule group."""
    _progress_logging()
    cfg = _cfg(config_path)
    attach_log_file(cfg, "run-retry")
    engine = JobEngine(cfg)
    if failed_groups:
        if run_id:
            raise click.ClickException(_("Use only one of --run-id and --failed-groups"))
        runs = _failed_daily_group_runs(engine)
        if not runs:
            click.echo(_("No failed daily schedule-group runs to retry."))
            return
        failed = False
        for record in runs:
            click.echo(
                _("Retrying failed daily schedule-group run {run_id} ({job_name})").format(
                    run_id=record["run_id"], job_name=record["job_name"]
                )
            )
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
        raise click.ClickException(_("Give --run-id or --failed-groups"))
    result = _retry_single_run(engine, run_id)
    exit_code = _run_status_exit_code(str(result.get("status", "failed")))
    if exit_code:
        raise SystemExit(exit_code)
