"""Is the lake right, and can it prove it: `audit`, `verify`, `status`,
`stability`, `sources`, `source`.

`audit` asks whether what landed is correct; `verify` asks whether what should
have landed, landed. Different failures, so different commands.
"""

from __future__ import annotations

import json
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

import click
import polars as pl

from cnequity.cli._root import cli
from cnequity.cli._shared import (
    _cfg,
    _progress_logging,
    attach_log_file,
    comma_option,
    comma_values,
    config_option,
    ingest_scope_label,
    parse_date_option,
    resolve_config_path,
)
from cnequity.cli.backfill_cmds import _require_known_dataset, _run_backfill
from cnequity.domain.market_profile import BSE_FIRST_SESSION
from cnequity.domain.market_time import is_session_final, shanghai_today
from cnequity.i18n import N_, _, localize
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


#: Datasets whose expected tip key set is "every instrument active that
#: session", and which can therefore be proved against `instruments` alone.
#:
#: Deliberately short. `index_bars` is keyed by an index universe and
#: `financial_statement_items` by report period, so holding either to the
#: instrument list would invent a gap rather than find one — a cross-section
#: proof is only worth having while the expectation it checks is real.
INSTRUMENT_SCOPED_DATASETS = ("daily_bars", "trading_status")


def _gates_on_dataset(
    cfg, dataset: str, wanted_groups: set[str] | None, *, core_only: bool = False
) -> bool:
    """Whether a failure in *dataset* should fail this host's gate.

    With *core_only*, only a core dataset (``DatasetSpec.support``) can.

    ``--groups`` names the schedule groups this machine actually runs. A
    dataset nothing schedules still gates: "I cannot tell who fetches this" is
    not the same claim as "another host fetches it". A dataset the config took
    out of its groups on purpose (``steps_remove``, ``enabled = false``) is the
    one exception: the operator has said this host does not fetch it.
    """
    if core_only and not _is_core(dataset):
        return False
    if wanted_groups is None:
        return True
    by_group = stale_datasets_by_group(cfg, [dataset])
    owners = {group for group, names in by_group.items() if dataset in names}
    from cnequity.domain.datasets import removed_from_schedule

    if removed_from_schedule(cfg, dataset):
        return False
    if not owners:
        return True
    return any(group in wanted_groups or group == "(unscheduled)" for group in owners)


def _is_core(dataset: str) -> bool:
    """A core dataset, or a name the registry does not know (judged as before)."""
    from cnequity.domain.datasets import DATASETS, core_dataset_names

    return dataset not in DATASETS or dataset in core_dataset_names()


def _scope_exit_code(
    incomplete_init: bool, *, scope_incomplete: bool, scope_unverified: bool
) -> int:
    """Exit code for the coverage gates: 1 proven bad, 2 unprovable, 0 fine.

    Separated because they call for different actions. "5,207 securities have
    no bar" is a repair; "instruments is missing, so I cannot check" is a
    setup problem, and a caller that treats both as a red light learns to
    ignore both. 2 is already this command's spelling for degraded.
    """
    if incomplete_init or scope_incomplete:
        return 1
    return 2 if scope_unverified else 0


def _owed_symbols_on(cfg, dataset: str, day: date) -> set[str]:
    """Symbols the outstanding-key ledger already owes for *day*."""
    from cnequity.storage.state import StateStore

    try:
        rows = StateStore(cfg.meta_root).get_outstanding_keys(dataset)
    except Exception:  # noqa: BLE001 — a missing/garbled state file owes nothing
        return set()
    wanted = day.isoformat()
    return {
        str(row["symbol"])
        for row in rows
        if isinstance(row, dict) and row.get("symbol") and row.get("trade_date") == wanted
    }


def _proven_absent_on(
    cfg, dataset: str, day: date, *, undated: set[str] | frozenset[str] = frozenset()
) -> set[str]:
    """Symbols with live no-data evidence covering *day*.

    The ingest side already routes these out of the expected key set
    (``classify_daily_bar_ownership``); the proof has to honour the same
    evidence or the two contradict each other. A code the daily_bars probe
    found not yet listed has no trading status to publish either, so that
    claim also answers for ``trading_status``.

    So does any other no-data record for a code in *undated* (no listing
    date) that has never printed a traded bar: such a code cannot be
    suspended, only not yet listed. The probe writes ``not_yet_listed`` for
    the one day it asks; the next runs reuse that cached absence under a
    generic ``source_empty`` instead of asking again, and 001381.SZ read as a
    missing active stock on 2026-10-26, three days after its IPO allotment.
    """
    from cnequity.steps.common import (
        load_bar_universe,
        load_negative_evidence,
        negative_evidence_covers,
    )

    try:
        evidence = list(load_negative_evidence(cfg, "daily_bars"))
    except Exception:  # noqa: BLE001 — missing/garbled evidence proves nothing
        return set()
    covering = {
        str(item["symbol"]).strip().upper(): item
        for item in evidence
        if item.get("symbol") and negative_evidence_covers(item, str(item["symbol"]), day, day)
    }
    if dataset == "daily_bars":
        return set(covering)
    proven = {symbol for symbol, item in covering.items() if item.get("reason") == "not_yet_listed"}
    candidates = (set(covering) & set(undated)) - proven
    if candidates:
        try:
            traded = load_bar_universe(cfg)
        except Exception:  # noqa: BLE001 — without the universe nothing is proven
            traded = None
        if traded is not None:
            proven |= candidates - traded
    return proven


def _tip_scope(cfg, catalog: pl.DataFrame, dataset: str = "daily_bars") -> dict | None:
    """Cheap cross-section proof for a date-fresh daily-bars watermark.

    Freshness is one-dimensional: a one-symbol repair can advance a date just
    as a market-wide sweep can.  For a real lake, compare the tip partition to
    the instruments active that day. A bar proves an observed symbol; an
    explicit non-trading status proves why an active symbol has no bar.
    """
    if getattr(cfg, "lake_profile", None) in {"demo", "sample"}:
        return None
    rows = catalog.filter(pl.col("dataset") == dataset)
    if rows.is_empty() or not bool(rows["has_data"][0]):
        return None

    from cnequity.domain.symbols import filter_ingest_universe
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
    from cnequity.steps.common import load_curated_instruments, load_curated_trading_status

    bars_root = cfg.curated_root / dataset
    if not dataset_has_parquet(bars_root, dataset=dataset, meta_root=cfg.meta_root):
        # Test doubles and legacy catalog adapters can report rows without a
        # physical lake. They have no scope evidence to inspect here.
        return None
    raw_day = rows["watermark"][0] if bool(rows["watermarked"][0]) else None
    raw_day = raw_day or rows["coverage_end"][0]
    if raw_day is None:
        return {
            "dataset": dataset,
            "state": "unverified",
            "message": f"{dataset} has data but no watermark/coverage_end usable for the cross-section check",
        }
    tip = raw_day.date() if hasattr(raw_day, "date") and not isinstance(raw_day, date) else raw_day

    try:
        instruments = load_curated_instruments(cfg)
        if instruments is None or "symbol" not in instruments.columns:
            return {
                "dataset": dataset,
                "state": "unverified",
                "date": tip,
                "message": f"{dataset} has data but instruments is missing, so the whole-market symbol scope cannot be proven",
            }
        instrument_count = instruments["symbol"].drop_nulls().n_unique()
        instrument_receipt = Manifest(cfg.manifest_path).latest_successful_batch("instruments")
        receipt_count = (
            int(instrument_receipt["rows_written"] or 0) if instrument_receipt is not None else 0
        )
        if receipt_count and instrument_count < receipt_count:
            return {
                "dataset": dataset,
                "state": "unverified",
                "date": tip,
                "message": (
                    f"The current instruments has only {instrument_count} symbols, fewer than the {receipt_count} the latest successful batch wrote; the security table may be truncated"
                ),
            }
        expected_rows = instruments
        if "list_date" in expected_rows.columns:
            expected_rows = expected_rows.filter(
                pl.col("list_date").is_null() | (pl.col("list_date") <= tip)
            )
        if "delist_date" in expected_rows.columns:
            expected_rows = expected_rows.filter(
                pl.col("delist_date").is_null() | (pl.col("delist_date") >= tip)
            )
        expected = set(
            filter_ingest_universe(
                expected_rows["symbol"].drop_nulls().to_list(),
                cfg.ingest_universe,
            )
        )
        if not expected:
            return {
                "dataset": dataset,
                "state": "unverified",
                "date": tip,
                "message": "instruments has no active symbols to check on that date",
            }
        exchanges = {symbol.rsplit(".", 1)[-1] for symbol in expected if "." in symbol}
        required_exchanges = {"SH", "SZ"}
        if cfg.ingest_universe in {"all_a", "all_instruments"} and tip >= BSE_FIRST_SESSION:
            required_exchanges.add("BJ")
        missing_exchanges = sorted(required_exchanges - exchanges)
        if missing_exchanges:
            return {
                "dataset": dataset,
                "state": "unverified",
                "date": tip,
                "message": (
                    f"instruments lacks markets the configured scope should have: {', '.join(missing_exchanges)}; a truncated security table cannot prove the daily cross-section complete"
                ),
            }

        bars = scan_parquet_root(
            bars_root,
            partition_col="trade_date",
            start=tip,
            end=tip,
            dataset=dataset,
            meta_root=cfg.meta_root,
        )
        observed = set(
            bars.filter(pl.col("trade_date") == tip)
            .select("symbol")
            .unique()
            .collect(engine="streaming")["symbol"]
            .to_list()
        )
        status = load_curated_trading_status(cfg, start=tip, end=tip, symbols=sorted(expected))
        excused: set[str] = set()
        if status is not None and not status.is_empty():
            excused = set(status.filter(~pl.col("is_trading"))["symbol"].drop_nulls().to_list())
        undated = (
            set(
                expected_rows.filter(pl.col("list_date").is_null())["symbol"].drop_nulls().to_list()
            )
            if "list_date" in expected_rows.columns
            else set()
        )
        excused |= _proven_absent_on(cfg, dataset, tip, undated=undated)
        covered = expected & (observed | excused)
        ratio = len(covered) / len(expected)
        # Keys the ingest side already judged, tolerated and wrote down are
        # not news. `daily_bars` carries an explicit unresolved-key budget —
        # refusing a checkpoint over 14 of 5,500 threw away two hours of
        # `cne init` — and every key it lets through is recorded in the
        # outstanding ledger and reported separately below. Failing here for
        # those same keys would be the two halves of one system contradicting
        # each other, with the operator in between.
        owed = _owed_symbols_on(cfg, dataset, tip)
        missing = sorted(expected - covered)
        unexplained = sorted(set(missing) - owed)
        return {
            "dataset": dataset,
            # A proof, not a sample: a symbol counts as covered only with a bar,
            # explicit non-trading evidence, or a recorded debt. There is no
            # percentage tolerance, which in all-A could hide hundreds of names.
            "state": "complete" if not unexplained else "incomplete",
            "date": tip,
            "covered": len(covered),
            "expected": len(expected),
            "ratio": ratio,
            "missing": unexplained,
            "owed": len(set(missing) & owed),
        }
    except (OSError, ValueError, pl.exceptions.PolarsError) as exc:
        return {
            "dataset": dataset,
            "state": "unverified",
            "date": tip,
            "message": f"Failed to read cross-section evidence: {exc}",
        }


@cli.command()
@config_option
@click.option(
    "--run-id",
    multiple=True,
    help=N_("Run ID; repeat to merge evidence from these runs into one audit."),
)
@click.option(
    "--full",
    "full",
    is_flag=True,
    help=N_(
        "Health snapshot of the whole lake (current state + freshness), not the files of a single run."
    ),
)
@click.option(
    "--research-start",
    default=None,
    help=N_("Strictly validate the research window starting on this date (requires --full)."),
)
@click.option(
    "--quality-only",
    is_flag=True,
    help=N_(
        "With --full: gate only on quality errors; check schedule freshness separately with status."
    ),
)
@click.option(
    "--research-end",
    default=None,
    help=N_(
        "End of the research window (defaults to the latest daily_bars; requires --research-start)."
    ),
)
@click.option(
    "--research-universe",
    type=click.Choice(["all_a", "all_a_sh_sz"]),
    default="all_a",
    show_default=True,
    help=N_("Historical research universe checked by --full."),
)
def audit(
    config_path: str,
    run_id: tuple[str, ...],
    full: bool,
    research_start: str | None,
    research_end: str | None,
    research_universe: str,
    quality_only: bool = False,
):
    """Run the quality audit; add --full for a health snapshot of the whole lake as it is now.

    \b
    A bare `cne run daily` already audits once after all schedule groups (with `--core-only`, as the last step of the `finalize` wave),
    so running it here re-audits data that has already been audited; the repository's health_notify.sh does not go through `run daily` and audits once itself.
    What is not scheduled is `--full`: it judges the state of the lake right now rather than what one run wrote,
    and it is what the health check and the dashboard read.

    """
    cfg = _cfg(config_path)
    attach_log_file(cfg, "audit")

    if quality_only and not full:
        raise click.ClickException(_("--quality-only needs --full"))

    if research_start and not full:
        raise click.ClickException(_("--research-start needs --full"))
    if research_end and not research_start:
        raise click.ClickException(_("--research-end needs --research-start"))

    if full:
        from cnequity.quality.audit import lake_health

        start_date = parse_date_option(research_start, "--research-start")
        end_date = parse_date_option(research_end, "--research-end")
        if start_date and end_date and start_date > end_date:
            raise click.ClickException(_("--research-start must be on or before --research-end"))
        health = lake_health(
            cfg,
            shanghai_today(),
            research_start=start_date,
            research_end=end_date,
            research_universe=research_universe,
        )
        sev = health["findings_by_severity"]
        click.echo(
            _("Lake health @ last trading day {last_trading_day}").format(
                last_trading_day=health["last_trading_day"]
            )
        )
        click.echo(
            _("  findings: {value} error, {value2} warning, {value3} info").format(
                value=sev.get("error", 0), value2=sev.get("warning", 0), value3=sev.get("info", 0)
            )
        )
        if getattr(cfg, "lake_profile", None) == "sample":
            click.echo(
                _(
                    "  Sample lake: source=mock is the expected evidence of this offline profile and does not count toward the health gate; it is still not usable for research."
                )
            )
            if sev.get("error", 0):
                click.echo(
                    _(
                        "  Note: the remaining {error} errors are outside what synthetic data is expected to produce."
                    ).format(error=sev["error"]),
                    err=True,
                )
        if health["empty_datasets"]:
            if getattr(cfg, "lake_profile", None) in {"demo", "sample"}:
                click.echo(
                    _(
                        "  {value} registered datasets hold no data, as expected for a {profile} lake"
                    ).format(value=len(health["empty_datasets"]), profile=cfg.lake_profile)
                )
            else:
                click.echo(
                    _("  Empty datasets: {value}").format(value=", ".join(health["empty_datasets"]))
                )
        if health.get("expected_empty_datasets"):
            click.echo(
                _("  Datasets expected to be empty: {value}").format(
                    value=", ".join(health["expected_empty_datasets"])
                )
            )
        if health["stale_datasets"]:
            click.echo(
                _("  STALE datasets: {value}").format(value=", ".join(health["stale_datasets"]))
            )
        for f in health["error_findings"]:
            click.echo(f"  [error]   {f.get('dataset', ''):22} {f.get('message', '')}")
        for f in health["warning_findings"]:
            click.echo(f"  [warning] {f.get('dataset', ''):22} {f.get('message', '')}")
        for f in health.get("info_findings", []):
            if f.get("source_limited"):
                click.echo(f"  [info]    {f.get('dataset', ''):22} {f.get('message', '')}")
        validity = health["historical_universe_validity"]
        small_lake = getattr(cfg, "lake_profile", None) in {"demo", "sample"}
        if small_lake:
            # Research readiness is judged on a full-market lake; a lake of a few
            # symbols is never ready for it, so the verdict would only alarm.
            click.echo(
                _("  Research readiness is not assessed for a {profile} lake").format(
                    profile=cfg.lake_profile
                )
            )
        else:
            research_state = "READY" if validity["universe_ready"] else "BLOCKED"
            universe_label = (
                "all-A"
                if validity.get("universe", research_universe) == "all_a"
                else validity.get("universe", research_universe)
            )
            click.echo(
                _("  History {universe_label} {start}..{end}: {research_state}").format(
                    universe_label=universe_label,
                    start=validity["window"]["start"],
                    end=validity["window"]["end"],
                    research_state=research_state,
                )
            )
            supported = validity.get("supported_window")
            # Only worth a line when it adds something: a READY gate with no
            # computed window (a caller passing its own manifest) would otherwise
            # print a contradiction.
            if supported:
                click.echo(
                    _(
                        "  Endorsable window: {start}..{end} (the longest range the evidence supports; a research window inside it passes)"
                    ).format(start=supported["start"], end=supported["end"])
                )
            elif not validity["universe_ready"]:
                detail = validity.get("checks", {}).get("historical_st_labels", {})
                blocked_by = detail.get("supported_window_blocked_by")
                per_source = detail.get("supported_window_by_source") or {}
                offered = _(", ").join(
                    f"{name} {item['start']}..{item['end']}"
                    for name, item in sorted(per_source.items())
                )
                short = detail.get("supported_window_missing_symbols")
                total = detail.get("supported_window_current_symbols")
                near = (
                    _(", the closest receipt is short {short}/{total} symbols").format(
                        short=short, total=total
                    )
                    if short and total
                    else ""
                )
                click.echo(
                    _("  Endorsable window: none")
                    + (
                        _(
                            " ({blocked_by} has no receipt covering the current stock pool yet{near})"
                        ).format(blocked_by=blocked_by, near=near)
                        if blocked_by
                        else ""
                    )
                    + (
                        _("; ranges each source has: {offered}").format(offered=offered)
                        if offered
                        else ""
                    )
                )
            for blocker in validity["blockers"]:
                click.echo(f"  [research] {blocker['message']}")
                if blocker.get("remediation"):
                    click.echo(
                        _("              Suggested fix: {remediation}").format(
                            remediation=blocker["remediation"]
                        )
                    )
        if not health["healthy"]:
            click.echo("UNHEALTHY")
        elif research_start and not validity["universe_ready"]:
            # Operational freshness and research readiness are separate
            # contracts. Keep the former visible, but never let a green lake
            # label hide the strict research gate printed immediately above.
            click.echo(_("HEALTHY (operationally; BLOCKED for research)"))
        else:
            click.echo("HEALTHY")
        failed = bool(sev.get("error", 0)) if quality_only else not health["healthy"]
        if quality_only:
            click.echo(
                "Quality gate: FAILED"
                if failed
                else _("Quality gate: OK (freshness checked separately)")
            )
        if failed or (research_start and not validity["universe_ready"]):
            raise SystemExit(1)
        return

    manifest = Manifest(cfg.manifest_path)
    latest = manifest.latest_run() if not run_id else None
    rid = run_id[-1] if run_id else (latest["run_id"] if latest else "manual")

    severities: dict = {"related_run_ids": list(dict.fromkeys(run_id))} if run_id else {}
    n = run_audit(cfg, rid, shanghai_today(), severities)
    by_severity = severities.get("audit_by_severity", {})
    errors = int(by_severity.get("error", 0))
    warnings = int(by_severity.get("warning", 0))
    click.echo(
        _("Audit finished: wrote {n} findings ({errors} error, {warnings} warning)").format(
            n=n, errors=errors, warnings=warnings
        )
    )
    if getattr(cfg, "lake_profile", None) == "sample":
        # The fabricated-row check is doing its job here, loudly and correctly.
        # Say which lake it is looking at, so a first-time reader does not take
        # `cne init --profile sample` for a broken install.
        #
        # Conditional on what was actually downgraded. The profile allowance
        # covers the synthetic-source checks and nothing else, so an
        # unconditional "these are expected" would speak for errors it has
        # never seen — and the one error that is not expected is exactly the
        # one this message would hide.
        expected = int(severities.get("audit_profile_expected_findings", 0) or 0)
        if expected:
            click.echo(
                _(
                    'Sample lake: every row is deliberately generated synthetic data (source=mock), and {expected} "fabricated row" findings were downgraded to expected info; they do not mean the install failed. To build a lake with real data, run `cne init`.'
                ).format(expected=expected)
            )
        if errors:
            click.echo(
                _(
                    "Note: {errors} more errors are outside what the sample profile expects; they describe real problems, not the synthetic data itself."
                ).format(errors=errors),
                err=True,
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
    for _day in range(15):
        if is_trading_day(cfg, d):
            return d
        d -= timedelta(days=1)
    return today


_GAP_LABELS = {
    "empty": N_("empty"),
    "stale": N_("stale"),
    "interior": N_("gaps inside the range"),
    "shallow": N_("history too shallow"),
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
        raise click.UsageError(_("--bars needs --start"))
    start_date = parse_date_option(start, "--start")
    end_date = parse_date_option(end, "--end") or _last_trading_day(cfg, shanghai_today())
    if start_date > end_date:
        raise click.ClickException(_("--start must be on or before --end"))
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
        raise click.ClickException(_("Needs trading_calendar in curated")) from exc
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
        raise click.UsageError(_("Use only one of --bars and --runs"))
    mode = "bars" if bars else "runs" if runs else "datasets"
    for other, options in _VERIFY_MODE_OPTIONS.items():
        if other == mode:
            continue
        for flag, key in options:
            if given.get(key):
                hint = {
                    "datasets": _("the default dataset-coverage mode"),
                    "bars": "--bars",
                    "runs": "--runs",
                }[other]
                raise click.UsageError(_("{flag} belongs to {hint}").format(flag=flag, hint=hint))
    return mode


@cli.command()
@config_option
@click.option(
    "--derivatives",
    is_flag=True,
    help=N_(
        "Read-only acceptance check of a derivatives research window; requires --dataset, --start, --end."
    ),
)
@click.option(
    "--industry",
    is_flag=True,
    help=N_(
        "Read-only completeness of industry_members by classification system, level and snapshot date; --start/--end narrow it."
    ),
)
@click.option(
    "--bars",
    is_flag=True,
    help=N_(
        'Check "security × trading day" instead, including securities with no rows at all in the window. Requires --start.'
    ),
)
@click.option(
    "--runs",
    is_flag=True,
    help=N_("Check run evidence over consecutive trading days instead; fills no gaps."),
)
@comma_option(
    "--dataset",
    "only",
    default=None,
    help=N_(
        "Validate only these datasets (comma-separated); by default validates everything in the registry."
    ),
)
@click.option(
    "--repair",
    is_flag=True,
    help=N_("Run backfill over fillable gaps, dataset by dataset from newest to oldest."),
)
@comma_option(
    "--kind",
    "kinds",
    default=None,
    help=N_("Show only these gap types: empty,stale,interior,shallow."),
)
@click.option(
    "--start",
    default=None,
    help=N_("With --bars/--derivatives: coverage window start (inclusive)."),
)
@click.option(
    "--end",
    default=None,
    help=N_(
        "Coverage window end; --bars defaults to the last complete trading day, required for --derivatives."
    ),
)
@click.option(
    "--days",
    default=None,
    type=click.IntRange(min=1),
    # The default is DEFAULT_STABILITY_DAYS, written out for the catalog; a test
    # fails if it drifts.
    help=N_("With --runs: number of consecutive trading days required (default 20)."),
)
@click.option("--as-of", default=None, help=N_("With --runs: cutoff date YYYY-MM-DD (inclusive)."))
@click.option(
    "--enforce",
    is_flag=True,
    help=N_("With --runs: exit 1 if the consecutive-days gate fails."),
)
def verify(
    config_path: str,
    industry: bool,
    bars: bool,
    runs: bool,
    derivatives: bool,
    only: str | None,
    repair: bool,
    kinds: str | None,
    start: str | None,
    end: str | None,
    days: int | None,
    as_of: str | None,
    enforce: bool,
):
    """Compare what this lake should have with what it actually has.

    \b
    `cne audit` asks whether the data that landed is right. This asks whether what should have landed did:
    a different failure, and the kind a step that throws on contact causes. Without it, a dataset can fail every run for weeks
    while each run only records a failed batch.

    \b
    Gaps are split by whether they can be filled: a `by_date` dataset missing a session is a fault, while a snapshot dataset missing one
    is simply its shape, and no backfill could fill it honestly. `--repair` only runs the first kind.

    \b
    `--bars` and `--runs` ask the same question at two other grains: "security × session" instead of "dataset × session",
    and "one run per session" instead of rows in the lake.

    \b
    `--industry` asks it of a classification: each snapshot against the stocks listed that day, and each industry against
    the previous snapshot, so a classification that silently loses a slice of the market or a whole industry shows.

    """
    from cnequity.quality.verify import verify_lake

    if industry:
        if bars or runs or derivatives or repair or kinds or only or days or as_of or enforce:
            raise click.UsageError(_("--industry takes only --start and --end"))
        start_date = parse_date_option(start, "--start") if start else None
        end_date = parse_date_option(end, "--end") if end else None
        if start_date and end_date and start_date > end_date:
            raise click.UsageError(_("--start must be on or before --end"))
        from cnequity.quality.industry_coverage import industry_coverage_report

        report = industry_coverage_report(_cfg(config_path), start=start_date, end=end_date)
        click.echo(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(1 if report["state"] == "gaps" else 0)

    if derivatives:
        if bars or runs or repair or kinds or days or as_of or enforce:
            raise click.UsageError(
                _(
                    "--derivatives does not support other modes, --repair, --kind or run-evidence options"
                )
            )
        if only not in {"futures_bars", "option_bars"} or not start or not end:
            raise click.UsageError(
                _("--derivatives needs --dataset futures_bars|option_bars, --start and --end")
            )
        start_date = parse_date_option(start, "--start")
        end_date = parse_date_option(end, "--end")
        if start_date > end_date:
            raise click.UsageError(_("--start must be on or before --end"))
        from cnequity.quality.derivative_window import derivative_window

        report = derivative_window(_cfg(config_path), only, start_date, end_date)
        click.echo(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(1 if report["state"] == "incomplete" else 2)

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
    names = comma_values(only, "--dataset")
    if names:
        # `verify_lake` warns and skips an unknown name, which is right for a
        # library sweeping the whole registry and wrong for a name the caller
        # typed: a misspelt `--dataset` printed "覆盖完整" and exited 0, so a
        # typo read as proof the lake was fine.
        names = [_require_known_dataset(name) for name in names]
    kind_names = comma_values(kinds, "--kind")
    wanted = set(kind_names) if kind_names else None
    if wanted and wanted - {"empty", "stale", "interior", "shallow"}:
        raise click.BadParameter(
            _("Only empty,stale,interior,shallow are supported"), param_hint="--kind"
        )

    if names is None and getattr(cfg, "lake_profile", None) in {"demo", "sample"}:
        # A demo lake holds a handful of symbols and two or three datasets on
        # purpose. Measured against the whole registry it reported 35 gaps and
        # exited 1, which reads — to someone who has just run their first
        # command — as a broken install. Judge what this lake actually holds.
        names = _datasets_with_data(cfg)
        click.echo(
            _(
                "Demo lake ({profile}): only the {count} datasets it actually holds are checked "
                "({names}); the rest were never collected, which is how this profile is meant "
                "to be, not a gap."
            ).format(
                profile=cfg.lake_profile, count=len(names), names=", ".join(names) or _("none")
            )
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
                _(
                    "Sample lake: freshness is not judged for {value} datasets; these rows are synthetic (source=mock), with dates chosen by the generator."
                ).format(value=len(stale))
            )

    click.echo(
        _("Verify @ {value}: {value2} gaps").format(value=anchor.isoformat(), value2=len(gaps))
    )
    if not gaps:
        click.echo(_("Coverage complete: no repairable gaps."))
        return

    for gap in gaps:
        label = _(_GAP_LABELS.get(gap.kind, gap.kind))
        flag = _("repairable") if gap.repairable else _("the source's shape, cannot be backfilled")
        click.echo(f"  [{label}] {gap.dataset:28} {gap.detail}  ({flag})")
        if gap.sample:
            shown = ", ".join(d.isoformat() for d in gap.sample)
            more = (
                _(" … {value} more days").format(value=gap.missing_days - len(gap.sample))
                if gap.missing_days > len(gap.sample)
                else ""
            )
            click.echo(_("      e.g. {shown}{more}").format(shown=shown, more=more))

    repairable = [g for g in gaps if g.repairable]
    if not repair:
        if repairable:
            click.echo(
                _(
                    "\n{value} gaps are repairable. Add --repair to run them, or run by hand:"
                ).format(value=len(repairable))
            )
            for gap in repairable[:10]:
                click.echo(f"  {gap.repair_command(config_path)}")
        raise SystemExit(1)

    if not repairable:
        click.echo(_("\nNo repairable gaps."))
        raise SystemExit(1)

    click.echo(_("\nRepairing {value} gaps…").format(value=len(repairable)))
    failed: list[str] = []
    empty_upstream: set[str] = set()
    for gap in repairable:
        click.echo(f"  → {gap.dataset} ({_(_GAP_LABELS.get(gap.kind, gap.kind))})")
        try:
            result = _run_backfill(cfg, gap.dataset, gap.start, gap.end)
        except Exception as exc:  # noqa: BLE001 — one gap must not sink the rest
            failed.append(gap.dataset)
            click.echo(
                _("    Failed: {name}: {exc}").format(name=type(exc).__name__, exc=exc), err=True
            )
            continue
        # A failing step does not raise: the engine records a failed batch and
        # hands back status="failed". Reading only exceptions here reported
        # "全部修复完成" directly under a printed traceback.
        status = (result or {}).get("status", "unknown")
        written = (result or {}).get("rows_written", 0)
        if status != "success":
            failed.append(gap.dataset)
            click.echo(_("    Failed: status={status}").format(status=status), err=True)
        elif not written:
            # Succeeded and wrote nothing: the window is genuinely empty
            # upstream, so re-running will not change it. Say so rather than
            # claiming a repair.
            empty_upstream.add(gap.dataset)
            click.echo(
                _(
                    "    The source has no data in this range; the gap is unchanged (re-running will not change it)"
                )
            )
        else:
            click.echo(_("    Wrote {written:,} rows").format(written=written))
    if failed:
        click.echo(
            _("\n{value} could not be repaired: {value2}").format(
                value=len(failed), value2=", ".join(failed)
            ),
            err=True,
        )

    # Re-check here rather than asking for a second `cne verify`: a backfill that
    # reported success is a claim about the fetch, not about the coverage.
    rechecked = verify_lake(cfg, anchor=anchor, datasets=sorted({g.dataset for g in repairable}))
    if wanted:
        rechecked = [g for g in rechecked if g.kind in wanted]
    # An upstream that has nothing for the window keeps its gap whatever we do;
    # it was already reported above and must not fail every later run.
    left = [g for g in rechecked if g.repairable and g.dataset not in empty_upstream]
    if left:
        click.echo(
            _("\nRe-check: {value} repairable gaps are still unfilled:").format(value=len(left))
        )
        for gap in left:
            click.echo(
                f"  [{_(_GAP_LABELS.get(gap.kind, gap.kind))}] {gap.dataset:28} {gap.detail}"
            )
    else:
        click.echo(_("\nRe-check: the repaired datasets have no repairable gaps left."))
    if failed or left:
        raise SystemExit(1)


def _echo_resample_backlog(cfg, df) -> None:
    """Name stored 15m/30m/60m resamples that trail their minute inputs.

    They have no watermark, so the table can only say n/a; this line is the
    one place a reader learns that a backfill has overtaken them.
    """
    from cnequity.derive.minute_resample import resample_backlog
    from cnequity.domain.datasets import RESAMPLED_MINUTE_DATASETS

    stored = {
        row["dataset"]
        for row in df.select("dataset", "has_data").iter_rows(named=True)
        if row["has_data"] and row["dataset"] in RESAMPLED_MINUTE_DATASETS
    }
    for dataset in sorted(stored):
        pending = resample_backlog(cfg, dataset)["pending"]
        if pending:
            click.echo(
                _(
                    "\n{dataset}: {pending} sessions of input have not been recomputed yet; run cne derive {dataset} to update."
                ).format(dataset=dataset, pending=pending)
            )


@cli.command()
@config_option
@click.option(
    "--run",
    "run_selector",
    default=None,
    help=N_("Run id to show, or 'latest' (default). Includes stage results per dataset."),
)
@click.option(
    "--datasets",
    "show_datasets",
    is_flag=True,
    help=N_(
        "Per-dataset freshness: coverage range, watermark, and whether it is stale relative to the last trading day."
    ),
)
@click.option(
    "--all-columns",
    "all_columns",
    is_flag=True,
    help=N_("With --datasets: print all columns of the dataset inventory, not just freshness."),
)
@click.option(
    "--hide-empty",
    "hide_empty",
    is_flag=True,
    help=N_(
        "With --datasets: list only datasets that hold data or are stale, and fold the empty ones into one line."
    ),
)
@click.option(
    "--groups",
    "gate_groups",
    default=None,
    help=(
        N_(
            "With --datasets: fail only on datasets owned by these schedule groups (space- or comma-separated). Datasets of other groups are still listed and reported as schedule gaps but do not fail the gate. The cross-section check follows it too: daily_bars coverage is judged by whichever group owns it. An unfinished init belongs to no schedule group and always fails."
        )
    ),
)
@click.option(
    "--scope/--no-scope",
    "scope",
    default=True,
    show_default=True,
    help=(
        N_(
            "With --datasets: whether to run the cross-sectional symbol check for the latest trading day. It reads the daily_bars tip partition, instruments and trading_status, which costs more than checking watermarks alone; --no-scope returns this command to pure metadata."
        )
    ),
)
@click.option(
    "--gate",
    is_flag=True,
    help=N_("Enforce the gate for the selected scope and freshness; by default only reports."),
)
@click.option(
    "--core-only",
    "core_only",
    is_flag=True,
    help=N_(
        "With --datasets --gate: only core datasets can fail the gate; the rest are still listed. `cne check` uses this."
    ),
)
def status(
    config_path: str,
    run_selector: str | None,
    show_datasets: bool,
    all_columns: bool,
    hide_empty: bool,
    gate_groups: str | None,
    scope: bool,
    gate: bool,
    core_only: bool = False,
):
    """Show the latest run's status; add --datasets for per-dataset freshness.

    \b
    Exit codes with --gate: 0 fine; 1 genuinely failing (a dataset STALE, symbols missing from the cross-section, init not finished);
    2 cannot be proven (instruments missing, evidence unreadable). The latter is a setup problem, not a data gap.

    """
    cfg = _cfg(config_path)

    if all_columns and not show_datasets:
        raise click.UsageError(_("--all-columns only works with --datasets"))
    if hide_empty and not show_datasets:
        raise click.UsageError(_("--hide-empty only works with --datasets"))
    if gate_groups and not show_datasets:
        raise click.UsageError(_("--groups only works with --datasets"))
    if not scope and not show_datasets:
        raise click.UsageError(_("--no-scope only works with --datasets"))

    if show_datasets:
        import polars as pl_mod

        from cnequity.domain.datasets import (
            DATASETS,
            empty_freshness_label,
            freshness_state,
            is_dataset_enabled,
        )
        from cnequity.query.reader import list_datasets

        anchor = _last_trading_day(cfg, shanghai_today())
        df = list_datasets(config=cfg)
        lake_profile = getattr(cfg, "lake_profile", None)

        def _freshness(row: dict) -> str:
            if lake_profile == "sample":
                return "sample" if row["has_data"] else "n/a"
            if lake_profile == "demo" and not row["has_data"]:
                return "n/a"
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
            state = freshness_state(row["dataset"], mark, anchor, cfg)
            return {"stale": "STALE", "removed": "removed"}.get(state, state)

        df = df.with_columns(
            pl_mod.Series("freshness", [_freshness(r) for r in df.iter_rows(named=True)])
        )
        click.echo(_("Last trading day: {value}").format(value=anchor.isoformat()))
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
        hidden: list[str] = []
        if hide_empty:
            # A first lake holds a handful of the registered datasets; forty
            # rows of "empty" read as a broken install rather than as groups
            # nobody has run yet. They are not judged here, so fold them.
            listed = pl_mod.col("has_data") | (pl_mod.col("freshness") == "STALE")
            hidden = view.filter(~listed)["dataset"].to_list()
            view = view.filter(listed)
        # Piped output has no terminal to measure, and polars then wraps at 100
        # columns, splitting long dataset names mid-word.
        width = max(shutil.get_terminal_size((120, 24)).columns, 112)
        with pl_mod.Config(
            tbl_rows=-1,
            tbl_cols=-1,
            fmt_str_lengths=32,
            **({} if all_columns else {"tbl_width_chars": width}),
        ):
            click.echo(view)
        if hidden:
            click.echo(
                _(
                    "{value} more datasets have no data yet: their schedule groups are not enabled or have not run, which does not affect the conclusion here. Full list: cne status --datasets"
                ).format(value=len(hidden))
            )
        _echo_resample_backlog(cfg, df)
        if lake_profile == "sample":
            click.echo(
                _(
                    "\nSample lake: these are synthetic source=mock rows whose dates do not count toward the freshness gate; it only verifies the install, Parquet writes and the query path."
                )
            )
        wanted_groups = _gate_groups(gate_groups)
        scopes = [
            report
            for dataset in (INSTRUMENT_SCOPED_DATASETS if scope else ())
            if (report := _tip_scope(cfg, df, dataset)) is not None
        ]
        if scope:
            from cnequity.quality.derivative_checks import (
                DERIVATIVE_BAR_CONTRACTS,
                derivative_tip_scope,
            )

            scopes.extend(
                report
                for dataset in DERIVATIVE_BAR_CONTRACTS
                if (report := derivative_tip_scope(cfg, dataset)) is not None
            )
        gating_scopes = [
            report
            for report in scopes
            if _gates_on_dataset(cfg, report["dataset"], wanted_groups, core_only=core_only)
        ]
        scope_incomplete = any(r["state"] == "incomplete" for r in gating_scopes)
        scope_unverified = any(r["state"] == "unverified" for r in gating_scopes)
        default_heading = _("Ingest cross-section ({value})").format(
            value=ingest_scope_label(cfg.ingest_universe)
        )
        for report in scopes:
            dataset = report["dataset"]
            scope_heading = (
                localize(report["heading"]) if report.get("heading") else default_heading
            )
            if report.get("missing_exchanges"):
                click.echo(
                    _("{dataset} is missing whole exchanges: {value}").format(
                        dataset=dataset, value=", ".join(report["missing_exchanges"])
                    ),
                    err=True,
                )
            if report.get("owed_dates"):
                click.echo(
                    _("{dataset} sessions still owed: {owed} (e.g. {value})").format(
                        dataset=dataset,
                        owed=report["owed"],
                        value=", ".join(report["owed_dates"][:5]),
                    ),
                    err=True,
                )
                if _gates_on_dataset(cfg, dataset, wanted_groups, core_only=core_only):
                    scope_incomplete = True
            if all_columns and report.get("coverage"):
                click.echo(pl_mod.DataFrame(report["coverage"]))
            if report["state"] == "complete":
                owed_note = (
                    _(", plus {owed} symbols already in the ledger to fill").format(
                        owed=report["owed"]
                    )
                    if report.get("owed")
                    else ""
                )
                if report.get("unverifiable_exchanges"):
                    # A limitation of the route, stated beside the result; it does
                    # not fail the gate (the data on it is not shown to be short).
                    owed_note += _(
                        "; limitation, not counted: {exchanges} — vendor route with no rows on "
                        "days without trades, so it cannot be proven "
                        '(an official route, [futures].dce_route = "official", can be)'
                    ).format(exchanges=_(", ").join(report["unverifiable_exchanges"]))
                click.echo(
                    _(
                        "\n{scope_heading}: OK: {dataset} {date} coverage evidence {covered}/{expected} ({ratio:.1%}){owed_note}"
                    ).format(
                        scope_heading=scope_heading,
                        dataset=dataset,
                        date=report["date"],
                        covered=report["covered"],
                        expected=report["expected"],
                        ratio=report["ratio"],
                        owed_note=owed_note,
                    )
                )
            elif report["state"] == "incomplete":
                sample = ", ".join(report["missing"][:5])
                click.echo(
                    _(
                        "\n{scope_heading}: INCOMPLETE: on {date}, {count} {noun} of {dataset} "
                        "have no data, no suspension evidence and no ledger entry (coverage "
                        "{covered}/{expected}, {ratio:.1%}; e.g. {sample}). A fresh date does not "
                        "mean symbol coverage is complete."
                    ).format(
                        scope_heading=scope_heading,
                        date=report["date"],
                        count=len(report["missing"]),
                        noun=_(report.get("noun") or N_("active symbols")),
                        dataset=dataset,
                        covered=report["covered"],
                        expected=report["expected"],
                        ratio=report["ratio"],
                        sample=sample,
                    ),
                    err=True,
                )
            else:
                click.echo(
                    _(
                        '\n{scope_heading}: UNVERIFIED: {message}. This means "cannot be proven", not "proven incomplete"; a fresh date still does not mean symbol coverage is complete.'
                    ).format(scope_heading=scope_heading, message=report["message"]),
                    err=True,
                )
        if scope and is_dataset_enabled("option_greeks", cfg):
            from cnequity.derive.option_greeks import stale_sessions

            stale_greeks = stale_sessions(cfg)
            if stale_greeks:
                click.echo(
                    _(
                        "option_greeks: {value} sessions lack results or have changed dependencies; run cne derive option_greeks"
                    ).format(value=len(stale_greeks)),
                    err=True,
                )
                if _gates_on_dataset(cfg, "option_greeks", wanted_groups, core_only=core_only):
                    scope_incomplete = True
        incomplete_init = Manifest(cfg.manifest_path).latest_incomplete_init_run(
            discharged_by_later_runs=True
        )
        if incomplete_init is not None:
            click.echo(
                _(
                    '\nInit has not finished: run {run_id} is currently {status}. "fresh" above only means the dates of existing data are fresh, not that whole-market coverage is complete; running `cne init` continues from this run.'
                ).format(run_id=incomplete_init["run_id"], status=incomplete_init["status"]),
                err=True,
            )
        # A tolerated gap is invisible in freshness: the watermark moved over
        # the hole, so the dataset reads FRESH while still owing keys. The
        # ledger is the only place that knows, and nobody reads a json file
        # they were not told about.
        _report_outstanding_keys(cfg, df["dataset"].to_list())
        stale_rows = df.filter(pl_mod.col("freshness") == "STALE")
        if core_only:
            reported = sorted(
                name for name in stale_rows["dataset"].to_list() if not _is_core(name)
            )
            if reported:
                click.echo(
                    _("\nStale but not core, so not gating: {value}").format(
                        value=", ".join(reported)
                    )
                )
            stale_rows = stale_rows.filter(~pl_mod.col("dataset").is_in(reported))
        stale = stale_rows.height
        if stale:
            click.echo(
                _(
                    "\n{stale} datasets are STALE; look into the related runs with `cne status` / `cne run retry`."
                ).format(stale=stale)
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
                click.echo(_("By schedule group: {summary}").format(summary=summary))
                click.echo(
                    _(
                        "A schedule group you have not scheduled is a scheduling gap, not a failure; run it with `cne run daily --group <name>`."
                    )
                )
            # ...and until this flag existed, the gate said exactly that and then
            # failed anyway. A host scheduling `core` alone has twenty-odd
            # datasets no job ever fetches, so the freshness gate failed every
            # single day — 21 to 25 stale on 2026-09-12/13/14 — and the daily
            # "数据异常" notification became something to dismiss. Three real
            # `UNHEALTHY` days sat inside that noise unread.
            wanted = wanted_groups
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
                        _(
                            "The gate only looks at {value}: {value2} of them are stale, and {skipped} more belong to groups this machine does not run."
                        ).format(
                            value=", ".join(sorted(wanted)), value2=len(gating), skipped=skipped
                        )
                    )
                if gating and gate:
                    raise SystemExit(1)
            else:
                if gate:
                    raise SystemExit(1)
        code = _scope_exit_code(
            incomplete_init is not None,
            scope_incomplete=scope_incomplete,
            scope_unverified=scope_unverified,
        )
        if code and gate:
            raise SystemExit(code)
        return

    manifest = Manifest(cfg.manifest_path)
    selected = run_selector
    if selected and selected != "latest":
        latest = manifest.get_run(selected)
        if latest is None:
            raise click.ClickException(_("Unknown run_id: {selected}").format(selected=selected))
    else:
        latest = manifest.latest_run()
    if not latest:
        click.echo(_("No runs yet."))
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
    if sys.stderr.isatty():
        # The JSON below is the stable contract for scripts; a person at a
        # terminal gets the one-line answer first, on stderr so pipes stay clean.
        click.echo(
            _("Run {run_id} ({job_name}): {status}").format(
                run_id=summary.get("run_id"),
                job_name=summary.get("job_name"),
                status=summary.get("status"),
            ),
            err=True,
        )
    click.echo(json.dumps(summary, indent=2, default=str))
    run_status = str(summary.get("dataset_status") or summary.get("status") or "success")
    if gate and run_status == "degraded":
        raise SystemExit(2)
    if gate and run_status == "failed":
        raise SystemExit(1)


@cli.group("sources")
def sources_grp():
    """Probe the data sources this lake depends on, and check the related evidence.

    \b
    Only `probe` touches the network; the rest only read stored probe history and the dataset registry.

    \b
    They used to be `cne sources` and `cne source <subcommand>`: two top-level commands one letter apart,
    so that this group's own help first had to explain which was which.

    """


@sources_grp.command("slo")
@config_option
@click.option("--window-days", default=30, show_default=True, type=click.IntRange(min=1))
@click.option("--minimum-observations", default=10, show_default=True, type=click.IntRange(min=1))
@click.option("--enforce", is_flag=True, help=N_("Exit 1 when a critical source misses its SLO."))
def source_slo(config_path: str, window_days: int, minimum_observations: int, enforce: bool):
    """Evaluate historical source-probe records, and output event payloads."""
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
@click.option(
    "--enforce", is_flag=True, help=N_("Exit 1 when a core dataset has no independent backup.")
)
@click.option(
    "--with-availability",
    is_flag=True,
    help=N_("Attach measured probe availability to each failure domain (reads the lake)."),
)
@click.option("--window-days", default=30, show_default=True, type=click.IntRange(min=1))
def source_resilience(
    config_path: str,
    out: Path | None,
    enforce: bool,
    with_availability: bool,
    window_days: int,
):
    """Show source concentration, blast radius, and the independent-backup gate.

    \b
    Concentration alone cannot settle a routing question. The largest domain carries most datasets in the registry,
    but how serious that is depends on how often it is unreachable, which is measured, not declared.
    So `--with-availability` joins the probe history this lake has accumulated onto every failure domain.

    \b
    The report itself is computed from the registry, so it needs no lake and gives the same answer wherever it runs.
    `--config` is only read with `--with-availability`; passed explicitly it is still parsed,
    so a misspelled path is an error here rather than quietly ignored.

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
    """Show source usage policies; unclear permission is treated as refusal."""
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
        raise click.ClickException(_("Unknown source policy {source!r}").format(source=source))
    assessment = usage_profile(
        policies[source],
        profile=profile,
        redistribution=redistribution,
    )
    click.echo(json.dumps(assessment.as_dict(), indent=2, ensure_ascii=False))
    if not assessment.allowed:
        raise SystemExit(1)


@sources_grp.command("limits")
@config_option
def source_limits(config_path: str):
    """Show shared cooldowns, egress budgets, recent request telemetry and debt-resume entry points, offline."""
    from cnequity.diagnostics.source_limits import build_source_limits

    payload = build_source_limits(_cfg(config_path))
    click.echo(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


@sources_grp.command("probe")
@config_option
@click.option(
    "--vantage",
    default=None,
    show_default="CNE_SOURCE_VANTAGE, else [source_health].vantage, else local",
    help=N_(
        "Where this probe runs from — 'cn', 'overseas', or any label you use. Some sources reject non-mainland egress, so results without this label cannot be interpreted."
    ),
)
@comma_option(
    "--only",
    default=None,
    help=N_(
        "Keys to probe, comma-separated; costly/challenged endpoints are probed only when listed explicitly."
    ),
)
@click.option(
    "--stale-only",
    is_flag=True,
    help=N_(
        "Reuse collection evidence verified within 12 hours; actively probe only endpoints lacking fresh evidence."
    ),
)
@click.option(
    "--list",
    "list_only",
    is_flag=True,
    help=N_("List probe source names offline; no config needed."),
)
@click.option(
    "--out",
    default=None,
    help=(
        N_(
            "Where to write the JSON report. Defaults to meta/source_health/<vantage>.json in the lake, which `cne serve` also reads."
        )
    ),
)
def sources_probe(
    config_path: str,
    vantage: str | None,
    only: str | None,
    out: str | None,
    list_only: bool = False,
    stale_only: bool = False,
):
    """Probe the public data sources this lake depends on.

    \b
    Minimal probes run source by source (some sources need paging or a connection handshake), sharing the ingest rate limits and cooldowns:
    a health check that probes itself into a rate-limit ban creates exactly the failure it was meant to observe.

    \b
    The report is written into the lake, and `cne serve` renders it at /source-health.
    Probing is deliberately a CLI action: the dashboard stays read-only,
    and an unauthenticated local service that can reach out to a dozen third parties is not something to leave listening.

    """
    from cnequity.diagnostics.source_health import (
        PROBES,
        STATUS_LABELS,
        ProbeStatus,
        effective_vantage,
        run_probes,
    )

    if list_only:
        for probe in PROBES:
            suffix = _(" [explicit probe only]") if probe.manual_only else ""
            click.echo(f"{probe.key:<24} {_(probe.label)}{suffix}")
        return
    _progress_logging(quiet=True)
    keys = comma_values(only, "--only")
    cfg = _cfg(config_path)
    vantage = effective_vantage(cfg, vantage)
    try:
        report = run_probes(cfg, vantage=vantage, only=keys, stale_only=stale_only)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from None

    for result in report.results:
        latency = f"{result.latency_ms:>6}ms" if result.latency_ms is not None else "     \u2014"
        label = _(STATUS_LABELS[ProbeStatus(result.status)])
        click.echo(
            f"{result.status:<8}{label:<5}{latency}  {result.key:<22}{localize(result.detail)}"
        )

    if out:
        path = Path(out)
        write_json_atomic(path, report.to_dict(), indent=2, ensure_ascii=False)
    else:
        from cnequity.diagnostics.source_slo import store_health_report

        path, historical = store_health_report(cfg.meta_root, report)
        click.echo(_("Historical sample: {historical}").format(historical=historical))
    click.echo(_("\nWritten to {path}").format(path=path))
    click.echo(_("View it with: cne serve  →  http://127.0.0.1:8787/source-health"))


@sources_grp.command("substitutes")
@config_option
@click.option(
    "--vantage",
    default=None,
    show_default="CNE_SOURCE_VANTAGE, else [source_health].vantage, else local",
    help=N_("Which egress location (vantage) report to read."),
)
@click.option(
    "--probe/--no-probe",
    default=False,
    show_default=True,
    help=N_(
        "Measure now instead of reading the stored report. Makes the same requests as `sources probe`."
    ),
)
@click.option("--json", "as_json", is_flag=True, help=N_("Output machine-readable JSON."))
def sources_substitutes(config_path: str, vantage: str | None, probe: bool, as_json: bool):
    """For each source that is down, who else can answer for its datasets.

    \b
    The probe report says what is alive; this says what to do about the dead ones.
    Candidates are ranked by independence first, because an endpoint sharing the failed source's blast radius is no second opinion:
    EastMoney's history host cannot stand in for EastMoney's snapshot host.

    \b
    Exits non-zero when a dataset is stranded: what it needs is down, and no reachable alternative is left.

    """
    import json as json_mod

    from cnequity.diagnostics.source_health import (
        HealthReport,
        effective_vantage,
        run_probes,
        validate_vantage,
    )
    from cnequity.diagnostics.substitutes import (
        IGNORED,
        render_substitutions,
        substitution_report,
        to_dict,
    )

    _progress_logging(quiet=True)
    cfg = _cfg(config_path)
    vantage = effective_vantage(cfg, vantage)
    try:
        validate_vantage(vantage)
    except ValueError as exc:
        raise click.BadParameter(str(exc), param_hint="--vantage") from None
    if probe:
        report = run_probes(cfg, vantage=vantage)
    else:
        path = cfg.meta_root / "source_health" / f"{vantage}.json"
        if not path.exists():
            raise click.ClickException(
                _(
                    "No probe report for vantage {vantage!r} under {path}. Run `cne sources probe` first, or add --probe to measure now."
                ).format(path=path, vantage=vantage)
            )
        report = HealthReport.from_dict(json_mod.loads(path.read_text(encoding="utf-8")))

    entries = substitution_report(report)
    if as_json:
        click.echo(json.dumps(to_dict(entries), indent=2, ensure_ascii=False))
    else:
        click.echo(
            _("Probed at {generated_at} · vantage {vantage}").format(
                generated_at=report.generated_at, vantage=report.vantage
            )
        )
        probed_any = any(r.status not in IGNORED for r in report.results)
        for line in render_substitutions(entries, probed_any=probed_any):
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
        _(
            "\nKeys owed by tolerated gaps: {summary}\nThese sessions are already past the watermark, and no incremental run will ask for them again; fill them with `cne backfill <dataset> --outstanding`."
        ).format(summary=summary)
    )
    # Separated because they need a different decision. A key three repairs
    # could not fill is not backlog, it is a key no configured source serves,
    # and re-running the repair will not change that.
    stuck = [(dataset, n) for dataset, _count, n in sorted(owed) if n]
    if stuck:
        detail = ", ".join(f"{dataset} {n}" for dataset, n in stuck)
        click.echo(
            _(
                "Of these, still unfilled after 3 or more repair attempts: {detail}. No configured source provides them; check `cne sources probe`, or accept the gap."
            ).format(detail=detail)
        )


@cli.command("check")
@config_option
@click.option(
    "--full",
    is_flag=True,
    help=N_(
        "Rerun the full-lake audit now (reads every historical partition; can take hours on a large lake); by default reads the latest audit result."
    ),
)
@click.option(
    "--pack",
    "pack_names",
    multiple=True,
    type=click.Choice(["market", "fundamentals", "universe"]),
    help=N_(
        "Report window, gaps and next steps per research pack. Repeatable. When given, missing data or uncovered historical ST worsens the exit code."
    ),
)
@click.pass_context
def check(ctx: click.Context, config_path: str, full: bool, pack_names: tuple[str, ...]):
    """Accept this lake: freshness and coverage, data quality and size, with a verdict from one command.

    \b
    In order:
      1. Freshness and coverage: the same as `cne status --datasets --gate --core-only` (including the cross-section check and unfinished init);
      2. Data quality: the latest run's audit and the latest whole-lake audit snapshot; `--full` re-runs the whole-lake audit on the spot;
      3. Size: datasets, rows and volume (stats are recomputed first when stale).
    Only the core datasets (see the catalog's support levels) can fail it; problems elsewhere are listed, not counted.
    The exit code is the worst of them: 0 usable; 1 gaps or quality errors; 2 cannot be proven (missing evidence or a config problem).
    `--pack` also gives the window, gaps and next step per research pack.

    """
    cfg = _cfg(config_path)
    codes: list[int] = []
    problems: list[str] = []

    click.echo(_("== Freshness and coverage =="))
    try:
        ctx.invoke(
            status,
            config_path=config_path,
            run_selector=None,
            show_datasets=True,
            all_columns=False,
            hide_empty=True,
            gate_groups=None,
            scope=True,
            gate=True,
            core_only=True,
        )
        codes.append(0)
    except SystemExit as exc:
        code = int(exc.code or 0)
        codes.append(code)
        if code:
            problems.append(
                _("freshness or coverage failing") if code == 1 else _("freshness cannot be proven")
            )

    click.echo(_("\n== Data quality =="))
    code, note = _check_quality(cfg, full=full)
    codes.append(code)
    if note:
        problems.append(note)

    click.echo(_("\n== Size =="))
    _check_size(cfg)

    if pack_names:
        from cnequity.research.packs import assess, format_readiness, readiness_exit_code

        click.echo(_("\n== Research packs =="))
        rows = assess(cfg, pack_names)
        click.echo(format_readiness(cfg, pack_names))
        pack_code = readiness_exit_code(rows)
        codes.append(pack_code)
        if pack_code:
            problems.append(
                _("research packs still have gaps")
                if pack_code == 1
                else _("research packs cannot be proven")
            )

    worst = 1 if 1 in codes else max(codes, default=0)
    click.echo(_("\n== Verdict =="))
    if worst == 0:
        if getattr(cfg, "lake_profile", None) == "sample":
            click.echo(
                _(
                    "✓ Install verified: the offline sample lake reads back correctly (synthetic data, not for research)"
                )
            )
            return
        click.echo(_("✓ The lake is usable: freshness, coverage and quality all pass"))
        return
    click.echo(f"✗ {_('; ').join(problems)}")
    raise SystemExit(worst)


def _check_quality(cfg, *, full: bool) -> tuple[int, str | None]:
    """Gate on recorded audit errors; rerun the whole-lake audit only when asked."""
    from datetime import datetime

    from cnequity.quality.publication import coverage_only

    quality_root = cfg.meta_root / "quality"
    code, note = 0, None

    runs = sorted(
        (quality_root / "findings").glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True
    )
    if runs:
        payload = json.loads(runs[0].read_text(encoding="utf-8"))
        from cnequity.quality.factor_policy import advisory_factor_findings

        findings = advisory_factor_findings(payload.get("findings", []))
        errors = [
            f
            for f in findings
            if f.get("severity") == "error" and not coverage_only(f) and _gating_finding(f)
        ]
        warnings = sum(f.get("severity") == "warning" for f in findings)
        click.echo(
            _("  Latest run audit ({value}, {value2}): {value3} error, {warnings} warning").format(
                value=payload.get("run_id"),
                value2=payload.get("trade_date"),
                value3=len(errors),
                warnings=warnings,
            )
        )
        for finding in errors[:10]:
            click.echo(f"    [error] {finding.get('dataset', ''):22} {finding.get('message', '')}")
        if errors:
            code, note = 1, _("the latest audit has {value} errors").format(value=len(errors))
    else:
        click.echo(_("  No run audit records yet"))

    if full:
        from cnequity.quality.audit import lake_health

        click.echo(_("  Re-running the whole-lake audit (reads every historical partition)…"))
        health = lake_health(cfg, shanghai_today())
        health_errors = _health_errors(health, coverage_only)
        click.echo(
            _("  Whole-lake audit (just now): {value} error").format(value=len(health_errors))
        )
        label = _("whole-lake audit")
    else:
        snapshot = quality_root / "health-latest.json"
        health_errors = []
        if snapshot.exists():
            health = json.loads(snapshot.read_text(encoding="utf-8"))
            age = (datetime.now() - datetime.fromtimestamp(snapshot.stat().st_mtime)).days
            health_errors = _health_errors(health, coverage_only)
            click.echo(
                _(
                    "  Whole-lake audit snapshot ({value}, {age} days ago): {value2} error; add --full to re-run now"
                ).format(value=health.get("trade_date"), age=age, value2=len(health_errors))
            )
        else:
            click.echo(_("  No whole-lake audit snapshot yet; add --full to generate one now"))
        label = _("whole-lake audit snapshot")
    health_errors = [f for f in health_errors if not f or _gating_finding(f)]
    for finding in [f for f in health_errors if f][:10]:
        click.echo(f"    [error] {finding.get('dataset', ''):22} {finding.get('message', '')}")
    if health_errors:
        found = _("{label} has {value} errors").format(label=label, value=len(health_errors))
        code, note = (
            1,
            found if note is None else _("{note}, {found}").format(note=note, found=found),
        )
    if not runs and not full:
        if getattr(cfg, "lake_profile", None) == "sample":
            click.echo(_("  Sample lake: the data is synthetic, so no quality audit applies"))
            return code, note
        return 2, _("no audit records, quality cannot be proven (add --full to audit now)")
    return code, note


def _gating_finding(finding: dict) -> bool:
    """An error `cne check` counts: a core dataset's, or one about no dataset."""
    return _is_core(str(finding.get("dataset") or ""))


def _health_errors(health: dict, coverage_only) -> list[dict]:
    """Quality errors in a whole-lake audit, without the coverage-only ones.

    A snapshot from before ``error_findings`` was recorded has only counts;
    those count as errors with nothing to list.
    """
    if "error_findings" not in health:
        count = int(health.get("findings_by_severity", {}).get("error", 0))
        return [{} for _ in range(count)]
    from cnequity.quality.factor_policy import factor_advisory

    return [f for f in health["error_findings"] if not coverage_only(f) and not factor_advisory(f)]


def _check_size(cfg) -> None:
    from cnequity.storage.stats import load_summary, refresh_stats_if_stale, stats_freshness

    if stats_freshness(cfg).stale:
        click.echo(_("  Stats are stale or not generated yet; recomputing…"), err=True)
        refresh_stats_if_stale(cfg)
    summary = load_summary(cfg)
    if summary is None:
        click.echo(_("  No stats tables yet (another recompute is in progress)"))
        return
    click.echo(
        _("  {value} datasets, {value2:,} rows, {value3:.1f} GB (counted at {value4})").format(
            value=int(summary.get("datasets") or 0),
            value2=int(summary.get("rows") or 0),
            value3=int(summary.get("bytes") or 0) / 1e9,
            value4=summary.get("generated_at"),
        )
    )
