"""Repairs of stored data, each published as a new retained revision."""

from __future__ import annotations

import json

import click

from cnequity.cli import backfill_cmds
from cnequity.cli._root import cli
from cnequity.cli._shared import (
    _cfg,
    apply_option,
    comma_option,
    comma_values,
    config_option,
    parse_date_option,
)
from cnequity.i18n import N_, _
from cnequity.orchestrator.backfill_request import REPAIRS, BackfillRequest


def _emit(value: dict) -> None:
    click.echo(json.dumps(value, indent=2, ensure_ascii=False, default=str))


@cli.command("migrate")
@config_option
@click.option(
    "--apply",
    is_flag=True,
    help=N_("Run the owed migrations and record each result in meta/lake.json."),
)
@comma_option(
    "--only",
    "only",
    help=N_(
        "Comma-separated migration ids; runs them even when already recorded. "
        "Default: every migration this lake still owes."
    ),
)
@click.option("--json", "as_json", is_flag=True, help=N_("Output machine-readable JSON."))
def migrate(config_path: str, apply: bool, only: str | None, as_json: bool):
    """Preview, or with --apply run, the one-off fixes this lake still owes.

    
    A release that fixes a writer can leave rows the old writer stored. Each such fix is a
    migration; meta/lake.json records the version that built the lake (it owes nothing
    introduced earlier) and every migration it has applied or found nothing to do.
    A preview reports how many rows each would change. --apply runs them in order: each takes the
    lake lock, publishes through the repair gate and keeps the previous revision readable.

    """
    from cnequity.storage.migrations import MIGRATIONS, affected, owed, read_lake, record

    cfg = _cfg(config_path)
    by_id = {m.id: m for m in MIGRATIONS}
    only = comma_values(only, "--only")
    if only:
        unknown = sorted(set(only) - set(by_id))
        if unknown:
            raise click.BadParameter(
                _("unknown migration(s): {names}; known: {known}").format(
                    names=", ".join(unknown), known=", ".join(by_id)
                ),
                param_hint="--only",
            )
        chosen = [m for m in MIGRATIONS if m.id in only]
    else:
        chosen = owed(cfg)
    results: list[dict] = []
    failed = False
    if not chosen and not as_json:
        created = read_lake(cfg).get("created_with")
        click.echo(
            _(
                "Nothing owed: the lake was built by {version}, or every migration is recorded."
            ).format(version=created)
            if created
            else _("Nothing owed: every migration is recorded in meta/lake.json.")
        )
        return
    for migration in chosen:
        if not as_json:
            note = " (" + _("reads") + f" {_(migration.reads)})" if migration.reads else ""
            click.echo(f"--- {migration.id}: {_(migration.title)}{note}", err=True)
        try:
            report = migration.apply(cfg, apply=apply)
        except Exception as exc:  # noqa: BLE001 — report it and stop; later ones may depend on it
            failed = True
            results.append({"id": migration.id, "error": f"{type(exc).__name__}: {exc}"})
            if not as_json:
                click.echo(_("  failed: {exc}").format(exc=exc), err=True)
            break
        count = affected(report)
        if apply:
            record(cfg, migration, report)
        results.append({"id": migration.id, "affected": count, "report": report})
        if not as_json:
            if apply:
                line = _("  applied: {count} row(s)") if count else _("  nothing to do; recorded")
            else:
                line = _("  would change {count} row(s)") if count else _("  nothing to do")
            click.echo(line.format(count=count), err=True)
    if as_json:
        click.echo(json.dumps({"applied": apply, "migrations": results}, default=str, indent=2))
    elif not apply and not failed:
        click.echo(_("Preview only. Run `cne migrate --apply` to apply and record them."))
    if failed:
        raise SystemExit(1)


_UPGRADE_STAGES = {
    "copy": N_("registering"),
    "verify": N_("verifying"),
    "lay_out": N_("laying out"),
    "repair": N_("repairing"),
}


@cli.command("upgrade")
@config_option
@click.option(
    "--apply",
    is_flag=True,
    help=N_("Upgrade the lake. Without it, only show what the upgrade would do."),
)
@click.option(
    "--no-backup",
    is_flag=True,
    help=N_("Skip the copy of the lake's records that is taken before upgrading."),
)
@click.option("--json", "as_json", is_flag=True, help=N_("Output machine-readable JSON."))
def upgrade(config_path: str, apply: bool, no_backup: bool, as_json: bool):
    """Bring a lake built by an earlier version up to this one, in one step.

    
    Shows what this version changes about the lake and checks that this machine can do it;
    --apply then does all of it in order: a backup of the lake's records, the move onto the
    lake catalog, and every other fix the lake still owes. Data files are linked, not copied.
    Reading the lake keeps working throughout; jobs that write wait until the upgrade is done.
    A run that stops can be started again and continues where it stopped.

    """
    import time

    from cnequity.ops import upgrade as upgrading
    from cnequity.storage.migrations import MIGRATIONS

    cfg = _cfg(config_path)
    plan = upgrading.plan(cfg)
    titles = {migration.id: migration.title for migration in MIGRATIONS}
    catalog = plan["catalog"]
    if as_json and not apply:
        _emit({**plan, "checks": [vars(check) for check in plan["checks"]], "applied": False})
        return
    if not plan["needed"]:
        if as_json:
            _emit({"needed": False, "applied": False, "version": plan["version"]})
        else:
            click.echo(
                _("This lake is up to date with cnequity {version}; nothing to upgrade.").format(
                    version=plan["version"]
                )
            )
        return
    say = (lambda *args, **kwargs: None) if as_json else click.echo
    if plan["created_with"]:
        say(
            _("This lake was built by {built}; cnequity {version} needs {count} step(s):").format(
                built=plan["created_with"], version=plan["version"], count=len(plan["migrations"])
            )
        )
    else:
        say(
            _(
                "This lake was built by an earlier version; cnequity {version} needs {count} step(s):"
            ).format(version=plan["version"], count=len(plan["migrations"]))
        )
    for index, name in enumerate(plan["migrations"], 1):
        say(f"  {index}. {name}: {_(titles[name])}")
        if name == "lake-catalog":
            say(
                "     "
                + _(
                    "{copy} dataset(s) to register in the lake catalog, "
                    "{layout} table(s) to lay out by partition"
                ).format(copy=len(catalog["to_copy"]), layout=len(catalog["to_lay_out"]))
            )
    for check in plan["checks"]:
        say(f"  [{_('ok') if check.ok else _('BLOCKED')}] {check.detail}")
    if not plan["ready"]:
        say(_("Nothing was changed. Fix what is blocked above, then run this again."))
        if as_json:
            _emit({**plan, "checks": [vars(check) for check in plan["checks"]], "applied": False})
        raise SystemExit(1)
    if not apply:
        say(
            _(
                "Nothing was changed. Run `cne upgrade --apply` to upgrade. Reading the lake "
                "keeps working meanwhile; jobs that write wait until it is done."
            )
        )
        return

    backup = None if no_backup else upgrading.back_up(cfg)
    if backup is not None:
        say(_("Backed up the lake's records to {path}").format(path=backup))
    totals = {
        "copy": len(catalog["to_copy"]),
        "verify": int(catalog.get("datasets") or 0),
        "lay_out": len(catalog["to_lay_out"]),
    }
    seen: dict[tuple[str, str], int] = {}

    def progress(migration: str, stage: str, dataset: str) -> None:
        count = seen[(migration, stage)] = seen.get((migration, stage), 0) + 1
        total = totals.get(stage) if migration == "lake-catalog" else None
        position = f" {count}/{total}" if total and count <= total else ""
        say(f"  [{migration}] {_(_UPGRADE_STAGES.get(stage, stage))}{position} {dataset}".rstrip())

    started = time.monotonic()
    result = upgrading.run(cfg, progress)
    elapsed = int(time.monotonic() - started)
    took = f"{elapsed // 60}m {elapsed % 60}s" if elapsed >= 60 else f"{elapsed}s"
    if as_json:
        _emit({**result, "applied": True, "backup": backup, "seconds": elapsed})
    failed = next((item for item in result["migrations"] if "error" in item), None)
    if failed is not None:
        say(
            _(
                "Stopped at {name} after {took}: {error}\nThe steps before it are done and "
                "recorded; what this step had not finished is unchanged. Fix the cause, then "
                "run `cne upgrade --apply` again."
            ).format(name=failed["id"], took=took, error=failed["error"])
        )
        raise SystemExit(1)
    say(_("Upgraded in {took}.").format(took=took))
    say(
        _(
            "Next: restart `cne serve` and any running `cne mcp`. Scheduled jobs continue "
            "on their next run."
        )
    )


@cli.group()
def repair():
    """Repair stored data: previews by default; --apply verifies, publishes, and keeps the old version."""


@repair.command("fund-volume")
@config_option
@click.option(
    "--apply", is_flag=True, help=N_("Replace verified whole rows and publish a retained revision.")
)
@click.option(
    "--refresh-sources",
    is_flag=True,
    help=N_(
        "Archive official historical K-lines and any required independent turnover evidence; replay archives offline by default."
    ),
)
@click.option("--max-symbols", type=click.IntRange(min=1), default=16, show_default=True)
def fund_volume(config_path: str, apply: bool, refresh_sources: bool, max_symbols: int):
    """Repair Shanghai/Shenzhen exchange-traded funds with zero volume but positive turnover; must agree with official daily prices and amounts."""
    from cnequity.storage.repairs.fund_volume import repair_fund_volume

    try:
        _emit(
            repair_fund_volume(
                _cfg(config_path),
                apply=apply,
                refresh_sources=refresh_sources,
                max_symbols=max_symbols,
            )
        )
    except (ValueError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc


@repair.command("financial-originals")
@config_option
@comma_option("--symbols", help=N_("Comma-separated stock symbols."))
@click.option(
    "--all-stocks", is_flag=True, help=N_("Use the lake stock catalogue; choose this or --symbols.")
)
@click.option(
    "--after",
    help=N_("Resume from the previous next_cursor (YEAR:SYMBOL); retry failures without a cursor."),
)
@click.option(
    "--quarter",
    type=click.Choice(["Q1", "Q2", "Q3", "Q4"]),
    default="Q4",
    show_default=True,
    help=N_(
        "Report period: Q2 is half-year and Q4 annual; income and cash flow use year-to-date figures only."
    ),
)
@click.option("--start-year", type=click.IntRange(min=2001), default=2016, show_default=True)
@click.option("--end-year", type=click.IntRange(min=2001), default=2024, show_default=True)
@click.option("--max-reports", type=click.IntRange(min=1), default=16, show_default=True)
@click.option(
    "--reparse",
    is_flag=True,
    help=N_(
        "Reparse existing original reports; unchanged facts retain their original observation times."
    ),
)
@click.option(
    "--refresh-sources",
    is_flag=True,
    help=N_(
        "Fetch complete CNINFO announcement queries and original periodic reports; replay archived sources by default."
    ),
)
@click.option(
    "--apply",
    is_flag=True,
    help=N_(
        "Append verifiable current-year figures from original reports; retain existing vintages and honest observation times."
    ),
)
def financial_originals(
    config_path: str,
    symbols: str | None,
    all_stocks: bool,
    after: str | None,
    quarter: str,
    start_year: int,
    end_year: int,
    max_reports: int,
    reparse: bool,
    refresh_sources: bool,
    apply: bool,
):
    """Recover consolidated statement items from original periodic reports; does not use comparative-period figures, and does not pass off today's collection as a historical observation."""
    from cnequity.cli._shared import comma_values
    from cnequity.storage.repairs.financial_originals import repair_financial_originals

    try:
        _emit(
            repair_financial_originals(
                _cfg(config_path),
                symbols=comma_values(symbols, "--symbols") if symbols else None,
                all_stocks=all_stocks,
                after=after,
                quarter=int(quarter[-1]),
                start_year=start_year,
                end_year=end_year,
                max_reports=max_reports,
                reparse=reparse,
                refresh_sources=refresh_sources,
                apply=apply,
            )
        )
    except (ValueError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc


@repair.command("fund-no-print")
@config_option
@click.option(
    "--apply",
    is_flag=True,
    help=N_(
        "Approve fund auction-bar coverage evidence and retire corresponding debts without creating bars or halt rows."
    ),
)
@click.option(
    "--refresh-sources",
    is_flag=True,
    help=N_("Fetch historical SSE/SZSE fund reports; replays retained reports offline by default."),
)
@click.option(
    "--max-sessions",
    type=click.IntRange(min=1),
    default=32,
    show_default=True,
    help=N_("Maximum owed trading sessions to review per batch."),
)
def fund_no_print(config_path: str, apply: bool, refresh_sources: bool, max_sessions: int):
    """Check debts against explicit fund auction daily-bar coverage evidence from the Shanghai and Shenzhen exchanges; being tradable does not mean there was an auction trade that day."""
    from cnequity.storage.repairs.fund_no_print import repair_fund_no_print

    _emit(
        repair_fund_no_print(
            _cfg(config_path),
            apply=apply,
            refresh_sources=refresh_sources,
            max_sessions=max_sessions,
        )
    )


@repair.command("adj-factors")
@config_option
@click.option(
    "--apply",
    is_flag=True,
    help=N_(
        "Publish supported factor corrections and retain old revisions; unresolved events remain advisory."
    ),
)
@click.option(
    "--refresh-sources",
    is_flag=True,
    help=N_("Fetch independent factor evidence; uses local caches only by default."),
)
@comma_option(
    "--symbols",
    default=None,
    help=N_("Comma-separated symbols; checks historical issues in bounded batches by default."),
)
@click.option(
    "--start",
    default=None,
    help=N_("Start of the event window (YYYY-MM-DD); cumulative effects continue afterward."),
)
@click.option(
    "--end",
    default=None,
    help=N_("End of the event window (YYYY-MM-DD); cumulative effects continue afterward."),
)
@click.option(
    "--max-symbols",
    default=100,
    type=click.IntRange(min=1),
    show_default=True,
    help=N_("Maximum symbols per batch; saves historical progress after applying."),
)
@click.option(
    "--plan",
    default=None,
    help=N_("Require the specified preview plan ID; changed inputs require a new preview."),
)
def adj_factors(config_path, apply, refresh_sources, symbols, start, end, max_symbols, plan):
    """Repair stock factors and ETF/LOF share conversions together; previews offline and saves a plan by default."""
    from cnequity.cli._shared import comma_values, parse_date_option
    from cnequity.storage.repairs.adj_factors import repair_adj_factors

    try:
        _emit(
            repair_adj_factors(
                _cfg(config_path),
                apply=apply,
                refresh_sources=refresh_sources,
                symbols=comma_values(symbols, "--symbols"),
                start=parse_date_option(start, "--start"),
                end=parse_date_option(end, "--end"),
                max_symbols=max_symbols,
                plan_id=plan,
            )
        )
    except (ValueError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc


@repair.command("index-snapshots")
@config_option
@comma_option(
    "--indices",
    default=None,
    help=N_(
        "Comma-separated indices; defaults to CSI 300, STAR 50, Shenzhen Component and ChiNext."
    ),
)
@click.option(
    "--apply",
    is_flag=True,
    help=N_(
        "Replace complete index snapshots using their actual file dates; retain old revisions."
    ),
)
@click.option(
    "--refresh-sources",
    is_flag=True,
    help=N_("Fetch official constituent and weight files; replay offline by default."),
)
def index_snapshots(config_path: str, indices: str | None, apply: bool, refresh_sources: bool):
    """Recover the official dated constituents and real weights that are obtainable; does not project backwards or fill weights across dates."""
    from cnequity.cli._shared import comma_values
    from cnequity.storage.repairs.index_snapshots import repair_index_snapshots

    try:
        _emit(
            repair_index_snapshots(
                _cfg(config_path),
                indices=comma_values(indices, "--indices"),
                apply=apply,
                refresh_sources=refresh_sources,
            )
        )
    except (ValueError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc


@repair.command("index-history")
@config_option
@click.option(
    "--index-symbol",
    type=click.Choice(["399001.SZ", "000688.SH"]),
    default="399001.SZ",
    show_default=True,
    help=N_("Select a reviewed historical membership catalog."),
)
@click.option(
    "--apply",
    is_flag=True,
    help=N_(
        "Replace whole membership history within the reviewed range; retain official dated weights and old revisions."
    ),
)
@click.option(
    "--refresh-sources",
    is_flag=True,
    help=N_(
        "Archive missing official historical attachments from the catalog; replay offline by default."
    ),
)
def index_history(config_path: str, index_symbol: str, apply: bool, refresh_sources: bool):
    """Replay the verified historical announcements of the SZSE Component Index or STAR 50; each effective date stores the full list, with unknown historical weights left empty."""
    from cnequity.storage.repairs.index_history import repair_index_history

    try:
        _emit(
            repair_index_history(
                _cfg(config_path),
                index_symbol=index_symbol,
                apply=apply,
                refresh_sources=refresh_sources,
            )
        )
    except (ValueError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc


@repair.command("stock-listing-dates")
@config_option
@click.option(
    "--apply",
    is_flag=True,
    help=N_(
        "Publish stock listing dates confirmed by the official IPO calendar; retain old revisions."
    ),
)
@click.option(
    "--refresh-sources",
    is_flag=True,
    help=N_("Fetch CNINFO IPO calendars; replay archived sources by default."),
)
@click.option("--max-sessions", type=click.IntRange(min=1), default=32, show_default=True)
@click.option(
    "--event-date",
    multiple=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help=N_(
        "Replay official listing events for an additional date; repeatable and usable before a stock has daily bars."
    ),
)
def stock_listing_dates(
    config_path: str, apply: bool, refresh_sources: bool, max_sessions: int, event_date: tuple
):
    """Fill stock listing dates confirmed by the official calendar; the first daily bar or an explicit date only locates the request."""
    from cnequity.storage.repairs.stock_listing_dates import repair_stock_listing_dates

    _emit(
        repair_stock_listing_dates(
            _cfg(config_path),
            apply=apply,
            refresh_sources=refresh_sources,
            max_sessions=max_sessions,
            event_dates=tuple(day.date() for day in event_date),
        )
    )


@repair.command("layout")
@click.argument("dataset")
@config_option
@click.option(
    "--apply",
    is_flag=True,
    help=N_("Write and publish a new version; by default only prints the plan."),
)
def layout(dataset: str, config_path: str, apply: bool):
    """No longer does anything: the lake catalog lays out each partition's files. Removed in 0.26.0."""
    _emit(
        {
            "dataset": dataset.lower(),
            "applied": False,
            "note": _(
                "Nothing to repair: the lake catalog lays out each partition's files. "
                "This command is removed in 0.26.0."
            ),
        }
    )


@repair.command("corporate-action-gaps")
@config_option
@click.option(
    "--apply",
    is_flag=True,
    help=N_(
        "Fetch from Baostock and publish a new version; by default only prints the plan offline."
    ),
)
def corporate_action_gaps(config_path: str, apply: bool):
    """Add the ex-rights events where both the Sina and Baostock factors step but the lake has no record.

    \b
    Based on the evidence of the latest `cne derive adj_factor_source`. A record within 10 days whose date is off, and whose
    terms explain the step, moves to the step date; the rest fetch dividends from Baostock by security and year,
    accepting only rows whose ex-date is exactly the step date and whose terms explain the step. What cannot be explained stays a gap and is never invented.

    """
    from cnequity.storage.repairs.corporate_action_gaps import repair_corporate_action_gaps

    try:
        _emit(repair_corporate_action_gaps(_cfg(config_path), apply=apply))
    except (ValueError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc


@repair.command("status-traded")
@config_option
@click.option(
    "--start",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help=N_("Start of the traded-status repair window (YYYY-MM-DD)."),
)
@click.option(
    "--end",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help=N_("End of the traded-status repair window (YYYY-MM-DD)."),
)
@click.option(
    "--apply",
    is_flag=True,
    help=N_("Publish a new version after verification; by default only prints the plan."),
)
def status_traded(config_path: str, start, end, apply: bool):
    """Fill missing status keys from actual trading bars, leaving stock ST unknown.

    \b
    Only fills stock/ETF keys within known listing periods that have volume above zero and no status row, with source derived_traded.
    ETF/fund rows are written not-ST, since ST does not apply to funds; earlier fund rows written as unknown are corrected.
    Does not overwrite other status rows, does not read days without trades as suspensions, and does not create ST coverage evidence.
    The default preview writes nothing; --apply re-checks the current version, then publishes, keeping the old version.

    """
    from cnequity.storage.repairs.status_traded import repair_status_traded

    if end < start:
        raise click.BadParameter(_("--end must be on or after --start"))
    _emit(repair_status_traded(_cfg(config_path), start=start.date(), end=end.date(), apply=apply))


@repair.command("repartition")
@click.argument("dataset", required=False)
@config_option
@click.option("--all", "do_all", is_flag=True, help=N_("Every dataset whose layout needs it."))
@click.option(
    "--apply", is_flag=True, help=N_("Rewrite and swap the partitions; by default only previews.")
)
def repartition(dataset: str | None, config_path: str, do_all: bool, apply: bool):
    """No longer does anything: the lake catalog lays out each partition's files. Removed in 0.26.0."""
    if dataset and do_all:
        raise click.UsageError(_("Give a dataset or --all, not both."))
    _emit(
        {
            "needs_repartition": [],
            "applied": False,
            "note": _(
                "Nothing to repartition: the lake catalog lays out each partition's files. "
                "This command is removed in 0.26.0."
            ),
        }
    )


# Each of these used to be a mode flag on `cne backfill`, valid for one dataset,
# which made its help a list of exceptions. Each builds a `BackfillRequest` with
# its `REPAIRS` mode and runs it through `run_backfill`, so it keeps the same
# machinery — the window, scope, lineage follow-up and `--plan` output — while
# following the repair convention: a preview by default, `--apply` to run.


def _run_repair(
    mode: str,
    config_path: str,
    apply: bool,
    *,
    start_str: str | None = None,
    end_str: str | None = None,
    symbols_str: str | None = None,
    **fields,
) -> None:
    """Run a ``REPAIRS`` mode on the backfill machinery; a preview unless *apply*."""
    request = BackfillRequest(
        dataset=REPAIRS[mode].dataset,
        start=parse_date_option(start_str, "--start"),
        end=parse_date_option(end_str, "--end"),
        symbols=backfill_cmds._symbol_tuple(symbols_str),
        plan=not apply,
        repair=mode,
        **fields,
    )
    backfill_cmds.run_backfill(config_path, request)
    if not apply:
        click.echo(_("Preview only; add --apply to run it."), err=True)


_window_options = (
    click.option("--start", "start_str", required=True, help=N_("Window start (YYYY-MM-DD).")),
    click.option("--end", "end_str", required=True, help=N_("Window end (YYYY-MM-DD).")),
)


def _with(*decorators):
    def wrap(func):
        for decorator in reversed(decorators):
            func = decorator(func)
        return func

    return wrap


@repair.command("payment-dates")
@config_option
@comma_option("--symbols", "symbols_str", required=True, help=N_("Comma-separated symbols."))
@_with(*_window_options)
@click.option(
    "--issuer-notices-only",
    is_flag=True,
    help=N_(
        "Use issuer implementation announcements only (reviewed list, CNINFO, BSE); unmatched "
        "events stay gaps and Baostock is not queried."
    ),
)
@apply_option
def repair_payment_dates(config_path, symbols_str, start_str, end_str, issuer_notices_only, apply):
    """Repair corporate_actions payment dates: reviewed issuer announcements first, then Baostock.

    A payment date earlier than the ex-date is treated as unknown. With --issuer-notices-only,
    reviewed bonus/transfer share terms are repaired too and Baostock is not queried.
    """
    _run_repair(
        "issuer-notices" if issuer_notices_only else "payment-dates",
        config_path,
        apply,
        symbols_str=symbols_str,
        start_str=start_str,
        end_str=end_str,
    )


@repair.command("delisted-actions")
@config_option
@click.option(
    "--source",
    type=click.Choice(["baostock", "ths"]),
    required=True,
    help=N_("baostock for delisted Shanghai/Shenzhen symbols; ths for delisted Beijing symbols."),
)
@comma_option("--symbols", "symbols_str", help=N_("Comma-separated symbols."))
@click.option("--start", "start_str", default=None, help=N_("Window start (YYYY-MM-DD)."))
@click.option("--end", "end_str", default=None, help=N_("Window end (YYYY-MM-DD)."))
@apply_option
def repair_delisted_actions(config_path, source, symbols_str, start_str, end_str, apply):
    """Fill historical dividends and ex-rights of delisted symbols from Baostock or THS."""
    _run_repair(
        f"{source}-actions",
        config_path,
        apply,
        symbols_str=symbols_str,
        start_str=start_str,
        end_str=end_str,
    )


@repair.command("ex-dates")
@config_option
@click.option(
    "--ex-dates", "ex_dates_str", required=True, help=N_("Comma-separated ex-dates, YYYY-MM-DD.")
)
@comma_option("--symbols", "symbols_str", help=N_("Comma-separated symbols."))
@apply_option
def repair_ex_dates(config_path, ex_dates_str, symbols_str, apply):
    """Request historical ex-rights rows from EastMoney for the given ex-dates, day by day.

    The backfill path's primary source is TDX; only EastMoney's per-day filter reaches rows
    before 2015-09-29.
    """
    raw_dates = [d.strip() for d in ex_dates_str.split(",") if d.strip()]
    _run_repair(
        "ex-dates",
        config_path,
        apply,
        symbols_str=symbols_str,
        ex_dates=tuple(parse_date_option(value, "--ex-dates") for value in raw_dates),
    )


@repair.command("bar-amounts")
@config_option
@_with(*_window_options)
@comma_option("--symbols", "symbols_str", help=N_("Comma-separated symbols."))
@apply_option
def repair_bar_amounts(config_path, start_str, end_str, symbols_str, apply):
    """Fill daily_bars turnover of Sina-filled rows from TDX, after checking OHLC and volume.

    Turnover is filled only when open/high/low/close match and volume differs by under one lot;
    codes TDX lacks keep their Sina rows. Stored prices and volumes are never touched.
    """
    _run_repair(
        "bar-amounts",
        config_path,
        apply,
        start_str=start_str,
        end_str=end_str,
        symbols_str=symbols_str,
    )


@repair.command("bar-turnover")
@config_option
@_with(*_window_options)
@comma_option("--symbols", "symbols_str", help=N_("Comma-separated symbols."))
@click.option(
    "--peer-review",
    is_flag=True,
    help=N_(
        "Review price conflicts against historical exchange prices, TDX and EastMoney; replace "
        "whole bars only with agreeing evidence."
    ),
)
@apply_option
def repair_bar_turnover(config_path, start_str, end_str, symbols_str, peer_review, apply):
    """Replace Shanghai/Shenzhen daily_bars rows whose turnover is missing, zero or misaligned.

    The whole same-day Baostock row replaces it when OHLC agree within half a cent; otherwise,
    or when Baostock has no row, the original values stay.
    """
    _run_repair(
        "bar-turnover",
        config_path,
        apply,
        start_str=start_str,
        end_str=end_str,
        symbols_str=symbols_str,
        peer_review=peer_review,
    )


@repair.command("bse-tip-turnover")
@config_option
@comma_option("--symbols", "symbols_str", required=True, help=N_("Comma-separated symbols."))
@click.option("--date", "day", required=True, help=N_("The session (YYYY-MM-DD)."))
@apply_option
def repair_bse_tip_turnover(config_path, symbols_str, day, apply):
    """Fill Beijing daily_bars turnover for the latest session from the BSE quote board.

    For when the board was unavailable during the daily update. The board is a current
    snapshot, so only the latest session qualifies; Sina is not refetched.
    """
    _run_repair(
        "bse-tip",
        config_path,
        apply,
        symbols_str=symbols_str,
        start_str=day,
        end_str=day,
    )


@repair.command("valuation-outage")
@config_option
@_with(*_window_options)
@apply_option
def repair_valuation_outage(config_path, start_str, end_str, apply):
    """Fill valuation_metrics for an EastMoney snapshot outage from the datacenter report.

    Covers the window after EastMoney's last complete day and before today; writes only when the
    whole market is retrieved.
    """
    _run_repair(
        "valuation-outage",
        config_path,
        apply,
        start_str=start_str,
        end_str=end_str,
    )
