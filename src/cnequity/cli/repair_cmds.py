"""Repairs of stored data, each published as a new retained revision."""

from __future__ import annotations

import json

import click

from cnequity.cli._root import cli
from cnequity.cli._shared import (
    _cfg,
    comma_option,
    comma_values,
    config_option,
)
from cnequity.i18n import N_, _


def _emit(value: dict) -> None:
    click.echo(json.dumps(value, indent=2, ensure_ascii=False, default=str))


@cli.command("migrate")
@config_option
@click.option(
    "--apply",
    is_flag=True,
    help=N_("Run the owed migrations and record each result in meta/state/lake.json."),
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
    migration; meta/state/lake.json records the version that built the lake (it owes nothing
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
            else _("Nothing owed: every migration is recorded in meta/state/lake.json.")
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
    """Merge files under a partitioned dataset's root or a wrong-key directory into the registered partitions.

    \b
    Several observations of one primary key only fill each other's blanks when their non-null values agree exactly; conflicting keys keep one whole row by the canonical rules.
    All original observations of duplicated keys are written to _quarantine as evidence, and old versions are kept.

    """
    from cnequity.storage.repairs.layout import LayoutRepairError, repair_layout

    try:
        _emit(repair_layout(_cfg(config_path), dataset.lower(), apply=apply))
    except LayoutRepairError as exc:
        raise click.ClickException(str(exc)) from exc


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
    """Rewrite a dataset's partitions at its registered granularity.

    \b
    Without a dataset or --all it lists what is out of layout. With one it previews the effect
    (files, partitions, size); --apply rewrites under the lake lock and swaps the dataset root.
    Reads work whatever span the directories have, so this reclaims space and file opens rather
    than fixing correctness: the audit points here when a granularity change left an old layout.

    """
    from cnequity.storage.repairs.repartition import (
        RepartitionError,
        repartition_candidates,
        repartition_dataset,
    )

    if dataset and do_all:
        raise click.UsageError(_("Give a dataset or --all, not both."))
    cfg = _cfg(config_path)
    candidates = repartition_candidates(cfg)
    if not dataset and not do_all:
        _emit({"needs_repartition": candidates})
        return
    results = []
    for name in [dataset.lower()] if dataset else candidates:
        try:
            res = repartition_dataset(cfg, name, dry_run=not apply)
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
    _emit({"applied": apply, "results": results})
