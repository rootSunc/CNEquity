"""`backfill` and the chunking, scoping and recovery it needs.

The helpers are the bulk of it: a backfill is one command with several failure
modes that each need their own repair path (symbol-chunked, day-chunked, and
recovering staging an interrupted terminal run left behind).
"""

from __future__ import annotations

import difflib
import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

import click

from cnequity.cli._root import cli
from cnequity.cli._shared import (
    _cfg,
    _run_status_exit_code,
    attach_log_file,
    comma_option,
    comma_values,
    config_option,
    parse_date_option,
)
from cnequity.domain.datasets import get_dataset
from cnequity.domain.market_time import shanghai_today
from cnequity.i18n import N_, _
from cnequity.orchestrator.engine import JobEngine


@cli.command()
@click.argument("dataset")
@config_option
@click.option(
    "--profile",
    type=click.Choice(["default", "delisted"]),
    default="default",
    show_default=True,
    help=N_(
        "daily_bars accepts delisted: backfill only the confirmed delisting list, reusing the independent discovery evidence."
    ),
)
@click.option(
    "--plan",
    is_flag=True,
    help=N_(
        "Print only sources, scope and fetch method, plus a request budget for derivatives. Fetches no data and does not write the lake."
    ),
)
@click.option(
    "--exchange",
    "exchanges",
    multiple=True,
    type=click.Choice(["SHF", "INE", "CZC", "GFE", "DCE", "CFE"], case_sensitive=False),
    help=N_(
        "Derivatives only: restrict to an exchange; repeatable. INE is routed via SHF; 2018 futures also fetch INE daily files."
    ),
)
@click.option(
    "--source",
    default=None,
    help=N_(
        "Source to use for this backfill instead of the configured one, without editing the config. "
        "Supported for margin_trading (exchange or eastmoney) and financial_statement_items "
        "(tdx_fin: fill only the items the lake lacks)."
    ),
)
@click.option(
    "--refetch",
    is_flag=True,
    help=N_(
        "Ignore completion receipts and checkpoints and fetch the range again (sector_bars, "
        "futures_bars, option_bars); circuit breakers still apply. Without it a backfill resumes."
    ),
)
@click.option(
    "--start",
    "start_str",
    default=None,
    help=(
        N_(
            "Backfill start date (YYYY-MM-DD), covering daily_bars, minute_bars, derivatives daily bars, date/report-period advancement and sector_bars. sector_bars defaults to 400 days back; ranges beyond the source's history depth are kept as gaps, while the available range is delivered as usual with guidance for filling the rest."
        )
    ),
)
@click.option(
    "--end",
    "end_str",
    default=None,
    help=(
        N_(
            "Backfill end date (YYYY-MM-DD, default today); with --start, bounds the historical window."
        )
    ),
)
@click.option(
    "--outstanding",
    is_flag=True,
    help=(
        N_(
            "Repair only the keys owed by tolerated gaps; scope and window come from the debt ledger, ignoring --symbols/--start/--end. Filled keys are cleared from the ledger; keys still missing stay owed."
        )
    ),
)
@comma_option(
    "--symbols",
    "symbols_str",
    default=None,
    help=(
        N_(
            "Comma-separated symbols to restrict the scope: for scoped backfills of intraday, trading_status and corporate_actions, and scoped repairs of financial_statement_items, daily_bars and share_structure. trading_status records the exact scope in its checkpoint and coverage evidence; daily_bars writes this explicit scope into the backfill metadata."
        )
    ),
)
@click.option(
    "--workers",
    default=1,
    show_default=True,
    help=N_(
        "margin_trading only: concurrency for date advancement. Each request still goes through the shared source rate limiter from the config; must be 1 for other datasets."
    ),
)
def backfill(
    dataset: str,
    config_path: str,
    profile: str,
    plan: bool,
    exchanges: tuple[str, ...],
    start_str: str | None,
    end_str: str | None,
    symbols_str: str | None,
    outstanding: bool,
    workers: int,
    source: str | None = None,
    refetch: bool = False,
    *,
    # Modes of the `cne repair` / `cne import` commands below, which run on this
    # machinery; they are not options of `cne backfill` itself.
    shfe_annual_archive: Path | None = None,
    archive_year: int | None = None,
    accept_partial_fields: bool = False,
    archive_url: str | None = None,
    archive_downloaded_at: str | None = None,
    refresh: bool = False,
    force: bool = False,
    margin_source: str | None = None,
    baostock_repair: bool = False,
    payment_date_repair: bool = False,
    issuer_notice_repair: bool = False,
    ths_repair: bool = False,
    eastmoney_date_repair: bool = False,
    ex_dates_str: str | None = None,
    bse_tip_repair: bool = False,
    tdx_amount_repair: bool = False,
    turnover_repair: bool = False,
    turnover_peer_review: bool = False,
    fill_em_outage: bool = False,
):
    """Backfill a dataset.

    \b
    Cost is counted in the source's billing unit, not by window. daily_bars fetches symbol by symbol, so `--start D --end D`
    sweeps the whole market just like a multi-year window: one session is not one request.
    To verify quickly rather than run the whole market, narrow the scope with `--symbols`.

    """
    dataset = _require_known_dataset(dataset)
    if source == "tdx_fin" and dataset == "financial_statement_items":
        _backfill_financials_tdx_fin(config_path, start_str, end_str)
        return
    if source is not None:
        if dataset != "margin_trading":
            raise click.ClickException(
                _(
                    "--source is supported for margin_trading and financial_statement_items (tdx_fin)"
                )
            )
        if source not in {"exchange", "eastmoney"}:
            raise click.BadParameter(
                _("margin_trading sources are exchange and eastmoney"), param_hint="--source"
            )
        margin_source = source
    if refetch:
        if dataset == "sector_bars":
            force = True
        elif dataset in {"futures_bars", "option_bars"}:
            refresh = True
        else:
            raise click.ClickException(
                _("--refetch applies to sector_bars, futures_bars and option_bars")
            )
    if turnover_peer_review and not turnover_repair:
        raise click.ClickException(_("--turnover-peer-review needs --turnover-repair"))
    if profile == "delisted":
        if dataset != "daily_bars":
            raise click.ClickException(_("--profile delisted only applies to daily_bars"))
        if (
            end_str
            or symbols_str
            or outstanding
            or exchanges
            or refresh
            or force
            or workers != 1
            or margin_source
            or baostock_repair
            or payment_date_repair
            or issuer_notice_repair
            or ths_repair
            or eastmoney_date_repair
            or ex_dates_str
            or bse_tip_repair
            or tdx_amount_repair
            or turnover_repair
            or fill_em_outage
            or shfe_annual_archive
            or archive_year
            or accept_partial_fields
            or archive_url
            or archive_downloaded_at
        ):
            raise click.ClickException(
                _("--profile delisted only accepts --start, --plan and --config")
            )
        since = parse_date_option(start_str or "2016-01-01", "--start")
        if plan:
            click.echo(
                json.dumps(
                    {
                        "dataset": dataset,
                        "profile": profile,
                        "since": since.isoformat(),
                        "source": "sina",
                        "mode": "confirmed_delisted_catalog",
                        "note": "When run, resumes over the pending symbols of the delisted directory; the request count depends on the directory and completed receipts",
                    },
                    indent=2,
                )
            )
            return
        cfg = _cfg(config_path)
        attach_log_file(cfg, "delisted-backfill")
        window = _bar_lineage_window(since, None)
        bars_before = _bar_fingerprint(cfg, window)
        result = _run_delisted_profile(cfg, since)
        if result.get("status") != "failed":
            _follow_lineage(result, lambda: _after_daily_bars(cfg, bars_before, window, result))
        click.echo(json.dumps(result, indent=2, default=str))
        if _run_status_exit_code(result["status"]):
            raise click.ClickException("delisted recovery execution or publication failed")
        return
    if shfe_annual_archive is not None:
        if dataset not in {"futures_bars", "option_bars"}:
            raise click.ClickException(
                _("--shfe-annual-archive only applies to futures_bars/option_bars")
            )
        if archive_year is None or not accept_partial_fields:
            raise click.ClickException(
                _(
                    "The annual package lacks daily-file fields; --archive-year and --accept-partial-fields are required"
                )
            )
        if archive_url:
            parsed_url = urlsplit(archive_url)
            host = parsed_url.hostname or ""
            if parsed_url.scheme != "https" or not (
                host == "shfe.com.cn" or host.endswith(".shfe.com.cn")
            ):
                raise click.ClickException(
                    _("--archive-url must be an HTTPS link on the SHFE website")
                )
        if archive_downloaded_at:
            try:
                downloaded = datetime.fromisoformat(archive_downloaded_at.replace("Z", "+00:00"))
            except ValueError as exc:
                raise click.ClickException(
                    _("--archive-downloaded-at needs an ISO 8601 time")
                ) from exc
            if downloaded.tzinfo is None:
                raise click.ClickException(_("--archive-downloaded-at must include a time zone"))
            archive_downloaded_at = downloaded.isoformat()
        if (
            profile != "default"
            or symbols_str
            or outstanding
            or exchanges
            or refresh
            or force
            or workers != 1
            or margin_source
            or baostock_repair
            or payment_date_repair
            or issuer_notice_repair
            or ths_repair
            or eastmoney_date_repair
            or ex_dates_str
            or bse_tip_repair
            or tdx_amount_repair
            or turnover_repair
            or fill_em_outage
        ):
            raise click.ClickException(
                _("Annual package import only accepts --start/--end/--plan and the archive options")
            )
        start = parse_date_option(start_str, "--start") if start_str else None
        end = parse_date_option(end_str, "--end") if end_str else None
        first, last = date(archive_year, 1, 1), date(archive_year, 12, 31)
        if (start and not first <= start <= last) or (end and not first <= end <= last):
            raise click.ClickException(_("--start/--end must fall inside --archive-year"))
        if start and end and start > end:
            raise click.ClickException(_("--start must not be after --end"))
        if plan:
            click.echo(
                json.dumps(
                    {
                        "dataset": dataset,
                        "archive": str(shfe_annual_archive),
                        "year": archive_year,
                        "start": str(start or first),
                        "end": str(end or last),
                        "mode": "offline_partial_fields",
                        "existing_daily_rows": "protected",
                        "network_requests": 0,
                    },
                    indent=2,
                )
            )
            return
        cfg = _cfg(config_path)
        attach_log_file(cfg, "shfe-annual-import")
        result = _run_shfe_annual_archive(
            cfg,
            dataset,
            shfe_annual_archive,
            archive_year,
            start=start,
            end=end,
            source_url=archive_url,
            downloaded_at=archive_downloaded_at,
        )
        click.echo(json.dumps(result, indent=2, default=str))
        if result["status"] != "success":
            raise click.ClickException(
                _(
                    "The annual package has unverified members or rows; the valid slices and the gaps were saved"
                )
            )
        return
    if archive_year is not None or accept_partial_fields or archive_url or archive_downloaded_at:
        raise click.ClickException(_("The annual archive options need --shfe-annual-archive ZIP"))
    symbols = comma_values(symbols_str, "--symbols")
    if symbols is not None:
        symbols = list(dict.fromkeys(s.upper() for s in symbols))
        symbols_str = ",".join(symbols)
    cfg = _cfg(config_path)
    derivatives = {
        "futures_bars",
        "option_bars",
        "futures_contracts",
        "option_contracts",
        "futures_minute_bars",
    }
    if (exchanges or refresh) and dataset not in derivatives:
        raise click.ClickException(_("--exchange / --refresh only apply to derivatives datasets"))
    if force and dataset != "sector_bars":
        raise click.ClickException(
            _(
                "--force / --retry-failed only apply to sector_bars; to refetch derivatives use --refresh"
            )
        )
    if dataset in derivatives:
        cfg.futures_enabled = True
        if dataset.startswith("option_"):
            cfg.futures_options = True
        if exchanges:
            cfg.futures_exchanges = [e.upper() for e in exchanges]
        if refresh and dataset not in {"futures_bars", "option_bars"}:
            raise click.ClickException(_("--refresh only applies to futures_bars / option_bars"))
        if outstanding:
            raise click.ClickException(
                _(
                    "Derivatives debts are retried persistently by the daily update; give --start / --end to repair a historical range"
                )
            )
        if dataset == "futures_minute_bars" and (start_str or end_str):
            raise click.ClickException(
                _(
                    "futures_minute_bars can only collect a recent window and cannot request history by --start / --end"
                )
            )
        if symbols_str and dataset != "futures_minute_bars":
            raise click.ClickException(
                _(
                    "Derivatives daily bars are fetched from exchange files; use --exchange, --symbols is not supported"
                )
            )
        import uuid

        cfg._derivative_batch = uuid.uuid4().hex
        cfg._derivatives_refresh = refresh
    if margin_source:
        if dataset != "margin_trading":
            raise click.ClickException(_("--margin-source only applies to margin_trading"))
        cfg.margin_trading_source = margin_source
    if payment_date_repair or issuer_notice_repair:
        if dataset != "corporate_actions" or not symbols_str or not start_str or not end_str:
            raise click.ClickException(
                _("Payment-date repair needs corporate_actions, --symbols, --start and --end")
            )
        if payment_date_repair and issuer_notice_repair:
            raise click.ClickException(
                _("The two payment-date repair modes cannot be used together")
            )
        if any((baostock_repair, ths_repair, eastmoney_date_repair, outstanding)):
            raise click.ClickException(_("Payment-date repair must run on its own"))
        cfg._corporate_actions_payment_repair = True
        cfg._corporate_actions_issuer_notice_only = issuer_notice_repair
    if workers < 1:
        raise click.ClickException(_("--workers must be at least 1"))
    if workers > 1 and dataset != "margin_trading":
        raise click.ClickException(
            _(
                "--workers > 1 is only supported for margin_trading for now; other backfills use a single date-advancing lane"
            )
        )
    if baostock_repair and dataset != "corporate_actions":
        raise click.ClickException(_("--baostock-repair only applies to corporate_actions"))
    if ths_repair and dataset != "corporate_actions":
        raise click.ClickException(_("--ths-repair only applies to corporate_actions"))
    if eastmoney_date_repair and dataset != "corporate_actions":
        raise click.ClickException(_("--eastmoney-date-repair only applies to corporate_actions"))
    if ex_dates_str and not eastmoney_date_repair:
        raise click.ClickException(_("--ex-dates needs --eastmoney-date-repair"))
    if bse_tip_repair and dataset != "daily_bars":
        raise click.ClickException(_("--bse-tip-repair only applies to daily_bars"))
    if tdx_amount_repair and dataset != "daily_bars":
        raise click.ClickException(_("--tdx-amount-repair only applies to daily_bars"))
    if baostock_repair:
        cfg._corporate_actions_baostock_repair = True
    if ths_repair:
        cfg._corporate_actions_ths_repair = True
    if eastmoney_date_repair:
        raw_dates = [d.strip() for d in (ex_dates_str or "").split(",") if d.strip()]
        if not raw_dates:
            raise click.ClickException(_("--eastmoney-date-repair needs --ex-dates"))
        seen = {parse_date_option(value, "--ex-dates") for value in raw_dates}
        cfg._corporate_actions_eastmoney_date_repair = sorted(seen)
    if dataset == "sector_bars":
        cfg._sector_bars_force = force
    start_d = parse_date_option(start_str, "--start")
    end_d = parse_date_option(end_str, "--end")
    if start_d and end_d and start_d > end_d:
        # Transposing the two used to cost a full network sweep: the walk had no
        # days in it, the step raised, the engine logged the traceback, and the
        # command still printed status=success with rows_written=0. `derive`,
        # `verify --bars` and `audit` all refuse this up front; so does this now.
        raise click.ClickException(_("--start must be on or before --end"))
    if tdx_amount_repair:
        if start_d is None or end_d is None:
            raise click.ClickException(_("--tdx-amount-repair needs both --start and --end"))
        cfg._tdx_amount_repair = True
    if turnover_repair:
        if dataset != "daily_bars":
            raise click.ClickException(_("--turnover-repair only applies to daily_bars"))
        if start_d is None or end_d is None:
            raise click.ClickException(_("--turnover-repair needs both --start and --end"))
        cfg._turnover_repair = True
        cfg._turnover_peer_review = turnover_peer_review
    if fill_em_outage:
        if dataset != "valuation_metrics":
            raise click.ClickException(_("--fill-em-outage only applies to valuation_metrics"))
        if start_d is None or end_d is None:
            raise click.ClickException(_("--fill-em-outage needs both --start and --end"))
        cfg._valuation_fill_em_outage = True
    if bse_tip_repair:
        if not symbols_str:
            raise click.ClickException(_("--bse-tip-repair needs --symbols"))
        if start_d is None or end_d is None or start_d != end_d:
            raise click.ClickException(
                _("--bse-tip-repair needs an explicit --start and --end on the same day")
            )
        cfg._bse_tip_repair = True
    if outstanding:
        if plan:
            raise click.ClickException(
                _("--plan needs a dataset scope; --outstanding repairs from the debt ledger")
            )
        if symbols_str or start_d or end_d:
            raise click.ClickException(
                _(
                    "--outstanding takes its scope from the debt ledger; drop --symbols/--start/--end"
                )
            )
        attach_log_file(cfg, f"backfill-{dataset}")
        result = _repair_outstanding(cfg, dataset, workers)
        click.echo(json.dumps(result, indent=2, default=str))
        if _fetched_nothing(result):
            _explain_fetched_nothing()
            raise SystemExit(1)
        if code := _run_status_exit_code(result["status"]):
            raise SystemExit(code)
        return
    if symbols is not None:
        if dataset in (
            "daily_bars",
            "trading_status",
            "corporate_actions",
            "financial_statement_items",
            "share_structure",
        ):
            cfg._backfill_symbols = symbols
        else:
            _override_scope(cfg, dataset, symbols)
        if not plan:
            click.echo(
                _("[{dataset}] this run's scope is overridden to {value} symbols").format(
                    dataset=dataset, value=len(symbols)
                ),
                err=True,
            )
    if start_d:
        cfg._backfill_start = start_d
    if end_d:
        cfg._backfill_end = end_d
    cfg._backfill_workers = workers
    if plan:
        repair_modes = [
            flag
            for enabled, flag in (
                (payment_date_repair, "payment-date-repair"),
                (issuer_notice_repair, "issuer-notice-repair"),
                (baostock_repair, "baostock-repair"),
                (ths_repair, "ths-repair"),
                (eastmoney_date_repair, "eastmoney-date-repair"),
                (bse_tip_repair, "bse-tip-repair"),
                (tdx_amount_repair, "tdx-amount-repair"),
                (turnover_repair, "turnover-repair"),
                (fill_em_outage, "fill-em-outage"),
                (force, "force"),
            )
            if enabled
        ]
        payload = (
            _derivatives_plan(cfg, dataset, start_str, end_str, symbols_str)
            if dataset in derivatives
            else _backfill_plan(cfg, dataset, start_d, end_d, symbols, workers, repair_modes)
        )
        click.echo(json.dumps(payload, indent=2, default=str, ensure_ascii=False))
        return
    attach_log_file(cfg, f"backfill-{dataset}")
    if dataset == "trading_status":
        # The per-run bound exists so `cne init` is not held behind baostock's
        # pacing for ten hours. Asking for this backfill by name *is* asking to
        # sit through it, and silently stopping at 400 symbols would look like
        # the command had finished the job.
        cfg.st_history_symbols_per_run = 0

    spec = get_dataset(dataset)
    watermark_before = _watermark(cfg, dataset)
    # Explicit lineage: fingerprint the inputs a derive reads before fetching,
    # so the follow-up below derives exactly what this backfill changed.
    actions_before = bars_before = bar_window = None
    if dataset == "corporate_actions":
        from cnequity.derive.lineage import corporate_action_fingerprint

        actions_before = corporate_action_fingerprint(cfg)
    elif dataset == "daily_bars":
        bar_window = _bar_lineage_window(start_d, end_d)
        bars_before = _bar_fingerprint(cfg, bar_window)
    # Offset-paged sources (intraday) chunk by symbol, not by date, so each
    # symbol pays for locating the window only once.
    if spec.backfill_chunk_symbols and start_d and end_d:
        result = _backfill_symbol_chunked(cfg, dataset, start_d, end_d, spec.backfill_chunk_symbols)
    elif spec.backfill_chunk_days and start_d and end_d:
        result = _backfill_chunked(cfg, dataset, start_d, end_d, spec.backfill_chunk_days)
    else:
        result = _backfill_once(cfg, dataset)
    if (
        dataset in {"futures_bars", "option_bars", "futures_contracts", "option_contracts"}
        and result.get("status") != "failed"
    ):
        steps = _derivatives_followup(dataset)
        followup = JobEngine(cfg).run_job("derivatives-rebuild", steps=steps, backfill=True)
        result["followup"] = followup
        if followup.get("status") != "success":
            result["status"] = followup.get("status", "degraded")
    fetched_nothing = _fetched_nothing(result)
    if actions_before is not None and result.get("status") != "failed" and not fetched_nothing:
        result["adj_factors_sync"] = _follow_lineage(
            result, lambda: _after_corporate_actions(cfg, actions_before, result)
        )
    if bars_before is not None and result.get("status") != "failed" and not fetched_nothing:
        _follow_lineage(result, lambda: _after_daily_bars(cfg, bars_before, bar_window, result))
    if outstanding:
        result["outstanding"] = _settle_outstanding(cfg, dataset)
    _explain_watermark_pullback(cfg, dataset, watermark_before, result)
    click.echo(json.dumps(result, indent=2, default=str))
    if fetched_nothing:
        _explain_fetched_nothing()
        raise SystemExit(1)
    code = _run_status_exit_code(result["status"])
    if code:
        raise SystemExit(code)


def _fetched_nothing(result: dict) -> bool:
    """A backfill that asked for data and got nothing usable, and wrote nothing.

    The step still reports `degraded`, so a daily run can publish what it has;
    a backfill exists to fill this range, and having filled none of it is a failure.
    A `success` is left alone: a step that found nothing new may report no coverage
    evidence, and that is not a source failure.
    """
    return (
        result.get("status") != "success"
        and result.get("usable_result") is False
        and not result.get("rows_written")
    )


def _explain_fetched_nothing() -> None:
    click.echo(
        _(
            "✗ This backfill fetched nothing usable, so the range stays a gap in the coverage "
            "record. `fallback` above gives the reason (sources unavailable, or unable to serve "
            "this range) and the sources tried; the log has the reason for each key."
        ),
        err=True,
    )


def _watermark(cfg, dataset: str) -> date | None:
    """Current watermark, or None; this feeds an advisory note and must not fail a backfill."""
    from cnequity.storage.state import StateStore

    meta_root = getattr(cfg, "meta_root", None)
    if meta_root is None:
        return None
    try:
        return StateStore(meta_root).get_date(dataset)
    except (OSError, ValueError):
        return None


def _explain_watermark_pullback(cfg, dataset: str, before: date | None, result: dict) -> None:
    """Say why a backfill made the dataset read STALE.

    A dense dataset's watermark stops before its first missing session, so an
    older window that leaves a hole between itself and the existing data pulls
    the watermark back. That is deliberate — the next run fills the hole — but
    without a word here `cne status` simply turned STALE after a successful run.
    """
    after = _watermark(cfg, dataset)
    if before is None or after is None or after >= before:
        return
    gap_start = after + timedelta(days=1)
    result["watermark"] = {"before": before.isoformat(), "after": after.isoformat()}
    click.echo(
        _(
            "[{dataset}] watermark moved back from {before} to {after}: sessions after {after} "
            "are not all in the lake, so it reads STALE until they are. The next daily run "
            "fills them, or now: `cne backfill {dataset} --start {start}`"
        ).format(
            dataset=dataset,
            before=before.isoformat(),
            after=after.isoformat(),
            start=gap_start.isoformat(),
        ),
        err=True,
    )


def _follow_lineage(result: dict, derive) -> dict:
    """Run a downstream derive; its failure degrades the backfill, never hides it.

    The fetched rows are already published by this point, so the command's own
    output must still reach the caller with the derive's failure recorded.
    """
    try:
        summary = derive()
    except Exception as exc:  # noqa: BLE001 — report it alongside the published fetch
        logging.getLogger(__name__).exception("downstream derive failed")
        click.echo(
            _("Downstream derivation failed: {name}: {exc}").format(
                name=type(exc).__name__, exc=exc
            ),
            err=True,
        )
        summary = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
    if (
        summary.get("status") in {"failed", "degraded", "warning"}
        or summary.get("synchronized") is False
    ) and result.get("status") == "success":
        result["status"] = "degraded"
    return summary


def _after_corporate_actions(cfg, before, result: dict) -> dict:
    """Refetch and check factors for the symbols whose action terms changed."""
    from cnequity.derive.lineage import changed_symbols, corporate_action_fingerprint

    affected = changed_symbols(before, corporate_action_fingerprint(cfg))
    click.echo(_("\nCorporate actions backfill finished"), err=True)
    click.echo(
        _("  Written:            {value:,} records").format(
            value=int(result.get("rows_written") or 0)
        ),
        err=True,
    )
    click.echo(_("  Affected symbols:   {value:,}").format(value=len(affected)), err=True)
    if not affected:
        click.echo(
            _("✓ Corporate actions changed no adjustment-factor inputs; nothing to recompute"),
            err=True,
        )
        return {"affected_symbols": 0, "synchronized": True}
    return _sync_adj_factors(
        cfg,
        affected,
        realign=False,
        input_label=_("corporate actions"),
        rerun="cne backfill corporate_actions --symbols",
    )


def _bar_lineage_window(start: date | None, end: date | None) -> tuple[date, date]:
    """The bar range a daily_bars backfill can touch: its own default when unset."""
    from cnequity.steps.common import BACKFILL_START

    return start or BACKFILL_START, end or shanghai_today()


def _bar_fingerprint(cfg, window: tuple[date, date]):
    from cnequity.derive.lineage import daily_bar_fingerprint

    return daily_bar_fingerprint(cfg, *window)


def _after_daily_bars(cfg, before, window: tuple[date, date], result: dict) -> dict:
    """Realign factors and re-derive suspensions over the bars that changed.

    Same lineage as corporate actions: the next daily run would only realign a
    capped batch of symbols with history the factor table does not reach, and
    no daily step re-derives suspensions at all.
    """
    from cnequity.derive.lineage import changed_bar_scope
    from cnequity.steps.reference import DERIVE_TAIL_DAYS

    affected, first, last = changed_bar_scope(before, _bar_fingerprint(cfg, window))
    click.echo(_("\nDaily bars backfill finished"), err=True)
    click.echo(
        _("  Written:            {value:,} rows").format(
            value=int(result.get("rows_written") or 0)
        ),
        err=True,
    )
    click.echo(_("  Affected symbols:   {value:,}").format(value=len(affected)), err=True)
    if not affected:
        click.echo(
            _("✓ Daily bars did not change; adjustment factors and suspensions need no recompute"),
            err=True,
        )
        summary = {"affected_symbols": 0, "synchronized": True}
        result["adj_factors_sync"] = summary
        return summary
    click.echo(
        _("  Months changed:     {first:%Y-%m} .. {last:%Y-%m}").format(first=first, last=last),
        err=True,
    )
    result["adj_factors_sync"] = _follow_lineage(
        result,
        lambda: _sync_adj_factors(
            cfg,
            affected,
            realign=True,
            input_label=_("daily bars"),
            rerun="cne backfill daily_bars --symbols",
        ),
    )
    # A halt shows only as a gap between two traded bars, so the window reaches
    # back past the first changed month by the derive's own tail.
    month_end = (last.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    start = first - timedelta(days=DERIVE_TAIL_DAYS)
    end = min(month_end, window[1])
    result["trading_status_derive"] = _follow_lineage(
        result, lambda: _derive_suspensions(cfg, start, end)
    )
    return {"affected_symbols": len(affected), "status": result.get("status")}


def _sync_adj_factors(
    cfg, affected: list[str], *, realign: bool, input_label: str, rerun: str
) -> dict:
    """Derive factors for *affected*, then check each reaches its latest traded bar.

    Explicit lineage instead of hoping the next daily run notices: see
    `cnequity.derive.lineage`. *realign* keeps cached factors (bars changed,
    factors did not); otherwise the factors are refetched. A symbol left
    unsynchronized degrades the command.
    """
    from cnequity.cli.maintain_cmds import _published_derive
    from cnequity.derive.adj_factors import compute_adj_factors
    from cnequity.derive.lineage import verify_factor_sync

    click.echo(_("\nDeriving adjustment factors…"), err=True)
    with _published_derive(cfg, "adj_factors") as outcome:
        derived = (
            compute_adj_factors(cfg, realign_symbols=affected)
            if realign
            else compute_adj_factors(cfg, refresh_symbols=affected)
        )
        outcome["rows_written"] = derived.rows
        if derived.failed:
            outcome["status"] = "degraded"
    sync = verify_factor_sync(cfg, affected, failed=derived.failed, rows=derived.rows)
    reasons = {"no_bars": _("no daily bars"), "cdr": "CDR"}
    skipped = ", ".join(
        f"{reasons[r]} {sum(v == r for v in sync.skipped.values())}"
        for r in reasons
        if r in sync.skipped.values()
    )
    click.echo(_("  Processed:          {value:,}").format(value=len(affected)), err=True)
    click.echo(_("  Updated:            {value:,}").format(value=len(sync.updated)), err=True)
    click.echo(
        _("  Skipped:            {value:,}").format(value=len(sync.skipped))
        + (_(" ({skipped})").format(skipped=skipped) if skipped else ""),
        err=True,
    )
    if sync.synchronized:
        click.echo(
            _("\n✓ {input_label} and adjustment factors are in sync").format(
                input_label=input_label
            ),
            err=True,
        )
    else:
        behind = sorted(sync.failed + sync.lagging)
        preview = ",".join(behind[:20])
        click.echo(
            _(
                "  Failed:             {value:,}\n  Missing latest bars: {value2:,}\n\n✗ Adjustment factors of {value3} symbols did not catch up with {input_label}; re-run: cne derive adj_factors or {rerun} {preview}"
            ).format(
                value=len(sync.failed),
                value2=len(sync.lagging),
                value3=len(behind),
                input_label=input_label,
                rerun=rerun,
                preview=preview,
            ),
            err=True,
        )
    return sync.as_dict()


def _derive_suspensions(cfg, start: date, end: date) -> dict:
    """Reconstruct suspensions over the bar window a backfill changed.

    Bar-gap suspensions are not part of the lean daily core: the daily
    `trading_status` snapshot records each session's halts. Only a change to
    bar history can reveal suspensions nobody snapshotted, so the derivation
    follows the command that changes it.
    """
    from cnequity.cli.maintain_cmds import _derive_trading_status

    summary = _derive_trading_status(cfg, start=start, end=end)
    click.echo(
        _(
            "\nRecomputed suspensions from the daily bars of {value}..{value2}: {value3:,} rows ({value4})"
        ).format(
            value=start.isoformat(),
            value2=end.isoformat(),
            value3=int(summary.get("rows_staged") or 0),
            value4=summary.get("status"),
        ),
        err=True,
    )
    return summary


def _backfill_plan(cfg, dataset, start, end, symbols, workers, repair_modes) -> dict:
    from cnequity.diagnostics.source_limits import effective_source_policy
    from cnequity.domain.datasets import history_mode_for
    from cnequity.domain.http_policy import cooldown_status, source_family
    from cnequity.domain.rate_limit import _read_json

    spec = get_dataset(dataset)
    registered = {
        "primary": spec.primary_source,
        "backup": spec.backup_source,
        "backfill": spec.backfill_source,
        "supplementary": list(spec.supplementary_sources),
        "repair_only": list(spec.repair_sources),
    }
    names = {
        name
        for name in (
            spec.primary_source,
            spec.backup_source,
            spec.backfill_source,
            *spec.supplementary_sources,
            *spec.repair_sources,
        )
        if name
    }
    statuses = {}
    for name in sorted(names):
        family = source_family(name)
        statuses[name] = {
            "enabled": cfg.sources.get(name, True)
            and cfg.sources.get(family, True)
            and (not family.startswith("eastmoney") or cfg.sources.get("eastmoney", True))
            and (family != "tdx_protocol" or cfg.tdx_enabled),
            **cooldown_status(cfg.rate_limit_root, name),
            **effective_source_policy(cfg, name, names),
        }
    if dataset == "margin_trading":
        routing = {
            "selected": cfg.margin_trading_source,
            "source_enabled": statuses.get(cfg.margin_trading_source, {}).get("enabled"),
            "confidence": "configured",
        }
    elif dataset == "daily_bars" and symbols:
        routing = {
            "markets": {
                market: sorted(s for s in symbols if s.endswith(f".{market}"))
                for market in ("SH", "SZ", "BJ")
                if any(s.endswith(f".{market}") for s in symbols)
            },
            "confidence": "market-scope-only; actual fallback depends on gaps and source responses",
        }
    else:
        routing = {"confidence": "actual source depends on gaps, route policy and responses"}
    state = _read_json(cfg.meta_root / "state" / f"{dataset}.json")
    outstanding = state.get("outstanding_keys")
    cold_minimum = None
    broad_tip = None
    if dataset == "daily_bars" and symbols:
        cold_minimum = sum(not s.endswith(".BJ") for s in symbols) + int(
            any(s.endswith(".BJ") for s in symbols)
        )
        scoped_history = start is not None and end is not None and start < end
        broad_tip = {
            "exchange": "skip"
            if scoped_history or sum(not s.endswith(".BJ") for s in symbols) <= 4
            else "eligible",
            "bse": "skip"
            if scoped_history or sum(s.endswith(".BJ") for s in symbols) <= 4
            else "eligible",
            "reason": "Explicit history or at most 4 symbols skips the whole-market snapshot; per-symbol requests are still bound by the shared rate limit",
        }
    return {
        "dataset": dataset,
        "data_root": str(cfg.data_root),
        "start": start,
        "end": end,
        "window_note": "Unspecified bounds come from the dataset's incremental watermark and history capability; planning creates no watermark and makes no network request",
        "symbols": symbols,
        "scope": f"explicit {len(symbols)} symbols"
        if symbols
        else "dataset default scope, possibly the whole market",
        "fetch_semantics": spec.fetch_semantics,
        "history_mode": history_mode_for(spec),
        "history_note": (
            "The source only has a current snapshot; history requests report the capability limit normally, keep existing data and give a daily collection plan"
            if history_mode_for(spec) == "snapshot_only"
            else "Ranges beyond the source's capability are recorded as coverage gaps; running fetches whatever range the source can still provide"
        ),
        "registered_sources": registered,
        "source_status": statuses,
        "routing": routing,
        "repair_modes": repair_modes,
        "workers": workers,
        "checkpoint": {
            "watermark": state.get("watermark"),
            "outstanding_keys": len(outstanding) if isinstance(outstanding, list) else 0,
        },
        "margin_source": cfg.margin_trading_source if dataset == "margin_trading" else None,
        "source_note": "The registered source describes capability; per-market routing and explicit repair modes are in the fetch guide, and the actual source is written into the data rows",
        "chunk_symbols": spec.backfill_chunk_symbols,
        "chunk_days": spec.backfill_chunk_days,
        "earliest_available": spec.earliest_available(shanghai_today()),
        "requests": None,
        "cold_request_lower_bound": cold_minimum,
        "broad_tip_snapshots": broad_tip,
        "tdx_history_window": {
            "strategy": "exponential-bracket-binary-seek-then-scan",
            "applies_when": "TDX K-line backfill whose end is before the latest full page; other sources keep their own strategies",
            "cost": "Per symbol: logarithmic locating pages + target window pages; after jumping, one extra check of the latest page",
            "limits": "Limited to history the source still keeps; unknown dates fall back to sequential reads, and a failure commits none of that symbol's partial results",
        }
        if dataset in {"daily_bars", "index_bars", "minute_bars", "minute_bars_5m"}
        else None,
        "cost_note": "Paging, gaps and caches decide the request count; a one-day window is not one request. Verify with a few --symbols first",
        "pacing_seconds": cfg.source_intervals,
        "rate_limit_root": str(cfg.rate_limit_root),
        "writes": False,
    }


def _derivatives_followup(dataset: str) -> list[str]:
    if dataset in {"futures_bars", "futures_contracts"}:
        return ["futures_contracts", "compact", "derive_futures_continuous", "derive_option_greeks"]
    return ["option_contracts", "compact", "derive_option_greeks"]


def _derivatives_plan(cfg, dataset, start_str, end_str, symbols_str) -> dict:
    from cnequity.adapters.futures_exchange import shfe
    from cnequity.adapters.futures_exchange.registry import capabilities, enabled_exchanges, reader
    from cnequity.adapters.sina.dce_futures import candidate_codes
    from cnequity.query.calendar import list_trading_dates
    from cnequity.steps.derivatives import minute_scope

    start = parse_date_option(start_str, "--start")
    end = parse_date_option(end_str, "--end") or shanghai_today()
    if start and start > end:
        raise click.ClickException(_("--start must be on or before --end"))
    if symbols_str:
        _override_scope(
            cfg, dataset, [s.strip().upper() for s in symbols_str.split(",") if s.strip()]
        )
    if dataset == "futures_minute_bars":
        symbols = minute_scope(cfg)
        return {
            "dataset": dataset,
            "contracts": symbols,
            "requests_cold": len(symbols),
            "limit": "About 1023 recent bars per contract; accumulates only after enabling and cannot be backfilled",
            "writes": False,
        }
    kind = "options" if dataset.startswith("option_") else "futures"
    routes = []
    for exchange in enabled_exchanges(cfg):
        route = reader(cfg, exchange)
        floor = route.first_session(kind)
        if floor is None:
            routes.append({"exchange": exchange, "supported": False})
            continue
        lo = max(start or floor, floor)
        days = list_trading_dates(cfg, lo, end) if lo <= end else []
        reference_cost = (
            len(days) if route.fetch_reference is not None and route.reference_history else 0
        )
        if dataset.endswith("_contracts"):
            cost = (
                reference_cost
                if (start_str or end_str)
                else (1 if route.fetch_reference is not None else 0)
            )
        elif exchange == "DCE" and cfg.futures_dce_route == "sina":
            cost = len({code for day in days for code in candidate_codes(day)}) + 3
        else:
            cost = len(days) * (17 if exchange == "DCE" and kind == "options" else 1)
            if exchange == "SHF" and dataset == "futures_bars":
                cost += sum(
                    shfe.INE_FIRST_SESSION <= day <= shfe.INE_DIRECT_LAST_SESSION for day in days
                )
        if dataset.endswith("_contracts") and exchange == "GFE" and not (start_str or end_str):
            cost = None
        routes.append(
            {
                "exchange": exchange,
                "supported": True,
                "start": lo,
                "end": end,
                "sessions": len(days),
                "cold_request_estimate": cost,
                "reference_requests_upper_bound": reference_cost
                if (start_str or end_str)
                else (None if exchange == "GFE" else (1 if route.fetch_reference else 0)),
                "reference_budget_note": "GFEX current snapshot needs one product-list request plus one request per product; no fixed upper bound"
                if exchange == "GFE"
                else "one reference file per observed session",
            }
        )
    return {
        "dataset": dataset,
        "routes": routes,
        "source_capabilities": capabilities(),
        "refresh": bool(getattr(cfg, "_derivatives_refresh", False)),
        "cache": "Persistent response cache; skipped only when the receipt matches. The quote budget excludes retries; reference files list an upper bound separately (read only on dates with quotes). Actual cache hits must be checked against the full request key",
        "pacing_seconds": {"futures_exchange": 1.0, "sina": 0.3, **cfg.source_intervals},
        "followup_steps": _derivatives_followup(dataset),
        "quality": "A finished resume does not mean the whole market is complete; DCE via Sina lacks zero-volume days, and option arbitrage lacks synchronized order books",
        "writes": False,
    }


def _repair_outstanding(cfg, dataset: str, workers: int) -> dict:
    """Repair monthly key scopes and close with truthful remaining coverage."""
    from collections import defaultdict

    from cnequity.orchestrator.manifest import Manifest
    from cnequity.steps.bars import _last_final_session
    from cnequity.storage.state import StateStore

    # A later ordinary run may already have published many owed keys. Reconcile
    # against committed rows before grouping months, or those keys would drive
    # unnecessary source requests. This is not a repair attempt for missing keys.
    _settle_outstanding(cfg, dataset, note_missing_attempt=False)
    owed = StateStore(cfg.meta_root).get_outstanding_keys(dataset)
    if not owed:
        return {"dataset": dataset, "status": "success", "outstanding": 0, "note": "nothing owed"}

    # A key for a session that has not closed yet would make its whole monthly
    # pass fail the finality guard, and every other key in that month with it:
    # one 2026-09-18 key held back 242 owed sessions at 03:36 Shanghai. It stays
    # on the ledger for a later run rather than blocking today's repair.
    final = _last_final_session().isoformat() if dataset == "daily_bars" else None
    buckets: dict[str, set[str]] = defaultdict(set)
    days_in: dict[str, list[str]] = defaultdict(list)
    deferred = 0
    for row in owed:
        symbol, day = row.get("symbol"), row.get("trade_date")
        if not symbol or not day:
            continue
        if final and day > final:
            deferred += 1
            continue
        buckets[day[:7]].add(symbol)
        days_in[day[:7]].append(day)
    if deferred:
        click.echo(
            _(
                "[{dataset}] {deferred} keys belong to sessions not yet settled; they stay owed for a later run"
            ).format(dataset=dataset, deferred=deferred),
            err=True,
        )
    if not buckets:
        return {
            "dataset": dataset,
            "status": "success",
            "outstanding": len(owed),
            "note": "every owed key is for a session that is not final yet",
        }

    click.echo(
        _(
            "[{dataset}] owes {value} keys across {value2} symbols; repairing in {value3} monthly batches"
        ).format(
            dataset=dataset,
            value=len(owed),
            value2=len({r["symbol"] for r in owed}),
            value3=len(buckets),
        ),
        err=True,
    )
    failures: list[str] = []
    followups: list[dict] = []
    lineage_degraded = False
    passes = 0
    filled = 0
    fatal = False
    for index, month in enumerate(sorted(buckets), start=1):
        symbols = sorted(buckets[month])
        lo, hi = min(days_in[month]), max(days_in[month])
        click.echo(
            _("[{dataset}] {index}/{value} {month}: {value2} symbols {lo}..{hi}").format(
                dataset=dataset,
                index=index,
                value=len(buckets),
                month=month,
                value2=len(symbols),
                lo=lo,
                hi=hi,
            ),
            err=True,
        )
        cfg._backfill_symbols = symbols
        cfg._backfill_start = date.fromisoformat(lo)
        cfg._backfill_end = date.fromisoformat(hi)
        cfg._backfill_workers = workers
        window = (cfg._backfill_start, cfg._backfill_end)
        before = _bar_fingerprint(cfg, window) if dataset == "daily_bars" else None
        out = _backfill_once(cfg, dataset)
        if before is not None and out.get("status") != "failed":
            summary = _follow_lineage(
                out, lambda b=before, w=window, o=out: _after_daily_bars(cfg, b, w, o)
            )
            followups.append({"month": month, **summary})
            lineage_degraded |= summary.get("status") in {"failed", "warning", "degraded"}
        passes += 1
        if out.get("status") not in ("success", "warning", "degraded"):
            failures.append(f"{month}: {out.get('status')}")
        # Settle after each pass, not once at the end. A repair of a real
        # backlog runs for hours — the first version reached pass 30 of 37 over
        # three hours and, killed there, had struck nothing off: every row it
        # had fetched was still owed. Interrupting this now costs the pass in
        # flight, not the run.
        settled = _settle_outstanding(cfg, dataset, attempted_scope=(set(symbols), lo, hi))
        filled += settled["filled"]
        if out.get("status") == "failed" and out.get("run_id"):
            aggregate = Manifest(cfg.manifest_path).aggregate_run_status(out["run_id"])
            if aggregate["core_failures"] or not aggregate["source_failures"]:
                fatal = True
                break

    settled = settled if buckets else _settle_outstanding(cfg, dataset)
    return {
        "dataset": dataset,
        "status": "failed"
        if fatal or (failures and not filled)
        else "warning"
        if settled["still_owed"]
        else "degraded"
        if lineage_degraded
        else "success",
        "passes": passes,
        "failed_passes": failures,
        "followups": followups,
        "outstanding": settled,
        "result_schema_version": 2,
        "execution_status": "failed" if fatal or (failures and not filled) else "completed",
        "coverage_status": "partial" if settled["still_owed"] else "complete",
        "publication_status": "partial"
        if filled and settled["still_owed"]
        else "published"
        if filled
        else "unchanged",
        "usable_result": bool(filled or not settled["still_owed"]),
    }


def _settle_outstanding(
    cfg,
    dataset: str,
    *,
    note_missing_attempt: bool = True,
    attempted_scope: tuple[set[str], str, str] | None = None,
) -> dict:
    """Strike off the owed keys that are now in the lake, and report the rest.

    Checked against what actually landed rather than against the run's exit
    status: a repair that reaches some of the keys should shrink the debt by
    exactly those, and a key the vendor still does not serve must stay owed
    rather than be quietly forgotten by a successful-looking run.
    """
    import polars as pl

    from cnequity.domain.datasets import get_dataset as _spec
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
    from cnequity.storage.state import StateStore

    store = StateStore(cfg.meta_root)
    owed = store.get_outstanding_keys(dataset)
    if not owed:
        return {"before": 0, "filled": 0, "still_owed": 0}
    root = cfg.curated_root / dataset
    if not dataset_has_parquet(root):
        return {"before": len(owed), "filled": 0, "still_owed": len(owed)}
    date_col = _spec(dataset).partition_col
    # Pushed down to the owed scope. Collecting the whole dataset to check a
    # handful of keys reads 14GB to answer a question about 5,000 rows, and a
    # settle that costs more than the repair will not get run.
    wanted_symbols = sorted({row["symbol"] for row in owed if row.get("symbol")})
    wanted_days = sorted({row["trade_date"] for row in owed if row.get("trade_date")})
    present = set(
        scan_parquet_root(root, partition_col=date_col)
        .select("symbol", pl.col(date_col).cast(pl.Utf8).alias("_d"))
        .filter(
            pl.col("symbol").is_in(wanted_symbols)
            & pl.col("_d").is_between(pl.lit(wanted_days[0]), pl.lit(wanted_days[-1]))
        )
        .unique()
        .collect()
        .iter_rows()
    )
    filled = [
        (row["symbol"], row["trade_date"])
        for row in owed
        if (row.get("symbol"), row.get("trade_date")) in present
    ]
    missed = [
        (row["symbol"], row["trade_date"])
        for row in owed
        if (row.get("symbol"), row.get("trade_date")) not in present
    ]
    left = store.clear_outstanding_keys(dataset, filled) if filled else len(owed)
    # A key this repair reached for and still did not get is worth counting:
    # nothing here expires, so the attempt count is the only thing that will
    # ever distinguish last night's blip from a vendor that has stopped
    # serving the symbol at all.
    if note_missing_attempt:
        attempted = missed
        if attempted_scope is not None:
            symbols, start, end = attempted_scope
            attempted = [
                (symbol, day) for symbol, day in missed if symbol in symbols and start <= day <= end
            ]
        store.note_repair_attempt(dataset, attempted)
    stubborn = sum(
        1 for row in store.get_outstanding_keys(dataset) if int(row.get("attempts", 0) or 0) >= 3
    )
    out = {"before": len(owed), "filled": len(filled), "still_owed": left}
    if stubborn:
        out["unfilled_after_3_attempts"] = stubborn
    return out


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
    if dataset == "futures_minute_bars":
        from cnequity.domain.derivatives import parse_future_code

        for symbol in symbols:
            code, _sep, exchange = symbol.partition(".")
            try:
                contract = parse_future_code(code, exchange, shanghai_today())
            except ValueError as exc:
                raise click.ClickException(
                    _("Invalid futures contract {symbol}: {exc}").format(symbol=symbol, exc=exc)
                ) from exc
            if contract.symbol != symbol:
                raise click.ClickException(
                    _("Use a standard futures contract code: {symbol}").format(
                        symbol=contract.symbol
                    )
                )
        cfg.futures_enabled = cfg.futures_minute_enabled = True
        cfg.futures_minute_contracts = symbols
        cfg.futures_minute_products = []
        cfg.futures_minute_max_contracts = max(cfg.futures_minute_max_contracts, len(symbols))
        return
    block = SCOPED_DATASETS.get(dataset)
    if block is None:
        raise click.ClickException(
            _(
                "--symbols only applies to datasets with a scope in the config ({value}); {dataset} does not support overriding the scope by symbol."
            ).format(value=", ".join(sorted(SCOPED_DATASETS)), dataset=dataset)
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
            _(
                "{dataset}: --start {start} is before the source's history floor. For every symbol, upstream provides nothing earlier than {earliest}, and no backfill source extends it. Use --start {earliest} or later."
            ).format(dataset=dataset, start=start, earliest=earliest)
        )
    block = SCOPED_DATASETS.get(dataset, "minute_bars")
    raise click.ClickException(
        _(
            "{dataset}: --start {start} is beyond the history depth the source can provide. For symbols quoted every session, upstream keeps only about {history_horizon_days} sessions per symbol (back to roughly {earliest}), and no backfill source extends it. Use --start {earliest} or later. (Thinly traded symbols have bars on fewer days, so they reach further back. To get those, narrow [{block}].scope to a watchlist first: sweeping the whole market from that start burns hours on symbols with no data at all.)"
        ).format(
            dataset=dataset,
            start=start,
            history_horizon_days=spec.history_horizon_days,
            earliest=earliest,
            block=block,
        )
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
    # batch is what later lets `cne run clean` release this run's staging.
    result["compact"] = engine.run_step("compact", shanghai_today(), run_id)
    compact_status = result["compact"].get("status", "success")
    aggregate = (
        engine.manifest.aggregate_run_status(run_id)
        if hasattr(engine.manifest, "aggregate_run_status")
        else {}
    )
    if aggregate.get("results"):
        result["status"] = aggregate["status"]
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
    if hasattr(engine, "_public_outcome"):
        result.update(engine._public_outcome(run_id))
    persisted = engine.manifest.get_run(run_id) if hasattr(engine.manifest, "get_run") else None
    if persisted is not None:
        result["status"] = persisted["status"]
    return result


def _run_had_step_failure(engine: JobEngine, run_id: str) -> bool:
    """Whether a step in *run_id* actually failed, whatever tier softened it.

    ``aggregate_run_status`` deliberately reports a *run* as degraded rather
    than failed when the step that raised was not core: in the daily job the
    other datasets still landed and the lake stays usable. A single-dataset
    sweep has no such consolation — that one dataset is the entire job — and
    35 of the registered steps are non-core, so reading the run tier here let
    `cne backfill` print ``"status": "success"`` and exit 0 for a sweep whose
    every slice had raised.
    """
    aggregate = engine.manifest.aggregate_run_status(run_id)
    if "status" in aggregate:
        return aggregate["status"] == "failed"
    return bool(aggregate["core_failures"]) or any(
        item["status"] in {"failed", "blocked"} for item in aggregate["degraded_results"]
    )


def _source_limited_slice(engine, run_id: str) -> bool:
    """An unusable source slice does not cancel independent later slices."""
    result = (
        engine.manifest.aggregate_run_status(run_id)
        if hasattr(engine.manifest, "aggregate_run_status")
        else {}
    )
    return bool(result.get("source_failures")) and not result["core_failures"]


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
    from cnequity.orchestrator.compact_gate import compact_allowed
    from cnequity.storage import StagingWriter

    config = getattr(engine, "config", None)
    if config is None:  # lightweight engine doubles in CLI/unit tests
        return []
    # A hard-killed worker leaves its manifest row as ``running``. Reconcile
    # stale rows before selecting recovery candidates; otherwise their staged
    # facts stay invisible and the next retry fetches already checkpointed
    # symbols again. Active runs remain protected by the per-run lock.
    reconciled = engine.manifest.reconcile_orphaned_runs(
        stale_after_seconds=config.batch_stale_seconds,
        locks_root=config.meta_root,
    )
    if reconciled.get("runs_closed"):
        logging.getLogger(__name__).warning(
            "Reconciled %d orphaned backfill run(s) before staging recovery",
            reconciled["runs_closed"],
        )
    writer = StagingWriter(config.staging_root)
    recovered: list[str] = []
    for run in engine.manifest.list_runs("backfill"):
        run_id = str(run["run_id"])
        # Name the in-flight states, not the terminal ones. Listing the
        # terminal spellings is how `degraded` — a status this same release
        # taught the engine to return — came to be skipped here, leaving the
        # staged rows of exactly the runs most likely to have some.
        if run["status"] in ("running", "stale"):
            continue
        batches = engine.manifest.get_batches_for_run(run_id)
        # A compact that published with warnings is as done as a clean one.
        # Reading only ``success`` replayed such runs' staging on every later
        # backfill and republished rows a repair had since removed (920021.BJ).
        if any(
            batch["dataset"] == "compact" and batch["status"] in ("success", "warning")
            for batch in batches
        ):
            continue
        if not writer.list_run_files(dataset, run_id):
            continue
        # A failed blocking batch cannot be published by compact. Replaying
        # compact on every later backfill only rescans the same immutable
        # partitions before the gate rejects it; leave staging and failure
        # evidence untouched until its batch is actually resolved.
        allowed, incomplete = compact_allowed(
            engine.manifest,
            run_id,
            dataset,
            stale_after_seconds=config.batch_stale_seconds,
        )
        if not allowed:
            logging.getLogger(__name__).info(
                "Skipped staged %s from run %s: %d blocking batch(es) remain",
                dataset,
                run_id,
                incomplete,
            )
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


def _run_delisted_profile(cfg, since: date) -> dict:
    """Run the delisted profile through one manifest and compact path."""
    from cnequity.orchestrator.outcomes import step_outcome
    from cnequity.orchestrator.registry import get_step
    from cnequity.steps.delisted import backfill_delisted_bars

    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run(
        "delisted_backfill", {"since": since.isoformat(), "profile": "delisted"}
    )
    try:
        result = backfill_delisted_bars(cfg, run_id, since)
        engine._record_step_result(
            name="daily_bars",
            entry=get_step("daily_bars"),
            run_id=run_id,
            status=result.get("status", "success"),
            out=result,
        )
        compact_out = engine.run_step("compact", shanghai_today(), run_id)
    except (KeyboardInterrupt, SystemExit):
        engine.manifest.interrupt_run(run_id, error_message="delisted recovery interrupted")
        raise
    except Exception as exc:
        engine.manifest.finish_run(run_id, "failed", error_message=str(exc))
        raise
    complete = (
        result.get("status", "success") == "success"
        and compact_out.get("status", "success") == "success"
    )
    run_status = "success" if complete else "warning"
    if compact_out.get("status") in {"failed", "blocked"} or step_outcome(
        result.get("status", "success"), result
    ).execution_status in {"failed", "interrupted"}:
        run_status = "failed"
    error_message = None if complete else "delisted recovery has unresolved targets"
    engine.manifest.finish_run(
        run_id,
        run_status,
        rows_read=result.get("rows_read", 0),
        rows_written=result.get("rows_written", 0),
        error_message=error_message,
    )
    return {
        "run_id": run_id,
        **result,
        "status": run_status,
        "compact": compact_out,
        **engine._public_outcome(run_id),
    }


def _run_shfe_annual_archive(
    cfg,
    dataset: str,
    path: Path,
    year: int,
    *,
    start: date | None,
    end: date | None,
    source_url: str | None,
    downloaded_at: str | None = None,
) -> dict:
    """Keep an explicit partial-field import in the normal run/compact ledger."""
    from cnequity.steps.derivative_archive import import_shfe_annual

    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run(
        "shfe_annual_import",
        {
            "dataset": dataset,
            "year": year,
            "archive": str(path),
            "start": str(start) if start else None,
            "end": str(end) if end else None,
            "partial_fields_accepted": True,
        },
    )
    try:
        result = import_shfe_annual(
            cfg,
            run_id,
            dataset,
            path,
            year=year,
            start=start,
            end=end,
            source_url=source_url,
            downloaded_at=downloaded_at,
        )
        compact = engine.run_step("compact", shanghai_today(), run_id)
        status = result["status"]
        if compact.get("status") == "failed":
            status = "failed"
        elif compact.get("status") == "warning" and status == "success":
            status = "warning"
    except Exception as exc:
        engine.manifest.finish_run(run_id, "failed", error_message=str(exc))
        raise
    engine.manifest.finish_run(
        run_id,
        status,
        rows_read=result["rows_read"],
        rows_written=result["rows_written"],
        error_message="annual archive has unresolved members or rows"
        if status != "success"
        else None,
    )
    return {"run_id": run_id, **result, "status": status, "compact": compact}


def _require_known_dataset(dataset: str) -> str:
    """Reject a mistyped name with the near misses, not a ``KeyError`` dump.

    `cne backfill` takes a dataset, and the registry lookup that rejects an
    unknown one raised straight through the CLI — so a typo printed a Python
    traceback instead of telling the operator what to type.
    """
    from cnequity.domain.datasets import DATASETS

    # Registry names are lower case, and command names are already
    # case-insensitive, so a dataset typed in caps should resolve the same way.
    # Returns the canonical spelling for the caller to use from here on.
    canonical = dataset.lower()
    if canonical in DATASETS:
        return canonical
    close = difflib.get_close_matches(canonical, sorted(DATASETS), n=3)
    hint = _("Did you mean: {value}? ").format(value=", ".join(close)) if close else ""
    raise click.ClickException(
        _("Unknown dataset {dataset!r}. {hint}`cne status --datasets` lists every dataset.").format(
            dataset=dataset, hint=hint
        )
    )


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
    # CNINFO range steps also protect direct step invocations with an internal
    # 31-day window, but the CLI must make each window a separate run so the
    # compact boundary drains staging before the next window is fetched.  If
    # no explicit range was supplied, an omitted --end means today.
    # `regulatory_events` is chunked alongside it for the same compact bound,
    # though it no longer fetches: it derives from the announcements already
    # indexed and clamps each slice to their range, so a floor that predates
    # the lake's own history costs a skipped slice, not a failed sweep.
    if dataset in {"announcement_index", "regulatory_events"}:
        start = getattr(cfg, "_backfill_start", None) or date(2010, 1, 1)
        end = getattr(cfg, "_backfill_end", None) or shanghai_today()
        return _backfill_chunked(cfg, dataset, start, end, get_dataset(dataset).backfill_chunk_days)
    engine = JobEngine(cfg)
    _recover_compactable_backfill_staging(engine, dataset)
    # Do not finish_run until after compact — otherwise a kill between the two
    # leaves status=success with no compact batch, and `cne run clean` cannot reclaim
    # staging that never reached curated (same ordering as delisted CLI).
    result = engine.run_job("backfill", steps=[dataset], backfill=True, finalize_run=False)
    return _finish_backfill_run(engine, result)


def _backfill_symbol_chunked(cfg, dataset: str, start: date, end: date, chunk_symbols: int) -> dict:
    """Backfill a tip-paged dataset as compacted symbol slices over [start, end].

    TDX intraday locates old windows by offset before scanning their pages.
    Chunking by symbol pays that positioning cost once per name, bounds compact
    memory, and makes an interruption cost only the current symbol batch.
    """
    from cnequity.steps.intraday import (
        _filter_all_scope_to_listed_symbols,
        resolve_scope,
    )

    symbols = resolve_scope(cfg)
    if (cfg.minute_bars_scope or "").strip() == "all":
        symbols = _filter_all_scope_to_listed_symbols(cfg, symbols, start, end)
    if not symbols:
        raise click.ClickException(
            _("{dataset}: the scope resolved to 0 symbols; check [minute_bars].scope").format(
                dataset=dataset
            )
        )

    engine = JobEngine(cfg)
    _recover_compactable_backfill_staging(engine, dataset)
    chunks: list[dict] = []
    failed_scopes: list[dict] = []
    usable = False
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
                _(
                    "[{dataset}] symbols {value}..{value2}/{value3} ({value4}..{value5}) window {start}..{end}"
                ).format(
                    dataset=dataset,
                    value=index + 1,
                    value2=index + len(chunk),
                    value3=len(symbols),
                    value4=chunk[0],
                    value5=chunk[-1],
                    start=start,
                    end=end,
                ),
                err=True,
            )
            result = engine.run_job("backfill", steps=[dataset], backfill=True, finalize_run=False)
            if _run_had_step_failure(engine, result["run_id"]):
                result["status"] = "failed"
            result = _finish_backfill_run(engine, result)
            usable |= bool(
                result.get("usable_result", result["status"] == "success")
                or result.get("rows_written")
            )
            rows_read += int(result.get("rows_read", 0))
            rows_written += int(result.get("rows_written", 0))
            chunks.append(
                {
                    "symbols_from": index + 1,
                    "run_id": result["run_id"],
                    "symbols_to": index + len(chunk),
                    "first_symbol": chunk[0],
                    "last_symbol": chunk[-1],
                    "start": start,
                    "end": end,
                    "status": result["status"],
                    "rows_written": result.get("rows_written", 0),
                    "fallback": result.get("fallback", []),
                }
            )
            if result["status"] == "failed":
                if _source_limited_slice(engine, result["run_id"]):
                    failed_scopes.append(chunks[-1])
                    status = "warning"
                    continue
                status = "failed"
                break
            if result["status"] in {"warning", "degraded"} and status == "success":
                status = result["status"]
            if result["status"] in {"warning", "degraded"}:
                failed_scopes.append(chunks[-1])
    finally:
        cfg.minute_bars_scope = original_scope
        cfg.minute_bars_symbols = original_symbols

    return {
        "dataset": dataset,
        "status": status,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "chunks": chunks,
        "failed_scopes": failed_scopes,
        "usable_result": usable,
        "fallback": [option for chunk in chunks for option in chunk["fallback"]],
        "result_schema_version": 2,
        "execution_status": "failed" if status == "failed" else "completed",
        "coverage_status": "unknown" if status == "success" else "partial",
        "publication_status": "partial"
        if status != "success" and usable
        else "published"
        if rows_written
        else "unchanged",
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
    failed_scopes: list[dict] = []
    usable = False
    status = "success"
    rows_read = rows_written = 0
    cursor = start
    while cursor <= end:
        slice_end = min(cursor + timedelta(days=chunk_days - 1), end)
        cfg._backfill_start, cfg._backfill_end = cursor, slice_end
        click.echo(
            _("[{dataset}] slice {cursor}..{slice_end}").format(
                dataset=dataset, cursor=cursor, slice_end=slice_end
            ),
            err=True,
        )
        result = engine.run_job("backfill", steps=[dataset], backfill=True, finalize_run=False)
        if _run_had_step_failure(engine, result["run_id"]):
            result["status"] = "failed"
        result = _finish_backfill_run(engine, result)
        usable |= bool(
            result.get("usable_result", result["status"] == "success") or result.get("rows_written")
        )
        rows_read += int(result.get("rows_read", 0))
        rows_written += int(result.get("rows_written", 0))
        slices.append(
            {
                "start": cursor,
                "run_id": result["run_id"],
                "end": slice_end,
                "status": result["status"],
                "rows_written": result.get("rows_written", 0),
                "fallback": result.get("fallback", []),
            }
        )
        if result["status"] == "failed":
            if _source_limited_slice(engine, result["run_id"]):
                failed_scopes.append(slices[-1])
                status = "warning"
                cursor = slice_end + timedelta(days=1)
                continue
            status = "failed"
            break
        # `degraded` is an outcome, not a synonym for success: a slice the
        # source could not supply must not leave the sweep claiming it did.
        if result["status"] in {"warning", "degraded"} and status == "success":
            status = result["status"]
        if result["status"] in {"warning", "degraded"}:
            failed_scopes.append(slices[-1])
        cursor = slice_end + timedelta(days=1)
    return {
        "dataset": dataset,
        "status": status,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "slices": slices,
        "failed_scopes": failed_scopes,
        "usable_result": usable,
        "fallback": [option for part in slices for option in part["fallback"]],
        "result_schema_version": 2,
        "execution_status": "failed" if status == "failed" else "completed",
        "coverage_status": "unknown" if status == "success" else "partial",
        "publication_status": "partial"
        if status != "success" and usable
        else "published"
        if rows_written
        else "unchanged",
        "resume_from": slices[-1]["start"] if status == "failed" and slices else None,
    }


# --- Repairs and imports that run on the backfill machinery -------------------
#
# Each of these used to be a mode flag on `cne backfill`, valid for one dataset,
# which made its help a list of exceptions. They keep the same machinery — the
# window, scope, lineage follow-up and `--plan` output — but follow the repair
# convention: a preview by default, `--apply` to run.


def _repair_group():
    from cnequity.cli.repair_cmds import repair

    return repair


def _run_on_backfill(dataset: str, config_path: str, apply: bool, **flags) -> None:
    ctx = click.get_current_context()
    ctx.invoke(backfill, dataset=dataset, config_path=config_path, plan=not apply, **flags)
    if not apply:
        click.echo(_("Preview only; add --apply to run it."), err=True)


_apply_option = click.option(
    "--apply", is_flag=True, help=N_("Run it; without this only the plan is printed.")
)
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


@_repair_group().command("payment-dates")
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
@_apply_option
def repair_payment_dates(config_path, symbols_str, start_str, end_str, issuer_notices_only, apply):
    """Repair corporate_actions payment dates: reviewed issuer announcements first, then Baostock.

    A payment date earlier than the ex-date is treated as unknown. With --issuer-notices-only,
    reviewed bonus/transfer share terms are repaired too and Baostock is not queried.
    """
    _run_on_backfill(
        "corporate_actions",
        config_path,
        apply,
        symbols_str=symbols_str,
        start_str=start_str,
        end_str=end_str,
        payment_date_repair=not issuer_notices_only,
        issuer_notice_repair=issuer_notices_only,
    )


@_repair_group().command("delisted-actions")
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
@_apply_option
def repair_delisted_actions(config_path, source, symbols_str, start_str, end_str, apply):
    """Fill historical dividends and ex-rights of delisted symbols from Baostock or THS."""
    _run_on_backfill(
        "corporate_actions",
        config_path,
        apply,
        symbols_str=symbols_str,
        start_str=start_str,
        end_str=end_str,
        baostock_repair=source == "baostock",
        ths_repair=source == "ths",
    )


@_repair_group().command("ex-dates")
@config_option
@click.option(
    "--ex-dates", "ex_dates_str", required=True, help=N_("Comma-separated ex-dates, YYYY-MM-DD.")
)
@comma_option("--symbols", "symbols_str", help=N_("Comma-separated symbols."))
@_apply_option
def repair_ex_dates(config_path, ex_dates_str, symbols_str, apply):
    """Request historical ex-rights rows from EastMoney for the given ex-dates, day by day.

    The backfill path's primary source is TDX; only EastMoney's per-day filter reaches rows
    before 2015-09-29.
    """
    _run_on_backfill(
        "corporate_actions",
        config_path,
        apply,
        symbols_str=symbols_str,
        eastmoney_date_repair=True,
        ex_dates_str=ex_dates_str,
    )


@_repair_group().command("bar-amounts")
@config_option
@_with(*_window_options)
@comma_option("--symbols", "symbols_str", help=N_("Comma-separated symbols."))
@_apply_option
def repair_bar_amounts(config_path, start_str, end_str, symbols_str, apply):
    """Fill daily_bars turnover of Sina-filled rows from TDX, after checking OHLC and volume.

    Turnover is filled only when open/high/low/close match and volume differs by under one lot;
    codes TDX lacks keep their Sina rows. Stored prices and volumes are never touched.
    """
    _run_on_backfill(
        "daily_bars",
        config_path,
        apply,
        start_str=start_str,
        end_str=end_str,
        symbols_str=symbols_str,
        tdx_amount_repair=True,
    )


@_repair_group().command("bar-turnover")
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
@_apply_option
def repair_bar_turnover(config_path, start_str, end_str, symbols_str, peer_review, apply):
    """Replace Shanghai/Shenzhen daily_bars rows whose turnover is missing, zero or misaligned.

    The whole same-day Baostock row replaces it when OHLC agree within half a cent; otherwise,
    or when Baostock has no row, the original values stay.
    """
    _run_on_backfill(
        "daily_bars",
        config_path,
        apply,
        start_str=start_str,
        end_str=end_str,
        symbols_str=symbols_str,
        turnover_repair=True,
        turnover_peer_review=peer_review,
    )


@_repair_group().command("bse-tip-turnover")
@config_option
@comma_option("--symbols", "symbols_str", required=True, help=N_("Comma-separated symbols."))
@click.option("--date", "day", required=True, help=N_("The session (YYYY-MM-DD)."))
@_apply_option
def repair_bse_tip_turnover(config_path, symbols_str, day, apply):
    """Fill Beijing daily_bars turnover for the latest session from the BSE quote board.

    For when the board was unavailable during the daily update. The board is a current
    snapshot, so only the latest session qualifies; Sina is not refetched.
    """
    _run_on_backfill(
        "daily_bars",
        config_path,
        apply,
        symbols_str=symbols_str,
        start_str=day,
        end_str=day,
        bse_tip_repair=True,
    )


@_repair_group().command("valuation-outage")
@config_option
@_with(*_window_options)
@_apply_option
def repair_valuation_outage(config_path, start_str, end_str, apply):
    """Fill valuation_metrics for an EastMoney snapshot outage from the datacenter report.

    Covers the window after EastMoney's last complete day and before today; writes only when the
    whole market is retrieved.
    """
    _run_on_backfill(
        "valuation_metrics",
        config_path,
        apply,
        start_str=start_str,
        end_str=end_str,
        fill_em_outage=True,
    )


@cli.group("import")
def import_group():
    """Import data from local files into the lake (offline)."""


@import_group.command("shfe-archive")
@click.argument("archive", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@config_option
@click.option(
    "--dataset",
    type=click.Choice(["futures_bars", "option_bars"]),
    required=True,
    help=N_("Which daily bars the annual package holds."),
)
@click.option(
    "--year", type=click.IntRange(2000, 2100), required=True, help=N_("Trading year it covers.")
)
@click.option(
    "--accept-partial-fields",
    is_flag=True,
    help=N_(
        "Accept that the annual package lacks fields only the daily files have; existing complete "
        "daily bars are not overwritten. Required."
    ),
)
@click.option("--url", default=None, help=N_("Optional: its original HTTPS URL on the SHFE site."))
@click.option(
    "--downloaded-at",
    default=None,
    help=N_("Optional: when it was downloaded, ISO 8601 with zone."),
)
@click.option("--start", "start_str", default=None, help=N_("Within --year: first day to import."))
@click.option("--end", "end_str", default=None, help=N_("Within --year: last day to import."))
@_apply_option
def import_shfe_archive(
    archive,
    config_path,
    dataset,
    year,
    accept_partial_fields,
    url,
    downloaded_at,
    start_str,
    end_str,
    apply,
):
    """Import a Shanghai Futures Exchange annual ZIP of daily bars, offline and verified."""
    _run_on_backfill(
        dataset,
        config_path,
        apply,
        shfe_annual_archive=archive,
        archive_year=year,
        accept_partial_fields=accept_partial_fields,
        archive_url=url,
        archive_downloaded_at=downloaded_at,
        start_str=start_str,
        end_str=end_str,
    )


def _backfill_financials_tdx_fin(config_path: str, start_str: str | None, end_str: str | None):
    """Fill the statement items the lake lacks from 通达信's quarterly packages."""
    from cnequity.adapters.tdx_fin.packages import latest_periods
    from cnequity.cli.ths_official_cmds import _publish_staged, _staged_run
    from cnequity.steps.fundamentals import stage_financials_tdx_fin

    cfg = _cfg(config_path)
    today = shanghai_today()
    end = parse_date_option(end_str, "--end") if end_str else today
    start = parse_date_option(start_str, "--start") if start_str else date(2001, 1, 1)
    if start > end:
        raise click.BadParameter(_("--start is after --end"), param_hint="--start")
    periods = latest_periods(end, count=4 * (end.year - start.year + 1))
    periods = sorted(p for p in periods if p >= f"{start.year}Q{(start.month - 1) // 3 + 1}")
    attach_log_file(cfg, "backfill-financial_statement_items-tdx_fin")
    with _staged_run(
        cfg,
        "tdx_fin_backfill",
        "tdx-fin-backfill",
        {"start": start.isoformat(), "end": end.isoformat()},
    ) as (run_id, outcome):
        result = stage_financials_tdx_fin(cfg, run_id, today, periods=periods, gap_only=True)
        outcome["result"] = result
    compact = _publish_staged(cfg, run_id, result)
    click.echo(json.dumps({"run_id": run_id, **result, "compact": compact}, indent=2, default=str))
