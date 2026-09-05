"""`backfill` and the chunking, scoping and recovery it needs.

The helpers are the bulk of it: a backfill is one command with several failure
modes that each need their own repair path (symbol-chunked, day-chunked, and
recovering staging an interrupted terminal run left behind).
"""

from __future__ import annotations

import difflib
import json
import logging
from datetime import date, timedelta

import click

from cnequity.cli._root import cli
from cnequity.cli._shared import (
    _cfg,
    _progress_logging,
    config_option,
    parse_date_option,
)
from cnequity.domain.datasets import fetch_semantics, get_dataset
from cnequity.domain.market_time import shanghai_today
from cnequity.orchestrator.engine import JobEngine


@cli.command()
@click.argument("dataset")
@config_option
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
    help="Comma-separated symbols for a scoped intraday, trading_status, or "
    "corporate_actions backfill, or a scoped daily_bars repair. The "
    "trading_status checkpoint and coverage evidence retain the exact scope; "
    "daily_bars keeps the explicit scope in backfill metadata.",
)
@click.option(
    "--workers",
    default=1,
    show_default=True,
    help="Concurrent date-walk workers for margin_trading only. Every request "
    "still uses the configured shared source limiter; other datasets require 1.",
)
@click.option(
    "--baostock-repair",
    is_flag=True,
    help="For corporate_actions only: explicitly repair delisted SH/SZ symbols via Baostock.",
)
@click.option(
    "--ths-repair",
    is_flag=True,
    help="For corporate_actions only: explicitly repair delisted BJ symbols via Tonghuashun.",
)
@click.option(
    "--eastmoney-bj-repair",
    is_flag=True,
    help="For corporate_actions only: repair legacy BJ symbols through current 920xxx EastMoney codes.",
)
@click.option(
    "--bse-tip-repair",
    is_flag=True,
    help="For daily_bars only: fill an existing session's BJ amount from BSE without re-fetching Sina.",
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
    baostock_repair: bool,
    ths_repair: bool,
    eastmoney_bj_repair: bool,
    bse_tip_repair: bool,
):
    """Backfill a dataset."""
    _progress_logging()
    _require_known_dataset(dataset)
    if fetch_semantics(dataset) == "snapshot" and not get_dataset(dataset).backfill_source:
        raise click.ClickException(
            f"{dataset}: backfill not supported — fetch semantics are snapshot "
            "(live page stamped with trade_date; historical values unavailable). "
            "Run daily ingestion on trading days instead."
        )
    cfg = _cfg(config_path)
    if workers < 1:
        raise click.ClickException("--workers must be at least 1")
    if workers > 1 and dataset != "margin_trading":
        raise click.ClickException(
            "--workers > 1 is currently supported only for margin_trading; "
            "other backfills use one date-walk lane"
        )
    if baostock_repair and dataset != "corporate_actions":
        raise click.ClickException("--baostock-repair only applies to corporate_actions")
    if ths_repair and dataset != "corporate_actions":
        raise click.ClickException("--ths-repair only applies to corporate_actions")
    if eastmoney_bj_repair and dataset != "corporate_actions":
        raise click.ClickException("--eastmoney-bj-repair only applies to corporate_actions")
    if bse_tip_repair and dataset != "daily_bars":
        raise click.ClickException("--bse-tip-repair only applies to daily_bars")
    if baostock_repair:
        cfg._corporate_actions_baostock_repair = True
    if ths_repair:
        cfg._corporate_actions_ths_repair = True
    if eastmoney_bj_repair:
        cfg._corporate_actions_eastmoney_bj_repair = True
    if dataset == "sector_bars":
        if retry_failed and force:
            raise click.ClickException("Use either --retry-failed or --force, not both.")
        cfg._sector_bars_force = force
    start_d = parse_date_option(start_str, "--start")
    end_d = parse_date_option(end_str, "--end")
    if bse_tip_repair:
        if not symbols_str:
            raise click.ClickException("--bse-tip-repair requires --symbols")
        if start_d is None or end_d is None or start_d != end_d:
            raise click.ClickException(
                "--bse-tip-repair requires the same explicit --start and --end session"
            )
        cfg._bse_tip_repair = True
    _guard_history_horizon(dataset, start_d)
    if symbols_str:
        symbols = [s.strip().upper() for s in symbols_str.split(",") if s.strip()]
        if dataset in ("daily_bars", "trading_status", "corporate_actions"):
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
    return bool(aggregate["core_failures"]) or any(
        str(item["status"]) in {"failed", "blocked"} for item in aggregate["degraded_results"]
    )


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


def _require_known_dataset(dataset: str) -> None:
    """Reject a mistyped name with the near misses, not a ``KeyError`` dump.

    `cne backfill` takes a dataset, and the registry lookup that rejects an
    unknown one raised straight through the CLI — so a typo printed a Python
    traceback instead of telling the operator what to type.
    """
    from cnequity.domain.datasets import DATASETS

    if dataset in DATASETS:
        return
    close = difflib.get_close_matches(dataset, sorted(DATASETS), n=3)
    hint = f" Did you mean: {', '.join(close)}?" if close else ""
    raise click.ClickException(
        f"unknown dataset {dataset!r}.{hint} `cne status --datasets` lists every dataset."
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
    from cnequity.steps.intraday import (
        _filter_all_scope_to_listed_symbols,
        resolve_scope,
    )

    symbols = resolve_scope(cfg)
    if (cfg.minute_bars_scope or "").strip() == "all":
        symbols = _filter_all_scope_to_listed_symbols(cfg, symbols, start, end)
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
            if _run_had_step_failure(engine, result["run_id"]):
                result["status"] = "failed"
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
            if result["status"] in {"warning", "degraded"} and status == "success":
                status = result["status"]
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
        if _run_had_step_failure(engine, result["run_id"]):
            result["status"] = "failed"
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
        # `degraded` is an outcome, not a synonym for success: a slice the
        # source could not supply must not leave the sweep claiming it did.
        if result["status"] in {"warning", "degraded"} and status == "success":
            status = result["status"]
        cursor = slice_end + timedelta(days=1)
    return {
        "dataset": dataset,
        "status": status,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "slices": slices,
        "resume_from": slices[-1]["start"] if status == "failed" and slices else None,
    }
