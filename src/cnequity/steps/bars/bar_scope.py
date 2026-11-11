"""Which symbols a daily-bar run owes, and who owns each batch."""

from __future__ import annotations

import hashlib
import json
from contextvars import ContextVar
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import polars as pl

from cnequity.config import Config
from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.domain.market_time import shanghai_now
from cnequity.run_scope import backfill_of
from cnequity.steps.bars import etf_debts
from cnequity.steps.bars._base import _DAILY_BAR_FINAL_AT, logger
from cnequity.steps.common import (
    BACKFILL_START,
    DailyBarOwnership,
    instrument_metadata,
    is_trading_day,
    last_session_on_or_before,
    list_trading_dates,
    load_bar_universe,
    load_symbols,
)


def _reject_unfinished_daily_bar_window(
    config: Config,
    end: date,
    *,
    now: datetime | None = None,
) -> None:
    """Reject a window whose newest daily bar is still forming in Shanghai.

    TDX ``start=0`` includes the current daily K. Once trading begins that row
    has plausible OHLC and non-zero volume, so content checks cannot distinguish
    it from a settled bar. Refuse the fetch before any symbol batch starts: a
    market-wide sweep that crosses 15:00 would otherwise mix partial and final
    bars in one curated partition.

    Historical windows and non-trading days are unaffected. ``now`` is
    injectable so the timezone boundary is deterministic in tests.
    """
    local_now = shanghai_now(now)
    today = local_now.date()
    if end < today or local_now.time() >= _DAILY_BAR_FINAL_AT:
        return
    if not is_trading_day(config, today):
        return
    raise RuntimeError(
        f"daily_bars {end}: the current A-share session is not final until "
        f"{_DAILY_BAR_FINAL_AT.strftime('%H:%M')} Asia/Shanghai "
        f"(now {local_now.strftime('%H:%M:%S')}); refusing to stage an "
        "in-progress daily bar. Re-run after the cutoff."
    )


def _last_final_session(now: datetime | None = None) -> date:
    """The newest date whose daily bar has settled.

    The mirror of :func:`_reject_unfinished_daily_bar_window`: what that refuses
    to fetch, this is what a history sweep should ask for instead.

    Deliberately clock-only. Asking the calendar whether today trades would
    make a window depend on a `trading_calendar` the lake may not have yet —
    `cne init` builds it one phase before the first backfill — and it buys
    nothing: stepping back a day on a Sunday lands on Saturday, and a window
    ending on a day with no session loses none of the sessions before it.
    """
    local_now = shanghai_now(now)
    if local_now.time() >= _DAILY_BAR_FINAL_AT:
        return local_now.date()
    return local_now.date() - timedelta(days=1)


def _backfill_window(config: Config, trade_date: date) -> tuple[date, date]:
    """``--start/--end`` window for a backfill, defaulting to the full history.

    Repairing a single bad session must not mean re-fetching a decade for every
    symbol. A capture that fires before the close writes a truncated bar — right
    open, wrong close, partial volume — and the repair is one day wide.

    An unspecified end means "as much history as there is", which is the last
    *settled trading session* — not today, whose bar is still forming until
    15:05, and not a Saturday/holiday for which no bar can exist. It
    defaulted to today, so `cne init` run during a session failed the whole
    phase on a window nobody asked for: 37 minutes of reference and corporate
    actions, then `phase2c_daily_bars_backfill` refused in 2.8ms and phases 3
    and 4 never ran. An explicit trading-day `--end` is still honored exactly,
    so repairing today's truncated bar before the close fails loudly. An
    explicit non-trading end is normalized to the preceding session instead
    of manufacturing a permanently missing Saturday/holiday key.

    `trade_date` bounds it from the other side. Taking the settled session alone
    ignored the date the caller named: replaying an old one — `cne run daily
    --trade-date 2025-01-10` — asked for everything up to *today* instead, which
    is not the window anybody requested and not a run anybody could reproduce.
    The smaller of the two is the only answer that satisfies both: today's run
    stops before the forming bar, and a replay stops where it was told.
    """
    settled = _last_final_session()
    requested_end = backfill_of(config).end or min(trade_date, settled)
    end = last_session_on_or_before(config, requested_end)
    start = backfill_of(config).start or BACKFILL_START
    return start, end


def _instrument_spans(
    config: Config,
) -> dict[str, tuple[date | None, date | None, str | None]]:
    return {
        row["symbol"]: (row["list_date"], row["delist_date"], row.get("asset_type"))
        for row in instrument_metadata(config).iter_rows(named=True)
    }


def _placeholder_bar_universe(
    config: Config,
    spans: dict[str, tuple[date | None, date | None, str | None]],
) -> set[str] | None:
    """Return traded bars only when an undated symbol needs reconciliation.

    Scanning every daily_bars file is unnecessary for normal runs. An empty
    traded universe is also not evidence that every undated symbol is a
    placeholder, so leave the classifier conservative in a brand-new lake.

    A run that names its symbols gets no placeholder universe at all. The
    classification is a cost control for the full-market sweep — it keeps
    pre-listing codes out of the per-symbol fallback — and it reads an absent
    list_date as "not listed yet". An operator asking for a symbol by name has
    already made that call, so honouring the guess instead fetched nothing,
    wrote nothing and still reported success: `cne backfill daily_bars
    --symbols 000001.SZ` returned 0 rows against a source that had them.
    """
    if backfill_of(config).symbol_list():
        return None
    if not any(list_date is None for list_date, _delist_date, _asset in spans.values()):
        return None
    universe = load_bar_universe(config)
    return universe or None


def _ownership_context(
    config: Config,
    ownership: DailyBarOwnership,
    start: date,
    end: date,
) -> tuple[dict, bool]:
    from cnequity.steps.delisted import delisted_recovery_covers

    delegated_complete = delisted_recovery_covers(config, start, end, ownership.delegated_delisted)
    findings = [
        {
            "dataset": "daily_bars",
            "severity": "info" if delegated_complete else "warning",
            "check": "daily_bars_source_ownership",
            "message": (
                f"generic={len(ownership.generic)}, "
                f"delegated_delisted={len(ownership.delegated_delisted)}, "
                f"expected_no_data={len(ownership.expected_no_data)}, "
                f"placeholder={len(ownership.placeholder)}, "
                f"probe={len(ownership.probe)}, "
                f"negative_cached={len(ownership.negative_cached)}, "
                f"unknown={len(ownership.unknown)}, "
                f"delegated_complete={delegated_complete}"
            ),
            "start": start.isoformat(),
            "end": end.isoformat(),
        }
    ]
    if ownership.placeholder:
        preview = ", ".join(sorted(ownership.placeholder)[:8])
        suffix = "..." if len(ownership.placeholder) > 8 else ""
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "daily_bars_placeholder_skipped",
                "message": (
                    f"{len(ownership.placeholder)} undated placeholder(s) skipped "
                    "(no list_date and no traded bar anywhere in the lake; not "
                    f"verified no-data): {preview}{suffix}"
                ),
                "symbols": sorted(ownership.placeholder),
            }
        )
    if ownership.unknown:
        preview = ", ".join(sorted(ownership.unknown)[:8])
        suffix = "..." if len(ownership.unknown) > 8 else ""
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "daily_bars_unknown_ownership",
                "message": (
                    f"{len(ownership.unknown)} symbol(s) lack sufficient instrument/status "
                    f"evidence and remain strict fetch obligations: {preview}{suffix}"
                ),
                "symbols": sorted(ownership.unknown),
            }
        )
    return {
        "daily_bars_ownership": {
            "generic": len(ownership.generic),
            "delegated_delisted": len(ownership.delegated_delisted),
            "expected_no_data": len(ownership.expected_no_data),
            "placeholder": len(ownership.placeholder),
            "probe": len(ownership.probe),
            "negative_cached": len(ownership.negative_cached),
            "unknown": len(ownership.unknown),
            "delegated_complete": delegated_complete,
        },
        "audit_findings": findings,
    }, delegated_complete


def _record_delegated_ownership_batch(
    config: Config,
    run_id: str,
    symbols: list[str],
    start: date,
    end: date,
    *,
    batch_id: str | None = None,
) -> bool:
    from cnequity.orchestrator.manifest import Manifest
    from cnequity.steps.delisted import delisted_recovery_covers

    if not symbols:
        return True
    complete = delisted_recovery_covers(config, start, end, symbols)
    manifest = Manifest(config.manifest_path)
    if batch_id is None:
        identity = json.dumps(
            {"symbols": sorted(symbols), "start": start.isoformat(), "end": end.isoformat()},
            sort_keys=True,
            separators=(",", ":"),
        )
        batch_id = f"ownership-{hashlib.sha256(identity.encode()).hexdigest()[:16]}"
    existing = manifest.get_batch(run_id, batch_id)
    if existing is None or existing["status"] != "success":
        # Re-open the same deterministic ownership batch on retry. Without
        # this, finish_batch() only updates rows still marked ``running`` and
        # a prior warning can never become successful after recovery completes.
        manifest.start_batch(
            run_id,
            batch_id,
            task_id="daily_bars_ownership",
            dataset="daily_bars",
            symbols=sorted(symbols),
            window_start=start.isoformat(),
            window_end=end.isoformat(),
            blocks_compaction=True,
        )
    manifest.finish_batch(
        run_id,
        batch_id,
        "success" if complete else "warning",
        error_message=(
            "delegated delisted recovery receipt verified"
            if complete
            else "delegated delisted symbols lack a complete recovery receipt"
        ),
    )
    return complete


def _reuse_successful_daily_bars(
    config: Config,
    run_id: str,
    symbols: list[str],
    start: date,
    end: date,
) -> set[str]:
    """Seed this run from verified staging batches of an interrupted run.

    A failed catchup should not make the next run re-fetch every symbol whose
    earlier batch already finished successfully. Only manifest-successful
    batches with the exact same window are eligible; failed/running staging is
    never reused. A symbol is removed from the new fetch scope only when all
    trading sessions in the window are present.
    """
    import polars as pl

    from cnequity.orchestrator.manifest import Manifest
    from cnequity.storage import StagingWriter

    sessions = list_trading_dates(config, start, end)
    if not symbols or not sessions:
        return set()
    batches = Manifest(config.manifest_path).get_successful_batches(
        "daily_bars",
        start.isoformat(),
        end.isoformat(),
        exclude_run_id=run_id,
    )
    if not batches:
        return set()

    files = []
    for batch in batches:
        path = (
            config.staging_root
            / "daily_bars"
            / f"run_id={batch['run_id']}"
            / (f"part-{batch['batch_id']}.parquet")
        )
        if path.exists():
            files.append(path)
    if not files:
        return set()

    frames = [pl.read_parquet(path) for path in files]
    reused = pl.concat(frames, how="diagonal_relaxed").filter(
        pl.col("symbol").is_in(symbols) & pl.col("trade_date").is_in(sessions)
    )
    if reused.is_empty():
        return set()
    reused = dedupe_by_primary_key(reused, "daily_bars")
    reused_symbols = set(
        reused.group_by("symbol")
        .len()
        .filter(pl.col("len") == len(sessions))
        .get_column("symbol")
        .to_list()
    )
    if not reused_symbols:
        return set()

    reused = reused.filter(pl.col("symbol").is_in(sorted(reused_symbols)))
    StagingWriter(config.staging_root).write_batch(
        "daily_bars",
        run_id,
        f"reused-successful-{start.isoformat()}-{end.isoformat()}",
        reused,
    )
    logger.info(
        "daily_bars: reused %d symbol(s) from %d prior successful batch(es); "
        "fetching the remaining scope",
        len(reused_symbols),
        len(files),
    )
    return reused_symbols


def _merge_ownership_result(
    out: dict,
    config: Config,
    ownership: DailyBarOwnership,
    start: date,
    end: date,
    run_id: str,
) -> dict:
    updates, delegated_complete = _ownership_context(config, ownership, start, end)
    context = out.setdefault("context_updates", {})
    context.setdefault("audit_findings", []).extend(updates["audit_findings"])
    context["daily_bars_ownership"] = updates["daily_bars_ownership"]
    if ownership.delegated_delisted and not delegated_complete:
        out["status"] = "warning"
        out["delegated_symbols"] = len(ownership.delegated_delisted)
    for settle in (
        etf_debts._settle_official_etf_prelisting_debts,
        etf_debts._settle_official_etf_delisted_debts,
    ):
        settlement = settle(config, end, run_id)
        if settlement:
            context.setdefault("audit_findings", []).append(settlement)
    return out


def _resolve_daily_bar_scope(config: Config, symbols: list[str]) -> list[str]:
    """Validate an explicit daily-bar repair scope against instruments."""
    requested = list(
        dict.fromkeys(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip())
    )
    if not requested:
        raise RuntimeError("daily_bars backfill symbols must not be empty")
    known = set(load_symbols(config))
    unknown = sorted(set(requested) - known)
    if unknown:
        preview = ", ".join(unknown[:8])
        suffix = "..." if len(unknown) > 8 else ""
        raise RuntimeError(
            f"daily_bars backfill symbols are not present in instruments: {preview}{suffix}"
        )
    return requested


_ETF_PROFILE_MAX_AGE_DAYS = 14


#: Fund-scope failures seen by the current ``step_daily_bars`` call.
_FUND_SCOPE_ISSUES: ContextVar[list[dict] | None] = ContextVar(
    "daily_bars_fund_scope_issues", default=None
)


def _fund_bar_scope(config: Config, on: date, *, retry: bool = False) -> list[str]:
    if not config.ingest_listed_funds:
        return _eligible_etf_bar_symbols(config, on, retry=retry)
    from cnequity.storage.instrument_catalog import listed_fund_symbols, load_curated_instruments

    instruments = load_curated_instruments(config)
    if instruments is None:
        raise RuntimeError("fund bar scope unavailable: no committed instruments")
    return listed_fund_symbols(instruments, on)


def _daily_fund_bar_symbols(config: Config, on: date, *, retry: bool = False) -> list[str] | None:
    """Fund codes to fetch on *on*, or None when the fund scope is unavailable.

    A broken fund directory holds the funds back, never the stocks: the step
    carries on and reports a warning naming the cause.
    """
    try:
        return _fund_bar_scope(config, on, retry=retry)
    except RuntimeError as exc:
        logger.warning("daily_bars: fund scope unavailable on %s; stocks continue: %s", on, exc)
        issues = _FUND_SCOPE_ISSUES.get()
        if issues is not None and not any(item["message"] == str(exc) for item in issues):
            issues.append(
                {
                    "dataset": "daily_bars",
                    "severity": "warning",
                    "check": "fund_bar_scope_unavailable",
                    "message": str(exc),
                    "trade_date": on.isoformat(),
                }
            )
        return None


def _eligible_etf_bar_symbols(config: Config, on: date, *, retry: bool = False) -> list[str]:
    """Extend bar ingestion only from a current, published ETF classification.

    The directory is an observed snapshot, never a historical eligibility
    service. A missing or failed collector fails the scope explicitly rather
    than silently omitting funds. The pinned read receipt makes the exact
    classification used for the fetch independently replayable.
    """
    if not config.ingest_eligible_etfs:
        return []
    from cnequity.query import dataset_attempt, load_with_receipt
    from cnequity.query.receipt import ReadReceiptError

    attempt = dataset_attempt("etf_profiles", config=config) if not retry else None
    if attempt and attempt.get("status") in {"failed", "blocked", "degraded"}:
        started = datetime.fromisoformat(attempt["started_at"])
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        if started.astimezone(ZoneInfo("Asia/Shanghai")).date() <= on:
            logger.warning(
                "ETF bar scope held after failed directory attempt %s", attempt.get("run_id")
            )
            raise RuntimeError("ETF bar scope unavailable after failed directory attempt")
    try:
        read = load_with_receipt("etf_profiles", config=config, end=on, require_replayable=True)
    except (ReadReceiptError, FileNotFoundError, ValueError) as exc:
        logger.warning("ETF bar scope unavailable: %s", exc)
        raise RuntimeError(f"ETF bar scope unavailable: {exc}") from exc
    frame = read.frame
    required = {"symbol", "as_of_date", "eligibility_status"}
    if not required.issubset(frame.columns) or frame.is_empty():
        logger.warning("ETF bar scope has no valid directory rows by %s", on)
        raise RuntimeError(f"ETF bar scope has no valid directory rows by {on}")
    current = frame.filter(pl.col("as_of_date") <= on)
    if current.is_empty():
        raise RuntimeError(f"ETF bar scope has no directory snapshot by {on}")
    observed = current["as_of_date"].max()
    if (on - observed).days > _ETF_PROFILE_MAX_AGE_DAYS:
        logger.warning("ETF bar scope held: directory dated %s is stale on %s", observed, on)
        raise RuntimeError(f"ETF bar scope directory dated {observed} is stale on {on}")
    return sorted(
        set(
            current.filter(
                (pl.col("as_of_date") == observed) & (pl.col("eligibility_status") == "eligible")
            )["symbol"].to_list()
        )
    )
