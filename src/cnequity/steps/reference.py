"""L0 reference steps: instruments, trading_calendar, trading_status."""

from __future__ import annotations

import logging
from datetime import date, timedelta

import polars as pl

from cnequity.adapters.calendar.exchange_calendar import curated_bar_dates
from cnequity.adapters.eastmoney.instruments import enrich_instrument_list_dates
from cnequity.adapters.tdx_protocol.client import (
    fetch_instruments,
    fetch_trading_calendar,
    fetch_trading_status,
    normalize_with_source,
)
from cnequity.config import Config
from cnequity.domain.schemas import with_provenance
from cnequity.domain.symbols import (
    is_all_a_symbol,
    is_cdr_symbol,
    is_tdx_servable,
    parse_symbol,
)
from cnequity.orchestrator.registry import register_step
from cnequity.quality.failover import (
    fetch_trading_status_backup,
    snapshot_trading_status_backup,
)
from cnequity.quality.st_coverage import (
    ST_EVIDENCE_VERSION,
    build_st_scope,
    current_st_universe,
    load_st_checkpoint,
    reusable_st_checkpoint_symbols,
    write_st_checkpoint,
)
from cnequity.steps.common import (
    BACKFILL_START,
    fetch_incremental_daily,
    load_bar_universe,
    load_symbols,
    write_simple,
)
from cnequity.steps.http_common import write_fetched

logger = logging.getLogger(__name__)

# Flush + checkpoint on the same boundary as Baostock's anti-blacklist batch.
# A larger chunk used to discard up to ~200 symbols of in-memory evidence when
# a process died between checkpoints, even though the provider had already
# paid the request cost. Keeping this at the provider batch size preserves the
# required cooldown while limiting resumable loss to one batch.
_ST_BACKFILL_CHUNK = 20


@register_step("instruments", group="core", requires_workers=False)
def step_instruments(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    rl = config.tdx_rate_limit_spec()
    df = fetch_instruments(rate_limit=rl, allow_mock=config.tdx_allow_mock, config=config)
    df = normalize_with_source(df)
    df = enrich_instrument_list_dates(config, df)
    df = _merge_untdxable_instruments(config, df)
    if getattr(config, "_backfill", False):
        df = _merge_delisted_instruments(config, df)
    return write_simple(config, run_id, "instruments", df)


def _merge_untdxable_instruments(config: Config, df: pl.DataFrame) -> pl.DataFrame:
    """Add listed symbols the TDX security list structurally cannot contain.

    TDX serves Shanghai and Shenzhen only, so the Beijing exchange never
    appeared in the snapshot and the lake carried zero BJ instruments — meaning
    ``universe="all_a"`` quietly resolved to two exchanges out of three. The
    code-space sweep is what discovers them (``cne delisted discover``); this
    reads its live-but-missing bucket so the daily bar step has symbols to
    route to the fallback vendor.

    Runs every day, not only under --backfill: without it the next instruments
    compact would see every BJ name as absent from the snapshot and start
    inferring delistings for stocks that are trading normally.
    """
    from cnequity.steps.delisted import load_live_missing

    try:
        live_missing = load_live_missing(config)
    except Exception as exc:  # noqa: BLE001 — a missing catalogue is not fatal
        logger.debug("no delisted catalogue to read untdxable instruments from: %s", exc)
        return df
    known = set(df["symbol"].to_list()) if not df.is_empty() else set()
    recovered = sorted(s for s in live_missing if s not in known and not is_tdx_servable(s))
    if not recovered:
        return df

    logger.info(
        "instruments: +%d listed symbol(s) with no TDX route (e.g. %s)",
        len(recovered),
        ", ".join(recovered[:3]),
    )
    rows = pl.DataFrame(
        {
            "symbol": recovered,
            "name": [None] * len(recovered),
            "exchange": [s.split(".")[1] for s in recovered],
            "asset_type": ["stock"] * len(recovered),
            "list_date": pl.Series([None] * len(recovered), dtype=pl.Date),
            "delist_date": pl.Series([None] * len(recovered), dtype=pl.Date),
            "prev_symbol": [None] * len(recovered),
        }
    )
    return pl.concat(
        [df, with_provenance(rows, source="sina", data_version="v1")], how="diagonal_relaxed"
    )


def _merge_delisted_instruments(config: Config, df: pl.DataFrame) -> pl.DataFrame:
    """Add baostock's delisted names to a live-snapshot instrument list.

    TDX and EastMoney both answer "what is listed today", so on their own they
    build a survivors-only lake (audit: ``universe_survivorship_absent``).
    baostock's ``query_stock_basic`` is the one free source that also returns
    codes that *stopped* existing, which is what makes a point-in-time universe
    possible at all.

    Only rows baostock marks delisted are appended. Names it calls listed but the
    live snapshot omits are ambiguous — a delisting the snapshot has not caught up
    with, or a baostock staleness artefact — and appending them would inject
    untradable symbols into ``all_a``; they are counted and logged instead.

    Fail-loud: this runs only under an explicit ``--backfill``, whose entire
    purpose is the delisted set, so a broken baostock session must not quietly
    degrade into "no delisted names exist".
    """
    if not config.sources.get("baostock", False):
        logger.warning(
            "instruments backfill: [sources.baostock] disabled — delisted symbols "
            "cannot be recovered from TDX/EastMoney alone; universe stays survivors-only"
        )
        return df

    from cnequity.adapters.baostock.instruments import fetch_instrument_basics

    config.rate_limit("baostock")
    basics = fetch_instrument_basics()
    if basics.is_empty():
        raise RuntimeError(
            "baostock query_stock_basic returned no rows; refusing to write a "
            "survivors-only instrument list under --backfill"
        )

    live = set(df["symbol"].to_list())
    delisted = basics.filter(pl.col("delist_date").is_not_null() & ~pl.col("symbol").is_in(live))
    unlisted_unknown = basics.filter(
        pl.col("delist_date").is_null() & ~pl.col("symbol").is_in(live)
    ).height

    # baostock's ipoDate reaches further back than EastMoney's clist, so it also
    # fills list_date holes on names that are still trading.
    known_list_dates = basics.filter(pl.col("list_date").is_not_null()).select(
        ["symbol", pl.col("list_date").alias("_bs_list_date")]
    )
    df = (
        df.join(known_list_dates, on="symbol", how="left")
        .with_columns(pl.coalesce(pl.col("list_date"), pl.col("_bs_list_date")).alias("list_date"))
        .drop("_bs_list_date")
    )

    logger.info(
        "instruments backfill: +%d delisted symbol(s) from baostock "
        "(%d listed-but-absent skipped as ambiguous)",
        delisted.height,
        unlisted_unknown,
    )
    # Keep formal security-master identity separate from dates inferred by
    # instruments compaction or by the bar-recovery path.
    from cnequity.steps.delisted import write_delisted_identity_evidence

    write_delisted_identity_evidence(
        config,
        basics.filter(pl.col("delist_date").is_not_null()),
    )
    if delisted.is_empty():
        return df
    return pl.concat(
        [df, with_provenance(delisted, source="baostock", data_version="v1")],
        how="diagonal_relaxed",
    )


def _earliest_bar_date(config: Config) -> date | None:
    """First date any bar dataset carries — the calendar has to reach at least
    that far back or every window over the deep history resolves to zero
    sessions.

    Both datasets, matching ``_trading_days_from_bars`` in the calendar adapter.
    Reading only daily_bars pinned the calendar's start to 2001 while index_bars
    reached 1990-12-19, so 2,538 index_bars dates sat before the calendar
    existed and the audit reported them as bars on non-trading days — a warning
    about the calendar being short, dressed as a warning about the data.
    """
    earliest: date | None = None
    for dataset in ("index_bars", "daily_bars"):
        root = config.curated_root / dataset
        if not root.exists():
            continue
        dates = curated_bar_dates(config.curated_root, dataset)
        if not dates:
            continue
        first = min(dates)
        if first is not None and (earliest is None or first < earliest):
            earliest = first
    return earliest


@register_step("trading_calendar", group="core")
def step_trading_calendar(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if getattr(config, "_backfill", False):
        start = getattr(config, "_backfill_start", None) or BACKFILL_START
        # Deep history reaches back further than BACKFILL_START; follow the data.
        earliest = _earliest_bar_date(config)
        if earliest is not None and earliest < start:
            start = earliest
    else:
        start = trade_date - timedelta(days=30)
    end = trade_date + timedelta(days=365)
    rl = config.tdx_rate_limit_spec()
    seed_path = config.meta_root / "seeds" / "trading_calendar.csv"
    df = fetch_trading_calendar(
        start,
        end,
        rate_limit=rl,
        allow_mock=config.tdx_allow_mock,
        curated_root=config.curated_root,
        seed_path=seed_path if seed_path.exists() else None,
    )
    df = normalize_with_source(df, source="exchange_calendar")
    return write_simple(config, run_id, "trading_calendar", df)


@register_step("trading_status", group="core")
def step_trading_status(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if getattr(config, "_backfill", False):
        return _backfill_trading_status_st(config, trade_date, run_id)

    symbols = context.get("symbols") or load_symbols(config)
    expected_symbols = set(symbols)
    rl = config.tdx_rate_limit_spec()

    # EastMoney is the primary daily ST/suspension feed; the baostock snapshot
    # is the failover when EastMoney's push2/datacenter legs fail (see
    # [[failover.datasets]] name="trading_status"). BJ ST tags are not covered
    # by either vendor; the coordinator counts BJ defaults explicitly.
    degraded: dict = {}

    def _fetch(day: date):
        nonlocal degraded
        try:
            frame = fetch_trading_status(
                symbols,
                day,
                rate_limit=rl,
                allow_mock=config.tdx_allow_mock,
                config=config,
            )
        except Exception as primary_exc:
            backup, degraded = fetch_trading_status_backup(config, symbols, day)
            if backup is None or backup.is_empty():
                raise primary_exc
            frame = backup
        if frame.is_empty():
            return frame
        if "symbol" not in frame.columns:
            raise RuntimeError("trading_status: response is missing the symbol column")
        observed_symbols = set(frame.get_column("symbol").drop_nulls().to_list())
        missing = sorted(expected_symbols - observed_symbols)
        unexpected = sorted(observed_symbols - expected_symbols)
        if missing or unexpected:
            details: list[str] = []
            if missing:
                details.append(f"missing {len(missing)} requested symbol(s)")
            if unexpected:
                details.append(f"returned {len(unexpected)} unexpected symbol(s)")
            raise RuntimeError("trading_status: incomplete daily snapshot; " + "; ".join(details))
        return frame

    df, _findings = fetch_incremental_daily(
        config,
        "trading_status",
        trade_date,
        _fetch,
        allow_empty=False,
    )
    if df.is_empty():
        result = {"rows_read": 0, "rows_written": 0}
        if _findings:
            result["context_updates"] = {"audit_findings": _findings}
        return result
    # The adapter is a current-state snapshot even when it is exposed through
    # the TDX facade. Record the actual evidence owner so downstream PIT
    # precedence never mistakes a degraded day for exchange history.
    origin = degraded.get("source", "eastmoney")
    df = with_provenance(df.drop("source", strict=False), source=origin, data_version="v1")
    result = write_simple(config, run_id, "trading_status", df)
    findings = list(_findings)
    if degraded.get("failover_used"):
        snapshot_trading_status_backup(config, df=df, run_id=run_id, trade_date=trade_date)
        findings.append(
            {
                "dataset": "trading_status",
                "check": "failover_degraded",
                "severity": "warning",
                "detail": (
                    "primary (eastmoney) failed; baostock backup used "
                    f"(n_filled={degraded.get('n_filled', 0)}, "
                    f"n_scope_defaults={degraded.get('n_scope_defaults', 0)}, "
                    f"n_bj_defaulted={degraded.get('n_bj_defaulted', 0)})"
                ),
            }
        )
        result["status"] = "warning"
    if findings:
        result["context_updates"] = {"audit_findings": findings}
    return result


def _is_all_a(symbol: str) -> bool:
    try:
        info = parse_symbol(symbol)
    except ValueError:
        return False
    return is_all_a_symbol(info.code, info.exchange) and not is_cdr_symbol(info.code, info.exchange)


def _resolve_explicit_st_symbols(config: Config, raw: list[str]) -> list[str]:
    instruments = set(load_symbols(config))
    resolved: list[str] = []
    for value in raw:
        symbol = str(value).strip().upper()
        if "." in symbol:
            candidates = [symbol] if symbol in instruments else []
        else:
            candidates = [item for item in instruments if item.startswith(symbol + ".")]
        if len(candidates) != 1 or not _is_all_a(candidates[0]):
            raise ValueError(
                f"trading_status ST backfill cannot resolve {value!r} "
                f"to exactly one all-A instrument"
            )
        resolved.append(candidates[0])
    return sorted(set(resolved))


def _backfill_trading_status_st(config: Config, trade_date: date, run_id: str) -> dict:
    """Persist complete historical ST/normal evidence from Baostock.

    Completion is an exact versioned scope, not inferred from row presence. The
    old sparse-ST checkpoint is deliberately ignored because it marked never-ST
    names complete without storing their negative evidence.
    """
    from cnequity.adapters.baostock.st_history import fetch_st_history

    start = getattr(config, "_backfill_start", None) or BACKFILL_START
    end = getattr(config, "_backfill_end", None) or trade_date
    explicit = getattr(config, "_backfill_symbols", None)
    if explicit is not None:
        universe = _resolve_explicit_st_symbols(config, explicit)
        universe_name = "explicit"
    else:
        universe = current_st_universe(config)
        if not universe:
            universe = [symbol for symbol in load_symbols(config) if _is_all_a(symbol)]
            bars = load_bar_universe(config)
            if bars:
                universe = [symbol for symbol in universe if symbol in bars]
        universe_name = "all_a"

    scope = build_st_scope(universe, start, end, universe=universe_name)
    checkpoint = load_st_checkpoint(config, scope)
    completed = reusable_st_checkpoint_symbols(config, checkpoint, run_id)
    evidence_rows = {
        symbol: int(count)
        for symbol, count in (checkpoint.get("evidence_rows_by_symbol") or {}).items()
        if symbol in completed
    }
    unresolved = set(checkpoint.get("unresolved_symbols", []))
    todo = [symbol for symbol in universe if symbol not in completed]
    if not todo:
        checkpoint["status"] = "complete"
        checkpoint["unresolved_symbols"] = []
        checkpoint["completion_run_id"] = run_id
        write_st_checkpoint(config, checkpoint)
        return {
            "rows_read": 0,
            "rows_written": 0,
            "scope_id": scope["scope_id"],
            "evidence_version": ST_EVIDENCE_VERSION,
            "coverage_pending_compact": True,
            "note": "all symbols already have ST evidence for this exact scope",
        }

    rows_read = 0
    rows_written = 0
    for offset in range(0, len(todo), _ST_BACKFILL_CHUNK):
        batch = todo[offset : offset + _ST_BACKFILL_CHUNK]
        is_last_batch = offset + len(batch) >= len(todo)
        df, failed = fetch_st_history(
            batch,
            start,
            end,
            config=config,
            rest_after_batch=not is_last_batch,
        )
        if not df.is_empty():
            chunk = write_fetched(
                config,
                run_id,
                "trading_status",
                df,
                source="baostock",
                batch_id=f"batch-{offset:05d}",
            )
            rows_read += int(chunk.get("rows_read", 0))
            rows_written += int(chunk.get("rows_written", 0))
        failed_set = set(failed)
        swept = [s for s in batch if s not in failed_set]
        chunk_counts = (
            {
                row["symbol"]: int(row["len"])
                for row in df.group_by("symbol").len().iter_rows(named=True)
            }
            if not df.is_empty()
            else {}
        )
        for symbol in swept:
            evidence_rows[symbol] = chunk_counts.get(symbol, 0)
        completed.update(swept)
        unresolved.difference_update(swept)
        unresolved.update(failed_set)
        checkpoint.update(
            {
                "status": "incomplete" if unresolved else "pending",
                "completed_symbols": sorted(completed),
                "evidence_rows_by_symbol": dict(sorted(evidence_rows.items())),
                "unresolved_symbols": sorted(unresolved),
            }
        )
        write_st_checkpoint(config, checkpoint)
    complete = completed == set(universe) and not unresolved
    checkpoint["status"] = "complete" if complete else "incomplete"
    checkpoint["completed_symbols"] = sorted(completed)
    checkpoint["evidence_rows_by_symbol"] = dict(sorted(evidence_rows.items()))
    checkpoint["unresolved_symbols"] = sorted(unresolved)
    if complete:
        checkpoint["completion_run_id"] = run_id
    else:
        checkpoint.pop("completion_run_id", None)
    checkpoint_path = write_st_checkpoint(config, checkpoint)
    result: dict = {
        "rows_read": rows_read,
        "rows_written": rows_written,
        "scope_id": scope["scope_id"],
        "evidence_version": ST_EVIDENCE_VERSION,
        "checkpoint": str(checkpoint_path),
        "completed_symbols": len(completed),
        "expected_symbols": len(universe),
    }
    if complete:
        result["coverage_pending_compact"] = True
    else:
        result["status"] = "warning"
        result["failed_symbols"] = len(unresolved)
        finding = {
            "dataset": "trading_status",
            "severity": "warning",
            "code": "baostock_st_backfill_incomplete",
            "message": (
                f"{len(unresolved)}/{len(universe)} symbols remain unresolved in "
                "Baostock ST evidence; re-run the same scoped backfill to resume."
            ),
        }
        result.setdefault("context_updates", {})["audit_findings"] = [finding]
    return result
