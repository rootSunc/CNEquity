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
from cnequity.domain.frames import with_columns_unless_blank
from cnequity.domain.schemas import with_provenance
from cnequity.domain.symbols import (
    is_all_a_symbol,
    is_cdr_symbol,
    is_tdx_servable,
    parse_symbol,
)
from cnequity.domain.trading_status import (
    DELISTED_SOURCE,
    STATUS_DELISTED,
    STATUS_SUSPENDED,
)
from cnequity.orchestrator.manifest import Manifest
from cnequity.orchestrator.registry import register_step
from cnequity.quality.st_coverage import (
    ST_EVIDENCE_VERSION,
    build_st_scope,
    current_st_universe,
    load_st_checkpoint,
    reusable_st_checkpoint_symbols,
    st_evidence_source_symbols,
    st_evidence_unsupported_symbols,
    write_st_checkpoint,
)
from cnequity.steps.common import (
    BACKFILL_START,
    fetch_incremental_daily,
    load_bar_universe,
    load_curated_instruments,
    load_symbols,
    write_simple,
)
from cnequity.steps.http_common import write_fetched

logger = logging.getLogger(__name__)

_CACHED_TRADING_STATUS_MAX_AGE = timedelta(days=5)

# Flush + checkpoint on the same boundary as Baostock's anti-blacklist batch.
# A larger chunk used to discard up to ~200 symbols of in-memory evidence when
# a process died between checkpoints, even though the provider had already
# paid the request cost. Keeping this at the provider batch size preserves the
# required cooldown while limiting resumable loss to one batch.
_ST_BACKFILL_CHUNK = 20
# Tushare's 2016 ``bak_basic`` query is market-wide. Keeping BJ symbols in a
# large batch avoids replaying the same 2016 date list once per 20-symbol
# Baostock batch; the adapter still fails and checkpoints individual symbols.
_TUSHARE_ST_BACKFILL_CHUNK = 500


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
    code-space sweep is what discovers them (``scripts/delisted_ops.py discover``); this
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

    basics = fetch_instrument_basics(config=config)
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


# Rows for securities that had already left the market. The daily feeds cannot
# supply these: EastMoney's boards answer "is it halted" and "is it on the risk
# board", and a delisted name is on neither, so the old `else` branch published
# it as normally trading. Measured 2026-08-28 on a full lake, that was 611
# symbols — every symbol carrying a `delist_date`, one of them since 1999-07-12.
#
# The classification comes from `instruments`, not from a vendor snapshot, so
# the rows carry their own source rather than being stamped as EastMoney's
# answer. `derive/trading_status_history.py` ranks it explicitly.


def _delisted_instruments(config: Config) -> pl.DataFrame:
    """``symbol, delist_date, risk_warning`` for every formally delisted name.

    ``risk_warning`` is read from the final 简称 the catalog carries — a name
    such as ``*ST元成`` is the exchange's own designation at the point trading
    stopped, and it is the only evidence available once the boards drop the
    symbol.
    """
    from cnequity.adapters.exchange.st_lists import is_st_name

    frame = load_curated_instruments(config)
    if frame is None or frame.is_empty():
        return pl.DataFrame(
            schema={"symbol": pl.Utf8, "delist_date": pl.Date, "risk_warning": pl.Boolean}
        )
    if not {"symbol", "delist_date"}.issubset(frame.columns):
        return pl.DataFrame(
            schema={"symbol": pl.Utf8, "delist_date": pl.Date, "risk_warning": pl.Boolean}
        )
    name_col = pl.col("name") if "name" in frame.columns else pl.lit(None, dtype=pl.Utf8)
    return (
        frame.filter(pl.col("delist_date").is_not_null())
        .select(
            "symbol",
            "delist_date",
            name_col.map_elements(
                lambda value: is_st_name(value) if value else False, return_dtype=pl.Boolean
            ).alias("risk_warning"),
        )
        .unique(subset=["symbol"], keep="last")
    )


def _delisted_status_rows(
    delisted: pl.DataFrame, symbols: list[str], days: list[date]
) -> pl.DataFrame:
    """One ``status=delisted`` row per (symbol, day) already past delisting."""
    if delisted.is_empty() or not symbols or not days:
        return pl.DataFrame()
    scoped = delisted.filter(pl.col("symbol").is_in(symbols))
    if scoped.is_empty():
        return pl.DataFrame()
    rows = scoped.join(pl.DataFrame({"trade_date": days}), how="cross").filter(
        pl.col("delist_date") <= pl.col("trade_date")
    )
    if rows.is_empty():
        return pl.DataFrame()
    return rows.select(
        "symbol",
        "trade_date",
        pl.lit(False).alias("is_trading"),
        pl.lit(STATUS_DELISTED).alias("status"),
        "risk_warning",
        # Only the owner is stamped here. The run-level provenance is applied
        # once, to every row, after the fetch.
        pl.lit(DELISTED_SOURCE).alias("source"),
    )


@register_step("trading_status", group="core")
def step_trading_status(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if getattr(config, "_backfill", False):
        return _backfill_trading_status_st(
            config,
            trade_date,
            run_id,
            batch_id=context.get("_batch_id"),
        )

    symbols = context.get("symbols") or load_symbols(config)
    rl = config.tdx_rate_limit_spec()

    # A delisted security is not something the daily boards can report on, so
    # it is removed from the request and written separately below. Scoped per
    # day rather than once for the step: a watermark catch-up spans several
    # sessions, and a name delisted inside that span was genuinely live for the
    # earlier ones.
    delisted = _delisted_instruments(config)
    delist_dates: dict[str, date] = (
        dict(zip(delisted["symbol"], delisted["delist_date"], strict=True))
        if not delisted.is_empty()
        else {}
    )

    def _live_symbols(day: date) -> list[str]:
        if not delist_dates:
            return symbols
        return [sym for sym in symbols if (gone := delist_dates.get(sym)) is None or gone > day]

    # EastMoney is the only daily ST feed. An AkShare union used to sit here as a
    # "second source", but `ak.stock_zh_a_st_em` requests the same push2 clist
    # endpoint with the same `fs=m:0+f:4,m:1+f:4` filter that
    # adapters/eastmoney/trading_status.py already queries — same vendor, same
    # board, same filter — so it could only ever repeat this answer or fail. The
    # push2 → push2delay failover in the EastMoney client is the real robustness
    # here. The one genuinely independent ST reading, baostock's per-day `isST`,
    # is a per-symbol sweep and stays where it is affordable: the `--backfill`
    # path below. See issue #3.
    fallback_days: list[date] = []
    fallback_errors: list[str] = []
    cached_snapshot: pl.DataFrame | None = None

    def _cached_status(day: date) -> pl.DataFrame | None:
        """Return the last curated status snapshot, conservatively expanded.

        trading_status is advisory for the daily signal gate. When EastMoney is
        temporarily unreachable, keeping known ST/halt labels is safer than
        turning every name into ``normal``. Symbols absent from the cached
        snapshot are marked suspended so an outage cannot add an unverified
        name to the tradable universe.
        """
        nonlocal cached_snapshot
        if cached_snapshot is None:
            try:
                from cnequity.query.reader import load

                history = load(
                    "trading_status",
                    data_root=config.data_root,
                    # A cached fallback is accepted only when its latest
                    # snapshot is at most five days old. Bound the read to
                    # that same window; passing only ``end`` would decode the
                    # entire historical status lake (17M+ rows in a real
                    # full-market lake) during an outage.
                    start=day - _CACHED_TRADING_STATUS_MAX_AGE,
                    end=day - timedelta(days=1),
                )
            except Exception as exc:  # noqa: BLE001 — fallback is best effort
                logger.warning("cached trading_status unavailable: %s", exc)
                return None
            if history.is_empty():
                return None
            latest = history.get_column("trade_date").max()
            if latest is None or day - latest > _CACHED_TRADING_STATUS_MAX_AGE:
                logger.warning(
                    "cached trading_status is too old for %s: latest=%s max_age=%s",
                    day,
                    latest,
                    _CACHED_TRADING_STATUS_MAX_AGE,
                )
                return None
            columns = ["symbol", "is_trading", "status"]
            if "risk_warning" in history.columns:
                columns.append("risk_warning")
            cached_snapshot = history.filter(pl.col("trade_date") == latest).select(columns)

        known = {
            row["symbol"]: row for row in cached_snapshot.iter_rows(named=True) if row.get("symbol")
        }
        rows = []
        for symbol in _live_symbols(day):
            previous = known.get(symbol)
            rows.append(
                {
                    "symbol": symbol,
                    "trade_date": day,
                    "is_trading": bool(previous["is_trading"]) if previous else False,
                    "status": previous["status"] if previous else STATUS_SUSPENDED,
                    # An outage must not clear a risk-warning label. Unknown
                    # stays unknown rather than becoming a claim of "clean".
                    "risk_warning": previous.get("risk_warning") if previous else None,
                }
            )
        return pl.DataFrame(rows, schema_overrides={"risk_warning": pl.Boolean})

    def _fetch(day: date):
        """Vendor rows for the live universe plus this day's delisted rows.

        The two are merged here rather than after the fetch so that a day whose
        whole universe has delisted still produces rows, and so that each row
        carries the owner that actually knows the fact.
        """
        gone = _delisted_status_rows(delisted, symbols, [day])
        vendor = _fetch_vendor(day)
        # `with_columns_unless_blank`: a session whose whole universe had
        # delisted returns the column-less empty frame, and stamping it would
        # add a row with no symbol and no date (see domain/frames.py).
        vendor = with_columns_unless_blank(
            vendor.drop("source", strict=False),
            pl.lit(None, dtype=pl.Utf8).alias("source"),
        )
        if gone.is_empty():
            return vendor
        if vendor.is_empty():
            return gone
        return pl.concat([vendor, gone], how="diagonal_relaxed")

    def _fetch_vendor(day: date):
        nonlocal cached_snapshot
        day_symbols = _live_symbols(day)
        if not day_symbols:
            # Every name in the universe had already delisted by this session.
            # Asking the boards about nothing is a wasted request, not a state
            # the adapters are expected to handle.
            return pl.DataFrame()
        expected_symbols = set(day_symbols)
        try:
            frame = fetch_trading_status(
                day_symbols,
                day,
                rate_limit=rl,
                allow_mock=config.tdx_allow_mock,
                config=config,
            )
            if frame.is_empty():
                raise RuntimeError("trading_status: no rows returned")
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
                raise RuntimeError(
                    "trading_status: incomplete daily snapshot; " + "; ".join(details)
                )
            cached_snapshot = frame.select("symbol", "is_trading", "status")
            return frame
        except Exception as exc:  # noqa: BLE001 — EastMoney outage is advisory here
            cached = _cached_status(day)
            if cached is None:
                raise
            fallback_days.append(day)
            fallback_errors.append(f"{day.isoformat()}: {exc}")
            logger.warning(
                "trading_status: EastMoney unavailable for %s; using last curated snapshot",
                day,
            )
            return cached

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
    findings = list(_findings)
    if fallback_days:
        findings.append(
            {
                "dataset": "trading_status",
                "severity": "warning",
                "check": "trading_status_cached_fallback",
                "message": (
                    f"EastMoney unavailable for {len(fallback_days)} day(s); reused the "
                    "last curated status snapshot and conservatively suspended unknown symbols"
                ),
                "days": [day.isoformat() for day in fallback_days],
                "errors": fallback_errors[:3],
            }
        )
    # This adapter is an EastMoney current-state snapshot even though it is
    # exposed through the TDX facade. Preserve the actual evidence owner so
    # downstream PIT precedence never mistakes it for exchange history.
    source = "eastmoney_cached" if fallback_days else "eastmoney"
    # Rows that named their own owner keep it — a delisting is a fact from
    # `instruments`, not the vendor's answer. Everything else is the snapshot.
    if "source" in df.columns:
        df = df.with_columns(pl.col("source").fill_null(source))
    df = with_provenance(df, source=source, data_version="v1")
    result = write_simple(config, run_id, "trading_status", df)
    if findings:
        result["context_updates"] = {"audit_findings": findings}
    return result


# How far back the daily run re-examines sessions for interior bar gaps. A halt
# only becomes visible here once the symbol trades again, so the window has to
# outlive an ordinary suspension rather than just cover the last few sessions.
# Anything older is closed by an explicit rebuild (`cne derive trading_status`,
# which walks the full history) — the daily step deliberately does not pay for
# a whole-history cross-join every evening.
DERIVE_TAIL_DAYS = 90


@register_step("trading_status_derive", group="core", depends_on=["daily_bars"])
def step_trading_status_derive(
    config: Config, trade_date: date, run_id: str, context: dict
) -> dict:
    """Stage suspensions reconstructed from `daily_bars` interior gaps.

    Runs before compact so the rows are published as part of this run's
    committed generation. Nothing here reads the mutable curated directory,
    and compact resolves any collision with the vendor board by evidence class
    (`domain/trading_status`), so re-running the step is a no-op rather than a
    fight with the daily snapshot.

    Today's bars are still in staging at this point, which costs nothing: a
    halt is only visible as a gap once the symbol has traded again, so the
    newest session never contributes a row either way.

    In backfill mode — `cne backfill`, and init's `phase5_derive_and_publish`,
    which runs after the bars have been committed — the window is dropped and
    the whole bar history is reconstructed in one pass.
    """
    from cnequity.derive.trading_status_history import derive_suspension_history

    start = context.get("derive_start")
    end = context.get("derive_end")
    full = context.get("derive_full") or getattr(config, "_backfill", False)
    if start is None and end is None and not full:
        start = trade_date - timedelta(days=DERIVE_TAIL_DAYS)
        end = trade_date
    rows = derive_suspension_history(
        config,
        run_id,
        start=start,
        end=end,
        # Distinct from the vendor snapshot's batch: both write the same
        # dataset under this run_id, and a shared batch id would make one
        # staging file overwrite the other.
        batch_id=str(context.get("_batch_id") or "derive-0"),
    )
    return {"rows_read": rows, "rows_written": rows}


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


def _resolve_st_backfill_universe(config: Config, start: date, end: date) -> tuple[list[str], str]:
    explicit = getattr(config, "_backfill_symbols", None)
    if explicit is not None:
        return _resolve_explicit_st_symbols(config, explicit), "explicit"
    universe = current_st_universe(config, start=start, end=end)
    if not universe:
        universe = [symbol for symbol in load_symbols(config) if _is_all_a(symbol)]
        bars = load_bar_universe(config)
        if bars:
            universe = [symbol for symbol in universe if symbol in bars]
    return universe, "all_a"


def _backfill_trading_status_st_source(
    config: Config,
    trade_date: date,
    run_id: str,
    *,
    universe: list[str],
    universe_name: str,
    source: str,
    batch_id: str | None = None,
) -> dict:
    """Backfill one source-owned slice of the historical ST evidence scope."""
    if source == "baostock":
        from cnequity.adapters.baostock.st_history import fetch_st_history
    elif source == "tushare":
        from cnequity.adapters.tushare.st_history import fetch_st_history
    else:
        raise ValueError(f"unknown historical ST source: {source}")

    start = getattr(config, "_backfill_start", None) or BACKFILL_START
    end = getattr(config, "_backfill_end", None) or trade_date
    scope = build_st_scope(universe, start, end, universe=universe_name, source=source)
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
            "completed_symbols": len(completed),
            "expected_symbols": len(universe),
            "note": "all symbols already have ST evidence for this exact scope",
        }

    rows_read = 0
    rows_written = 0
    manifest = Manifest(config.manifest_path) if batch_id else None
    chunk_size = _TUSHARE_ST_BACKFILL_CHUNK if source == "tushare" else _ST_BACKFILL_CHUNK
    for offset in range(0, len(todo), chunk_size):
        batch = todo[offset : offset + chunk_size]
        is_last_batch = offset + len(batch) >= len(todo)
        if manifest is not None:
            manifest.touch_batch_heartbeat(run_id, batch_id)
        kwargs = {"config": config, "rest_after_batch": not is_last_batch}
        if source == "tushare":
            kwargs.pop("rest_after_batch")
        df, failed = fetch_st_history(batch, start, end, **kwargs)
        if not df.is_empty():
            chunk = write_fetched(
                config,
                run_id,
                "trading_status",
                df,
                source=source,
                batch_id=f"{source}-batch-{offset:05d}",
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
        "source": source,
    }
    if complete:
        result["coverage_pending_compact"] = True
    else:
        result["status"] = "warning"
        result["failed_symbols"] = len(unresolved)
        finding = {
            "dataset": "trading_status",
            "severity": "warning",
            "code": f"{source}_st_backfill_incomplete",
            "message": (
                f"{len(unresolved)}/{len(universe)} symbols remain unresolved in "
                f"{source} ST evidence; re-run the same scoped backfill to resume."
            ),
        }
        result.setdefault("context_updates", {})["audit_findings"] = [finding]
    return result


def _backfill_trading_status_st(
    config: Config,
    trade_date: date,
    run_id: str,
    *,
    batch_id: str | None = None,
) -> dict:
    start = getattr(config, "_backfill_start", None) or BACKFILL_START
    end = getattr(config, "_backfill_end", None) or trade_date
    explicit = getattr(config, "_backfill_symbols", None)
    universe, universe_name = _resolve_st_backfill_universe(config, start, end)

    # Baostock has no BJ historical ST series. Do not spend a full retry cycle
    # on a known provider capability gap; the coverage report still keeps those
    # valid all-A symbols visible as an explicit unsupported-exchange blocker.
    unsupported_symbols = st_evidence_unsupported_symbols(
        universe,
        config=config,
        start=start,
        end=end,
    )
    if explicit is not None and unsupported_symbols:
        preview = ", ".join(unsupported_symbols[:10])
        suffix = "..." if len(unsupported_symbols) > 10 else ""
        raise ValueError(
            f"trading_status ST backfill cannot query BJ symbols with Baostock: {preview}{suffix}"
        )
    results: list[dict] = []
    for source in ("baostock", "tushare"):
        source_symbols = st_evidence_source_symbols(
            universe,
            source,
            config=config,
            start=start,
            end=end,
        )
        if not source_symbols:
            continue
        results.append(
            _backfill_trading_status_st_source(
                config,
                trade_date,
                run_id,
                universe=source_symbols,
                universe_name=universe_name,
                source=source,
                batch_id=batch_id,
            )
        )
    if not results:
        raise RuntimeError("trading_status ST backfill has no configured historical source")
    result: dict = {
        "rows_read": sum(int(item.get("rows_read", 0)) for item in results),
        "rows_written": sum(int(item.get("rows_written", 0)) for item in results),
        "completed_symbols": sum(int(item.get("completed_symbols", 0)) for item in results),
        "expected_symbols": sum(int(item.get("expected_symbols", 0)) for item in results),
        "source_results": results,
    }
    if unsupported_symbols:
        result["unsupported_symbols"] = len(unsupported_symbols)
        result["unsupported_exchanges"] = ["BJ"]
    if any(item.get("status") == "warning" for item in results):
        result["status"] = "warning"
        result["failed_symbols"] = sum(int(item.get("failed_symbols", 0)) for item in results)
        result["context_updates"] = {
            "audit_findings": [
                finding
                for item in results
                for finding in item.get("context_updates", {}).get("audit_findings", [])
            ]
        }
    else:
        result["coverage_pending_compact"] = True
    return result
