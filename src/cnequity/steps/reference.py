"""L0 reference steps: instruments, trading_calendar, trading_status."""

from __future__ import annotations

import logging
from collections.abc import Callable
from copy import copy
from datetime import date, timedelta

import polars as pl

from cnequity.adapters.calendar.exchange_calendar import calendar_seed_end, curated_bar_dates
from cnequity.adapters.eastmoney.instruments import enrich_instrument_list_dates
from cnequity.adapters.tdx_protocol.client import (
    fetch_instruments,
    fetch_trading_calendar,
    fetch_trading_status,
    normalize_with_source,
)
from cnequity.config import Config
from cnequity.domain.frames import with_columns_unless_blank
from cnequity.domain.http_policy import SourceCoolingDown
from cnequity.domain.market_profile import BSE_FIRST_SESSION, served
from cnequity.domain.schemas import with_provenance
from cnequity.domain.symbols import (
    is_all_a_symbol,
    is_cdr_symbol,
    is_etf_symbol,
    is_tdx_servable,
    parse_symbol,
)
from cnequity.domain.trading_status import (
    CARRY_SESSIONS,
    DELISTED_SOURCE,
    STATUS_DELISTED,
    cached_source,
    impossible_delistings,
    last_first_hand_status,
    split_before_listing,
)
from cnequity.i18n import N_
from cnequity.orchestrator.manifest import Manifest
from cnequity.orchestrator.outcomes import (
    SOURCE_LIMIT_REASONS,
    SourceUnavailableError,
    error_kind,
    result_is_usable,
)
from cnequity.orchestrator.registry import register_step
from cnequity.progress import stage_progress
from cnequity.quality.failover import snapshot_trading_status_exchange
from cnequity.quality.st_coverage import (
    ST_EVIDENCE_UNSUPPORTED_EXCHANGES,
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
    list_trading_dates,
    load_bar_universe,
    load_curated_instruments,
    load_symbols,
    write_simple,
)
from cnequity.steps.http_common import write_fetched
from cnequity.storage.state import StateStore

logger = logging.getLogger(__name__)

# Counted in sessions: a calendar-day limit expired the cache across every
# National Day and Spring Festival break, which is exactly when it is needed.
_CACHED_TRADING_STATUS_MAX_SESSIONS = CARRY_SESSIONS
# Calendar span searched for those sessions; longer than any exchange closure.
_CACHED_TRADING_STATUS_SEARCH_SPAN = timedelta(days=30)


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
    backfill = getattr(config, "_backfill", False)
    stage = stage_progress(logger, "instruments", 6 if backfill else 5)
    stage(N_("TDX security list"))
    df = fetch_instruments(rate_limit=rl, allow_mock=config.tdx_allow_mock, config=config)
    df = normalize_with_source(df, "tdx_protocol")
    # Enrichment last, and deliberately: it fills null list_dates from
    # EastMoney, and the two merges below are exactly where the undated rows
    # come from. Enriching the TDX snapshot first left every Beijing name with
    # a null list_date forever, which is also what marks a symbol as a
    # not-yet-listed placeholder — so the names these merges recover would have
    # been discovered and then never fetched.
    stage(N_("Beijing board"), held=df.height)
    df = _merge_bse_instruments(config, df, trade_date)
    df = _merge_untdxable_instruments(config, df)
    findings = []
    beijing_gap = _require_beijing_instrument_scope(config, df, trade_date)
    if beijing_gap:
        findings.append(beijing_gap)
    # A listing date is a fact the lake keeps; vendors are asked only about
    # names it cannot date yet. Re-deriving every date daily cost ~100s of
    # Baostock paging (2,000 rows per ~14s page) for answers already held.
    df = _carry_known_list_dates(config, df)
    ask_vendors = _undated_need_vendors(config, df, trade_date)
    stage(N_("fund listing dates"), held=df.height)
    df = _enrich_etf_list_dates_from_profiles(config, df, trade_date)
    if ask_vendors and config.ingest_listed_funds and config.sources.get("exchange", True):
        df, listing_findings = _enrich_fund_list_dates_from_exchange(config, df, trade_date, run_id)
        findings.extend(listing_findings)
    stage(N_("listing dates (Baostock, EastMoney)"), held=df.height)
    basics_source = _instrument_basics_once(config)
    if ask_vendors:
        df = enrich_instrument_list_dates(config, df, basics_source=basics_source)
        if basics_source.fetched or not config.sources.get("baostock", False):
            _remember_undated(config, df, trade_date)
    if backfill:
        stage(N_("delisted directory (Baostock)"), held=df.height)
        try:
            df = _merge_delisted_instruments(config, df, basics_source=basics_source)
        except Exception as exc:
            if error_kind(exc) not in SOURCE_LIMIT_REASONS:
                raise
            logger.warning("instruments: historical directory unavailable: %s", exc)
            findings.append(
                {
                    "dataset": "instruments",
                    "severity": "warning",
                    "check": "instruments_history_incomplete",
                    "message": str(exc),
                }
            )
    stage(N_("write"), held=df.height)
    df = _carry_lake_facts(config, df)
    result = write_simple(config, run_id, "instruments", df)
    if not backfill:
        findings.extend(_classify_new_delistings(config))
    new = new_instrument_rows(config, df)
    if new:
        result["context_updates"] = {"new_instruments": new}
    if findings:
        result.update(
            status="warning", coverage_status="partial", reason_code="source_scope_incomplete"
        )
        result.setdefault("context_updates", {})["audit_findings"] = findings
    return result


#: Codes a routine run may probe for the delisted directory. Normally a day
#: delists a handful at most; a larger backlog is the one-off sweep's job.
DAILY_DELISTED_DISCOVERY_LIMIT = 20


def _classify_new_delistings(config: Config) -> list[dict]:
    """File codes that stopped trading since the last sweep in the delisted directory.

    Without this a delisting reached the directory only when someone ran
    ``scripts/delisted_ops.py discover``, and until then the research-window
    survivorship check reported an unprobed code (2026-10-30: 301139.SZ). The
    probe is the sweep's own and resumable; a failed probe stays pending.
    """
    if config.tdx_allow_mock or not config.sources.get("sina", True):
        return []
    from cnequity.steps.delisted import discover_delisted

    try:
        result = discover_delisted(config, limit=DAILY_DELISTED_DISCOVERY_LIMIT)
    except Exception as exc:  # noqa: BLE001 — the directory is evidence, not a gate on instruments
        logger.warning("instruments: delisted discovery failed: %s", exc)
        return [
            {
                "dataset": "instruments",
                "severity": "warning",
                "check": "delisted_discovery_incomplete",
                "message": str(exc),
            }
        ]
    if result.failed or result.remaining:
        return [
            {
                "dataset": "instruments",
                "severity": "warning",
                "check": "delisted_discovery_incomplete",
                "message": f"{len(result.failed)} probe(s) failed, {result.remaining} code(s) left for the next run",
            }
        ]
    return []


def _enrich_fund_list_dates_from_exchange(
    config: Config, instruments: pl.DataFrame, on: date, run_id: str
) -> tuple[pl.DataFrame, list[dict]]:
    from cnequity.adapters.eastmoney.instruments import _fill_list_dates
    from cnequity.adapters.exchange.fund_listings import fetch_exchange_fund_listings
    from cnequity.storage.raw_archive import RawPayloadArchive

    missing = instruments.filter((pl.col("asset_type") == "etf") & pl.col("list_date").is_null())
    if missing.is_empty():
        return instruments, []
    exchanges = {symbol.rsplit(".", 1)[1] for symbol in missing["symbol"].to_list()}
    try:
        dates, wire = fetch_exchange_fund_listings(config, exchanges=exchanges)
        archive = RawPayloadArchive(
            config.meta_root,
            enabled=True,
            datasets=["instruments"],
            compression=config.raw_archive_compression,
            max_payload_bytes=config.raw_archive_max_payload_bytes,
        )
        for url, payload, params, kind in wire:
            archive.archive(
                "instruments",
                payload,
                source="exchange",
                run_id=run_id,
                request_params=params,
                url=url,
                payload_format=kind,
            )
    except Exception as exc:
        return instruments, [
            {
                "dataset": "instruments",
                "severity": "warning",
                "check": "fund_listing_dates_unavailable",
                "message": str(exc),
                "symbols": missing["symbol"].to_list(),
            }
        ]
    wanted = set(missing["symbol"].to_list())
    return _fill_list_dates(
        instruments,
        {symbol: listed for symbol, listed in dates.items() if symbol in wanted and listed <= on},
    ), []


def _enrich_etf_list_dates_from_profiles(
    config: Config, instruments: pl.DataFrame, on: date
) -> pl.DataFrame:
    """Fill missing ETF listing dates from a published exchange directory.

    A fund's first price must not create an obligation for sessions before its
    official listing. The market-wide instrument sources can omit that date,
    while ``etf_profiles`` records it with a replayable exchange response.
    Classification eligibility is separate: even an unverified ETF has a
    factual listing date. Never rewrite a date already supplied by instruments.
    """
    if not {"symbol", "asset_type", "list_date"} <= set(instruments.columns):
        return instruments
    from cnequity.query import load_with_receipt
    from cnequity.query.receipt import ReadReceiptError

    try:
        read = load_with_receipt("etf_profiles", config=config, end=on, require_replayable=True)
    except (ReadReceiptError, FileNotFoundError, ValueError):
        return instruments
    profiles = read.frame
    if not {"symbol", "as_of_date", "list_date"} <= set(profiles.columns):
        return instruments
    listings = (
        profiles.filter(
            (pl.col("as_of_date") <= on)
            & pl.col("list_date").is_not_null()
            & (pl.col("list_date") <= on)
        )
        .sort("as_of_date")
        .unique(subset=["symbol"], keep="last")
        .select("symbol", pl.col("list_date").alias("_exchange_list_date"))
    )
    if listings.is_empty():
        return instruments
    return (
        instruments.join(listings, on="symbol", how="left")
        .with_columns(
            pl.when((pl.col("asset_type") == "etf") & pl.col("list_date").is_null())
            .then(pl.col("_exchange_list_date"))
            .otherwise(pl.col("list_date"))
            .alias("list_date")
        )
        .drop("_exchange_list_date")
    )


def new_instrument_rows(config: Config, df: pl.DataFrame) -> list[dict]:
    """Today's instrument rows the curated table does not hold yet.

    Everything downstream reads its universe from *curated* instruments, and
    this step's rows only reach curated at the wave's compact. So a security
    listing today was invisible to every step of today's run: 920201.BJ listed
    on 2026-09-24 and got neither a bar nor a status row that day. The next
    day's incremental window starts after the watermark, so the first session
    was left to a later reconciliation pass to recover. Handing the new rows on
    through the run context closes that without reading stray staging
    fragments.
    """
    if df.is_empty() or "symbol" not in df.columns:
        return []
    curated = load_curated_instruments(config)
    known = set(curated["symbol"].to_list()) if curated is not None else set()
    columns = [c for c in ("symbol", "asset_type", "list_date", "delist_date") if c in df.columns]
    fresh = df.filter(~pl.col("symbol").is_in(sorted(known)) & pl.col("symbol").is_not_null())
    if "delist_date" in fresh.columns:
        fresh = fresh.filter(pl.col("delist_date").is_null())
    return fresh.select(columns).to_dicts()


def _with_new_instruments(symbols: list[str], context: dict) -> list[str]:
    """*symbols* plus the instruments this run discovered (see above)."""
    new = [row["symbol"] for row in context.get("new_instruments") or () if row.get("symbol")]
    return list(dict.fromkeys([*symbols, *new]))


def _require_beijing_instrument_scope(
    config: Config, frame: pl.DataFrame, trade_date: date
) -> dict | None:
    """Keep missing Beijing coverage explicit while allowing partial delivery."""
    if config.ingest_universe not in {"all_a", "all_instruments"} or trade_date < BSE_FIRST_SESSION:
        return
    if not config.sources.get("bse", True):
        return {
            "dataset": "instruments",
            "severity": "warning",
            "check": "instruments_beijing_coverage",
            "message": "[sources.bse] 已禁用；保留已取得的目录，北交所新上市身份仍待补。",
        }
    if "symbol" in frame.columns and frame["symbol"].str.ends_with(".BJ").any():
        return

    # A daily refresh may temporarily lose the board endpoint. Compaction keeps
    # existing instruments, so an established lake can safely carry its active
    # BJ rows forward; only a new/never-complete lake must fail here.
    existing = load_curated_instruments(config)
    if existing is not None and "symbol" in existing.columns:
        beijing = existing.filter(pl.col("symbol").str.ends_with(".BJ"))
        if "delist_date" in beijing.columns:
            beijing = beijing.filter(
                pl.col("delist_date").is_null() | (pl.col("delist_date") >= trade_date)
            )
        if not beijing.is_empty():
            logger.warning(
                "instruments: BSE snapshot unavailable; carrying %d existing active BJ row(s)",
                beijing.height,
            )
            return {
                "dataset": "instruments",
                "severity": "warning",
                "check": "instruments_beijing_coverage",
                "message": "BSE 当前目录不可用；复用已有 BJ 身份，不视作最新完整目录。",
            }

    return {
        "dataset": "instruments",
        "severity": "warning",
        "check": "instruments_beijing_coverage",
        "message": "未取得 active BJ 证券；交付已取得的沪深目录，北交所覆盖继续保留为缺口。",
    }


def _carry_lake_facts(config: Config, df: pl.DataFrame) -> pl.DataFrame:
    """Keep the facts this lake knows and no vendor reports.

    The catalogue is rebuilt from live sources every run, and none of them has
    ever heard of `prev_symbol` — it is written by the BJ code migration, from
    the lake's own old-code map. Compaction dedupes on `symbol` keeping the
    newest row, so the fresh null won, and 246 of the 248 recorded renames were
    erased on the next daily run: measured across the published revisions of
    2026-09-18, 248 -> 2 -> 248, the last step being a manual re-run of the
    migration. The rename lineage a survivorship-free universe depends on was
    therefore true only on the days somebody re-ran it by hand.

    Only fills nulls: a row that arrives carrying a rename is the authority on
    it.
    """
    if "symbol" not in df.columns:
        return df
    existing = load_curated_instruments(config)
    if existing is None or "prev_symbol" not in existing.columns:
        return df
    known = (
        existing.select("symbol", pl.col("prev_symbol").alias("_prev_known"))
        .filter(pl.col("_prev_known").is_not_null())
        .unique(subset=["symbol"], keep="last")
    )
    if known.is_empty():
        return df
    if "prev_symbol" not in df.columns:
        df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("prev_symbol"))
    carried = (
        df.join(known, on="symbol", how="left")
        .with_columns(
            pl.when(pl.col("prev_symbol").is_null())
            .then(pl.col("_prev_known"))
            .otherwise(pl.col("prev_symbol"))
            .alias("prev_symbol")
        )
        .drop("_prev_known")
    )
    filled = (
        carried.filter(pl.col("prev_symbol").is_not_null()).height
        - df.filter(pl.col("prev_symbol").is_not_null()).height
    )
    if filled:
        logger.info("instruments: carried %d recorded rename(s) forward", filled)
    return carried


def _merge_bse_instruments(config: Config, df: pl.DataFrame, trade_date: date) -> pl.DataFrame:
    """Add whatever the Beijing board is listing that the snapshot does not have.

    `_merge_untdxable_instruments` below keeps the *known* BJ names alive, but
    it can only replay the last code-space sweep, so it never discovers one.
    Between sweeps that is a hole in the universe, not a stale field: on
    2026-09-15 sixteen Beijing names were trading with no row in the lake at
    all. The exchange publishes its own board daily, for free, in a read the
    quote path already makes.

    Additive only, and best effort. A board that fails to load leaves the
    snapshot exactly as it was, which is what the sweep-replay underneath is
    for — this must not be able to turn a bad Beijing day into a day that
    inferred delistings.
    """
    if not config.sources.get("bse", True):
        return df
    from cnequity.adapters.bse.instruments import fetch_bse_instruments

    try:
        board = fetch_bse_instruments(trade_date, config=config)
    except Exception as exc:  # noqa: BLE001 — the sweep replay is the floor
        logger.warning("instruments: BSE board unavailable for %s: %s", trade_date, exc)
        return df
    if board.is_empty():
        return df

    known = set(df["symbol"].to_list()) if not df.is_empty() else set()
    fresh = board.filter(~pl.col("symbol").is_in(list(known)))
    if not fresh.is_empty():
        logger.info(
            "instruments: +%d Beijing listing(s) the snapshot had never seen (%s)",
            fresh.height,
            ", ".join(fresh["symbol"].to_list()[:3]),
        )
    # The names are worth carrying for the ones already known, too: these rows
    # came in from a code-space sweep with `name` null, and Beijing's ST
    # designation lives in 证券简称 and nowhere else this pipeline can reach.
    return pl.concat(
        [with_provenance(board, source="bse", data_version="v1"), df], how="diagonal_relaxed"
    )


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


def _instrument_basics_once(config: Config) -> Callable[[], pl.DataFrame]:
    """Baostock's instrument basics, queried at most once per step.

    Listing-date enrichment and the delisted merge both read it, and each
    asked baostock separately: ~90s apiece on a full-market init. A failure
    is not remembered, so the second reader still gets its own attempt.
    """
    held: dict[str, pl.DataFrame] = {}

    def basics() -> pl.DataFrame:
        if "frame" not in held:
            from cnequity.adapters.baostock import instruments as baostock_instruments

            held["frame"] = baostock_instruments.fetch_instrument_basics(config=config)
            basics.fetched = True
        return held["frame"]

    basics.fetched = False
    return basics


#: How long a "no vendor can date this" answer stands before vendors are
#: asked again. A new listing is never covered by it: it is asked daily until
#: some vendor dates it.
_UNDATED_RECHECK_DAYS = 7
_UNDATED_CHECKED_ON = "undated_checked_on"
_UNDATED_SYMBOLS = "undated_symbols"


def _carry_known_list_dates(config: Config, df: pl.DataFrame) -> pl.DataFrame:
    """Fill null list_dates from the lake's own catalogue.

    The live snapshots carry no listing date, so each run used to re-derive
    every date from vendors. A date the lake holds, including one an operator
    repaired, now stands; vendors fill only what is still unknown.
    """
    if "list_date" not in df.columns or "symbol" not in df.columns:
        return df
    existing = load_curated_instruments(config)
    if existing is None or "list_date" not in existing.columns:
        return df
    known = (
        existing.select("symbol", pl.col("list_date").alias("_known_list_date"))
        .filter(pl.col("_known_list_date").is_not_null())
        .unique(subset=["symbol"], keep="last")
    )
    if known.is_empty():
        return df
    return (
        df.join(known, on="symbol", how="left")
        .with_columns(pl.coalesce("list_date", "_known_list_date").alias("list_date"))
        .drop("_known_list_date")
    )


def _undated_symbols(df: pl.DataFrame) -> set[str]:
    if "list_date" not in df.columns:
        return set()
    return set(df.filter(pl.col("list_date").is_null())["symbol"].to_list())


def _undated_need_vendors(config: Config, df: pl.DataFrame, on: date) -> bool:
    """Whether any undated name is worth a vendor round today."""
    undated = _undated_symbols(df)
    if not undated:
        return False
    state = StateStore(config.meta_root)
    checked_on = state.get_date("instruments", _UNDATED_CHECKED_ON)
    if checked_on is None or not 0 <= (on - checked_on).days < _UNDATED_RECHECK_DAYS:
        return True
    return not undated <= state.get_string_set("instruments", _UNDATED_SYMBOLS)


def _remember_undated(config: Config, df: pl.DataFrame, on: date) -> None:
    """Record the names a full vendor round could not date."""
    state = StateStore(config.meta_root)
    state.set_string_set("instruments", _UNDATED_SYMBOLS, _undated_symbols(df))
    state.set_date("instruments", on, field=_UNDATED_CHECKED_ON)


def _merge_delisted_instruments(
    config: Config,
    df: pl.DataFrame,
    *,
    basics_source: Callable[[], pl.DataFrame] | None = None,
) -> pl.DataFrame:
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

    if basics_source is None:
        basics_source = _instrument_basics_once(config)
    basics = basics_source()
    if basics.is_empty():
        raise SourceUnavailableError(
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
    # outDate is the removal date; delist_date is the last listed session.
    from cnequity.storage.repairs.delist_dates import last_listed_sessions

    sessions = last_listed_sessions(
        config, dict(delisted.select("symbol", "delist_date").iter_rows())
    )
    delisted = delisted.with_columns(
        pl.col("symbol")
        .replace_strict(sessions, default=None, return_dtype=pl.Date)
        .fill_null(pl.col("delist_date"))
        .alias("delist_date")
    )
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
    # A year ahead, but never past the bundled holiday table. Beyond it no
    # weekday can be classified, and asking anyway failed this step — and so
    # phase 1 of every `cne init` and the daily calendar — from the first day
    # the year ahead crossed the table's end. Shrinking forward coverage is
    # the audit's `calendar_forward_coverage` warning to report, not a reason
    # to stop.
    end = min(trade_date + timedelta(days=365), max(calendar_seed_end(), trade_date))
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

    Which is why a missing 简称 is null and not ``False``. Reading "no name" as
    "not ST" turns the absence of the only available evidence into a clean bill
    of health: 253 retired Beijing codes carry no name at all, and every one of
    them was published as not-under-risk-warning on every session since it
    delisted.
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
    impossible = impossible_delistings(frame)
    return (
        frame.filter(pl.col("delist_date").is_not_null() & ~pl.col("symbol").is_in(impossible))
        .select(
            "symbol",
            "delist_date",
            name_col.map_elements(
                lambda value: is_st_name(value) if value else None, return_dtype=pl.Boolean
            ).alias("risk_warning"),
        )
        .unique(subset=["symbol"], keep="last")
    )


def _beijing_status(
    config: Config, frame: pl.DataFrame, day: date, day_symbols: list[str]
) -> pl.DataFrame:
    """Replace EastMoney's Beijing rows with the Beijing exchange's own.

    EastMoney does not serve this exchange on either column, and both gaps
    were being stored as facts:

    * The ST board's `fs` selects m:0 (Shenzhen) and m:1 (Shanghai) only, so
      every BJ name was published `risk_warning=False` — while the exchange
      listed *ST康乐, *ST田野 and *ST同辉. That column is null at the adapter
      now, and this is what fills it.
    * The suspension feed does not reach Beijing either. Measured over the
      lake's whole history: 15,515 EastMoney-sourced BJ rows, every one of
      them `is_trading=True`, against 116 halts in 148,933 SH/SZ rows. At
      the SH/SZ rate roughly a dozen BJ halts were stored as normal
      sessions.

    Not a source switch under ADR-0003: there is no competing answer to
    override, only two columns the primary leaves unserved by construction.
    Best effort in both directions — a board that fails, or a walk too
    incomplete to read an absence, leaves EastMoney's rows exactly as they
    were, and `risk_warning` stays null, which is what unknown means here.
    """
    if not config.sources.get("bse", True):
        return frame
    bj = served("bse_boards", day_symbols)
    if not bj or frame.is_empty():
        return frame
    from cnequity.adapters.bse.trading_status import fetch_trading_status_bse

    try:
        result = fetch_trading_status_bse(bj, day, config=config)
    except Exception as exc:  # noqa: BLE001 — EastMoney's row is the floor
        logger.warning("trading_status: BSE board unavailable for %s: %s", day, exc)
        return frame
    if result.is_empty:
        return frame

    rows = result.rows.with_columns(pl.lit("bse").alias("source"))
    judged = set(rows.get_column("symbol").to_list())
    logger.info(
        "trading_status: %d Beijing row(s) from the exchange's own board for %s (%d halted)",
        len(judged),
        day,
        int((~rows.get_column("is_trading")).sum()),
    )
    return pl.concat(
        [frame.filter(~pl.col("symbol").is_in(sorted(judged))), rows],
        how="diagonal_relaxed",
    )


def _drop_unlisted_codes(
    config: Config, df: pl.DataFrame, trade_date: date, context: dict
) -> tuple[pl.DataFrame, list[dict], object | None]:
    """Remove run-day rows for codes that have not started trading.

    The EastMoney feed is a suspension list, so every requested code off it is
    written ``normal`` — including a code TDX publishes before its listing
    (001246.SZ and 301716.SZ were stored as trading normally on 2026-09-24
    with no listing yet). The Beijing board does the mirror image and calls a
    complete walk's absentees halted. A code with no listing date and no bar
    anywhere in the lake is only kept when its own exchange's board lists it
    that session; a board that did not answer proves nothing, and its rows
    stay. Returns the frame, findings, and the SH/SZ board reading so the
    daily exchange snapshot can reuse it instead of asking twice.
    """
    if df.is_empty() or "symbol" not in df.columns:
        return df, [], None
    from cnequity.steps.common import instrument_metadata

    undated = set(
        instrument_metadata(config).filter(pl.col("list_date").is_null())["symbol"].to_list()
    )
    undated |= {
        row["symbol"]
        for row in context.get("new_instruments") or ()
        if row.get("symbol") and row.get("list_date") is None
    }
    today = set(df.filter(pl.col("trade_date") == trade_date)["symbol"].drop_nulls().to_list())
    candidates = undated & today
    if not candidates:
        return df, [], None
    candidates -= load_bar_universe(config)
    if not candidates:
        return df, [], None

    unlisted: set[str] = set()
    sh_sz_board = None
    sh_sz = set(served("exchange_boards", candidates))
    if sh_sz and config.sources.get("exchange", True):
        from cnequity.adapters.exchange.trading_status import fetch_trading_status_exchange

        try:
            sh_sz_board = fetch_trading_status_exchange(None, trade_date, config=config)
        except Exception as exc:  # noqa: BLE001 — no board, no verdict
            logger.warning(
                "trading_status: exchange boards unavailable for %s: %s", trade_date, exc
            )
        if sh_sz_board is not None and not sh_sz_board.is_empty:
            on_board = set(sh_sz_board.rows["symbol"].to_list())
            answered = {"SH"} if "sse" in sh_sz_board.covered else set()
            answered |= {"SZ"} if "szse" in sh_sz_board.covered else set()
            unlisted |= {s for s in sh_sz if s.rsplit(".", 1)[-1] in answered and s not in on_board}
    bj = candidates - sh_sz
    if bj and config.sources.get("bse", True):
        from cnequity.adapters.bse.trading_status import board_names

        try:
            listed, complete = board_names(trade_date, config=config)
        except Exception as exc:  # noqa: BLE001 — no board, no verdict
            logger.warning("trading_status: BSE board unavailable for %s: %s", trade_date, exc)
        else:
            if complete and listed:
                unlisted |= bj - set(listed)
    if not unlisted:
        return df, [], sh_sz_board
    kept = df.filter(
        ~(pl.col("symbol").is_in(sorted(unlisted)) & (pl.col("trade_date") == trade_date))
    )
    finding = {
        "dataset": "trading_status",
        "severity": "info",
        "check": "trading_status_unlisted_dropped",
        "message": (
            f"{len(unlisted)} code(s) with no listing date, no bar and no place on their "
            f"exchange's board for {trade_date} are not listed yet; no status row written"
        ),
        "symbols": sorted(unlisted),
    }
    return kept, [finding], sh_sz_board


def first_traded_dates(config: Config, symbols: list[str]) -> pl.DataFrame:
    """``symbol / first_traded``: each code's earliest traded ``daily_bars`` session."""
    schema = {"symbol": pl.Utf8, "first_traded": pl.Date}
    root = config.curated_root / "daily_bars"
    if not symbols:
        return pl.DataFrame(schema=schema)
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    if not dataset_has_parquet(root):
        return pl.DataFrame(schema=schema)
    return (
        scan_parquet_root(
            root,
            partition_col="trade_date",
            symbols=sorted(symbols),
            traded_only=True,
            dataset="daily_bars",
            meta_root=config.meta_root,
        )
        .group_by("symbol")
        .agg(pl.col("trade_date").min().alias("first_traded"))
        .collect()
        .cast(schema)
    )


def _drop_before_listing(config: Config, df: pl.DataFrame) -> tuple[pl.DataFrame, list[dict]]:
    """Remove rows stamped on sessions before a code's known listing date.

    :func:`_drop_unlisted_codes` covers codes with no listing date yet; this
    covers the ones TDX already dates, on every session a catch-up spans.
    """
    if df.is_empty() or "symbol" not in df.columns:
        return df, []
    from cnequity.steps.common import instrument_metadata

    first_day = df.get_column("trade_date").min()
    listings = instrument_metadata(config).filter(pl.col("list_date") > first_day)
    listings = listings.filter(pl.col("symbol").is_in(df.get_column("symbol").unique().to_list()))
    if listings.is_empty():
        return df, []
    listings = listings.join(
        first_traded_dates(config, listings["symbol"].to_list()), on="symbol", how="left"
    )
    kept, dropped = split_before_listing(df, listings)
    if dropped.is_empty():
        return df, []
    finding = {
        "dataset": "trading_status",
        "severity": "info",
        "check": "trading_status_before_listing_dropped",
        "message": (
            f"{dropped.height} row(s) for {dropped['symbol'].n_unique()} code(s) fall before "
            "the code's listing date; no status row written"
        ),
        "symbols": sorted(dropped["symbol"].unique().to_list()),
    }
    return kept, [finding]


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


@register_step("trading_status", group="core", depends_on=["instruments"])
def step_trading_status(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if context.get("_init"):
        # Index history shares this phase's backfill flag. Init status is a
        # current snapshot; explicit backfill retains the historical ST path.
        config = copy(config)
        config._backfill = False
    if getattr(config, "_backfill", False):
        return _backfill_trading_status_st(
            config,
            trade_date,
            run_id,
            batch_id=context.get("_batch_id"),
        )

    symbols = context.get("symbols") or _with_new_instruments(load_symbols(config), context)
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
    # Per session: (day, exchange boards, EastMoney, carried forward, dated
    # symbols left without a row, undated codes left without a row).
    coverage: list[tuple[date, int, int, int, int, int]] = []
    source_errors: list[str] = []
    board_failures: list[str] = []
    cached_snapshot: pl.DataFrame | None = None
    list_dates: dict[str, date | None] | None = None

    def _cached_status(day: date) -> pl.DataFrame | None:
        """Carry each symbol's last first-hand status forward, or None.

        trading_status is advisory for the daily signal gate. When EastMoney is
        temporarily unreachable, keeping known ST/halt labels is safer than
        turning every name into ``normal``. Only a first-hand reading from the
        last ``_CACHED_TRADING_STATUS_MAX_SESSIONS`` sessions is carried: a row
        this fallback wrote itself is not evidence, and chaining on it let one
        partial day propagate. A symbol with no such reading gets no row rather
        than a ``suspended`` one — that default published 4,863 trading names as
        halted after National Day 2026. A missing row is what makes the strict
        universe refuse the day.
        """
        nonlocal cached_snapshot
        sessions = list_trading_dates(
            config, day - _CACHED_TRADING_STATUS_SEARCH_SPAN, day - timedelta(days=1)
        )[-_CACHED_TRADING_STATUS_MAX_SESSIONS:]
        if not sessions:
            return None
        oldest = sessions[0]
        if cached_snapshot is None:
            try:
                from cnequity.query.reader import load

                # Bounded to the carry window; passing only ``end`` would
                # decode the entire historical status lake (17M+ rows in a
                # real full-market lake) during an outage.
                history = load(
                    "trading_status",
                    data_root=config.data_root,
                    start=oldest,
                    end=day - timedelta(days=1),
                )
            except Exception as exc:  # noqa: BLE001 — fallback is best effort
                logger.warning("cached trading_status unavailable: %s", exc)
                return None
            cached_snapshot = last_first_hand_status(
                history.drop("fetched_at", strict=False), with_source=True
            )

        evidence = cached_snapshot.filter(
            (pl.col("evidence_date") >= oldest) & pl.col("symbol").is_in(_live_symbols(day))
        )
        if evidence.is_empty():
            logger.warning(
                "no first-hand trading_status within %d session(s) before %s",
                _CACHED_TRADING_STATUS_MAX_SESSIONS,
                day,
            )
            return None
        origin = (
            pl.col("source").map_elements(cached_source, return_dtype=pl.Utf8)
            if "source" in evidence.columns
            else pl.lit(cached_source(None))
        )
        return evidence.select(
            "symbol",
            pl.lit(day).alias("trade_date"),
            "is_trading",
            "status",
            # An outage must not clear a risk-warning label. Unknown stays
            # unknown rather than becoming a claim of "clean".
            "risk_warning",
            # Named by the reading it repeats, so a carried exchange row is
            # not filed under EastMoney.
            origin.alias("source"),
        )

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
        #
        # A null `source` means "the step names this row"; a set one means the
        # row already knows its owner. Dropping the column unconditionally used
        # to be right, because the vendor frame was always one vendor's. It is
        # not any more: an EastMoney outage is served partly by the exchange
        # boards and partly by the cached snapshot, and erasing the label would
        # publish the exchange's reading under EastMoney's name.
        if "source" not in vendor.columns:
            vendor = with_columns_unless_blank(vendor, pl.lit(None, dtype=pl.Utf8).alias("source"))
        if gone.is_empty():
            return vendor
        if vendor.is_empty():
            return gone
        return pl.concat([vendor, gone], how="diagonal_relaxed")

    def _exchange_status(day: date, day_symbols: list[str]):
        """Halt/ST straight from the exchanges themselves, or None.

        All three boards, because all three encode it differently: Shanghai and
        Shenzhen list a halted security with its open/high/low at zero beside a
        reference close, while Beijing simply omits it — so the BSE reading
        needs its own completeness guard before an absence may be read as a
        halt.

        Best effort by construction: this is the fallback path, so a failure
        here must leave the cached snapshot underneath rather than replace one
        outage with two. Each board is attempted independently; one being down
        does not cost the others their reading.
        """
        frames: list[pl.DataFrame] = []
        sh_sz = served("exchange_boards", day_symbols)
        bj = served("bse_boards", day_symbols)

        if sh_sz and config.sources.get("exchange", True):
            from cnequity.adapters.exchange.trading_status import fetch_trading_status_exchange

            try:
                result = fetch_trading_status_exchange(sh_sz, day, config=config)
                board_failures.extend(
                    f"{day.isoformat()}: {name}: {why}" for name, why in result.failures.items()
                )
                if not result.is_empty:
                    frames.append(result.rows.with_columns(pl.lit("exchange").alias("source")))
            except Exception as exc:  # noqa: BLE001
                logger.warning("trading_status: SH/SZ boards unavailable for %s: %s", day, exc)
                board_failures.append(f"{day.isoformat()}: SH/SZ boards: {exc}")

        # The equity boards above carry no ETF or LOF; EastMoney was the only
        # feed that did, so without this every fund goes unanswered on an outage.
        funds = [
            sym for sym in sh_sz if is_etf_symbol(sym.rsplit(".", 1)[0], sym.rsplit(".", 1)[-1])
        ]
        if funds and config.sources.get("exchange", True):
            from cnequity.adapters.exchange.trading_status import (
                fetch_fund_trading_status_exchange,
            )

            try:
                result = fetch_fund_trading_status_exchange(funds, day, config=config)
                board_failures.extend(
                    f"{day.isoformat()}: {name}: {why}" for name, why in result.failures.items()
                )
                if not result.is_empty:
                    frames.append(result.rows.with_columns(pl.lit("exchange").alias("source")))
            except Exception as exc:  # noqa: BLE001
                logger.warning("trading_status: fund boards unavailable for %s: %s", day, exc)
                board_failures.append(f"{day.isoformat()}: fund boards: {exc}")

        if bj and config.sources.get("bse", True):
            from cnequity.adapters.bse.trading_status import fetch_trading_status_bse

            try:
                result = fetch_trading_status_bse(bj, day, config=config)
                # An incomplete walk still yields the names it did see; it just
                # leaves the absent ones to the cached snapshot instead of
                # calling the whole exchange suspended.
                if not result.is_empty:
                    frames.append(result.rows.with_columns(pl.lit("bse").alias("source")))
            except Exception as exc:  # noqa: BLE001
                logger.warning("trading_status: BSE board unavailable for %s: %s", day, exc)
                board_failures.append(f"{day.isoformat()}: BSE board: {exc}")

        if not frames:
            return None
        # Name the owner per row: the day mixes these with the cached snapshot,
        # and `with_provenance` keeps a source a frame already carries.
        return pl.concat(frames, how="diagonal_relaxed")

    def _fetch_vendor(day: date):
        """The exchanges' own boards first, EastMoney for what they leave, then the cache.

        The boards publish every listed SH/SZ/BJ security, so they answer
        nearly the whole market; EastMoney is asked only for the remainder and
        is skipped outright while push2 is paused. Each row names its owner.
        """
        nonlocal cached_snapshot, list_dates
        day_symbols = _live_symbols(day)
        if not day_symbols:
            # Every name in the universe had already delisted by this session.
            # Asking the boards about nothing is a wasted request, not a state
            # the adapters are expected to handle.
            return pl.DataFrame()
        expected = set(day_symbols)
        parts: list[pl.DataFrame] = []
        exchange_rows = _exchange_status(day, day_symbols)
        if exchange_rows is not None and not exchange_rows.is_empty():
            parts.append(exchange_rows)
        covered = set(exchange_rows.get_column("symbol").to_list()) if parts else set()
        boards = len(covered)

        vendor = 0
        remaining = sorted(expected - covered)
        if remaining:
            try:
                frame = fetch_trading_status(
                    remaining,
                    day,
                    rate_limit=rl,
                    allow_mock=config.tdx_allow_mock,
                    config=config,
                )
                if frame.is_empty() or "symbol" not in frame.columns:
                    raise SourceUnavailableError("trading_status: no rows returned")
                missing = set(remaining) - set(frame.get_column("symbol").drop_nulls().to_list())
                if missing:
                    # EastMoney answers every code it is asked about; a short
                    # reply is a broken payload, not a set of readings to keep.
                    raise RuntimeError(
                        "trading_status: incomplete daily snapshot; "
                        f"missing {len(missing)} requested symbol(s)"
                    )
                frame = _beijing_status(config, frame, day, remaining).filter(
                    pl.col("symbol").is_in(remaining)
                )
                if "source" not in frame.columns:
                    frame = frame.with_columns(pl.lit("eastmoney").alias("source"))
                parts.append(frame)
                vendor = frame.height
                covered |= set(frame.get_column("symbol").to_list())
            except Exception as exc:  # noqa: BLE001 — EastMoney is the backup here
                source_errors.append(f"{day.isoformat()}: {exc}")

        first_hand = (
            pl.concat(parts, how="diagonal_relaxed").with_columns(pl.lit(day).alias("trade_date"))
            if parts
            else None
        )
        carried = 0
        remaining = sorted(expected - covered)
        if remaining:
            cached = _cached_status(day)
            if cached is not None:
                remainder = cached.filter(pl.col("symbol").is_in(remaining))
                carried = remainder.height
                if carried:
                    parts.append(remainder)
        if first_hand is not None:
            today = last_first_hand_status(first_hand, with_source=True)
            cached_snapshot = (
                today
                if cached_snapshot is None
                else pl.concat(
                    [
                        cached_snapshot.filter(~pl.col("symbol").is_in(today["symbol"].to_list())),
                        today.select(cached_snapshot.columns),
                    ],
                    how="vertical_relaxed",
                )
            )
        if not parts:
            raise SourceUnavailableError(
                f"trading_status: no source answered for {day}; " + "; ".join(source_errors[-1:])
            )
        out = parts[0] if len(parts) == 1 else pl.concat(parts, how="diagonal_relaxed")
        unanswered = expected - set(out.get_column("symbol").to_list())
        if unanswered and list_dates is None:
            from cnequity.steps.common import instrument_metadata

            meta = instrument_metadata(config)
            list_dates = dict(zip(meta["symbol"], meta["list_date"], strict=True))
        # An undated code that no board lists is a placeholder or not listed
        # yet (89 of the 97 on 2026-10-27), not a security missing its status.
        # A code instruments does not know at all is counted as listed.
        undated = sum(1 for sym in unanswered if sym in list_dates and list_dates[sym] is None)
        coverage.append((day, boards, vendor, carried, len(unanswered) - undated, undated))
        if carried or len(unanswered) > undated:
            logger.warning(
                "trading_status %s: %d from the exchange boards, %d from EastMoney, %d carried "
                "forward, %d listed symbol(s) without a row",
                day,
                boards,
                vendor,
                carried,
                len(unanswered) - undated,
            )
        return out

    df, _findings = fetch_incremental_daily(
        config,
        "trading_status",
        trade_date,
        _fetch,
        allow_empty=False,
        snapshot_only=bool(context.get("_init")),
    )
    # Two requests and about three seconds, once per session, whatever the
    # vendor path did. The exchange reading is only reachable today when
    # EastMoney fails, so a normal day leaves no exchange-grade record of SH/SZ
    # status — and the ST evidence receipt admits a board precisely because a
    # board is not an aggregator. Snapshot only: authority is unchanged.
    df, unlisted_findings, sh_sz_board = _drop_unlisted_codes(config, df, trade_date, context)
    df, early_findings = _drop_before_listing(config, df)
    unlisted_findings = [*unlisted_findings, *early_findings]
    try:
        captured = snapshot_trading_status_exchange(
            config,
            trade_date=trade_date,
            symbols=_live_symbols(trade_date),
            run_id=run_id,
            prefetched=sh_sz_board,
        )
        if captured:
            logger.info("trading_status: snapshotted %d exchange board row(s)", captured)
    except Exception as exc:  # noqa: BLE001 — a record for later, never this run's verdict
        logger.warning("trading_status: exchange board snapshot unavailable: %s", exc)
    if df.is_empty():
        result = {"rows_read": 0, "rows_written": 0}
        if _findings:
            result["context_updates"] = {"audit_findings": _findings}
        return result
    findings = [*_findings, *unlisted_findings]
    unanswered = sum(row[4] for row in coverage)
    carried_total = sum(row[3] for row in coverage)
    degraded = bool(board_failures) or unanswered > 0
    undated_total = sum(row[5] for row in coverage)
    if coverage and (carried_total or unanswered or undated_total or board_failures):
        findings.append(
            {
                "dataset": "trading_status",
                "severity": "warning" if degraded else "info",
                "check": "trading_status_cached_fallback",
                "message": (
                    f"{carried_total} symbol-day(s) carried forward from an earlier reading; "
                    f"{unanswered} listed symbol-day(s) have no status row; "
                    f"{undated_total} undated code-day(s) no board lists"
                    + (
                        f"; {len(board_failures)} exchange board(s) failed"
                        if board_failures
                        else ""
                    )
                ),
                "coverage": [
                    {
                        "trade_date": day.isoformat(),
                        "exchange_boards": boards,
                        "eastmoney": vendor,
                        "carried_forward": carried,
                        "without_row": missing,
                        "undated_without_row": undated,
                    }
                    for day, boards, vendor, carried, missing, undated in coverage
                ],
                "errors": (board_failures + source_errors)[:3],
            }
        )
    # Every part above names its owner; a row that does not is EastMoney's,
    # and a delisting keeps the label `instruments` gave it.
    if "source" in df.columns:
        df = df.with_columns(pl.col("source").fill_null("eastmoney"))
    df = with_provenance(df, source="eastmoney", data_version="v1")
    result = write_simple(config, run_id, "trading_status", df)
    if degraded:
        result["status"] = "warning"
        if unanswered:
            result["coverage_status"] = "partial"
    if findings:
        result["context_updates"] = {"audit_findings": findings}
    return result


# How far back an unbounded derive re-examines sessions for interior bar gaps
# — the tail an incremental `cne backfill daily_bars` re-derives. A halt only
# becomes visible here once the symbol trades again, so the window has to
# outlive an ordinary suspension rather than just cover the last few sessions.
# The daily core does not run this step at all: the `trading_status` snapshot
# records each session's halts, and only a change to bar history can reveal
# one nobody snapshotted. Anything older is closed by an explicit rebuild
# (`cne derive trading_status`, which walks the full history).
DERIVE_TAIL_DAYS = 90


@register_step(
    "trading_status_derive",
    group="core",
    depends_on=["daily_bars"],
    input_datasets=("daily_bars", "instruments", "trading_calendar"),
)
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
    from cnequity.storage.state import StateStore

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
    gaps = StateStore(config.meta_root).get_payload("daily_bars")
    partial = any(gaps.get(key) for key in ("missing_ranges", "missing_units", "outstanding_keys"))
    return {
        "rows_read": rows,
        "rows_written": rows,
        "usable_result": True,
        "coverage_status": "partial" if partial else "unknown",
        "status": "warning" if partial else "success",
        "reason_code": "input_coverage_limited" if partial else None,
    }


def _is_all_a(symbol: str) -> bool:
    try:
        info = parse_symbol(symbol)
    except ValueError:
        return False
    return is_all_a_symbol(info.code, info.exchange) and not is_cdr_symbol(info.code, info.exchange)


def _resolve_explicit_st_symbols(config: Config, raw: list[str]) -> list[str]:
    instruments: set[str] | None = None
    resolved: list[str] = []
    for value in raw:
        symbol = str(value).strip().upper()
        if (
            "." in symbol
            and _is_all_a(symbol)
            and symbol.rsplit(".", 1)[1] in ST_EVIDENCE_UNSUPPORTED_EXCHANGES
        ):
            resolved.append(symbol)
            continue
        if instruments is None:
            instruments = set(load_symbols(config))
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
            "usable_result": True,
        }

    # Bounded per run so a fresh `cne init` is not held for ten hours behind a
    # free API's pacing. What is left is neither done nor failed — it simply has
    # not had its turn — and the checkpoint already resumes from exactly here.
    budget = int(getattr(config, "st_history_symbols_per_run", 0) or 0)
    deferred: list[str] = []
    if budget > 0 and len(todo) > budget:
        deferred = todo[budget:]
        todo = todo[:budget]

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
        try:
            df, failed = fetch_st_history(batch, start, end, **kwargs)
        except SourceCoolingDown as exc:
            logger.warning(
                "ST backfill stopped: %s; completed symbols stay checkpointed",
                exc,
            )
            unresolved.update(todo[offset:])
            break
        if not df.is_empty():
            chunk = write_fetched(
                config,
                run_id,
                "trading_status",
                df,
                source=source,
                batch_id=f"{source}-batch-{offset:05d}",
                preserve_fetched_at=source == "tushare",
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
    complete = completed == set(universe) and not unresolved and not deferred
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
        "usable_result": bool(completed) or rows_written > 0,
        "coverage_status": "complete" if complete else "partial",
    }
    if complete:
        result["coverage_pending_compact"] = True
    else:
        result["status"] = "warning"
        result["failed_symbols"] = len(unresolved)
        result["reason_code"] = "scope_not_swept" if deferred else "source_unavailable"
        # Deferred and unresolved mean different things and must not be summed:
        # one is a symbol this run did not reach, the other one the source would
        # not answer for. Reporting "N unresolved" for a paused sweep would send
        # an operator hunting a vendor outage that is not happening.
        parts = []
        if deferred:
            result["deferred_symbols"] = len(deferred)
            parts.append(f"{len(deferred)} not yet swept (paused at {budget} symbols/run)")
        if unresolved:
            parts.append(f"{len(unresolved)} unresolved by {source}")
        finding = {
            "dataset": "trading_status",
            "severity": "warning",
            "check": f"{source}_st_backfill_incomplete",
            "message": (
                f"{len(completed)}/{len(universe)} symbols have {source} ST evidence: "
                + "; ".join(parts)
                + ". Resume with `cne backfill trading_status` — the checkpoint "
                "continues from here, and this run's rows are already staged."
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
        if unsupported_symbols:
            return {
                "status": "warning",
                "rows_read": 0,
                "rows_written": 0,
                "coverage_status": "partial",
                "reason_code": "capability_limit",
                "unsupported_symbols": len(unsupported_symbols),
                "unsupported_exchanges": sorted(ST_EVIDENCE_UNSUPPORTED_EXCHANGES),
                "completed_symbols": 0,
                "expected_symbols": len(universe),
            }
        raise SourceUnavailableError(
            "trading_status ST backfill has no configured historical source"
        )
    result: dict = {
        "rows_read": sum(int(item.get("rows_read", 0)) for item in results),
        "rows_written": sum(int(item.get("rows_written", 0)) for item in results),
        "completed_symbols": sum(int(item.get("completed_symbols", 0)) for item in results),
        "expected_symbols": sum(int(item.get("expected_symbols", 0)) for item in results),
        "source_results": results,
        "usable_result": any(result_is_usable(item) for item in results),
    }
    if unsupported_symbols:
        result["unsupported_symbols"] = len(unsupported_symbols)
        result["unsupported_exchanges"] = sorted(ST_EVIDENCE_UNSUPPORTED_EXCHANGES)
        result["coverage_status"] = "partial"
        result["reason_code"] = "capability_limit"
        result["status"] = "warning"
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
