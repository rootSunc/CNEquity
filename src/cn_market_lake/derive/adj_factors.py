from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import httpx
import polars as pl

from cn_market_lake.adapters.sina.adj_factors import fetch_adj_factor_series
from cn_market_lake.config import Config
from cn_market_lake.domain.rate_limit import wait_source
from cn_market_lake.domain.schemas import with_provenance
from cn_market_lake.domain.symbols import is_cdr_symbol, parse_symbol
from cn_market_lake.file_lock import lake_mutation_lock
from cn_market_lake.storage.atomic import write_parquet_atomic

logger = logging.getLogger(__name__)

# Only hfq is persisted; qfq is derived at query time (ADR-0004).
STORED_ADJUST_TYPE = "hfq"

# Derive step fails when uncached fetch failures exceed this share of symbol×type tasks.
FAIL_RATIO_THRESHOLD = 0.05

# A single trading day's hfq factor step beyond this ratio cannot come from any real
# corporate action — even the largest historical splits step the factor by ~10x. Such a
# jump signals a factor break (a dropped/misaligned event date or a corrupt source series),
# so we surface it as a fail-loud finding. This is a coarse tripwire for gross corruption;
# it deliberately does not try to catch ~2x breaks, which are indistinguishable from a real
# 10-for-10 bonus at the factor level alone and are guarded downstream via raw-vs-adjusted
# return divergence.
MAX_FACTOR_STEP_RATIO = 20.0

# Symbols whose missing history one incremental run will realign. A daily run
# normally finds none; this bounds the first run after a deep `cml backfill
# daily_bars`, which would otherwise realign the whole market in one go.
UNCOVERED_REFRESH_LIMIT = 500

_EMPTY_BAR_DATES = pl.DataFrame(schema={"symbol": pl.Utf8, "trade_date": pl.Date})
_ADJ_PK = ["symbol", "trade_date", "adjust_type"]


class AdjFactorsFetchError(RuntimeError):
    """Raised when adj factor fetch fails and no cache is available."""


class AdjFactorsDeriveError(RuntimeError):
    """Raised when too many symbols lack adj factors after derive."""

    def __init__(self, message: str, *, findings: list[dict]):
        super().__init__(message)
        self.findings = findings


class AdjFactorsResult:
    __slots__ = ("rows", "task_count", "failed", "findings")

    def __init__(
        self,
        rows: int,
        task_count: int,
        failed: list[str],
        findings: list[dict],
    ) -> None:
        self.rows = rows
        self.task_count = task_count
        self.failed = failed
        self.findings = findings

    @property
    def fail_ratio(self) -> float:
        if not self.task_count:
            return 0.0
        return len(self.failed) / self.task_count


def _is_cdr(symbol: str) -> bool:
    try:
        info = parse_symbol(symbol)
    except ValueError:
        return False
    return is_cdr_symbol(info.code, info.exchange)


def _adj_factors_watermark(config: Config) -> date | None:
    """Latest trade_date partition already present under derived/adj_factors."""
    # Lazy import: query.reader imports STORED_ADJUST_TYPE from this module.
    from cn_market_lake.query.parquet_scan import list_hive_partition_dates

    dates = list_hive_partition_dates(config.derived_root / "adj_factors", "trade_date")
    return dates[-1] if dates else None


def _load_daily_bar_dates(
    config: Config,
    *,
    start: date | None = None,
    symbols: list[str] | None = None,
) -> pl.DataFrame:
    """Load symbol×trade_date pairs from daily_bars (lazy hive scan when possible)."""
    # Lazy import: query.reader imports STORED_ADJUST_TYPE from this module.
    from cn_market_lake.query.parquet_scan import collect_parquet_root

    bars_path = config.curated_root / "daily_bars"
    try:
        bars = collect_parquet_root(
            bars_path,
            partition_col="trade_date",
            start=start,
            symbols=symbols,
        )
    except FileNotFoundError:
        return _EMPTY_BAR_DATES.clone()
    if bars.is_empty() or not {"symbol", "trade_date"}.issubset(bars.columns):
        return _EMPTY_BAR_DATES.clone()
    return bars.select(["symbol", "trade_date"]).unique().sort(["symbol", "trade_date"])


def _bars_for_derive(
    config: Config,
    *,
    watermark: date | None,
    refresh_set: set[str],
    full: bool,
) -> pl.DataFrame:
    """Select bar dates needed for this derive run (append-only when possible)."""
    if full or watermark is None:
        return _load_daily_bar_dates(config)

    frames: list[pl.DataFrame] = []
    # New trading days since the last derived partition.
    incremental = _load_daily_bar_dates(config, start=watermark).filter(
        pl.col("trade_date") > watermark
    )
    if not incremental.is_empty():
        frames.append(incremental)
    # Ex-date / new-listing / explicit rebackfill: realign full history for those symbols.
    if refresh_set:
        refreshed = _load_daily_bar_dates(config, symbols=sorted(refresh_set))
        if not refreshed.is_empty():
            frames.append(refreshed)
    if not frames:
        return _EMPTY_BAR_DATES.clone()
    return (
        pl.concat(frames, how="diagonal_relaxed")
        .unique(subset=["symbol", "trade_date"], keep="last")
        .sort(["symbol", "trade_date"])
    )


def _align_factors_to_bars(
    sym_bars: pl.DataFrame,
    symbol: str,
    factors: pl.DataFrame,
    adjust_type: str,
) -> pl.DataFrame:
    sym_dates = sym_bars.select("trade_date").sort("trade_date")
    if sym_dates.is_empty():
        return pl.DataFrame()

    # Sina emits a sparse step function: one row per corporate-action date, with the
    # factor level that applies from that date forward. The factor on any trading day is
    # therefore the most recent event on or before it — an as-of (backward) join, not an
    # exact-date join. An exact join drops every event date that isn't itself a bar date
    # (e.g. all pre-history events when bars start long after IPO), leaving leading bars to
    # default to 1.0 and turning the first in-window event into a spurious >1000x jump.
    factors_sorted = factors.select(["trade_date", "factor"]).sort("trade_date")
    aligned = sym_dates.join_asof(factors_sorted, on="trade_date", strategy="backward")
    aligned = aligned.with_columns(pl.col("factor").fill_null(1.0))
    return aligned.with_columns(
        pl.lit(symbol).alias("symbol"),
        pl.lit(adjust_type).alias("adjust_type"),
    )


def _cache_path(config: Config, symbol: str, adjust_type: str) -> Path:
    cache_dir = config.meta_root / "adj_factors_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    safe = symbol.replace(".", "_")
    return cache_dir / f"{safe}_{adjust_type}.parquet"


def _load_cache(config: Config, symbol: str, adjust_type: str) -> pl.DataFrame | None:
    path = _cache_path(config, symbol, adjust_type)
    if not path.exists():
        return None
    return pl.read_parquet(path).select(["trade_date", "factor"])


def _save_cache(config: Config, symbol: str, adjust_type: str, factors: pl.DataFrame) -> None:
    if factors.is_empty():
        return
    path = _cache_path(config, symbol, adjust_type)
    factors.write_parquet(path, compression="zstd")


def _read_parquet_files(files: list[Path]) -> pl.DataFrame:
    if not files:
        return pl.DataFrame()
    return pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")


def _corporate_action_symbols_on(config: Config, trade_date: date) -> set[str]:
    root = config.curated_root / "corporate_actions"
    if not root.exists():
        return set()

    part_files = list((root / f"ex_date={trade_date.isoformat()}").glob("**/*.parquet"))
    files = part_files or list(root.glob("**/*.parquet"))
    df = _read_parquet_files(files)
    if df.is_empty() or not {"symbol", "ex_date"}.issubset(df.columns):
        return set()
    today = df.filter(pl.col("ex_date") == trade_date)
    return set(today["symbol"].unique().to_list())


def _new_listing_symbols_on(config: Config, trade_date: date) -> set[str]:
    root = config.curated_root / "instruments"
    if not root.exists():
        return set()

    df = _read_parquet_files(list(root.glob("**/*.parquet")))
    if df.is_empty() or not {"symbol", "list_date"}.issubset(df.columns):
        return set()
    listed = df.filter(pl.col("list_date") == trade_date)
    return set(listed["symbol"].unique().to_list())


def _uncovered_symbols(config: Config) -> set[str]:
    """Symbols with bars the factor table does not reach.

    The derive is append-only from its watermark, which handles new sessions and
    misses everything else. `cml backfill daily_bars` adds *old* dates, and old
    dates are behind the watermark by definition, so the history it lands never
    gets a factor — silently, because `load(adjust=…)` fills factor=1.0 and only
    marks `adj_is_exact`. Measured on a real lake: 260 stocks with no factor at
    all and ~220k unadjusted bar rows, which read as "Sina does not cover
    北交所" until a targeted re-derive filled three of them going back to 2016.

    Compared per symbol rather than per row: a (symbol, trade_date) anti-join
    against a 338M-row daily_bars on every run would cost more than the derive.
    Min/max per symbol catches both a symbol with no factors and one whose
    coverage stops short, and a refreshed symbol realigns its whole history
    anyway.
    """
    from cn_market_lake.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    bars_root = config.curated_root / "daily_bars"
    if not dataset_has_parquet(bars_root):
        return set()
    bar_span = (
        scan_parquet_root(bars_root)
        .group_by("symbol")
        .agg(
            pl.col("trade_date").min().alias("bar_first"),
            pl.col("trade_date").max().alias("bar_last"),
        )
        .collect()
    )
    if bar_span.is_empty():
        return set()

    fac_root = config.derived_root / "adj_factors"
    if not dataset_has_parquet(fac_root):
        return set(bar_span["symbol"].to_list())
    fac_span = (
        scan_parquet_root(fac_root)
        .group_by("symbol")
        .agg(
            pl.col("trade_date").min().alias("fac_first"),
            pl.col("trade_date").max().alias("fac_last"),
        )
        .collect()
    )
    # Only the *backward* direction. `fac_last < bar_last` is true on every
    # ordinary run — today's bar lands before its factor is derived — so
    # including it would force a full-history realign of the entire market
    # daily. New sessions are precisely what the incremental path is for.
    joined = bar_span.join(fac_span, on="symbol", how="left")
    uncovered = joined.filter(
        pl.col("fac_first").is_null() | (pl.col("fac_first") > pl.col("bar_first"))
    )
    # Only stocks. ETFs and LOFs have no hfq factor series to fetch — 91 of the
    # 103 names left after the first self-heal run were ETFs, and without this
    # they would be re-fetched on every run forever. CDRs go for the same
    # reason: the task loop already drops them, so they can never be covered.
    candidates = {s for s in uncovered["symbol"].to_list() if not _is_cdr(s)}
    inst_root = config.curated_root / "instruments"
    if dataset_has_parquet(inst_root):
        instruments = scan_parquet_root(inst_root).collect()
        if "asset_type" in instruments.columns:
            stocks = set(instruments.filter(pl.col("asset_type") == "stock")["symbol"].to_list())
            if stocks:
                candidates &= stocks
    return candidates


def _event_refresh_symbols(config: Config, trade_date: date) -> set[str]:
    """Symbols whose factor cache should be refreshed for this trading date."""
    return _corporate_action_symbols_on(config, trade_date) | _new_listing_symbols_on(
        config, trade_date
    )


def _needs_refresh(
    cached: pl.DataFrame | None,
    force: bool,
) -> bool:
    if force:
        return True
    if cached is None or cached.is_empty():
        return True
    return False


def _resolve_factors(
    config: Config,
    symbol: str,
    adjust_type: str,
    sym_bars: pl.DataFrame,
    *,
    force: bool,
    client: httpx.Client,
) -> pl.DataFrame | None:
    cached = _load_cache(config, symbol, adjust_type)
    if not _needs_refresh(cached, force):
        return cached

    source = config.adj_factors_source
    interval = config.source_intervals.get(source, 0.2)
    rate_state_dir = config.meta_root / "rate_limits"
    try:
        wait_source(rate_state_dir, source, interval)
        if source != "sina":
            logger.warning("Unknown adj_factors source %s; skipping %s", source, symbol)
            return cached
        factors = fetch_adj_factor_series(symbol, adjust_type, client=client)
        _save_cache(config, symbol, adjust_type, factors)
        return factors
    except Exception as exc:
        if cached is None or cached.is_empty():
            raise AdjFactorsFetchError(
                f"No cached adj factors for {symbol} ({adjust_type}): {exc}"
            ) from exc
        logger.warning("External adj factors failed for %s (%s): %s", symbol, adjust_type, exc)
        return cached


def _factor_continuity_findings(out: pl.DataFrame) -> list[dict]:
    """Flag symbols whose stored factor jumps beyond any plausible corporate action.

    hfq factors are a step function that moves only on ex-dates by a bounded ratio; a
    day-over-day jump above MAX_FACTOR_STEP_RATIO (or a symmetric collapse) means the
    aligned series is broken. Emits one finding per offending symbol at its worst step.
    """
    if out.is_empty() or out.height < 2:
        return []
    ratios = (
        out.sort(["symbol", "adjust_type", "trade_date"])
        .with_columns(
            (pl.col("factor") / pl.col("factor").shift(1))
            .over(["symbol", "adjust_type"])
            .alias("_step_ratio")
        )
        .filter(pl.col("_step_ratio").is_not_null() & (pl.col("factor") > 0))
        .filter(
            (pl.col("_step_ratio") > MAX_FACTOR_STEP_RATIO)
            | (pl.col("_step_ratio") < 1.0 / MAX_FACTOR_STEP_RATIO)
        )
    )
    if ratios.is_empty():
        return []

    ratios = ratios.with_columns(
        pl.max_horizontal(pl.col("_step_ratio"), (1.0 / pl.col("_step_ratio"))).alias("_severity")
    )
    worst = (
        ratios.sort("_severity", descending=True)
        .group_by(["symbol", "adjust_type"], maintain_order=True)
        .first()
    )
    findings: list[dict] = []
    for row in worst.iter_rows(named=True):
        td = row["trade_date"]
        td_str = td.isoformat() if isinstance(td, date) else str(td)
        findings.append(
            {
                "dataset": "adj_factors",
                "severity": "error",
                "check": "adj_factor_continuity",
                "message": (
                    f"{row['symbol']} ({row['adjust_type']}) factor jumps "
                    f"{row['_step_ratio']:.1f}x on {td_str} to {row['factor']:.4g} "
                    f"(>{MAX_FACTOR_STEP_RATIO:.0f}x); likely a factor break, not a "
                    f"corporate action"
                ),
                "symbol": row["symbol"],
                "adjust_type": row["adjust_type"],
                "trade_date": td_str,
            }
        )
    return findings


def _fetch_failure_finding(symbol: str, adjust_type: str, exc: Exception) -> dict:
    return {
        "dataset": "adj_factors",
        "severity": "error",
        "check": "adj_factor_fetch_failed",
        "message": f"No cached adj factors for {symbol} ({adjust_type}): {exc}",
        "symbol": symbol,
        "adjust_type": adjust_type,
    }


def _process_symbol_adj(
    config: Config,
    sym: str,
    adj: str,
    sym_bars: pl.DataFrame,
    *,
    force: bool,
    client: httpx.Client | None = None,
) -> tuple[pl.DataFrame | None, str | None, dict | None]:
    own_client = client is None
    if own_client:
        client = httpx.Client(timeout=20.0)
    try:
        try:
            factors = _resolve_factors(config, sym, adj, sym_bars, force=force, client=client)
        except AdjFactorsFetchError as exc:
            return None, f"{sym}:{adj}", _fetch_failure_finding(sym, adj, exc)
        if factors is None or factors.is_empty():
            return None, None, None
        aligned = _align_factors_to_bars(sym_bars, sym, factors, adj)
        if aligned.is_empty():
            return None, None, None
        return aligned, None, None
    finally:
        if own_client:
            client.close()


def _write_adj_partitions(
    config: Config,
    out: pl.DataFrame,
    *,
    replace: bool,
) -> int:
    """Persist aligned factors. Append-only merges into existing partitions unless *replace*."""
    total = 0
    for key, group in out.partition_by("trade_date", as_dict=True).items():
        td = key[0] if isinstance(key, tuple) else key
        td_str = td.isoformat() if isinstance(td, date) else str(td)
        out_dir = config.derived_root / "adj_factors" / f"trade_date={td_str}"
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / "part-0.parquet"
        if replace or not path.exists():
            write_parquet_atomic(path, group, compression="zstd")
        else:
            existing = pl.read_parquet(path)
            merged = pl.concat([existing, group], how="diagonal_relaxed").unique(
                subset=_ADJ_PK, keep="last"
            )
            write_parquet_atomic(path, merged, compression="zstd")
        total += group.height
    return total


def compute_adj_factors(
    config: Config,
    adjust_type: str | None = None,
    *,
    refresh_symbols: list[str] | None = None,
    full: bool = False,
) -> AdjFactorsResult:
    """Derive hfq adj_factors (ADR-0004).

    Default path is append-only: only new trade_date partitions since the derived
    watermark are written, plus full-history merge for ex-date / new-listing /
    explicit refresh symbols. Pass ``full=True`` to rewrite every partition.
    """
    # The derive reads existing partitions and then merges/replaces them.  It
    # must not overlap compact, repartition, or another derive invocation.
    with lake_mutation_lock(config.meta_root, blocking=True):
        return _compute_adj_factors_locked(
            config,
            adjust_type,
            refresh_symbols=refresh_symbols,
            full=full,
        )


def _compute_adj_factors_locked(
    config: Config,
    adjust_type: str | None = None,
    *,
    refresh_symbols: list[str] | None = None,
    full: bool = False,
) -> AdjFactorsResult:
    """Implementation of :func:`compute_adj_factors` under the mutation lock."""
    adjust_types = [adjust_type] if adjust_type else list(config.adj_factors_types)
    skipped = [t for t in adjust_types if t != STORED_ADJUST_TYPE]
    if skipped:
        logger.warning(
            "adj_factors: ignoring non-persisted adjust_types %s (only %s is stored; "
            "derive qfq via load(..., adjust='qfq') — ADR-0004)",
            skipped,
            STORED_ADJUST_TYPE,
        )
    adjust_types = [STORED_ADJUST_TYPE]

    watermark = None if full else _adj_factors_watermark(config)
    # Event refresh uses the latest bar date in the lake (not just this run's slice).
    from cn_market_lake.query.parquet_scan import list_hive_partition_dates

    latest_bar_date = None
    try:
        all_dates = list_hive_partition_dates(config.curated_root / "daily_bars", "trade_date")
        latest_bar_date = all_dates[-1] if all_dates else None
    except OSError:
        latest_bar_date = None

    refresh_set = set(refresh_symbols or [])
    if isinstance(latest_bar_date, date):
        refresh_set |= _event_refresh_symbols(config, latest_bar_date)
    if not full and watermark is not None:
        # Self-heal history the append-only path cannot see — see
        # `_uncovered_symbols`. Only meaningful when that path is in play:
        # `full`, and a lake with no watermark at all, already load every date,
        # and forcing a refresh there would only bypass a valid factor cache.
        uncovered = sorted(_uncovered_symbols(config) - refresh_set)
        if uncovered:
            # Capped so a lake that has never derived does not turn one daily run
            # into a full-market sweep; the remainder is picked up next run, and
            # a name Sina genuinely cannot serve lands in `failed` rather than
            # being retried without limit.
            batch = uncovered[:UNCOVERED_REFRESH_LIMIT]
            logger.info(
                "adj_factors: %d symbol(s) have bars the factor table does not reach; "
                "realigning %d this run (e.g. %s)",
                len(uncovered),
                len(batch),
                batch[:5],
            )
            refresh_set |= set(batch)

    bars = _bars_for_derive(config, watermark=watermark, refresh_set=refresh_set, full=full)
    if bars.is_empty():
        logger.info(
            "adj_factors: nothing to derive (watermark=%s, refresh=%d, full=%s)",
            watermark,
            len(refresh_set),
            full,
        )
        return AdjFactorsResult(0, 0, [], [])

    replace = full or watermark is None
    mode = "full-replace" if replace else "append-only"
    logger.info(
        "adj_factors: %s watermark=%s symbols=%d dates=%d refresh=%d",
        mode,
        watermark,
        bars["symbol"].n_unique(),
        bars["trade_date"].n_unique(),
        len(refresh_set),
    )

    workers = max(1, min(config.workers, 16))

    tasks: list[tuple[str, str, pl.DataFrame, bool]] = []
    skipped_cdr: list[str] = []
    for group in bars.partition_by("symbol"):
        sym = group["symbol"][0]
        if _is_cdr(sym):
            skipped_cdr.append(sym)
            continue
        sym_bars = group.select("trade_date").sort("trade_date")
        force = sym in refresh_set
        for adj in adjust_types:
            tasks.append((sym, adj, sym_bars, force))
    if skipped_cdr:
        logger.info(
            "adj_factors: skipping %d CDR symbol(s) %s — sina has no CDR factor "
            "coverage and all_a excludes CDRs; loads report adj_is_exact=False",
            len(skipped_cdr),
            skipped_cdr,
        )

    frames: list[pl.DataFrame] = []
    failed: list[str] = []
    findings: list[dict] = []

    if workers <= 1 or len(tasks) == 1:
        with httpx.Client(timeout=20.0) as client:
            for sym, adj, sym_bars, force in tasks:
                aligned, fail_key, finding = _process_symbol_adj(
                    config, sym, adj, sym_bars, force=force, client=client
                )
                if fail_key:
                    failed.append(fail_key)
                if finding:
                    findings.append(finding)
                if aligned is not None:
                    frames.append(aligned)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [
                pool.submit(
                    _process_symbol_adj,
                    config,
                    sym,
                    adj,
                    sym_bars,
                    force=force,
                )
                for sym, adj, sym_bars, force in tasks
            ]
            for fut in as_completed(futures):
                aligned, fail_key, finding = fut.result()
                if fail_key:
                    failed.append(fail_key)
                if finding:
                    findings.append(finding)
                if aligned is not None:
                    frames.append(aligned)

    if not frames:
        return AdjFactorsResult(0, len(tasks), failed, findings)

    out = pl.concat(frames, how="diagonal_relaxed").unique(subset=_ADJ_PK, keep="last")
    findings.extend(_factor_continuity_findings(out))
    out = with_provenance(out, source=config.adj_factors_source, data_version="v1")

    total = _write_adj_partitions(config, out, replace=replace)
    return AdjFactorsResult(total, len(tasks), failed, findings)
