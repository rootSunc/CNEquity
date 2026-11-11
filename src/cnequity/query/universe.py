"""Universe filtering for the query reader."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.domain.canonical import dedupe_by_primary_key, dedupe_lazy_by_primary_key
from cnequity.domain.market_profile import exchange_start
from cnequity.domain.symbols import (
    CDR_PREFIXES,
    ETF_PREFIXES,
    EXCLUDED_PREFIXES,
    PREFIX_WHITELIST,
)
from cnequity.domain.trading_status import risk_warning_expr
from cnequity.query.parquet_scan import (
    collect_parquet_root,
    coverage_start_from_partitions,
    list_partitions,
    scan_parquet_root,
)
from cnequity.storage.read_context import ReadContext, read_root
from cnequity.storage.revisions import resolve_committed_root

# Statuses a tradable-universe query drops. ST no longer lives in `status`, so
# the risk-warning test is a separate predicate (`_excluded_status_expr`) that
# also understands the legacy encoding.
EXCLUDED_STATUSES = frozenset({"suspended", "delisted", "st", "*st"})


def _excluded_status_expr(columns) -> pl.Expr:
    """Rows a tradable universe must drop: halted, delisted or risk-warned."""
    return pl.col("status").is_in(list(EXCLUDED_STATUSES)) | risk_warning_expr(columns)


SUPPORTED_UNIVERSES = frozenset({"all_a", "all_a_sh_sz"})


class UniverseCoverageError(ValueError):
    """Raised when a strict universe query lacks required coverage."""


def _all_a_symbol_expr(symbol_col: str = "symbol", *, universe: str = "all_a") -> pl.Expr:
    if universe not in SUPPORTED_UNIVERSES:
        raise ValueError(
            f"unsupported universe: {universe!r} "
            f"(supported: {', '.join(sorted(SUPPORTED_UNIVERSES))})"
        )
    code = pl.col(symbol_col).str.split(".").list.first()
    exchange = pl.col(symbol_col).str.split(".").list.last()
    excluded = pl.lit(False)
    # 81–89 is a SH/SZ reservation; do not apply it to BJ (legacy 83xxxx).
    for prefix in EXCLUDED_PREFIXES:
        excluded = excluded | (exchange.is_in(["SH", "SZ"]) & code.str.starts_with(prefix))
    # CDRs (SH 689xxx) are depositary receipts, not common stock; they stay in
    # the lake but are not part of the all_a selection universe.
    for prefix in CDR_PREFIXES:
        excluded = excluded | ((exchange == "SH") & code.str.starts_with(prefix))
    # ETFs/LOFs stay in instruments + daily_bars for UI/quotes, but never enter
    # the all_a research universe (PREFIX_WHITELIST also omits them).
    for exch, prefixes in ETF_PREFIXES.items():
        for prefix in prefixes:
            excluded = excluded | ((exchange == exch) & code.str.starts_with(prefix))
    allowed = pl.lit(False)
    for exch, prefixes in PREFIX_WHITELIST.items():
        for prefix in prefixes:
            allowed = allowed | ((exchange == exch) & code.str.starts_with(prefix))
    if universe == "all_a_sh_sz":
        allowed = allowed & exchange.is_in(["SH", "SZ"])
    return (~excluded) & allowed


def _load_instruments(config: Config, read_context: ReadContext | None = None) -> pl.DataFrame:
    root = read_root(config, "instruments", read_context)
    try:
        return dedupe_by_primary_key(
            collect_parquet_root(
                root,
                hive=False,
                dataset="instruments",
                meta_root=config.meta_root,
                committed=False,
            ),
            "instruments",
        )
    except FileNotFoundError:
        return pl.DataFrame()


def _load_trading_status(
    config: Config,
    *,
    trade_date: date | None = None,
) -> pl.DataFrame:
    root = config.curated_root / "trading_status"
    try:
        return dedupe_by_primary_key(
            collect_parquet_root(
                root,
                partition_col="trade_date",
                start=trade_date,
                end=trade_date,
                dataset="trading_status",
                meta_root=config.meta_root,
            ),
            "trading_status",
        )
    except FileNotFoundError:
        return pl.DataFrame()


def coverage_start_date(
    config: Config,
    dataset: str,
    *,
    date_col: str = "trade_date",
) -> date | None:
    """Earliest *date_col* present in curated *dataset*, if any."""
    root = resolve_committed_root(
        config.curated_root / dataset,
        dataset=dataset,
        meta_root=config.meta_root,
    )
    if not root.exists():
        return None

    # A day partition's directory value is the exact date it can contain. A
    # month/year/quarter directory is only a container, though: using its
    # theoretical start would claim coverage before the first real row. Mixed
    # layouts and loose root-level files need the same exact scan.
    parts = list_partitions(root, date_col, resolve=False)
    if (
        dataset != "daily_bars"
        and parts
        and all(part.start == part.end for part in parts)
        and not list(root.glob("*.parquet"))
    ):
        return coverage_start_from_partitions(root, date_col)
    if (
        dataset == "daily_bars"
        and date_col == "trade_date"
        and parts
        and all(part.start == part.end for part in parts)
        and not list(root.glob("*.parquet"))
    ):
        # Day partitions bound the scan exactly. Keep walking from the front
        # because the earliest partition can contain only zero-volume
        # suspension placeholders, which do not establish traded coverage.
        for part in parts:
            scan = scan_parquet_root(
                root,
                partition_col=date_col,
                start=part.start,
                end=part.end,
                traded_only=True,
                dataset=dataset,
                meta_root=config.meta_root,
                committed=False,
            )
            value = scan.select(pl.col(date_col).min()).collect().item()
            if value is not None:
                return value
        return None
    try:
        scan = scan_parquet_root(
            root,
            partition_col=date_col,
            traded_only=dataset == "daily_bars" and date_col == "trade_date",
            dataset=dataset,
            meta_root=config.meta_root,
            committed=False,
        )
        return scan.select(pl.col(date_col).min()).collect().item()
    except FileNotFoundError:
        return None


def coverage_end_date(
    config: Config,
    dataset: str,
    *,
    date_col: str = "trade_date",
    read_context: ReadContext | None = None,
) -> date | None:
    """Latest *date_col* present in curated *dataset*, if any.

    Partition directory bounds are exact only for day partitions. Coarser or
    mixed layouts must use the date stored in the row; otherwise a partial
    current month/year can make an incomplete research window look complete.
    """
    root = read_root(config, dataset, read_context)
    if not root.exists():
        return None
    parts = list_partitions(root, date_col, resolve=False)
    if (
        dataset != "daily_bars"
        and parts
        and all(part.start == part.end for part in parts)
        and not list(root.glob("*.parquet"))
    ):
        return parts[-1].end
    if (
        dataset == "daily_bars"
        and date_col == "trade_date"
        and parts
        and all(part.start == part.end for part in parts)
        and not list(root.glob("*.parquet"))
    ):
        # As above, do not trust a final placeholder-only partition. In the
        # normal lake this reads one Parquet partition instead of every
        # historical file; the fallback remains fully row-based for mixed or
        # loose layouts.
        for part in reversed(parts):
            scan = scan_parquet_root(
                root,
                partition_col=date_col,
                start=part.start,
                end=part.end,
                traded_only=True,
                dataset=dataset,
                meta_root=config.meta_root,
                committed=False,
            )
            value = scan.select(pl.col(date_col).max()).collect().item()
            if value is not None:
                return value
        return None
    try:
        scan = scan_parquet_root(
            root,
            partition_col=date_col,
            traded_only=dataset == "daily_bars" and date_col == "trade_date",
            dataset=dataset,
            meta_root=config.meta_root,
            committed=False,
        )
        return scan.select(pl.col(date_col).max()).collect().item()
    except FileNotFoundError:
        return None


def trading_status_coverage_start(config: Config) -> date | None:
    """First trade_date with curated trading_status rows."""
    return coverage_start_date(config, "trading_status")


def st_coverage_start(config: Config) -> date | None:
    """First trade_date with an ``st``/``*st`` label in trading_status.

    Suspension is reconstructed from bar gaps (whole history). This is only a
    descriptive positive-label boundary; strict historical research must use a
    complete versioned ST evidence receipt instead of this minimum date.
    """
    root = resolve_committed_root(
        config.curated_root / "trading_status",
        dataset="trading_status",
        meta_root=config.meta_root,
    )
    if not root.exists():
        return None
    try:
        status = dedupe_lazy_by_primary_key(
            scan_parquet_root(
                root,
                partition_col="trade_date",
                dataset="trading_status",
                meta_root=config.meta_root,
                committed=False,
            ),
            "trading_status",
        )
        return (
            status.filter(risk_warning_expr(status.collect_schema().names()))
            .select(pl.col("trade_date").min())
            .collect()
            .item()
        )
    except FileNotFoundError:
        return None


def tradable_symbols_on_date(
    config: Config,
    trade_date: date,
    *,
    universe: str = "all_a",
    strict: bool = False,
) -> pl.DataFrame | None:
    """Return ``symbol`` rows tradable on *trade_date* for the given universe rule.

    Applies list/delist dates from ``instruments``. ST/suspended filtering uses
    ``trading_status`` only when rows exist for *trade_date*; strict historical
    research additionally requires a complete ST evidence receipt.
    """
    if universe not in SUPPORTED_UNIVERSES:
        raise ValueError(
            f"unsupported universe: {universe!r} "
            f"(supported: {', '.join(sorted(SUPPORTED_UNIVERSES))})"
        )

    instruments = _load_instruments(config)
    if instruments.is_empty():
        if strict:
            raise UniverseCoverageError(f"{universe} universe requires curated instruments")
        return None

    out = (
        instruments.filter(_all_a_symbol_expr(universe=universe))
        .with_columns(_exchange_listing_expr().alias("list_date"))
        .filter(pl.col("list_date").is_null() | (pl.col("list_date") <= trade_date))
        .filter(pl.col("delist_date").is_null() | (pl.col("delist_date") >= trade_date))
        .select("symbol")
    )
    if out.is_empty():
        return pl.DataFrame(schema={"symbol": pl.Utf8})

    status = _load_trading_status(config, trade_date=trade_date)
    if status.is_empty():
        if strict:
            raise UniverseCoverageError(
                f"{universe} universe has no trading_status coverage for {trade_date.isoformat()}"
            )
        return out

    if strict:
        covered_symbols = set(status.get_column("symbol").drop_nulls().to_list())
        missing_symbols = sorted(set(out.get_column("symbol").to_list()) - covered_symbols)
        if missing_symbols:
            raise UniverseCoverageError(
                f"{universe} universe trading_status is missing {len(missing_symbols)} "
                f"symbol(s) for {trade_date.isoformat()}, first={missing_symbols[0]}"
            )

    bad = status.filter((~pl.col("is_trading")) | _excluded_status_expr(status.columns))["symbol"]
    if not bad.is_empty():
        out = out.filter(~pl.col("symbol").is_in(bad))
    return out


def _exchange_listing_expr() -> pl.Expr:
    """Listing date as the universe reads it: a Beijing security's exchange start.

    Its NEEQ over-the-counter quotes before that are not part of any universe.
    """
    return (
        pl.when(pl.col("symbol").str.ends_with(".BJ"))
        .then(exchange_start(pl.col("list_date")))
        .otherwise(pl.col("list_date"))
    )


def apply_universe_filter(
    df: pl.DataFrame,
    config: Config,
    *,
    universe: str,
    date_col: str = "trade_date",
    strict: bool = False,
    read_context: ReadContext | None = None,
) -> pl.DataFrame:
    """Filter bar-like frames to tradable universe rows per *date_col*.

    ``instruments`` list/delist rules always apply. ST/suspended removal via
    ``trading_status`` only affects dates with status rows; strict historical
    reads also require a complete ST evidence receipt.
    """
    if universe not in SUPPORTED_UNIVERSES:
        raise ValueError(
            f"unsupported universe: {universe!r} "
            f"(supported: {', '.join(sorted(SUPPORTED_UNIVERSES))})"
        )
    if date_col not in df.columns:
        raise ValueError(
            f"apply_universe_filter requires date column {date_col!r} for universe filtering"
        )
    if df.is_empty():
        if strict:
            raise UniverseCoverageError(
                f"{universe} universe cannot prove coverage because the requested daily_bars "
                "scope returned no rows"
            )
        return df

    instruments = _load_instruments(config, read_context)
    if instruments.is_empty():
        if strict:
            raise UniverseCoverageError(f"{universe} universe requires curated instruments")
        return df

    valid_symbols = instruments.filter(_all_a_symbol_expr(universe=universe))["symbol"]
    inst = instruments.select("symbol", _exchange_listing_expr().alias("list_date"), "delist_date")
    df = df.join(inst, on="symbol", how="left")
    df = df.filter(
        pl.col("list_date").is_null() | (pl.col("list_date") <= pl.col(date_col))
    ).filter(pl.col("delist_date").is_null() | (pl.col("delist_date") >= pl.col(date_col)))
    # An existing instruments catalog with zero valid all-A symbols is a real
    # empty universe (for example an ETF/CDR-only fixture), not a reason to
    # bypass the filter and leak every input row through.
    df = df.filter(pl.col("symbol").is_in(valid_symbols.to_list()))
    if df.is_empty():
        return df.drop(["list_date", "delist_date"], strict=False)

    # Status is partitioned by month but can span the whole research history.
    # The universe decision only needs the dates present in this bar query;
    # leaving the bounds unset would decode the full status lake for every
    # `load(..., universe=...)` call.
    status_start = df.get_column(date_col).min()
    status_end = df.get_column(date_col).max()

    try:
        status = dedupe_lazy_by_primary_key(
            scan_parquet_root(
                read_root(config, "trading_status", read_context),
                partition_col="trade_date",
                start=status_start,
                end=status_end,
                dataset="trading_status",
                meta_root=config.meta_root,
                committed=False,
            ),
            "trading_status",
        )
    except FileNotFoundError:
        if strict:
            raise UniverseCoverageError(
                f"{universe} universe requires curated trading_status"
            ) from None
        return df.drop(["list_date", "delist_date"], strict=False)

    if strict and not df.is_empty():
        requested = df.select(["symbol", date_col]).unique()
        covered = status.select(["symbol", pl.col("trade_date").alias(date_col)]).unique().collect()
        missing = requested.join(covered, on=["symbol", date_col], how="anti")
        if not missing.is_empty():
            missing_dates = sorted(missing[date_col].unique().to_list())
            missing_symbols = sorted(missing["symbol"].unique().to_list())
            raise UniverseCoverageError(
                f"{universe} universe trading_status missing {missing.height} "
                f"symbol-date row(s) across {len(missing_dates)} date(s), "
                f"first={missing_symbols[0]}@{missing_dates[0].isoformat()}"
            )

        # Presence of a trading_status row proves that a row was written, not
        # that an historical ST sweep checked the whole requested scope. A
        # strict all-A read must also have a versioned evidence receipt; this
        # is what prevents a backtest from silently treating an unobserved
        # historical symbol as normal. The receipt check is opt-in through
        # ``strict_universe`` because ordinary exploratory loads retain their
        # documented permissive semantics.
        from cnequity.quality.st_coverage import st_evidence_coverage_report

        evidence = st_evidence_coverage_report(
            config,
            start=requested[date_col].min(),
            end=requested[date_col].max(),
            symbols=sorted(requested["symbol"].drop_nulls().unique().to_list()),
            universe=universe,
            read_context=read_context,
        )
        if not evidence["verified"]:
            unsupported = int(evidence.get("unsupported_symbols", 0) or 0)
            if evidence.get("reason") == "unsupported_exchange_symbols":
                exchange_counts = evidence.get("unsupported_exchange_counts") or {}
                detail = ", ".join(
                    f"{exchange}={count}" for exchange, count in sorted(exchange_counts.items())
                )
                raise UniverseCoverageError(
                    f"{universe} universe requires historical ST evidence, but the requested "
                    f"scope contains {unsupported} symbol(s) on unsupported exchange(s) "
                    f"({detail or 'unknown'})"
                )
            raise UniverseCoverageError(
                f"{universe} universe requires a complete historical ST evidence receipt for "
                f"{requested[date_col].min().isoformat()}.."
                f"{requested[date_col].max().isoformat()} "
                f"({evidence.get('reason') or 'no_matching_complete_receipt'}). "
                "Current trading_status does not replace it. "
                "Next: `cne backfill trading_status`"
            )

    bad = (
        status.filter(
            (~pl.col("is_trading")) | _excluded_status_expr(status.collect_schema().names())
        )
        .select(["symbol", pl.col("trade_date").alias(date_col)])
        .collect()
    )
    if bad.is_empty():
        return df.drop(["list_date", "delist_date"], strict=False)

    df = df.join(bad, on=["symbol", date_col], how="anti")
    return df.drop(["list_date", "delist_date"], strict=False)
