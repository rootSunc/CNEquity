"""Python read API — load curated datasets with adjustment, universe, and PIT filters."""

from __future__ import annotations

import logging
import os
import warnings
from collections.abc import Mapping
from datetime import date, timedelta
from pathlib import Path
from typing import Literal

import polars as pl

from cnequity.config import Config, load_config
from cnequity.derive.adj_factors import STORED_ADJUST_TYPE
from cnequity.domain.action_sessions import effective_session
from cnequity.domain.datasets import (
    DATASETS,
    DERIVE_COMMAND_DATASETS,
    RESAMPLED_MINUTE_DATASETS,
    curated_dataset_names,
    derived_dataset_names,
    intraday_dataset_names,
    pit_dataset_names,
)
from cnequity.domain.market_profile import OTC_SCOPED_DATASETS, otc_quote_expr
from cnequity.domain.pit import (
    PIT_STORAGE_COLUMNS,
    PitMode,
    classify_pit_rows,
    normalize_pit_storage_columns,
    retain_pit_vintages,
)
from cnequity.domain.schemas import DATASET_SCHEMAS, PRIMARY_KEYS, validate_dataframe
from cnequity.domain.universe_profiles import (
    UniverseProfile,
    UniverseProfileError,
    resolve_universe_profile,
)
from cnequity.query.canonical import dedupe_by_primary_key
from cnequity.query.parquet_scan import (
    collect_parquet_root,
    dataset_has_parquet,
    partition_col_for_dataset,
    scan_parquet_root,
)
from cnequity.query.universe import apply_universe_filter
from cnequity.storage.read_context import ReadContext

logger = logging.getLogger(__name__)

AdjustType = Literal["qfq", "hfq", "total_return"]
UniverseType = Literal["all_a", "all_a_sh_sz"]
UniverseProfileLike = str | UniverseProfile
RevisionRef = int | str
RevisionSelection = RevisionRef | Mapping[str, RevisionRef]

# All derived from the DatasetSpec registry (domain/datasets.py).
CURATED_DATASETS = curated_dataset_names()
DERIVED_DATASETS = derived_dataset_names()
DATE_COLUMNS: dict[str, str] = {
    name: spec.query_date_col for name, spec in DATASETS.items() if spec.query_date_col is not None
}
PIT_DATASETS = pit_dataset_names()
# Columns an adjustment factor multiplies. `price` is here for trade_ticks,
# which has no OHLC — without it the dataset would be in ADJUSTABLE_DATASETS
# and `load(adjust="hfq")` would return no adj_ columns at all, silently.
PRICE_COLS = ("open", "high", "low", "close", "price")
# Datasets carrying per-share prices that adj_factors can adjust. Intraday
# datasets come from the registry so a new frequency is adjustable the day it
# is registered. Index levels are not per-share prices and must never be
# multiplied by stock adjustment factors.
# trade_ticks is listed by name rather than inherited from
# intraday_dataset_names(): it deliberately carries no `intraday_frequency`
# (see its DatasetSpec), but its prices still cross ex-dividend dates and a
# comparison spanning one would otherwise see a gap that is not a price move.
# The stored 15m/30m/60m resamples are unadjusted per-share prices too.
ADJUSTABLE_DATASETS = (
    {"daily_bars", "trade_ticks"} | set(intraday_dataset_names()) | set(RESAMPLED_MINUTE_DATASETS)
)


class ReaderError(ValueError):
    """Raised when load() arguments or dataset state are invalid."""


class MissingAdjustmentError(ReaderError):
    """An exact adjusted read needs factors absent from the available lake."""


def _merge_revision_selection(
    revision: RevisionSelection | None,
    revision_map: Mapping[str, RevisionRef] | None,
) -> RevisionSelection | None:
    """Combine the scalar compatibility argument with an explicit map.

    A scalar revision historically identified the dataset being loaded.  An
    adjusted read touches a second independently versioned dataset, however,
    so treating that same integer as ``adj_factors`` is incorrect.  Callers
    can pin each dataset with ``revision={"daily_bars": 7,
    "adj_factors": 12}`` (or the ``revision_map=`` alias); retaining a scalar
    here keeps old unadjusted reads source-compatible.
    """

    if revision_map is None:
        return revision
    if revision is None:
        return dict(revision_map)
    if isinstance(revision, Mapping):
        merged = dict(revision)
        overlap = set(merged) & set(revision_map)
        for key in overlap:
            if merged[key] != revision_map[key]:
                raise ReaderError(f"revision and revision_map disagree for dataset {key!r}")
        merged.update(revision_map)
        return merged
    # A scalar is the legacy selection for the primary dataset.  Keep it in a
    # distinguished key so a revision map can add (for example) the factor
    # vintage without accidentally applying the scalar to every dataset.
    merged = dict(revision_map)
    merged.setdefault("__primary__", revision)
    return merged


def _revision_for_dataset(
    selection: RevisionSelection | None,
    dataset: str,
    *,
    fallback_primary: bool = True,
) -> RevisionRef | None:
    """Resolve one dataset's revision from a scalar or per-dataset map."""

    if selection is None:
        return None
    if not isinstance(selection, Mapping):
        return selection
    # Accept both the compact map and a serialized research-manifest shape.
    nested = selection.get("datasets")
    if isinstance(nested, Mapping) and dataset in nested:
        return nested[dataset]
    if dataset in selection:
        return selection[dataset]
    wildcard = selection.get("*")
    if wildcard is not None and not isinstance(wildcard, Mapping):
        return wildcard
    global_selection = selection.get("global")
    if isinstance(global_selection, Mapping) and dataset in global_selection:
        return global_selection[dataset]
    if global_selection is not None and not isinstance(global_selection, Mapping):
        return global_selection
    if fallback_primary and dataset != "__primary__":
        return selection.get("__primary__")
    return None


def _resolve_reader_scope(
    *,
    universe: UniverseType | str | None,
    profile: UniverseProfileLike | None,
    universe_profile: UniverseProfileLike | None,
    strict_universe: bool,
) -> tuple[str | None, UniverseProfile | None, bool]:
    """Resolve profile/universe compatibility arguments for one read.

    The old ``universe`` argument remains a permissive compatibility path.  A
    named profile is an explicit research contract and enables its strict
    evidence requirements automatically.  ``universe_profile`` is accepted as
    a spelling used by integrations; supplying both profile spellings is only
    valid when they resolve to the same registry record.
    """

    selected = profile
    if profile is not None and universe_profile is not None:
        try:
            if resolve_universe_profile(profile) != resolve_universe_profile(universe_profile):
                raise ReaderError("profile and universe_profile refer to different profiles")
        except UniverseProfileError as exc:
            raise ReaderError(str(exc)) from exc
    elif selected is None:
        selected = universe_profile

    if selected is not None:
        try:
            resolved = resolve_universe_profile(selected)
        except UniverseProfileError as exc:
            raise ReaderError(str(exc)) from exc
        if universe is not None and universe != resolved.legacy_universe:
            raise ReaderError(
                f"universe={universe!r} conflicts with profile={resolved.name!r} "
                f"(profile uses {resolved.legacy_universe!r})"
            )
        return resolved.legacy_universe, resolved, bool(strict_universe or resolved.strict_research)

    if universe == "all_a":
        warnings.warn(
            "universe='all_a' is a deprecated compatibility alias; choose an explicit "
            "versioned profile such as 'cn_a_sh_sz_research_v1' or "
            "'cn_a_all_experimental_v1'",
            DeprecationWarning,
            stacklevel=3,
        )
    return universe, None, strict_universe


def _require_profile_delisting_evidence(
    config: Config,
    frame: pl.DataFrame,
    profile: UniverseProfile,
    read_context: ReadContext | None = None,
) -> None:
    """Fail closed when a strict profile cannot prove survivorship coverage."""
    if not profile.strict_research or frame.is_empty() or "trade_date" not in frame.columns:
        return
    if not any(
        requirement.startswith("historical_delisting_coverage")
        for requirement in profile.evidence_requirements
    ):
        return
    from cnequity.quality.delisted_coverage import delisted_coverage_report

    start = frame["trade_date"].min()
    end = frame["trade_date"].max()
    report = delisted_coverage_report(
        config,
        start,
        end,
        universe=profile.legacy_universe,
        read_context=read_context,
    )
    if not report.get("verified"):
        counts = report.get("counts") or {}
        unresolved = sum(
            int(counts.get(key, 0) or 0)
            for key in (
                "pending_probe",
                "missing_bars",
                "unknown_overlap",
                "terminal_mismatch",
                "missing_instrument",
                "invalid_delist_date",
                "recent_quarantined",
                "formal_unresolved",
            )
        )
        raise ReaderError(
            f"profile={profile.name!r} requires complete historical delisting evidence for "
            f"{start.isoformat()}..{end.isoformat()} (unresolved={unresolved})"
        )


def _parse_date(value: str | date | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def resolve_config(
    *,
    config: Config | None = None,
    data_root: str | Path | None = None,
) -> Config:
    if config is not None:
        return config
    if data_root is not None:
        return Config(data_root=Path(data_root).expanduser().resolve())
    # The same lookup as the CLI, so `load()` in a notebook opened from any
    # directory reads the lake `cne` commands update.
    from cnequity.config.locate import PROJECT_CONFIG, find_config, user_config_path

    env = os.environ.get("CNE_CONFIG")
    if env:
        return load_config(Path(env).expanduser())
    path = find_config()
    if path is not None:
        return load_config(path)
    raise ReaderError(
        "No config found: pass config= or data_root=, set CNE_CONFIG, or create "
        f"{PROJECT_CONFIG.as_posix()} or {user_config_path()} (`cne init` writes the latter)"
    )


def _missing_dataset_message(dataset: str, root, config) -> str:
    """Say what is absent and what fills it.

    The path alone answers "is it there" and nothing else, which is the wrong
    half for the most common way to meet this error: `load(..., adjust="hfq")`
    on a lake that has bars but no factors yet, where the missing dataset is
    one the reader asked for rather than one the caller named.
    """
    from cnequity.research.packs import next_command_for_dataset

    direct = (
        f"cne derive {dataset}" if dataset in DERIVE_COMMAND_DATASETS else f"cne backfill {dataset}"
    )
    # A demo or sample lake is never grown by `cne init` or a pack's daily
    # run (both refuse it, or would build a whole market); only the direct
    # command fills the one dataset that is missing.
    demo = getattr(config, "lake_profile", None) in {"demo", "sample"}
    remedy = direct if demo else (next_command_for_dataset(dataset) or direct)
    return (
        f"no parquet data for dataset {dataset!r} under {root} (data_root={config.data_root}); "
        f"build it with `{remedy}`"
    )


def _dataset_root(config: Config, dataset: str) -> Path:
    if dataset in DERIVED_DATASETS:
        return config.derived_root / dataset
    if dataset in CURATED_DATASETS:
        return config.curated_root / dataset
    raise ReaderError(f"unknown dataset {dataset!r}")


def _day_has_traded_bar(config: Config, day: date) -> bool:
    """Whether one daily-bar session contains a real print.

    Coverage only needs the first and last such session. Opening every
    partition footer to answer that question is what made the dashboard's
    first paint scan the whole price history.
    """
    try:
        found = (
            scan_parquet_root(
                _dataset_root(config, "daily_bars"),
                partition_col="trade_date",
                start=day,
                end=day,
                traded_only=True,
                dataset="daily_bars",
                meta_root=config.meta_root,
            )
            .limit(1)
            .collect()
        )
    except FileNotFoundError:
        return False
    return found.height > 0


def _catalog_coverage_bounds(config: Config, dataset: str) -> tuple[date | None, date | None]:
    """Return truthful catalog bounds without scanning non-date columns.

    Day partitions encode their exact coverage in the directory name. Coarser
    or mixed date partitions do not: ``trade_date=2026`` may contain only the
    first week of the year. For those layouts, read only the registered date
    column so the catalog cannot turn a partial current period into a fresh
    dataset. Report-period partitions intentionally retain their period bounds
    because ``coverage_start``/``coverage_end`` describe the report periods for
    those datasets, not their announcement dates.
    """
    spec = DATASETS[dataset]
    root = _dataset_root(config, dataset)
    if (
        not dataset_has_parquet(
            root,
            dataset=dataset,
            meta_root=config.meta_root,
            revision=None,
        )
        or spec.partition_col is None
    ):
        return None, None

    from cnequity.query.parquet_scan import list_partitions

    parts = list_partitions(root, spec.partition_col)
    root_files = list(root.glob("*.parquet"))
    if parts and spec.partition_granularity == "quarter" and not root_files:
        return parts[0].start, parts[-1].end
    if parts and spec.query_date_col != spec.partition_col and not root_files:
        return parts[0].start, parts[-1].end
    if parts and all(part.start == part.end for part in parts) and not root_files:
        if dataset != "daily_bars":
            return parts[0].start, parts[-1].end
        # An empty or all-zero day must not extend coverage, but the days in
        # between are not needed to name the two endpoints.
        start = next(
            (part.start for part in parts if _day_has_traded_bar(config, part.start)), None
        )
        if start is None:
            return None, None
        end = next(
            (part.start for part in reversed(parts) if _day_has_traded_bar(config, part.start)),
            None,
        )
        return start, end

    date_col = spec.query_date_col
    if date_col is None:
        return None, None
    try:
        day_hive = bool(parts and all(part.start == part.end for part in parts) and not root_files)
        lf = scan_parquet_root(
            root,
            partition_col=spec.partition_col,
            hive=day_hive,
            traded_only=dataset == "daily_bars",
            dataset=dataset,
            meta_root=config.meta_root,
        )
        if date_col not in lf.collect_schema().names():
            return None, None
        bounds = (
            lf.select(
                pl.col(date_col).min().alias("_catalog_min"),
                pl.col(date_col).max().alias("_catalog_max"),
            )
            .collect()
            .row(0)
        )
    except FileNotFoundError:
        return None, None
    if spec.name in {"market_breadth", "industry_index"} and bounds[1] is not None:
        from cnequity.quality.verify import last_contiguous_dense_date

        safe_end = last_contiguous_dense_date(config, spec)
        if safe_end is not None:
            return bounds[0], min(bounds[1], safe_end)
    return bounds[0], bounds[1]


def _has_listing_dates(config: Config) -> bool:
    """Whether a committed ``instruments`` exists for the NEEQ cut to read.

    Without one the cut falls back to the select tier's opening date alone,
    and the read depends on nothing more than the dataset itself.
    """
    from cnequity.storage.revisions import RevisionStore

    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root, create=False)
    return store.current_root("instruments") is not None


def _drop_otc_quotes(
    df: pl.DataFrame, config: Config, read_context: ReadContext | None, date_col: str
) -> pl.DataFrame:
    """Leave out Beijing rows from before the security's exchange start."""
    from cnequity.query.universe import _load_instruments

    # A pinned read carries instruments when the lake has a committed one;
    # otherwise the listing dates come from what the lake holds now.
    pinned = read_context if read_context and "instruments" in read_context.roots else None
    instruments = _load_instruments(config, pinned)
    listing = (
        instruments.select("symbol", "list_date").unique("symbol")
        if not instruments.is_empty() and "list_date" in instruments.columns
        else pl.DataFrame(schema={"symbol": pl.Utf8, "list_date": pl.Date})
    )
    return (
        df.join(listing.rename({"list_date": "_list_date"}), on="symbol", how="left")
        .filter(~otc_quote_expr(day=date_col, list_date="_list_date").fill_null(False))
        .drop("_list_date")
    )


def _preview_can_answer(dataset: str, symbols, revision, read_context) -> bool:
    """Finished symbols are readable on the live lake, not on a pinned revision.

    ``load`` always captures the current committed roots. That capture is not a
    historical pin; a pin arrives as ``revision``.
    """
    del read_context
    return dataset == "daily_bars" and bool(symbols) and revision is None


def _with_daily_bar_preview(
    config: Config,
    df: pl.DataFrame,
    *,
    dataset: str,
    symbols: list[str] | None,
    start: date | None,
    end: date | None,
    revision: RevisionRef | None,
    read_context: ReadContext | None,
) -> pl.DataFrame:
    """Add sealed, not-yet-published bars for an explicit symbol list.

    A sweep publishes the whole market in one compact. Until then, finished
    batches are readable only when the caller names symbols and does not pin a
    revision. Adjusted columns still require published ``adj_factors``.
    """
    if not _preview_can_answer(dataset, symbols, revision, read_context):
        return df
    have: set[str] = set()
    if not df.is_empty() and "symbol" in df.columns:
        have = set(df.get_column("symbol").to_list())
    missing = [symbol for symbol in symbols or [] if symbol not in have]
    if not missing:
        return df
    from cnequity.research.preview import read_daily_bar_preview

    extra = read_daily_bar_preview(config, missing, start, end)
    if extra.is_empty():
        return df
    if df.is_empty():
        return extra
    return pl.concat([df, extra], how="diagonal_relaxed")


def _read_dataset(
    config: Config,
    dataset: str,
    *,
    start: date | None = None,
    end: date | None = None,
    symbols: list[str] | None = None,
    universe: UniverseType | str | None = None,
    strict_universe: bool = False,
    revision: RevisionRef | None = None,
    read_context: ReadContext | None = None,
    include_otc: bool = True,
) -> pl.DataFrame:
    root = read_context.roots[dataset] if read_context else _dataset_root(config, dataset)
    published = dataset_has_parquet(
        root,
        dataset=dataset,
        meta_root=config.meta_root,
        revision=revision,
        committed=read_context is None,
    )
    if not published and not _preview_can_answer(dataset, symbols, revision, read_context):
        raise ReaderError(_missing_dataset_message(dataset, root, config))

    partition_col = DATE_COLUMNS.get(dataset) or partition_col_for_dataset(dataset)
    df = pl.DataFrame()
    if published:
        try:
            df = collect_parquet_root(
                root,
                partition_col=partition_col,
                start=start,
                end=end,
                symbols=symbols,
                dataset=dataset,
                meta_root=config.meta_root,
                revision=revision,
                committed=read_context is None,
            )
        except FileNotFoundError as exc:
            raise ReaderError(_missing_dataset_message(dataset, root, config)) from exc
    df = _with_daily_bar_preview(
        config,
        df,
        dataset=dataset,
        symbols=symbols,
        start=start,
        end=end,
        revision=revision,
        read_context=read_context,
    )
    if df.is_empty() and not published:
        raise ReaderError(_missing_dataset_message(dataset, root, config))
    if not include_otc and dataset in OTC_SCOPED_DATASETS and not df.is_empty():
        df = _drop_otc_quotes(df, config, read_context, DATE_COLUMNS.get(dataset, "trade_date"))
    # Apply semantic scope before strict schema validation.  Live snapshots
    # legitimately contain retired, future-listed, and unavailable quote
    # rows; those rows are outside an ``all_a`` query and must not make a
    # scoped research read fail before the scope can remove them.
    if universe and dataset == "daily_bars":
        df = apply_universe_filter(
            df,
            config,
            universe=universe,
            date_col=DATE_COLUMNS[dataset],
            strict=strict_universe,
            read_context=read_context,
        )
        price_cols = [col for col in ("open", "high", "low", "close") if col in df.columns]
        if price_cols and not df.is_empty():
            usable = pl.all_horizontal(pl.col(col) > 0 for col in price_cols)
            dropped = df.filter(~usable).height
            if dropped:
                logger.warning(
                    "daily_bars: dropped %d non-positive-price placeholder row(s) from universe=%s",
                    dropped,
                    universe,
                )
                df = df.filter(usable)

    if dataset in DATASET_SCHEMAS:
        # Bitemporal PIT columns are an additive, optional storage contract.
        # ``validate_dataframe`` carries them through when the frame already
        # has them, so normalising first is enough: a file written before the
        # columns existed gains them here, a current one keeps what it stores.
        # This used to split them out and ``hstack`` them back on, because
        # validation dropped every unregistered column.
        if dataset in PIT_DATASETS:
            df = normalize_pit_storage_columns(df, dataset)
        df = validate_dataframe(df, dataset)
        if dataset in PIT_DATASETS:
            return retain_pit_vintages(df, dataset)
        return dedupe_by_primary_key(df, dataset)
    return df


def _apply_date_range(
    df: pl.DataFrame,
    dataset: str,
    start: date | None,
    end: date | None,
) -> pl.DataFrame:
    col = DATE_COLUMNS.get(dataset)
    if col is None or col not in df.columns or df.is_empty():
        return df
    if start is not None:
        df = df.filter(pl.col(col) >= start)
    if end is not None:
        df = df.filter(pl.col(col) <= end)
    return df


def _quarter_label(value: date) -> str:
    """Map a calendar boundary to the report-period label it intersects."""
    quarter = (value.month - 1) // 3 + 1
    return f"{value.year}Q{quarter}"


def _apply_pit_date_range(
    df: pl.DataFrame,
    dataset: str,
    start: date | None,
    end: date | None,
) -> pl.DataFrame:
    """Apply ``start``/``end`` to PIT rows without comparing dates to strings."""
    if df.is_empty() or (start is None and end is None):
        return df
    column = DATE_COLUMNS.get(dataset)
    if column is None or column not in df.columns:
        return df
    dtype = df.schema[column]
    if dtype == pl.String:
        if dataset != "financial_statement_items" or column != "report_period":
            raise ReaderError(
                f"{dataset}: start/end cannot filter non-temporal date column {column!r}"
            )
        if start is not None:
            df = df.filter(pl.col(column) >= _quarter_label(start))
        if end is not None:
            df = df.filter(pl.col(column) <= _quarter_label(end))
        return df
    if dtype not in (pl.Date, pl.Datetime):
        raise ReaderError(f"{dataset}: unsupported date-column type for {column!r}: {dtype}")
    return _apply_date_range(df, dataset, start, end)


def _apply_symbol_filter(df: pl.DataFrame, symbols: list[str] | None) -> pl.DataFrame:
    if not symbols or df.is_empty() or "symbol" not in df.columns:
        return df
    return df.filter(pl.col("symbol").is_in(symbols))


def _hfq_anchor_factors(
    factors: pl.DataFrame,
    bars: pl.DataFrame,
    end: date | None,
) -> pl.DataFrame:
    """Per-symbol latest factor not later than the qfq anchor bar.

    A factor gap on the last bar must make only that bar inexact. Requiring an
    exact factor on the latest bar made the anchor null for the whole symbol,
    which silently returned earlier rows at raw prices too.
    """
    if end is not None:
        anchor_bars = bars.filter(pl.col("trade_date") <= end)
        anchor_factors = factors.filter(pl.col("trade_date") <= end)
    else:
        anchor_bars = bars
        anchor_factors = factors

    bar_anchors = anchor_bars.group_by("symbol").agg(
        pl.col("trade_date").max().alias("anchor_date")
    )
    return (
        anchor_factors.join(bar_anchors, on="symbol")
        .filter(pl.col("trade_date") <= pl.col("anchor_date"))
        .sort(["symbol", "trade_date"])
        .group_by("symbol", maintain_order=True)
        .last()
        .select(["symbol", pl.col("factor").alias("hfq_anchor")])
    )


def _with_distribution_multiplier(
    joined: pl.DataFrame,
    config: Config,
    start: date | None,
    end: date | None,
    *,
    read_context: ReadContext | None,
) -> pl.DataFrame:
    """Fold fund cash distributions into ``factor`` for a total-return series.

    A stock's hfq factor already reinvests dividends. The vendor's fund factor
    moves on unit splits only, so for a fund each recorded cash distribution
    is reinvested at the close before its ex-date: the factor is multiplied by
    ``prev_close / (prev_close - cash)`` from that date on. The multiplier is 1
    on each fund's first bar in scope, so its level there equals the hfq level,
    the way ``qfq`` anchors on the last bar. hfq and qfq are unchanged.
    """
    funds = _read_dataset(config, "instruments", read_context=read_context)
    if "asset_type" not in funds.columns:
        return joined
    fund_symbols = set(
        funds.filter(pl.col("asset_type") == "etf").get_column("symbol").to_list()
    ) & set(joined.get_column("symbol").unique().to_list())
    if not fund_symbols:
        return joined
    try:
        actions = _read_dataset(
            config,
            "corporate_actions",
            start=start,
            end=end,
            symbols=sorted(fund_symbols),
            read_context=read_context,
        )
    except ReaderError:
        return joined
    payouts = (
        actions.filter((pl.col("action_type") == "cash_dividend") & (pl.col("cash_dividend") > 0))
        .group_by("symbol", "ex_date")
        .agg(pl.col("cash_dividend").max().alias("_cash"))
    )
    if payouts.is_empty():
        return joined
    ordered = joined.with_row_index("_row").sort("symbol", "trade_date")
    payouts = (
        effective_session(payouts, ordered.select("symbol", "trade_date"))
        .filter(pl.col("effective_session").is_not_null())
        .group_by("symbol", "effective_session")
        .agg(pl.col("_cash").sum())
    )
    with_steps = (
        ordered.with_columns(pl.col("close").shift(1).over("symbol").alias("_prev_close"))
        .join(
            payouts,
            left_on=["symbol", "trade_date"],
            right_on=["symbol", "effective_session"],
            how="left",
        )
        .with_columns(
            pl.when(
                pl.col("_cash").is_not_null()
                & (pl.col("_prev_close") > pl.col("_cash"))
                & pl.col("symbol").is_in(list(fund_symbols))
            )
            .then(pl.col("_prev_close") / (pl.col("_prev_close") - pl.col("_cash")))
            .otherwise(1.0)
            .alias("_step")
        )
        .with_columns(pl.col("_step").cum_prod().over("symbol").alias("_multiplier"))
        .with_columns((pl.col("factor") * pl.col("_multiplier")).alias("factor"))
    )
    return with_steps.sort("_row").drop("_row", "_prev_close", "_cash", "_step", "_multiplier")


# A restructuring suspension can run for months. The last published hfq factor
# still applies to the zero-volume placeholder bars the vendor keeps writing;
# looking back this far covers that without scanning the whole factor history
# on every adjusted load.
_SUSPENDED_FACTOR_LOOKBACK_DAYS = 400

_FACTOR_SCHEMA = {"symbol": pl.Utf8, "trade_date": pl.Date, "factor": pl.Float64}


def _positive_hfq_factors(factors: pl.DataFrame) -> pl.DataFrame:
    """Keep one finite positive stored hfq factor per symbol and session."""

    if factors.is_empty():
        return pl.DataFrame(schema=_FACTOR_SCHEMA)
    frame = factors
    if "adjust_type" in frame.columns:
        frame = frame.filter(pl.col("adjust_type") == STORED_ADJUST_TYPE)
    if not {"symbol", "trade_date", "factor"} <= set(frame.columns):
        return pl.DataFrame(schema=_FACTOR_SCHEMA)
    return (
        frame.select(["symbol", "trade_date", pl.col("factor").cast(pl.Float64)])
        .filter(
            pl.col("factor").is_not_null() & pl.col("factor").is_finite() & (pl.col("factor") > 0)
        )
        .unique(subset=["symbol", "trade_date"], keep="last")
    )


def _forward_filled_factors(gaps: pl.DataFrame, known: pl.DataFrame) -> pl.DataFrame:
    """The last known factor at or before each gap key; unmatched gaps are dropped.

    Forward-fill within each symbol never reads a later session, so a factor
    that changes when trading resumes stays on the resume session.
    """

    if gaps.is_empty() or known.is_empty():
        return pl.DataFrame(schema=_FACTOR_SCHEMA)
    on_dtype = gaps.schema["trade_date"]
    history = (
        pl.concat(
            [
                known.select(
                    "symbol", pl.col("trade_date").cast(on_dtype), pl.col("factor"), _gap=False
                ),
                gaps.with_columns(pl.lit(None, dtype=pl.Float64).alias("factor"), _gap=True),
            ],
            how="diagonal_relaxed",
        )
        .sort(["symbol", "trade_date"])
        .with_columns(pl.col("factor").forward_fill().over("symbol"))
    )
    return history.filter(pl.col("_gap") & pl.col("factor").is_not_null()).drop("_gap")


def _with_carried_suspension_factors(
    bars: pl.DataFrame,
    factors: pl.DataFrame,
    config: Config,
    start: date | None,
    *,
    revision: RevisionRef | None,
    read_context: ReadContext | None,
) -> pl.DataFrame:
    """Stored hfq factors plus the carried factor for unfactored suspended bars.

    The derive carries the last factor onto every suspended session. A lake
    derived before that was fixed can still lack rows for zero-volume
    placeholders, and the next derive only refetches a bounded batch. Until
    then those bars take the last published factor here, before qfq, hfq and
    total_return diverge, so every mode prices the same row the same way. A
    row that actually traded and still has no factor stays missing.
    """

    known = _positive_hfq_factors(factors)
    if (
        "volume" not in bars.columns
        or "bar_time" in bars.columns
        or not {"symbol", "trade_date"} <= set(bars.columns)
    ):
        return known
    gaps = (
        bars.filter(pl.col("volume").is_not_null() & (pl.col("volume") == 0))
        .select("symbol", "trade_date")
        .unique()
    )
    if gaps.is_empty():
        return known
    gaps = gaps.join(
        known.select("symbol", pl.col("trade_date").cast(gaps.schema["trade_date"])),
        on=["symbol", "trade_date"],
        how="anti",
    )
    if gaps.is_empty():
        return known
    carried = _forward_filled_factors(gaps, known)
    unmatched = gaps.join(carried, on=["symbol", "trade_date"], how="anti")
    if not unmatched.is_empty() and isinstance(start, date):
        try:
            earlier = _read_dataset(
                config,
                "adj_factors",
                start=start - timedelta(days=_SUSPENDED_FACTOR_LOOKBACK_DAYS),
                end=start - timedelta(days=1),
                symbols=sorted(unmatched["symbol"].unique().to_list()),
                revision=revision,
                read_context=read_context,
            )
        except ReaderError:
            earlier = pl.DataFrame()
        prior = _positive_hfq_factors(earlier)
        if not prior.is_empty():
            carried = pl.concat(
                [carried, _forward_filled_factors(unmatched, pl.concat([prior, known]))],
                how="diagonal_relaxed",
            )
    if carried.is_empty():
        return known
    return pl.concat(
        [known, carried.with_columns(pl.col("trade_date").cast(known.schema["trade_date"]))],
        how="diagonal_relaxed",
    )


def _apply_adjustment(
    bars: pl.DataFrame,
    config: Config,
    adjust: AdjustType,
    start: date | None,
    end: date | None,
    *,
    strict_adj: bool = False,
    revision: RevisionRef | None = None,
    read_context: ReadContext | None = None,
) -> pl.DataFrame:
    if bars.is_empty():
        return bars

    factors = _read_dataset(
        config,
        "adj_factors",
        start=start,
        end=end,
        revision=revision,
        read_context=read_context,
    )
    if not factors.is_empty():
        factors = factors.filter(pl.col("adjust_type") == STORED_ADJUST_TYPE)
        if factors.is_empty():
            raise ReaderError(
                f"adj_factors has no {STORED_ADJUST_TYPE!r} rows; re-run derive (ADR-0004)"
            )
    # Historical factor diagnostics are advisory and belong to the read
    # receipt (load_with_receipt, MCP, serve), not to every adjusted load.
    # The window can also sit entirely inside a suspension whose last factor
    # was published earlier, so carry before declaring the table empty.
    factors = _with_carried_suspension_factors(
        bars, factors, config, start, revision=revision, read_context=read_context
    )
    if factors.is_empty():
        out = bars.with_columns(
            *[pl.col(c).alias(f"adj_{c}") for c in PRICE_COLS if c in bars.columns],
            pl.lit(False).alias("adj_is_exact"),
        )
        if strict_adj:
            raise MissingAdjustmentError(
                "adj_factors dataset is empty; cannot compute exact adjusted prices"
            )
        logger.warning("adj_factors missing; adj_is_exact=False for all rows")
        return out

    if adjust == "qfq":
        anchors = _hfq_anchor_factors(factors, bars, end)
        factors = factors.join(anchors, on="symbol", how="left")
        factors = factors.with_columns(
            (pl.col("factor") / pl.col("hfq_anchor")).alias("factor")
        ).drop("hfq_anchor")
    elif adjust not in ("hfq", "total_return"):
        raise ReaderError(f"unsupported adjust type {adjust!r}")

    joined = bars.join(
        factors.with_columns(pl.col("trade_date").cast(bars.schema["trade_date"])),
        on=["symbol", "trade_date"],
        how="left",
    )
    if adjust == "total_return":
        joined = _with_distribution_multiplier(
            joined, config, start, end, read_context=read_context
        )
    joined = joined.with_columns(pl.col("factor").is_not_null().alias("adj_is_exact"))
    inexact = joined.filter(~pl.col("adj_is_exact")).height
    if inexact:
        msg = f"{inexact} bar row(s) missing adj_factors for adjust={adjust!r}"
        if strict_adj:
            raise MissingAdjustmentError(msg)
        logger.warning("%s; using factor=1.0 with adj_is_exact=False", msg)
    joined = joined.with_columns(pl.col("factor").fill_null(1.0))
    adj_exprs = [
        (pl.col(c) * pl.col("factor")).alias(f"adj_{c}") for c in PRICE_COLS if c in joined.columns
    ]
    return joined.with_columns(adj_exprs).drop("factor")


def _apply_pit_filters(
    df: pl.DataFrame,
    dataset: str,
    *,
    as_of: date,
    items: list[str] | None,
    all_vintages: bool,
    pit_mode: PitMode,
) -> pl.DataFrame:
    try:
        df = classify_pit_rows(
            df,
            dataset,
            as_of=as_of,
            pit_mode=pit_mode,
        )
    except ValueError as exc:
        raise ReaderError(str(exc)) from exc
    if items and "item_code" in df.columns:
        df = df.filter(pl.col("item_code").is_in(items))
    if df.is_empty():
        return df
    if all_vintages or df.is_empty():
        return df

    # announce_date is in the PK, so a restated fact keeps both its original and
    # its revised row. Filtering alone would return every vintage announced on or
    # before as_of and silently double-count the fact; collapse to the one that
    # was current on that date.
    key = [c for c in PRIMARY_KEYS.get(dataset, []) if c != "announce_date"]
    if not key or not all(c in df.columns for c in key):
        return df
    order = [column for column in ("announce_date", "observed_at", "fetched_at") if column in df]
    if order:
        df = df.sort(order)
    return df.group_by(key, maintain_order=True).last()


def load(
    dataset: str,
    *,
    start: str | date | None = None,
    end: str | date | None = None,
    adjust: AdjustType | None = None,
    universe: UniverseType | None = None,
    profile: UniverseProfileLike | None = None,
    universe_profile: UniverseProfileLike | None = None,
    as_of: str | date | None = None,
    items: list[str] | None = None,
    symbols: list[str] | None = None,
    strict_adj: bool = False,
    strict_universe: bool = False,
    all_vintages: bool = False,
    pit_mode: PitMode | None = None,
    config: Config | None = None,
    data_root: str | Path | None = None,
    revision: RevisionSelection | None = None,
    revision_map: Mapping[str, RevisionRef] | None = None,
    include_otc: bool = False,
) -> pl.DataFrame:
    """Load a curated dataset with optional adjustment, universe, and PIT filters.

    Parameters
    ----------
    dataset:
        Curated dataset name (e.g. ``daily_bars``, ``financial_statement_items``).
    start, end:
        Inclusive date window on the dataset's primary date column.
    adjust:
        ``qfq`` or ``hfq`` — joins stored ``hfq`` ``adj_factors`` and adds
        ``adj_open`` … ``adj_close`` plus ``adj_is_exact``. ``qfq`` is derived
        at query time as ``hfq_factor / hfq_anchor`` (anchor = latest bar date
        in scope); only ``hfq`` is persisted (ADR-0004).
        ``total_return`` (``daily_bars`` only) — hfq plus fund cash
        distributions reinvested at the close before each ex-date. A stock's
        hfq already reinvests dividends, so stocks read the same as ``hfq``;
        a fund's hfq adjusts unit splits only. Its level equals hfq on each
        fund's first bar in scope, so compare returns, not levels, across
        queries with different starts. hfq and qfq keep their meaning.
    include_otc:
        ``daily_bars`` / ``adj_factors`` only. A Beijing security's rows from
        before its exchange start — the later of its listing date and the NEEQ
        select tier's opening, 2020-07-27 — are NEEQ over-the-counter quotes
        under different trading rules, and are left out unless this is
        ``True``. Shanghai and Shenzhen rows are never affected.
    universe:
        ``all_a`` — drop unlisted/delisted rows per day via ``instruments``, and
        drop ST/suspended rows when ``trading_status`` has data for that day.
        ``all_a_sh_sz`` applies the same rules to the explicitly named
        Shanghai/Shenzhen subset and excludes North Exchange symbols; use it
        only when that scope is intentional and record the exclusion in the
        research manifest.
        CDRs (SH 689xxx, e.g. 689009.SH) are excluded: they are depositary
        receipts with no adj-factor source coverage; query them via ``symbols=``
        without a universe if needed. Only valid for ``daily_bars``; passing it
        to any other dataset raises ``ReaderError``.

        **Limitation:** ST/suspension filtering is only research-safe when the
        requested window has complete ``trading_status`` rows and a versioned
        historical ST evidence receipt. Dates before that evidence coverage
        are not filtered in permissive mode; strict research reads fail closed.
    profile / universe_profile:
        Explicit versioned :class:`cnequity.domain.UniverseProfile` name (or
        object). Profiles bind exchange/board, CDR/ETF, ST/suspension,
        delisting and PIT-evidence rules and persist a stable ``scope_hash``.
        A strict profile enables strict evidence checks even when
        ``strict_universe`` is omitted. ``universe_profile`` is a spelling
        compatibility alias; pass only one of the two profile arguments.
    as_of:
        Point-in-time date for ``financial_statement_items``. Keeps only facts
        announced on or before this date, and — because a restatement stores a
        second vintage of the same fact rather than overwriting the first —
        returns the vintage that was current on that date.
        For ``index_constituents``, selects each index's latest whole retained
        snapshot on or before this date; never backdates future membership.
    items:
        ``item_code`` filter for ``financial_statement_items``.
    all_vintages:
        Return every vintage announced on or before ``as_of`` instead of only
        the one current then. For studying revisions (a restatement's size and
        direction is itself a signal); not for cross-sectional screens, where
        multiple vintages of one fact would double-count it.
    pit_mode:
        ``"strict"`` keeps only vintages whose source disclosure,
        publication/availability, and lake observation are all provably no
        later than ``as_of``. Rows from the current historical backfill are
        reconstructed and are excluded. ``"best_effort"`` keeps those rows
        for exploratory work and adds ``pit_is_exact=False``/
        ``pit_quality="reconstructed"``. Omitting the mode (or passing None)
        selects best-effort, including history collected after ``as_of``.
        Choose strict explicitly when the research needs evidence the lake
        held on that date, and record the mode in research manifests.
    symbols:
        Restrict to these symbols when the dataset has a ``symbol`` column.
    strict_universe:
        If true, a supported ``universe`` raises when instruments, daily
        trading-status coverage, or versioned historical ST evidence is missing
        for the requested scope. Use it for research reads; the default remains
        permissive for exploratory queries.
    config, data_root:
        Lake location; when omitted, ``CNE_CONFIG``, then ``configs/cnequity.toml``
        under the working directory, then the per-user config.
        Raises ``ReaderError`` if config or dataset parquet files are missing.
    revision:
        Committed revision number/id for the primary dataset, or a mapping
        of dataset names to revisions. A scalar does not pin dependencies.
    revision_map:
        Per-dataset revisions for data and its dependencies (factors,
        instruments, trading status, calendar). Each required generation is
        selected once for this load; unspecified dependencies select current.
        Missing explicit versions fail closed. Quality receipts, operator
        configuration and software versions are not pinned by this map.
    """
    cfg = resolve_config(config=config, data_root=data_root)
    revision_selection = _merge_revision_selection(revision, revision_map)
    effective_universe, resolved_profile, effective_strict_universe = _resolve_reader_scope(
        universe=universe,
        profile=profile,
        universe_profile=universe_profile,
        strict_universe=strict_universe,
    )
    if dataset not in CURATED_DATASETS | DERIVED_DATASETS:
        raise ReaderError(f"unknown dataset {dataset!r}")
    effective_pit_mode: PitMode = "best_effort" if pit_mode is None else pit_mode
    if effective_pit_mode not in ("strict", "best_effort"):
        raise ReaderError(f"unsupported pit_mode {pit_mode!r}; use 'strict' or 'best_effort'")

    if (effective_universe or resolved_profile) and dataset != "daily_bars":
        raise ReaderError(
            f"universe filter applies to daily_bars only; it is not supported for {dataset}"
        )
    if adjust and dataset == "index_bars":
        raise ReaderError(
            "adjustment applies to per-share prices only; index_bars levels are not adjustable"
        )
    if adjust == "total_return" and dataset != "daily_bars":
        raise ReaderError("adjust='total_return' applies to daily_bars only")

    start_d = _parse_date(start)
    end_d = _parse_date(end)
    as_of_d = _parse_date(as_of)

    dependencies = {dataset}
    if dataset == "flash_news_wire":
        dependencies.add("news_headlines")
    if adjust and dataset in ADJUSTABLE_DATASETS:
        dependencies.add("adj_factors")
    if adjust == "total_return":
        # Fund distributions and which securities are funds are read as well.
        dependencies.update({"corporate_actions", "instruments"})
    if effective_universe:
        dependencies.update({"instruments", "trading_status", "trading_calendar"})
    if dataset in OTC_SCOPED_DATASETS and not include_otc and _has_listing_dates(cfg):
        # Which Beijing rows are NEEQ quotes is read from listing dates.
        dependencies.add("instruments")
    selected_revisions = {
        name: (
            _revision_for_dataset(revision_selection, name, fallback_primary=name == dataset)
            if name == dataset or isinstance(revision_selection, Mapping)
            else None
        )
        for name in dependencies
    }
    read_context = ReadContext.capture(cfg, dependencies, selected_revisions)
    from cnequity.storage.research_inputs import validate_research_policies

    validate_research_policies(
        cfg,
        dependencies,
        resolved_profile.name
        if resolved_profile
        else f"legacy_{effective_universe}"
        if effective_universe
        else None,
    )

    if dataset in PIT_DATASETS:
        if as_of_d is None:
            raise ReaderError(f"{dataset} requires as_of= for point-in-time queries")
        df = _read_dataset(
            cfg,
            dataset,
            symbols=symbols,
            revision=_revision_for_dataset(revision_selection, dataset),
            read_context=read_context,
        )
        df = _apply_pit_filters(
            df,
            dataset,
            as_of=as_of_d,
            items=items,
            all_vintages=all_vintages,
            pit_mode=effective_pit_mode,
        )
        df = _apply_pit_date_range(df, dataset, start_d, end_d)
        df = _apply_symbol_filter(df, symbols)
        sort_cols = [
            c for c in ("announce_date", "symbol", "report_period", "item_code") if c in df.columns
        ]
        return df.sort(sort_cols) if sort_cols else df

    if dataset == "flash_news_wire":
        from cnequity.adapters.eastmoney.news_wire import flash_rows_from_news

        frames: list[pl.DataFrame] = []
        if dataset_has_parquet(read_context.roots["news_headlines"]):
            headlines = _read_dataset(
                cfg, "news_headlines", start=start_d, end=end_d, read_context=read_context
            )
            if not headlines.is_empty():
                frames.append(flash_rows_from_news(headlines))
        if dataset_has_parquet(read_context.roots[dataset]):
            frames.append(
                _read_dataset(cfg, dataset, start=start_d, end=end_d, read_context=read_context)
            )
        if not frames:
            raise ReaderError(
                "flash_news_wire has no legacy revision or news_headlines canonical data"
            )
        df = dedupe_by_primary_key(pl.concat(frames, how="diagonal_relaxed"), dataset)
    else:
        df = _read_dataset(
            cfg,
            dataset,
            start=start_d,
            end=end_d,
            symbols=symbols,
            universe=effective_universe,
            strict_universe=effective_strict_universe,
            revision=_revision_for_dataset(revision_selection, dataset),
            read_context=read_context,
            include_otc=include_otc,
        )
    if resolved_profile is not None and dataset == "daily_bars":
        _require_profile_delisting_evidence(cfg, df, resolved_profile, read_context)

    if dataset == "index_constituents" and as_of_d is not None:
        from cnequity.domain.membership import index_members_as_of

        df = index_members_as_of(df, as_of_d)

    # Intraday datasets join on (symbol, trade_date) like the daily bars do: a
    # corporate action applies to a whole session, so every bar in a day shares
    # that day's factor. Intraday prices are stored unadjusted for the same
    # reason daily ones are — the factor series can be recomputed, a price
    # written adjusted cannot be undone.
    if adjust and dataset in ADJUSTABLE_DATASETS:
        df = _apply_adjustment(
            df,
            cfg,
            adjust,
            start_d,
            end_d,
            strict_adj=strict_adj,
            read_context=read_context,
            # A scalar revision belongs to the primary dataset.  Adjustment
            # factors have their own revision sequence; absent an explicit
            # map, use their current committed generation for backwards
            # compatibility instead of interpreting (say) daily revision 7 as
            # factor revision 7.  Research callers can pin both explicitly.
            revision=(
                _revision_for_dataset(
                    revision_selection,
                    "adj_factors",
                    fallback_primary=False,
                )
                if isinstance(revision_selection, Mapping)
                else None
            ),
        )

    # Intraday rows sort by symbol then timestamp: trade_date alone would leave
    # a session's 240 bars in whatever order the pages arrived, and grouping by
    # symbol first is what every resampling consumer wants.
    #
    # Transaction records need `tick_seq` rather than the timestamp, because
    # the timestamp has no seconds: sorting a session by `trade_time` leaves
    # the twenty records sharing a minute in file order, which is the one thing
    # this dataset exists to preserve.
    if "tick_seq" in df.columns:
        order = ("symbol", "trade_date", "tick_seq")
    elif "bar_time" in df.columns:
        order = ("symbol", "bar_time")
    else:
        order = (DATE_COLUMNS.get(dataset), "symbol")
    sort_cols = [c for c in order if c and c in df.columns]
    if sort_cols:
        df = df.sort(sort_cols)
    return df


def scan(
    dataset: str,
    *,
    start: str | date | None = None,
    end: str | date | None = None,
    symbols: list[str] | None = None,
    config: Config | None = None,
    data_root: str | Path | None = None,
    revision: RevisionSelection | None = None,
    revision_map: Mapping[str, RevisionRef] | None = None,
) -> pl.LazyFrame:
    """Return a LazyFrame over a dataset with hive partition pruning.

    Raw scan for heavy pipelines — no adjustment/universe/PIT semantics
    (use ``load`` for those); date window and symbol filters push down to
    the partition scan.
    """
    cfg = resolve_config(config=config, data_root=data_root)
    revision_selection = _merge_revision_selection(revision, revision_map)
    if dataset not in CURATED_DATASETS | DERIVED_DATASETS:
        raise ReaderError(f"unknown dataset {dataset!r}")
    root = _dataset_root(cfg, dataset)
    if not dataset_has_parquet(
        root,
        dataset=dataset,
        meta_root=cfg.meta_root,
        revision=_revision_for_dataset(revision_selection, dataset),
    ):
        raise ReaderError(_missing_dataset_message(dataset, root, cfg))
    return scan_parquet_root(
        root,
        partition_col=DATE_COLUMNS.get(dataset) or partition_col_for_dataset(dataset),
        start=_parse_date(start),
        end=_parse_date(end),
        symbols=symbols,
        dataset=dataset,
        meta_root=cfg.meta_root,
        revision=_revision_for_dataset(revision_selection, dataset),
    )


def dataset_schema(dataset: str) -> dict[str, pl.DataType]:
    """Column contract (polars dtypes) for a dataset."""
    if dataset not in DATASET_SCHEMAS:
        raise ReaderError(f"unknown dataset {dataset!r}")
    return dict(DATASET_SCHEMAS[dataset])


def list_datasets(
    *,
    config: Config | None = None,
    data_root: str | Path | None = None,
) -> pl.DataFrame:
    """Catalog of all datasets: layer, history mode, coverage, and watermark.

    Uses hive partition directory names and ``meta/state`` watermarks — no
    parquet data is read, so this is cheap even on a 10-year lake.

    ``history_mode`` / ``backfill_source`` / ``coverage_start`` are the
    programmatic available-from contract for research consumers, and
    ``history_horizon_days`` is the ceiling on how far back that contract can
    ever reach (None = no source-imposed limit).
    """
    from cnequity.domain.datasets import history_mode_for, is_dataset_enabled
    from cnequity.storage.state import StateStore

    cfg = resolve_config(config=config, data_root=data_root)
    state = StateStore(cfg.meta_root)
    rows = []
    for name, spec in sorted(DATASETS.items()):
        state_payload = state.get_payload(name)
        root = (cfg.derived_root if spec.layer == "derived" else cfg.curated_root) / name
        has_data = dataset_has_parquet(
            root,
            dataset=name,
            meta_root=cfg.meta_root,
        )
        first_part = last_part = None
        if has_data and spec.partition_col:
            try:
                first_part, last_part = _catalog_coverage_bounds(cfg, name)
            except (OSError, pl.exceptions.PolarsError, ValueError) as exc:
                # The quality audit owns the detailed unreadable-file finding.
                # Keep the catalog endpoint usable and avoid presenting a
                # fabricated coverage bound when a coarse/mixed partition is
                # damaged.
                logger.warning("catalog coverage unavailable for %s: %s", name, exc)
        rows.append(
            {
                "dataset": name,
                "layer": spec.layer,
                # How much the project stands behind it; a best_effort dataset
                # is enabled only when the config opts in.
                "support": spec.support,
                "enabled": is_dataset_enabled(name, cfg),
                "date_col": spec.query_date_col,
                "fetch_semantics": spec.fetch_semantics,
                "history_mode": history_mode_for(spec),
                "backfill_source": spec.backfill_source,
                "pit_quality": spec.pit_quality,
                "pit_storage_columns": list(PIT_STORAGE_COLUMNS) if spec.pit else [],
                # None = unbounded. A number means the source itself serves only
                # that many trading days back, so anything earlier is
                # unreachable rather than merely un-backfilled.
                "history_horizon_days": spec.history_horizon_days,
                "pit": spec.pit,
                "has_data": has_data,
                "coverage_start": first_part,
                "coverage_end": last_part,
                "watermarked": spec.watermark,
                "watermark": state.get_date(name) if spec.watermark else None,
                # Snapshot-only feeds have no PIT watermark.  This capture
                # marker records when the rolling live window was last
                # observed so stale scheduling can recover a missed day
                # without pretending that a future event is historical data.
                "snapshot_date": (
                    state.get_date(name, field="last_snapshot_date")
                    if not spec.watermark and spec.fetch_semantics == "snapshot"
                    else None
                ),
                "revision": state_payload.get("revision"),
                "revision_id": state_payload.get("revision_id"),
                "schema_version": state_payload.get("schema_version"),
                "contract_fingerprint": state_payload.get("contract_fingerprint"),
            }
        )
    return pl.DataFrame(rows)
