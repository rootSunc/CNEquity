"""Point-in-time profiles: many fields, many securities, many dates in one read.

``load_profile`` answers "what could be known about each security on each date"
as a long table, one row per ``(symbol, as_of, field)``. Every value says which
date it describes (``field_date``), the earliest date it could have been known
(``visible_from``), where it came from and how firm that claim is
(``pit_quality``). A missing value says why it is missing instead of being
silently absent.

Each source dataset is read once for the whole batch and joined as-of per
symbol, so asking for more dates costs a join, not another read.

Fields
------
``listed``
    1 while listed on ``as_of`` (``instruments``).
``status.trading`` / ``status.st``
    The latest ``trading_status`` row on or before ``as_of``: 1/0 for trading,
    1/0/null for ST (null is unknown, never "not ST").
``shares.total`` / ``shares.float`` / ``shares.free_float``
    ``share_structure``; a count is known once both its change date and its
    announcement have passed.
``holders.count``
    ``shareholder_counts``, known from its announcement.
``fsi:<item_code>``
    The latest report period of a ``financial_statement_items`` line visible on
    ``as_of``; ``field_date`` is the period end.
``industry.<system>.l<1|2|3>``
    The industry code at that level in the latest ``industry_members`` snapshot
    on or before ``as_of``; ``label`` is its name. Always ``snapshot_only``:
    no source publishes when a class was decided.

PIT quality
-----------
``strict`` — from a first-hand source and observed by this lake no later than
``as_of``. ``reconstructed`` — known by its announcement date but recovered
later (backfills, derived rows). ``snapshot_only`` — a dated snapshot with no
publication time. ``pit_mode="strict"`` drops everything but ``strict`` and
says ``no_pit_evidence`` where that is the only reason a value is missing.
``pit_mode="published"`` goes further: a value is visible only from a recorded
publication time (``available_at``), cut at ``cutoff``, and never from an
announcement or ingestion date; industry snapshots, which have none, are then
``no_pit_evidence``. Listing and trading status are session facts and read the
same in every mode.

Missing reasons
---------------
``before_listing``, ``after_delisting``, ``outside_coverage`` (``as_of`` earlier
than anything the dataset holds), ``not_yet_published`` (a record for an
earlier date exists but was not yet visible), ``value_unknown`` (a record
exists and leaves the value unknown, such as ST on a derived status row),
``no_pit_evidence``, and ``no_record``. A value older than the field's ``STALE_DAYS`` is returned with
``missing_reason="stale"`` so a caller can drop it deliberately.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import date, time, timedelta
from pathlib import Path
from typing import Literal

import polars as pl

from cnequity.config import Config
from cnequity.domain.market_time import SHANGHAI_ZONE

SHARE_FIELDS = {
    "shares.total": "total_shares",
    "shares.float": "float_shares",
    "shares.free_float": "free_float_shares",
}
STATUS_FIELDS = ("status.trading", "status.st")
#: A value older than this many days is returned marked ``stale``.
STALE_DAYS = {
    "listed": None,
    "status": 10,
    "shares": None,
    "holders": 400,
    "fsi": 550,
    "industry": 45,
}
#: First-hand sources first; the rest are recovered after the fact.
_SOURCE_RANK = {"eastmoney": 0, "cninfo_original": 1, "cninfo": 1, "exchange": 1, "ths_official": 2}
_RECONSTRUCTED = "backfill|reconstruct|snapshot|derived|cached"

OUT_SCHEMA = {
    "symbol": pl.Utf8,
    "as_of": pl.Date,
    "field": pl.Utf8,
    "value_num": pl.Float64,
    "value_str": pl.Utf8,
    "label": pl.Utf8,
    "field_date": pl.Date,
    "visible_from": pl.Date,
    "source": pl.Utf8,
    "pit_quality": pl.Utf8,
    "age_days": pl.Int64,
    "missing_reason": pl.Utf8,
}


ProfileMode = Literal["best_effort", "strict", "published"]
#: Per-dataset date column for ``canonical=True`` (its historically effective code).
_DATE_COLUMNS = {
    "share_structure": "change_date",
    "shareholder_counts": "count_date",
    "financial_statement_items": "announce_date",
    "industry_members": "as_of_date",
    "trading_status": "trade_date",
}
#: Fields whose rows make no publication claim; every mode reads them alike.
_SESSION_FIELDS = (*("status.trading", "status.st"), "listed")


class ProfileError(ValueError):
    pass


def _family(field: str) -> str:
    if field.startswith("fsi:"):
        return "fsi"
    return field.split(".", 1)[0]


def _check_fields(fields: Sequence[str]) -> None:
    known = {"listed", *STATUS_FIELDS, *SHARE_FIELDS, "holders.count"}
    for field in fields:
        if field in known or (field.startswith("fsi:") and len(field) > 4):
            continue
        parts = field.split(".")
        if (
            len(parts) == 3
            and parts[0] == "industry"
            and parts[1] in {"sw", "eastmoney"}
            and parts[2] in {"l1", "l2", "l3"}
        ):
            continue
        raise ProfileError(f"unknown profile field {field!r}")


def _observed_day(frame: pl.DataFrame) -> pl.Expr:
    """The Shanghai date this lake observed a row (``observed_at``, else ``fetched_at``)."""
    cols = [pl.col(c) for c in ("observed_at", "fetched_at") if c in frame.columns]
    if not cols:
        return pl.lit(None, dtype=pl.Date).alias("_observed")
    stamp = pl.coalesce(cols).dt.convert_time_zone(SHANGHAI_ZONE)
    return stamp.dt.date().alias("_observed")


def _first_hand() -> pl.Expr:
    return ~pl.col("source").fill_null("").str.to_lowercase().str.contains(_RECONSTRUCTED)


def _day(column: str) -> pl.Expr:
    """A timestamp column as its Shanghai date, null-safe."""
    return pl.col(column).dt.convert_time_zone(SHANGHAI_ZONE).dt.date()


def _published_day(frame: pl.DataFrame, cutoff: time | None) -> pl.Expr:
    """The first ``as_of`` date by whose ``cutoff`` a recorded ``available_at`` had passed.

    Published after the cutoff on day D, a row is first visible on D+1; with no
    cutoff, on D itself. Null when the source recorded no publication time.
    """
    if "available_at" not in frame.columns:
        return pl.lit(None, dtype=pl.Date).alias("_published")
    local = pl.col("available_at").dt.convert_time_zone(SHANGHAI_ZONE)
    day = local.dt.date()
    if cutoff is None:
        return day.alias("_published")
    late = local.dt.time() > pl.lit(cutoff)
    return pl.when(late).then(day + timedelta(days=1)).otherwise(day).alias("_published")


def _period_end(period: pl.Expr) -> pl.Expr:
    year = period.str.slice(0, 4).cast(pl.Int32, strict=False)
    quarter = period.str.slice(5, 1).cast(pl.Int32, strict=False)
    month = quarter * 3
    first = pl.date(year, month, 1)
    return first.dt.month_end()


def _candidates(
    config: Config,
    fields: Sequence[str],
    symbols: list[str],
    last: date,
    *,
    cutoff: time | None,
    read_kwargs: dict,
    canonical: bool,
) -> pl.DataFrame:
    """``[symbol, field, visible_from, field_date, value_num, value_str, label, source, _observed, _first_hand, _rank]``."""
    from cnequity.query.reader import ReaderError, load

    wanted = set(symbols)
    if canonical:
        from cnequity.domain.security_identifiers import identifier_aliases

        for symbol in symbols:
            wanted |= identifier_aliases(symbol)

    def read(dataset: str, **kwargs) -> pl.DataFrame:
        try:
            frame = load(dataset, config=config, symbols=sorted(wanted), **read_kwargs, **kwargs)
        except ReaderError:
            return pl.DataFrame()
        if canonical and not frame.is_empty():
            from cnequity.domain.schemas import PRIMARY_KEYS
            from cnequity.domain.security_identifiers import canonicalize_dated_identifiers

            frame = canonicalize_dated_identifiers(
                frame,
                date_col=_DATE_COLUMNS[dataset],
                key_columns=[c for c in PRIMARY_KEYS[dataset] if c in frame.columns],
            )
        return frame

    parts: list[pl.DataFrame] = []
    common = ["symbol", "field", "visible_from", "field_date", "value_num", "value_str", "label"]

    def add(frame: pl.DataFrame) -> None:
        if frame.is_empty():
            return
        frame = frame.with_columns(
            pl.col("source").fill_null(""),
            _first_hand().alias("_first_hand"),
            pl.col("source")
            .fill_null("")
            .replace_strict(_SOURCE_RANK, default=9, return_dtype=pl.Int8)
            .alias("_rank"),
        )
        for name in ("_effective", "_published"):
            if name not in frame.columns:
                frame = frame.with_columns(pl.lit(None, dtype=pl.Date).alias(name))
        parts.append(
            frame.select(
                *common, "source", "_observed", "_effective", "_published", "_first_hand", "_rank"
            ).cast({"value_num": pl.Float64, "value_str": pl.Utf8, "label": pl.Utf8})
        )

    wanted_shares = [f for f in fields if f in SHARE_FIELDS]
    if wanted_shares:
        ss = read("share_structure", as_of=last, pit_mode="best_effort", all_vintages=True)
        if not ss.is_empty():
            base = ss.with_columns(
                pl.max_horizontal("change_date", "announce_date").alias("visible_from"),
                pl.col("change_date").alias("field_date"),
                pl.col("change_date").alias("_effective"),
                _published_day(ss, cutoff),
                _observed_day(ss),
            )
            for field in wanted_shares:
                add(
                    base.select(
                        "symbol",
                        pl.lit(field).alias("field"),
                        "visible_from",
                        "field_date",
                        pl.col(SHARE_FIELDS[field]).alias("value_num"),
                        pl.lit(None, dtype=pl.Utf8).alias("value_str"),
                        pl.lit(None, dtype=pl.Utf8).alias("label"),
                        "source",
                        "_observed",
                        "_effective",
                        "_published",
                    ).filter(pl.col("value_num").is_not_null())
                )

    if "holders.count" in fields:
        hc = read("shareholder_counts", as_of=last, pit_mode="best_effort", all_vintages=True)
        if not hc.is_empty():
            add(
                hc.select(
                    "symbol",
                    pl.lit("holders.count").alias("field"),
                    pl.max_horizontal("count_date", "announce_date").alias("visible_from"),
                    pl.col("count_date").alias("field_date"),
                    pl.col("holder_count").alias("value_num"),
                    pl.lit(None, dtype=pl.Utf8).alias("value_str"),
                    pl.lit(None, dtype=pl.Utf8).alias("label"),
                    "source",
                    _observed_day(hc),
                    pl.col("count_date").alias("_effective"),
                    _published_day(hc, cutoff),
                ).filter(pl.col("value_num").is_not_null())
            )

    items = [f[4:] for f in fields if f.startswith("fsi:")]
    if items:
        fsi = read(
            "financial_statement_items",
            as_of=last,
            pit_mode="best_effort",
            all_vintages=True,
            items=items,
        )
        if not fsi.is_empty():
            fsi = fsi.with_columns(_published_day(fsi, cutoff))
            avail = pl.max_horizontal(pl.col("announce_date"), pl.col("_published"))
            add(
                fsi.select(
                    "symbol",
                    pl.concat_str([pl.lit("fsi:"), pl.col("item_code")]).alias("field"),
                    avail.alias("visible_from"),
                    _period_end(pl.col("report_period")).alias("field_date"),
                    pl.col("item_value").alias("value_num"),
                    pl.col("report_period").alias("value_str"),
                    pl.lit(None, dtype=pl.Utf8).alias("label"),
                    "source",
                    _observed_day(fsi),
                    "_published",
                ).filter(pl.col("value_num").is_not_null())
            )

    industry = [f for f in fields if f.startswith("industry.")]
    if industry:
        im = read("industry_members", end=last)
        for field in industry:
            _, system, level = field.split(".")
            if im.is_empty() or f"{level}_code" not in im.columns:
                continue
            add(
                im.filter(
                    (pl.col("classification_system") == system)
                    & pl.col(f"{level}_code").is_not_null()
                ).select(
                    "symbol",
                    pl.lit(field).alias("field"),
                    pl.col("as_of_date").alias("visible_from"),
                    pl.col("as_of_date").alias("field_date"),
                    pl.lit(None, dtype=pl.Float64).alias("value_num"),
                    pl.col(f"{level}_code").alias("value_str"),
                    pl.col(f"{level}_name").alias("label"),
                    "source",
                    _observed_day(im),
                )
            )

    status = [f for f in fields if f in STATUS_FIELDS]
    if status:
        ts = read("trading_status", end=last)
        if not ts.is_empty():
            from cnequity.domain.trading_status import risk_warning_value_expr

            ts = ts.with_columns(
                risk_warning_value_expr(ts.columns).alias("_rw"), _observed_day(ts)
            )
            if "status.trading" in status:
                add(
                    ts.select(
                        "symbol",
                        pl.lit("status.trading").alias("field"),
                        pl.col("trade_date").alias("visible_from"),
                        pl.col("trade_date").alias("field_date"),
                        pl.col("is_trading").cast(pl.Float64).alias("value_num"),
                        pl.col("status").alias("value_str"),
                        pl.lit(None, dtype=pl.Utf8).alias("label"),
                        "source",
                        "_observed",
                    )
                )
            if "status.st" in status:
                add(
                    ts.select(
                        "symbol",
                        pl.lit("status.st").alias("field"),
                        pl.col("trade_date").alias("visible_from"),
                        pl.col("trade_date").alias("field_date"),
                        pl.col("_rw").cast(pl.Float64).alias("value_num"),
                        pl.lit(None, dtype=pl.Utf8).alias("value_str"),
                        pl.lit(None, dtype=pl.Utf8).alias("label"),
                        "source",
                        "_observed",
                    )
                )

    if not parts:
        return pl.DataFrame()
    return pl.concat(parts, how="vertical_relaxed").drop_nulls(["symbol", "visible_from"])


def _quality(field: str, mode: ProfileMode) -> pl.Expr:
    if field.startswith("industry."):
        return pl.lit("snapshot_only")
    if mode == "published" and field not in _SESSION_FIELDS:
        return pl.lit("strict")
    strict = (
        pl.col("_first_hand")
        & pl.col("_observed").is_not_null()
        & (pl.col("_observed") <= pl.col("as_of"))
    )
    return pl.when(strict).then(pl.lit("strict")).otherwise(pl.lit("reconstructed"))


def _asof(grid: pl.DataFrame, cand: pl.DataFrame, field: str, mode: ProfileMode) -> pl.DataFrame:
    """Latest visible value per ``(symbol, as_of)`` for one field, with quality.

    The value known on a date is the one for the latest ``field_date`` among
    rows visible by then, and of those the latest published (a restatement).
    A per-symbol sweep marks each row that becomes that best value when it
    appears; an as-of join then picks the state on each requested date, so no
    row is expanded against every date.
    """
    rows = cand.filter(pl.col("field") == field)
    snapshot = field.startswith("industry.")
    if mode == "published" and field not in _SESSION_FIELDS:
        # Known only from a recorded publication time, and not before the
        # date it took effect; announcement and ingestion dates never stand in.
        rows = rows.filter(pl.col("_published").is_not_null() & ~pl.lit(snapshot)).with_columns(
            pl.max_horizontal("_effective", "_published").alias("visible_from")
        )
    elif mode == "strict" and not snapshot:
        # Strictly known only from the day this lake itself observed it.
        rows = rows.filter(pl.col("_first_hand") & pl.col("_observed").is_not_null()).with_columns(
            pl.max_horizontal("visible_from", "_observed").alias("visible_from")
        )
    if rows.is_empty():
        return pl.DataFrame()
    # One row per (symbol, field_date, visible_from): first-hand, then source rank.
    rows = rows.sort(
        ["symbol", "field_date", "visible_from", "_first_hand", "_rank", "_observed"],
        descending=[False, False, False, True, False, False],
        nulls_last=True,
    ).unique(["symbol", "field_date", "visible_from"], keep="first", maintain_order=True)
    key = pl.col("field_date").cast(pl.Int64) * 100_000 + pl.col("visible_from").cast(pl.Int64)
    best = (
        rows.with_columns(key.alias("_key"))
        .sort("symbol", "visible_from", "_key")
        .with_columns(pl.col("_key").cum_max().over("symbol").alias("_best"))
        .filter(pl.col("_key") == pl.col("_best"))
        .unique(["symbol", "visible_from"], keep="last", maintain_order=True)
        .sort("visible_from")
    )
    picked = (
        grid.sort("as_of")
        .join_asof(
            best,
            left_on="as_of",
            right_on="visible_from",
            by="symbol",
            strategy="backward",
            check_sortedness=False,
        )
        .filter(pl.col("visible_from").is_not_null())
    )
    return picked.with_columns(_quality(field, mode).alias("pit_quality")).select(
        "symbol",
        "as_of",
        pl.lit(field).alias("field"),
        "value_num",
        "value_str",
        "label",
        "field_date",
        "visible_from",
        "source",
        "pit_quality",
    )


def _listed(config: Config, grid: pl.DataFrame, read_kwargs: dict) -> pl.DataFrame:
    from cnequity.query.reader import ReaderError, load

    try:
        inst = load("instruments", config=config, **read_kwargs)
    except ReaderError:
        inst = pl.DataFrame(
            schema={"symbol": pl.Utf8, "list_date": pl.Date, "delist_date": pl.Date}
        )
    return grid.join(
        inst.select("symbol", "list_date", "delist_date"), on="symbol", how="left"
    ).with_columns(
        pl.when(pl.col("list_date").is_null())
        .then(pl.lit(None, dtype=pl.Utf8))
        .when(pl.col("as_of") < pl.col("list_date"))
        .then(pl.lit("before_listing"))
        .when(pl.col("delist_date").is_not_null() & (pl.col("as_of") >= pl.col("delist_date")))
        .then(pl.lit("after_delisting"))
        .alias("_life")
    )


def load_profile(
    fields: Iterable[str],
    symbols: Iterable[str],
    as_of_dates: Iterable[date],
    *,
    pit_mode: ProfileMode = "best_effort",
    cutoff: time | None = None,
    canonical: bool = False,
    config: Config | None = None,
    data_root: str | Path | None = None,
    revision_map: Mapping[str, object] | None = None,
) -> pl.DataFrame:
    """One row per ``(symbol, as_of, field)``; see the module docstring.

    ``cutoff`` is the Shanghai time of day by which a recorded publication
    time must have passed to count on ``as_of`` (``time(15)`` for a close
    signal); without it the whole day counts. ``canonical`` re-keys each
    source row onto its historically effective code before matching, as
    ``canonicalize_dated_identifiers`` does. ``data_root`` and
    ``revision_map`` go to every read, so a pinned build reads pinned data.
    """
    from cnequity.query.reader import resolve_config

    if pit_mode not in ("strict", "best_effort", "published"):
        raise ProfileError(f"unsupported pit_mode {pit_mode!r}")
    fields = list(dict.fromkeys(fields))
    symbols = sorted(set(symbols))
    dates = sorted(set(as_of_dates))
    _check_fields(fields)
    if not fields or not symbols or not dates:
        return pl.DataFrame(schema=OUT_SCHEMA)
    cfg = config or resolve_config(data_root=data_root)
    read_kwargs = {"revision_map": dict(revision_map)} if revision_map else {}
    grid = pl.DataFrame({"symbol": symbols}).join(
        pl.DataFrame({"as_of": dates}, schema={"as_of": pl.Date}), how="cross"
    )
    life = _listed(cfg, grid, read_kwargs)
    cand = _candidates(
        cfg,
        [f for f in fields if f != "listed"],
        symbols,
        dates[-1],
        cutoff=cutoff,
        read_kwargs=read_kwargs,
        canonical=canonical,
    )
    out: list[pl.DataFrame] = []
    for field in fields:
        if field == "listed":
            got = life.filter(pl.col("list_date").is_not_null()).select(
                "symbol",
                "as_of",
                pl.lit(field).alias("field"),
                pl.col("_life").is_null().cast(pl.Float64).alias("value_num"),
                pl.lit(None, dtype=pl.Utf8).alias("value_str"),
                pl.lit(None, dtype=pl.Utf8).alias("label"),
                pl.col("list_date").alias("field_date"),
                pl.col("list_date").alias("visible_from"),
                pl.lit("instruments").alias("source"),
                pl.lit("strict").alias("pit_quality"),
            )
        else:
            got = _asof(grid, cand, field, pit_mode) if not cand.is_empty() else pl.DataFrame()
        full = (
            life.select("symbol", "as_of", "_life")
            .join(
                got
                if not got.is_empty()
                else pl.DataFrame(
                    schema={
                        k: v
                        for k, v in OUT_SCHEMA.items()
                        if k not in ("age_days", "missing_reason")
                    }
                ),
                on=["symbol", "as_of"],
                how="left",
            )
            .with_columns(pl.lit(field).alias("field"))
        )
        out.append(_explain(full, cand, field, pit_mode))
    return (
        pl.concat(out, how="vertical_relaxed")
        .select(list(OUT_SCHEMA))
        .cast(OUT_SCHEMA)
        .sort("symbol", "as_of", "field")
    )


def _explain(full: pl.DataFrame, cand: pl.DataFrame, field: str, mode: ProfileMode) -> pl.DataFrame:
    """Add ``age_days`` and ``missing_reason`` to one field's rows."""
    rows = cand.filter(pl.col("field") == field) if not cand.is_empty() else pl.DataFrame()
    stale = STALE_DAYS.get(_family(field))
    full = full.with_columns(
        (pl.col("as_of") - pl.col("field_date")).dt.total_days().alias("age_days")
    )
    if rows.is_empty():
        flags = full.with_columns(
            pl.lit(None, dtype=pl.Date).alias("_first_date"),
            pl.lit(None, dtype=pl.Date).alias("_first_visible"),
            pl.lit(None, dtype=pl.Date).alias("_first"),
        )
    else:
        first_seen = rows.select(pl.col("visible_from").min()).item()
        per_symbol = rows.group_by("symbol").agg(
            pl.col("field_date").min().alias("_first_date"),
            pl.col("visible_from").min().alias("_first_visible"),
        )
        flags = full.join(per_symbol, on="symbol", how="left").with_columns(
            pl.lit(first_seen).alias("_first")
        )
    flags = flags.with_columns(
        (pl.col("_first_date") <= pl.col("as_of")).fill_null(False).alias("_earlier"),
        (pl.col("_first_visible") <= pl.col("as_of")).fill_null(False).alias("_any_visible"),
    )
    if field != "listed":
        # Outside a listed life there is no profile to give, only the reason.
        outside = pl.col("_life").is_not_null()
        flags = flags.with_columns(
            pl.when(outside).then(None).otherwise(pl.col(c)).alias(c)
            for c in (
                "value_num",
                "value_str",
                "label",
                "field_date",
                "visible_from",
                "source",
                "pit_quality",
                "age_days",
            )
        )
    reason = (
        pl.when(pl.col("value_num").is_not_null() | pl.col("value_str").is_not_null())
        .then(
            pl.when(pl.lit(stale is not None) & (pl.col("age_days") > (stale or 0)))
            .then(pl.lit("stale"))
            .otherwise(pl.lit(None, dtype=pl.Utf8))
        )
        .when(pl.col("_life").is_not_null() & (pl.lit(field) != "listed"))
        .then(pl.col("_life"))
        # A record for the date exists but carries no value (e.g. ST unknown).
        .when(pl.col("field_date").is_not_null())
        .then(pl.lit("value_unknown"))
        .when(pl.col("_first").is_null() | (pl.col("as_of") < pl.col("_first")))
        .then(pl.lit("outside_coverage"))
        .when(
            pl.lit(mode in ("strict", "published") and field not in _SESSION_FIELDS)
            & pl.col("_any_visible").fill_null(False)
        )
        .then(pl.lit("no_pit_evidence"))
        .when(pl.col("_earlier").fill_null(False))
        .then(pl.lit("not_yet_published"))
        .otherwise(pl.lit("no_record"))
    )
    if field == "listed":
        reason = (
            pl.when(pl.col("value_num").is_null())
            .then(pl.lit("no_record"))
            .otherwise(pl.lit(None, dtype=pl.Utf8))
        )
    return flags.with_columns(reason.alias("missing_reason"))
