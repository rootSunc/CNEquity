"""Rebuild independently supported event corrections from original factors."""

from __future__ import annotations

import math
from datetime import date, timedelta

import polars as pl

from cnequity.domain.action_sessions import effective_session

SOURCE = "derived_stock_actions"
_SHARE_REFORM = (date(2005, 4, 29), date(2007, 12, 31))
# Share counts are exact, but per-holder rounding of consideration shares and
# terms quoted to a few decimals leave a small gap (000048.SZ: 1.332 vs 1.33).
_SHARE_RATIO_TOLERANCE = 0.005
_SHARE_CHANGE_BEFORE = timedelta(days=7)
_SHARE_CHANGE_AFTER = timedelta(days=3)


def preserve_pending_steps(series, published, events, source):
    """A partial rebuild must not retract previously published pending steps."""
    if series.is_empty() or published.is_empty() or "source" not in published.columns:
        return series
    pending = {
        date.fromisoformat(e["trade_date"]) for e in events if "reason" in e and e.get("trade_date")
    }
    if not pending:
        # Nothing pending, nothing to carry: skip the full-history join that
        # every symbol used to pay for on every daily derive.
        return series
    aligned = (
        series.sort("trade_date")
        .join_asof(
            published.select("trade_date", pl.col("factor").alias("_published"), "source").sort(
                "trade_date"
            ),
            on="trade_date",
            strategy="backward",
        )
        .with_columns(
            (pl.col("_published") / pl.col("_published").shift(1)).alias("_old_step"),
            (pl.col("factor") / pl.col("factor").shift(1)).alias("_new_step"),
        )
    )
    for row in aligned.filter(
        pl.col("trade_date").is_in(sorted(pending)) & (pl.col("source") == source)
    ).iter_rows(named=True):
        old, new = row["_old_step"], row["_new_step"]
        if old and new and math.isfinite(old / new) and old / new > 0:
            series = series.with_columns(
                pl.when(pl.col("trade_date") >= row["trade_date"])
                .then(pl.col("factor") * old / new)
                .otherwise(pl.col("factor"))
                .alias("factor")
            )
    return series


def _float_share_ratio(counts: dict[date, float], prior: date, day: date) -> float | None:
    """Float A-share growth across the share changes dated around *day*.

    A share-reform holder ends with exactly this many shares per share held,
    so it settles consideration terms that the two vendors read differently.
    The change is dated the registration day, usually the session before the
    shares list, but sometimes the day after the vendor's ex session
    (600213.SH: 2007-07-06 against 07-05). A long reform halt must not reach
    back to an unrelated change (its 2006-12-07 share transfer).
    """
    start = max(prior, day - _SHARE_CHANGE_BEFORE)
    end = day + _SHARE_CHANGE_AFTER
    before = [d for d in counts if d < start]
    inside = [d for d in counts if start <= d <= end]
    if not before or not inside:
        return None
    return counts[max(inside)] / counts[max(before)]


def _zero(value):
    return 0 if value is None else value


def _event_groups(events: pl.DataFrame) -> list[dict]:
    """Each effective session's actions as plain lists, in first-seen order.

    One aggregation per symbol rather than several tiny Polars operations per
    event: a full history has dozens of events and the daily derive walks
    ~5,000 symbols. Values stay unsummed so the caller's arithmetic, and so
    every finding, is exactly what it was.
    """
    if events.is_empty():
        return []
    columns = ["ex_date", "_dividend", "_bonus", "_transfer", "_allotment", "_allot_cash", "_split"]
    aggregations = [pl.col(name) for name in columns] + [pl.len().alias("height")]
    if "_reference" in events.columns:
        aggregations.append(pl.col("_reference").drop_nulls().unique().alias("refs"))
    if "_term_sources" in events.columns:
        aggregations.append(
            pl.col("_term_sources").explode(empty_as_null=True).drop_nulls().alias("sources")
        )
    groups = events.group_by("effective_session", maintain_order=True).agg(aggregations)
    out = []
    for row in groups.iter_rows(named=True):
        row["refs"] = list(row.get("refs") or [])
        row["sources"] = set(row.get("sources") or [])
        out.append(row)
    return out


def stock_event_series(
    bars: pl.DataFrame,
    vendor: pl.DataFrame,
    terms: pl.DataFrame,
    peer: pl.DataFrame,
    *,
    tolerance_bps: float = 20.0,
    proofs: dict | None = None,
    share_counts: pl.DataFrame | None = None,
) -> tuple[pl.DataFrame, list[dict]]:
    """Correct an event only when terms and independent reference evidence fit.

    Market closes do not prove a step. Missing/ambiguous events remain visible
    and leave other events repairable. Always pass the unmodified vendor series.
    """
    if (
        vendor.is_empty()
        or vendor.filter(
            pl.col("factor").is_null() | ~pl.col("factor").is_finite() | (pl.col("factor") <= 0)
        ).height
    ):
        return vendor.clear(), [{"reason": "original vendor factors missing or invalid"}]
    series = (
        bars.select("trade_date")
        .sort("trade_date")
        .join_asof(
            vendor.select("trade_date", "factor").sort("trade_date"),
            on="trade_date",
            strategy="backward",
        )
        .drop_nulls("factor")
    )
    if series.is_empty() or terms.is_empty():
        return series, []
    dates = series["trade_date"].to_list()
    previous = dict(zip(dates[1:], dates[:-1], strict=True))
    levels = dict(series.iter_rows())
    events = effective_session(
        terms.with_columns(pl.lit("_").alias("symbol")),
        series.with_columns(pl.lit("_").alias("symbol")),
    )
    # Only event sessions and the session before each are ever looked up.
    # Materialising every bar of a full history as a dict cost ~11ms per
    # symbol, on every symbol with any action, on every daily derive.
    wanted = set()
    for day in events["effective_session"].drop_nulls().to_list():
        if day in previous:
            wanted.update((day, previous[day]))
    prices = {
        r["trade_date"]: r
        for r in bars.filter(pl.col("trade_date").is_in(sorted(wanted))).iter_rows(named=True)
    }
    peer_levels = {}
    if (
        not peer.is_empty()
        and not peer.filter(
            pl.col("factor").is_null() | ~pl.col("factor").is_finite() | (pl.col("factor") <= 0)
        ).height
    ):
        peer = peer.filter(
            pl.col("factor").is_not_null() & pl.col("factor").is_finite() & (pl.col("factor") > 0)
        ).sort("trade_date")
        aligned = series.select("trade_date").join_asof(peer, on="trade_date", strategy="backward")
        peer_levels = dict(aligned.iter_rows())
    event_days = set(events["effective_session"].drop_nulls().to_list())
    counts = (
        dict(share_counts.select("change_date", "float_a_shares").iter_rows())
        if share_counts is not None
        else {}
    )
    adjustments: dict[date, float] = {}
    findings = []
    for group in _event_groups(events):
        day = group["effective_session"]
        if day is None or day not in previous:
            continue
        prior = previous[day]
        before = prices[prior]["close"]
        ex_dates = group["ex_date"]
        detail = {"trade_date": str(day), "ex_dates": [str(d) for d in ex_dates]}
        refs = group["refs"]
        if refs and (len(refs) != 1 or group["height"] != 1):
            findings.append({**detail, "reason": "ambiguous event reference"})
            continue
        cash = sum(_zero(v) for v in group["_dividend"])
        shares = sum(
            _zero(b) + _zero(t) + _zero(a)
            for b, t, a in zip(
                group["_bonus"], group["_transfer"], group["_allotment"], strict=True
            )
        )
        allot_cash = sum(_zero(v) for v in group["_allot_cash"])
        split = math.prod(1.0 if v is None else v for v in group["_split"])
        denominator = (1.0 + shares) * split
        reference = (
            refs[0]
            if refs
            else (before - cash + allot_cash) / denominator
            if denominator > 0
            else 0
        )
        if not (before > 0 and reference > 0 and math.isfinite(reference)):
            findings.append({**detail, "reason": "event terms imply an invalid reference"})
            continue
        expected = before / reference
        actual = levels[day] / levels[prior]
        tolerance = tolerance_bps / 10_000 + 0.005 / reference
        if abs(actual / expected - 1.0) <= tolerance:
            continue
        evidence = []
        # Only a disagreeing step needs the guard: most 2005-2007 actions are
        # ordinary dividends the vendor already steps on correctly. Prices
        # cannot settle the rest (exchanges often skipped the ex-right for
        # consideration paid by non-tradable holders); the float share count can.
        if any(_SHARE_REFORM[0] <= d <= _SHARE_REFORM[1] for d in [day, *ex_dates]):
            ratio = _float_share_ratio(counts, prior, day) if denominator > 0 else None
            cash_part = expected / denominator
            detail.update(vendor_step=actual, expected_step=expected, share_ratio=ratio)
            if ratio and abs(denominator / ratio - 1.0) <= _SHARE_RATIO_TOLERANCE:
                evidence.append(("share_count_ratio", expected))
            elif ratio and abs(actual / (ratio * cash_part) - 1.0) <= _SHARE_RATIO_TOLERANCE:
                findings.append({**detail, "confirmed": "vendor step matches float share count"})
                continue
            else:
                findings.append(
                    {**detail, "reason": "share reform requires exceptional event evidence"}
                )
                continue
        by_shares = bool(evidence)
        quote = None if by_shares else prices[day].get("pre_close")
        quote_source = prices[day].get("source") or ""
        if quote is not None and quote > 0 and not quote_source.startswith("derived"):
            evidence.append(("native_pre_close", before / quote))
        action_sources = group["sources"]
        if (
            not by_shares
            and "baostock" not in action_sources
            and peer_levels.get(prior)
            and peer_levels.get(day)
        ):
            # Across a long halt, the peer can contain several intervening
            # events. Use the exchange reference instead of that aggregate.
            if (day - prior).days <= 10:
                evidence.append(("baostock_event", peer_levels[day] / peer_levels[prior]))
        retained = (proofs or {}).get(str(day), {})
        if (
            not evidence
            and retained.get("evidence")
            and abs(retained.get("expected_step", 0) / expected - 1) <= tolerance
        ):
            evidence.extend(
                (f"retained:{name}", value) for name, value in retained["evidence"].items()
            )
        detail.update(
            vendor_step=actual,
            expected_step=expected,
            reference_price=reference,
            evidence={name: value for name, value in evidence},
        )
        if not evidence or any(abs(value / expected - 1.0) > tolerance for _, value in evidence):
            findings.append({**detail, "reason": "independent reference missing or disagrees"})
            continue
        # A nearby matching vendor step can be a date offset. Without stronger
        # date evidence, adding a second step would count the event twice.
        index = dates.index(day)
        neighbors = dates[max(1, index - 2) : min(len(dates), index + 3)]
        if any(
            neighbor != day
            and neighbor not in event_days
            and abs((levels[neighbor] / levels[previous[neighbor]]) / expected - 1.0) <= tolerance
            and abs(levels[neighbor] / levels[previous[neighbor]] - 1.0) > tolerance
            for neighbor in neighbors
        ):
            findings.append({**detail, "reason": "nearby vendor step requires date evidence"})
            continue
        adjustments[day] = expected / actual
        findings.append({**detail, "correction": adjustments[day]})
    scale = 1.0
    rebuilt = []
    for day, factor in series.iter_rows():
        scale *= adjustments.get(day, 1.0)
        rebuilt.append(factor * scale)
    return series.with_columns(pl.Series("factor", rebuilt)), findings


def _by_symbol(frame: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """Split *frame* by symbol once, keeping each group's row order."""
    if frame.is_empty() or "symbol" not in frame.columns:
        return {}
    return {
        key[0]: group
        for key, group in frame.partition_by("symbol", as_dict=True, maintain_order=True).items()
    }


def _reform_share_counts(config, symbols, read_context=None) -> pl.DataFrame:
    """Float A-share counts by change date, through the share-reform window."""
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
    from cnequity.storage.read_context import read_root

    empty = pl.DataFrame(
        schema={"symbol": pl.String, "change_date": pl.Date, "float_a_shares": pl.Float64}
    )
    root = read_root(config, "share_structure", read_context)
    if not dataset_has_parquet(root, committed=False):
        return empty
    scan = scan_parquet_root(
        root, partition_col="change_date", symbols=symbols, end=_SHARE_REFORM[1], committed=False
    )
    # Each change appears once per announcement revision, some without the
    # float A count; the last positive count on a date is that date's level.
    order = [c for c in ("announce_date", "observed_at") if c in scan.collect_schema().names()]
    return (
        scan.filter(pl.col("float_a_shares") > 0)
        .select("symbol", pl.col("change_date").cast(pl.Date), "float_a_shares", *order)
        .collect()
        .sort(["symbol", "change_date", *order])
        .group_by("symbol", "change_date", maintain_order=True)
        .last()
        .select(empty.columns)
    )


def apply_stock_event_factors(
    config,
    out: pl.DataFrame,
    *,
    read_context=None,
    peer=None,
    start: date | None = None,
    end: date | None = None,
):
    """Rebuild full raw histories, then select the requested output slice."""
    from cnequity.derive.adj_factors import _action_terms
    from cnequity.derive.factor_arbitration import _cached_batches, baostock_factor_symbol
    from cnequity.query.canonical import dedupe_lazy_by_primary_key
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
    from cnequity.storage.read_context import read_root
    from cnequity.storage.revisions import RevisionStore

    if out.is_empty():
        return out, []
    scope = out.filter(
        (pl.col("adjust_type") == "hfq")
        & pl.col("symbol").map_elements(baostock_factor_symbol, return_dtype=pl.Boolean)
    )
    if scope.is_empty():
        return out, []
    symbols = scope["symbol"].unique().sort().to_list()
    terms = _action_terms(config, symbols, date.min, date.max, read_context=read_context)
    if start:
        terms = terms.filter(pl.col("ex_date") >= start)
    if end:
        terms = terms.filter(pl.col("ex_date") <= end)
    if terms.is_empty():
        return out, []
    symbols = sorted(set(symbols) & set(terms["symbol"].to_list()))
    root = read_root(config, "daily_bars", read_context)
    bars_scan = dedupe_lazy_by_primary_key(
        scan_parquet_root(
            root, partition_col="trade_date", symbols=symbols, traded_only=True, committed=False
        ),
        "daily_bars",
    )
    if "pre_close" not in bars_scan.collect_schema().names():
        bars_scan = bars_scan.with_columns(pl.lit(None, dtype=pl.Float64).alias("pre_close"))
    bar_columns = ["symbol", "trade_date", "close", "pre_close"]
    if "source" in bars_scan.collect_schema().names():
        bar_columns.append("source")
    bars = bars_scan.select(bar_columns).collect()
    if bars.is_empty():
        return out, []
    share_counts = _reform_share_counts(config, symbols, read_context)
    if peer is None:
        batches, _ = _cached_batches(config, bars["trade_date"].max())
        peer = pl.concat(batches) if batches else pl.DataFrame()
    stored_root = read_root(config, "adj_factors", read_context)
    stored = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(
                stored_root, partition_col="trade_date", symbols=symbols, committed=False
            ),
            "adj_factors",
        )
        .filter(pl.col("adjust_type") == "hfq")
        .collect()
        if dataset_has_parquet(stored_root, committed=False)
        else scope
    )
    revision = RevisionStore(
        config.meta_root, config.curated_root, config.derived_root, create=False
    ).latest("adj_factors")
    proofs = revision.metadata.get("factor_event_proofs", {}) if revision is not None else {}
    replacements, findings = [], []
    # One pass per frame instead of one full-frame filter per symbol: with
    # ~5,000 symbols the filters alone rescanned the whole history ~20,000
    # times a day.
    scope_by = _by_symbol(scope)
    stored_by = _by_symbol(stored)
    bars_by = _by_symbol(bars)
    terms_by = _by_symbol(terms)
    counts_by = _by_symbol(share_counts)
    peer_by = (
        _by_symbol(peer.select("symbol", "trade_date", "factor")) if not peer.is_empty() else {}
    )
    for symbol in symbols:
        rows = scope_by.get(symbol, scope.clear())
        original = (
            config.meta_root / "adj_factors_cache" / f"{symbol.replace('.', '_')}_hfq.parquet"
        )
        published = stored_by.get(symbol, stored.clear())
        sources = set(published["source"].drop_nulls()) if "source" in published.columns else set()
        if sources and not sources.issubset({"sina", SOURCE}):
            continue
        if original.exists():
            raw = pl.read_parquet(original).select("trade_date", "factor")
        elif sources == {"sina"}:
            raw = published.select("trade_date", "factor")
        else:
            findings.append({"symbol": symbol, "reason": "original vendor factors unavailable"})
            continue
        if (start or end) and SOURCE in sources:
            # Scoped repairs preserve already published corrections outside
            # the event window; daily/full rebuilding still uses raw factors.
            raw = published.select("trade_date", "factor")
        symbol_peer = peer_by.get(symbol)
        rebuilt, events = stock_event_series(
            bars_by.get(symbol, bars.clear()),
            raw,
            terms_by.get(symbol, terms.clear()),
            symbol_peer.select("trade_date", "factor")
            if symbol_peer is not None
            else (
                peer.clear().select("trade_date", "factor")
                if not peer.is_empty()
                else pl.DataFrame()
            ),
            tolerance_bps=config.adj_factors_crosscheck_tolerance_bps,
            proofs={
                key.split("|", 1)[1]: value
                for key, value in proofs.items()
                if key.startswith(f"{symbol}|")
            },
            share_counts=counts_by.get(symbol),
        )
        findings.extend({"symbol": symbol, **event} for event in events)
        rebuilt = preserve_pending_steps(rebuilt, published, events, SOURCE)
        if not any("correction" in event for event in events):
            continue
        first = min(date.fromisoformat(e["trade_date"]) for e in events if "correction" in e)
        selected = (
            rows.select("symbol", "trade_date", "adjust_type")
            .sort("trade_date")
            .join_asof(rebuilt.sort("trade_date"), on="trade_date", strategy="backward")
            .filter(pl.col("trade_date") >= first)
            .rename({"factor": "_stock_factor"})
        )
        replacements.append(selected)
    if not replacements:
        return out, findings
    return out.join(
        pl.concat(replacements), on=["symbol", "trade_date", "adjust_type"], how="left"
    ).with_columns(
        pl.coalesce("_stock_factor", "factor").alias("factor"),
        pl.when(pl.col("_stock_factor").is_not_null())
        .then(pl.lit(SOURCE))
        .otherwise(pl.col("_vendor") if "_vendor" in out.columns else pl.col("source"))
        .alias("_vendor"),
    ).drop("_stock_factor"), findings
