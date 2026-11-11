"""Overlay proven fund share conversions on an unmodified vendor series."""

from __future__ import annotations

import math
from datetime import date

import polars as pl

from cnequity.domain.action_sessions import effective_session

SOURCE = "derived_fund_actions"


def fund_share_series(
    bars: pl.DataFrame, vendor: pl.DataFrame, terms: pl.DataFrame, *, tolerance_bps: float = 20.0
) -> tuple[pl.DataFrame, list[dict]]:
    """Correct share-event steps, preserving every other vendor step.

    Input vendor factors must be the original series, never a repaired output.
    Cash distributions do not change the fund unit multiplier. Events inside
    a halt take effect on resumption; distinct conversions compose by product.
    The first observed factor level is retained.
    """
    if (
        vendor.is_empty()
        or vendor.filter(
            pl.col("factor").is_null() | ~pl.col("factor").is_finite() | (pl.col("factor") <= 0)
        ).height
    ):
        raise ValueError("original vendor fund factors missing or invalid")
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
    if series.filter(~pl.col("factor").is_finite() | (pl.col("factor") <= 0)).height:
        raise ValueError("invalid vendor fund factor")
    first = series["trade_date"].min()
    terms = terms.filter(
        (pl.col("_split").fill_null(1.0) != 1.0)
        | (pl.col("_bonus").fill_null(0.0) != 0.0)
        | (pl.col("_transfer").fill_null(0.0) != 0.0)
    )
    if terms.is_empty():
        return series, []
    events = effective_session(
        terms.with_columns(pl.lit("_").alias("symbol")),
        series.with_columns(pl.lit("_").alias("symbol")),
    )
    levels = dict(series.iter_rows())
    closes = dict(bars.select("trade_date", "close").iter_rows())
    dates = series["trade_date"].to_list()
    previous = dict(zip(dates[1:], dates[:-1], strict=True))
    following = dict(zip(dates[:-1], dates[1:], strict=True))
    event_days = set(events["effective_session"].drop_nulls().to_list())
    adjustments = {}
    findings = []
    for key, group in events.partition_by("effective_session", as_dict=True).items():
        day = key[0]
        if day is None or day <= first:
            continue
        expected = 1.0
        error = None
        for row in group.iter_rows(named=True):
            mult = (1.0 + (row.get("_bonus") or 0.0) + (row.get("_transfer") or 0.0)) * (
                row.get("_split") or 1.0
            )
            if row.get("_allotment") or row.get("_allot_cash"):
                error = "fund allotment requires independent reference evidence"
                break
            if row.get("_reference") is not None:
                if group.height != 1 or not (
                    closes.get(previous[day], 0) > 0 and row["_reference"] > 0
                ):
                    error = "ambiguous or unpriced reference"
                    break
                mult = closes[previous[day]] / row["_reference"]
            if not math.isfinite(mult) or mult <= 0:
                error = "invalid share conversion"
                break
            expected *= mult
        if error:
            findings.append({"trade_date": str(day), "reason": error})
            continue
        actual = levels[day] / levels[previous[day]]
        if abs(actual / expected - 1.0) * 10_000 > tolerance_bps:
            # A vendor may date the same split one session early or late.
            # Relocate it only when the recorded terms and the raw price
            # discontinuity agree; adding another step would double it.
            nearby = []
            for neighbor in (previous.get(day), following.get(day)):
                if neighbor in previous and neighbor not in event_days:
                    step = levels[neighbor] / levels[previous[neighbor]]
                    if (
                        abs(step - 1.0) * 10_000 > tolerance_bps
                        and abs(step / expected - 1.0) * 10_000 <= tolerance_bps
                    ):
                        nearby.append((neighbor, step))
            relocation = {}
            if nearby:
                price_step = closes.get(previous[day], 0) / closes[day] if closes[day] > 0 else 0
                if (
                    len(nearby) != 1
                    or nearby[0][0] in adjustments
                    or abs(price_step / expected - 1.0) * 10_000 > tolerance_bps
                ):
                    findings.append(
                        {
                            "trade_date": str(day),
                            "reason": "adjacent vendor step requires date evidence",
                        }
                    )
                    continue
                neighbor, step = nearby[0]
                adjustments[neighbor] = 1.0 / step
                relocation = {"relocated_from": str(neighbor), "price_step": price_step}
            adjustments[day] = expected / actual
            findings.append(
                {
                    "trade_date": str(day),
                    "vendor_step": actual,
                    "expected_step": expected,
                    "correction": expected / actual,
                    **relocation,
                }
            )
    if not adjustments:
        return series, findings
    scale = 1.0
    factors = []
    for day, factor in series.iter_rows():
        scale *= adjustments.get(day, 1.0)
        factors.append(factor * scale)
    return series.with_columns(pl.Series("factor", factors)), findings


def apply_fund_share_factors(
    config, out: pl.DataFrame, *, read_context=None, start=None, end=None
) -> tuple[pl.DataFrame, list[dict]]:
    """Rebuild each touched fund from full raw history before selecting its slice."""
    from cnequity.derive.adj_factors import _action_terms
    from cnequity.domain.canonical import dedupe_lazy_by_primary_key
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
    from cnequity.storage.instrument_catalog import load_curated_instruments
    from cnequity.storage.read_context import read_root

    if out.is_empty():
        return out, []
    instruments = load_curated_instruments(config, read_context)
    if instruments is None or "asset_type" not in instruments.columns:
        return out, []
    funds = instruments.filter(pl.col("asset_type").is_in(["etf", "lof"]))["symbol"].to_list()
    scope = out.filter(pl.col("symbol").is_in(funds) & (pl.col("adjust_type") == "hfq"))
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
    action_symbols = (
        terms.filter(
            (pl.col("_split") != 1.0) | (pl.col("_bonus") != 0.0) | (pl.col("_transfer") != 0.0)
        )["symbol"]
        .unique()
        .to_list()
    )
    symbols = sorted(set(symbols) & set(action_symbols))
    if not symbols:
        return out, []
    root = read_root(config, "daily_bars", read_context)
    bars = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(
                root, partition_col="trade_date", symbols=symbols, traded_only=True, committed=False
            ),
            "daily_bars",
        )
        .select("symbol", "trade_date", "close")
        .collect()
    )
    if bars.is_empty():
        return out, []
    stored_root = read_root(config, "adj_factors", read_context)
    stored = (
        scan_parquet_root(stored_root, partition_col="trade_date", symbols=symbols, committed=False)
        .filter(pl.col("adjust_type") == "hfq")
        .collect()
        if dataset_has_parquet(stored_root, committed=False)
        else pl.DataFrame()
    )
    corrected = []
    findings = []
    for symbol in symbols:
        path = config.meta_root / "adj_factors_cache" / f"{symbol.replace('.', '_')}_hfq.parquet"
        raw = pl.read_parquet(path).select("trade_date", "factor") if path.exists() else None
        if raw is not None and raw.is_empty():
            findings.append({"symbol": symbol, "reason": "original vendor factors unavailable"})
            continue
        if raw is None:
            parts = [scope.filter(pl.col("symbol") == symbol)]
            if not stored.is_empty():
                parts.insert(0, stored.filter(pl.col("symbol") == symbol))
            available = pl.concat(parts, how="diagonal_relaxed")
            sources = (
                set(available["source"].drop_nulls().to_list())
                if "source" in available.columns
                else set(available["_vendor"].drop_nulls().to_list())
                if "_vendor" in available.columns
                else set()
            )
            if sources != {"sina"}:
                findings.append({"symbol": symbol, "reason": "original vendor factors unavailable"})
                continue
            raw = available.unique("trade_date", keep="last").select("trade_date", "factor")
        published = (
            stored.filter(pl.col("symbol") == symbol) if not stored.is_empty() else pl.DataFrame()
        )
        if (start or end) and not published.is_empty() and SOURCE in published["source"].to_list():
            raw = published.select("trade_date", "factor")
        series, events = fund_share_series(
            bars.filter(pl.col("symbol") == symbol),
            raw,
            terms.filter(pl.col("symbol") == symbol),
            tolerance_bps=config.adj_factors_crosscheck_tolerance_bps,
        )
        findings.extend({"symbol": symbol, **event} for event in events)
        from cnequity.derive.stock_factors import preserve_pending_steps

        series = preserve_pending_steps(series, published, events, SOURCE)
        if any("correction" in event for event in events):
            corrected.append(series.with_columns(pl.lit(symbol).alias("symbol")))
    if not corrected:
        return out, findings
    replacement = pl.concat(corrected).rename({"factor": "_fund_factor"})
    # Zero-volume sessions also have stored factors. Carry the last traded
    # level through them, otherwise an unchanged halt row creates a false
    # reverse step between two corrected traded sessions.
    replacement = (
        scope.select("symbol", "trade_date", "adjust_type")
        .sort("symbol", "trade_date")
        .join_asof(
            replacement.sort("symbol", "trade_date"),
            on="trade_date",
            by="symbol",
            strategy="backward",
            check_sortedness=False,
        )
    )
    return out.join(
        replacement, on=["symbol", "trade_date", "adjust_type"], how="left"
    ).with_columns(
        pl.coalesce("_fund_factor", "factor").alias("factor"),
        pl.when(pl.col("_fund_factor").is_not_null())
        .then(pl.lit(SOURCE))
        .otherwise(pl.col("_vendor") if "_vendor" in out.columns else pl.col("source"))
        .alias("_vendor"),
    ).drop("_fund_factor"), findings
