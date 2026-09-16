"""Session-aligned OHLC from actual trades in complete one-minute bars."""

from __future__ import annotations

import polars as pl

_COLUMNS = [
    "symbol",
    "trade_date",
    "bar_time",
    "frequency",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
]


def resample_trade_bars(frame: pl.DataFrame, frequency: str = "5m") -> pl.DataFrame:
    """Aggregate complete 1m intervals without counting no-trade carry prices.

    A positive amount also identifies a trade: a vendor may round a small
    share count to zero. Entirely untraded intervals retain their last quoted
    close with zero turnover. Partially populated intervals and duplicate
    timestamps fail explicitly; wholly absent intervals remain absent. Input provenance is
    not copied to the derived result, whose lineage remains the input frame.
    """
    if frequency not in {"5m", "15m", "30m", "60m"}:
        raise ValueError("frequency must be 5m, 15m, 30m or 60m")
    missing = set(_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"missing minute columns: {sorted(missing)}")
    if frame.is_empty():
        return frame.select(_COLUMNS)
    if frame["frequency"].null_count() or set(frame["frequency"]) != {"1m"}:
        raise ValueError("resampling requires exclusively 1m bars")
    if frame.select("symbol", "bar_time").n_unique() != frame.height:
        raise ValueError("duplicate symbol/minute keys")
    if frame.select(_COLUMNS).null_count().sum_horizontal().item():
        raise ValueError("null minute fields")
    if not isinstance(frame.schema["bar_time"], pl.Datetime) or frame.schema["bar_time"].time_zone:
        raise ValueError("bar_time must use exchange-local naive datetimes")
    clock = pl.col("bar_time").dt.hour().cast(pl.Int32) * 60 + pl.col("bar_time").dt.minute()
    legal = clock.is_between(571, 690) | clock.is_between(781, 900)
    invalid = (
        ~legal
        | (pl.col("bar_time").dt.second() != 0)
        | (pl.col("bar_time").dt.microsecond() != 0)
        | (pl.col("bar_time").dt.date() != pl.col("trade_date"))
    )
    if frame.filter(invalid).height:
        raise ValueError("minute timestamps must match their trade date and session")
    numeric = ["open", "high", "low", "close", "volume", "amount"]
    if frame.filter(
        pl.any_horizontal([~pl.col(k).is_finite() for k in numeric])
        | pl.any_horizontal([pl.col(k) <= 0 for k in ["open", "high", "low", "close"]])
        | (pl.col("volume") < 0)
        | (pl.col("amount") < 0)
    ).height:
        raise ValueError("invalid minute price or quantity")
    minutes = int(frequency[:-1])
    base = pl.when(clock < 720).then(570).otherwise(780)
    label = base + ((clock - base - 1) // minutes + 1) * minutes
    bars = (
        frame.select(_COLUMNS)
        .sort("symbol", "bar_time")
        .with_columns(
            (pl.col("bar_time").dt.truncate("1d") + pl.duration(minutes=label)).alias("_end"),
            ((pl.col("volume") > 0) | (pl.col("amount") > 0)).alias("_traded"),
        )
    )
    result = bars.group_by("symbol", "trade_date", "_end", maintain_order=True).agg(
        pl.len().alias("_count"),
        pl.col("open").filter(pl.col("_traded")).first().alias("open"),
        pl.col("high").filter(pl.col("_traded")).max().alias("high"),
        pl.col("low").filter(pl.col("_traded")).min().alias("low"),
        pl.col("close").filter(pl.col("_traded")).last().alias("close"),
        pl.col("close").last().alias("_carry"),
        pl.col("volume").sum(),
        pl.col("amount").sum(),
    )
    if result.filter(pl.col("_count") != minutes).height:
        raise ValueError("incomplete resampling interval; fetch every constituent minute")
    return (
        result.with_columns(
            [pl.col(k).fill_null(pl.col("_carry")) for k in ["open", "high", "low", "close"]]
        )
        .rename({"_end": "bar_time"})
        .with_columns(pl.lit(frequency).alias("frequency"))
        .select(_COLUMNS)
        .sort("symbol", "bar_time")
    )
