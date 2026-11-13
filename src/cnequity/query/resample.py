"""Session-aligned OHLC from actual trades in complete 1m or 5m bars."""

from __future__ import annotations

import polars as pl

#: Bump when the bars built from the same minute input change. Stored
#: ``minute_bars_{15,30,60}m`` record it, so a bump rebuilds every session.
RESAMPLE_VERSION = 1

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

# Input frequency -> output frequencies it can build. Every output is a whole
# multiple of its input, and every boundary falls on the input's session grid.
_TARGETS = {
    "1m": ("5m", "15m", "30m", "60m"),
    "5m": ("15m", "30m", "60m"),
}

# Minutes since midnight, and the opening minute of the half-session a bar is in.
_CLOCK = pl.col("bar_time").dt.hour().cast(pl.Int32) * 60 + pl.col("bar_time").dt.minute()
_BASE = pl.when(_CLOCK < 720).then(570).otherwise(780)


def _interval_end(minutes: int) -> pl.Expr:
    """Closing timestamp of the *minutes*-wide interval holding each bar."""
    label = _BASE + ((_CLOCK - _BASE - 1) // minutes + 1) * minutes
    return pl.col("bar_time").dt.truncate("1d") + pl.duration(minutes=label)


def resample_trade_bars(frame: pl.DataFrame, frequency: str = "5m") -> pl.DataFrame:
    """Aggregate complete 1m or 5m intervals without counting no-trade carry prices.

    1m input builds 5m/15m/30m/60m; 5m input, whose source history is longer,
    builds 15m/30m/60m. A positive amount also identifies a trade: a vendor may
    round a small share count to zero. Entirely untraded intervals retain their last quoted
    close with zero turnover. Partially populated intervals and duplicate
    timestamps fail explicitly; wholly absent intervals remain absent. Input provenance is
    not copied to the derived result, whose lineage remains the input frame.
    """
    if frequency not in _TARGETS["1m"]:
        raise ValueError("frequency must be 5m, 15m, 30m or 60m")
    missing = set(_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"missing minute columns: {sorted(missing)}")
    if frame.is_empty():
        return frame.select(_COLUMNS)
    sources = set(frame["frequency"])
    if frame["frequency"].null_count() or len(sources) != 1 or not sources <= _TARGETS.keys():
        raise ValueError("resampling requires exclusively 1m or exclusively 5m bars")
    source = sources.pop()
    if frequency not in _TARGETS[source]:
        raise ValueError(f"{source} bars resample only to {', '.join(_TARGETS[source])}")
    step = int(source[:-1])
    if frame.select("symbol", "bar_time").n_unique() != frame.height:
        raise ValueError("duplicate symbol/minute keys")
    if frame.select(_COLUMNS).null_count().sum_horizontal().item():
        raise ValueError("null minute fields")
    if not isinstance(frame.schema["bar_time"], pl.Datetime) or frame.schema["bar_time"].time_zone:
        raise ValueError("bar_time must use exchange-local naive datetimes")
    legal = _CLOCK.is_between(570 + step, 690) | _CLOCK.is_between(780 + step, 900)
    invalid = (
        ~legal
        | ((_CLOCK - _BASE) % step != 0)
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
    bars = (
        frame.select(_COLUMNS)
        .sort("symbol", "bar_time")
        .with_columns(
            _interval_end(minutes).alias("_end"),
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
    if result.filter(pl.col("_count") != minutes // step).height:
        raise ValueError(f"incomplete resampling interval; fetch every constituent {source} bar")
    return (
        result.with_columns(
            [pl.col(k).fill_null(pl.col("_carry")) for k in ["open", "high", "low", "close"]]
        )
        .rename({"_end": "bar_time"})
        .with_columns(pl.lit(frequency).alias("frequency"))
        .select(_COLUMNS)
        .sort("symbol", "bar_time")
    )


def incomplete_symbol_days(frame: pl.DataFrame, frequency: str) -> pl.DataFrame:
    """Symbol-days with an interval that holds some, but not all, of its bars.

    These are exactly the symbol-days on which :func:`resample_trade_bars`
    raises for an incomplete interval, so a caller can set them aside rather
    than lose a whole batch to one partly fetched name. Input checks are left
    to the resampler itself.
    """
    keys = ["symbol", "trade_date"]
    if frame.is_empty():
        return frame.select(keys)
    step = int(frame["frequency"][0][:-1])
    minutes = int(frequency[:-1])
    return (
        frame.group_by(*keys, _interval_end(minutes).alias("_end"))
        .len()
        .filter(pl.col("len") != minutes // step)
        .select(keys)
        .unique()
        .sort(keys)
    )


def resample_minute_history(
    minute_1m: pl.DataFrame, minute_5m: pl.DataFrame, frequency: str = "15m"
) -> pl.DataFrame:
    """Resample each symbol-day from 1m where it exists and from 5m otherwise.

    1m keeps trade-only OHLC but has a short source history; 5m reaches back
    about two years with vendor OHLC that already includes carried quotes. A
    symbol-day present in 1m is built from 1m alone, even when its 1m bars are
    incomplete, so that failure surfaces instead of switching convention
    silently. `resampled_from` records which input built each bar.
    """
    if frequency not in _TARGETS["5m"]:
        raise ValueError("frequency must be 15m, 30m or 60m")
    for frame, expected in [(minute_1m, "1m"), (minute_5m, "5m")]:
        if "frequency" in frame.columns and not frame.is_empty():
            if frame["frequency"].null_count() or set(frame["frequency"]) != {expected}:
                raise ValueError(f"minute_{expected} must contain only {expected} bars")
    keys = ["symbol", "trade_date"]
    fine = resample_trade_bars(minute_1m, frequency)
    coarse = resample_trade_bars(
        minute_5m.join(minute_1m.select(keys).unique(), on=keys, how="anti"), frequency
    )
    return pl.concat(
        [
            coarse.with_columns(pl.lit("5m").alias("resampled_from")),
            fine.with_columns(pl.lit("1m").alias("resampled_from")),
        ],
        how="vertical_relaxed",
    ).sort("symbol", "bar_time")
