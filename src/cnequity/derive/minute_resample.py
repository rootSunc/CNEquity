"""15m / 30m / 60m bars stored on request, resampled from the lake's 1m and 5m.

Nothing schedules this. The coarser bars are a pure function of minute data the
lake already holds, so by default they are computed at read time
(``cnequity.query.resample_minute_history``); ``cne derive minute_bars_15m``
and its siblings are for users who want them as ordinary datasets — readable by
SQL, the HTTP API and MCP without a Python step.

Each symbol-day is built from 1m when the lake has 1m for it and from 5m
otherwise, and ``resampled_from`` records which. 1m keeps trade-only OHLC; the
vendor's 5m already folds untraded carry quotes into its OHLC, but reaches back
about two years where 1m stops after roughly 95 trading days.

A symbol-day whose bars leave an interval partly filled is skipped and counted
instead of failing the session. Halted names are the ordinary case: the source
returns 239 untraded 1m bars for them, one short of a session. Those are counted
apart as ``halted`` — a symbol-day without a single trade — and so are intraday
suspensions, whose bars start or stop mid-session without a hole (measured on
the lake: every traded-but-short day in two years was one), so that only a hole
inside a traded day reads as a data problem. Any other input
the resampler rejects is isolated to its symbol-day the same way, so one bad
name cannot stop a two-year rebuild halfway.

Staleness is decided by content, not file times. Each written session leaves a
receipt naming the digests of its 1m and 5m partitions, of the code that built
it and of the output; a session is rebuilt when any of them differ. File times
alone would miss a rollback or a restore, both of which bring back older
content under preserved mtimes. A digest is the content of one day's rows
(``storage.published.day_digests``), whatever files hold them, cached per file
so checking rereads only files that changed.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from cnequity.domain.datasets import RESAMPLED_MINUTE_DATASETS
from cnequity.query.resample import incomplete_symbol_days, resample_minute_history

if TYPE_CHECKING:
    from cnequity.config import Config

logger = logging.getLogger(__name__)

FREQUENCY_BY_DATASET = RESAMPLED_MINUTE_DATASETS
INPUTS = ("minute_bars", "minute_bars_5m")
# Why a symbol-day was set aside without being invalid input. Only
# ``incomplete`` is a data problem; the other two are market events.
GAP_REASONS = ("halted", "partial_session", "incomplete")


def _partition_day(directory: Path) -> date | None:
    try:
        return date.fromisoformat(directory.name.split("=", 1)[1])
    except (IndexError, ValueError):
        return None


#: Receipts carrying content digests. Older ones lack the key.
RECEIPT_FORMAT = 2


def _partition_digests(config: Config, dataset: str) -> dict[date, str]:
    """Content digest of each day of *dataset*'s published rows."""
    from cnequity.storage.published import day_digests

    return day_digests(config, dataset)


#: Bump when the stored bars written for the same input change (how a
#: symbol-day picks 1m or 5m, what is set aside). With ``RESAMPLE_VERSION``
#: it decides whether old sessions are rebuilt; a refactor that writes the
#: same rows must leave both alone.
DERIVATION_VERSION = 1


def _derivation_identity() -> str:
    """Changing how bars are built must rebuild what the old code wrote."""
    from cnequity.query.resample import RESAMPLE_VERSION

    return f"resample-v{RESAMPLE_VERSION}/derive-v{DERIVATION_VERSION}"


def _receipt_path(config: Config, dataset: str, day: date) -> Path:
    return config.meta_root / "derivations" / dataset / f"{day.isoformat()}.json"


def _input_days(config: Config) -> dict[date, dict[str, str | None]]:
    """Every session either input holds, with the digest of each input."""
    digests = {name: _partition_digests(config, name) for name in INPUTS}
    days = set().union(*digests.values())
    return {day: {name: digests[name].get(day) for name in INPUTS} for day in days}


def stale_sessions(config: Config, dataset: str) -> list[date]:
    """Sessions never built, or whose inputs, code or output no longer match.

    A later 1m backfill is the case that matters most: a session first built
    from 5m must be rebuilt once 1m arrives for it.
    """
    from cnequity.storage.derivative_evidence import read_json

    derivation = _derivation_identity()
    outputs = _partition_digests(config, dataset)
    stale = []
    for day, inputs in _input_days(config).items():
        receipt = read_json(_receipt_path(config, dataset, day))
        if (
            receipt.get("format") != RECEIPT_FORMAT
            or receipt.get("inputs") != inputs
            or receipt.get("derivation") != derivation
            or receipt.get("output") != outputs.get(day)
        ):
            stale.append(day)
    return sorted(stale)


def resample_backlog(config: Config, dataset: str) -> dict:
    """How far a stored resample trails its minute inputs, for status and the panel.

    ``pending`` counts the sessions a default ``cne derive`` would rebuild:
    nothing reruns these datasets when 1m or 5m change, so without this number
    a reader could not tell current bars from bars a backfill has overtaken.
    """
    return {
        "pending": len(stale_sessions(config, dataset)),
        "input_sessions": len(_input_days(config)),
        "derived": bool(_partition_digests(config, dataset)),
    }


def _session(config: Config, dataset: str, day: date) -> pl.DataFrame:
    from cnequity.domain.canonical import dedupe_by_primary_key
    from cnequity.domain.schemas import DATASET_SCHEMAS
    from cnequity.storage.published import published

    store = published(config, dataset)
    scan = store.scan(store.in_range(day, day))
    if scan is None:
        return pl.DataFrame(schema=DATASET_SCHEMAS[dataset])
    return dedupe_by_primary_key(scan.filter(pl.col("trade_date") == day).collect(), dataset)


def _day_shape(frame: pl.DataFrame, step: int) -> pl.DataFrame:
    """Per symbol-day: did it trade, and are its bars one unbroken run?

    A name suspended for part of the day has no bars at all for the halted
    stretch, so the interval it resumes (or stops) in is only partly filled.
    Its bars are still contiguous; a hole *between* bars is what a missing
    fetch looks like.
    """
    keys = ["symbol", "trade_date"]
    clock = pl.col("bar_time").dt.hour().cast(pl.Int32) * 60 + pl.col("bar_time").dt.minute()
    slot = (
        pl.when(clock <= 690)
        .then((clock - 570) // step)
        .otherwise((clock - 780) // step + 120 // step)
    )
    return (
        frame.with_columns(slot.alias("_slot"))
        .group_by(keys)
        .agg(
            ((pl.col("volume") > 0) | (pl.col("amount") > 0)).any().alias("traded"),
            (pl.col("_slot").max() - pl.col("_slot").min() + 1 == pl.len()).alias("contiguous"),
        )
    )


def resample_session(
    minute_1m: pl.DataFrame, minute_5m: pl.DataFrame, frequency: str
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Resample one session; return the bars and the symbol-days set aside.

    A symbol-day with 1m is judged on its 1m alone, as in
    :func:`resample_minute_history`, so an incomplete 1m day is skipped rather
    than quietly rebuilt from 5m. A skipped day with no trade at all is a halt
    (``reason="halted"``); one that traded only part of the day, with its bars
    unbroken, is an intraday suspension (``"partial_session"``); a hole between
    bars of a traded day is ``"incomplete"``.
    """
    keys = ["symbol", "trade_date"]
    minute_5m = minute_5m.join(minute_1m.select(keys).unique(), on=keys, how="anti")
    shape = pl.concat([_day_shape(minute_1m, 1), _day_shape(minute_5m, 5)])
    skipped = (
        pl.concat(
            [
                incomplete_symbol_days(minute_1m, frequency).with_columns(
                    pl.lit("1m").alias("input")
                ),
                incomplete_symbol_days(minute_5m, frequency).with_columns(
                    pl.lit("5m").alias("input")
                ),
            ]
        )
        .join(shape, on=keys, how="left")
        .select(
            *keys,
            "input",
            pl.when(~pl.col("traded"))
            .then(pl.lit("halted"))
            .when(pl.col("contiguous"))
            .then(pl.lit("partial_session"))
            .otherwise(pl.lit("incomplete"))
            .alias("reason"),
        )
    )
    minute_1m = minute_1m.join(skipped, on=keys, how="anti")
    minute_5m = minute_5m.join(skipped, on=keys, how="anti")
    try:
        return resample_minute_history(minute_1m, minute_5m, frequency), skipped
    except ValueError:
        pass
    # Something other than a gap: find which symbol-days, keep the rest.
    by_symbol = {
        source: {
            symbol: group for (symbol,), group in frame.partition_by("symbol", as_dict=True).items()
        }
        for source, frame in (("1m", minute_1m), ("5m", minute_5m))
    }
    parts, rejected = [], []
    for symbol in sorted(set(by_symbol["1m"]) | set(by_symbol["5m"])):
        fine = by_symbol["1m"].get(symbol, minute_1m.clear())
        coarse = by_symbol["5m"].get(symbol, minute_5m.clear())
        try:
            parts.append(resample_minute_history(fine, coarse, frequency))
        except ValueError as exc:
            source = coarse if fine.is_empty() else fine
            rejected.append(
                {
                    "symbol": symbol,
                    "trade_date": source["trade_date"][0],
                    "input": "5m" if fine.is_empty() else "1m",
                    "reason": str(exc),
                }
            )
    if rejected:
        logger.warning(
            "%s: %d symbol-day(s) rejected, e.g. %s", frequency, len(rejected), rejected[0]
        )
    invalid = pl.DataFrame(rejected, schema=skipped.schema)
    bars = (
        pl.concat(parts, how="vertical_relaxed")
        if parts
        else resample_minute_history(minute_1m.clear(), minute_5m.clear(), frequency)
    )
    return bars, pl.concat([skipped, invalid])


def derive_minute_resample(
    config: Config,
    dataset: str,
    *,
    start: date | None = None,
    end: date | None = None,
    full: bool = False,
) -> dict:
    """Compute and write one of the ``minute_bars_{15,30,60}m`` datasets.

    By default only :func:`stale_sessions` are rebuilt; ``start``/``end``
    pick a window instead, and ``full`` rebuilds every input session. No
    watermark is kept: nothing schedules these datasets, so one would only
    ever read as stale.
    """
    from cnequity.domain.schemas import validate_dataframe, with_provenance
    from cnequity.file_lock import lake_mutation_lock
    from cnequity.storage.atomic import write_json_atomic
    from cnequity.storage.parquet import CuratedWriter
    from cnequity.storage.published import frame_day_digests

    frequency = FREQUENCY_BY_DATASET[dataset]
    inputs = _input_days(config)
    if start is None and end is None and not full:
        sessions = stale_sessions(config, dataset)
        if not sessions:
            return {"rows": 0, "note": f"{dataset} 已是最新：分钟线输入和计算规则都没有变化"}
    else:
        sessions = sorted(
            day for day in inputs if (start is None or day >= start) and (end is None or day <= end)
        )
        if not sessions:
            return {"rows": 0, "note": f"{dataset}：所选窗口内没有 1m 或 5m 数据"}
    derivation = _derivation_identity()
    writer = CuratedWriter(config.derived_root)
    rows = 0
    built = {"1m": 0, "5m": 0}
    skipped = {(reason, source): 0 for reason in GAP_REASONS for source in ("1m", "5m")}
    invalid: list[dict] = []
    holes: list[dict] = []
    written: list[date] = []
    cleared: list[date] = []
    with lake_mutation_lock(config.meta_root, blocking=True):
        for day in sessions:
            bars, rejected = resample_session(
                _session(config, "minute_bars", day),
                _session(config, "minute_bars_5m", day),
                frequency,
            )
            gaps = rejected.filter(pl.col("reason").is_in(GAP_REASONS))
            for reason, source, count in gaps.group_by("reason", "input").len().iter_rows():
                skipped[(reason, source)] += count
            holes.extend(gaps.filter(pl.col("reason") == "incomplete").head(5).to_dicts())
            invalid.extend(rejected.filter(~pl.col("reason").is_in(GAP_REASONS)).to_dicts())
            partition = writer.partition_path(dataset, "trade_date", day.isoformat())
            if bars.is_empty() and not partition.exists():
                output = None
            else:
                if bars.is_empty():
                    # A rebuild with nothing left must not leave the old bars
                    # published. An empty file, not a deletion: a revision
                    # commit is a set of written files, so a removal alone
                    # would never reach readers.
                    cleared.append(day)
                else:
                    for source, count in (
                        bars.select("symbol", "resampled_from")
                        .unique()
                        .group_by("resampled_from")
                        .len()
                    ).iter_rows():
                        built[source] += count
                    rows += bars.height
                    written.append(day)
                frame = validate_dataframe(
                    with_provenance(bars, source="derived", data_version="v1"), dataset
                )
                writer.write_partition(
                    dataset, "trade_date", day.isoformat(), frame, "part-000.parquet"
                )
                output = frame_day_digests(frame, dataset).get(day)
            write_json_atomic(
                _receipt_path(config, dataset, day),
                {
                    "format": RECEIPT_FORMAT,
                    "inputs": inputs.get(day),
                    "derivation": derivation,
                    "output": output,
                },
            )
    for item in (*invalid, *holes):
        item["trade_date"] = str(item["trade_date"])
    return {
        "rows": rows,
        "sessions": len(written),
        "first": str(written[0]) if written else None,
        "last": str(written[-1]) if written else None,
        "symbol_days_from_1m": built["1m"],
        "symbol_days_from_5m": built["5m"],
        "skipped_halted_1m": skipped[("halted", "1m")],
        "skipped_halted_5m": skipped[("halted", "5m")],
        "skipped_partial_session_1m": skipped[("partial_session", "1m")],
        "skipped_partial_session_5m": skipped[("partial_session", "5m")],
        "skipped_incomplete_1m": skipped[("incomplete", "1m")],
        "skipped_incomplete_5m": skipped[("incomplete", "5m")],
        "incomplete_examples": [
            {key: item[key] for key in ("symbol", "trade_date", "input")} for item in holes[:5]
        ],
        "sessions_cleared": len(cleared),
        "skipped_invalid": len(invalid),
        "invalid_examples": invalid[:5],
    }
