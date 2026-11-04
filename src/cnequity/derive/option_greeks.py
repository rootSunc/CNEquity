"""Implied volatility and Greeks for every option settlement in the lake.

Inputs, per option row of ``option_bars``:

- **Price** — the settlement. Every listed strike has one, traded or not,
  where a close only exists when something traded.
- **Underlying** — for commodity options, the same session's settlement of
  the futures contract they are written on. CFFEX index options are written
  on the spot index, whose forward carries dividends nobody publishes, so the
  forward is read off the options themselves: at each expiry,
  ``F = K + e^{rT}(C − P)`` at the three strikes where calls and puts are
  closest in price, and the median taken (``forward_source = parity``).
- **Expiry** — ``option_contracts.expiry_date``; time to expiry is calendar
  days / 365, with the expiry session itself counted as one day. On that
  session the settlement is the exercise value (zero out of the money), so it
  gets ``status = expiry_day`` and no IV.
- **Rate** — ``macro_indicators`` ``shibor_3m`` on or before the session.
  Where the lake has none (its history is short) a flat 2% is used and
  ``rate_source`` says so; futures options discount only the premium, so the
  rate moves their Greeks little.
- **Model** — Black-76 for European options, Barone-Adesi–Whaley for
  American ones (``domain.futures_products`` says which is which).

The exchanges publish their own delta, and some an IV, in ``option_bars``.
Those are kept untouched; this table is the lake's own, one method across
every exchange, so values can be compared.
"""

from __future__ import annotations

import math
from datetime import date

import numpy as np
import polars as pl

from cnequity.domain.option_pricing import greeks, implied_vol

FALLBACK_RATE = 0.02
RATE_INDICATOR = "shibor_3m"
PARITY_STRIKES = 3

__all__ = ["compute_option_greeks", "derive_option_greeks"]


#: Index and ETF options settle on no traded future; their forward comes from
#: put-call parity (CFFEX index options, SSE/SZSE ETF options).
_PARITY_EXCHANGES = ["CFE", "SH", "SZ"]


def _parity_forwards(frame: pl.DataFrame) -> dict[tuple[str, date], float]:
    """Forward per (series, session) from put-call parity on settlements."""
    out: dict[tuple[str, date], float] = {}
    calls = frame.filter(pl.col("option_type") == "C").select(
        "series", "trade_date", "strike", pl.col("settle").alias("c"), "rate", "t"
    )
    puts = frame.filter(pl.col("option_type") == "P").select(
        "series", "trade_date", "strike", pl.col("settle").alias("p")
    )
    pairs = calls.join(puts, on=["series", "trade_date", "strike"], how="inner").drop_nulls(
        ["c", "p"]
    )
    for (series, day), group in pairs.group_by(["series", "trade_date"]):
        nearest = group.with_columns((pl.col("c") - pl.col("p")).abs().alias("_gap")).sort("_gap")
        near = nearest.head(PARITY_STRIKES)
        forwards = [
            row["strike"] + math.exp(row["rate"] * row["t"]) * (row["c"] - row["p"])
            for row in near.iter_rows(named=True)
        ]
        out[(series, day)] = float(np.median(forwards))
    return out


def compute_option_greeks(
    options: pl.DataFrame,
    *,
    contracts: pl.DataFrame,
    futures: pl.DataFrame,
    rates: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """One row per option row with a settlement: IV and Greeks, or a status."""
    if options.is_empty():
        return pl.DataFrame()
    frame = options.select(
        "symbol", "trade_date", "exchange", "underlying_symbol", "option_type", "strike", "settle"
    ).join(
        contracts.with_columns(
            pl.when(pl.col("dates_basis") == "exchange")
            .then(pl.col("expiry_date"))
            .otherwise(None)
            .alias("expiry_date")
        ).select("symbol", "expiry_date", "exercise_style", "expiry_month")
        if "dates_basis" in contracts.columns
        else contracts.select("symbol", "expiry_date", "exercise_style", "expiry_month"),
        on="symbol",
        how="left",
    )
    frame = frame.with_columns(
        # Use the actual expiry, not the underlying delivery month, for parity.
        (pl.col("underlying_symbol") + "@" + pl.col("expiry_date").cast(pl.Utf8)).alias("series"),
        (pl.col("expiry_date") - pl.col("trade_date")).dt.total_days().alias("_days"),
        ((pl.col("expiry_date") - pl.col("trade_date")).dt.total_days())
        .clip(lower_bound=1)
        .truediv(365.0)
        .alias("t"),
    )
    if rates is not None and not rates.is_empty():
        frame = (
            frame.sort("trade_date")
            .join_asof(
                rates.select(
                    pl.col("obs_date").alias("trade_date"), (pl.col("value") / 100).alias("rate")
                ).sort("trade_date"),
                on="trade_date",
                strategy="backward",
            )
            .with_columns(
                pl.when(pl.col("rate").is_null())
                .then(pl.lit("fallback_constant"))
                .otherwise(pl.lit(RATE_INDICATOR))
                .alias("rate_source"),
                pl.col("rate").fill_null(FALLBACK_RATE),
            )
        )
    else:
        frame = frame.with_columns(
            pl.lit(FALLBACK_RATE).alias("rate"), pl.lit("fallback_constant").alias("rate_source")
        )
    settles = futures.select(
        pl.col("symbol").alias("underlying_symbol"), "trade_date", pl.col("settle").alias("_fut")
    )
    frame = frame.join(settles, on=["underlying_symbol", "trade_date"], how="left")
    forwards = _parity_forwards(
        frame.filter(pl.col("exchange").is_in(_PARITY_EXCHANGES) & pl.col("t").is_not_null())
    )
    if forwards:
        frame = frame.join(
            pl.DataFrame(
                {
                    "series": [key[0] for key in forwards],
                    "trade_date": [key[1] for key in forwards],
                    "_parity": list(forwards.values()),
                }
            ),
            on=["series", "trade_date"],
            how="left",
        )
    else:
        frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias("_parity"))
    frame = frame.with_columns(
        pl.when(pl.col("exchange").is_in(_PARITY_EXCHANGES))
        .then(pl.col("_parity"))
        .otherwise(pl.col("_fut"))
        .alias("underlying_price"),
        pl.when(pl.col("exchange").is_in(_PARITY_EXCHANGES))
        .then(pl.lit("parity"))
        .otherwise(pl.lit("future_settle"))
        .alias("forward_source"),
        pl.when(pl.col("exercise_style") == "european")
        .then(pl.lit("black76"))
        .when(pl.col("exercise_style") == "american")
        .then(pl.lit("baw"))
        .otherwise(pl.lit("unknown"))
        .alias("model"),
    )
    parts = []
    for (model,), group in frame.group_by(["model"]):
        parts.append(_solve(group, model))
    return pl.concat(parts, how="diagonal_relaxed").sort("trade_date", "symbol")


def _solve(group: pl.DataFrame, model: str) -> pl.DataFrame:
    n = group.height
    status = np.full(n, "ok", dtype=object)
    price = group["settle"].to_numpy(allow_copy=True).astype(float)
    f = group["underlying_price"].to_numpy(allow_copy=True).astype(float)
    k = group["strike"].to_numpy(allow_copy=True).astype(float)
    t = group["t"].to_numpy(allow_copy=True).astype(float)
    r = group["rate"].to_numpy(allow_copy=True).astype(float)
    is_call = (group["option_type"] == "C").to_numpy()
    days = group["_days"].to_numpy(allow_copy=True).astype(float)
    status[np.isnan(t)] = "no_expiry"
    # The expiry session prices exercise, not time: settlements sit at
    # intrinsic value (zero out of the money), so there is no IV to solve for.
    status[(days <= 0) & (status == "ok")] = "expiry_day"
    status[np.isnan(f) & (status == "ok")] = "no_underlying"
    status[(np.isnan(price) | (price <= 0)) & (status == "ok")] = "no_price"
    if model == "unknown":
        status[status == "ok"] = "no_exercise_style"
    usable = status == "ok"
    iv = np.full(n, np.nan)
    out = {name: np.full(n, np.nan) for name in ("delta", "gamma", "vega", "theta", "rho")}
    if usable.any():
        sigma, solved = implied_vol(
            model, price[usable], f[usable], k[usable], t[usable], r[usable], is_call[usable]
        )
        status[usable] = solved
        iv[usable] = sigma
        ok = usable.copy()
        ok[usable] = solved == "ok"
        if ok.any():
            values = greeks(model, f[ok], k[ok], t[ok], r[ok], iv[ok], is_call[ok])
            for name, array in values.items():
                out[name][ok] = array
    return group.select(
        "symbol", "trade_date", "underlying_symbol", "underlying_price", "forward_source",
        pl.col("t").alias("time_to_expiry"), "rate", "rate_source", "model",
    ).with_columns(
        # NaN is "no answer"; the lake stores that as null, never as NaN.
        pl.Series("iv", iv, nan_to_null=True),
        *[pl.Series(name, array, nan_to_null=True) for name, array in out.items()],
        pl.Series("status", status.astype(str)),
    )  # fmt: skip


#: The inputs a derive reads, pinned once so its receipts name exactly the
#: generations the Greeks were computed from.
_INPUTS = ("option_bars", "futures_bars", "option_contracts", "macro_indicators")
_TERM_COLUMNS = ("expiry_date", "exercise_style", "expiry_month", "dates_basis")


def _pin_inputs(config):
    from cnequity.storage.read_context import ReadContext

    return ReadContext.capture(config, _INPUTS, {})


def _curated(config, dataset: str, context=None, **filters) -> pl.DataFrame | None:
    from cnequity.storage.read_context import read_root

    root = read_root(config, dataset, context)
    files = list(root.glob("**/*.parquet")) if root.exists() else []
    if not files:
        return None
    from cnequity.query.canonical import dedupe_by_primary_key
    from cnequity.query.parquet_scan import scan_parquet_files

    scan = scan_parquet_files(files)
    for column, (lo, hi) in filters.items():
        scan = scan.filter(pl.col(column).is_between(lo, hi))
    return dedupe_by_primary_key(scan.collect(), dataset)


def _newest_mtime(directory) -> float | None:
    times = [p.stat().st_mtime for p in directory.glob("*.parquet")] if directory.is_dir() else []
    return max(times) if times else None


def _partition_day(directory) -> date | None:
    try:
        return date.fromisoformat(directory.name.split("=", 1)[1])
    except (IndexError, ValueError):
        return None


def _dependency_identity(config) -> dict:
    """The code every session's Greeks come from."""
    from pathlib import Path

    from cnequity.domain import option_pricing
    from cnequity.storage.derivative_evidence import fingerprint

    return {
        "model": fingerprint(Path(option_pricing.__file__)),
        "derivation": fingerprint(Path(__file__)),
    }


def _session_dependencies(config, context) -> dict[date, dict]:
    """Per session: the contract terms it trades and the rate it applies.

    Fingerprinting the whole contract and rate tables made every session
    stale whenever a contract was listed or a rate observed — every day — so
    the full history (12.8M rows, ~50 min) was recomputed daily. A session's
    Greeks read only its own contracts' terms and the last rate on or before
    it, so those are what its receipt records.
    """
    from cnequity.query.parquet_scan import scan_parquet_files
    from cnequity.storage.read_context import read_root

    options_root = read_root(config, "option_bars", context)
    files = list(options_root.glob("**/*.parquet")) if options_root.exists() else []
    if not files:
        return {}
    traded = scan_parquet_files(files).select("symbol", "trade_date").unique().collect()
    contracts = _curated(config, "option_contracts", context)
    columns = [c for c in _TERM_COLUMNS if contracts is not None and c in contracts.columns]
    if contracts is not None and columns:
        traded = traded.join(
            contracts.select("symbol", *columns).unique(subset=["symbol"], keep="last"),
            on="symbol",
            how="left",
        )
    term_hash = pl.struct("symbol", *columns).hash(seed=0) % 1_000_000_007
    terms = traded.group_by("trade_date").agg(term_hash.sum().alias("terms"), pl.len().alias("n"))
    rates = _curated(config, "macro_indicators", context)
    if rates is not None and not rates.is_empty() and "indicator_id" in rates.columns:
        observed = (
            rates.filter(pl.col("indicator_id") == RATE_INDICATOR)
            .select(pl.col("obs_date").alias("trade_date"), "value")
            .with_columns(pl.col("trade_date").alias("rate_date"))
            .sort("trade_date")
        )
        terms = terms.sort("trade_date").join_asof(observed, on="trade_date", strategy="backward")
    else:
        terms = terms.with_columns(
            pl.lit(None, dtype=pl.Date).alias("rate_date"),
            pl.lit(None, dtype=pl.Float64).alias("value"),
        )
    shared = _dependency_identity(config)
    return {
        row["trade_date"]: {
            **shared,
            "terms": [int(row["terms"]), int(row["n"])],
            "rate": None
            if row["rate_date"] is None
            else [row["rate_date"].isoformat(), float(row["value"])],
        }
        for row in terms.iter_rows(named=True)
    }


def _receipt(config, day):
    return config.meta_root / "derivatives" / "greeks_dependencies" / f"{day}.json"


def stale_sessions(config, context=None) -> list[date]:
    """Sessions whose Greeks are missing or older than the bars they come from.

    An option session reads its own ``option_bars`` partition and the month's
    ``futures_bars`` partition (futures are stored by month). Either being
    rewritten after the Greeks were — a backfill of an earlier window, a
    refilled exchange day — makes the session stale. A watermark alone misses
    exactly those: it only ever moves forward.
    """
    from cnequity.storage.read_context import read_root

    context = context or _pin_inputs(config)
    options_root = read_root(config, "option_bars", context)
    futures_root = read_root(config, "futures_bars", context)
    greeks_root = read_root(config, "option_greeks")
    if not options_root.is_dir():
        return []
    from cnequity.storage.derivative_evidence import fingerprint, read_json

    dependencies = _session_dependencies(config, context)
    month_identity: dict[str, str] = {}
    month_mtime: dict[str, float | None] = {}
    stale: list[date] = []
    for directory in options_root.glob("trade_date=*"):
        day = _partition_day(directory)
        source = _newest_mtime(directory)
        if day is None or source is None:
            continue
        month = day.strftime("%Y-%m")
        if month not in month_mtime:
            month_mtime[month] = _newest_mtime(futures_root / f"trade_date={month}")
        source = max(source, month_mtime[month] or 0.0)
        derived = _newest_mtime(greeks_root / f"trade_date={day.isoformat()}")
        if month not in month_identity:
            month_identity[month] = fingerprint(futures_root / f"trade_date={month}")
        receipt = read_json(_receipt(config, day))
        matches = (
            receipt.get("dependencies") == dependencies.get(day)
            and receipt.get("options") == fingerprint(directory)
            and receipt.get("futures") == month_identity[month]
            and receipt.get("output") == fingerprint(greeks_root / f"trade_date={day}")
        )
        if derived is None or derived < source or not matches:
            stale.append(day)
    return sorted(stale)


def derive_option_greeks(
    config, *, start: date | None = None, end: date | None = None, full: bool = False
) -> dict:
    # Not under the lake lock: the inputs are pinned generations, and only the
    # write section mutates the lake. Holding the lock for the whole compute
    # stalled every other job's compact for ~50 minutes a day.
    return _derive_option_greeks(config, start=start, end=end, full=full)


def _derive_option_greeks(
    config, *, start: date | None = None, end: date | None = None, full: bool = False
) -> dict:
    """Compute and write ``option_greeks``.

    Rows do not depend on one another across sessions, so recomputing only
    the :func:`stale_sessions` is exact; ``start``/``end`` pick a window
    instead, and ``full`` recomputes everything (for a model or input change,
    such as a longer rate history).
    """
    from cnequity.domain.market_time import shanghai_today
    from cnequity.domain.schemas import validate_dataframe, with_provenance
    from cnequity.file_lock import lake_mutation_lock
    from cnequity.storage.parquet import CuratedWriter
    from cnequity.storage.state import StateStore

    state = StateStore(config.meta_root)
    context = _pin_inputs(config)
    sessions: list[date] | None = None
    if start is None and not full:
        sessions = [d for d in stale_sessions(config, context) if end is None or d <= end]
        if not sessions:
            return {
                "rows": 0,
                "note": "option_greeks is current: input, model and output fingerprints match",
            }
        start, end = sessions[0], sessions[-1]
    start = start or date(2017, 1, 1)
    end = end or shanghai_today()
    if start > end:
        return {"rows": 0, "note": f"empty window {start}..{end}"}
    options = _curated(config, "option_bars", context, trade_date=(start, end))
    if options is not None and sessions is not None:
        options = options.filter(pl.col("trade_date").is_in(sessions))
    contracts = _curated(config, "option_contracts", context)
    if options is None or options.is_empty() or contracts is None:
        return {"rows": 0, "note": f"no option_bars/option_contracts in {start}..{end}"}
    futures = _curated(config, "futures_bars", context, trade_date=(start, end))
    rates = _curated(config, "macro_indicators", context)
    if rates is not None:
        rates = rates.filter(pl.col("indicator_id") == RATE_INDICATOR)
    dependencies = _session_dependencies(config, context)
    frame = compute_option_greeks(
        options,
        contracts=contracts,
        futures=futures
        if futures is not None
        else pl.DataFrame(schema={"symbol": pl.Utf8, "trade_date": pl.Date, "settle": pl.Float64}),
        rates=rates,
    )
    frame = validate_dataframe(
        with_provenance(frame, source="derived", data_version="v1"), "option_greeks"
    )
    writer = CuratedWriter(config.derived_root)
    with lake_mutation_lock(config.meta_root, blocking=True):
        for (day,), group in frame.partition_by("trade_date", as_dict=True).items():
            writer.write_partition(
                "option_greeks", "trade_date", day.isoformat(), group, "part-000.parquet"
            )
            from cnequity.storage.atomic import write_json_atomic
            from cnequity.storage.derivative_evidence import fingerprint
            from cnequity.storage.read_context import read_root

            write_json_atomic(
                _receipt(config, day),
                {
                    "dependencies": dependencies.get(day),
                    "options": fingerprint(
                        read_root(config, "option_bars", context) / f"trade_date={day}"
                    ),
                    "futures": fingerprint(
                        read_root(config, "futures_bars", context) / f"trade_date={day:%Y-%m}"
                    ),
                    "output": fingerprint(
                        config.derived_root / "option_greeks" / f"trade_date={day}"
                    ),
                },
            )
        # Rewriting an earlier window must not pull the watermark back.
        latest = frame["trade_date"].max()
        previous = state.get_date("option_greeks")
        state.set_date("option_greeks", latest if previous is None else max(previous, latest))
    counts = frame["status"].value_counts().sort("status")
    return {
        "rows": frame.height,
        "sessions": frame["trade_date"].n_unique(),
        "status": dict(counts.iter_rows()),
        "first": str(frame["trade_date"].min()),
        "last": str(frame["trade_date"].max()),
    }
