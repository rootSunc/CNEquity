"""The tools an agent gets, and what they return.

Six tools rather than one per dataset. An agent picks from a flat list on every
turn, so 39 dataset tools would spend most of the context window on names it
will not call and still leave it guessing which one answers the question. These
are cut by *question shape* instead — describe, resolve, read bars, read
fundamentals, read anything else, aggregate — and the dataset becomes an
argument.

Everything here is a plain function over a ``Config``: no protocol types, no
stdio. ``protocol.py`` is what turns them into JSON-RPC, and the tests exercise
this module directly.

**Row payloads are columnar** (``columns`` + ``rows``) rather than a list of
objects. The same 200 rows cost roughly a third as many tokens, which is the
difference between an agent reading a quarter of results and reading all of it.

**Provenance is summarised, not repeated.** Every curated row carries
``source`` / ``data_version`` / ``fetched_at``; emitting them per row would
roughly triple a bar payload to restate the same three values 200 times. The
response reports the distinct sources instead, so "where did this come from"
stays answerable without paying for it on every row.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import polars as pl

from ashare_lake.config import Config
from ashare_lake.domain.datasets import DATASETS, TIER_LABELS
from ashare_lake.mcp_server import live
from ashare_lake.query.reader import ReaderError, load

# What a tool returns before the caller says otherwise. Low enough that a
# careless call cannot blow the context window, and every truncated response
# says so explicitly (see `_frame_payload`) rather than looking complete.
DEFAULT_LIMIT = 200
MAX_LIMIT = 2000

# Per-row provenance, folded into a summary instead of repeated. Kept as a tuple
# rather than read from the schema because these three are the contract every
# curated dataset shares (docs/datasets/catalog.md), not an accident of one.
PROVENANCE_COLS = ("source", "data_version", "fetched_at")

BAR_DATASETS = ("daily_bars", "index_bars", "minute_bars", "minute_bars_5m")


class ToolError(ValueError):
    """A bad call, phrased for the agent that made it.

    Raised instead of letting a ``ReaderError`` or a polars exception through:
    the agent's next move depends on being told *which argument* to change, and
    a stack trace tells it to give up instead.
    """


def _scalar(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _frame_payload(
    df: pl.DataFrame,
    *,
    limit: int,
    offset: int = 0,
    include_provenance: bool = False,
) -> dict:
    """Columnar page over *df*, with the honest total beside it.

    ``truncated`` is not decoration. An agent that receives 200 of 4,300 rows
    and is told only about the 200 will average them and report the number as
    the market's. The total and the flag are what let it choose ``run_sql``
    instead.
    """
    total = df.height
    sources = sorted(set(df["source"].to_list())) if "source" in df.columns else []
    if not include_provenance:
        df = df.drop([c for c in PROVENANCE_COLS if c in df.columns])

    page = df.slice(offset, limit)
    payload: dict[str, Any] = {
        "columns": page.columns,
        "rows": [[_scalar(v) for v in row] for row in page.iter_rows()],
        "returned": page.height,
        "total": total,
        "offset": offset,
        "truncated": offset + page.height < total,
    }
    if sources:
        payload["sources"] = sources
    if payload["truncated"]:
        payload["note"] = (
            f"{total} rows matched, {page.height} returned. Raise `limit`, page with "
            "`offset`, or use `run_sql` — do not treat this page as the full result."
        )
    return payload


def _clamp_limit(limit: int | None) -> int:
    if limit is None:
        return DEFAULT_LIMIT
    try:
        value = int(limit)
    except (TypeError, ValueError) as exc:
        raise ToolError(f"limit must be an integer, got {limit!r}") from exc
    return max(1, min(value, MAX_LIMIT))


def _symbols(value: Any) -> list[str] | None:
    if value in (None, [], ""):
        return None
    if isinstance(value, str):
        # Agents hand back a comma string about as often as a list, and failing
        # on it teaches nothing — the intent is unambiguous either way.
        value = [part.strip() for part in value.split(",")]
    if not isinstance(value, list):
        raise ToolError(f"symbols must be a list of strings, got {type(value).__name__}")
    return [str(s).strip() for s in value if str(s).strip()]


class LakeEmpty(ToolError):
    """The lake holds nothing for this dataset — distinct from a bad argument.

    Only this case may fall through to a live fetch. A malformed `as_of` or an
    unknown symbol must keep failing, or live mode would turn every mistake into
    a plausible answer from a different source.
    """


def _read(config: Config, dataset: str, **kwargs: Any) -> pl.DataFrame:
    # config is threaded through explicitly rather than left to load()'s
    # auto-detection: the fallback resolves ./configs/ashare-lake.toml from the
    # working directory, and an MCP server is started by a client from whatever
    # directory it happens to be in. Getting that wrong reads a different lake
    # and answers confidently from it.
    try:
        return load(dataset, config=config, **kwargs)
    except ReaderError as exc:
        if "no parquet data" in str(exc):
            raise LakeEmpty(str(exc)) from exc
        raise ToolError(str(exc)) from exc


def _live_only(config: Config, tool: str, reason: str) -> None:
    """Refuse a tool that live mode cannot serve honestly, and say why.

    Silence or an empty result would read as "this did not happen"; the agent
    has to know the difference between no data and no lake.
    """
    raise ToolError(
        f"{tool} needs a lake — {reason} "
        f"Live mode serves only {' and '.join(live.SUPPORTED)}. "
        "Build one with `asl init` (or `asl demo` for a 5-symbol sample)."
    )


# ---------------------------------------------------------------------------
# describe_lake


# The rules a correct answer depends on, stated once per session rather than
# hoped for. An agent that has not been told prices are stored unadjusted will
# read `close` across a 10-for-1 split and report a 90% crash; one that has not
# been told which datasets are snapshot-only will read three days of `fund_flow`
# as a trend. Both are confident, both are wrong, and neither is visible in the
# column names — so they go in the one tool every session starts with.
_CONTRACT = [
    "Prices in `daily_bars` / `minute_bars*` are UNADJUSTED as stored. Pass "
    "adjust='hfq' (or 'qfq') to any bar query that compares prices across time, "
    "or a split will read as a crash. Returned adj_* columns carry "
    "`adj_is_exact` — false means no factor was found and 1.0 was used.",
    "`financial_statement_items` is point-in-time: `as_of` is required and "
    "filters on announce_date, so a backtest sees only what was public then. "
    "Restatements are kept as separate vintages, not overwritten.",
    "history_mode='snapshot_only' means the source serves today's page and "
    "nothing else. Rows accumulate one day at a time from when this lake "
    "started collecting; there is no honest deeper history and no backfill for "
    "it. Do not present such a series as if it reached back further.",
    "history_horizon_days, when set, is the vendor's retention, not this "
    "lake's backlog: an earlier window returns nothing and cannot be filled.",
    "universe='all_a' drops unlisted/delisted names per day and ST/suspended "
    "ones where trading_status covers that day. Without it, a cross-sectional "
    "screen includes names that were not tradable.",
    "coverage_start is where THIS lake's data begins, which is usually later "
    "than the source's history. An empty result inside coverage is a real gap; "
    "outside it just means nothing was backfilled that far.",
]


def describe_lake(config: Config, *, include_empty: bool = False) -> dict:
    """The map an agent needs before its first query: what is here, how far back, and the口径."""
    from ashare_lake.query.reader import list_datasets

    catalog = list_datasets(config=config)
    rows = []
    for row in catalog.iter_rows(named=True):
        if not include_empty and not row["has_data"]:
            continue
        spec = DATASETS.get(row["dataset"])
        rows.append(
            {
                "dataset": row["dataset"],
                "tier": spec.tier if spec else None,
                "tier_label": TIER_LABELS.get(spec.tier) if spec else None,
                "date_col": row["date_col"],
                "history_mode": row["history_mode"],
                "history_horizon_days": row["history_horizon_days"],
                "pit": row["pit"],
                "coverage_start": _scalar(row["coverage_start"]),
                "coverage_end": _scalar(row["coverage_end"]),
                "watermark": _scalar(row["watermark"]),
                "has_data": row["has_data"],
            }
        )

    populated = [r for r in rows if r["has_data"]]
    contract = list(_CONTRACT)
    if live.enabled(config):
        # Stated first, because it changes how every other line should be read.
        contract.insert(
            0,
            "LIVE MODE IS ON. Where this lake holds nothing, resolve_symbol and "
            "unadjusted daily_bars are fetched from the vendor on demand and NOT "
            "stored; those responses carry origin='live' and a warning. Every "
            "other tool refuses rather than answering from a source that cannot "
            "honour adjustment, universe or point-in-time. A lake is what makes "
            "the rest of this contract true.",
        )
    return {
        "data_root": str(config.data_root),
        "live_mode": live.enabled(config),
        "contract": contract,
        "datasets": rows,
        "summary": {
            "with_data": len(populated),
            "registered": len(DATASETS),
            "snapshot_only": sorted(
                r["dataset"] for r in populated if r["history_mode"] == "snapshot_only"
            ),
            "point_in_time": sorted(r["dataset"] for r in populated if r["pit"]),
        },
    }


# ---------------------------------------------------------------------------
# resolve_symbol


def resolve_symbol(config: Config, *, query: str, limit: int | None = None) -> dict:
    """Name or partial code to a lake symbol.

    An agent is asked about 茅台, not about 600519.SH, and a guessed suffix is
    a silently empty result rather than an error. Delisted names are returned
    too, flagged — they are exactly what a survivorship-aware question is
    about, and dropping them here would quietly reintroduce the bias the lake
    exists to remove.
    """
    text = str(query or "").strip()
    if not text:
        raise ToolError("query must be a non-empty name fragment or code")
    limit = _clamp_limit(limit)

    origin = "lake"
    try:
        df = _read(config, "instruments")
    except LakeEmpty:
        if not live.enabled(config):
            raise
        origin = "live"
        df = live.instruments(config)
    code = text.split(".")[0].upper()
    matched = df.filter(
        pl.col("symbol").str.to_uppercase().str.starts_with(code)
        | pl.col("name").fill_null("").str.contains(text, literal=True)
    )
    # Live names first: a query matching both a listed stock and its delisted
    # predecessor should lead with the one that trades today. Guarded like the
    # projection below, because a live security master is a current roster and
    # a vendor that omits the column should degrade to plain code order rather
    # than fail the lookup.
    order = [pl.col("symbol")]
    if "delist_date" in matched.columns:
        order.insert(0, pl.col("delist_date").is_not_null())
    matched = matched.sort(order)
    out = matched.select(
        [
            c
            for c in ("symbol", "name", "exchange", "asset_type", "list_date", "delist_date")
            if c in matched.columns
        ]
    )
    payload = _frame_payload(out, limit=limit)
    payload["query"] = text
    payload["origin"] = origin
    if origin == "live":
        payload["warning"] = (
            live.LIVE_WARNING + " The current security master has no delisted "
            "names in it at all, so a delisted code will simply not be found."
        )
    if payload["total"] == 0:
        payload["note"] = (
            f"No instrument matches {text!r}. Symbols are '<code>.<EX>' "
            "(600519.SH, 000001.SZ, 920982.BJ). Beijing exchange codes were "
            "renumbered to 920xxx — an old 43x/83x/87x code will not match."
        )
    return payload


# ---------------------------------------------------------------------------
# query_bars


def query_bars(
    config: Config,
    *,
    dataset: str = "daily_bars",
    symbols: Any = None,
    start: str | None = None,
    end: str | None = None,
    adjust: str | None = None,
    universe: str | None = None,
    limit: int | None = None,
    offset: int = 0,
    include_provenance: bool = False,
) -> dict:
    """Price bars with the adjustment and universe contract applied.

    Separate from ``query_dataset`` because ``adjust`` and ``universe`` are the
    whole point: they are the two arguments whose absence produces a plausible
    wrong answer rather than an error.
    """
    if dataset not in BAR_DATASETS:
        raise ToolError(
            f"query_bars handles {', '.join(BAR_DATASETS)}; for {dataset!r} use query_dataset"
        )
    if adjust not in (None, "qfq", "hfq"):
        raise ToolError(f"adjust must be 'qfq', 'hfq' or omitted, got {adjust!r}")
    if universe not in (None, "all_a"):
        raise ToolError(f"universe must be 'all_a' or omitted, got {universe!r}")

    origin = "lake"
    try:
        df = _read(
            config,
            dataset,
            start=start,
            end=end,
            symbols=_symbols(symbols),
            adjust=adjust,
            universe=universe,
        )
    except LakeEmpty:
        if not live.enabled(config):
            raise
        if dataset != "daily_bars":
            _live_only(config, dataset, "the quote protocol serves daily bars here.")
        if adjust or universe:
            raise ToolError(
                f"live mode cannot honour {'adjust' if adjust else 'universe'}: "
                "adjustment factors and trading_status are datasets this lake "
                "derives, not fields on a vendor's bar. Drop the argument to get "
                "raw prices, or build a lake — an unadjusted series across a "
                "split is wrong in a way the numbers do not show."
            ) from None
        origin = "live"
        try:
            df = live.daily_bars(config, symbols=_symbols(symbols), start=start, end=end)
        except live.LiveUnavailable as exc:
            raise ToolError(str(exc)) from exc
    payload = _frame_payload(
        df, limit=_clamp_limit(limit), offset=offset, include_provenance=include_provenance
    )
    payload["dataset"] = dataset
    payload["adjust"] = adjust
    payload["origin"] = origin
    if origin == "live":
        payload["warning"] = live.LIVE_WARNING
        return payload
    if adjust is None and not df.is_empty():
        payload["warning"] = (
            "Unadjusted prices. Any comparison across an ex-dividend or split "
            "date is wrong without adjust='hfq' or 'qfq'."
        )
    elif adjust and "adj_is_exact" in df.columns:
        inexact = int(df.filter(~pl.col("adj_is_exact")).height)
        if inexact:
            payload["warning"] = (
                f"{inexact} of {df.height} rows had no adjustment factor and use "
                "1.0 (adj_is_exact=false). Treat those prices as unadjusted."
            )
    return payload


# ---------------------------------------------------------------------------
# query_fundamentals


def query_fundamentals(
    config: Config,
    *,
    as_of: str | None = None,
    symbols: Any = None,
    items: Any = None,
    all_vintages: bool = False,
    limit: int | None = None,
    offset: int = 0,
) -> dict:
    """Point-in-time financial statement items as known on *as_of*.

    ``as_of`` has no default on purpose. Defaulting it to today would answer
    every historical question with today's knowledge — the exact look-ahead this
    dataset is stored to prevent — and the agent would have no way to notice.
    """
    if not as_of:
        raise ToolError(
            "as_of is required (YYYY-MM-DD): this dataset is point-in-time, and "
            "without a cutoff the answer would include figures announced after "
            "the date being studied. Use today's date for a current view."
        )
    try:
        df = _read(
            config,
            "financial_statement_items",
            as_of=as_of,
            symbols=_symbols(symbols),
            items=_symbols(items),
            all_vintages=all_vintages,
        )
    except LakeEmpty:
        if live.enabled(config):
            _live_only(
                config,
                "query_fundamentals",
                "a vendor returns today's view of a restated figure, so there is "
                "no honest as_of. Answering a 2018 question with 2026 knowledge "
                "is the exact look-ahead this dataset is stored to prevent.",
            )
        raise
    payload = _frame_payload(df, limit=_clamp_limit(limit), offset=offset)
    payload["origin"] = "lake"
    payload["as_of"] = as_of
    payload["semantics"] = (
        "Every row was publicly announced on or before as_of; for a restated "
        "fact only the vintage current on that date is returned "
        "(all_vintages=true returns them all)."
    )
    return payload


# ---------------------------------------------------------------------------
# query_dataset


def query_dataset(
    config: Config,
    *,
    dataset: str,
    start: str | None = None,
    end: str | None = None,
    symbols: Any = None,
    limit: int | None = None,
    offset: int = 0,
    include_provenance: bool = False,
) -> dict:
    """Read any registered dataset by date window and symbol.

    The escape hatch that keeps the tool list at six: fund flows, margin
    balances, dragon-tiger records, unlock schedules and everything else share
    one shape — filter by date, filter by symbol, page.
    """
    if dataset not in DATASETS:
        raise ToolError(
            f"unknown dataset {dataset!r} — call describe_lake for the list this lake holds"
        )
    spec = DATASETS[dataset]
    if spec.pit:
        raise ToolError(
            f"{dataset} is point-in-time; use query_fundamentals so an as_of cutoff is applied"
        )
    if dataset in BAR_DATASETS:
        raise ToolError(f"{dataset} is a bar dataset; use query_bars so adjustment is applied")

    try:
        df = _read(config, dataset, start=start, end=end, symbols=_symbols(symbols))
    except LakeEmpty:
        if live.enabled(config):
            _live_only(
                config,
                dataset,
                "each of these comes from its own adapter with its own pagination "
                "and quality checks, and a live one-shot would be a different "
                "series wearing the same column names.",
            )
        raise
    payload = _frame_payload(
        df, limit=_clamp_limit(limit), offset=offset, include_provenance=include_provenance
    )
    payload["dataset"] = dataset
    payload["origin"] = "lake"
    payload["history_mode"] = _history_mode(dataset)
    if payload["history_mode"] == "snapshot_only":
        payload["warning"] = (
            f"{dataset} is snapshot-only: rows exist only for days this lake "
            "was running. Gaps are not missing data and the series does not "
            "reach back before collection started."
        )
    return payload


def _history_mode(dataset: str) -> str:
    from ashare_lake.domain.datasets import history_mode_for

    return history_mode_for(DATASETS[dataset])


# ---------------------------------------------------------------------------
# run_sql


def _guard_sql(sql: str) -> str:
    """Accept exactly one read-only statement, using DuckDB's own parser.

    A regex on the string is not enough. This lake holds `news_headlines` and
    `flash_news_wire` — vendor text that an agent reads and that nobody here
    wrote — so the SQL reaching this function can be shaped by content the
    lake ingested. Parsing is what tells `SELECT ... -- ; DROP` apart from two
    statements, and it is the parser that will run the query anyway.

    The connection is opened read-only as well; this catches what read-only does
    not, notably `COPY ... TO`, which writes outside the database file.
    """
    import duckdb

    text = str(sql or "").strip()
    if not text:
        raise ToolError("sql must be a non-empty query")
    try:
        statements = duckdb.extract_statements(text)
    except Exception as exc:  # noqa: BLE001 — a parse failure is the agent's to fix
        raise ToolError(f"could not parse SQL: {exc}") from exc

    if len(statements) != 1:
        raise ToolError(f"expected one statement, got {len(statements)}. Send a single SELECT.")
    kind = statements[0].type
    if kind != duckdb.StatementType.SELECT:
        raise ToolError(
            f"only SELECT is allowed here, got {kind.name}. This tool reads the "
            "lake; ingestion and maintenance are `asl` commands."
        )
    return text


def run_sql(config: Config, *, sql: str, limit: int | None = None) -> dict:
    """One read-only DuckDB SELECT across every dataset in the lake.

    The tool the other five cannot replace: ranking, aggregation, and joins
    across datasets happen here rather than by paging rows into the agent's
    context and having it add them up. ``daily_bars_adj`` is a view with hfq_*
    and qfq_* columns already computed.
    """
    from ashare_lake.query.views import ensure_duckdb_views

    text = _guard_sql(sql)
    limit = _clamp_limit(limit)
    if live.enabled(config) and not any(config.curated_root.rglob("*.parquet")):
        _live_only(
            config,
            "run_sql",
            "it queries the parquet on disk, and in live mode nothing is written there.",
        )

    import duckdb

    db_path = ensure_duckdb_views(config)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        df = con.execute(text).pl()
    except Exception as exc:  # noqa: BLE001 — surfaced as a fixable tool error
        raise ToolError(f"query failed: {exc}") from exc
    finally:
        con.close()

    payload = _frame_payload(df, limit=limit, include_provenance=True)
    payload["sql"] = text
    return payload
