"""Close the ``corporate_actions`` gaps that two factor vendors agree on.

Factor arbitration finds ex-dates where Sina's and Baostock's factors both
step but the lake records no action. Two causes, repaired differently:

* **Misdated rows.** The lake holds the same event a few days off (TDX dates
  on a Saturday, or a day early), on a date where neither factor steps. When
  that row's own terms explain the step, it is moved to the step date.
* **Missing rows.** Nothing nearby: Baostock's dividend endpoint is asked for
  that symbol and year only, and a row is added only when its ex-date is the
  step date and its terms explain the step.

Everything else stays open: a nearby row whose terms do not fit (share-reform
consideration, typically) is not moved, and nothing is invented. The result is
published as one revision; the previous generation stays readable.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from cnequity.config import Config
from cnequity.domain.action_sessions import effective_session
from cnequity.domain.canonical import dedupe_by_primary_key, dedupe_lazy_by_primary_key
from cnequity.domain.schemas import sanitize_dataset_rows, validate_dataframe, with_provenance
from cnequity.query.parquet_scan import scan_parquet_root
from cnequity.storage.lake_write import planned_on
from cnequity.storage.repairs.publish import PartitionRewrite, scan_published
from cnequity.storage.revisions import RevisionStore

logger = logging.getLogger(__name__)

_DATASET = "corporate_actions"
# A misdated row is looked for this many calendar days either side.
_MAX_OFFSET_DAYS = 10
# Terms explain a step within this relative error, or this absolute floor.
_FIT_RELATIVE = 0.1
_FIT_ABSOLUTE = 5e-4

Fetch = Callable[..., tuple[pl.DataFrame, list[str]]]


def latest_arbitration_evidence(config: Config) -> Path:
    folder = config.meta_root / "quality" / "evidence"
    found = sorted(folder.glob("adj_factor_source_arbitration-*.json"))
    if not found:
        raise RuntimeError("no factor arbitration evidence; run `cne derive adj_factor_source`")
    return found[-1]


def _missing_steps(evidence: Path) -> pl.DataFrame:
    rows = json.loads(evidence.read_text(encoding="utf-8")).get("verdict_rows") or []
    frame = pl.DataFrame(
        rows, schema_overrides={"ex_date": pl.Utf8, "bao_step": pl.Float64}, strict=False
    )
    if frame.is_empty():
        return pl.DataFrame(schema={"symbol": pl.Utf8, "step_date": pl.Date, "step": pl.Float64})
    return frame.filter(pl.col("verdict") == "action_missing").select(
        "symbol",
        pl.col("ex_date").str.to_date().alias("step_date"),
        pl.col("bao_step").abs().alias("step"),
    )


def _implied_step(cash: pl.Expr, shares: pl.Expr, prev_close: pl.Expr) -> pl.Expr:
    """Back-adjusted factor step one day's cash and share terms imply."""
    return prev_close * (1 + shares) / (prev_close - cash) - 1


def _fits(implied: pl.Expr, step: pl.Expr) -> pl.Expr:
    tolerance = pl.max_horizontal(pl.lit(_FIT_ABSOLUTE), _FIT_RELATIVE * step)
    return ((implied - step).abs() <= tolerance).fill_null(False)


def _terms(actions: pl.DataFrame) -> pl.DataFrame:
    return actions.group_by("symbol", "ex_date").agg(
        pl.col("cash_dividend").fill_null(0).sum().alias("cash"),
        (pl.col("bonus_ratio").fill_null(0) + pl.col("transfer_ratio").fill_null(0))
        .sum()
        .alias("shares"),
        # Terms the share formula cannot price: an allotment's cash leg, and a
        # restructuring conversion priced at its published reference price.
        pl.col("action_type").is_in(["allotment", "reorg_transfer"]).any().alias("allotment"),
    )


def _prev_closes(config: Config, symbols: list[str]) -> pl.DataFrame:
    return (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(
                config.curated_root / "daily_bars", partition_col="trade_date", traded_only=True
            ),
            "daily_bars",
        )
        .filter(pl.col("symbol").is_in(symbols))
        .select("symbol", "trade_date", "close")
        .collect()
        .sort("symbol", "trade_date")
        .select(
            "symbol",
            pl.col("trade_date").alias("step_date"),
            pl.col("close").shift(1).over("symbol").alias("prev_close"),
        )
    )


def _factor_sessions(config: Config, symbols: list[str]) -> pl.DataFrame:
    """Sina hfq factor per traded session: the sessions a factor can step on."""
    return (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(config.derived_root / "adj_factors", partition_col="trade_date"),
            "adj_factors",
        )
        .filter((pl.col("adjust_type") == "hfq") & pl.col("symbol").is_in(symbols))
        .select("symbol", "trade_date", "factor")
        .collect()
        .sort("symbol", "trade_date")
    )


def _effective(frame: pl.DataFrame, factors: pl.DataFrame) -> pl.DataFrame:
    """Add ``effective``: the first traded session on or after each ``ex_date``.

    An ex-date in a trading halt, or on a day the market was shut, takes
    effect when the security next trades; that is where its factor steps.
    """
    return effective_session(frame, factors).rename({"effective_session": "effective"})


def plan_moves(
    missing: pl.DataFrame, actions: pl.DataFrame, closes: pl.DataFrame, factors: pl.DataFrame
) -> pl.DataFrame:
    """``(symbol, d0, step_date)``: recorded dates to move onto a missing step.

    The recorded date must be a session the security traded without a Sina
    factor step: a date in a halt is a genuine ex-date whose step lands on
    resumption, not a misdated row. It must carry no allotment (whose terms
    this check cannot price), and its terms must explain the step. The
    nearest fitting date wins, and a date is moved at most once.
    """
    empty = pl.DataFrame(schema={"symbol": pl.Utf8, "d0": pl.Date, "step_date": pl.Date})
    if missing.is_empty() or actions.is_empty():
        return empty
    quiet = factors.filter(
        (pl.col("factor") / pl.col("factor").shift(1).over("symbol") - 1).abs().fill_null(0) <= 1e-6
    ).select("symbol", pl.col("trade_date").alias("d0"))
    near = (
        missing.join(
            _terms(actions).rename({"ex_date": "d0"}),
            on="symbol",
        )
        .with_columns((pl.col("d0") - pl.col("step_date")).dt.total_days().alias("offset"))
        .filter(pl.col("offset").abs().is_between(1, _MAX_OFFSET_DAYS) & ~pl.col("allotment"))
        .join(quiet, on=["symbol", "d0"], how="semi")
        .join(closes, on=["symbol", "step_date"], how="left")
        .filter(
            _fits(
                _implied_step(pl.col("cash"), pl.col("shares"), pl.col("prev_close")),
                pl.col("step"),
            )
        )
    )
    best = near.sort(pl.col("offset").abs(), "d0").unique(
        ["symbol", "step_date"], keep="first", maintain_order=True
    )
    return (
        best.filter(pl.len().over("symbol", "d0") == 1)
        .select("symbol", "d0", "step_date")
        .sort("symbol", "step_date")
    )


def pick_added(
    fetched: pl.DataFrame, unpaired: pl.DataFrame, closes: pl.DataFrame, factors: pl.DataFrame
) -> pl.DataFrame:
    """Baostock rows that take effect on a missing step and explain it.

    Rows keep Baostock's own ex-date; they are matched to the step through
    the first traded session on or after it.
    """
    if fetched.is_empty() or unpaired.is_empty():
        return fetched.clear()
    steps = unpaired.rename({"step_date": "effective"})
    on_step = _effective(fetched, factors).join(
        steps.select("symbol", "effective"), on=["symbol", "effective"], how="semi"
    )
    fitting = (
        on_step.group_by("symbol", "effective")
        .agg(
            pl.col("cash_dividend").fill_null(0).sum().alias("cash"),
            (pl.col("bonus_ratio").fill_null(0) + pl.col("transfer_ratio").fill_null(0))
            .sum()
            .alias("shares"),
        )
        .join(steps, on=["symbol", "effective"])
        .join(closes.rename({"step_date": "effective"}), on=["symbol", "effective"], how="left")
        .filter(
            _fits(
                _implied_step(pl.col("cash"), pl.col("shares"), pl.col("prev_close")),
                pl.col("step"),
            )
        )
        .select("symbol", "effective")
    )
    return on_step.join(fitting, on=["symbol", "effective"], how="semi").drop("effective")


def pick_reorg(
    notices: dict[tuple[str, date], list[dict]],
    unpaired: pl.DataFrame,
    closes: pl.DataFrame,
    factors: pl.DataFrame,
) -> pl.DataFrame:
    """``reorg_transfer`` rows whose notices explain a missing step.

    The notices must agree on one ratio and one ex-date that takes effect on
    the step's session, and exactly one stated reference price must give the
    step as ``previous close / reference price``.
    """
    from cnequity.adapters.cninfo.reorg_notices import combine_notices

    steps = {
        (row["symbol"], row["step_date"]): row["step"] for row in unpaired.iter_rows(named=True)
    }
    prev = {
        (row["symbol"], row["step_date"]): row["prev_close"] for row in closes.iter_rows(named=True)
    }
    rows = []
    for (symbol, step_date), found in notices.items():
        event = combine_notices(found)
        step, close = steps.get((symbol, step_date)), prev.get((symbol, step_date))
        if event is None or step is None or not close:
            continue
        lands = _effective(
            pl.DataFrame({"symbol": [symbol], "ex_date": [event["ex_date"]]}), factors
        ).get_column("effective")[0]
        if lands != step_date:
            continue
        # A stated price is rounded to the cent; that alone moves the implied
        # step by about (1 + step) * 0.005 / price. A second candidate further
        # off than that (an early estimate) does not fit.
        fitting = [
            p
            for p in event["reference_prices"]
            if abs(close / p - 1 - step) <= max(_FIT_ABSOLUTE, (1 + step) * 0.006 / p)
        ]
        if len(fitting) != 1:
            continue
        rows.append(
            {
                "symbol": symbol,
                "ex_date": event["ex_date"],
                "action_type": "reorg_transfer",
                "cash_dividend": 0.0,
                "bonus_ratio": 0.0,
                "transfer_ratio": event["transfer_ratio"],
                "allotment_ratio": None,
                "allotment_price": None,
                "reference_price": fitting[0],
                "payment_date": None,
                "payment_source": None,
            }
        )
    return pl.DataFrame(
        rows,
        schema={
            "symbol": pl.Utf8,
            "ex_date": pl.Date,
            "action_type": pl.Utf8,
            "cash_dividend": pl.Float64,
            "bonus_ratio": pl.Float64,
            "transfer_ratio": pl.Float64,
            "allotment_ratio": pl.Float64,
            "allotment_price": pl.Float64,
            "reference_price": pl.Float64,
            "payment_date": pl.Date,
            "payment_source": pl.Utf8,
        },
    )


def _default_fetch(config: Config, run_id: str) -> Fetch:
    from cnequity.adapters.baostock.corporate_actions import fetch_corporate_actions_baostock

    def fetch(symbols: list[str], start: date, end: date) -> tuple[pl.DataFrame, list[str]]:
        return fetch_corporate_actions_baostock(
            symbols,
            start,
            end,
            config=config,
            run_id=run_id,
            request_scope=f"repair:gaps:{start.year}",
        )

    return fetch


def repair_corporate_action_gaps(
    config: Config,
    *,
    apply: bool = False,
    evidence: Path | None = None,
    fetch: Fetch | None = None,
    reorg_fetch: Callable[..., tuple[dict, list[dict]]] | None = None,
) -> dict:
    """Plan (default, offline) or fetch and publish the gap repair.

    The Baostock sweep runs outside the lake mutation lock, so a scheduled
    run can still publish during it; the plan is recomputed under the lock
    before anything is written.
    """
    evidence = evidence or latest_arbitration_evidence(config)
    plan = _plan(config, evidence)
    if not apply or plan["missing"].is_empty():
        return plan["report"]
    run_id = f"corporate-action-gaps-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    fetched, failed = _fetch_gaps(plan["unpaired"], fetch or _default_fetch(config, run_id))
    # What Baostock's dividends do not explain goes to the issuer's own notices.
    picked = pick_added(fetched, plan["unpaired"], plan["closes"], plan["factors"])
    remaining = plan["unpaired"]
    if picked.height:
        explained = _effective(picked, plan["factors"]).select(
            "symbol", pl.col("effective").alias("step_date")
        )
        remaining = remaining.join(explained, on=["symbol", "step_date"], how="anti")
    if reorg_fetch is None:
        from cnequity.adapters.cninfo.reorg_notices import fetch_reorg_notices as reorg_fetch
    notices, notice_diagnostics = reorg_fetch(
        config, run_id, list(remaining.select("symbol", "step_date").iter_rows())
    )
    with planned_on(config):
        fresh = _plan(config, evidence)
        fresh["report"]["notice_diagnostics"] = notice_diagnostics[:50]
        return _publish(config, fresh, fetched, failed, notices, run_id=run_id, evidence=evidence)


def _plan(config: Config, evidence: Path) -> dict:
    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
    scan = scan_published(config, _DATASET) if store.current_root(_DATASET) else None
    if scan is None:
        raise RuntimeError(f"{_DATASET}: no committed generation to repair")
    missing = _missing_steps(evidence)
    report: dict = {"evidence": str(evidence), "missing": missing.height, "applied": False}
    plan = {"store": store, "missing": missing, "report": report}
    if missing.is_empty():
        return plan
    symbols = sorted(missing.get_column("symbol").unique().to_list())
    current = dedupe_by_primary_key(
        sanitize_dataset_rows(
            validate_dataframe(scan.collect(), _DATASET),
            _DATASET,
        ),
        _DATASET,
    )
    closes = _prev_closes(config, symbols)
    factors = _factor_sessions(config, symbols)
    moves = plan_moves(
        missing,
        current.filter(pl.col("symbol").is_in(symbols)),
        closes,
        factors,
    )
    unpaired = missing.join(
        moves.select("symbol", "step_date"), on=["symbol", "step_date"], how="anti"
    )
    report.update(
        moves=moves.height,
        unpaired=unpaired.height,
        unpaired_symbol_years=unpaired.select("symbol", pl.col("step_date").dt.year())
        .unique()
        .height,
    )
    plan.update(current=current, closes=closes, factors=factors, moves=moves, unpaired=unpaired)
    return plan


def _fetch_gaps(unpaired: pl.DataFrame, fetch: Fetch) -> tuple[pl.DataFrame, list[str]]:
    """One Baostock request per symbol-year that nothing nearby explains."""
    fetched: list[pl.DataFrame] = []
    failed: list[str] = []
    by_year = unpaired.group_by(pl.col("step_date").dt.year().alias("year")).agg(
        pl.col("symbol").unique().sort()
    )
    for year, names in sorted(by_year.iter_rows()):
        frame, year_failed = fetch(list(names), date(year, 1, 1), date(year, 12, 31))
        logger.info(
            "corporate action gaps %d: %d symbol(s), %d row(s), %d failed",
            year,
            len(names),
            frame.height,
            len(year_failed),
        )
        if not frame.is_empty():
            fetched.append(frame)
        failed.extend(f"{name}:{year}" for name in year_failed)
    return (pl.concat(fetched, how="diagonal_relaxed") if fetched else pl.DataFrame()), failed


def _publish(
    config: Config,
    plan: dict,
    fetched: pl.DataFrame,
    failed: list[str],
    notices: dict[tuple[str, date], list[dict]],
    *,
    run_id: str,
    evidence: Path,
) -> dict:
    report, store = plan["report"], plan["store"]
    current, moves = plan["current"], plan["moves"]
    added = pick_added(fetched, plan["unpaired"], plan["closes"], plan["factors"])
    report.update(
        added_events=added.select("symbol", "ex_date").unique().height if added.height else 0,
        added_rows=added.height,
        baostock_failed=len(failed),
        baostock_failed_sample=failed[:20],
    )
    moved_keys = moves.rename({"d0": "ex_date"})
    moved = (
        current.join(moved_keys, on=["symbol", "ex_date"], how="inner")
        .with_columns(pl.col("step_date").alias("ex_date"))
        .drop("step_date")
    )
    kept = current.join(
        moved_keys.select("symbol", "ex_date"), on=["symbol", "ex_date"], how="anti"
    )
    if not added.is_empty():
        added = with_provenance(added, source="baostock", data_version="v1")
    reorg = pick_reorg(notices, plan["unpaired"], plan["closes"], plan["factors"])
    report["reorg_transfers"] = reorg.height
    if not reorg.is_empty():
        reorg = with_provenance(reorg, source="cninfo", data_version="v1")
        added = reorg if added.is_empty() else pl.concat([added, reorg], how="diagonal_relaxed")
    parts = [kept, moved] + ([added] if not added.is_empty() else [])
    repaired = dedupe_by_primary_key(
        validate_dataframe(pl.concat(parts, how="diagonal_relaxed"), _DATASET), _DATASET
    )
    touched = sorted(
        {d.year for d in moves.get_column("d0").to_list()}
        | {d.year for d in moves.get_column("step_date").to_list()}
        | ({d.year for d in added.get_column("ex_date").to_list()} if added.height else set())
    )
    if not touched:
        return report
    affected_symbols = frozenset(
        moves.get_column("symbol").to_list()
        + (added.get_column("symbol").to_list() if not added.is_empty() else [])
    )
    with PartitionRewrite(config, store, _DATASET, run_id=run_id, apply=True) as rewrite:
        for year in touched:
            rewrite.replace(
                f"ex_date={year}",
                repaired.filter(pl.col("ex_date").dt.year() == year).select(current.columns),
            )
        published = rewrite.publish(
            {
                "reason": "corporate_action_gap_repair",
                "evidence": str(evidence),
                "moves": moves.height,
                "added_rows": report["added_rows"],
            },
            symbols=affected_symbols,
        )
    if published is None:
        raise RuntimeError(f"{_DATASET}: gap repair produced no revision")
    report.update(
        applied=True,
        run_id=run_id,
        partitions_changed=len(touched),
        revision=published["revision"],
        publication_audit=published["publication_audit"],
    )
    return report
