"""Frozen, source-neutral inventory of historical cash-payment gaps.

This module records what the lake cannot yet prove. A missing payment date is
*unreviewed*, not evidence that the issuer never published one.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import date
from pathlib import Path

import polars as pl

from cnequity.domain.action_evidence import ex_date_rule_applies
from cnequity.query.reader import ReaderError, load
from cnequity.storage.atomic import publish_once
from cnequity.storage.revisions import committed_revision

_NO_ACTIONS = "corporate_actions has no committed revision to pin; build it with `cne backfill corporate_actions`"


def check_research_window(start: date, end: date, *, holdout_start: date | None = None) -> None:
    """Reject an inverted window, or one reaching a research holdout, before any read.

    ``[research] holdout_start`` keeps a later period out of every evidence
    inventory, so research cannot peek at data reserved for out-of-sample
    checks.  Unset, any window is valid.
    """
    if end < start:
        raise ValueError("the window ends before it starts")
    if holdout_start is not None and end >= holdout_start:
        raise ValueError(f"the window reaches the research holdout starting {holdout_start}")


def source_route(symbol: str) -> str:
    """Route by code convention only; this does not certify issuer identity."""
    code, _, exchange = symbol.partition(".")
    if len(code) != 6 or not code.isdigit():
        return "identity_unresolved"
    if code.startswith("519"):
        return "identity_unresolved"  # fund-like code, historical identity unproved
    if exchange == "BJ" and code.startswith(("92", "43", "83", "87")):
        return "bse_equity_candidate"
    if exchange == "SH" and code.startswith(("501", "502", "51", "52", "53", "56", "58")):
        return "fund_candidate"
    if exchange == "SZ" and code.startswith(("15", "16")):
        return "fund_candidate"
    if exchange == "SH" and code.startswith(("60", "68")):
        return "sh_sz_equity_candidate"
    if exchange == "SZ" and code.startswith(("00", "30")):
        return "sh_sz_equity_candidate"
    return "identity_unresolved"


def _canonical(payload: object) -> bytes:
    return (
        json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        + "\n"
    ).encode()


def _code_sha256() -> str:
    package_root = Path(__file__).resolve().parents[1]
    digest = hashlib.sha256()
    for path in sorted(package_root.rglob("*.py")):
        digest.update(path.relative_to(package_root).as_posix().encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _bar_relation(ex_date: date, span: tuple[date, date] | None) -> str:
    """Where the event falls against the code's observed bars (not identity proof)."""
    if span is None:
        return "no_observed_bar"
    if ex_date < span[0]:
        return "before_first_observed_bar"
    if ex_date > span[1]:
        return "after_last_observed_bar"
    return "within_observed_bar_span"


def inventory(
    *,
    data_root: Path,
    start: date,
    end: date,
    revision: tuple[int, str] | None = None,
    holdout_start: date | None = None,
) -> dict:
    """Freeze missing cash-payment dates of one pinned corporate-action revision.

    Each event says whether it falls inside the code's observed trading span —
    outside it no holding can have received the cash, so it cannot move a
    backtest — and whether the ex-date rule may stand in for the missing date.
    A span is a routing hint, not proof of the issuer's identity on that day.
    """
    check_research_window(start, end, holdout_start=holdout_start)
    root = Path(data_root)
    pinned = revision or committed_revision(
        root / "curated" / "corporate_actions",
        dataset="corporate_actions",
        meta_root=root / "meta",
    )
    if pinned is None:
        raise ReaderError(_NO_ACTIONS)
    frame = load("corporate_actions", data_root=root, start=start, end=end, revision=pinned[1])
    missing = frame.filter(
        (pl.col("action_type") == "cash_dividend")
        & (pl.col("cash_dividend") > 0)
        & pl.col("payment_date").is_null()
    )
    symbols = sorted(set(missing["symbol"].to_list()))
    bars_revision = committed_revision(
        root / "curated" / "daily_bars", dataset="daily_bars", meta_root=root / "meta"
    )
    spans: dict[str, tuple[date, date]] = {}
    if symbols and bars_revision is not None:
        bars = load(
            "daily_bars",
            data_root=root,
            symbols=symbols,
            start=start,
            end=end,
            revision=bars_revision[1],
        )
        spans = {
            row["symbol"]: (row["first_bar"], row["last_bar"])
            for row in bars.group_by("symbol")
            .agg(
                pl.col("trade_date").min().alias("first_bar"),
                pl.col("trade_date").max().alias("last_bar"),
            )
            .iter_rows(named=True)
        }
    events = []
    for row in missing.iter_rows(named=True):
        route = source_route(row["symbol"])
        events.append(
            {
                "symbol": row["symbol"],
                "ex_date": row["ex_date"].isoformat(),
                "action_type": row["action_type"],
                "cash_dividend": row["cash_dividend"],
                "source": row["source"],
                "data_version": row["data_version"],
                "source_route": route,
                "bar_relation": _bar_relation(row["ex_date"], spans.get(row["symbol"])),
                "ex_date_rule_eligible": ex_date_rule_applies(row["symbol"]),
                "evidence_status": "unreviewed",
                "source_document_id": None,
                "source_sha256": None,
                "source_published_at": None,
                "effective_at": None,
                "observed_at": None,
                "supersedes_document_id": None,
                "payment_date": None,
            }
        )
    events.sort(key=lambda row: (row["symbol"], row["ex_date"], row["action_type"]))
    counts: dict[str, int] = {}
    relations: dict[str, int] = {}
    for row in events:
        counts[row["source_route"]] = counts.get(row["source_route"], 0) + 1
        relations[row["bar_relation"]] = relations.get(row["bar_relation"], 0) + 1
    in_span = [row for row in events if row["bar_relation"] == "within_observed_bar_span"]
    return {
        "schema": "cnequity.decision_payment_gaps.v2",
        "window": {"start": start.isoformat(), "end": end.isoformat()},
        "dataset": "corporate_actions",
        "revision": pinned[0],
        "revision_id": pinned[1],
        "daily_bars_revision_id": bars_revision[1] if bars_revision else None,
        "code_sha256": _code_sha256(),
        "count": len(events),
        "source_route_counts": counts,
        "bar_relation_counts": dict(sorted(relations.items())),
        # The two numbers a consumer acts on: gaps a holding could have been
        # paid in, and of those the ones the ex-date rule cannot settle.
        "within_span_count": len(in_span),
        "within_span_rule_ineligible_count": sum(
            not row["ex_date_rule_eligible"] for row in in_span
        ),
        "bar_relation_is_not_identity_proof": True,
        "events": events,
    }


def dual_stock_action_events(frame: pl.DataFrame) -> list[dict]:
    """Locate dates with both bonus and transfer rows for issuer review.

    Coexistence is a diagnostic, not proof of an error: a genuine plan may
    contain both.  The issuer notice must settle the economic terms.
    """
    events = []
    for (symbol, ex_date), group in frame.filter(
        ((pl.col("action_type") == "bonus") & (pl.col("bonus_ratio").fill_null(0) > 0))
        | ((pl.col("action_type") == "transfer") & (pl.col("transfer_ratio").fill_null(0) > 0))
    ).group_by("symbol", "ex_date"):
        if set(group["action_type"].to_list()) != {"bonus", "transfer"}:
            continue
        events.append(
            {
                "symbol": symbol,
                "ex_date": ex_date.isoformat(),
                "source_route": source_route(symbol),
                "evidence_status": "requires_issuer_reconciliation",
                "stock_actions": sorted(
                    [
                        {
                            "action_type": row["action_type"],
                            "bonus_ratio": row["bonus_ratio"],
                            "transfer_ratio": row["transfer_ratio"],
                            "source": row["source"],
                        }
                        for row in group.iter_rows(named=True)
                    ],
                    key=lambda row: row["action_type"],
                ),
            }
        )
    return sorted(events, key=lambda row: (row["symbol"], row["ex_date"]))


def stock_term_diagnostic(
    *, data_root: Path, start: date, end: date, holdout_start: date | None = None
) -> dict:
    """Freeze dual stock-action candidates against one committed revision."""
    check_research_window(start, end, holdout_start=holdout_start)
    root = Path(data_root)
    pinned = committed_revision(
        root / "curated" / "corporate_actions",
        dataset="corporate_actions",
        meta_root=root / "meta",
    )
    if pinned is None:
        raise ReaderError(_NO_ACTIONS)
    frame = load("corporate_actions", data_root=root, start=start, end=end, revision=pinned[1])
    events = dual_stock_action_events(frame)
    return {
        "schema": "cnequity.decision_stock_term_diagnostic.v1",
        "window": {"start": start.isoformat(), "end": end.isoformat()},
        "corporate_actions_revision_id": pinned[1],
        "code_sha256": _code_sha256(),
        "count": len(events),
        "coexistence_is_not_error_proof": True,
        "events": events,
    }


def save_immutable(payload: dict, output_dir: Path, *, prefix: str = "payment-gaps") -> Path:
    """Content-addressed, create-only write; an existing manifest is never replaced."""
    encoded = _canonical(payload)
    digest = hashlib.sha256(encoded).hexdigest()
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f"{prefix}-{digest}.json"
    fd, tmp_name = tempfile.mkstemp(dir=output_dir, prefix=f".{prefix}-", suffix=".tmp")
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        if not publish_once(tmp, target) and target.read_bytes() != encoded:
            raise ValueError(f"existing gap inventory has different content: {target}")
    finally:
        tmp.unlink(missing_ok=True)
    return target
