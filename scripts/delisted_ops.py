#!/usr/bin/env python3
"""Rebuild the delisted universe: discover, reconcile, repair, verify coverage.

Survivorship-bias repair is a project, not an operation. The sweep runs for
hours and is resumable across days; reconcile and repair are one-off corrections
you apply once the evidence is in; coverage is the gate you check afterwards.
None of it is on a daily path, and all of it is opinionated about a particular
lake's history — so it lives here rather than in the published CLI.

What stayed in the CLI is the part with a routine shape:

* ``cne delisted status``   — read the catalogue (safe, fast, no side effects)
* ``cne delisted backfill`` — fetch the price history it names

The normal order::

    cne delisted status                             # what is known so far
    python scripts/delisted_ops.py discover         # sweep; resumable, re-run it
    cne delisted backfill --since 2016-01-01        # fetch what the sweep found
    python scripts/delisted_ops.py repair           # wire delist_date into instruments
    python scripts/delisted_ops.py reconcile        # audit terminals (dry-run)
    python scripts/delisted_ops.py reconcile --apply
    python scripts/delisted_ops.py coverage --start 2016-01-01   # exits 1 until verified

`coverage` is the only one that gates: it exits 1 when the window is not
verified, so it is the one to put in front of anything that claims a
survivorship-safe universe.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import cnequity.steps  # noqa: F401 — register steps
from cnequity.config import load_config
from cnequity.domain.market_time import shanghai_today
from cnequity.orchestrator.engine import JobEngine
from cnequity.query.views import ensure_duckdb_views

DEFAULT_CONFIG = "configs/cnequity.toml"


def _progress() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def discover(cfg, *, limit: int | None) -> int:
    """Sweep the issued code space for codes that used to trade.

    Resumable: a re-run continues where the last one stopped. A code whose probe
    failed stays pending rather than being filed as never-issued — the whole
    point is not to conclude "this never existed" from a network error.
    """
    from cnequity.steps.delisted import discover_delisted

    _progress()
    result = discover_delisted(cfg, limit=limit)
    print(
        json.dumps(
            {
                "probed": result.probed,
                "delisted": result.delisted,
                "never_issued": result.never_issued,
                "failed": len(result.failed),
                "remaining": result.remaining,
                "complete": result.complete,
            },
            indent=2,
        )
    )
    return 0


def reconcile(cfg, *, apply: bool, sample: int) -> int:
    """Audit catalogue terminals; with --apply, correct the proven ones.

    Dry-run by default. ``--apply`` refuses to run during any active ingestion,
    backs up the catalogue, and writes a quality receipt.
    """
    from cnequity.steps.delisted import (
        delisted_catalog_reconciliation_report,
        reconcile_delisted_catalog,
    )

    try:
        report = (
            reconcile_delisted_catalog(cfg, sample=sample)
            if apply
            else delisted_catalog_reconciliation_report(cfg, sample=sample)
        )
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2))
    return 0


def repair(cfg, *, since: str | None) -> int:
    """Wire catalogued / orphan-bar delistings into instruments without re-fetching.

    For the state where daily_bars already holds the recovered series (from
    baostock, say) but instruments still has no delist_date — the gap that
    leaves ``universe=all_a`` selecting dead names. Also drops ``认购款`` stubs.
    """
    from cnequity.steps.delisted import (
        purge_subscription_placeholders,
        repair_delisted_instruments,
    )

    _progress()
    start = date.fromisoformat(since) if since else None
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run("delisted_repair", {"since": since} if since else {})
    result = repair_delisted_instruments(cfg, run_id, start=start)
    compact_out = engine.run_step("compact", shanghai_today(), run_id)
    # Compact can re-introduce nothing for placeholders; purge once more after
    # the merge in case an older curated copy still carried them.
    result["purged_placeholders_after_compact"] = purge_subscription_placeholders(cfg)

    # Anything that is not success has to survive the merge. Listing only the
    # spellings seen today is how `cne backfill` came to report success for a
    # sweep whose every slice had failed: `degraded` fell through the elif and
    # landed on the success branch.
    statuses = (result.get("status", "success"), compact_out.get("status", "success"))
    if "failed" in statuses:
        run_status = "failed"
    elif any(status != "success" for status in statuses):
        run_status = "warning"
    else:
        run_status = "success"

    engine.manifest.finish_run(
        run_id,
        run_status,
        rows_read=result.get("rows_read", 0),
        rows_written=result.get("rows_written", 0),
        error_message=None if run_status == "success" else "delisted repair is incomplete",
    )
    ensure_duckdb_views(cfg)
    print(
        json.dumps(
            {"run_id": run_id, **result, "status": run_status, "compact": compact_out},
            indent=2,
            default=str,
        )
    )
    if run_status != "success":
        print("error: delisted repair is incomplete; retry the missing scope", file=sys.stderr)
        return 1
    return 0


def coverage(cfg, *, start: str, end: str | None, sample: int, universe: str) -> int:
    """Fail unless the requested window has verified delisting coverage.

    Read-only. The JSON separates incomplete discovery, definite missing bars,
    uncertain overlap, terminal mismatches, and instruments identity gaps —
    they need different fixes, so collapsing them into one number would hide
    which one you have.
    """
    from cnequity.steps.delisted import delisted_coverage_report

    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end) if end else None
    if end_date is not None and start_date > end_date:
        print("error: --start must be on or before --end", file=sys.stderr)
        return 1

    report = delisted_coverage_report(cfg, start_date, end_date, sample=sample, universe=universe)
    print(json.dumps(report, indent=2))
    return 0 if report["verified"] else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    subs = parser.add_subparsers(dest="command", required=True)

    p = subs.add_parser("discover", help=discover.__doc__.splitlines()[0])
    p.add_argument("--limit", type=int, default=None, help="Probe at most N codes this run.")

    p = subs.add_parser("reconcile", help=reconcile.__doc__.splitlines()[0])
    p.add_argument("--apply", action="store_true", help="Apply high-confidence corrections.")
    p.add_argument("--sample", type=int, default=15)

    p = subs.add_parser("repair", help=repair.__doc__.splitlines()[0])
    p.add_argument(
        "--since",
        default=None,
        help="Only catalogued delistings on/after this date (default: all genuine).",
    )

    p = subs.add_parser("coverage", help=coverage.__doc__.splitlines()[0])
    p.add_argument("--start", default="2016-01-01", help="Research window start.")
    p.add_argument("--end", default=None, help="Research window end (default: latest session).")
    p.add_argument("--sample", type=int, default=15)
    p.add_argument(
        "--universe",
        choices=["all_a", "all_a_sh_sz"],
        default="all_a",
        help="Historical research universe checked by the coverage report.",
    )

    args = parser.parse_args(argv)
    cfg = load_config(Path(args.config))

    if args.command == "discover":
        return discover(cfg, limit=args.limit)
    if args.command == "reconcile":
        return reconcile(cfg, apply=args.apply, sample=args.sample)
    if args.command == "repair":
        return repair(cfg, since=args.since)
    return coverage(cfg, start=args.start, end=args.end, sample=args.sample, universe=args.universe)


if __name__ == "__main__":
    raise SystemExit(main())
