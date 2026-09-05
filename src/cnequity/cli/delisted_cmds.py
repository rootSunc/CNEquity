"""`delisted status` and `delisted backfill`.

Rebuilding the catalogue is a one-off project and lives in
`scripts/delisted_ops.py`; reading it and fetching what it names are routine, so
they stayed here.
"""

from __future__ import annotations

import json
import logging

import click

from cnequity.cli._root import cli
from cnequity.cli._shared import (
    _cfg,
    config_option,
    parse_date_option,
)
from cnequity.domain.market_time import shanghai_today
from cnequity.orchestrator.engine import JobEngine

# Exit 0 even when sources are down. A red source is this command's *output*,
# not its failure.


@cli.group("delisted")
def delisted_grp():
    """Read the delisted catalogue and fetch the history it names.

    Rebuilding the catalogue — the code-space sweep, terminal reconciliation,
    instruments repair and the coverage gate — is a one-off project rather than
    an operation, and lives in `scripts/delisted_ops.py`.
    """


@delisted_grp.command("status")
@config_option
@click.option("--since", default="2016-01-01", show_default=True, help="Lake window start.")
@click.option("--sample", default=15, show_default=True, help="Rows of detail to print.")
def delisted_status(config_path: str, since: str, sample: int):
    """Summarise the catalogue: how many, from when, and what is left to probe."""
    from collections import Counter

    from cnequity.steps.delisted import (
        LIVE_RECENCY_DAYS,
        classify_catalog,
        delisted_symbols_in_window,
        pending_codes,
    )

    cfg = _cfg(config_path)
    start = parse_date_option(since, "--since")
    catalog, live_missing = classify_catalog(cfg)
    in_window = {s: d for s, d in catalog.items() if d >= start}
    by_year = Counter(d.year for d in in_window.values())
    by_board = Counter(s.split(".")[1] for s in in_window)
    recent = sorted(in_window.items(), key=lambda kv: kv[1], reverse=True)[:sample]
    click.echo(
        json.dumps(
            {
                "delisted": len(catalog),
                "in_window": len(in_window),
                # Still quoting near the latest session: either a code the
                # instrument list is missing, or a delisting inside the recency
                # window that will reclassify on the next sweep.
                "live_or_recent": len(live_missing),
                "live_or_recent_by_exchange": dict(
                    sorted(Counter(s.split(".")[1] for s in live_missing).items())
                ),
                "live_recency_days": LIVE_RECENCY_DAYS,
                "window_start": since,
                "pending_probe": len(pending_codes(cfg)),
                "not_yet_ingested": len(delisted_symbols_in_window(cfg, start)),
                "by_year": dict(sorted(by_year.items())),
                "by_exchange": dict(sorted(by_board.items())),
                "most_recent": [{"symbol": s, "last_traded": d.isoformat()} for s, d in recent],
            },
            indent=2,
        )
    )


@delisted_grp.command("backfill")
@config_option
@click.option("--since", default="2016-01-01", show_default=True, help="Lake window start.")
def delisted_backfill(config_path: str, since: str):
    """Fetch price history for catalogued delistings and compact it into the lake."""

    from cnequity.steps.delisted import backfill_delisted_bars

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    cfg = _cfg(config_path)
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run("delisted_backfill", {"since": since})
    result = backfill_delisted_bars(cfg, run_id, parse_date_option(since, "--since"))
    compact_out = engine.run_step("compact", shanghai_today(), run_id)
    complete = (
        result.get("status", "success") == "success"
        and compact_out.get("status", "success") == "success"
    )
    run_status = "success" if complete else "warning"
    error_message = None if complete else "delisted recovery has unresolved targets"
    engine.manifest.finish_run(
        run_id,
        run_status,
        rows_read=result.get("rows_read", 0),
        rows_written=result.get("rows_written", 0),
        error_message=error_message,
    )
    click.echo(
        json.dumps(
            {"run_id": run_id, **result, "status": run_status, "compact": compact_out},
            indent=2,
            default=str,
        )
    )
    if not complete:
        raise click.ClickException(error_message)
