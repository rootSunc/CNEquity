"""`ths-official snapshot`, `backfill` and `repair-bars`.

Everything here needs a 同花顺 API key and does nothing without one. The source
is optional by design: a lake with no key keeps the sources it already has, and
these commands report that they were skipped rather than failing.

The split mirrors the one in the config. ``snapshot`` is verification — it
writes to ``meta/source_snapshots`` and never to curated, so it only feeds the
arbitration checks in `cne audit`. ``backfill`` and ``repair-bars`` change what
the lake holds and are gated separately, on ``[sources.ths_official].backfill``.

See ``docs/development/ths-official-integration.md`` for the measurements these
commands act on.
"""

from __future__ import annotations

import contextlib
import json
import uuid
from datetime import date

import click

from cnequity.cli._root import cli, moved_hints
from cnequity.cli._shared import (
    _cfg,
    _progress_logging,
    attach_log_file,
    config_option,
    parse_date_option,
)

# The service floors its history around here; earlier requests come back empty.
_DEEP_HISTORY_START = "2005-01-01"
_DEEP_HISTORY_END = "2015-12-31"


def _require_key(cfg) -> str | None:
    """Return an error message when this lake cannot reach the source."""
    if not getattr(cfg, "ths_official_api_key", None):
        return (
            "no API key: set HITHINK_FINANCE_API_KEY, or [sources.ths_official].api_key "
            "in the config"
        )
    if not cfg.sources.get("ths_official", False):
        return "source disabled: set [sources.ths_official] enabled = true"
    return None


def _skip(reason: str) -> None:
    click.echo(json.dumps({"status": "skipped", "reason": reason}, indent=2))


# `capture` was `snapshot`, which collided head-on with the top-level `cne
# snapshot` — one freezes the lake into a portable archive, the other fetches a
# vendor's rows for the arbitration checks, and nothing but position in the
# command line told them apart.
@cli.group("ths-official", cls=moved_hints({"snapshot": "cne ths-official capture"}))
def ths_official_grp():
    """Cross-check and backfill against the 同花顺 official API (needs a key).

    Without a key every command here reports "skipped" and changes nothing —
    the lake keeps the sources it already has.
    """


def _run_outcome(result: dict) -> tuple[str, int, int]:
    """Map a step result onto the manifest's run vocabulary."""
    status = str(result.get("status") or "success")
    if status in {"applied", "dry_run"}:
        status = "success"
    failures = int(result.get("failed_symbols") or 0) + int(result.get("failed") or 0)
    if status == "success" and failures:
        status = "warning"
    rows_written = int(result.get("rows_written") or 0)
    rows_read = int(result.get("rows_read") or result.get("rows_fetched") or rows_written)
    return status, rows_read, rows_written


@contextlib.contextmanager
def _staged_run(cfg, job_name: str, prefix: str, metadata: dict, *, record: bool = True):
    """Open a manifest run around work that only stages rows.

    These commands mint a run id, stage against it, and leave the publish to
    `cne run compact --run-id <id>` — but they never opened a run, so the id
    named nothing. The rows still reached curated; what was missing was the
    parent row. `cne status` could not see the run at all, and `cne run clean`
    files staging it cannot prove is finished under *skipped*, which is never
    reclaimed rather than aged out the way a manifest-less orphan is: three
    `ths-*` runs had 23MB stranded that way.

    Closed as soon as the fetch is done, because the compact really is a
    separate step. It is recorded against the same run, and staging still needs
    that successful compact batch before anything will remove it.
    """
    from cnequity.orchestrator.manifest import Manifest

    run_id = f"{prefix}-{uuid.uuid4()}"
    outcome: dict = {}
    if not record:
        # A dry run stages nothing, so there is nothing for a run to account for.
        yield run_id, outcome
        return
    manifest = Manifest(cfg.manifest_path)
    manifest.start_run(job_name, metadata, run_id=run_id)
    try:
        yield run_id, outcome
    except Exception as exc:
        manifest.finish_run(run_id, "failed", error_message=str(exc))
        raise
    status, rows_read, rows_written = _run_outcome(outcome.get("result") or {})
    manifest.finish_run(run_id, status, rows_read=rows_read, rows_written=rows_written)


@ths_official_grp.command("capture")
@config_option
@click.option(
    "--what",
    type=click.Choice(["corporate-actions", "daily-bars", "financials", "valuations", "all"]),
    default="all",
    show_default=True,
    help="Which peer snapshot to capture.",
)
@click.option("--days", default=45, show_default=True, help="Bar window, in calendar days.")
@click.option("--sample", default=400, show_default=True, help="Securities to sample for bars.")
def ths_snapshot(config_path: str, what: str, days: int, sample: int):
    """Capture peer data for the arbitration checks. Never writes curated rows.

    The checks in `cne audit` are silent until this has run at least once:
    `adj_factor_arbitration` and `daily_bars_arbitration` both read the
    snapshot store, and neither invents an opinion it has not been given.

    Corporate actions come from one full-history dump — 66,105 rows in a single
    download, and the only route that carries 配股. Bars are sampled because the
    point is to arbitrate the days two incumbents already disagree on, not to
    mirror the market.
    """
    from datetime import timedelta

    import polars as pl

    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
    from cnequity.steps.bars import snapshot_daily_bars_ths_official
    from cnequity.steps.capital import snapshot_valuations_ths_official
    from cnequity.steps.fundamentals import (
        snapshot_corporate_actions_ths_official,
        snapshot_financials_ths_official,
    )

    _progress_logging()
    cfg = _cfg(config_path)
    problem = _require_key(cfg)
    if problem:
        _skip(problem)
        return

    attach_log_file(cfg, "ths-official-capture")
    run_id = f"ths-snapshot-{uuid.uuid4()}"
    out: dict = {}
    if what in ("corporate-actions", "all"):
        out["corporate_actions"] = snapshot_corporate_actions_ths_official(cfg, run_id)
    if what in ("daily-bars", "all"):
        root = cfg.curated_root / "daily_bars"
        if not dataset_has_parquet(root):
            out["daily_bars"] = {"status": "skipped", "reason": "no daily_bars in the lake"}
        else:
            last = (
                scan_parquet_root(root, partition_col="trade_date")
                .select(pl.col("trade_date").max())
                .collect()
                .item()
            )
            out["daily_bars"] = snapshot_daily_bars_ths_official(
                cfg, run_id, start=last - timedelta(days=days), end=last, sample=sample
            )
    if what in ("valuations", "all"):
        out["valuations"] = snapshot_valuations_ths_official(cfg, run_id)
    if what in ("financials", "all"):
        out["financials"] = snapshot_financials_ths_official(
            cfg, run_id, start=date(2016, 1, 1), end=date(2024, 12, 31), sample=sample
        )
    click.echo(json.dumps(out, indent=2, default=str))


@ths_official_grp.command("backfill")
@config_option
@click.option("--start", default="2016-01-01", show_default=True, help="First report period.")
@click.option("--end", default="2024-12-31", show_default=True, help="Last report period.")
@click.option("--chunk-size", default=200, show_default=True, help="Securities per staged batch.")
@click.option("--workers", default=4, show_default=True, help="Concurrent requests.")
@click.option(
    "--symbols",
    "symbols_str",
    default=None,
    help="Comma-separated symbols instead of the whole market. A full sweep is "
    "78 minutes, so a handful lost to a transient transport error is not worth "
    "re-running one for — the closing JSON names them under `failed_symbols`.",
)
def ths_backfill(
    config_path: str,
    start: str,
    end: str,
    chunk_size: int,
    workers: int,
    symbols_str: str | None,
):
    """Fill the balance-sheet and cash-flow gap the lake carries for 2016-2024.

    Those two statements cover 0 to 37 securities a year over that window while
    income covers 4,623 to 5,558 — a hole `backfill_missing_statement_types` has
    been reporting all along. Routing rather than switching: the primary keys
    are empty, so nothing canonical is overwritten.

    Disclosure dates are borrowed from the income rows the lake already holds,
    because the upstream's own date is the *next* year's filing and would push
    every PIT date forward by a year. A period with no borrowable date is
    skipped rather than given an invented one.

    Needs `[sources.ths_official] backfill = true`; it changes what the lake
    holds. Stages rows — run `cne run compact --run-id <id>` afterwards.

    `failed_symbols` in the closing JSON separates a vendor gap from a local
    one: `Unknown thscode` means the peer does not carry that security, while a
    transport error means the request never arrived. Feed the second kind back
    through `--symbols` rather than re-running the market.
    """
    from cnequity.steps.fundamentals import backfill_statement_gap_ths_official

    _progress_logging()
    cfg = _cfg(config_path)
    problem = _require_key(cfg)
    if problem:
        _skip(problem)
        return
    if not getattr(cfg, "ths_official_backfill_enabled", False):
        _skip("backfill disabled: set [sources.ths_official] backfill = true")
        return

    attach_log_file(cfg, "ths-official-backfill")
    symbols = [s.strip() for s in symbols_str.split(",") if s.strip()] if symbols_str else None
    with _staged_run(
        cfg,
        "ths_official_backfill",
        "ths-backfill",
        {"start": start, "end": end, "symbols": symbols},
    ) as (run_id, outcome):
        result = backfill_statement_gap_ths_official(
            cfg,
            run_id,
            start=parse_date_option(start, "--start"),
            end=parse_date_option(end, "--end"),
            symbols=symbols,
            chunk_size=chunk_size,
            workers=workers,
        )
        outcome["result"] = result
    click.echo(json.dumps({"run_id": run_id, **result}, indent=2, default=str))


@ths_official_grp.command("repair-bars")
@config_option
@click.option("--start", default=_DEEP_HISTORY_START, show_default=True)
@click.option("--end", default=_DEEP_HISTORY_END, show_default=True)
@click.option(
    "--apply",
    is_flag=True,
    help="Actually write. Without it this reports the diff and changes nothing.",
)
@click.option(
    "--adjudicator",
    type=click.Path(exists=True, dir_okay=False),
    help="Parquet of (symbol, trade_date, close) from an independent source.",
)
@click.option("--diff-out", type=click.Path(dir_okay=False), help="Write the disputed rows here.")
@click.option("--workers", default=4, show_default=True)
def ths_repair_bars(
    config_path: str,
    start: str,
    end: str,
    apply: bool,
    adjudicator: str | None,
    diff_out: str | None,
    workers: int,
):
    """Re-source the deep history from the licensed API instead of the scraper.

    `daily_bars` splits by source: an unauthenticated scrape of 10jqka's public
    pages owns 2001-2015 alone, while 2016 onward has a configured backup. This
    moves the part the official API reaches — measured at 4,398,523 rows, 98.68%
    of that window once applied.

    **Switching, not routing**, so `--apply` is required and a bare run only
    reports. Pass `--adjudicator` as well and a *disputed* row is only switched
    when an independent source backs the peer: measured against baostock over
    1,418 disputes, the peer was right 1,167 times and the incumbent 251, with
    no three-way split — so switching blindly imports 251 known regressions.

    Build the adjudicator from a vendor sharing no lineage with either side.
    Both candidates here are 同花顺's, so they cannot arbitrate each other.
    """
    from pathlib import Path

    import polars as pl

    from cnequity.steps.bars import repair_deep_history_ths_official

    _progress_logging()
    cfg = _cfg(config_path)
    problem = _require_key(cfg)
    if problem:
        _skip(problem)
        return
    if apply and not getattr(cfg, "ths_official_backfill_enabled", False):
        _skip("--apply needs [sources.ths_official] backfill = true")
        return

    attach_log_file(cfg, "ths-official-repair-bars")

    judge = pl.read_parquet(adjudicator) if adjudicator else None
    if apply and judge is None:
        click.echo(
            "warning: applying without --adjudicator switches every disputed row, "
            "including the ones an independent source would rule against",
            err=True,
        )

    with _staged_run(
        cfg,
        "ths_official_repair_bars",
        "ths-repair-bars",
        {"start": start, "end": end, "adjudicated": bool(judge)},
        record=apply,
    ) as (run_id, outcome):
        result = repair_deep_history_ths_official(
            cfg,
            run_id,
            start=parse_date_option(start, "--start") or date(2005, 1, 1),
            end=parse_date_option(end, "--end") or date(2015, 12, 31),
            dry_run=not apply,
            adjudicator=judge,
            diff_out=Path(diff_out) if diff_out else None,
            workers=workers,
        )
        outcome["result"] = result
    click.echo(json.dumps({"run_id": run_id, **result}, indent=2, default=str))


@ths_official_grp.command("resource-sectors")
@config_option
@click.option("--start", default="2022-01-04", show_default=True, help="Service floor.")
@click.option("--end", default=None, help="Defaults to today.")
@click.option(
    "--apply",
    is_flag=True,
    help="Actually write. Without it this fetches and reports, changing nothing.",
)
@click.option("--workers", default=4, show_default=True)
def ths_resource_sectors(config_path: str, start: str, end: str | None, apply: bool, workers: int):
    """Move sector_bars from the scraper to the licensed endpoint.

    sector_bars is the lake's only dataset with no second source at all: 303,559
    rows from an unauthenticated scrape of 10jqka's public pages. Unusually,
    there is no accuracy question to settle first — measured over 2,547
    comparable sessions, close, volume and turnover agreed within 10bps with a
    median difference of zero. Same numbers, licensed channel.

    Switching rather than routing, so `--apply` is required and also needs
    `[sources.ths_official] backfill = true`. The service floor at 2022-01-04
    leaves 12.3% of rows on the scraper; those years carry 2 boards in 2018 and
    39 in 2019, against 432 today.

    Stages rows — run `cne run compact --run-id <id>` afterwards.
    """
    from cnequity.steps.rotation import resource_sector_bars_ths_official

    _progress_logging()
    cfg = _cfg(config_path)
    problem = _require_key(cfg)
    if problem:
        _skip(problem)
        return

    attach_log_file(cfg, "ths-official-resource-sectors")
    with _staged_run(
        cfg,
        "ths_official_resource_sectors",
        "ths-sectors",
        {"start": start, "end": end},
        record=apply,
    ) as (run_id, outcome):
        result = resource_sector_bars_ths_official(
            cfg,
            run_id,
            start=parse_date_option(start, "--start"),
            end=parse_date_option(end, "--end") if end else None,
            workers=workers,
            dry_run=not apply,
        )
        outcome["result"] = result
    click.echo(json.dumps({"run_id": run_id, **result}, indent=2, default=str))
