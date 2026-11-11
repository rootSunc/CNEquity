"""`ths-official snapshot`, `backfill` and `repair-bars`.

Everything here needs a 同花顺 API key and does nothing without one. The source
is optional by design: a lake with no key keeps the sources it already has, and
these commands report that they were skipped rather than failing.

The split mirrors the one in the config. ``snapshot`` is verification — it
writes to ``meta/source_snapshots`` and never to curated, so it only feeds the
arbitration checks in `cne audit`. ``backfill`` and ``repair-bars`` change what
the lake holds and are gated separately, on ``[sources.ths_official].backfill``.

See ``docs/zh/getting-started/configuration.md`` (THS 官方接口) for the user-facing boundaries these
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
    attach_log_file,
    comma_option,
    comma_values,
    config_option,
    parse_date_option,
)
from cnequity.i18n import N_, _

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
    """Cross-check and backfill against the THS (同花顺) official API (needs a key).

    Without a key every command here reports "skipped" and changes nothing;
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

    Closed as soon as the fetch is done; `_publish_staged` then compacts the
    same run, so the command publishes what it staged instead of leaving a
    second command to the operator. Staging still needs that successful
    compact batch before anything will remove it.
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


def _publish_staged(cfg, run_id: str, result: dict, *, applied: bool = True) -> dict | None:
    """Compact what *run_id* staged, as `cne run compact --run-id` would.

    Skipped for a dry run and for a failed or empty fetch: there is nothing
    trustworthy to publish, and the run's staging stays for `cne run retry`.
    """
    status, _rows_read, rows_written = _run_outcome(result)
    if not applied or status == "failed" or rows_written <= 0:
        return None
    from cnequity.domain.market_time import shanghai_today
    from cnequity.orchestrator.engine import JobEngine

    return JobEngine(cfg).run_step("compact", shanghai_today(), run_id)


@ths_official_grp.command("capture")
@config_option
@click.option(
    "--what",
    type=click.Choice(["corporate-actions", "daily-bars", "financials", "valuations", "all"]),
    default="all",
    show_default=True,
    help=N_("Which peer snapshot to fetch."),
)
@click.option("--days", default=45, show_default=True, help=N_("Bar window in calendar days."))
@click.option(
    "--sample", default=400, show_default=True, help=N_("Number of securities to sample for bars.")
)
def ths_snapshot(config_path: str, what: str, days: int, sample: int):
    """Capture peer-source snapshots for audit arbitration, without overwriting curated data.

    Corporate actions prefer the full-history file; daily bars are sampled per --sample.
    These operations call the configured keyed API; with no snapshot, the matching
    arbitration checks stay silent.

    """
    from datetime import timedelta

    import polars as pl

    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
    from cnequity.steps.bars.ths_official import snapshot_daily_bars_ths_official
    from cnequity.steps.capital import snapshot_valuations_ths_official
    from cnequity.steps.fundamentals import (
        snapshot_corporate_actions_ths_official,
        snapshot_financials_ths_official,
    )

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
@click.option("--start", default="2016-01-01", show_default=True, help=N_("Start report period."))
@click.option("--end", default="2024-12-31", show_default=True, help=N_("End report period."))
@click.option(
    "--chunk-size", default=200, show_default=True, help=N_("Securities per staging batch.")
)
@click.option("--workers", default=4, show_default=True, help=N_("Number of concurrent requests."))
@comma_option(
    "--symbols",
    "symbols_str",
    default=None,
    help=(
        N_(
            "Comma-separated symbols instead of the whole market, to retry failed securities; the final JSON lists securities still missing in `failed_symbols`."
        )
    ),
)
def ths_backfill(
    config_path: str,
    start: str,
    end: str,
    chunk_size: int,
    workers: int,
    symbols_str: str | None,
):
    """Fill the balance sheets and cash-flow statements this lake lacks for 2016-2024.

    \b
    In that window those two statements cover only 0 to 37 securities a year, while the
    income statement covers 4,623 to 5,558; `backfill_missing_statement_types` has been
    reporting the hole all along. This is routing, not switching: the primary-key slots
    are empty, so no authoritative data is overwritten.

    \b
    Disclosure dates are borrowed from income-statement rows already in the lake, because
    the upstream's own date is the filing date of the **next** year, and using it would push
    every PIT date a full year late. A period with no date to borrow is skipped rather than
    given an invented one.

    \b
    Requires `[sources.ths_official] backfill = true`, because it changes what the lake holds.
    Fetches written to staging are compacted and published automatically; there is no need
    to run `cne run compact`.

    \b
    `failed_symbols` in the closing JSON tells upstream gaps from local problems:
    `Unknown thscode` means the peer has no such security, while a transport error means
    the request never got there. Re-run the second kind on its own with `--symbols`
    rather than repeating the whole market.

    """
    from cnequity.steps.fundamentals import backfill_statement_gap_ths_official

    cfg = _cfg(config_path)
    problem = _require_key(cfg)
    if problem:
        _skip(problem)
        return
    if not getattr(cfg, "ths_official_backfill_enabled", False):
        _skip("backfill disabled: set [sources.ths_official] backfill = true")
        return

    attach_log_file(cfg, "ths-official-backfill")
    symbols = comma_values(symbols_str, "--symbols")
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
    compact = _publish_staged(cfg, run_id, result)
    click.echo(json.dumps({"run_id": run_id, **result, "compact": compact}, indent=2, default=str))


@ths_official_grp.command("repair-bars")
@config_option
@click.option("--start", default=_DEEP_HISTORY_START, show_default=True)
@click.option("--end", default=_DEEP_HISTORY_END, show_default=True)
@click.option(
    "--apply",
    is_flag=True,
    help=N_("Actually write. Without it, only report differences and change nothing."),
)
@click.option(
    "--adjudicator",
    type=click.Path(exists=True, dir_okay=False),
    help=N_("Parquet of (symbol, trade_date, close) from an independent source."),
)
@click.option("--diff-out", type=click.Path(dir_okay=False), help=N_("Write disputed rows here."))
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
    moves the part the official API reaches: measured at 4,398,523 rows, 98.68%
    of that window once applied.

    **Switching, not routing**, so `--apply` is required and a bare run only
    reports. Pass `--adjudicator` as well and a *disputed* row is only switched
    when an independent source backs the peer: measured against baostock over
    1,418 disputes, the peer was right 1,167 times and the incumbent 251, with
    no three-way split, so switching blindly imports 251 known regressions.

    Build the adjudicator from a vendor sharing no lineage with either side.
    Both candidates here are THS (同花顺) sources, so they cannot arbitrate each other.

    """
    from pathlib import Path

    import polars as pl

    from cnequity.steps.bars.ths_official import repair_deep_history_ths_official

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
            _(
                "Warning: applying without --adjudicator switches every disputed row, including the ones an independent source would judge wrong"
            ),
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
    compact = _publish_staged(cfg, run_id, result, applied=apply)
    click.echo(json.dumps({"run_id": run_id, **result, "compact": compact}, indent=2, default=str))


@ths_official_grp.command("resource-sectors")
@config_option
@click.option("--start", default="2022-01-04", show_default=True, help=N_("Service start date."))
@click.option("--end", default=None, help=N_("Defaults to today."))
@click.option(
    "--apply",
    is_flag=True,
    help=N_("Actually write. Without it, only fetch and report, changing nothing."),
)
@click.option("--workers", default=4, show_default=True)
def ths_resource_sectors(config_path: str, start: str, end: str | None, apply: bool, workers: int):
    """Cross-check and explicitly replace the sector-quote source through the keyed API.

    By default it also fetches over the network and reports; only --apply writes staging,
    and writing additionally needs [sources.ths_official] backfill = true. Coverage depends
    on what the source can provide: it does not promise to replace all of this lake's history,
    and does not delete old rows the source did not return.

    Writes to staging are compacted and published automatically.

    """
    from cnequity.steps.rotation import resource_sector_bars_ths_official

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
    compact = _publish_staged(cfg, run_id, result, applied=apply)
    click.echo(json.dumps({"run_id": run_id, **result, "compact": compact}, indent=2, default=str))
