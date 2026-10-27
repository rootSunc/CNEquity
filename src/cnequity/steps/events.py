"""L2 corporate-event steps: corporate_actions, announcement_index,
earnings_disclosure_schedule."""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import polars as pl

from cnequity.adapters.baostock.corporate_actions import fetch_corporate_actions_baostock
from cnequity.adapters.cninfo.announcements import (
    CNINFO_SOURCE_REVISION,
    fetch_announcement_index,
    fetch_announcement_index_range,
)
from cnequity.adapters.eastmoney.corporate_actions import fetch_corporate_actions_eastmoney
from cnequity.adapters.eastmoney.earnings_disclosure import (
    _backfill_report_dates,
    fetch_earnings_disclosure_schedule,
)
from cnequity.adapters.tdx_protocol.client import (
    CORPORATE_ACTIONS_BACKFILL_START,
    fetch_corporate_actions,
)
from cnequity.adapters.ths.corporate_actions import fetch_corporate_actions_ths
from cnequity.config import Config
from cnequity.domain.datasets import get_dataset
from cnequity.domain.schemas import with_provenance
from cnequity.domain.symbols import filter_ingest_universe
from cnequity.orchestrator.manifest import Manifest
from cnequity.orchestrator.outcomes import SourcePayloadError, SourceUnavailableError
from cnequity.orchestrator.registry import register_step
from cnequity.orchestrator.source_gaps import record_source_gap, source_gap_dates
from cnequity.progress import sweep_progress
from cnequity.quality.ex_events import (
    EX_EVENT_LOOKBACK_SESSIONS,
    unexplained_factor_steps,
)
from cnequity.quality.failover import (
    failover_spec,
    snapshot_corporate_actions_backup,
    snapshot_corporate_actions_tdx_backup,
)
from cnequity.steps.common import (
    fetch_incremental_daily,
    incremental_trade_dates,
    instrument_metadata,
    load_symbols,
)
from cnequity.steps.http_common import run_incremental_fetched, verify_raw_archive, write_fetched
from cnequity.storage.observation import note_observation

# TDX xdxr is per-symbol (backfill); EastMoney datacenter supports ex-date filter (daily).
_CANONICAL_BACKFILL = "tdx_protocol"
_CANONICAL_DAILY = "eastmoney"
_CORPORATE_ACTIONS_CHUNK_TASK = "corporate_actions_chunk"
_MIN_EARNINGS_SCHEDULE_SYMBOLS_PER_PERIOD = 100
_CNINFO_CHECKPOINT_TTL_DAYS = 7


def _corporate_actions_chunk_id(symbols: list[str]) -> str:
    """A chunk id derived from the symbols it covers, not from where it sits.

    The id used to embed the parent batch's uuid and the chunk's position, so
    it changed on every attempt and shifted whenever an earlier chunk
    succeeded and shortened the remaining list. Neither a plan written before
    a sweep nor the receipts left by the last one could then be matched to the
    work in front of it. The symbol set is what identifies a chunk across
    attempts.
    """
    digest = hashlib.sha1("|".join(symbols).encode("utf-8")).hexdigest()[:12]
    return f"corporate_actions-chunk-{digest}"


logger = logging.getLogger(__name__)


def _cninfo_checkpoint(config: Config, dataset: str, start: date, end: date):
    """Stable, range-scoped checkpoint path for a resumable CNINFO fetch."""
    return (
        config.meta_root / "checkpoints" / dataset / f"{start.isoformat()}_{end.isoformat()}.json"
    )


def _record_cninfo_metrics(
    config: Config,
    run_id: str,
    dataset: str,
    metrics: dict,
    *,
    batch_id: str | None = None,
) -> None:
    """Persist source/page metrics in the run manifest without making them a gate."""
    try:
        manifest = Manifest(config.manifest_path)
        manifest.record_performance_metrics(run_id, dataset, metrics)
        if batch_id:
            manifest.record_batch_telemetry(run_id, batch_id, metrics)
    except Exception as exc:  # noqa: BLE001 — telemetry must not lose data
        logger.warning("%s: unable to persist CNINFO metrics: %s", dataset, exc)


def _cninfo_checkpoint_options(
    config: Config, dataset: str, start: date, end: date | None = None
) -> dict:
    """Resolve the production checkpoint policy for one CNINFO feed.

    Running checkpoints are useful only when they are tied to the provider
    contract and bounded in age.  Keep the defaults here (rather than relying
    on adapter call defaults) so production step calls cannot accidentally
    omit the source revision/TTL when a new adapter is introduced.
    """
    ttl = getattr(config, "cninfo_checkpoint_ttl_days", _CNINFO_CHECKPOINT_TTL_DAYS)
    if ttl is not None:
        ttl = int(ttl)
        if ttl < 0:
            raise ValueError("cninfo_checkpoint_ttl_days must be >= 0")
    configured_revision = getattr(config, "cninfo_source_revision", None)
    options = {
        "checkpoint_ttl_days": ttl,
        "source_revision": str(configured_revision or CNINFO_SOURCE_REVISION),
        # A normal run refreshes completed slices so same-date corrections are
        # observed; an explicit config override can opt into a resume cache.
        "refresh": bool(getattr(config, "cninfo_checkpoint_refresh", True)),
    }
    if getattr(config, "meta_root", None) is not None:
        options["checkpoint_path"] = _cninfo_checkpoint(config, dataset, start, end or start)
    return options


def _fetch_cninfo_single(
    fetcher,
    day: date,
    config: Config,
    metrics: dict,
    *,
    dataset: str = "announcement_index",
) -> pl.DataFrame:
    """Call a CNINFO adapter with metrics while keeping lightweight test doubles compatible."""
    try:
        parameters = inspect.signature(fetcher).parameters
    except (TypeError, ValueError):
        parameters = {}
    accepts_var_kwargs = any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()
    )
    options = {
        **_cninfo_checkpoint_options(config, dataset, day),
        "config": config,
        "metrics": metrics,
    }
    label = "announcement" if dataset == "announcement_index" else "regulatory"
    options.update(
        {
            "run_id": metrics.get("run_id"),
            "request_scope": f"range:{label}:{day.isoformat()}:{day.isoformat()}",
        }
    )
    kwargs = {
        key: value for key, value in options.items() if accepts_var_kwargs or key in parameters
    }
    return fetcher(day, **kwargs)


def _cninfo_range_backfill(
    config: Config,
    trade_date: date,
    run_id: str,
    dataset: str,
    fetch_range,
    *,
    date_col: str,
    floor: date,
    batch_id: str | None = None,
) -> dict:
    """Use bounded range-aware CNINFO walks behind the normal day-stage helper.

    CNINFO's pagination is not a durable cursor: broad ``seDate`` requests
    have returned the same page again at a deep offset (and occasionally
    failed at a much shallower one).  Running the complete historical window
    through one request would make the whole backfill depend on that single
    page walk and would also retain every row until the final compact.  The
    registry therefore gives these feeds a small calendar chunk.  Each chunk
    gets its own source capture and checkpoint identity, while the adapter's
    recursive split remains the last line of defence for an unusually busy
    window/day.

    A chunk is published at most once (the registry value is below
    ``walk_day_backfill``'s 60-day flush boundary), which is important for the
    exact-wire capture receipt: consuming one receipt per chunk cannot make a
    later flush fail with "capture already consumed".  The caller's private
    backfill bounds are restored even when a page walk raises, so a failed
    chunk can be resumed with the same persisted checkpoint on the next run.
    """
    from cnequity.steps.common import walk_day_backfill

    requested_start = getattr(config, "_backfill_start", None) or floor
    requested_end = min(getattr(config, "_backfill_end", None) or trade_date, trade_date)
    if requested_start > requested_end:
        return {
            "rows_read": 0,
            "rows_written": 0,
            "days_fetched": 0,
            "days_skipped": 0,
            "days_empty": 0,
            "slices": [],
        }

    # Keep the window bounded even when the caller invokes the step directly
    # (the CLI's generic ``_backfill_chunked`` path is only selected when both
    # --start and --end were supplied).  A malformed registry value must not
    # turn into an infinite loop or silently disable the guard.
    configured_chunk_days = get_dataset(dataset).backfill_chunk_days
    chunk_days = int(configured_chunk_days or 0)
    if chunk_days < 1:
        raise RuntimeError(f"{dataset}: CNINFO backfill chunk size must be a positive integer")

    original_start_present = hasattr(config, "_backfill_start")
    original_end_present = hasattr(config, "_backfill_end")
    original_start = getattr(config, "_backfill_start", None)
    original_end = getattr(config, "_backfill_end", None)
    slices: list[dict] = []
    total_rows_read = 0
    total_rows_written = 0
    total_days_fetched = 0
    total_days_skipped = 0
    total_days_empty = 0
    overall_status = "success"
    all_findings: list[dict] = []
    all_metrics: list[dict] = []
    label = "announcement" if dataset == "announcement_index" else "regulatory"

    def run_slice(start: date, end: date) -> dict:
        """Fetch and stage one bounded date window."""
        metrics: dict = {
            "run_id": run_id,
            "slice_start": start.isoformat(),
            "slice_end": end.isoformat(),
        }
        # Register before fetching so a failing later slice participates in
        # the run-level retry total as well as successful earlier slices.
        all_metrics.append(metrics)
        cached: dict[str, pl.DataFrame] = {}
        request_scope = f"range:{label}:{start.isoformat()}:{end.isoformat()}"

        def fetch_one(day: date) -> pl.DataFrame:
            if "frame" not in cached:
                # Keep the source observation tied to this run even when a
                # lightweight range adapter accepts an explicit run_id instead
                # of reading it from the metrics object. Signature filtering
                # preserves narrow offline doubles for archive-disabled tests.
                options = {
                    "config": config,
                    "metrics": metrics,
                    "run_id": run_id,
                    "request_scope": request_scope,
                }
                options.update(_cninfo_checkpoint_options(config, dataset, start, end))
                try:
                    parameters = inspect.signature(fetch_range).parameters
                except (TypeError, ValueError):
                    parameters = {}
                accepts_var_kwargs = any(
                    parameter.kind is inspect.Parameter.VAR_KEYWORD
                    for parameter in parameters.values()
                )
                if not accepts_var_kwargs:
                    options = {key: value for key, value in options.items() if key in parameters}
                try:
                    cached["frame"] = fetch_range(start, end, **options)
                except Exception:
                    # The adapter persists its page checkpoint before
                    # propagating a transport/contract failure. Preserve the
                    # matching per-slice metrics as well; the next run can
                    # identify the exact bounded window that needs a retry.
                    metrics["status"] = "failed"
                    _record_cninfo_metrics(config, run_id, dataset, metrics, batch_id=batch_id)
                    raise
                _record_cninfo_metrics(config, run_id, dataset, metrics, batch_id=batch_id)
            frame = cached["frame"]
            if frame.is_empty() or date_col not in frame.columns:
                return pl.DataFrame()
            return frame.filter(pl.col(date_col) == day)

        def publish(part: pl.DataFrame, batch_id: str) -> object:
            # CNINFO range adapters archive each POST response before returning
            # their normalized frame. Verification happens immediately before
            # the writer boundary; a missing/captureless receipt therefore
            # leaves no staging artifact to be mistaken for success.
            evidence = (
                verify_raw_archive(
                    config,
                    dataset,
                    run_id,
                    source="cninfo",
                    request_scope=request_scope,
                )
                if config.should_archive_raw(dataset)
                else None
            )
            return write_fetched(
                config,
                run_id,
                dataset,
                part,
                source="cninfo",
                # ``walk_day_backfill`` numbers parts from zero on every
                # invocation. Include the slice in the staging key so a later
                # slice cannot replace an earlier ``bf-0000`` in this run.
                batch_id=f"{start.isoformat()}_{end.isoformat()}-{batch_id}",
                raw_archive_evidence=evidence,
            )

        # ``walk_day_backfill`` reads these private bounds to decide its date
        # list. They are set by the outer loop and restored in the caller's
        # finally block.
        result = walk_day_backfill(
            config,
            end,
            run_id,
            dataset,
            fetch_one,
            source="cninfo",
            date_col=date_col,
            floor=start,
            calendar_days=True,
            # CNINFO returns a fully bounded range response. A missing day in
            # that successful response is a valid negative observation for
            # sparse announcements/regulatory events, not a fetch failure.
            allow_empty_days=True,
            publish_fn=publish,
        )
        result["metrics"] = metrics
        return result

    def aggregate_cninfo_metrics() -> dict:
        return {
            "run_id": run_id,
            "dataset": dataset,
            "slices": all_metrics,
            "slice_count": len(all_metrics),
            "requests": sum(int(item.get("requests", 0) or 0) for item in all_metrics),
            "retries": sum(int(item.get("retries", 0) or 0) for item in all_metrics),
            "pages": sum(int(item.get("pages", 0) or 0) for item in all_metrics),
            "split_reasons": sum(int(item.get("split_reasons", 0) or 0) for item in all_metrics),
        }

    try:
        cursor = requested_start
        while cursor <= requested_end:
            slice_end = min(cursor + timedelta(days=chunk_days - 1), requested_end)
            config._backfill_start = cursor
            config._backfill_end = slice_end
            result = run_slice(cursor, slice_end)
            rows_read = int(result.get("rows_read", 0) or 0)
            rows_written = int(result.get("rows_written", 0) or 0)
            days_fetched = int(result.get("days_fetched", 0) or 0)
            days_skipped = int(result.get("days_skipped", 0) or 0)
            days_empty = int(result.get("days_empty", 0) or 0)
            total_rows_read += rows_read
            total_rows_written += rows_written
            total_days_fetched += days_fetched
            total_days_skipped += days_skipped
            total_days_empty += days_empty
            slice_info = {
                "start": cursor,
                "end": slice_end,
                "status": result.get("status", "success"),
                "rows_read": rows_read,
                "rows_written": rows_written,
                "days_fetched": days_fetched,
                "days_skipped": days_skipped,
                "days_empty": days_empty,
            }
            slices.append(slice_info)
            updates = result.get("context_updates")
            if isinstance(updates, dict):
                findings = updates.get("audit_findings")
                if isinstance(findings, list):
                    all_findings.extend(item for item in findings if isinstance(item, dict))
            if result.get("status") == "warning":
                overall_status = "warning"
            cursor = slice_end + timedelta(days=1)
    except Exception:
        # The normal aggregate below is unreachable when a later date slice
        # raises. Persist cumulative metrics here before propagating so batch
        # telemetry reflects all request retries already spent in this step.
        _record_cninfo_metrics(
            config,
            run_id,
            dataset,
            aggregate_cninfo_metrics(),
            batch_id=batch_id,
        )
        raise
    finally:
        if original_start_present:
            config._backfill_start = original_start
        else:
            try:
                delattr(config, "_backfill_start")
            except AttributeError:
                pass
        if original_end_present:
            config._backfill_end = original_end
        else:
            try:
                delattr(config, "_backfill_end")
            except AttributeError:
                pass

    # Persist one aggregate performance document. Per-slice metrics remain in
    # the document so operators can identify the exact date window that was
    # slow or failed, while aggregate counters make run-level SLOs cheap to
    # query.
    aggregate_metrics = aggregate_cninfo_metrics()
    _record_cninfo_metrics(config, run_id, dataset, aggregate_metrics, batch_id=batch_id)
    result = {
        "rows_read": total_rows_read,
        "rows_written": total_rows_written,
        "days_fetched": total_days_fetched,
        "days_skipped": total_days_skipped,
        "days_empty": total_days_empty,
        "slices": slices,
        "status": overall_status,
        "metrics": aggregate_metrics,
    }
    if all_findings:
        result["context_updates"] = {"audit_findings": all_findings}
    return result


def _delisted_windows(
    config: Config,
    symbols: list[str],
    start: date,
    end: date,
    exchanges: tuple[str, ...],
) -> dict[str, tuple[date, date]]:
    """Scope an explicit repair source to delisted symbols and listing windows."""
    metadata = instrument_metadata(config)
    if metadata.is_empty() or "delist_date" not in metadata.columns:
        return {}
    symbol_set = set(symbols)
    suffixes = tuple(f".{exchange}" for exchange in exchanges)
    windows: dict[str, tuple[date, date]] = {}
    for row in metadata.iter_rows(named=True):
        symbol = str(row.get("symbol") or "")
        if (
            symbol not in symbol_set
            or not symbol.endswith(suffixes)
            or row.get("delist_date") is None
        ):
            continue
        symbol_start = max(start, row["list_date"]) if row.get("list_date") else start
        symbol_end = min(end, row["delist_date"]) if row.get("delist_date") else end
        if symbol_start <= symbol_end:
            windows[symbol] = (symbol_start, symbol_end)
    return windows


def _delisted_sh_sz_windows(
    config: Config,
    symbols: list[str],
    start: date,
    end: date,
) -> dict[str, tuple[date, date]]:
    return _delisted_windows(config, symbols, start, end, ("SH", "SZ"))


def _delisted_bj_windows(
    config: Config,
    symbols: list[str],
    start: date,
    end: date,
) -> dict[str, tuple[date, date]]:
    from cnequity.domain.market_profile import COVERAGE

    return _delisted_windows(
        config, symbols, start, end, tuple(sorted(COVERAGE["ths_delisted_actions"]))
    )


def _validate_earnings_schedule_snapshot(df: pl.DataFrame) -> pl.DataFrame:
    """Reject a non-empty but obviously truncated report-period snapshot."""
    if df.is_empty():
        return df
    required = {"symbol", "report_period"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise SourcePayloadError(
            "earnings_disclosure_schedule: response is missing required column(s): "
            + ", ".join(missing)
        )
    counts = (
        df.unique(subset=["symbol", "report_period"])
        .group_by("report_period")
        .agg(pl.len().alias("_symbol_count"))
        .filter(pl.col("_symbol_count") < _MIN_EARNINGS_SCHEDULE_SYMBOLS_PER_PERIOD)
    )
    if not counts.is_empty():
        details = ", ".join(
            f"{row['report_period']}={row['_symbol_count']}" for row in counts.iter_rows(named=True)
        )
        record_source_gap(
            "earnings_disclosure_schedule",
            (
                "earnings_disclosure_schedule: incomplete report-period snapshot; each "
                f"observed period needs at least {_MIN_EARNINGS_SCHEDULE_SYMBOLS_PER_PERIOD} "
                f"unique symbol(s) ({details})"
            ),
            frame=df,
        )
    return df


def _heal_unrecorded_ex_events(config: Config, trade_date: date, run_id: str, rate_limit) -> int:
    """Ask TDX for a recent ex-event the factor series shows and nobody recorded.

    The daily source is EastMoney's dividend report, which has no concept of a
    fund unit split — `_map_action_type` knows 配/转/送/派 and nothing else — so
    a 份额折算 only ever reached the lake through a symbol backfill somebody
    remembered to run. xdxr carries it (category 11), and the factor series
    names the day, so the two together make the repair automatic instead of
    remembered.

    Best effort by construction: the daily fetch has already succeeded by the
    time this runs, and an unreachable peer must not turn a good day into a
    failed step.
    """
    if not config.sources.get("tdx_protocol", True):
        return 0
    try:
        suspects = unexplained_factor_steps(config, upto=trade_date)
        if suspects.is_empty():
            return 0
        symbols = sorted(suspects.get_column("symbol").unique().to_list())
        scope = f"heal:xdxr:{trade_date.isoformat()}"
        logger.info(
            "corporate_actions: %d unrecorded factor step(s) over the last %d session(s); "
            "asking TDX about %d symbol(s)",
            suspects.height,
            EX_EVENT_LOOKBACK_SESSIONS,
            len(symbols),
        )
        frame = fetch_corporate_actions(
            trade_date,
            symbols=symbols,
            backfill=True,
            rate_limit=rate_limit,
            allow_mock=config.tdx_allow_mock,
            primary_only=True,
            config=config,
            run_id=run_id,
            fail_loud=False,
            allow_empty=True,
            request_scope=scope,
        )
        if frame.is_empty():
            return 0
        # Only the days in question: a symbol's whole xdxr history is not what
        # a daily run was asked for, and the rest of it is already in the lake.
        healed = frame.join(
            suspects.select("symbol", "ex_date"), on=["symbol", "ex_date"], how="inner"
        )
        if healed.is_empty():
            logger.info(
                "corporate_actions: TDX has nothing for %d unrecorded step(s); "
                "they fall out of the window on their own",
                suspects.height,
            )
            return 0
        healed = with_provenance(healed, source=_CANONICAL_BACKFILL, data_version="v1")
        result = write_fetched(
            config,
            run_id,
            "corporate_actions",
            healed,
            source=_CANONICAL_BACKFILL,
            batch_id="batch-0-ex-event-heal",
            raw_archive_evidence=(
                verify_raw_archive(
                    config,
                    "corporate_actions",
                    run_id,
                    source=_CANONICAL_BACKFILL,
                    request_scope=scope,
                )
                if config.should_archive_raw("corporate_actions")
                else None
            ),
        )
        logger.info(
            "corporate_actions: recorded %d previously unexplained ex-event(s)", healed.height
        )
        return int(result.get("rows_written", healed.height))
    except Exception as exc:  # noqa: BLE001 — opportunistic repair, never the day's verdict
        logger.warning(
            "corporate_actions: ex-event self-heal unavailable (%s: %s); "
            "the audit still reports the gap",
            type(exc).__name__,
            exc,
        )
        return 0


#: Quotes per TDX request (the protocol's batch limit).
_QUOTE_BATCH = 80


def _previous_closes(config: Config, trade_date: date, symbols: list[str]) -> dict[str, float]:
    """Each symbol's last stored close before *trade_date*."""
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    root = config.curated_root / "daily_bars"
    if not symbols or not dataset_has_parquet(root):
        return {}
    start = trade_date - timedelta(days=21)
    frame = (
        scan_parquet_root(root, partition_col="trade_date", start=start, end=trade_date)
        .filter(
            (pl.col("trade_date") < trade_date)
            & pl.col("symbol").is_in(symbols)
            & pl.col("close").is_not_null()
        )
        .sort("trade_date")
        .group_by("symbol")
        .agg(pl.col("close").last())
        .collect()
    )
    return dict(frame.iter_rows())


def _stored_vendor_shares(config: Config, since: date, until: date) -> pl.DataFrame:
    """Stored 送/转 rows from sources other than TDX, ex-dated in the window."""
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    root = config.curated_root / "corporate_actions"
    if not dataset_has_parquet(root):
        return pl.DataFrame()
    return (
        scan_parquet_root(root, partition_col="ex_date", start=since, end=until)
        .filter(
            pl.col("ex_date").is_between(since, until)
            & pl.col("action_type").is_in(["bonus", "transfer"])
            & (pl.col("source") != _CANONICAL_BACKFILL)
        )
        .select("symbol", "ex_date", "action_type", "bonus_ratio", "transfer_ratio")
        .collect()
    )


def detect_ex_symbols(
    config: Config, trade_date: date, symbols: list[str], *, client=None
) -> set[str]:
    """Symbols whose exchange previous close for *trade_date* is not their last close.

    On an ex-date the exchange publishes an adjusted previous close (前收盘),
    and TDX's quote carries it, so one request per 80 codes finds the whole
    market's ex-events without asking each symbol for its history. Measured
    2026-10-27 before the session: exactly EastMoney's six announced ex-dates,
    each gap equal to the cash dividend rounded to the tick. Blind to a cash
    dividend under half a tick; the EastMoney window and the weekly sweep
    catch those. Only meaningful while *trade_date* is the current session.
    """
    from cnequity.adapters.tdx_protocol._wire.helper import get_security_coefficient
    from cnequity.adapters.tdx_protocol.client import _quotes_client
    from cnequity.domain.symbols import parse_symbol

    previous = _previous_closes(config, trade_date, symbols)
    pairs = []
    for symbol in symbols:
        if symbol not in previous:
            continue
        try:
            info = parse_symbol(symbol)
        except ValueError:
            continue
        if info.exchange in ("SH", "SZ"):
            pairs.append((1 if info.exchange == "SH" else 0, info.code, symbol))
    owns = client is None
    client = client or _quotes_client(config)
    found: set[str] = set()
    try:
        for start in range(0, len(pairs), _QUOTE_BATCH):
            batch = pairs[start : start + _QUOTE_BATCH]
            by_code = {(market, code): symbol for market, code, symbol in batch}
            for quote in client.security_quotes([(m, c) for m, c, _ in batch]):
                symbol = by_code.get((quote.get("market"), quote.get("code")))
                raw = quote.get("last_close_raw")
                if symbol is None or not raw:
                    continue
                try:
                    tick = get_security_coefficient(quote["market"], quote["code"])
                except NotImplementedError:
                    # A code range the wire helper cannot price; the weekly
                    # sweep still reaches it.
                    continue
                if abs(raw * tick - previous[symbol]) >= tick / 2:
                    found.add(symbol)
    finally:
        if owns:
            client.close()
    return found


def _tdx_primary_events(
    config: Config, trade_date: date, run_id: str, rate_limit
) -> tuple[pl.DataFrame, str]:
    """TDX's ex-rights records for the session's detected ex-events."""
    symbols = detect_ex_symbols(config, trade_date, _sweep_universe(config))
    scope = f"primary:xdxr:{trade_date.isoformat()}"
    if not symbols:
        return pl.DataFrame(), scope
    frame = fetch_corporate_actions(
        trade_date,
        symbols=sorted(symbols),
        backfill=True,
        rate_limit=rate_limit,
        allow_mock=config.tdx_allow_mock,
        primary_only=True,
        config=config,
        run_id=run_id,
        fail_loud=True,
        allow_empty=True,
        request_scope=scope,
    )
    if frame.is_empty():
        return frame, scope
    return frame.filter(pl.col("ex_date") == trade_date), scope


def _split_with_vendor(
    tdx: pl.DataFrame, vendor: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Hand a share distribution to the vendor row when it splits 送 from 转.

    TDX records 送股 and 转增 as one ``bonus`` ratio; EastMoney keeps them
    apart. Where EastMoney's two add up to TDX's total for the same symbol and
    ex-date, TDX's combined row is dropped and EastMoney's are kept; elsewhere
    TDX's row stands. Returns (tdx rows to write, vendor rows taking over).
    """
    if tdx.is_empty() or vendor.is_empty() or "bonus_ratio" not in tdx.columns:
        return tdx, vendor.clear()
    keys = ["symbol", "ex_date"]
    combined = tdx.filter((pl.col("action_type") == "bonus") & (pl.col("bonus_ratio") > 0))
    shares = (
        vendor.filter(pl.col("action_type").is_in(["bonus", "transfer"]))
        .group_by(keys)
        .agg(
            (pl.col("bonus_ratio").fill_null(0) + pl.col("transfer_ratio").fill_null(0))
            .sum()
            .alias("_vendor_total")
        )
    )
    agreed = (
        combined.join(shares, on=keys)
        # TDX ratios carry five decimals (0.51471 against 0.5147058).
        .filter((pl.col("bonus_ratio") - pl.col("_vendor_total")).abs() <= 1e-4)
        .select(keys)
    )
    if agreed.is_empty():
        return tdx, vendor.clear()
    kept = tdx.join(
        agreed.with_columns(pl.lit("bonus").alias("action_type")),
        on=[*keys, "action_type"],
        how="anti",
    )
    taking_over = vendor.filter(pl.col("action_type").is_in(["bonus", "transfer"])).join(
        agreed, on=keys, how="semi"
    )
    return kept, taking_over


def _peer_finding(tdx: pl.DataFrame, vendor: pl.DataFrame, trade_date: date) -> dict | None:
    """Compare the session's events on both sources: missing on either, or different."""
    keys = ["symbol", "ex_date"]

    def totals(frame: pl.DataFrame, prefix: str) -> pl.DataFrame:
        if frame.is_empty():
            return pl.DataFrame(
                schema={
                    "symbol": pl.Utf8,
                    "ex_date": pl.Date,
                    f"{prefix}cash": pl.Float64,
                    f"{prefix}shares": pl.Float64,
                }
            )
        return (
            frame.filter(pl.col("ex_date") == trade_date)
            .filter(pl.col("action_type") != "unit_split")
            .group_by(keys)
            .agg(
                pl.col("cash_dividend").fill_null(0).sum().alias(f"{prefix}cash"),
                (pl.col("bonus_ratio").fill_null(0) + pl.col("transfer_ratio").fill_null(0))
                .sum()
                .alias(f"{prefix}shares"),
            )
        )

    joined = totals(tdx, "t_").join(totals(vendor, "v_"), on=keys, how="full", coalesce=True)
    if joined.is_empty():
        return None
    vendor_only = joined.filter(pl.col("t_cash").is_null())["symbol"].to_list()
    tdx_only = joined.filter(pl.col("v_cash").is_null())["symbol"].to_list()
    both = joined.filter(pl.col("t_cash").is_not_null() & pl.col("v_cash").is_not_null())
    differ = both.filter(
        ((pl.col("t_cash") - pl.col("v_cash")).abs() > 1e-4)
        | ((pl.col("t_shares") - pl.col("v_shares")).abs() > 1e-4)
    )["symbol"].to_list()
    return {
        "dataset": "corporate_actions",
        "severity": "info" if not (vendor_only or differ) else "warning",
        "check": "corporate_actions_peer",
        "trade_date": trade_date.isoformat(),
        "message": (
            f"corporate_actions {trade_date}: TDX and EastMoney agree on "
            f"{both.height - len(differ)} event(s); {len(differ)} differ, "
            f"{len(vendor_only)} only on EastMoney, {len(tdx_only)} only on TDX"
        ),
        "differ": sorted(differ),
        "eastmoney_only": sorted(vendor_only),
        "tdx_only": sorted(tdx_only),
    }


def _funds_ingested(config: Config) -> bool:
    return bool(config.ingest_listed_funds or config.ingest_eligible_etfs)


def _sweep_universe(config: Config) -> list[str]:
    """Symbols a full xdxr sweep asks TDX about.

    Funds are left out while this lake ingests no fund bars: nothing reads their
    actions, and they were a quarter of a first build's sweep (2,161 of 8,075).
    Turning fund ingestion on later is caught up by
    :func:`_sweep_unswept_fund_history`, so leaving them out loses nothing.
    """
    symbols = load_symbols(config)
    if _funds_ingested(config):
        return symbols
    from cnequity.steps.common import instrument_metadata

    funds = set(
        instrument_metadata(config)
        .filter(pl.col("asset_type").is_in(["etf", "lof"]))
        .get_column("symbol")
        .to_list()
    )
    kept = [symbol for symbol in symbols if symbol not in funds]
    if len(kept) < len(symbols):
        logger.info(
            "corporate_actions: %d fund code(s) left out of the sweep; this lake ingests no fund bars",
            len(symbols) - len(kept),
        )
    return kept


def _swept_symbols(config: Config) -> set[str]:
    """Every symbol whose whole xdxr history a successful chunk has recorded."""
    swept: set[str] = set()
    for row in Manifest(config.manifest_path).successful_batches_for_task(
        _CORPORATE_ACTIONS_CHUNK_TASK
    ):
        swept.update(json.loads(row["symbols_json"] or "[]"))
    return swept


#: Funds caught up per daily run; a lake that just enabled fund ingestion
#: spreads its one-time ~2,200-code history sweep over a few days.
FUND_HISTORY_SWEEP_LIMIT = 500


def _sweep_unswept_fund_history(
    config: Config, trade_date: date, run_id: str, rate_limit
) -> tuple[int, dict | None]:
    """Fetch the xdxr history of ingested funds no sweep has covered yet.

    The daily source has no history, so a fund's past splits only ever reach
    the lake through a sweep. A lake whose sweep skipped funds (fund ingestion
    off) or a fund listed since then would otherwise keep a factor series
    without its old splits, and nothing would say so. Each chunk records the
    same receipt as a backfill chunk, so a fund is swept once. Best effort: an
    unreachable TDX leaves the funds owed for the next run.
    """
    if not _funds_ingested(config) or not config.sources.get("tdx_protocol", True):
        return 0, None
    try:
        from cnequity.steps.bars import _daily_fund_bar_symbols

        scope = _daily_fund_bar_symbols(config, trade_date) or []
        owed = sorted(set(scope) - _swept_symbols(config))
        if not owed:
            return 0, None
        batch = owed[:FUND_HISTORY_SWEEP_LIMIT]
        logger.info(
            "corporate_actions: sweeping xdxr history for %d fund(s) never swept (%d owed)",
            len(batch),
            len(owed),
        )
        manifest = Manifest(config.manifest_path)
        written = 0
        swept: set[str] = set()
        size = max(1, config.batch_size)
        for index in range(0, len(batch), size):
            chunk = batch[index : index + size]
            chunk_batch_id = f"fund-history-{_corporate_actions_chunk_id(chunk)}"
            scope_id = f"chunk:{chunk_batch_id}"
            try:
                frame = fetch_corporate_actions(
                    trade_date,
                    symbols=chunk,
                    backfill=True,
                    rate_limit=rate_limit,
                    allow_mock=config.tdx_allow_mock,
                    primary_only=True,
                    config=config,
                    run_id=run_id,
                    fail_loud=True,
                    allow_empty=True,
                    request_scope=scope_id,
                )
            except Exception as exc:  # noqa: BLE001 — owed funds wait for the next run
                logger.warning("corporate_actions: fund history chunk failed: %s", exc)
                continue
            if not frame.is_empty():
                frame = frame.with_columns(pl.col("ex_date").cast(pl.Date, strict=False)).filter(
                    pl.col("ex_date").is_between(CORPORATE_ACTIONS_BACKFILL_START, trade_date)
                )
            if not frame.is_empty():
                frame = with_provenance(frame, source=_CANONICAL_BACKFILL, data_version="v1")
                result = write_fetched(
                    config,
                    run_id,
                    "corporate_actions",
                    frame,
                    source=_CANONICAL_BACKFILL,
                    batch_id=chunk_batch_id,
                    raw_archive_evidence=(
                        verify_raw_archive(
                            config,
                            "corporate_actions",
                            run_id,
                            source=_CANONICAL_BACKFILL,
                            request_scope=scope_id,
                        )
                        if config.should_archive_raw("corporate_actions")
                        else None
                    ),
                )
                written += int(result.get("rows_written", frame.height))
            manifest.start_batch(
                run_id,
                f"{chunk_batch_id}-receipt",
                task_id=_CORPORATE_ACTIONS_CHUNK_TASK,
                dataset="corporate_actions",
                symbols=chunk,
                window_start=CORPORATE_ACTIONS_BACKFILL_START.isoformat(),
                window_end=trade_date.isoformat(),
                blocks_compaction=False,
            )
            manifest.finish_batch(
                run_id,
                f"{chunk_batch_id}-receipt",
                "success",
                rows_read=frame.height,
                rows_written=frame.height,
            )
            swept.update(chunk)
    except Exception as exc:  # noqa: BLE001 — catch-up, never the day's verdict
        logger.warning("corporate_actions: fund history catch-up unavailable: %s", exc)
        return 0, None
    remaining = sorted(set(owed) - swept)
    if not remaining:
        return written, None
    return written, {
        "dataset": "corporate_actions",
        "severity": "warning",
        "check": "fund_history_unswept",
        "message": (
            f"{len(remaining)} ingested fund(s) still lack their xdxr history; old splits are "
            "missing from their factors until a later run sweeps them"
        ),
        "symbols": remaining[:20],
    }


def _window_fetcher(config: Config, trade_date: date, run_id: str):
    """Per-day fetch for the reconciliation window, served by one query.

    The first call asks for the whole window (``incremental_trade_dates``) in a
    single ``in``-list request and later days are sliced from it; if that
    request fails, each day falls back to its own exact-date query as before.
    """
    from cnequity.steps.common import incremental_trade_dates

    state: dict = {}

    def _one(d: date) -> pl.DataFrame:
        return fetch_corporate_actions_eastmoney(
            d,
            backfill=False,
            config=config,
            run_id=run_id,
            request_scope=f"daily:{d.isoformat()}",
        )

    def fetch(d: date) -> pl.DataFrame:
        if "window" not in state:
            window = incremental_trade_dates(config, "corporate_actions", trade_date)
            state["window"] = set(window)
            try:
                state["frame"] = fetch_corporate_actions_eastmoney(
                    trade_date,
                    backfill=False,
                    config=config,
                    run_id=run_id,
                    request_scope=f"daily:{trade_date.isoformat()}",
                    dates=window,
                )
            except Exception as exc:  # noqa: BLE001 — per-day queries still work
                logger.warning(
                    "corporate_actions: window query failed (%s); falling back to per-day", exc
                )
                state["frame"] = None
        frame = state["frame"]
        if frame is None or d not in state["window"]:
            return _one(d)
        return frame.filter(pl.col("ex_date") == d) if frame.height else pl.DataFrame()

    return fetch


def _xdxr_lanes(config: Config, chunks: int) -> int:
    """Parallel TDX connections for a corporate-action sweep.

    The configured TDX in-flight cap, which pacing and the cross-process
    lease ledger already enforce for every TDX request, and never more than
    ``_XDXR_MAX_LANES``: measured on a mainland link, one lane spends ~317ms
    per symbol and four reach ~110ms, the 100ms pacing floor, so a fifth would
    only queue behind the pacer.
    """
    cap = config.source_concurrency_for("tdx_protocol", config.tdx_daily_worker_count())
    return max(1, min(cap, _XDXR_MAX_LANES, chunks))


_XDXR_MAX_LANES = 4


@register_step("corporate_actions", group="core", depends_on=["instruments"])
def step_corporate_actions(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if getattr(config, "_corporate_actions_payment_repair", False):
        from cnequity.steps.payment_dates import repair_payment_dates

        return repair_payment_dates(config, trade_date, run_id, context)
    rl = config.tdx_rate_limit_spec()
    backfill = getattr(config, "_backfill", False)
    findings: list[dict] = []
    failed_symbols: list[str] = []
    heal_written = 0
    em_ok = False
    # The [start, through] window this run asked of the whole lake and got a
    # clean answer for; None when it did not (see storage/observation.py).
    observed: tuple[date | None, date] | None = None

    if backfill:
        symbols = list(
            context.get("_retry_symbols")
            or getattr(config, "_backfill_symbols", None)
            or _sweep_universe(config)
        )
        if getattr(config, "_corporate_actions_eastmoney_date_repair", None) and not getattr(
            config, "_backfill_symbols", None
        ):
            # The repair asks EastMoney for named ex-dates. Sweeping every
            # symbol through TDX on the way would cost hours and answer a
            # different question, so an unscoped date repair runs alone.
            logger.info(
                "corporate_actions: EastMoney date repair runs alone; "
                "pass --symbols to also sweep TDX"
            )
            symbols = []
        batch_id = context.get("_batch_id")
        manifest = Manifest(config.manifest_path) if batch_id else None

        if manifest is not None:
            manifest.set_batch_symbols(run_id, batch_id, symbols)

        completed_symbols: set[str] = set()
        if manifest is not None and context.get("_retry_symbols"):
            for row in manifest.get_batches_for_run(run_id):
                if row["task_id"] != _CORPORATE_ACTIONS_CHUNK_TASK or row["status"] != "success":
                    continue
                completed_symbols.update(json.loads(row["symbols_json"] or "[]"))
        remaining_symbols = [symbol for symbol in symbols if symbol not in completed_symbols]

        def on_progress(done: int, total: int) -> None:
            if manifest is not None and (done % 50 == 0 or done == total):
                try:
                    manifest.touch_batch_heartbeat(run_id, batch_id)
                except Exception as exc:  # noqa: BLE001 — heartbeat is auxiliary
                    logger.warning(
                        "corporate_actions: heartbeat update failed at %d/%d: %s",
                        done,
                        total,
                        exc,
                    )

        if config.failover_enabled and config.failover_backfill_snapshots:
            # Best-effort: this writes an EastMoney snapshot for cross-source
            # audit, not the canonical rows. It must never decide whether the
            # backfill runs — when EastMoney changed its filter grammar the
            # raise from here aborted the whole step before TDX, the actual
            # primary, was contacted at all.
            try:
                snapshot_corporate_actions_backup(
                    config, trade_date=trade_date, run_id=run_id, backfill=True
                )
            except Exception as exc:  # noqa: BLE001 — audit artifact, not the data
                logger.warning(
                    "corporate_actions: backup snapshot failed (%s: %s); "
                    "continuing with the canonical TDX fetch",
                    type(exc).__name__,
                    exc,
                )
        frames: list[pl.DataFrame] = []
        failed_symbols: list[str] = []
        batch_size = max(1, config.batch_size)
        # One reporter for the whole sweep, owned here because this is the only
        # scope that knows how many symbols the sweep has. Built per chunk
        # inside the adapter, it restarted at 1/100 every hundred names and its
        # ETA measured a window that kept resetting.
        sweep_report = sweep_progress(
            logger, "corporate_actions", len(remaining_symbols), unit="symbols"
        )
        chunks = [
            remaining_symbols[index : index + batch_size]
            for index in range(0, len(remaining_symbols), batch_size)
        ]
        backfill_start = getattr(config, "_backfill_start", None)
        backfill_end = getattr(config, "_backfill_end", None)
        chunk_window_start = backfill_start.isoformat() if backfill_start else None
        chunk_window_end = backfill_end.isoformat() if backfill_end else trade_date.isoformat()
        # How big the sweep was, recorded on the parent batch before the first
        # request. A chunk nobody reached leaves no receipt, so the ledger alone
        # could not tell an interrupted sweep from a short one: two successful
        # chunks read the same whether the plan had two or fifty-two. This is
        # deliberately telemetry rather than queued batch rows — a chunk is not
        # an independently retryable unit, and rows in the retry ledger would be
        # resolved back to this whole step and re-run once each.
        if manifest is not None and chunks:
            manifest.record_performance_metrics(
                run_id,
                "corporate_actions",
                {"chunks_planned": len(chunks), "chunk_symbols": len(remaining_symbols)},
            )
        lanes = _xdxr_lanes(config, len(chunks))
        stop = threading.Event()
        progress_lock = threading.Lock()
        swept = 0

        def tick() -> None:
            # Lanes finish symbols in any order; one shared count keeps the
            # progress line and the heartbeat monotonic across them.
            nonlocal swept
            with progress_lock:
                swept += 1
                done = swept
            sweep_report(done)
            on_progress(done, len(remaining_symbols))

        def fetch_chunk(chunk: list[str], chunk_scope: str) -> pl.DataFrame:
            return fetch_corporate_actions(
                trade_date,
                symbols=chunk,
                backfill=True,
                rate_limit=rl,
                allow_mock=config.tdx_allow_mock,
                primary_only=True,
                config=config,
                run_id=run_id,
                fail_loud=True,
                allow_empty=True,
                request_scope=chunk_scope,
                progress_report=lambda _index: tick(),
                should_stop=stop.is_set,
            )

        def record_chunk(chunk: list[str], chunk_batch_id: str, df_chunk: pl.DataFrame) -> None:
            if not df_chunk.is_empty():
                frames.append(df_chunk)
            if manifest is None:
                return
            # The fetch and staging happen before this success receipt. If
            # the process dies earlier, the parent batch remains retryable;
            # an unreceipted chunk is safely fetched again.
            chunk_scope = f"chunk:{chunk_batch_id}"
            staged_chunk = with_provenance(
                df_chunk,
                source=_CANONICAL_BACKFILL,
                data_version="v1",
            )
            write_fetched(
                config,
                run_id,
                "corporate_actions",
                staged_chunk,
                source=_CANONICAL_BACKFILL,
                batch_id=chunk_batch_id,
                raw_archive_evidence=(
                    verify_raw_archive(
                        config,
                        "corporate_actions",
                        run_id,
                        source=_CANONICAL_BACKFILL,
                        request_scope=chunk_scope,
                    )
                    if config.should_archive_raw("corporate_actions")
                    else None
                ),
            )
            manifest.start_batch(
                run_id,
                chunk_batch_id,
                task_id=_CORPORATE_ACTIONS_CHUNK_TASK,
                dataset="corporate_actions",
                symbols=chunk,
                window_start=chunk_window_start,
                window_end=chunk_window_end,
                blocks_compaction=False,
            )
            manifest.finish_batch(
                run_id,
                chunk_batch_id,
                "success",
                rows_read=df_chunk.height,
                rows_written=df_chunk.height,
            )

        def chunk_failed(chunk: list[str], chunk_batch_id: str, exc: Exception) -> None:
            failed_symbols.extend(chunk)
            logger.warning(
                "corporate_actions chunk %s failed for %d symbols: %s",
                chunk_batch_id,
                len(chunk),
                exc,
            )

        # Each lane owns one TDX connection and fetches whole chunks; every
        # staging write and ledger receipt stays on this thread, in the order
        # chunks finish. The shared pacing and in-flight cap still bound the
        # request rate, so lanes only fill the gaps one connection leaves
        # waiting on each reply (~0.33s per symbol on one lane, measured).
        if lanes > 1:
            logger.info("corporate_actions: %d chunk(s) over %d TDX lane(s)", len(chunks), lanes)
        pool = ThreadPoolExecutor(max_workers=lanes, thread_name_prefix="xdxr")
        try:
            pending = {}
            for chunk in chunks:
                chunk_batch_id = _corporate_actions_chunk_id(chunk)
                future = pool.submit(fetch_chunk, chunk, f"chunk:{chunk_batch_id}")
                pending[future] = (chunk, chunk_batch_id)
            for future in as_completed(pending):
                chunk, chunk_batch_id = pending[future]
                try:
                    df_chunk = future.result()
                except Exception as exc:  # noqa: BLE001 — preserve completed chunks for retry
                    chunk_failed(chunk, chunk_batch_id, exc)
                    continue
                record_chunk(chunk, chunk_batch_id, df_chunk)
        finally:
            # On Ctrl-C (or any escape) lanes stop at their next symbol rather
            # than finishing a 100-symbol chunk nobody will record.
            stop.set()
            pool.shutdown(wait=True, cancel_futures=True)

        if frames:
            df = pl.concat(frames, how="diagonal_relaxed")
        else:
            df = pl.DataFrame()
        # Stamp the canonical TDX rows before optionally appending Baostock.
        # ``with_provenance`` preserves an adapter-provided source column, so
        # the two repair sources remain auditable after concatenation.
        if not df.is_empty():
            df = with_provenance(df, source=_CANONICAL_BACKFILL, data_version="v1")

        if getattr(config, "_corporate_actions_baostock_repair", False):
            if not config.sources.get("baostock", False):
                raise RuntimeError(
                    "corporate_actions: the Baostock delisted-actions repair requires the baostock source"
                )
            repair_start = (
                getattr(config, "_backfill_start", None) or CORPORATE_ACTIONS_BACKFILL_START
            )
            repair_end = getattr(config, "_backfill_end", None) or trade_date
            repair_windows = _delisted_sh_sz_windows(
                config,
                symbols,
                repair_start,
                repair_end,
            )
            repair_symbols = sorted(repair_windows)
            repair_scope = f"repair:baostock:{repair_start.isoformat()}:{repair_end.isoformat()}"
            logger.info(
                "corporate_actions: Baostock repair scoped to %d delisted SH/SZ symbol(s)",
                len(repair_symbols),
            )
            if repair_symbols:
                repair_df, repair_failed = fetch_corporate_actions_baostock(
                    repair_symbols,
                    repair_start,
                    repair_end,
                    config=config,
                    run_id=run_id,
                    symbol_windows=repair_windows,
                    request_scope=repair_scope,
                )
            else:
                repair_df, repair_failed = pl.DataFrame(), []
            failed_symbols.extend(repair_failed)
            if not repair_df.is_empty():
                repair_df = with_provenance(repair_df, source="baostock", data_version="v1")
                if manifest is not None:
                    repair_batch_id = f"{batch_id or 'batch-0'}-baostock-repair"
                    write_fetched(
                        config,
                        run_id,
                        "corporate_actions",
                        repair_df,
                        source="baostock",
                        batch_id=repair_batch_id,
                        raw_archive_evidence=(
                            verify_raw_archive(
                                config,
                                "corporate_actions",
                                run_id,
                                source="baostock",
                                request_scope=repair_scope,
                            )
                            if config.should_archive_raw("corporate_actions")
                            else None
                        ),
                    )
                    manifest.start_batch(
                        run_id,
                        repair_batch_id,
                        task_id="corporate_actions_baostock_repair",
                        dataset="corporate_actions",
                        symbols=repair_symbols,
                        window_start=repair_start.isoformat(),
                        window_end=repair_end.isoformat(),
                        blocks_compaction=False,
                    )
                    manifest.finish_batch(
                        run_id,
                        repair_batch_id,
                        "success",
                        rows_read=repair_df.height,
                        rows_written=repair_df.height,
                    )
                df = (
                    repair_df
                    if df.is_empty()
                    else pl.concat([df, repair_df], how="diagonal_relaxed")
                )
        if getattr(config, "_corporate_actions_ths_repair", False):
            if not config.sources.get("ths_bonus", False):
                raise RuntimeError(
                    "corporate_actions: the THS delisted-actions repair requires the ths_bonus source"
                )
            repair_start = (
                getattr(config, "_backfill_start", None) or CORPORATE_ACTIONS_BACKFILL_START
            )
            repair_end = getattr(config, "_backfill_end", None) or trade_date
            repair_windows = _delisted_bj_windows(config, symbols, repair_start, repair_end)
            repair_symbols = sorted(repair_windows)
            repair_scope = f"repair:ths:{repair_start.isoformat()}:{repair_end.isoformat()}"
            logger.info(
                "corporate_actions: THS repair scoped to %d delisted BJ symbol(s)",
                len(repair_symbols),
            )
            if repair_symbols:
                repair_df, repair_failed = fetch_corporate_actions_ths(
                    repair_symbols,
                    repair_start,
                    repair_end,
                    config=config,
                    run_id=run_id,
                    symbol_windows=repair_windows,
                    request_scope=repair_scope,
                )
            else:
                repair_df, repair_failed = pl.DataFrame(), []
            failed_symbols.extend(repair_failed)
            if not repair_df.is_empty():
                repair_df = with_provenance(repair_df, source="ths", data_version="v1")
                if manifest is not None:
                    repair_batch_id = f"{batch_id or 'batch-0'}-ths-repair"
                    write_fetched(
                        config,
                        run_id,
                        "corporate_actions",
                        repair_df,
                        source="ths",
                        batch_id=repair_batch_id,
                        raw_archive_evidence=(
                            verify_raw_archive(
                                config,
                                "corporate_actions",
                                run_id,
                                source="ths",
                                request_scope=repair_scope,
                            )
                            if config.should_archive_raw("corporate_actions")
                            else None
                        ),
                    )
                    manifest.start_batch(
                        run_id,
                        repair_batch_id,
                        task_id="corporate_actions_ths_repair",
                        dataset="corporate_actions",
                        symbols=repair_symbols,
                        window_start=repair_start.isoformat(),
                        window_end=repair_end.isoformat(),
                        blocks_compaction=False,
                    )
                    manifest.finish_batch(
                        run_id,
                        repair_batch_id,
                        "success",
                        rows_read=repair_df.height,
                        rows_written=repair_df.height,
                    )
                df = (
                    repair_df
                    if df.is_empty()
                    else pl.concat([df, repair_df], how="diagonal_relaxed")
                )
        repair_dates = list(getattr(config, "_corporate_actions_eastmoney_date_repair", None) or [])
        if repair_dates:
            if not config.sources.get("eastmoney", False):
                raise RuntimeError(
                    "corporate_actions: the ex-dates repair requires the eastmoney source"
                )
            repair_start = (
                getattr(config, "_backfill_start", None) or CORPORATE_ACTIONS_BACKFILL_START
            )
            repair_end = getattr(config, "_backfill_end", None) or trade_date
            outside = [d for d in repair_dates if d < repair_start or d > repair_end]
            if outside:
                raise RuntimeError(
                    "corporate_actions: --ex-dates outside the backfill window "
                    f"{repair_start.isoformat()}..{repair_end.isoformat()}: "
                    + ", ".join(d.isoformat() for d in outside)
                )
            logger.info(
                "corporate_actions: EastMoney date repair over %d ex-date(s)",
                len(repair_dates),
            )
            for repair_date in repair_dates:
                # One capture scope per date: `begin_capture` resets the bucket
                # it is given, so a shared scope would leave the last date's
                # receipt standing for all of them.
                repair_scope = f"repair:eastmoney-date:{repair_date.isoformat()}"
                repair_df = fetch_corporate_actions_eastmoney(
                    repair_date,
                    backfill=False,
                    config=config,
                    run_id=run_id,
                    request_scope=repair_scope,
                )
                if repair_df.is_empty():
                    continue
                repair_df = with_provenance(repair_df, source="eastmoney", data_version="v1")
                if manifest is not None:
                    repair_batch_id = (
                        f"{batch_id or 'batch-0'}-eastmoney-date-"
                        f"{repair_date.isoformat().replace('-', '')}"
                    )
                    write_fetched(
                        config,
                        run_id,
                        "corporate_actions",
                        repair_df,
                        source="eastmoney",
                        batch_id=repair_batch_id,
                        raw_archive_evidence=(
                            verify_raw_archive(
                                config,
                                "corporate_actions",
                                run_id,
                                source="eastmoney",
                                request_scope=repair_scope,
                            )
                            if config.should_archive_raw("corporate_actions")
                            else None
                        ),
                    )
                    manifest.start_batch(
                        run_id,
                        repair_batch_id,
                        task_id="corporate_actions_eastmoney_date_repair",
                        dataset="corporate_actions",
                        symbols=sorted(repair_df.get_column("symbol").unique().to_list()),
                        window_start=repair_date.isoformat(),
                        window_end=repair_date.isoformat(),
                        blocks_compaction=False,
                    )
                    manifest.finish_batch(
                        run_id,
                        repair_batch_id,
                        "success",
                        rows_read=repair_df.height,
                        rows_written=repair_df.height,
                    )
                df = (
                    repair_df
                    if df.is_empty()
                    else pl.concat([df, repair_df], how="diagonal_relaxed")
                )
        if failed_symbols:
            failed_symbols = list(dict.fromkeys(failed_symbols))
        if failed_symbols:
            logger.warning(
                "corporate_actions backfill incomplete: %d/%d symbols failed; "
                "successful chunks are staged and will be skipped on retry",
                len(failed_symbols),
                len(symbols),
            )
        swept = set(symbols) | completed_symbols
        scoped = context.get("_retry_symbols") or getattr(config, "_backfill_symbols", None)
        # A `--symbols` sweep answers for those names only; it counts as a look
        # at the lake when, with this run's earlier chunks, it covered them all.
        if swept and not failed_symbols and (not scoped or swept >= set(_sweep_universe(config))):
            observed = (
                getattr(config, "_backfill_start", None),
                min(getattr(config, "_backfill_end", None) or trade_date, trade_date),
            )
        canonical_source = _CANONICAL_BACKFILL
    else:
        # TDX first: the exchange's adjusted previous close names the session's
        # ex-events (`detect_ex_symbols`), and xdxr answers for those symbols.
        # Quotes describe the current session only, so a catch-up run for an
        # earlier date goes to EastMoney alone.
        from cnequity.domain.market_time import shanghai_today

        tdx_rows = pl.DataFrame()
        tdx_scope = ""
        tdx_ok = False
        tdx_error: str | None = None
        if config.sources.get("tdx_protocol", True) and trade_date == shanghai_today():
            try:
                tdx_rows, tdx_scope = _tdx_primary_events(config, trade_date, run_id, rl)
                tdx_ok = True
            except Exception as exc:  # noqa: BLE001 — EastMoney still answers for the day
                tdx_error = f"{type(exc).__name__}: {exc}"[:200]
                logger.warning("corporate_actions: TDX primary unavailable (%s)", tdx_error)
        window: list[date] = []
        df, findings = pl.DataFrame(), []
        if config.sources.get("eastmoney", True):
            window = incremental_trade_dates(config, "corporate_actions", trade_date)
            try:
                df, findings = fetch_incremental_daily(
                    config,
                    "corporate_actions",
                    trade_date,
                    _window_fetcher(config, trade_date, run_id),
                    allow_empty=True,
                    date_col="ex_date",
                )
                em_ok = True
            except Exception as exc:
                if not tdx_ok:
                    raise
                logger.warning(
                    "corporate_actions: EastMoney window unavailable (%s); TDX answers alone", exc
                )
                findings.append(
                    {
                        "dataset": "corporate_actions",
                        "severity": "warning",
                        "check": "corporate_actions_vendor_unavailable",
                        "message": f"EastMoney window unavailable ({exc}); TDX events only",
                    }
                )
        elif not tdx_ok:
            raise SourceUnavailableError(
                "corporate_actions daily: eastmoney disabled and TDX unavailable"
            )
        if tdx_error is not None:
            findings.append(
                {
                    "dataset": "corporate_actions",
                    "severity": "warning",
                    "check": "corporate_actions_primary_unavailable",
                    "message": f"TDX primary unavailable ({tdx_error}); EastMoney used",
                }
            )
        if tdx_ok:
            peer = _peer_finding(tdx_rows, df, trade_date)
            if peer is not None:
                findings.append(peer)
            tdx_rows, _vendor_split = _split_with_vendor(tdx_rows, df)
            if not tdx_rows.is_empty():
                if not df.is_empty():
                    df = df.join(
                        tdx_rows.select("symbol", "ex_date", "action_type"),
                        on=["symbol", "ex_date", "action_type"],
                        how="anti",
                    )
                primary = with_provenance(tdx_rows, source=_CANONICAL_BACKFILL, data_version="v1")
                written = write_fetched(
                    config,
                    run_id,
                    "corporate_actions",
                    primary,
                    source=_CANONICAL_BACKFILL,
                    batch_id="batch-0-tdx-primary",
                    raw_archive_evidence=(
                        verify_raw_archive(
                            config,
                            "corporate_actions",
                            run_id,
                            source=_CANONICAL_BACKFILL,
                            request_scope=tdx_scope,
                        )
                        if config.should_archive_raw("corporate_actions")
                        else None
                    ),
                )
                heal_written += int(written.get("rows_written", primary.height))
        canonical_source = _CANONICAL_DAILY
        # The EastMoney ex-date query is market-wide, so a window with no
        # failed or source-gapped day is a clean look at the whole lake.
        if (
            em_ok
            and window
            and not any(f.get("check") == "fetch_failed_days" for f in findings)
            and not set(window) & source_gap_dates("corporate_actions")
        ):
            observed = (min(window), max(window))
        if config.failover_enabled and not tdx_ok:
            failover = failover_spec(config, "corporate_actions")
            if failover is not None and failover.snapshot_cadence == "daily":
                ex_today = (
                    df.filter(pl.col("ex_date") == trade_date) if df.height else pl.DataFrame()
                )
                # A clean primary day is still an observation that must be
                # checked by the independent TDX peer.  The caller may supply
                # a constrained universe (tests/repair runs); otherwise the
                # normal instrument universe is the only way to distinguish
                # "no actions" from an unqueried peer.
                backup_symbols = list(context.get("symbols") or [])
                if not backup_symbols:
                    backup_symbols = (
                        ex_today["symbol"].unique().to_list()
                        if ex_today.height
                        else load_symbols(config)
                    )
                try:
                    backup_captured = snapshot_corporate_actions_tdx_backup(
                        config,
                        trade_date=trade_date,
                        symbols=backup_symbols,
                        run_id=run_id,
                        rate_limit=rl,
                    )
                    if not backup_captured:
                        findings.append(
                            {
                                "dataset": "corporate_actions",
                                "severity": "warning",
                                "check": "backup_snapshot_unavailable",
                                "message": (
                                    "corporate_actions independent TDX backup snapshot returned "
                                    "no rows; primary data was not marked erroneous"
                                ),
                                "peer_unavailable": True,
                                "retryable": True,
                            }
                        )
                except Exception as exc:  # noqa: BLE001 — peer is best-effort
                    logger.warning(
                        "corporate_actions: independent TDX backup snapshot unavailable "
                        "(%s: %s); primary result remains valid and will be retried",
                        type(exc).__name__,
                        exc,
                    )
                    findings.append(
                        {
                            "dataset": "corporate_actions",
                            "severity": "warning",
                            "check": "backup_snapshot_unavailable",
                            "message": (
                                "corporate_actions independent backup snapshot was unavailable; "
                                "primary data was not marked erroneous"
                            ),
                            "peer_unavailable": True,
                            "retryable": True,
                        }
                    )
        heal_written += _heal_unrecorded_ex_events(config, trade_date, run_id, rl)
        fund_written, fund_finding = _sweep_unswept_fund_history(config, trade_date, run_id, rl)
        heal_written += fund_written
        if fund_finding:
            findings.append(fund_finding)

    if backfill and not df.is_empty():
        start = getattr(config, "_backfill_start", None) or CORPORATE_ACTIONS_BACKFILL_START
        end = getattr(config, "_backfill_end", None) or trade_date
        if "ex_date" not in df.columns:
            raise SourcePayloadError("corporate_actions: backfill response has no ex_date column")
        parsed_dates = df.get_column("ex_date").cast(pl.Date, strict=False)
        invalid = (
            parsed_dates.is_null()
            | (parsed_dates < start).fill_null(False)
            | (parsed_dates > end).fill_null(False)
        )
        if int(invalid.sum()):
            raise SourcePayloadError(
                "corporate_actions: backfill response returned row(s) outside "
                f"requested window {start.isoformat()}..{end.isoformat()}"
            )
        df = df.with_columns(parsed_dates.alias("ex_date"))

    if observed is not None:
        # Fetched rows count once compaction publishes them (finalize commits
        # the claim); an empty answer has nothing to publish and counts now.
        note_observation(
            config,
            "corporate_actions",
            run_id,
            start=observed[0],
            through=observed[1],
            pending_publish=not df.is_empty(),
        )

    context_updates: dict = {"symbols_to_rebackfill": []}
    if findings:
        context_updates["audit_findings"] = findings
    if df.is_empty():
        # Even an empty critical observation must have a source receipt.  It
        # is not staged, but rejecting a captureless adapter here keeps an
        # enabled archive policy from silently treating "no rows" as proof of
        # a complete source response.
        if not backfill and em_ok and config.should_archive_raw("corporate_actions"):
            verify_raw_archive(
                config,
                "corporate_actions",
                run_id,
                source=_CANONICAL_DAILY,
                request_scope=f"daily:{trade_date.isoformat()}",
            )
        result = {
            "rows_read": heal_written,
            "rows_written": heal_written,
            "context_updates": context_updates,
        }
        if backfill and failed_symbols:
            result["failed_symbols"] = failed_symbols
            result["status"] = "failed"
        return result

    df = with_provenance(df, source=canonical_source, data_version="v1")

    rebackfill: list[str] = []
    if df.height and "symbol" in df.columns and "ex_date" in df.columns:
        today = df.filter(pl.col("ex_date") == trade_date)
        if today.height:
            # Scoped here rather than at the consumers, because `daily_bars`
            # merges this list into its fetch scope *after* narrowing to the
            # ingest universe — so an out-of-scope code arriving this way is
            # fetched anyway. This dataset carries 348 ETF/LOF codes today, and
            # a fund's bars are a NAV series with zero volume on every session:
            # exactly the rows the old NAV-series cleanup had to delete.
            # An `all_instruments` lake drops nothing here.
            rebackfill = filter_ingest_universe(
                today["symbol"].unique().to_list(), config.ingest_universe
            )

    context_updates["symbols_to_rebackfill"] = rebackfill
    if backfill and manifest is not None:
        result = {
            "rows_read": df.height,
            "rows_written": df.height,
        }
    else:
        result = write_fetched(
            config,
            run_id,
            "corporate_actions",
            df,
            source=canonical_source,
            raw_archive_evidence=(
                verify_raw_archive(
                    config,
                    "corporate_actions",
                    run_id,
                    source=canonical_source,
                    request_scope=f"daily:{trade_date.isoformat()}",
                )
                if config.should_archive_raw("corporate_actions")
                else None
            ),
        )
        if heal_written:
            # Staged under their own batch, but still rows this step wrote.
            result["rows_read"] = int(result.get("rows_read", 0)) + heal_written
            result["rows_written"] = int(result.get("rows_written", 0)) + heal_written
    if backfill and failed_symbols:
        result["failed_symbols"] = failed_symbols
        result["status"] = "failed"
    result["context_updates"] = context_updates
    return result


#: Ex-dates the weekly sweep re-reads: enough to cover a missed week and its edges.
_SWEEP_WINDOW_DAYS = 30
_SWEEP_CHUNK = 200


@register_step(
    "corporate_actions_sweep",
    group="actions_sweep",
    depends_on=["instruments"],
    description="Re-read every symbol's TDX ex-rights records for the last month",
)
def step_corporate_actions_sweep(
    config: Config, trade_date: date, run_id: str, context: dict
) -> dict:
    """Catch the ex-events the daily detection cannot see.

    The daily path finds events from the exchange's adjusted previous close,
    which misses a cash dividend under half a tick and any day the run did
    not reach. This asks TDX about every symbol (about 24 minutes on one
    connection for 5,600 stocks, measured 2026-10-27) and keeps only events
    dated in the last month; scheduled weekly in its own group. Rows already
    in the lake are rewritten unchanged.
    """
    if not config.sources.get("tdx_protocol", True):
        return {"rows_written": 0, "status": "skipped", "reason": "tdx_protocol disabled"}
    rl = config.tdx_rate_limit_spec()
    since = trade_date - timedelta(days=_SWEEP_WINDOW_DAYS)
    symbols = _sweep_universe(config)
    # TDX files 送 and 转 as one bonus ratio; where the lake already holds the
    # split from another source, writing TDX's row would count the shares twice.
    stored_shares = _stored_vendor_shares(config, since, trade_date)
    written = 0
    failed: list[str] = []
    for start in range(0, len(symbols), _SWEEP_CHUNK):
        chunk = symbols[start : start + _SWEEP_CHUNK]
        scope = f"sweep:{trade_date.isoformat()}:{start:05d}"
        try:
            frame = fetch_corporate_actions(
                trade_date,
                symbols=chunk,
                backfill=True,
                rate_limit=rl,
                allow_mock=config.tdx_allow_mock,
                primary_only=True,
                config=config,
                run_id=run_id,
                fail_loud=True,
                allow_empty=True,
                request_scope=scope,
            )
        except Exception as exc:  # noqa: BLE001 — the next chunk is independent
            logger.warning("corporate_actions sweep: chunk %d failed: %s", start, exc)
            failed.extend(chunk)
            continue
        if frame.is_empty():
            continue
        recent = frame.filter((pl.col("ex_date") >= since) & (pl.col("ex_date") <= trade_date))
        recent, _stored = _split_with_vendor(recent, stored_shares)
        if recent.is_empty():
            continue
        result = write_fetched(
            config,
            run_id,
            "corporate_actions",
            with_provenance(recent, source=_CANONICAL_BACKFILL, data_version="v1"),
            source=_CANONICAL_BACKFILL,
            batch_id=f"sweep-{start:05d}",
            raw_archive_evidence=(
                verify_raw_archive(
                    config,
                    "corporate_actions",
                    run_id,
                    source=_CANONICAL_BACKFILL,
                    request_scope=scope,
                )
                if config.should_archive_raw("corporate_actions")
                else None
            ),
        )
        written += int(result.get("rows_written", recent.height))
    out: dict = {"rows_read": written, "rows_written": written, "symbols": len(symbols)}
    if failed:
        out["status"] = "degraded"
        out["failed_symbols"] = len(failed)
    return out


@register_step("earnings_disclosure_schedule", group="fundamentals", depends_on=["instruments"])
def step_earnings_disclosure_schedule(
    config: Config, trade_date: date, run_id: str, context: dict
) -> dict:
    if not config.sources.get("eastmoney", True):
        raise SourceUnavailableError(
            "earnings_disclosure_schedule: eastmoney source disabled in config"
        )
    # Period-keyed like financial_statement_items (watermark=False): daily runs
    # refresh the open disclosure windows; backfill walks every period 2016+.
    backfill = getattr(config, "_backfill", False)
    df = _validate_earnings_schedule_snapshot(
        fetch_earnings_disclosure_schedule(trade_date, backfill=backfill, config=config)
    )
    missing_periods: set[str] = set()
    if backfill:
        expected = {
            f"{period[:4]}Q{(int(period[5:7]) - 1) // 3 + 1}"
            for period in _backfill_report_dates(
                trade_date,
                start=getattr(config, "_backfill_start", None),
                end=getattr(config, "_backfill_end", None),
            )
        }
        observed = (
            set(df.get_column("report_period").drop_nulls().to_list())
            if not df.is_empty() and "report_period" in df.columns
            else set()
        )
        missing_periods = expected - observed
    if backfill and missing_periods:
        result: dict
        if df.is_empty():
            result = {"rows_read": 0, "rows_written": 0}
        else:
            result = write_fetched(
                config, run_id, "earnings_disclosure_schedule", df, source="eastmoney"
            )
        result["status"] = "warning"
        result["missing_periods"] = len(missing_periods)
        result["context_updates"] = {
            "audit_findings": [
                {
                    "dataset": "earnings_disclosure_schedule",
                    "severity": "warning",
                    "check": "backfill_missing_report_periods",
                    "message": (
                        f"earnings disclosure schedule missing {len(missing_periods)} "
                        f"requested report period(s): {', '.join(sorted(missing_periods)[:8])}"
                    ),
                    "missing_periods": sorted(missing_periods),
                }
            ]
        }
        return result
    if df.is_empty():
        return {"rows_read": 0, "rows_written": 0}
    return write_fetched(config, run_id, "earnings_disclosure_schedule", df, source="eastmoney")


@register_step("announcement_index", group="capital", depends_on=["instruments"])
def step_announcement_index(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if not config.sources.get("cninfo", True):
        raise SourceUnavailableError("announcement_index: cninfo source disabled in config")
    if getattr(config, "_backfill", False):
        return _cninfo_range_backfill(
            config,
            trade_date,
            run_id,
            "announcement_index",
            fetch_announcement_index_range,
            date_col="announce_date",
            floor=date(2010, 1, 1),
            batch_id=context.get("_batch_id"),
        )
    metrics: dict = {"run_id": run_id}
    result = run_incremental_fetched(
        config,
        trade_date,
        run_id,
        "announcement_index",
        lambda d: _fetch_cninfo_single(
            fetch_announcement_index,
            d,
            config,
            metrics,
            dataset="announcement_index",
        ),
        source="cninfo",
        date_col="announce_date",
        raw_archive_evidence_factory=lambda: verify_raw_archive(
            config,
            "announcement_index",
            run_id,
            source="cninfo",
            request_scope=f"range:announcement:{trade_date.isoformat()}:{trade_date.isoformat()}",
        ),
    )
    if len(metrics) > 1:
        _record_cninfo_metrics(
            config,
            run_id,
            "announcement_index",
            metrics,
            batch_id=context.get("_batch_id"),
        )
    result["metrics"] = metrics
    return result
