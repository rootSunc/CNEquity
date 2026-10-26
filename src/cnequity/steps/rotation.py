"""L7 rotation steps: hot rank, sector bars/flows, market news headlines."""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from datetime import date, timedelta

from cnequity.adapters.eastmoney.rotation import (
    fetch_hot_rank,
    fetch_sector_fund_flow,
)
from cnequity.config import Config
from cnequity.orchestrator.outcomes import SourcePayloadError, SourceUnavailableError
from cnequity.orchestrator.registry import register_step
from cnequity.orchestrator.source_gaps import record_source_gap
from cnequity.steps.http_common import (
    call_with_run_id,
    run_incremental_fetched,
    verify_raw_archive,
    write_fetched,
)
from cnequity.storage.atomic import replace_with_retry

# Board kline history depth for `cne backfill sector_bars` (~1y+ slack).
_SECTOR_BARS_BACKFILL_DAYS = 400
_SECTOR_BARS_BACKFILL_STATE = "sector_bars_backfill"
_SECTOR_BARS_FAILURE_THRESHOLD = 0.5
_SECTOR_FLOW_BOARD_TYPES = frozenset({"concept", "industry"})
_MIN_SECTOR_FLOW_ROWS_BY_TYPE = {"concept": 100, "industry": 50}


def _validate_sector_fund_flow_snapshot(df):
    """Reject a non-empty flow snapshot that is missing a board slice."""
    if df.is_empty():
        return df
    required = {"sector_code", "board_type", "trade_date"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise SourcePayloadError(
            "sector_fund_flow: response is missing required column(s): " + ", ".join(missing)
        )
    observed = set(df.get_column("board_type").drop_nulls().to_list())
    missing_types = sorted(_SECTOR_FLOW_BOARD_TYPES - observed)
    unique = df.unique(subset=["sector_code", "board_type", "trade_date"])
    counts = unique.group_by("board_type").len().rename({"len": "_sector_count"})
    thin_types = {
        row["board_type"]: row["_sector_count"]
        for row in counts.iter_rows(named=True)
        if row["board_type"] in _MIN_SECTOR_FLOW_ROWS_BY_TYPE
        and row["_sector_count"] < _MIN_SECTOR_FLOW_ROWS_BY_TYPE[row["board_type"]]
    }
    if missing_types or thin_types:
        details: list[str] = []
        if missing_types:
            details.append(f"missing board type(s): {', '.join(missing_types)}")
        details.extend(
            f"{board_type}={count} (minimum {_MIN_SECTOR_FLOW_ROWS_BY_TYPE[board_type]})"
            for board_type, count in sorted(thin_types.items())
        )
        record_source_gap(
            "sector_fund_flow",
            "sector_fund_flow: incomplete daily snapshot; " + "; ".join(details),
            frame=df,
        )
    return df


def _run_rotation_step(
    config: Config,
    trade_date: date,
    run_id: str,
    dataset: str,
    fetch_fn,
    *,
    allow_empty: bool = False,
    date_col: str | None = None,
) -> dict:
    if not config.sources.get("eastmoney", True):
        raise SourceUnavailableError(f"{dataset}: eastmoney source disabled in config")

    def _bound(d: date):
        return call_with_run_id(
            fetch_fn,
            d,
            pipeline_config=config,
            dataset=dataset,
            run_id=run_id,
            config=config,
        )

    return run_incremental_fetched(
        config,
        trade_date,
        run_id,
        dataset,
        _bound,
        source="eastmoney",
        allow_empty=allow_empty,
        date_col=date_col,
        raw_archive_evidence_factory=lambda: verify_raw_archive(
            config,
            dataset,
            run_id,
            source="eastmoney",
            request_scope=f"snapshot:{trade_date.isoformat()}",
        ),
    )


@register_step("hot_rank", group="research", depends_on=["instruments"])
def step_hot_rank(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    def _fetch(d: date, *, config: Config, run_id: str):
        return call_with_run_id(
            fetch_hot_rank,
            d,
            pipeline_config=config,
            dataset="hot_rank",
            run_id=run_id,
            config=config,
            require_top_n=True,
        )

    # The vendor endpoint is a live snapshot and can legally return its last
    # published session when the requested session has not been refreshed yet.
    # Validate the response date before staging it; otherwise a stale payload
    # is recorded as a successful update for the requested day.
    return _run_rotation_step(
        config,
        trade_date,
        run_id,
        "hot_rank",
        _fetch,
        date_col="trade_date",
    )


# Daily still walks a window rather than a single day: `last.js` carries ~140
# sessions for the same one request, so covering missed runs is free.
_SECTOR_BARS_DAILY_WINDOW_DAYS = 30


@register_step("sector_bars", group="research", depends_on=["instruments"])
def step_sector_bars(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """Board bars from 同花顺 — one source for history *and* daily.

    Both paths deliberately use the same source. Mixing them is what broke the
    previous lake: TDX-based history with EastMoney-based daily rows under one
    ``sector_code`` put two different index bases in one series, so the splice
    date showed a fake +79% median jump across 439 boards.
    """
    # 同花顺's rolling ``last`` endpoint includes the current intraday board
    # value. Keep sector daily bars under the same final-session contract as
    # stock and index bars, otherwise a pre-close run publishes partial OHLC.
    from cnequity.steps.bars import _reject_unfinished_daily_bar_window

    endpoint = getattr(config, "_backfill_end", None) or trade_date
    _reject_unfinished_daily_bar_window(config, endpoint)
    if not config.sources.get("ths", True):
        raise SourceUnavailableError("sector_bars: ths source disabled in config")
    if getattr(config, "_backfill", False):
        return _backfill_sector_bars(config, trade_date, run_id)
    return _sweep_sector_bars(
        config,
        run_id,
        start=trade_date - timedelta(days=_SECTOR_BARS_DAILY_WINDOW_DAYS),
        end=trade_date,
        skip_sectors=set(),
        mark_completed=False,
    )


def _sector_bars_backfill_state_path(config: Config):
    return config.meta_root / "state" / f"{_SECTOR_BARS_BACKFILL_STATE}.json"


def _read_sector_bars_checkpoint(config: Config) -> dict:
    path = _sector_bars_backfill_state_path(config)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        # A truncated checkpoint must cost a re-sweep, never a silent skip.
        return {}


def _sector_bars_completed(config: Config, window: tuple[date, date] | None = None) -> set[str]:
    """Boards already swept, but only those whose sweep covers ``window``.

    The checkpoint records *which* dates a board was fetched over. Without that,
    a narrow gap-fill (``--start/--end`` over 4 days) would mark all ~991 boards
    done and make the next full 400-day backfill a no-op that reports success.
    A checkpoint therefore licenses skipping only when its window contains the
    one being asked for now.
    """
    data = _read_sector_bars_checkpoint(config)
    completed = set(data.get("completed", []))
    if window is None or not completed:
        return completed
    stored = data.get("window") or {}
    try:
        covers = (
            date.fromisoformat(stored["start"]) <= window[0]
            and date.fromisoformat(stored["end"]) >= window[1]
        )
    except (KeyError, TypeError, ValueError):
        # Pre-window checkpoints carry no provenance — re-sweep rather than
        # trust them to cover a window they never recorded.
        return set()
    return completed if covers else set()


def clear_sector_bars_backfill_state(config: Config) -> None:
    path = _sector_bars_backfill_state_path(config)
    if path.exists():
        path.unlink()
    # Drop hybrid-era split checkpoints so a fresh EM sweep starts clean.
    for track in ("tdx", "em"):
        hybrid = config.meta_root / "state" / f"sector_bars_backfill_{track}.json"
        if hybrid.exists():
            hybrid.unlink()


def _mark_sector_bars_completed(
    config: Config, sector_codes: list[str], window: tuple[date, date] | None = None
) -> None:
    path = _sector_bars_backfill_state_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Boards carried over only if the stored sweep covers this one; a different
    # window starts its own tally rather than inheriting unrelated coverage.
    completed = sorted(_sector_bars_completed(config, window) | set(sector_codes))
    payload: dict = {"completed": completed}
    if window is not None:
        payload["window"] = {"start": window[0].isoformat(), "end": window[1].isoformat()}
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.stem}-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        replace_with_retry(tmp, path)
    except Exception:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def _sweep_sector_bars(
    config: Config,
    run_id: str,
    *,
    start: date,
    end: date,
    skip_sectors: set[str],
    mark_completed: bool,
) -> dict:
    """Fetch every board over ``[start, end]``, writing and checkpointing per batch.

    Shared by the daily and backfill paths so both land identical rows from the
    same source. Only the backfill checkpoints — the daily window is short enough
    to simply redo, and a checkpoint would make it skip boards tomorrow.
    """
    import polars as pl

    from cnequity.adapters.ths.boards import sweep_board_bars

    window = (start, end)
    written = {"rows_read": 0, "rows_written": 0}
    batches = 0

    def on_batch(rows: list[dict], sector_codes: list[str]) -> None:
        nonlocal batches
        if rows:
            out = write_fetched(
                config,
                run_id,
                "sector_bars",
                pl.DataFrame(rows),
                source="ths",
                batch_id=f"batch-{batches}",
            )
            written["rows_read"] += out["rows_read"]
            written["rows_written"] += out["rows_written"]
            batches += 1
        # Checkpoint only after the rows are durable — the reverse order would
        # let a crash mark boards done whose bars were never written.
        if mark_completed:
            _mark_sector_bars_completed(config, sector_codes, window)

    _, failed, succeeded, retired = sweep_board_bars(
        start,
        end,
        config=config,
        skip_sectors=skip_sectors,
        on_batch=on_batch,
    )
    attempted = len(succeeded) + len(failed)
    if attempted == 0 and not retired:
        return {"rows_read": 0, "rows_written": 0, "note": "all boards already swept"}

    result: dict = dict(written)
    findings: list[dict] = []
    if retired:
        result["retired_sectors"] = len(retired)
        findings.append(
            {
                "dataset": "sector_bars",
                "severity": "info",
                "check": "sector_bars_board_retired",
                "message": (
                    f"同花顺 lists {len(retired)} board(s) with no bars at all "
                    f"({', '.join(sorted(retired))}); bars already in the lake are kept."
                ),
            }
        )
    if failed:
        result["failed_sectors"] = len(failed)
        result["attempted_sectors"] = attempted
        findings.append(
            {
                "dataset": "sector_bars",
                "severity": "warning",
                "check": "sector_bars_sweep_incomplete",
                "message": (
                    f"{len(failed)}/{attempted} board(s) failed the 同花顺 sweep; "
                    "re-run `cne backfill sector_bars` to resume."
                ),
            }
        )
        if len(failed) / attempted > _SECTOR_BARS_FAILURE_THRESHOLD:
            result["status"] = "warning"
    if findings:
        result.setdefault("context_updates", {})["audit_findings"] = findings
    return result


def _backfill_sector_bars(config: Config, trade_date: date, run_id: str) -> dict:
    """Historical board bars from 同花顺. Partial sweeps surface as an audit finding."""
    if getattr(config, "_sector_bars_force", False):
        clear_sector_bars_backfill_state(config)

    # `--start/--end` narrow the sweep so filling a small hole does not re-fetch
    # the whole window across every board. Default stays the full backfill depth.
    end = getattr(config, "_backfill_end", None) or trade_date
    start = getattr(config, "_backfill_start", None) or (
        end - timedelta(days=_SECTOR_BARS_BACKFILL_DAYS)
    )
    return _sweep_sector_bars(
        config,
        run_id,
        start=start,
        end=end,
        skip_sectors=_sector_bars_completed(config, (start, end)),
        mark_completed=True,
    )


@register_step("sector_fund_flow", group="research", depends_on=["instruments"])
def step_sector_fund_flow(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    try:
        return _step_sector_fund_flow(config, trade_date, run_id)
    except Exception:
        # See step_fund_flow: 同花顺's board money flow, in its own table.
        from cnequity.steps.ths_fallback import stage_ths_fallback

        stage_ths_fallback(config, trade_date, run_id, "sector_fund_flow_ths")
        raise


def _step_sector_fund_flow(config: Config, trade_date: date, run_id: str) -> dict:
    return _run_rotation_step(
        config,
        trade_date,
        run_id,
        "sector_fund_flow",
        lambda d, *, config, run_id: _validate_sector_fund_flow_snapshot(
            call_with_run_id(
                fetch_sector_fund_flow,
                d,
                pipeline_config=config,
                dataset="sector_fund_flow",
                run_id=run_id,
                config=config,
            )
        ),
    )


@register_step("news_headlines", group="research")
def step_news_headlines(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    # One incremental fetch shared with flash_news_wire (steps/news_feed.py).
    from cnequity.steps.news_feed import stage_news

    return stage_news(config, trade_date, run_id, "news_headlines")


def resource_sector_bars_ths_official(
    config: Config,
    run_id: str,
    *,
    start: date | None = None,
    end: date | None = None,
    workers: int = 4,
    dry_run: bool = True,
) -> dict:
    """Move ``sector_bars`` from the scraper to the licensed endpoint.

    ``sector_bars`` is the lake's only dataset with no second source of any
    kind: 303,559 rows, all of them from ``ths``, an unauthenticated scrape of
    10jqka's public pages that ``sources/SOURCES.yml`` records as an
    unregistered client. There is nothing to arbitrate it against and nothing to
    fall back to.

    Unusually for a re-source, there is no accuracy question to settle first.
    Measured 2026-09-12 over five boards and 2,547 comparable sessions, close,
    volume and turnover all agreed within 10bps with a **median difference of
    zero** — the same numbers through a licensed channel. The peer also carried
    698 rows the lake did not.

    Still switching rather than routing (see docs/zh/reference/cli.md for explicit source repair),
    so ``dry_run`` defaults to true. The floor at 2022-01-04 leaves 37,304 rows
    (12.3%) on the scraper; those years hold a handful of boards, 2 in 2018 and
    39 in 2019, against 432 today.
    """
    import polars as pl

    from cnequity.adapters.ths_official import SOURCE as THS_SOURCE
    from cnequity.adapters.ths_official import client_from_config
    from cnequity.adapters.ths_official.sectors import (
        HISTORY_FLOOR,
        fetch_sector_bars,
        fetch_sector_catalog,
    )
    from cnequity.domain.market_time import shanghai_today
    from cnequity.query.canonical import dedupe_lazy_by_primary_key
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
    from cnequity.steps.http_common import write_fetched

    client = client_from_config(config)
    if client is None:
        return {"status": "skipped", "reason": "no api key", "rows_written": 0}
    if not dry_run and not getattr(config, "ths_official_backfill_enabled", False):
        client.close()
        return {"status": "skipped", "reason": "backfill disabled", "rows_written": 0}

    window_start = max(start or HISTORY_FLOOR, HISTORY_FLOOR)
    window_end = end or shanghai_today()
    try:
        catalog = fetch_sector_catalog(client)
        if catalog.is_empty():
            return {"status": "warning", "reason": "empty catalogue", "rows_written": 0}
        # Only boards the lake already tracks. The upstream lists 710 and the
        # lake carries 432; adopting the rest would widen the dataset on the
        # quiet, which is a separate decision from re-sourcing what it has.
        root = config.curated_root / "sector_bars"
        if dataset_has_parquet(root):
            known = (
                dedupe_lazy_by_primary_key(
                    scan_parquet_root(root, partition_col="trade_date"), "sector_bars"
                )
                .select("sector_code")
                .unique()
                .collect()
                .get_column("sector_code")
                .to_list()
            )
            catalog = catalog.filter(pl.col("sector_code").is_in(known))
        frame, counters = fetch_sector_bars(
            catalog, window_start, window_end, client=client, workers=workers
        )
    finally:
        client.close()

    result = {
        "boards": catalog.height,
        "rows_fetched": frame.height,
        "window": f"{window_start}..{window_end}",
        **counters,
    }
    if frame.is_empty():
        return {**result, "status": "warning", "reason": "peer returned nothing", "rows_written": 0}
    if dry_run:
        return {**result, "status": "dry_run", "rows_written": 0}

    written = write_fetched(config, run_id, "sector_bars", frame, source=THS_SOURCE)
    return {
        **result,
        "status": "applied",
        "rows_written": written.get("rows_written", frame.height),
    }
