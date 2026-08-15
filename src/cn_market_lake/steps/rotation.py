"""L7 rotation steps: hot rank, sector bars/flows, market news headlines."""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from datetime import date, timedelta

from cn_market_lake.adapters.eastmoney.rotation import (
    fetch_hot_rank,
    fetch_news_headlines,
    fetch_sector_fund_flow,
)
from cn_market_lake.config import Config
from cn_market_lake.orchestrator.registry import register_step
from cn_market_lake.steps.http_common import run_incremental_fetched, write_fetched

# Board kline history depth for `cml backfill sector_bars` (~1y+ slack).
_SECTOR_BARS_BACKFILL_DAYS = 400
_SECTOR_BARS_BACKFILL_STATE = "sector_bars_backfill"
_SECTOR_BARS_FAILURE_THRESHOLD = 0.5


def _run_rotation_step(
    config: Config,
    trade_date: date,
    run_id: str,
    dataset: str,
    fetch_fn,
    *,
    allow_empty: bool = True,
) -> dict:
    if not config.sources.get("eastmoney", True):
        raise RuntimeError(f"{dataset}: eastmoney source disabled in config")
    return run_incremental_fetched(
        config,
        trade_date,
        run_id,
        dataset,
        fetch_fn,
        source="eastmoney",
        allow_empty=allow_empty,
    )


@register_step("hot_rank", group="research", depends_on=["instruments"])
def step_hot_rank(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    return _run_rotation_step(config, trade_date, run_id, "hot_rank", fetch_hot_rank)


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
    if not config.sources.get("ths", True):
        raise RuntimeError("sector_bars: ths source disabled in config")
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
        os.replace(tmp, path)
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

    from cn_market_lake.adapters.ths.boards import sweep_board_bars

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

    _, failed, succeeded = sweep_board_bars(
        start,
        end,
        config=config,
        skip_sectors=skip_sectors,
        on_batch=on_batch,
    )
    attempted = len(succeeded) + len(failed)
    if attempted == 0:
        return {"rows_read": 0, "rows_written": 0, "note": "all boards already swept"}

    result: dict = dict(written)
    if failed:
        result["failed_sectors"] = len(failed)
        result["attempted_sectors"] = attempted
        result.setdefault("context_updates", {})["audit_findings"] = [
            {
                "dataset": "sector_bars",
                "severity": "warning",
                "code": "sector_bars_sweep_incomplete",
                "message": (
                    f"{len(failed)}/{attempted} board(s) failed the 同花顺 sweep; "
                    "re-run `cml backfill sector_bars --retry-failed` to resume."
                ),
            }
        ]
        if len(failed) / attempted > _SECTOR_BARS_FAILURE_THRESHOLD:
            result["status"] = "warning"
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
    return _run_rotation_step(
        config, trade_date, run_id, "sector_fund_flow", fetch_sector_fund_flow
    )


@register_step("news_headlines", group="research")
def step_news_headlines(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    return _run_rotation_step(
        config, trade_date, run_id, "news_headlines", fetch_news_headlines, allow_empty=True
    )
