"""同花顺 sector and industry index bars, through the licensed endpoint.

``sector_bars`` is 303,559 rows and every one of them comes from ``ths`` — the
unauthenticated scrape of 10jqka's public pages. It is the lake's only dataset
with no other source at all, not even a backup.

The licensed path serves the same boards under the same numbers with a ``.TI``
suffix, so 881101 and 881101.TI are the same industry index. It reaches back to
about 2022-01-04, which sounds thin against a dataset starting 2018-12-04 until
you look at where the rows are: the early years carry a handful of boards (2 in
2018, 39 in 2019) and 266,255 rows — 87.7% — fall on 2022 or later.

**A start date before the floor empties the whole response.** Measured
2026-09-12 against 881101.TI: 2022-01-01 to 2026-09-04 returns 1,133 bars, while
2018-12-04 to 2021-12-31 returns zero despite being a narrower window, and so
does every earlier span. There is no error and no code — an out-of-range request
is indistinguishable from a board that never traded, which is why the floor is a
constant here rather than something a caller is left to discover.
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING

import polars as pl

from cnequity.progress import sweep_progress

if TYPE_CHECKING:
    from cnequity.adapters.ths_official.client import ThsOfficialClient

logger = logging.getLogger(__name__)

__all__ = ["BOARD_TAGS", "HISTORY_FLOOR", "fetch_sector_bars", "fetch_sector_catalog"]

CST = timezone(timedelta(hours=8))
SOURCE = "ths_official"

# Measured: a request starting earlier comes back empty rather than clipped.
HISTORY_FLOOR = date(2022, 1, 4)
# Wider than this and the response empties too; 1,707 days was verified to work.
MAX_WINDOW_DAYS = 365 * 4

# Upstream catalogue tag -> the lake's `board_type`. `region` and `tszs` are
# real catalogues upstream but the lake carries no such boards, so they are left
# out rather than invented into a vocabulary nothing reads.
BOARD_TAGS: dict[str, str] = {
    "industry": "industry",
    "cn_concept": "concept",
}

_OUTPUT_SCHEMA = {
    "sector_code": pl.Utf8,
    "sector_name": pl.Utf8,
    "board_type": pl.Utf8,
    "trade_date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
    "amount": pl.Float64,
    "change_pct": pl.Float64,
}


def _ms(value: date) -> int:
    return int(datetime(value.year, value.month, value.day, tzinfo=CST).timestamp() * 1000)


def fetch_sector_catalog(client: ThsOfficialClient) -> pl.DataFrame:
    """Every board the upstream lists, keyed the way the lake keys them.

    The ``.TI`` suffix is dropped so the result joins straight onto
    ``sector_bars.sector_code``.
    """
    rows: list[dict] = []
    for tag, board_type in BOARD_TAGS.items():
        data = client.get("/api/a-share-index/catalog/ths-index-list", tag=tag)
        for item in (data or {}).get("item") or []:
            thscode = str(item.get("thscode") or "")
            if not thscode:
                continue
            rows.append(
                {
                    "thscode": thscode,
                    "sector_code": thscode.split(".")[0],
                    "sector_name": item.get("name"),
                    "board_type": board_type,
                }
            )
    if not rows:
        return pl.DataFrame(
            schema={
                "thscode": pl.Utf8,
                "sector_code": pl.Utf8,
                "sector_name": pl.Utf8,
                "board_type": pl.Utf8,
            }
        )
    return pl.DataFrame(rows).unique(subset=["sector_code"], keep="first")


def fetch_sector_bars(
    catalog: pl.DataFrame,
    start: date,
    end: date,
    *,
    client: ThsOfficialClient,
    workers: int = 1,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Daily bars for the boards in *catalog*.

    ``start`` is raised to :data:`HISTORY_FLOOR` when it falls below it. Sending
    the earlier date would return an empty list for every board and read as a
    catalogue of boards that never traded.
    """
    if start < HISTORY_FLOOR:
        logger.info(
            "ths_official sectors: raising start from %s to the service floor %s",
            start,
            HISTORY_FLOOR,
        )
        start = HISTORY_FLOOR
    if start > end:
        return pl.DataFrame(schema=_OUTPUT_SCHEMA), {"requests": 0, "empty": 0, "failed": 0}

    windows: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        stop = min(cursor + timedelta(days=MAX_WINDOW_DAYS), end)
        windows.append((cursor, stop))
        cursor = stop + timedelta(days=1)

    rows: list[dict] = []
    counters = {"requests": 0, "empty": 0, "failed": 0, "boards": 0}
    lock = threading.Lock()
    boards = catalog.to_dicts()
    # 432 boards × the windows each one needs is minutes of requests with
    # nothing on screen: measured at 99 log lines in the first 4.5 minutes of
    # `resource-sectors`, every one of them lock contention and not one of them
    # progress. Counted over boards finished, not rows, because a board that
    # returns nothing is still progress through the sweep.
    report = sweep_progress(
        logger, "ths_official sector bars", len(boards), every=25, unit="boards"
    )
    done = 0

    def one_board(board: dict) -> None:
        local: list[dict] = []
        for window_start, window_end in windows:
            with lock:
                counters["requests"] += 1
            try:
                data = client.get(
                    "/api/a-share-index/prices/historical",
                    thscode=board["thscode"],
                    interval="1d",
                    start=_ms(window_start),
                    end=_ms(window_end),
                )
            except Exception as exc:  # noqa: BLE001 — one board must not end the sweep
                with lock:
                    counters["failed"] += 1
                logger.warning("ths_official sectors failed for %s: %s", board["thscode"], exc)
                continue
            items = (data or {}).get("item") or []
            if not items:
                with lock:
                    counters["empty"] += 1
                continue
            for item in items:
                stamp = item.get("date_ms")
                if not stamp:
                    continue
                local.append(
                    {
                        "sector_code": board["sector_code"],
                        "sector_name": board["sector_name"],
                        "board_type": board["board_type"],
                        "trade_date": datetime.fromtimestamp(stamp / 1000, tz=CST).date(),
                        "open": item.get("open_price"),
                        "high": item.get("high_price"),
                        "low": item.get("low_price"),
                        "close": item.get("close_price"),
                        "volume": item.get("volume"),
                        "amount": item.get("turnover"),
                        # The upstream reports no session change; the lake's own
                        # derivation from consecutive closes is the honest source
                        # for it, so this stays null rather than being guessed.
                        "change_pct": None,
                    }
                )
        nonlocal done
        with lock:
            rows.extend(local)
            counters["boards"] += 1 if local else 0
            done += 1
            finished = done
        report(finished)

    if workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(one_board, boards))
    else:
        for board in boards:
            one_board(board)

    if not rows:
        return pl.DataFrame(schema=_OUTPUT_SCHEMA), counters
    frame = pl.DataFrame(rows).select(
        pl.col("sector_code").cast(pl.Utf8),
        pl.col("sector_name").cast(pl.Utf8),
        pl.col("board_type").cast(pl.Utf8),
        pl.col("trade_date").cast(pl.Date),
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64).round(0).cast(pl.Int64),
        pl.col("amount").cast(pl.Float64),
        pl.col("change_pct").cast(pl.Float64),
    )
    return frame.unique(subset=["sector_code", "trade_date"], keep="last").sort(
        ["sector_code", "trade_date"]
    ), counters
