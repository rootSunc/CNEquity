"""EastMoney F10 shareholder structure — 股本结构 / 股东户数 / 前十大股东.

Three datasets the long-format ``financial_statement_items`` cannot hold.
``top_holders`` is the clearest case: it is a ranked repeating group of ten per
period, and no number of ``item_code`` rows expresses a rank. The other two are
wide fixed records that would only be item codes by accident of shape.

Everything here is swept **market-wide with a filter**, never per symbol. One
period of 前十大流通股东 is ~55k rows, so a per-symbol sweep would be ~5,500
requests where one filtered sweep is ~110 pages. These are the heaviest
datacenter consumers in the project.

What the filter is on differs, and the difference matters. 股东户数 and the two
holder reports are keyed by report period, so they are swept per period. 股本结构
is **not**: ``RPT_F10_EH_EQUITY.END_DATE`` is the date the share count changed,
an arbitrary date, so it is swept by date window instead. Filtering it on
quarter-ends returns plenty of rows and quietly omits most of the report.

PIT. A 半年报 shareholder list is dated 06-30 and disclosed in late August, so
keying it by period alone lets a July backtest read August's filing. Three of
the four reports carry ``NOTICE_DATE``; ``RPT_F10_EH_HOLDERS`` does not, so its
announce date is joined from ``RPT_F10_EH_FREEHOLDERS`` on (symbol, period) —
the two are halves of the same filing and share a disclosure date by
construction. Rows that find no match are dropped rather than dated with a
guess, and the count is logged.
"""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cn_market_lake.adapters.eastmoney.common import symbol_from_secucode
from cn_market_lake.adapters.eastmoney.datacenter import fetch_datacenter
from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient
from cn_market_lake.config import Config

logger = logging.getLogger(__name__)

_EQUITY_REPORT = "RPT_F10_EH_EQUITY"
_EQUITY_COLUMNS = (
    "SECUCODE,END_DATE,TOTAL_SHARES,LIMITED_SHARES,UNLIMITED_SHARES,"
    "FREELIQCI_SHARES,CHANGE_REASON,NOTICE_DATE"
)

_HOLDERNUM_REPORT = "RPT_F10_EH_HOLDERNUM"
_HOLDERNUM_COLUMNS = (
    "SECUCODE,END_DATE,HOLDER_TOTAL_NUM,TOTAL_NUM_RATIO,AVG_FREE_SHARES,AVG_HOLD_AMT,NOTICE_DATE"
)

# 前十大股东 (all shares). No NOTICE_DATE on this report — see module docstring.
_HOLDERS_REPORT = "RPT_F10_EH_HOLDERS"
_HOLDERS_COLUMNS = "SECUCODE,END_DATE,HOLDER_NAME,HOLD_NUM,HOLD_NUM_RATIO,HOLDER_RANK,IS_HOLDORG"

# 前十大流通股东 (float only). Carries NOTICE_DATE and a holder classification.
_FREEHOLDERS_REPORT = "RPT_F10_EH_FREEHOLDERS"
_FREEHOLDERS_COLUMNS = (
    "SECUCODE,END_DATE,HOLDER_NAME,HOLD_NUM,FREE_HOLDNUM_RATIO,HOLDER_RANK,"
    "IS_HOLDORG,HOLDER_TYPE,NOTICE_DATE"
)

CHANGE_DATE = "change_date"
NOTICE_DATE = "notice_date"

SCOPE_TOTAL = "total"
SCOPE_FLOAT = "float"

# One period of 前十大流通股东 is ~55k rows = ~110 pages, and pageNumber is
# capped at 100 — see _MAX_PAGE_NUMBER in datacenter.py, which reports the cap
# as 服务器繁忙 and so looks exactly like throttling that patience would clear.
# It is not; waiting does nothing. Sorting by SECUCODE and handing the same
# column to keyset_column is what actually gets past page 100.
_KEYSET_COLUMN = "SECUCODE"

# Genuine busy answers do also happen on a sweep this long. A little more
# patience than the default 3/5s is cheap here because failing at page 90 throws
# away the 89 pages before it.
_SWEEP_RETRIES = 5
_SWEEP_BACKOFF_SECONDS = 15.0


def _num(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _em_date(value: object) -> date | None:
    text = str(value or "").strip()
    if len(text) < 10:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _is_org(value: object) -> bool | None:
    """IS_HOLDORG is "1"/"0" as a string; anything else is unknown, not False."""
    text = str(value or "").strip()
    if text == "1":
        return True
    if text == "0":
        return False
    return None


def _range_filter(column: str, start: date, end: date) -> str:
    """Inclusive at both ends.

    Date literals MUST be single-quoted. Unquoted the datacenter answers 9501
    (``参数预处理错误``) — it does not return fewer rows, it refuses the query,
    which at least fails loudly rather than looking like an empty window.
    """
    return f"({column}>='{start.isoformat()}')({column}<='{end.isoformat()}')"


def _fetch_filtered(
    client: EastMoneyClient,
    report: str,
    columns: str,
    filter_expr: str,
    *,
    config: Config | None,
) -> list[dict]:
    if config is not None:
        config.rate_limit("eastmoney")
    return fetch_datacenter(
        client,
        report,
        columns,
        filter_expr=filter_expr,
        # Ascending by the keyset column is a precondition of re-anchoring.
        sort_columns=_KEYSET_COLUMN,
        sort_types="1",
        keyset_column=_KEYSET_COLUMN,
        max_retries=_SWEEP_RETRIES,
        retry_backoff_seconds=_SWEEP_BACKOFF_SECONDS,
    )


def fetch_share_structure(
    start: date,
    end: date,
    *,
    by: str = CHANGE_DATE,
    client: EastMoneyClient | None = None,
    config: Config | None = None,
) -> pl.DataFrame:
    """股本结构变动 in a date window.

    Unlike the other two reports, ``RPT_F10_EH_EQUITY.END_DATE`` is the date the
    share count *changed*, not a report period: 600519 has 16 rows in its entire
    history, dated things like 2015-07-17 (送股上市) and 2025-09-01 (回购).
    Sweeping quarter-ends therefore collects only the changes that happen to
    fall on one — 2,446 rows on 2025-06-30 against ~50 a day for ordinary dates,
    which looks plausible right up until you notice the rest is missing.

    *by* picks which date the window applies to. ``change_date`` (END_DATE)
    matches the partition column, so a backfill window writes exactly the
    partitions it names. ``notice_date`` is for daily runs, where the question
    is "what was newly disclosed", and a change that took effect weeks ago can
    be announced today.
    """
    column = "END_DATE" if by == CHANGE_DATE else "NOTICE_DATE"
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)
    try:
        raw = _fetch_filtered(
            client,
            _EQUITY_REPORT,
            _EQUITY_COLUMNS,
            _range_filter(column, start, end),
            config=config,
        )
    finally:
        if owns:
            client.close()

    rows: list[dict] = []
    for item in raw:
        symbol = symbol_from_secucode(item.get("SECUCODE"))
        change_date = _em_date(item.get("END_DATE"))
        if not symbol or change_date is None:
            continue
        rows.append(
            {
                "symbol": symbol,
                "change_date": change_date,
                "total_shares": _num(item.get("TOTAL_SHARES")),
                "float_shares": _num(item.get("UNLIMITED_SHARES")),
                "restricted_shares": _num(item.get("LIMITED_SHARES")),
                "free_float_shares": _num(item.get("FREELIQCI_SHARES")),
                "change_reason": str(item.get("CHANGE_REASON") or "") or None,
                "announce_date": _em_date(item.get("NOTICE_DATE")),
            }
        )
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["symbol", "change_date", "announce_date"], keep="last")


def fetch_shareholder_counts(
    start: date,
    end: date,
    *,
    by: str = CHANGE_DATE,
    client: EastMoneyClient | None = None,
    config: Config | None = None,
) -> pl.DataFrame:
    """股东户数 in a date window.

    Not quarter-only, which is the whole reason this takes a window: companies
    disclose 户数 at 旬末 and 月末 too — 2025-07-10 carries 894 rows, 2025-07-31
    another 1,162, against 5,635 at the quarter-end. Sweeping quarter-ends
    collects the least timely third of a signal whose value is its timeliness.

    *by* windows on ``count_date`` (END_DATE, matching the partition column) or
    ``notice_date`` (NOTICE_DATE, for daily runs — see fetch_share_structure).
    """
    column = "END_DATE" if by == CHANGE_DATE else "NOTICE_DATE"
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)
    try:
        raw = _fetch_filtered(
            client,
            _HOLDERNUM_REPORT,
            _HOLDERNUM_COLUMNS,
            _range_filter(column, start, end),
            config=config,
        )
    finally:
        if owns:
            client.close()

    rows: list[dict] = []
    for item in raw:
        symbol = symbol_from_secucode(item.get("SECUCODE"))
        count_date = _em_date(item.get("END_DATE"))
        if not symbol or count_date is None:
            continue
        rows.append(
            {
                "symbol": symbol,
                "count_date": count_date,
                "holder_count": _num(item.get("HOLDER_TOTAL_NUM")),
                "holder_count_change_pct": _num(item.get("TOTAL_NUM_RATIO")),
                "avg_float_shares": _num(item.get("AVG_FREE_SHARES")),
                "avg_holding_value": _num(item.get("AVG_HOLD_AMT")),
                "announce_date": _em_date(item.get("NOTICE_DATE")),
            }
        )
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["symbol", "count_date", "announce_date"], keep="last")


def _holder_rows(
    raw: list[dict],
    *,
    scope: str,
    pct_field: str,
    window_label: str,
) -> list[dict]:
    rows: list[dict] = []
    off_universe = 0
    unidentifiable = 0
    for item in raw:
        symbol = symbol_from_secucode(item.get("SECUCODE"))
        end = _em_date(item.get("END_DATE"))
        rank = item.get("HOLDER_RANK")
        name = str(item.get("HOLDER_NAME") or "").strip() or None
        if not symbol or end is None:
            # F10 covers B shares (200xxx.SZ / 900xxx.SH) and NEEQ (.NQ); the
            # lake's universe is A shares. Counted rather than silently skipped
            # so "fetched < the count the server declared" has an answer.
            off_universe += 1
            continue
        if rank is None or name is None:
            # Both are in the primary key — see PRIMARY_KEYS["top_holders"].
            unidentifiable += 1
            continue
        rows.append(
            {
                "symbol": symbol,
                "record_date": end,
                "holder_scope": scope,
                "holder_rank": int(rank),
                "holder_name": name,
                "holding_shares": _num(item.get("HOLD_NUM")),
                "holding_pct": _num(item.get(pct_field)),
                "is_institution": _is_org(item.get("IS_HOLDORG")),
                "holder_type": str(item.get("HOLDER_TYPE") or "") or None,
                "announce_date": _em_date(item.get("NOTICE_DATE")),
            }
        )
    logger.info(
        "top_holders %s %s: kept %d of %d raw row(s) "
        "(%d outside the A-share universe, %d without a rank or name)",
        scope,
        window_label,
        len(rows),
        len(raw),
        off_universe,
        unidentifiable,
    )
    return rows


def fetch_top_holders(
    start: date,
    end: date,
    *,
    by: str = CHANGE_DATE,
    client: EastMoneyClient | None = None,
    config: Config | None = None,
) -> pl.DataFrame:
    """前十大股东 + 前十大流通股东 over a record-date window, one frame.

    Mostly quarter-ends but not only — 2025 Q3 has 10,749 total-scope rows dated
    to something else (prospectuses, 权益变动), so this windows on END_DATE
    rather than naming periods.

    The window is always on END_DATE, never the announcement date: the total
    report carries no ``NOTICE_DATE`` at all, and windowing the two reports on
    different columns would leave total rows with no float row to borrow a
    disclosure date from. The float report is fetched first for that reason.

    *by* exists only to keep the three fetchers callable the same way; anything
    but ``change_date`` is refused rather than silently ignored.
    """
    if by != CHANGE_DATE:
        raise ValueError(
            f"fetch_top_holders cannot window on {by!r}: {_HOLDERS_REPORT} has no NOTICE_DATE, "
            "so the two reports would cover different rows and the disclosure-date borrow "
            "would have nothing to match against"
        )
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)
    try:
        free_raw = _fetch_filtered(
            client,
            _FREEHOLDERS_REPORT,
            _FREEHOLDERS_COLUMNS,
            _range_filter("END_DATE", start, end),
            config=config,
        )
        total_raw = _fetch_filtered(
            client,
            _HOLDERS_REPORT,
            _HOLDERS_COLUMNS,
            _range_filter("END_DATE", start, end),
            config=config,
        )
    finally:
        if owns:
            client.close()

    label = f"{start.isoformat()}..{end.isoformat()}"
    free_rows = _holder_rows(
        free_raw, scope=SCOPE_FLOAT, pct_field="FREE_HOLDNUM_RATIO", window_label=label
    )
    total_rows = _holder_rows(
        total_raw, scope=SCOPE_TOTAL, pct_field="HOLD_NUM_RATIO", window_label=label
    )

    frames: list[pl.DataFrame] = []
    if free_rows:
        frames.append(pl.DataFrame(free_rows))

    if total_rows:
        total_df = pl.DataFrame(total_rows)
        # Borrow the disclosure date: RPT_F10_EH_HOLDERS carries none, and the
        # two reports are halves of one filing.
        if free_rows:
            notices = (
                pl.DataFrame(free_rows)
                .select("symbol", "record_date", "announce_date")
                .drop_nulls("announce_date")
                .unique(subset=["symbol", "record_date"], keep="last")
                .rename({"announce_date": "_notice"})
            )
            total_df = total_df.join(notices, on=["symbol", "record_date"], how="left")
            total_df = total_df.with_columns(
                pl.col("announce_date").fill_null(pl.col("_notice"))
            ).drop("_notice")
        # A row with no disclosure date cannot be served point-in-time. Dropping
        # is the honest option: dating it with the period end would assert the
        # list was known on 06-30, which is the exact lookahead this is for.
        undated = total_df.filter(pl.col("announce_date").is_null()).height
        if undated:
            logger.info(
                "top_holders %s: dropping %d total-scope row(s) with no disclosure date "
                "(no matching float-holder filing to borrow one from)",
                label,
                undated,
            )
            total_df = total_df.drop_nulls("announce_date")
        if total_df.height:
            frames.append(total_df)

    if not frames:
        return pl.DataFrame()
    combined = pl.concat(frames, how="diagonal_relaxed")
    deduped = combined.unique(
        subset=[
            "symbol",
            "record_date",
            "holder_scope",
            "holder_rank",
            "holder_name",
            "announce_date",
        ],
        keep="last",
    )
    # What survives here is a genuine restatement under the same disclosure
    # date, not a rank tie — ties are two different holders and holder_name
    # keeps them apart. Logged because collapsing is a second reason the row
    # count lands under the server's declared `count`.
    if deduped.height != combined.height:
        logger.info(
            "top_holders %s: collapsed %d duplicate key(s)",
            label,
            combined.height - deduped.height,
        )
    return deduped.sort(["record_date", "symbol", "holder_scope", "holder_rank"])
