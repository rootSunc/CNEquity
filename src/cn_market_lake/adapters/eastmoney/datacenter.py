"""EastMoney datacenter pagination helper."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from urllib.parse import quote

from cn_market_lake.adapters.eastmoney.common import DATACENTER_BASE
from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient

logger = logging.getLogger(__name__)

_EMPTY_RESULT_MESSAGES = frozenset({"返回数据为空"})

# The server refusing *this moment*, not the request. Reported the same way a
# broken schema is — success=false with a message — so without singling it out
# a busy server surfaced as "rejected schema" and, worse, was raised outside
# the retry loop and never retried.
_BUSY_MESSAGES = ("服务器繁忙", "系统繁忙", "请求过于频繁")

# EastMoney datacenter caps pageSize at 500; requesting more returns only 500
# rows and — because the short page looks like the last one — silently
# truncates every high-volume report. Clamp so pagination stays correct.
#
# The cap is per report, not global. Measured 2026-08-11: RPT_BOARD_CONSTITUENT
# serves a full 5000-row page, while RPT_SHAREBONUS_DET answers a pageSize=1000
# request with 500 rows and a pages count computed at 500. So the clamp stays
# the default and a caller that measured a bigger page opts out with
# trust_page_size — safe because a report that stops honoring it trips the
# short-page guard below and raises instead of truncating.
_MAX_PAGE_SIZE = 500

# pageNumber is capped at 100 and the refusal is a *lie*: page 101 answers
# 服务器繁忙 (code 9701), the same thing the server says when genuinely loaded,
# so a report needing more than 100 pages looks like throttling that no amount
# of waiting clears. Measured on RPT_F10_EH_FREEHOLDERS (54,679 rows / 110
# pages): page 100 at pageSize 500 succeeds, page 101 fails; page 51 at
# pageSize 1000 — the same 50,000-row offset — succeeds, and page 499 at
# pageSize 100 fails well below it. The limit follows the page number, not the
# offset, so a bigger pageSize only moves the wall.
#
# The way through is `keyset_column`: re-anchor the filter on the last key seen
# and start again at page 1. See fetch_datacenter.
_MAX_PAGE_NUMBER = 100


class EastMoneyDatacenterError(RuntimeError):
    """Raised when datacenter pagination fails after partial or zero results."""


class _TransientEmptyPage(Exception):
    """Empty response on a page the first page's `pages` field says must exist."""


class _ServerBusy(Exception):
    """Datacenter said it was busy — retry, do not report a schema break."""


def _is_busy(message: str) -> bool:
    return any(token in message for token in _BUSY_MESSAGES)


def fetch_datacenter(
    client: EastMoneyClient,
    report: str,
    columns: str,
    *,
    filter_expr: str = "",
    page_size: int = 5000,
    sort_columns: str | None = None,
    sort_types: str | None = None,
    max_retries: int = 3,
    retry_backoff_seconds: float = 5.0,
    stop_after: Callable[[list[dict]], bool] | None = None,
    keyset_column: str | None = None,
    trust_page_size: bool = False,
) -> list[dict]:
    """Page a datacenter report to exhaustion, or until *stop_after* says stop.

    ``stop_after`` receives each page's rows and returns True when no later page
    can contain anything the caller wants. It only makes sense on a sorted
    report, and it deliberately suppresses the completeness guards below — a
    short read is the point, so the declared ``count`` will not match.

    ``keyset_column`` is what makes reports longer than ``_MAX_PAGE_NUMBER``
    pages reachable at all. On reaching the cap the sweep appends
    ``(COL>="<last value seen>")`` to the filter and restarts at page 1, so the
    page counter never has to exceed 100. It requires the report to be sorted
    ascending by that same column, and the trailing rows sharing the last key
    are dropped before re-anchoring — a page boundary falling mid-key (a symbol
    with ten holder rows) would otherwise lose the remainder of that group.
    Without it, a report over the cap raises rather than silently truncating.

    ``trust_page_size`` sends ``page_size`` past the 500 clamp for the reports
    measured to honor it (see ``_MAX_PAGE_SIZE``). Fewer pages is the point:
    it is what keeps a 90k-row report clear of the pageNumber cap.
    """
    if not trust_page_size:
        page_size = min(page_size, _MAX_PAGE_SIZE)
    stopped_early = False
    rows: list[dict] = []
    # First page's result.pages/count; a transient "返回数据为空" on a later
    # page looks exactly like end-of-data and silently truncates at a 500×k
    # boundary (margin_trading 2026-07-02/03), so verify against these. Both are
    # per-keyset-shard and reset when the sweep re-anchors.
    expected_pages: int | None = None
    expected_count: int | None = None
    shard_rows = 0
    keyset_bound: str | None = None
    page = 1
    while True:
        effective_filter = filter_expr
        if keyset_bound is not None:
            effective_filter += f'({keyset_column}>="{keyset_bound}")'
        params = (
            f"reportName={report}"
            f"&columns={quote(columns, safe=',')}"
            f"&pageSize={page_size}"
            f"&pageNumber={page}"
        )
        if effective_filter:
            # `>` and `<` must NOT be in the safe set. Left raw they are illegal
            # in a query, so httpx re-quotes the whole component on its way out
            # and every %27 already written here becomes %2527 — the server then
            # rejects the filter with InputMismatchException. Encoding them to
            # %3E/%3C ourselves leaves nothing for httpx to normalize.
            params += f"&filter={quote(effective_filter, safe='()=')}"
        if sort_columns:
            params += f"&sortColumns={sort_columns}&sortTypes={sort_types or '-1'}"
        url = f"{DATACENTER_BASE}?{params}"

        last_exc: Exception | None = None
        payload = None
        for attempt in range(max_retries):
            try:
                resp = client.get(url)
                resp.raise_for_status()
                payload = resp.json()
                if payload.get("success") is False:
                    message = str(payload.get("message") or "")
                    if _is_busy(message):
                        raise _ServerBusy(f"{message} on page {page}")
                    if (
                        message in _EMPTY_RESULT_MESSAGES
                        and expected_pages is not None
                        and page <= expected_pages
                    ):
                        raise _TransientEmptyPage(f"empty response on page {page}/{expected_pages}")
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                from cn_market_lake.adapters.eastmoney.em_auth import (
                    is_transport_fail_fast,
                )

                if is_transport_fail_fast(exc):
                    break
                if attempt + 1 < max_retries:
                    time.sleep(retry_backoff_seconds * (attempt + 1))
        if last_exc is not None:
            if isinstance(last_exc, _ServerBusy):
                raise EastMoneyDatacenterError(
                    f"EastMoney datacenter {report} still busy after {max_retries} attempts "
                    f"({last_exc}); this is throttling, not a schema break — retry later or "
                    "raise [sources.eastmoney].min_interval_seconds"
                ) from last_exc
            if isinstance(last_exc, _TransientEmptyPage):
                raise EastMoneyDatacenterError(
                    f"EastMoney datacenter {report} truncated: {last_exc} persisted after "
                    f"{max_retries} attempts; got {len(rows)} of {expected_count} rows"
                ) from last_exc
            raise EastMoneyDatacenterError(
                f"EastMoney datacenter {report} page {page} failed after {max_retries} attempts"
            ) from last_exc

        if payload.get("success") is False:
            msg = str(payload.get("message") or "")
            if msg not in _EMPTY_RESULT_MESSAGES:
                code = payload.get("code")
                # Column renames land here as code=9501 ("X列不存在"). Name the
                # report so ops can jump to datacenter_contracts / the adapter.
                raise EastMoneyDatacenterError(
                    f"EastMoney datacenter {report} rejected schema: {msg}"
                    + (f" (code={code})" if code is not None else "")
                )
            break

        result = payload.get("result") or {}
        if expected_pages is None:
            raw_pages = result.get("pages")
            if isinstance(raw_pages, int) and raw_pages > 0:
                expected_pages = raw_pages
            raw_count = result.get("count")
            if isinstance(raw_count, int):
                expected_count = raw_count
        batch = result.get("data") or []
        rows.extend(batch)
        shard_rows += len(batch)
        if stop_after is not None and stop_after(batch):
            stopped_early = True
            break
        if expected_pages is not None:
            if page >= expected_pages:
                break
            if len(batch) < page_size:
                raise EastMoneyDatacenterError(
                    f"EastMoney datacenter {report} page {page}/{expected_pages} returned "
                    f"{len(batch)} rows (expected {page_size}); refusing truncated result"
                )
        elif len(batch) < page_size:
            break

        if page >= _MAX_PAGE_NUMBER:
            if keyset_column is None:
                raise EastMoneyDatacenterError(
                    f"EastMoney datacenter {report} needs more than {_MAX_PAGE_NUMBER} pages "
                    f"(count={expected_count}, pageSize={page_size}) and pageNumber is capped "
                    f"there; pass keyset_column to page past it"
                )
            next_bound = _next_keyset_bound(rows, keyset_column)
            if next_bound is None or next_bound == keyset_bound:
                raise EastMoneyDatacenterError(
                    f"EastMoney datacenter {report}: cannot re-anchor on {keyset_column} at "
                    f"page {page} (bound={next_bound!r}); the whole page shares one key or the "
                    "column is absent from columns="
                    f"{columns!r}"
                )
            logger.debug(
                "%s: page cap reached, re-anchoring %s>=%r (%d rows so far)",
                report,
                keyset_column,
                next_bound,
                len(rows),
            )
            keyset_bound = next_bound
            page = 1
            expected_pages = None
            expected_count = None
            shard_rows = 0
            continue
        page += 1
    # count is the current shard's, so compare against that shard's rows —
    # earlier shards were cut off at the page cap deliberately.
    if not stopped_early and expected_count is not None and shard_rows < expected_count:
        raise EastMoneyDatacenterError(
            f"EastMoney datacenter {report} returned {shard_rows} rows but page 1 "
            f"declared count={expected_count}; refusing truncated result"
        )
    return rows


def _next_keyset_bound(rows: list[dict], keyset_column: str) -> str | None:
    """Drop the trailing rows sharing the last key and return that key.

    The dropped rows are re-fetched by the next shard, which starts at ``>=``
    this value. Trimming is what keeps a key group that straddles the page-cap
    boundary intact; ``>`` on an untrimmed tail would swallow its remainder.
    """
    if not rows:
        return None
    last = str(rows[-1].get(keyset_column) or "")
    if not last:
        return None
    while rows and str(rows[-1].get(keyset_column) or "") == last:
        rows.pop()
    return last
