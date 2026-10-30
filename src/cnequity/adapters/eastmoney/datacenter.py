"""EastMoney datacenter pagination helper."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from urllib.parse import quote

from cnequity.adapters.eastmoney.common import DATACENTER_BASE
from cnequity.adapters.eastmoney.em_auth import EastMoneyClient
from cnequity.adapters.eastmoney.raw import archive_response
from cnequity.domain.http_policy import SourceCoolingDown, record_business_refusal
from cnequity.storage.raw_archive import RawArchiveError, RawPayloadArchive

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

    reason_code = "source_payload_invalid"


class _TransientEmptyPage(Exception):
    """Empty response on a page the first page's `pages` field says must exist."""


class _ServerBusy(Exception):
    """Datacenter said it was busy — retry, do not report a schema break."""


def _non_negative_int(value: object, *, field: str, report: str) -> int | None:
    """Normalize datacenter pagination metadata without silently disabling guards."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise EastMoneyDatacenterError(
            f"EastMoney datacenter {report} returned invalid {field}={value!r}; "
            "expected a non-negative integer"
        )
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.strip().isdigit():
        parsed = int(value.strip())
    else:
        raise EastMoneyDatacenterError(
            f"EastMoney datacenter {report} returned invalid {field}={value!r}; "
            "expected a non-negative integer"
        )
    if parsed < 0:
        raise EastMoneyDatacenterError(
            f"EastMoney datacenter {report} returned invalid {field}={value!r}; "
            "expected a non-negative integer"
        )
    return parsed


def _is_busy(message: str) -> bool:
    return any(token in message for token in _BUSY_MESSAGES)


def _parenthesize_filter(expr: str) -> str:
    """Return one datacenter filter expression in a stable form.

    Wrapping an already-joined ``(a)(b)`` base again is harmless:
    ``((a)(b))(c)`` is accepted. See `_join_filters` for why predicates are
    adjacent rather than joined with ``AND``.
    """
    text = str(expr or "").strip()
    if text.startswith("(") and text.endswith(")"):
        return text
    return f"({text})"


def _join_filters(base: str, predicate: str) -> str:
    """Compose ``(base)(predicate)``: datacenter ANDs adjacent predicates.

    Not ``AND``. Datacenter accepts ``(base) AND (predicate)`` and silently
    drops everything after the ``AND`` (live, 2026-10-31, all four F10
    shareholder reports): a keyset continuation then re-read page 1 of the
    unfiltered window, and the equality recovery returned the whole report,
    so top_holders stopped at the 100-page cap (56,505 rows, 2026-06-30).
    """
    right = _parenthesize_filter(predicate)
    return f"{_parenthesize_filter(base)}{right}" if str(base or "").strip() else right


def _string_literal(value: object) -> str:
    """A datacenter string literal. Strings take double quotes: a single-quoted
    string in an appended predicate is refused with 9501 (dates in the base
    range accept single quotes)."""
    return '"' + str(value).replace('"', '""') + '"'


def _keyset_filter(base: str, column: str, bound: str) -> str:
    """Build a strict keyset continuation predicate.

    A strict boundary is important.  Re-using ``>=`` makes every page-cap
    shard replay the boundary row(s), and relying on downstream dedupe then
    hides a source ordering bug while making it possible to lose a partial
    key group.  Duplicate key groups are recovered explicitly below before
    this predicate is emitted.
    """
    return _join_filters(base, f"{column}>{_string_literal(bound)}")


def _column_tokens(columns: str) -> set[str]:
    return {token.strip() for token in str(columns).split(",") if token.strip()}


def _validate_keyset_contract(
    *,
    columns: str,
    sort_columns: str | None,
    sort_types: str | None,
    keyset_column: str | None,
) -> None:
    """Fail before the first request when keyset ordering is not provable."""
    if keyset_column is None:
        return
    key = str(keyset_column).strip()
    if not key or "," in key:
        raise ValueError("keyset_column must name one column")
    if key not in _column_tokens(columns):
        raise ValueError(f"keyset_column {key!r} is absent from datacenter columns={columns!r}")
    if not sort_columns:
        # The caller omitted an order.  The fetcher can supply the only order
        # that makes a strict keyset cursor meaningful.
        return
    ordered = [token.strip() for token in str(sort_columns).split(",") if token.strip()]
    if key not in ordered:
        raise ValueError(f"keyset_column {key!r} must be included in sort_columns={sort_columns!r}")
    if ordered[0] != key:
        raise ValueError(
            f"keyset_column {key!r} must be the leading sort column; "
            f"got sort_columns={sort_columns!r}"
        )
    if sort_types:
        directions = [token.strip() for token in str(sort_types).split(",") if token.strip()]
        index = ordered.index(key)
        if index >= len(directions) or directions[index] not in {"1", "+1"}:
            raise ValueError(
                f"keyset_column {key!r} requires ascending sort_types; "
                f"got sort_columns={sort_columns!r}, sort_types={sort_types!r}"
            )


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
    on_valid_page: Callable[[list[dict]], None] | None = None,
    keyset_column: str | None = None,
    trust_page_size: bool = False,
    allow_count_overrun: bool = False,
    archive: RawPayloadArchive | None = None,
    archive_dataset: str | None = None,
    archive_run_id: str | None = None,
    archive_source: str = "eastmoney",
    archive_request_scope: str | None = None,
) -> list[dict]:
    """Page a datacenter report to exhaustion, or until *stop_after* says stop.

    ``stop_after`` receives each page's rows and returns True when no later page
    can contain anything the caller wants. It only makes sense on a sorted
    report, and it deliberately suppresses the completeness guards below — a
    short read is the point, so the declared ``count`` will not match.

    ``keyset_column`` is what makes reports longer than ``_MAX_PAGE_NUMBER``
    pages reachable at all. On reaching the cap the sweep re-anchors with a
    legal ``(base) AND (COL>'<last value seen>')`` filter and restarts at page
    1, so the page counter never has to exceed 100. It requires the report to
    be sorted ascending by that same leading column. If a page boundary falls
    in the middle of a duplicate-key group (a symbol with ten holder rows), the
    group is fetched once with an equality slice before the strict continuation
    is emitted. Without it, a report over the cap raises rather than silently
    truncating.

    ``trust_page_size`` sends ``page_size`` past the 500 clamp for the reports
    measured to honor it (see ``_MAX_PAGE_SIZE``). Fewer pages is the point:
    it is what keeps a 90k-row report clear of the pageNumber cap.
    """
    if not trust_page_size:
        page_size = min(page_size, _MAX_PAGE_SIZE)
    if keyset_column is not None:
        _validate_keyset_contract(
            columns=columns,
            sort_columns=sort_columns,
            sort_types=sort_types,
            keyset_column=keyset_column,
        )
        if not sort_columns:
            sort_columns = keyset_column
        if not sort_types:
            sort_types = "1"
    stopped_early = False
    rows: list[dict] = []
    # First page's result.pages/count; a transient "返回数据为空" on a later
    # page looks exactly like end-of-data and silently truncates at a 500×k
    # boundary (margin_trading 2026-07-02/03), so verify against these. Both are
    # per-keyset-shard and reset when the sweep re-anchors.
    expected_pages: int | None = None
    expected_count: int | None = None
    shard_rows = 0
    unique_row_signatures: set[str] = set()
    keyset_bound: str | None = None
    shard_last_key: str | None = None
    page = 1
    while True:
        effective_filter = filter_expr
        if keyset_bound is not None:
            effective_filter = _keyset_filter(effective_filter, keyset_column, keyset_bound)
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
                resp = (
                    client.get(url, _request_event="retry")
                    if attempt and isinstance(client, EastMoneyClient)
                    else client.get(url)
                )
                resp.raise_for_status()
                payload = resp.json()
                if archive is not None and archive_dataset and archive.enabled:
                    archive_response(
                        archive,
                        archive_dataset,
                        resp,
                        request_params={
                            "report": report,
                            "columns": columns,
                            "filter": effective_filter,
                            "page_size": page_size,
                            "page_number": page,
                        },
                        run_id=archive_run_id,
                        url=url,
                        source=archive_source,
                        request_scope=archive_request_scope,
                        observation_id=(
                            f"{archive_run_id or 'anonymous'}:{archive_source}:{report}:"
                            f"{effective_filter}:{page}:"
                            f"scope={archive_request_scope or 'scope-unknown'}"
                        ),
                        pagination={
                            "page": page,
                            "page_size": page_size,
                        },
                    )
                if payload.get("success") is False:
                    message = str(payload.get("message") or "")
                    if _is_busy(message):
                        if "请求过于频繁" in message:
                            record_business_refusal(
                                getattr(client, "config", None),
                                "eastmoney_dc",
                                kind="rate_limited",
                            )
                            raise SourceCoolingDown(
                                "EastMoney datacenter 报告请求过于频繁；共享冷却已开启"
                            )
                        raise _ServerBusy(f"{message} on page {page}")
                    if (
                        message in _EMPTY_RESULT_MESSAGES
                        and expected_pages is not None
                        and page <= expected_pages
                    ):
                        raise _TransientEmptyPage(f"empty response on page {page}/{expected_pages}")
                last_exc = None
                break
            except (RawArchiveError, SourceCoolingDown):
                # Missing exact response bytes are an archive-policy failure,
                # not a transient source error.  Do not retry and eventually
                # hide the concrete integrity failure behind a pagination
                # wrapper.
                raise
            except Exception as exc:
                last_exc = exc
                from cnequity.adapters.eastmoney.em_auth import (
                    is_transport_fail_fast,
                )

                if is_transport_fail_fast(exc):
                    break
                if attempt + 1 < max_retries:
                    time.sleep(retry_backoff_seconds * (attempt + 1))
        if last_exc is not None:
            if isinstance(last_exc, _ServerBusy):
                # "Busy" arrives as HTTP 200, so the client never counts it as a
                # refusal. Busy through every backoff is datacenter throttling
                # this IP outright: stop asking for the day before it escalates.
                from cnequity.adapters.eastmoney import host_guard

                host_guard.trip(getattr(client, "config", None), url, f"busy: {last_exc}")
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
                f"EastMoney datacenter {report} page {page} failed after {max_retries} attempts: "
                f"{last_exc}"
            ) from last_exc

        if payload.get("success") is False:
            msg = str(payload.get("message") or "")
            if msg not in _EMPTY_RESULT_MESSAGES:
                code = payload.get("code")
                # Column renames land here as code=9501 ("X列不存在"). Name the
                # report so ops can jump to the adapter.
                raise EastMoneyDatacenterError(
                    f"EastMoney datacenter {report} rejected schema: {msg}"
                    + (f" (code={code})" if code is not None else "")
                )
            break

        result = payload.get("result")
        if not isinstance(result, dict):
            raise EastMoneyDatacenterError(
                f"EastMoney datacenter {report} returned success without a result object"
            )
        if expected_pages is None:
            parsed_pages = _non_negative_int(result.get("pages"), field="pages", report=report)
            if parsed_pages is not None:
                expected_pages = parsed_pages
            expected_count = _non_negative_int(result.get("count"), field="count", report=report)
        raw_batch = result.get("data")
        if raw_batch is None:
            batch: list[dict] = []
        elif isinstance(raw_batch, list):
            if any(not isinstance(item, dict) for item in raw_batch):
                raise EastMoneyDatacenterError(
                    f"EastMoney datacenter {report} page {page} contains a non-object row"
                )
            batch = raw_batch
        else:
            raise EastMoneyDatacenterError(
                f"EastMoney datacenter {report} returned a non-list result.data"
            )
        if keyset_column is not None and batch:
            key_values = [str(item.get(keyset_column) or "") for item in batch]
            if any(not value for value in key_values):
                raise EastMoneyDatacenterError(
                    f"EastMoney datacenter {report} page {page} contains a missing "
                    f"keyset column {keyset_column!r}"
                )
            if key_values != sorted(key_values):
                raise EastMoneyDatacenterError(
                    f"EastMoney datacenter {report} page {page} is not ordered by "
                    f"ascending {keyset_column!r}"
                )
            if shard_last_key is not None and key_values[0] < shard_last_key:
                raise EastMoneyDatacenterError(
                    f"EastMoney datacenter {report} page {page} moved backwards in "
                    f"{keyset_column!r}: {key_values[0]!r} < {shard_last_key!r}"
                )
            if keyset_bound is not None and key_values[0] <= keyset_bound:
                raise EastMoneyDatacenterError(
                    f"EastMoney datacenter {report} keyset continuation did not advance "
                    f"{keyset_column!r}: bound={keyset_bound!r}, first={key_values[0]!r}"
                )
            shard_last_key = key_values[-1]
        if batch and expected_pages == 0:
            raise EastMoneyDatacenterError(
                f"EastMoney datacenter {report} declared pages=0 but returned rows"
            )
        if batch and expected_count == 0:
            raise EastMoneyDatacenterError(
                f"EastMoney datacenter {report} declared count=0 but returned rows"
            )
        rows.extend(batch)
        shard_rows += len(batch)
        for row in batch:
            unique_row_signatures.add(
                json.dumps(
                    row, sort_keys=True, ensure_ascii=False, default=str, separators=(",", ":")
                )
            )
        unique_rows = len(unique_row_signatures)
        if (
            expected_count is not None
            and shard_rows > expected_count
            and unique_rows > expected_count
            and not allow_count_overrun
        ):
            raise EastMoneyDatacenterError(
                f"EastMoney datacenter {report} returned more than declared "
                f"count={expected_count} rows ({shard_rows}; {unique_rows} unique rows)"
            )
        if (
            expected_count is not None
            and shard_rows > expected_count
            and (allow_count_overrun or unique_rows <= expected_count)
        ):
            logger.warning(
                "EastMoney datacenter %s count drifted upward: declared %d, observed %d raw/%d unique rows",
                report,
                expected_count,
                shard_rows,
                unique_rows,
            )
        if on_valid_page is not None and batch:
            # A page that passed row-shape, ordering and count guards is an
            # independently observed set of positive facts. Callers may
            # preserve it even if a later page fails. It is not a receipt for
            # the completeness of the enclosing dynamic report.
            on_valid_page(batch)
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
            trailing = _keyset_trailing_count(rows, keyset_column, next_bound)
            if trailing:
                # A strict ``>`` continuation cannot see the remainder of a
                # key group that straddled the cap.  Retrieve that group using
                # the same base constraints, then replace the partial tail in
                # place.  Refuse a server that ignored equality: silently
                # accepting that response would duplicate or omit data.
                boundary_filter = _join_filters(
                    effective_filter,
                    f"{keyset_column}={_string_literal(next_bound)}",
                )
                boundary_rows = fetch_datacenter(
                    client,
                    report,
                    columns,
                    filter_expr=boundary_filter,
                    page_size=page_size,
                    sort_columns=sort_columns,
                    sort_types=sort_types,
                    max_retries=max_retries,
                    retry_backoff_seconds=retry_backoff_seconds,
                    trust_page_size=trust_page_size,
                    allow_count_overrun=allow_count_overrun,
                    on_valid_page=on_valid_page,
                    archive=archive,
                    archive_dataset=archive_dataset,
                    archive_run_id=archive_run_id,
                    archive_source=archive_source,
                    archive_request_scope=archive_request_scope,
                )
                if any(str(item.get(keyset_column) or "") != next_bound for item in boundary_rows):
                    raise EastMoneyDatacenterError(
                        f"EastMoney datacenter {report} equality recovery for "
                        f"{keyset_column}={next_bound!r} returned rows outside the boundary key"
                    )
                del rows[-trailing:]
                rows.extend(boundary_rows)
                if not boundary_rows:
                    raise EastMoneyDatacenterError(
                        f"EastMoney datacenter {report} equality recovery for "
                        f"{keyset_column}={next_bound!r} returned no rows"
                    )
            logger.debug(
                "%s: page cap reached, re-anchoring %s>%r (%d rows so far)",
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
            unique_row_signatures = set()
            shard_last_key = None
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
    """Return the trailing key without mutating the accumulated result."""
    if not rows:
        return None
    last = str(rows[-1].get(keyset_column) or "")
    if not last:
        return None
    return last


def _keyset_trailing_count(rows: list[dict], keyset_column: str, bound: str) -> int:
    """Count the boundary-key tail that may be incomplete at a page cap."""
    count = 0
    for row in reversed(rows):
        if str(row.get(keyset_column) or "") != bound:
            break
        count += 1
    return count
