"""Paging past EastMoney's pageNumber cap.

The cap is 100 pages and the server reports it as 服务器繁忙 — the same message
it uses when genuinely loaded. That made a 110-page report look like throttling
no amount of retrying would clear. Keyset re-anchoring is the way through, and
the thing it must not do is lose rows at a shard boundary.
"""

from __future__ import annotations

import re
from urllib.parse import unquote

import pytest

from cn_market_lake.adapters.eastmoney.datacenter import (
    _MAX_PAGE_NUMBER,
    EastMoneyDatacenterError,
    fetch_datacenter,
)

_HOLDERS_PER_SYMBOL = 10


def _table(symbols: int = 250) -> list[dict]:
    """A holder table: one key repeated ten times, like the real report."""
    return [
        {"SECUCODE": f"S{s:03d}", "HOLDER_RANK": rank}
        for s in range(symbols)
        for rank in range(1, _HOLDERS_PER_SYMBOL + 1)
    ]


class FakeDatacenter:
    """Serves a sorted table, enforcing the real pageNumber cap."""

    def __init__(self, rows: list[dict], *, cap: int = _MAX_PAGE_NUMBER):
        self.rows = rows
        self.cap = cap
        self.filters: list[str] = []
        self.urls: list[str] = []

    def get(self, url: str, **kwargs):
        self.urls.append(url)
        page = int(re.search(r"pageNumber=(\d+)", url).group(1))
        size = int(re.search(r"pageSize=(\d+)", url).group(1))
        raw_filter = re.search(r"filter=([^&]*)", url)
        filter_expr = unquote(raw_filter.group(1)) if raw_filter else ""
        self.filters.append(filter_expr)

        if page > self.cap:
            return _Resp({"success": False, "message": "服务器繁忙", "code": 9701})

        rows = self.rows
        bound = re.search(r'SECUCODE>="([^"]+)"', filter_expr)
        if bound:
            rows = [r for r in rows if r["SECUCODE"] >= bound.group(1)]

        start = (page - 1) * size
        data = rows[start : start + size]
        pages = (len(rows) + size - 1) // size
        return _Resp(
            {"success": True, "result": {"data": data, "count": len(rows), "pages": pages}}
        )


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def _fetch(client, **kwargs):
    return fetch_datacenter(
        client,
        "RPT_TEST",
        "SECUCODE,HOLDER_RANK",
        page_size=7,  # straddles the 10-row key groups
        sort_columns="SECUCODE",
        sort_types="1",
        max_retries=1,
        retry_backoff_seconds=0,
        **kwargs,
    )


def test_report_over_the_cap_reads_completely_and_in_order():
    table = _table()
    client = FakeDatacenter(table)
    rows = _fetch(client, keyset_column="SECUCODE")
    # Not "most of it" — every row, once, in order. A dropped key group here is
    # a stock silently missing its top-ten holders.
    assert rows == table


def test_it_actually_had_to_re_anchor():
    """Guards the test above from passing because the cap never got hit."""
    client = FakeDatacenter(_table())
    _fetch(client, keyset_column="SECUCODE")
    anchored = [f for f in client.filters if "SECUCODE>=" in f]
    assert anchored, "expected at least one re-anchored shard"
    # One entry per request, so a shard's pages repeat its bound; collapse to
    # the sequence of shards. Bounds must strictly advance, or the sweep would
    # re-request the same shard forever.
    bounds: list[str] = []
    for f in anchored:
        bound = re.search(r'SECUCODE>="([^"]+)"', f).group(1)
        if not bounds or bounds[-1] != bound:
            bounds.append(bound)
    assert bounds == sorted(bounds)
    assert len(bounds) == len(set(bounds))


def test_re_anchor_lands_on_a_key_boundary_never_mid_group():
    """A page boundary inside a symbol's ten rows must not truncate it."""
    table = _table()
    client = FakeDatacenter(table)
    rows = _fetch(client, keyset_column="SECUCODE")
    counts = {}
    for row in rows:
        counts[row["SECUCODE"]] = counts.get(row["SECUCODE"], 0) + 1
    short = {k: v for k, v in counts.items() if v != _HOLDERS_PER_SYMBOL}
    assert not short, f"symbols with the wrong number of holder rows: {short}"


def test_the_anchored_filter_is_encoded_exactly_once():
    """`>` left raw is illegal in a query, so httpx re-quotes the whole
    component on the way out and every %27 becomes %2527. The server answers
    200 and then rejects the filter with InputMismatchException — a live-only
    failure that unit tests miss unless they look at the wire format."""
    client = FakeDatacenter(_table())
    _fetch(client, keyset_column="SECUCODE", filter_expr="(END_DATE='2025-06-30')")
    filters = [u.split("filter=")[-1].split("&")[0] for u in client.urls]
    anchored = [f for f in filters if "SECUCODE" in f]
    assert anchored, "expected a re-anchored request"
    for query in anchored:
        assert "%25" not in query, f"double-encoded filter: {query}"
        assert "%27" in query, f"lost the quoting of the date literal: {query}"
        assert "%3E" in query, f"`>` left raw for httpx to re-quote: {query}"


def test_without_a_keyset_column_it_names_the_cap_instead_of_blaming_load():
    client = FakeDatacenter(_table())
    with pytest.raises(EastMoneyDatacenterError, match=r"more than 100 pages"):
        _fetch(client)


def test_a_shard_that_is_all_one_key_raises_rather_than_looping():
    """Re-anchoring on the only key present would re-request the same shard."""
    client = FakeDatacenter([{"SECUCODE": "S000", "HOLDER_RANK": i} for i in range(2000)])
    with pytest.raises(EastMoneyDatacenterError, match=r"cannot re-anchor"):
        _fetch(client, keyset_column="SECUCODE")


def test_a_genuine_busy_answer_below_the_cap_is_still_reported_as_busy():
    """The cap fix must not swallow real throttling."""

    class Busy(FakeDatacenter):
        def get(self, url: str, **kwargs):
            return _Resp({"success": False, "message": "服务器繁忙", "code": 9701})

    with pytest.raises(EastMoneyDatacenterError, match=r"still busy"):
        _fetch(Busy(_table()), keyset_column="SECUCODE")
