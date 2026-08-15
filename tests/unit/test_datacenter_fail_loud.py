from unittest.mock import MagicMock

import pytest

from cn_market_lake.adapters.eastmoney.datacenter import (
    EastMoneyDatacenterError,
    fetch_datacenter,
)


class FakeClient:
    def __init__(self, responses: list[Exception | dict]):
        self.responses = responses
        self.calls = 0

    def get(self, url: str, **kwargs):
        if self.calls >= len(self.responses):
            raise RuntimeError("unexpected call")
        item = self.responses[self.calls]
        self.calls += 1
        if isinstance(item, Exception):
            raise item

        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return item

        return Resp()


def test_fetch_datacenter_raises_on_page_failure():
    client = FakeClient([RuntimeError("network"), RuntimeError("network"), RuntimeError("network")])
    with pytest.raises(EastMoneyDatacenterError):
        fetch_datacenter(client, "RPT_TEST", "COL", max_retries=3, retry_backoff_seconds=0)


def test_fetch_datacenter_treats_empty_result_as_no_rows():
    client = FakeClient([{"success": False, "message": "返回数据为空", "code": 0}])
    rows = fetch_datacenter(client, "RPT_TEST", "COL", max_retries=1, retry_backoff_seconds=0)
    assert rows == []


def test_fetch_datacenter_raises_on_api_rejection():
    client = FakeClient([{"success": False, "message": "TRADE_DATE列不存在", "code": 9501}])
    with pytest.raises(
        EastMoneyDatacenterError,
        match=r"RPT_TEST rejected schema: TRADE_DATE列不存在 \(code=9501\)",
    ):
        fetch_datacenter(client, "RPT_TEST", "COL", max_retries=1, retry_backoff_seconds=0)


def test_fetch_datacenter_paginates_until_short_page():
    client = FakeClient(
        [
            {"success": True, "result": {"data": [{"x": 1}] * 5000}},
            {"success": True, "result": {"data": [{"x": 2}]}},
        ]
    )
    rows = fetch_datacenter(client, "RPT_TEST", "COL", page_size=5000)
    assert len(rows) == 5001
    assert client.calls == 2


def test_fetch_datacenter_retries_transient_empty_mid_pagination():
    """A transient 返回数据为空 on page 2 of 2 must be retried, not treated as end-of-data."""
    page1 = {"success": True, "result": {"pages": 2, "count": 750, "data": [{"x": 1}] * 500}}
    page2 = {"success": True, "result": {"pages": 2, "count": 750, "data": [{"x": 2}] * 250}}
    client = FakeClient([page1, {"success": False, "message": "返回数据为空", "code": 0}, page2])
    rows = fetch_datacenter(client, "RPT_TEST", "COL", max_retries=2, retry_backoff_seconds=0)
    assert len(rows) == 750
    assert client.calls == 3


def test_fetch_datacenter_raises_when_empty_mid_pagination_persists():
    """Regression: margin_trading 2026-07-02/03 landed with exactly 500 rows because a
    persistent mid-pagination empty response was taken as end-of-data."""
    page1 = {"success": True, "result": {"pages": 2, "count": 750, "data": [{"x": 1}] * 500}}
    empty = {"success": False, "message": "返回数据为空", "code": 0}
    client = FakeClient([page1, empty, empty])
    with pytest.raises(EastMoneyDatacenterError, match="truncated"):
        fetch_datacenter(client, "RPT_TEST", "COL", max_retries=2, retry_backoff_seconds=0)


def test_fetch_datacenter_raises_on_short_non_final_page():
    page1 = {"success": True, "result": {"pages": 3, "count": 1200, "data": [{"x": 1}] * 500}}
    page2 = {"success": True, "result": {"pages": 3, "count": 1200, "data": [{"x": 2}] * 100}}
    client = FakeClient([page1, page2])
    with pytest.raises(EastMoneyDatacenterError, match="truncated"):
        fetch_datacenter(client, "RPT_TEST", "COL", max_retries=1, retry_backoff_seconds=0)


def test_fetch_datacenter_raises_when_rows_fall_short_of_declared_count():
    """Final page delivered fewer rows than page 1's count field promised."""
    page1 = {"success": True, "result": {"pages": 2, "count": 750, "data": [{"x": 1}] * 500}}
    page2 = {"success": True, "result": {"pages": 2, "count": 750, "data": [{"x": 2}] * 100}}
    client = FakeClient([page1, page2])
    with pytest.raises(EastMoneyDatacenterError, match="count=750"):
        fetch_datacenter(client, "RPT_TEST", "COL", max_retries=1, retry_backoff_seconds=0)


def test_fetch_datacenter_stops_at_declared_pages():
    """Exactly page_size rows on the last declared page must not trigger an extra request."""
    page1 = {"success": True, "result": {"pages": 1, "count": 500, "data": [{"x": 1}] * 500}}
    client = FakeClient([page1])
    rows = fetch_datacenter(client, "RPT_TEST", "COL", max_retries=1, retry_backoff_seconds=0)
    assert len(rows) == 500
    assert client.calls == 1


def test_fetch_datacenter_clamps_page_size_to_500():
    """pageSize>500 must be clamped or EM silently truncates high-volume reports."""
    captured = {}

    class CapClient:
        def get(self, url: str, **kwargs):
            captured["url"] = url

            class Resp:
                def raise_for_status(self):
                    return None

                def json(self):
                    return {"success": True, "result": {"data": []}}

            return Resp()

    fetch_datacenter(CapClient(), "RPT_TEST", "COL", page_size=5000, max_retries=1)
    assert "pageSize=500" in captured["url"]
    assert "pageSize=5000" not in captured["url"]


# --- throttling is not a schema break ---------------------------------------
# The datacenter reports "服务器繁忙" the same way it reports a broken schema:
# success=false with a message. Without singling it out, a busy server surfaced
# as "rejected schema" — and was raised outside the retry loop, so it was never
# retried. Hit while sweeping the F10 shareholder reports (~110 pages each).


def test_server_busy_is_retried_then_reported_as_throttling():
    import pytest

    from cn_market_lake.adapters.eastmoney.datacenter import (
        EastMoneyDatacenterError,
        fetch_datacenter,
    )

    calls = {"n": 0}

    class _Busy:
        def get(self, url):
            calls["n"] += 1
            resp = MagicMock()
            resp.raise_for_status.return_value = None
            resp.json.return_value = {"success": False, "message": "服务器繁忙", "code": 9701}
            return resp

    with pytest.raises(EastMoneyDatacenterError, match="throttling, not a schema break"):
        fetch_datacenter(_Busy(), "RPT_X", "a", max_retries=3, retry_backoff_seconds=0)
    assert calls["n"] == 3, "a busy server must be retried, not failed on contact"


def test_a_real_schema_break_still_fails_immediately():
    import pytest

    from cn_market_lake.adapters.eastmoney.datacenter import (
        EastMoneyDatacenterError,
        fetch_datacenter,
    )

    calls = {"n": 0}

    class _Broken:
        def get(self, url):
            calls["n"] += 1
            resp = MagicMock()
            resp.raise_for_status.return_value = None
            resp.json.return_value = {"success": False, "message": "XX列不存在", "code": 9501}
            return resp

    with pytest.raises(EastMoneyDatacenterError, match="rejected schema"):
        fetch_datacenter(_Broken(), "RPT_X", "a", max_retries=3, retry_backoff_seconds=0)
    assert calls["n"] == 1, "a schema break will not fix itself; do not burn the budget"
