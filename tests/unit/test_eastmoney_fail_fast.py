"""EastMoney transport errors must not burn full retry budgets."""

from __future__ import annotations

import httpx
import pytest

from cn_market_lake.adapters.eastmoney.clist import _fetch_clist_page
from cn_market_lake.adapters.eastmoney.datacenter import (
    EastMoneyDatacenterError,
    fetch_datacenter,
)
from cn_market_lake.adapters.eastmoney.em_auth import is_transport_fail_fast


def test_is_transport_fail_fast():
    assert is_transport_fail_fast(httpx.TimeoutException("t"))
    assert is_transport_fail_fast(httpx.ConnectError("c"))
    assert is_transport_fail_fast(httpx.RemoteProtocolError("r"))
    # A dead proxy is the overseas equivalent of a dead route: with
    # [sources.eastmoney].proxy set, every retry goes back through it.
    assert is_transport_fail_fast(httpx.ProxyError("p"))
    assert not is_transport_fail_fast(RuntimeError("other"))


def test_datacenter_does_not_retry_timeout():
    calls = {"n": 0}

    class FakeClient:
        def get(self, url):
            calls["n"] += 1
            raise httpx.TimeoutException("slow")

    with pytest.raises(EastMoneyDatacenterError, match="failed after"):
        fetch_datacenter(
            FakeClient(),
            "RPT_TEST",
            "SECUCODE",
            max_retries=3,
            retry_backoff_seconds=0,
        )
    assert calls["n"] == 1


def test_clist_does_not_retry_connect_error():
    calls = {"n": 0}

    class FakeClient:
        def get(self, url):
            calls["n"] += 1
            raise httpx.ConnectError("down")

    with pytest.raises(RuntimeError, match="clist page"):
        _fetch_clist_page(
            FakeClient(),
            host="https://push2.eastmoney.com",
            fields="f12",
            fs="m:1",
            page=1,
            page_size=10,
            max_retries=3,
            retry_backoff_seconds=0,
        )
    assert calls["n"] == 1
