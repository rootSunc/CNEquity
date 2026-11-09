"""One retry policy for every Sina sweep (`domain.rate_limit.sina_retry`)."""

from __future__ import annotations

from contextlib import contextmanager

import httpx
import pytest

from cnequity.domain import rate_limit
from cnequity.domain.rate_limit import (
    SINA_FETCH_ATTEMPTS,
    SINA_RATE_LIMIT_COOLDOWN_SECONDS,
    sina_retry,
)


class _Config:
    retry_backoff_seconds = 5

    def __init__(self):
        self.deferred: list[tuple[str, float]] = []

    def defer_source(self, source, seconds):
        self.deferred.append((source, seconds))


def _failing(*statuses: int, then="ok"):
    calls = []

    def operation():
        calls.append(1)
        if len(calls) <= len(statuses):
            status = statuses[len(calls) - 1]
            request = httpx.Request("GET", "https://example.test")
            response = httpx.Response(status, request=request)
            raise httpx.HTTPStatusError(str(status), request=request, response=response)
        return then

    return operation, calls


@pytest.fixture
def leases(monkeypatch):
    held: list[str] = []

    @contextmanager
    def fake_request(config, lane):
        held.append(lane)
        yield

    sleeps: list[float] = []
    monkeypatch.setattr(rate_limit, "source_request", fake_request)
    monkeypatch.setattr(rate_limit.time, "sleep", sleeps.append)
    return held, sleeps


def test_a_rate_limit_cools_the_vendor_and_a_5xx_backs_off(leases):
    held, sleeps = leases
    config = _Config()
    operation, calls = _failing(456, 503)

    assert sina_retry(config, "sina", operation, describe="test") == "ok"
    assert len(calls) == 3
    assert config.deferred == [("sina", SINA_RATE_LIMIT_COOLDOWN_SECONDS)]
    # 503 on the second try waits 5 s x 2; a rate limit never sleeps in-thread.
    assert sleeps == [10.0]
    assert held == ["sina"] * 3


def test_a_5xx_is_never_retried_straight_away(leases):
    """USD/CNH and commodity bars used to retry a 5xx with no wait at all."""
    _held, sleeps = leases
    operation, calls = _failing(502, 500)

    assert sina_retry(_Config(), "sina", operation, describe="test") == "ok"
    assert sleeps == [5.0, 10.0]


def test_other_errors_and_the_last_try_raise(leases):
    operation, calls = _failing(404)
    with pytest.raises(httpx.HTTPStatusError):
        sina_retry(_Config(), "sina", operation, describe="test")
    assert len(calls) == 1

    operation, calls = _failing(*([503] * SINA_FETCH_ATTEMPTS))
    with pytest.raises(httpx.HTTPStatusError):
        sina_retry(_Config(), "sina", operation, describe="test")
    assert len(calls) == SINA_FETCH_ATTEMPTS


def test_hooks_can_stop_and_an_adapter_can_hold_its_own_lease(leases):
    held, _sleeps = leases

    class Stop(Exception):
        pass

    def stop(_status):
        raise Stop

    operation, calls = _failing(456)
    config = _Config()
    with pytest.raises(Stop):
        sina_retry(config, "sina_bars", operation, describe="t", on_rate_limit=stop)
    assert len(calls) == 1 and config.deferred == []

    def closed():
        raise Stop

    operation, calls = _failing(503)
    with pytest.raises(Stop):
        sina_retry(_Config(), "sina_bars", operation, describe="t", before_retry=closed)
    assert len(calls) == 1

    held.clear()
    operation, _calls = _failing()
    sina_retry(_Config(), "sina_bars", operation, describe="t", request_managed=True)
    assert held == []
