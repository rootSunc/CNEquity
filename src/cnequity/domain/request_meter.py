"""Count the HTTP requests a pipeline step sends, for the run's metrics.

Adapters that page by hand (TDX) report their own `requests`. The EastMoney
client sends requests from inside whatever fetch function a step calls, which
has no metrics dict to hand, so a 400-request backfill was recorded as zero.
The engine opens a meter around each step; the client counts into it.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

from cnequity.domain.source_routes import source_family


class RequestMeter:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.requests = 0
        self.request_retries = 0
        self._routes: dict[str, set[str]] = {}

    def add(self, *, retry: bool = False) -> None:
        with self._lock:
            self.requests += 1
            if retry:
                self.request_retries += 1

    def record_route(self, source: str, event: str) -> None:
        with self._lock:
            self._routes.setdefault(source_family(source), set()).add(event)

    def routes(self) -> dict[str, list[str]]:
        with self._lock:
            return {name: sorted(events) for name, events in sorted(self._routes.items())}


_CURRENT: ContextVar[RequestMeter | None] = ContextVar("request_meter", default=None)


@contextmanager
def request_meter_scope() -> Iterator[RequestMeter]:
    meter = RequestMeter()
    token = _CURRENT.set(meter)
    try:
        yield meter
    finally:
        _CURRENT.reset(token)


def count_request(*, retry: bool = False) -> None:
    """Count one request sent inside the current step; a no-op outside one."""
    meter = _CURRENT.get()
    if meter is not None:
        meter.add(retry=retry)


def record_source_route(source: str, event: str) -> None:
    """Record a provider event within the current run's request scope."""
    meter = _CURRENT.get()
    if meter is not None:
        meter.record_route(source, event)
