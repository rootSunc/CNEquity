"""Fetching from a source, in three stages a market fills in two of.

1. **Plan.** The market turns what is missing into :class:`Request` objects:
   plain data, no network.
2. **Fetch.** The :class:`Fetcher` here sends them. Pacing, retries, the
   daily budget, the accounting and the raw archive are the same for every
   source and are done once, here.
3. **Parse.** The market turns a response body into rows: a pure function,
   testable on a saved body and replayable from the archive.

A source that pages says what the next request is from the body it got
(:meth:`Fetcher.pages`); the core does not know any vendor's paging.
"""

from __future__ import annotations

import gzip
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field

from equitylake_core.raw import RawArchive

#: Statuses worth asking again for: throttling and the server's own failures.
RETRYABLE = frozenset({408, 425, 429, 500, 502, 503, 504})


class FetchError(RuntimeError):
    """A request did not get a usable answer; nothing was parsed from it."""

    def __init__(self, request: Request, reason: str, status: int | None = None):
        super().__init__(f"{request.source} {request.key}: {reason}")
        self.request = request
        self.status = status


class BudgetExhausted(FetchError):
    """The source's request budget for this run is spent; the rest stays a gap."""


@dataclass(frozen=True)
class Request:
    #: Names the :class:`Policy` to send it under.
    source: str
    #: What the request is for, unique within the source: the archive's lookup key.
    key: str
    url: str
    params: tuple[tuple[str, str], ...] = ()
    #: Whether the answer goes into the raw archive. Leave it out only for an
    #: answer that is large, asked for often, and can be asked for again with
    #: the same result (an official publisher's current view of filed documents).
    archive: bool = True

    def with_params(self, **changes: str) -> Request:
        merged = {**dict(self.params), **changes}
        return Request(self.source, self.key, self.url, tuple(merged.items()), self.archive)


@dataclass(frozen=True)
class Response:
    status: int
    body: bytes


@dataclass(frozen=True)
class Policy:
    """How one source may be asked."""

    name: str
    #: Seconds between two requests to the source.
    min_interval: float = 0.0
    #: Sent with every request and never archived: credentials go here.
    headers: Mapping[str, str] = field(default_factory=dict)
    #: Attempts per request, the first included.
    attempts: int = 4
    #: Requests this fetcher may send to the source in its lifetime; ``None`` is no limit.
    budget: int | None = None
    timeout: float = 60.0


Transport = Callable[[str, Mapping[str, str], float], Response]


def http_transport(url: str, headers: Mapping[str, str], timeout: float) -> Response:
    """One GET with the standard library; a status the server sent is a response, not an error."""
    request = urllib.request.Request(url, headers={"Accept-Encoding": "gzip", **headers})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as reply:
            body, status = reply.read(), reply.status
            encoding = reply.headers.get("Content-Encoding")
    except urllib.error.HTTPError as exc:
        body, status, encoding = exc.read(), exc.code, exc.headers.get("Content-Encoding")
    if encoding == "gzip":
        body = gzip.decompress(body)
    return Response(status, body)


class Fetcher:
    """Sends requests under their source's policy and keeps count."""

    def __init__(
        self,
        policies: Mapping[str, Policy],
        *,
        archive: RawArchive | None = None,
        transport: Transport = http_transport,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.policies = dict(policies)
        self.archive = archive
        self._transport, self._sleep, self._clock = transport, sleep, clock
        self._last: dict[str, float] = {}
        #: Requests sent and bytes received, by source.
        self.requests: dict[str, int] = {}
        self.bytes: dict[str, int] = {}

    def _pace(self, policy: Policy) -> None:
        last = self._last.get(policy.name)
        if last is not None:
            wait = policy.min_interval - (self._clock() - last)
            if wait > 0:
                self._sleep(wait)

    def fetch(self, request: Request) -> bytes:
        """The body of a successful answer to *request*; raises :class:`FetchError` otherwise."""
        policy = self.policies[request.source]
        url = request.url + ("?" + urllib.parse.urlencode(request.params) if request.params else "")
        reason, status = "no attempt made", None
        for attempt in range(policy.attempts):
            if policy.budget is not None and self.requests.get(policy.name, 0) >= policy.budget:
                raise BudgetExhausted(request, f"the budget of {policy.budget} requests is spent")
            if attempt:
                self._sleep(min(60.0, 2.0**attempt))
            self._pace(policy)
            self.requests[policy.name] = self.requests.get(policy.name, 0) + 1
            try:
                response = self._transport(url, policy.headers, policy.timeout)
            except OSError as exc:  # no answer at all: refused, reset, timed out
                reason, status = f"{type(exc).__name__}: {exc}", None
                continue
            finally:
                self._last[policy.name] = self._clock()
            self.bytes[policy.name] = self.bytes.get(policy.name, 0) + len(response.body)
            if self.archive is not None and request.archive:
                self.archive.put(
                    request.source,
                    request.key,
                    request.url,
                    dict(request.params),
                    response.status,
                    response.body,
                )
            if response.status == 200:
                return response.body
            reason, status = f"HTTP {response.status}", response.status
            if response.status not in RETRYABLE:
                break
        raise FetchError(request, reason, status)

    def pages(
        self, request: Request, following: Callable[[Request, bytes], Request | None]
    ) -> Iterator[tuple[Request, bytes]]:
        """*request* and every page after it; *following* reads the next request off a body."""
        current: Request | None = request
        while current is not None:
            body = self.fetch(current)
            yield current, body
            current = following(current, body)
