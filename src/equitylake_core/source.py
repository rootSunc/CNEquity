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

A source that allows it is asked several requests at a time
(:meth:`Fetcher.fetch_all`, ``Policy.concurrency``). Pacing then counts the
moments requests start, whichever thread sends them, so a source never sees
more than one start per ``min_interval``.
"""

from __future__ import annotations

import gzip
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from typing import Protocol

import polars as pl

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
    #: The coverage cells, ``(period, entity)``, this request answers: what is
    #: recorded as answered empty when its rows hold none.
    cells: tuple[tuple[str, str], ...] = ()

    def with_params(self, **changes: str) -> Request:
        merged = {**dict(self.params), **changes}
        return replace(self, params=tuple(merged.items()))


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
    #: Requests that may be in flight at once in :meth:`Fetcher.fetch_all`.
    concurrency: int = 1


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
        #: When the last request to each source started, or is booked to start.
        self._last: dict[str, float] = {}
        self._lock = threading.Lock()
        #: Requests sent and bytes received, by source.
        self.requests: dict[str, int] = {}
        self.bytes: dict[str, int] = {}

    def _start(self, request: Request, policy: Policy) -> None:
        """Book this request's start ``min_interval`` after the last one's, then wait for it.

        The budget is checked and the request counted in the same step, so
        threads sharing the fetcher never overspend it.
        """
        with self._lock:
            if policy.budget is not None and self.requests.get(policy.name, 0) >= policy.budget:
                raise BudgetExhausted(request, f"the budget of {policy.budget} requests is spent")
            now = self._clock()
            last = self._last.get(policy.name)
            at = now if last is None else max(now, last + policy.min_interval)
            self._last[policy.name] = at
            self.requests[policy.name] = self.requests.get(policy.name, 0) + 1
        if at > now:
            self._sleep(at - now)

    def fetch(self, request: Request) -> bytes:
        """The body of a successful answer to *request*; raises :class:`FetchError` otherwise."""
        policy = self.policies[request.source]
        url = request.url + ("?" + urllib.parse.urlencode(request.params) if request.params else "")
        reason, status = "no attempt made", None
        for attempt in range(policy.attempts):
            if attempt:
                self._sleep(min(60.0, 2.0**attempt))
            self._start(request, policy)
            try:
                response = self._transport(url, policy.headers, policy.timeout)
            except OSError as exc:  # no answer at all: refused, reset, timed out
                reason, status = f"{type(exc).__name__}: {exc}", None
                continue
            with self._lock:
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

    def fetch_all(self, requests: Sequence[Request]) -> list[bytes | FetchError]:
        """Each request's body, or the :class:`FetchError` it ended in, in the order asked.

        Up to the largest ``concurrency`` of the sources asked are in flight
        at once; each source's pacing and budget hold across them. Once a
        budget is spent, the requests still to start end in
        :class:`BudgetExhausted`.
        """
        workers = max((self.policies[r.source].concurrency for r in requests), default=1)

        def one(request: Request) -> bytes | FetchError:
            try:
                return self.fetch(request)
            except FetchError as exc:
                return exc

        if workers <= 1 or len(requests) <= 1:
            return [one(request) for request in requests]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            return list(pool.map(one, requests))

    def pages(
        self, request: Request, following: Callable[[Request, bytes], Request | None]
    ) -> Iterator[tuple[Request, bytes]]:
        """*request* and every page after it; *following* reads the next request off a body."""
        current: Request | None = request
        while current is not None:
            body = self.fetch(current)
            yield current, body
            current = following(current, body)


class Source(Protocol):
    """A market's half of fetching: what to ask for a set of gaps, and what an answer says.

    Both halves are pure: :meth:`plan` reads no network, :meth:`parse` only
    the body it is given, so a source is tested on saved bodies and replayed
    from the raw archive.
    """

    def plan(self, gaps: pl.DataFrame) -> list[Request]:
        """Requests that answer *gaps* (``period``, ``entity``: the planner's ``gap`` cells).

        Each request names the cells it answers (:attr:`Request.cells`); a
        cell no request names stays a gap.
        """
        ...

    def parse(self, request: Request, body: bytes) -> pl.DataFrame:
        """The rows *body* holds, the answer to *request*."""
        ...


@dataclass
class Gathered:
    """What a source answered for one set of gaps."""

    rows: pl.DataFrame
    #: Cells whose request was answered and held no rows: record them empty.
    empty: list[tuple[str, str]] = field(default_factory=list)
    #: Requests that got no usable answer; their cells stay gaps.
    failed: list[FetchError] = field(default_factory=list)


def gather(fetcher: Fetcher, source: Source, gaps: pl.DataFrame) -> Gathered:
    """Plan *gaps* with *source*, send the requests, and parse the answers.

    The one execution path for a gap whatever asked for it (a daily run, a
    backfill, a repair): the caller writes ``rows`` and records ``empty`` in
    the same snapshot, and leaves ``failed`` for the next plan to find.
    """
    requests = source.plan(gaps)
    frames: list[pl.DataFrame] = []
    out = Gathered(pl.DataFrame())
    for request, answer in zip(requests, fetcher.fetch_all(requests), strict=True):
        if isinstance(answer, FetchError):
            out.failed.append(answer)
            continue
        rows = source.parse(request, answer)
        if rows.is_empty():
            out.empty.extend(request.cells)
        else:
            frames.append(rows)
    if frames:
        out.rows = pl.concat(frames, how="diagonal_relaxed")
    return out
