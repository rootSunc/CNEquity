"""Fetching under a policy: pacing, retries, the budget, the count and the raw archive."""

from __future__ import annotations

import pytest

from equitylake_core.raw import RawArchive
from equitylake_core.source import (
    BudgetExhausted,
    Fetcher,
    FetchError,
    Policy,
    Request,
    Response,
)

REQUEST = Request(
    "vendor", "bars/2024-01-02", "https://example.test/bars", (("day", "2024-01-02"),)
)


class Wire:
    """Answers from a script; records what was sent and how long was slept."""

    def __init__(self, *answers):
        self.answers = list(answers)
        self.sent: list[tuple[str, dict]] = []
        self.slept: list[float] = []
        self.now = 0.0

    def transport(self, url, headers, timeout):
        self.sent.append((url, dict(headers)))
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds

    def fetcher(self, policy, archive=None):
        return Fetcher(
            {policy.name: policy},
            archive=archive,
            transport=self.transport,
            sleep=self.sleep,
            clock=lambda: self.now,
        )


def test_a_request_is_sent_with_its_parameters_and_the_policys_headers():
    wire = Wire(Response(200, b"ok"))
    fetcher = wire.fetcher(Policy("vendor", headers={"Authorization": "Bearer secret"}))
    assert fetcher.fetch(REQUEST) == b"ok"
    assert wire.sent == [
        ("https://example.test/bars?day=2024-01-02", {"Authorization": "Bearer secret"})
    ]
    assert fetcher.requests == {"vendor": 1} and fetcher.bytes == {"vendor": 2}


def test_requests_to_one_source_keep_their_distance():
    wire = Wire(Response(200, b"a"), Response(200, b"b"))
    fetcher = wire.fetcher(Policy("vendor", min_interval=13.0))
    fetcher.fetch(REQUEST)
    wire.now += 4.0
    fetcher.fetch(REQUEST)
    assert wire.slept == [9.0]


def test_throttling_and_a_dropped_connection_are_asked_again():
    wire = Wire(Response(429, b"slow down"), ConnectionResetError("reset"), Response(200, b"ok"))
    fetcher = wire.fetcher(Policy("vendor"))
    assert fetcher.fetch(REQUEST) == b"ok"
    assert wire.slept == [2.0, 4.0]
    assert fetcher.requests == {"vendor": 3}


def test_a_refusal_is_not_asked_again_and_says_what_it_was():
    wire = Wire(Response(403, b"no"))
    with pytest.raises(FetchError, match="vendor bars/2024-01-02: HTTP 403") as caught:
        wire.fetcher(Policy("vendor")).fetch(REQUEST)
    assert caught.value.status == 403 and len(wire.sent) == 1


def test_attempts_run_out():
    wire = Wire(*[Response(503, b"")] * 2)
    with pytest.raises(FetchError, match="HTTP 503"):
        wire.fetcher(Policy("vendor", attempts=2)).fetch(REQUEST)
    assert len(wire.sent) == 2


def test_a_spent_budget_sends_nothing_more():
    wire = Wire(Response(200, b"a"), Response(200, b"b"))
    fetcher = wire.fetcher(Policy("vendor", budget=1))
    fetcher.fetch(REQUEST)
    with pytest.raises(BudgetExhausted, match="budget of 1"):
        fetcher.fetch(REQUEST)
    assert len(wire.sent) == 1


def test_pages_follow_what_each_body_says_comes_next():
    wire = Wire(Response(200, b"next=2"), Response(200, b"next="))

    def following(request, body):
        token = body.decode().partition("=")[2]
        if not token:
            return None
        return Request(request.source, f"{REQUEST.key}#{token}", request.url).with_params(
            page=token
        )

    pages = list(wire.fetcher(Policy("vendor")).pages(REQUEST, following))
    assert [(request.key, body) for request, body in pages] == [
        ("bars/2024-01-02", b"next=2"),
        ("bars/2024-01-02#2", b"next="),
    ]
    assert wire.sent[1][0] == "https://example.test/bars?page=2"


def test_every_answer_is_archived_without_its_credentials(tmp_path):
    archive = RawArchive(tmp_path / "raw")
    wire = Wire(Response(500, b"oops"), Response(200, b'{"rows": 1}'))
    fetcher = wire.fetcher(Policy("vendor", headers={"Authorization": "Bearer secret"}), archive)
    fetcher.fetch(REQUEST)
    entries = list(archive.entries("vendor"))
    assert [(entry["key"], entry["status"], entry["bytes"]) for entry in entries] == [
        ("bars/2024-01-02", 500, 4),
        ("bars/2024-01-02", 200, 11),
    ]
    assert entries[1]["params"] == {"day": "2024-01-02"}
    assert archive.body("vendor", entries[0]) == b"oops"
    assert archive.latest("vendor", "bars/2024-01-02") == b'{"rows": 1}'
    assert archive.latest("vendor", "other") is None
    assert list(archive.entries("nobody")) == []
    kept = b"".join(path.read_bytes() for path in (tmp_path / "raw").rglob("*") if path.is_file())
    assert b"secret" not in kept


def test_a_request_can_keep_its_answer_out_of_the_archive(tmp_path):
    archive = RawArchive(tmp_path / "raw")
    wire = Wire(Response(200, b"large"), Response(200, b"small"))
    fetcher = wire.fetcher(Policy("vendor"), archive)
    assert (
        fetcher.fetch(Request("vendor", "big", "https://example.test/big", archive=False))
        == b"large"
    )
    kept = Request("vendor", "small", "https://example.test/small", archive=False).with_params(
        a="1"
    )
    assert kept.archive is False
    assert fetcher.fetch(Request("vendor", "small", "https://example.test/small")) == b"small"
    assert [entry["key"] for entry in archive.entries("vendor")] == ["small"]
    assert fetcher.requests == {"vendor": 2}


def test_several_requests_are_in_flight_but_their_starts_keep_their_distance():
    import threading
    import time

    starts: list[float] = []
    lock = threading.Lock()

    def slow(url, headers, timeout):
        with lock:
            starts.append(time.monotonic())
        time.sleep(0.2)
        return Response(404 if url.endswith("/7") else 200, url.encode())

    fetcher = Fetcher(
        {"vendor": Policy("vendor", min_interval=0.05, concurrency=4, attempts=1)}, transport=slow
    )
    requests = [Request("vendor", str(i), f"https://example.test/{i}") for i in range(12)]
    began = time.monotonic()
    answers = fetcher.fetch_all(requests)
    took = time.monotonic() - began
    assert [a for i, a in enumerate(answers) if i != 7] == [
        f"https://example.test/{i}".encode() for i in range(12) if i != 7
    ]
    assert isinstance(answers[7], FetchError) and answers[7].status == 404
    # Starts are booked min_interval apart and none begins before its slot, so
    # the k-th is never sooner than k intervals after the first. A thread that
    # wakes a few ms late narrows one measured gap (42 ms seen) without the
    # rate ever running ahead; the allowance is for the first start's wake-up.
    starts.sort()
    assert all(at - starts[0] >= k * 0.05 - 0.01 for k, at in enumerate(starts))
    # One at a time this is 12 x 0.25 s; four in flight finish in well under half.
    assert took < 1.2
    assert fetcher.requests == {"vendor": 12}


def test_a_budget_holds_across_threads():
    fetcher = Fetcher(
        {"vendor": Policy("vendor", concurrency=4, budget=5)},
        transport=lambda url, headers, timeout: Response(200, b"x"),
    )
    answers = fetcher.fetch_all(
        [Request("vendor", str(i), "https://example.test") for i in range(9)]
    )
    assert sum(1 for a in answers if a == b"x") == 5
    assert sum(1 for a in answers if isinstance(a, BudgetExhausted)) == 4
    assert fetcher.requests == {"vendor": 5}


def test_a_source_plans_the_gaps_and_gather_sorts_the_answers():
    import json

    import polars as pl

    from equitylake_core.source import gather

    class Daily:
        def plan(self, gaps):
            return [
                Request("vendor", period, f"https://example.test/{period}", cells=((period, ""),))
                for period in gaps["period"].to_list()
            ]

        def parse(self, request, body):
            return pl.DataFrame(json.loads(body), schema={"day": pl.String, "v": pl.Int64})

    bodies = {
        "https://example.test/2026-11-18": Response(200, b'[{"day": "2026-11-18", "v": 1}]'),
        "https://example.test/2026-11-19": Response(200, b"[]"),
        "https://example.test/2026-11-20": Response(503, b""),
    }
    fetcher = Fetcher(
        {"vendor": Policy("vendor", attempts=1)}, transport=lambda url, h, t: bodies[url]
    )
    gaps = pl.DataFrame({"period": list(d[-10:] for d in bodies), "entity": [""] * 3})
    got = gather(fetcher, Daily(), gaps)
    assert got.rows["day"].to_list() == ["2026-11-18"]
    assert got.empty == [("2026-11-19", "")]
    assert [failure.request.key for failure in got.failed] == ["2026-11-20"]
