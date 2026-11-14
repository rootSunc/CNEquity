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
