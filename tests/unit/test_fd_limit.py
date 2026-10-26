from __future__ import annotations

import sys

import pytest

resource = pytest.importorskip("resource")

from cnequity.fd_limit import _DARWIN_OPEN_MAX, raise_open_file_limit  # noqa: E402


def _fake_limits(monkeypatch, soft, hard, refuse_above=None):
    calls = []

    def setrlimit(kind, value):
        if refuse_above is not None and value[0] > refuse_above:
            raise ValueError("not allowed")
        calls.append(value)

    monkeypatch.setattr(resource, "getrlimit", lambda kind: (soft, hard))
    monkeypatch.setattr(resource, "setrlimit", setrlimit)
    return calls


def test_raises_soft_limit_and_keeps_hard(monkeypatch):
    calls = _fake_limits(monkeypatch, 256, 100_000)

    assert raise_open_file_limit(65536) == 65536
    assert calls == [(65536, 100_000)]


def test_caps_at_hard_limit(monkeypatch):
    calls = _fake_limits(monkeypatch, 256, 4096)

    assert raise_open_file_limit(65536) == 4096
    assert calls == [(4096, 4096)]


def test_leaves_a_sufficient_limit_alone(monkeypatch):
    calls = _fake_limits(monkeypatch, 1_048_576, resource.RLIM_INFINITY)

    assert raise_open_file_limit(65536) == 1_048_576
    assert calls == []


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS OPEN_MAX fallback")
def test_falls_back_to_open_max_on_macos(monkeypatch):
    calls = _fake_limits(monkeypatch, 256, resource.RLIM_INFINITY, refuse_above=_DARWIN_OPEN_MAX)

    assert raise_open_file_limit(65536) == _DARWIN_OPEN_MAX
    assert calls == [(_DARWIN_OPEN_MAX, resource.RLIM_INFINITY)]


def test_reports_current_limit_when_refused(monkeypatch):
    _fake_limits(monkeypatch, 256, 100_000, refuse_above=0)

    assert raise_open_file_limit(65536) == 256
