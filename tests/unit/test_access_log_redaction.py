"""Access tokens carried in a URL must not reach uvicorn's access log."""

from __future__ import annotations

import logging

import pytest

from cnequity.cli import consume_cmds
from cnequity.cli.consume_cmds import _AccessLogRedactor, redact_url_secrets


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/mcp/s3cr3t-token", "/mcp/<redacted>"),
        ("/mcp/s3cr3t-token?x=1", "/mcp/<redacted>?x=1"),
        ("/?token=abc123", "/?token=<redacted>"),
        ("/mcp?token=abc123&x=1", "/mcp?token=<redacted>&x=1"),
        ("/api/runs?page=2&TOKEN=abc", "/api/runs?page=2&TOKEN=<redacted>"),
        ("/mcp", "/mcp"),
        ("/api/health", "/api/health"),
        ("/api/runs?tokenize=1", "/api/runs?tokenize=1"),
    ],
)
def test_redact_url_secrets(path, expected):
    assert redact_url_secrets(path) == expected


def _access_record(path: str) -> logging.LogRecord:
    # The shape uvicorn's h11/httptools protocols log with.
    return logging.LogRecord(
        "uvicorn.access",
        logging.INFO,
        __file__,
        1,
        '%s - "%s %s HTTP/%s" %d',
        ("127.0.0.1:5000", "POST", path, "1.1", 200),
        None,
    )


def test_filter_rewrites_the_logged_path():
    record = _access_record("/mcp/s3cr3t-token")
    assert _AccessLogRedactor().filter(record) is True
    assert "s3cr3t-token" not in record.getMessage()
    assert "/mcp/<redacted>" in record.getMessage()


def test_run_server_installs_the_filter_before_uvicorn_starts(monkeypatch):
    import uvicorn

    access = logging.getLogger("uvicorn.access")
    monkeypatch.setattr(access, "filters", [])
    seen: list[bool] = []

    def fake_run(*args, **kwargs):
        seen.append(any(isinstance(f, _AccessLogRedactor) for f in access.filters))

    monkeypatch.setattr(uvicorn, "run", fake_run)
    consume_cmds._run_server(object(), "127.0.0.1", 0, None)
    consume_cmds._run_server(object(), "127.0.0.1", 0, None)
    assert seen == [True, True]
    # Installing twice must not stack filters.
    assert sum(isinstance(f, _AccessLogRedactor) for f in access.filters) == 1
