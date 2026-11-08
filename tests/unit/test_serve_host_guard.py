"""Without a token the dashboard answers only to a loopback Host.

`cne serve` refuses a non-loopback ``--host`` without ``--token``, so a request
that names another host was routed here by DNS rebinding: a page on that name
is same-origin with the dashboard and would read the lake through any route the
check skipped. Ops and storage had their own guard; the read routes did not.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from cnequity.serve.app import create_app

READ_ROUTES = [
    "/",
    "/source-health",
    "/api/health",
    "/api/tiers",
    "/api/datasets",
    "/api/datasets/daily_bars",
    "/api/datasets/daily_bars/rows",
    "/api/read/daily_bars",
    "/api/quality",
    "/api/runs",
    "/api/stream/runs/some-run",
    "/api/ops",
    "/api/storage",
    "/api/openapi.json",
]


@pytest.mark.parametrize("path", READ_ROUTES)
def test_a_rebound_host_reads_nothing_without_a_token(config, path):
    client = TestClient(create_app(config), base_url="http://evil.example:8787")

    response = client.get(path)

    assert response.status_code == 403
    assert "localhost" in response.json()["detail"]


@pytest.mark.parametrize("host", ["127.0.0.1:8787", "localhost:8787", "[::1]:8787"])
def test_loopback_hosts_still_read_without_a_token(config, host):
    client = TestClient(create_app(config), base_url="http://127.0.0.1:8787")

    assert client.get("/api/tiers", headers={"Host": host}).status_code == 200


def test_a_token_still_admits_a_remote_host(config):
    client = TestClient(create_app(config, token="s3cret"), base_url="http://lake.lan:8787")

    assert client.get("/api/tiers").status_code == 401
    assert client.get("/api/tiers?token=s3cret").status_code == 200
