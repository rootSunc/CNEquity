"""The no-network guard, including the half that lives outside this process.

`-m 'not network'` skips the tests that declare a dependency on the network.
Nothing stopped one that reaches it by accident, and eight did — seven still
passed, because the adapter fell back, so the only symptom was time. The guard
turns that into one named failure.

`worker_pool` runs on `ProcessPoolExecutor`, whose start method is `spawn`: a
fresh interpreter that inherits no patching. Half a guard is worse than none,
because the parent looks clean while the work happens elsewhere.
"""

from __future__ import annotations

import socket
import subprocess
import sys
import threading
from concurrent.futures import ProcessPoolExecutor

import pytest


def _child_reaches_out() -> str:
    """Run in a pool worker: report whether the guard reached this process."""
    import socket as child_socket

    try:
        child_socket.create_connection(("push2.eastmoney.com", 443), timeout=5)
        return "connected"
    except AssertionError:
        return "blocked"
    except Exception as exc:  # noqa: BLE001 — any other failure is not the answer
        return f"inconclusive:{type(exc).__name__}"


def _child_reaches_loopback(port: int) -> str:
    import socket as child_socket

    conn = child_socket.socket()
    try:
        conn.connect(("127.0.0.1", port))
        return "ok"
    except AssertionError:
        return "blocked"
    finally:
        conn.close()


def _loopback_listener() -> tuple[socket.socket, int]:
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    threading.Thread(target=server.accept, daemon=True).start()
    return server, server.getsockname()[1]


def test_this_process_cannot_reach_an_external_host():
    with pytest.raises(AssertionError, match="outbound connection"):
        socket.create_connection(("push2.eastmoney.com", 443), timeout=5)


def test_this_process_can_still_use_loopback():
    """`cne serve` and `cne mcp` are exercised over a real local socket."""
    server, port = _loopback_listener()
    try:
        conn = socket.socket()
        conn.connect(("127.0.0.1", port))
        conn.close()
    finally:
        server.close()


def test_a_pool_worker_cannot_reach_an_external_host():
    with ProcessPoolExecutor(max_workers=1) as pool:
        assert pool.submit(_child_reaches_out).result(timeout=30) == "blocked"


def test_a_pool_worker_can_still_use_loopback():
    server, port = _loopback_listener()
    try:
        with ProcessPoolExecutor(max_workers=1) as pool:
            assert pool.submit(_child_reaches_loopback, port).result(timeout=30) == "ok"
    finally:
        server.close()


def test_a_plain_python_subprocess_is_covered_too():
    """`sitecustomize` runs before user code, so `-c` scripts are guarded."""
    script = (
        "import socket\n"
        "try:\n"
        "    socket.create_connection(('push2.eastmoney.com', 443), timeout=5)\n"
        "    print('connected')\n"
        "except AssertionError:\n"
        "    print('blocked')\n"
    )
    done = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
    )
    assert done.stdout.strip() == "blocked", done


@pytest.mark.network
def test_the_marker_means_the_same_thing_in_a_subprocess():
    """Otherwise the marker would let the test out and trap its pool workers."""
    with ProcessPoolExecutor(max_workers=1) as pool:
        assert pool.submit(_child_reaches_out).result(timeout=30) != "blocked"
