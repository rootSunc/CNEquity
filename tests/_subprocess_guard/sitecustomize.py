"""Carry the no-network guard into subprocesses.

`tests/conftest.py` patches `socket.socket.connect` in the *pytest* process.
`worker_pool` uses `ProcessPoolExecutor`, and on every platform this project
supports the start method is `spawn` — a fresh interpreter that re-imports
everything and inherits none of that patching. A child was free to reach the
network, which is the half of the guard nobody would notice was missing: the
parent's tests look fast and clean while the work actually happens elsewhere.

Python runs `sitecustomize` at interpreter start, before any user code, so
putting the directory that holds this file on `PYTHONPATH` extends the guard to
every descendant — pool workers and `subprocess.run([sys.executable, ...])`
alike. It is inert unless `CNE_TEST_NO_NETWORK` is set, so it does nothing to a
normal `cne` run that happens to share the environment.
"""

from __future__ import annotations

import os

if os.environ.get("CNE_TEST_NO_NETWORK") == "1":
    import socket

    _connect = socket.socket.connect
    _connect_ex = socket.socket.connect_ex

    def _loopback(address) -> bool:
        host = address[0] if isinstance(address, tuple) else address
        return isinstance(host, str) and (host in ("localhost", "::1") or host.startswith("127."))

    def _guard(real):
        def _wrapped(self, address):
            if _loopback(address):
                return real(self, address)
            raise AssertionError(
                f"pid {os.getpid()} opened an outbound connection to {address} from a "
                "subprocess of the test run. Stub the adapter, or mark the test "
                "`@pytest.mark.network`."
            )

        return _wrapped

    socket.socket.connect = _guard(_connect)
    socket.socket.connect_ex = _guard(_connect_ex)
