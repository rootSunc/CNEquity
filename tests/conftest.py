import os
import sys
from pathlib import Path

import pytest

# Captured at import, before any test runs. `register_step` writes to a
# process-global registry, and `test_deps` registers throwaway steps to build a
# cycle. It does clean up, so the registry is not actually leaky — but an
# assertion about the *shipped* step count should not depend on that staying
# true, nor on a run being interrupted between the registration and its
# `finally`. Compare against this snapshot rather than the live registry.
import cnequity.steps  # noqa: E402, F401 — importing is what registers them
from cnequity.config import load_config
from cnequity.config.bootstrap import path_for_toml
from cnequity.orchestrator.registry import STEP_REGISTRY as _LIVE_STEP_REGISTRY  # noqa: E402

PRISTINE_STEP_NAMES = frozenset(_LIVE_STEP_REGISTRY)


def _arm_subprocess_network_guard() -> None:
    """Put `tests/_subprocess_guard` on `PYTHONPATH` for every child process.

    The in-process fixture below cannot reach a `ProcessPoolExecutor` worker:
    the start method is `spawn`, so the child is a fresh interpreter that
    re-imports everything and inherits no patching. `sitecustomize` runs before
    any user code in that child, which is the one hook early enough to matter.

    Set here rather than in the fixture because a child inherits the
    environment as it was when it started, and pools outlive individual tests.
    """
    guard_dir = str(Path(__file__).parent / "_subprocess_guard")
    existing = os.environ.get("PYTHONPATH", "")
    parts = [p for p in existing.split(os.pathsep) if p]
    if guard_dir not in parts:
        os.environ["PYTHONPATH"] = os.pathsep.join([guard_dir, *parts])
    os.environ["CNE_TEST_NO_NETWORK"] = "1"


_arm_subprocess_network_guard()


def pytest_configure(config):
    """Keep Windows ProcessPoolExecutor teardown from aborting the suite.

    A spawned worker that exits on Windows can inject ``CTRL_C_EVENT`` into
    the parent's console group. pytest then raises ``KeyboardInterrupt``
    mid-session — CI has seen this right after the rate-limiter process
    tests, with every earlier test already green. Ignoring the console
    signal is the usual workaround (CPython issue 33725).
    """
    if sys.platform != "win32":
        return
    import ctypes

    ctypes.windll.kernel32.SetConsoleCtrlHandler(None, True)


@pytest.hookimpl(hookwrapper=True, trylast=True)
def pytest_sessionfinish(session, exitstatus):
    """Keep a green Windows suite from exiting 1 in multiprocessing atexit.

    After every test passed, CI still reported exit code 1: leftover
    ``ProcessPoolExecutor`` workers run atexit handlers that replace pytest's
    status. Wait for the terminal summary, then skip those handlers.
    """
    yield
    if sys.platform == "win32" and os.environ.get("CI") == "true" and int(exitstatus or 0) == 0:
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(0)


@pytest.fixture(autouse=True)
def _no_outbound_network(request):
    """Refuse an outbound connection from a test that never declared one.

    `-m 'not network'` only skips the tests that *say* they need the network.
    Nothing stopped one that reaches it by accident, and eight did: two asked
    baostock, four asked EastMoney, two asked a sentiment endpoint. Seven of
    them still passed — the adapter fell back when the connection failed — so
    the only symptom was time. `test_exchange_trading_status.py` took 19.2s
    against 0.09s with the socket closed, which is most of why a full run
    drifted between 129s and 169s and twice timed out.

    The eighth was worse: `test_a_window_spent_entirely_halted_...` passed only
    *because* the query succeeded, so its conclusion came partly from a live
    vendor. On a machine without that route it failed, and failed obscurely.

    Marked tests are let through — in their subprocesses too, by clearing the
    variable `sitecustomize` reads, or the marker would mean one thing in the
    test and the opposite in a pool worker it starts. Everything else fails at
    the connect, naming itself and the address, which turns "slow and
    occasionally red" into one obvious line.
    """
    if request.node.get_closest_marker("network"):
        previous = os.environ.pop("CNE_TEST_NO_NETWORK", None)
        try:
            yield
        finally:
            if previous is not None:
                os.environ["CNE_TEST_NO_NETWORK"] = previous
        return

    import socket

    connect = socket.socket.connect
    connect_ex = socket.socket.connect_ex
    local: set[int] = set()

    def _is_loopback(address) -> bool:
        # `cne serve` and `cne mcp` are exercised over a real loopback socket by
        # their own tests. Those are not the network this guards — nothing
        # outside the machine is reached — so they are let through by address.
        host = address[0] if isinstance(address, tuple) else address
        return isinstance(host, str) and (host in ("localhost", "::1") or host.startswith("127."))

    def _refuse(self, address, *, _real):
        if _is_loopback(address):
            local.add(self.fileno())
            return _real(self, address)
        raise AssertionError(
            f"{request.node.nodeid} opened an outbound connection to {address}. "
            "Stub the adapter, or mark the test `@pytest.mark.network`."
        )

    socket.socket.connect = lambda self, address: _refuse(self, address, _real=connect)
    socket.socket.connect_ex = lambda self, address: _refuse(self, address, _real=connect_ex)
    try:
        yield
    finally:
        socket.socket.connect = connect
        socket.socket.connect_ex = connect_ex


@pytest.fixture(autouse=True)
def _no_leaked_heartbeat():
    """Stop any heartbeat thread a test started before the next test runs.

    `start_heartbeat` spawns a daemon thread, which in a CLI process dies with
    the process and in a pytest process lives for thousands of tests after the
    one that armed it. Any test that CliRunner-invokes `cne init`, `cne run
    daily`, `cne run events` or `cne backfill` starts one.

    That leaked thread broke an unrelated test: `test_retry_hardening` fakes
    `time.sleep` to capture its arguments, and because `module.time` is the
    shared `time` module, the fake applied process-wide. The heartbeat's
    throttle became a no-op and the thread spun, appending tens of millions of
    entries to that test's list. It only failed under CPU contention — the
    window had to span the thread's wake-up — so it flaked rather than failed.
    """
    from cnequity.progress import stop_heartbeat

    yield
    stop_heartbeat()


@pytest.fixture
def config(tmp_path):
    """Minimal offline config wiring a daily Wave over mock adapters."""
    cfg_path = tmp_path / "test.toml"
    cfg_path.write_text(
        f"""
[data]
root = "{path_for_toml(tmp_path / "data")}"

[orchestrator]
workers = 1
batch_size = 2

[tdx_protocol]
allow_mock = true

[[job.daily.waves]]
name = "reference"
parallel = true
steps = ["instruments", "trading_calendar"]

[[job.daily.waves]]
name = "bars"
parallel = false
steps = ["daily_bars", "compact", "derive_adj_factors", "audit"]

[job.init.phases]
names = ["phase1_reference"]
""",
        encoding="utf-8",
    )
    return load_config(cfg_path)
