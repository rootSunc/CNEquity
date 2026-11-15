import os
import sys
import tempfile
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
    the start method is `spawn` on Windows/macOS and `forkserver` on Linux 3.14+,
    so the child is a fresh interpreter that re-imports everything and inherits
    no patching. `sitecustomize` runs before any user code in that child, which
    is the one hook early enough to matter.

    Set here rather than in the fixture because a child inherits the
    environment as it was when it started, and pools outlive individual tests.
    """
    guard_dir = str(Path(__file__).parent / "_subprocess_guard")
    existing = os.environ.get("PYTHONPATH", "")
    parts = [p for p in existing.split(os.pathsep) if p]
    if guard_dir not in parts:
        os.environ["PYTHONPATH"] = os.pathsep.join([guard_dir, *parts])
    os.environ["CNE_TEST_NO_NETWORK"] = "1"
    # `cne init` tests must not hold the developer's machine awake.
    os.environ["CNE_ALLOW_SLEEP"] = "1"


_arm_subprocess_network_guard()

# Output a test asserts on must not depend on the machine's locale: a zh_CN
# developer box would otherwise render `用法：` where CI renders `Usage:`. Set
# for the whole process, so CLI subprocesses inherit it too; a test about
# language selection sets its own and calls `cnequity.i18n.reset()`.
os.environ["CNE_LANG"] = "en"

# A command without `--config` falls back to the per-user config, and `cne init`
# writes one there. Point that location at an empty directory for the whole
# process (subprocesses inherit it) so no test reads or writes the developer's
# real lake config, and drop a CNE_CONFIG the developer's shell may export.
_USER_CONFIG_HOME = tempfile.mkdtemp(prefix="cne-test-config-")
os.environ["XDG_CONFIG_HOME"] = _USER_CONFIG_HOME
os.environ["APPDATA"] = _USER_CONFIG_HOME
os.environ.pop("CNE_CONFIG", None)
# Credentials in the developer's shell would outrank the config under test
# (environment first, see `config.loader._credential`).
for _credential_env in ("TUSHARE_TOKEN", "HITHINK_FINANCE_API_KEY"):
    os.environ.pop(_credential_env, None)


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
def _no_outbound_network(request, monkeypatch):
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

    def _is_local(address) -> bool:
        # `cne serve` and `cne mcp` are exercised over a real loopback socket by
        # their own tests. Those are not the network this guards — nothing
        # outside the machine is reached — so they are let through by address.
        # AF_UNIX is the same class of traffic: Linux 3.14's forkserver talks
        # to `/tmp/pymp-*/sock-*`, which is a filesystem path rather than
        # `(host, port)`. Treating that path as a hostname made every
        # ProcessPoolExecutor test fail on the interpreter CI added to close
        # an untested-claim gap.
        if not isinstance(address, tuple):
            return True
        host = address[0]
        return isinstance(host, str) and (host in ("localhost", "::1") or host.startswith("127."))

    def _refuse(self, address, *, _real):
        if _is_local(address):
            local.add(self.fileno())
            return _real(self, address)
        raise AssertionError(
            f"{request.node.nodeid} opened an outbound connection to {address}. "
            "Stub the adapter, or mark the test `@pytest.mark.network`."
        )

    socket.socket.connect = lambda self, address: _refuse(self, address, _real=connect)
    socket.socket.connect_ex = lambda self, address: _refuse(self, address, _real=connect_ex)
    # Native libcurl does not call Python's socket.connect. Keep AkShare's
    # curl_cffi transport inside the same offline boundary as Python clients.
    try:
        from curl_cffi.requests import Session
    except ImportError:
        pass
    else:
        from urllib.parse import urlparse

        real_request = Session.request

        def guarded_request(self, method, url, *args, **kwargs):
            parsed = urlparse(url)
            if _is_local((parsed.hostname, parsed.port)):
                return real_request(self, method, url, *args, **kwargs)
            raise AssertionError(
                f"{request.node.nodeid} attempted native outbound HTTP; stub the adapter"
            )

        monkeypatch.setattr(Session, "request", guarded_request)
    try:
        yield
    finally:
        socket.socket.connect = connect
        socket.socket.connect_ex = connect_ex


@pytest.fixture(autouse=True)
def _private_user_config_home(tmp_path_factory, monkeypatch):
    """A fresh per-user config directory for every test.

    The process-wide one above keeps collection-time code off the developer's
    real config; this one keeps a test that runs `cne init` or `config create`
    without `--config` from handing its config to every later test.
    """
    home = str(tmp_path_factory.mktemp("user-config"))
    monkeypatch.setenv("XDG_CONFIG_HOME", home)
    monkeypatch.setenv("APPDATA", home)


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


@pytest.fixture(autouse=True)
def _restore_logging_state():
    """Give every test the logging configuration the one before it found.

    CLI commands call ``basicConfig(force=True)`` and attach file handlers to
    the root logger. A test that did not undo that left a stderr handler
    behind, so a later CLI test saw an expected error's traceback in its
    output, but only when the two happened to run in that order.
    """
    import logging

    root = logging.getLogger()
    package = logging.getLogger("cnequity")
    saved = (list(root.handlers), root.level, package.level, logging.root.manager.disable)
    yield
    for handler in root.handlers[:]:
        if handler not in saved[0]:
            root.removeHandler(handler)
            try:
                handler.close()
            except Exception:  # noqa: BLE001 - a closed stream must not fail the suite
                pass
    root.handlers = saved[0]
    root.setLevel(saved[1])
    package.setLevel(saved[2])
    logging.disable(saved[3])


@pytest.fixture
def chinese_output(monkeypatch):
    """Render CLI output in Chinese, for tests that assert on Chinese wording.

    Messages are English in the code and Chinese in the catalog, so these tests
    also prove the catalog still says what the CLI used to say.
    """
    from cnequity import i18n

    monkeypatch.setenv(i18n.ENV_VAR, "zh_CN")
    i18n.reset()
    yield
    i18n.reset()
