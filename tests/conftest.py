import os
import sys

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
