"""Run-scoped caches shared through Config are initialised atomically."""

from __future__ import annotations

import threading
from types import SimpleNamespace

from cnequity.run_scope import ensure_run_scoped


def _ensure(config, scope):
    ensure_run_scoped(
        config,
        "_derivative_http_run",
        scope,
        {"_derivative_refreshed_requests": set, "_derivative_blocked_hosts": set},
    )


def test_same_scope_keeps_the_shared_objects():
    config = SimpleNamespace()
    _ensure(config, "run-1")
    config._derivative_blocked_hosts.add("www.shfe.com.cn")
    blocked = config._derivative_blocked_hosts

    _ensure(config, "run-1")

    assert config._derivative_blocked_hosts is blocked
    assert config._derivative_blocked_hosts == {"www.shfe.com.cn"}


def test_new_scope_starts_fresh():
    config = SimpleNamespace()
    _ensure(config, "run-1")
    config._derivative_blocked_hosts.add("www.shfe.com.cn")

    _ensure(config, "run-2")

    assert config._derivative_blocked_hosts == set()
    assert config._derivative_http_run == "run-2"


def test_parallel_steps_never_reset_each_others_record():
    """futures_bars and option_bars enter at the same time and both record.

    With the old check-then-act, a thread that passed the stale-marker check
    late could replace the set another thread had already written to.
    """
    for _ in range(200):
        config = SimpleNamespace(_derivative_http_run="previous-run")
        threads = 8
        barrier = threading.Barrier(threads)

        def step(index: int, config=config, barrier=barrier) -> None:
            barrier.wait()
            _ensure(config, "run-1")
            config._derivative_blocked_hosts.add(f"host-{index}")

        workers = [threading.Thread(target=step, args=(i,)) for i in range(threads)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()

        assert config._derivative_blocked_hosts == {f"host-{i}" for i in range(threads)}
