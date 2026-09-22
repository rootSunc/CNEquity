import json
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import contextmanager
from threading import Barrier, Event, Lock

import pytest

from cnequity.config import Config
from cnequity.domain.rate_limit import RateLimiter, SourceConcurrencyLimiter, wait_source
from cnequity.file_lock import LockUnavailable

INTERVAL = 0.1

# What the wait must clear. Not `INTERVAL`, because two clocks disagree by a
# little: the limiter computes its sleep from `time.time()` (it has to — the
# deadline is shared across processes through a JSON file, and a monotonic
# clock is not comparable between them), while the assertion measures with
# `perf_counter`. Windows `time.sleep` also returns early.
#
# Measured on CI: 0.0239 against a 0.05 interval (48%). The floor is below
# that ratio and still several times a no-op wait (~8ms), which is the
# failure this test actually guards.
MIN_OBSERVED = INTERVAL * 0.3


def test_rate_limiter_enforces_minimum_interval(tmp_path, monkeypatch):
    """The second call must wait out the interval the first one reserved.

    Asserts on the sleep the limiter asks for rather than on measured wall
    time. The deadline is computed from `time.time()` — it has to be, it is
    shared across processes through a JSON file — so a clock step on a CI
    runner retires it early and a `perf_counter` measurement then reports no
    wait while the limiter is behaving correctly. That failed a release build
    at 0.98 ms against a 30 ms floor.
    """
    now = [1_000.0]
    slept: list[float] = []

    def fake_sleep(seconds: float) -> None:
        slept.append(seconds)
        now[0] += seconds

    monkeypatch.setattr("cnequity.domain.rate_limit.time.time", lambda: now[0])
    monkeypatch.setattr("cnequity.domain.rate_limit.time.sleep", fake_sleep)

    limiter = RateLimiter("test", INTERVAL, tmp_path / "rate_limits")
    limiter.wait()
    limiter.wait()

    # The first call owns the current slot and sleeps not at all.
    assert slept == [pytest.approx(INTERVAL)]


def test_rate_limiter_defer_persists_a_shared_cooldown(tmp_path, monkeypatch):
    state_dir = tmp_path / "rate_limits"
    limiter = RateLimiter("sina_bars", 1.0, state_dir)
    monkeypatch.setattr("cnequity.domain.rate_limit.time.time", lambda: 100.0)

    limiter.defer(30.0)

    state = json.loads((state_dir / "sina_bars.json").read_text(encoding="utf-8"))
    assert state["next_allowed_at"] == 130.0


def _worker_wait(state_dir: str) -> float:
    t0 = time.perf_counter()
    wait_source(state_dir, "test", INTERVAL)
    return time.perf_counter() - t0


def test_rate_limiter_serializes_cross_process_requests(tmp_path):
    state_dir = tmp_path / "rate_limits"
    with ProcessPoolExecutor(max_workers=2) as pool:
        durations = list(pool.map(_worker_wait, [str(state_dir), str(state_dir)]))
    # One of the two must have waited: whichever lost the lock race sees the
    # other's timestamp already written.
    assert max(durations) >= MIN_OBSERVED


def test_corrupt_rate_state_is_replaced_atomically(tmp_path):
    state_dir = tmp_path / "rate_limits"
    state_dir.mkdir()
    state_path = state_dir / "test.json"
    state_path.write_text("{truncated", encoding="utf-8")

    RateLimiter("test", 0.01, state_dir).wait()

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["last"] > 0
    assert not list(state_dir.glob(".*.tmp"))


@pytest.mark.parametrize("payload", [{"last": "nan"}, {"next_allowed_at": "inf"}])
def test_non_finite_rate_state_does_not_poison_future_slots(tmp_path, payload):
    state_dir = tmp_path / "rate_limits"
    state_dir.mkdir()
    (state_dir / "test.json").write_text(json.dumps(payload), encoding="utf-8")

    RateLimiter("test", 0.01, state_dir).wait()

    state = json.loads((state_dir / "test.json").read_text(encoding="utf-8"))
    assert state["last"] > 0
    assert state["next_allowed_at"] > state["last"]


def test_rate_limiter_propagates_lock_timeout_instead_of_bypassing(monkeypatch, tmp_path):
    seen = {}

    @contextmanager
    def busy_lock(path, **kwargs):
        seen.update(kwargs)
        raise LockUnavailable("busy")
        yield  # pragma: no cover

    monkeypatch.setattr("cnequity.domain.rate_limit.exclusive_lock", busy_lock)

    with pytest.raises(LockUnavailable, match="busy"):
        RateLimiter("test", INTERVAL, tmp_path / "rate_limits").wait()

    assert seen["timeout"] == 15.0


def test_wait_spec_propagates_custom_lock_timeout(tmp_path, monkeypatch):
    seen = {}

    def fake_wait_source(state_dir, source, min_interval, lock_timeout):
        seen.update(
            state_dir=state_dir,
            source=source,
            min_interval=min_interval,
            lock_timeout=lock_timeout,
        )

    monkeypatch.setattr("cnequity.domain.rate_limit.wait_source", fake_wait_source)
    from cnequity.domain.rate_limit import RateLimitSpec, wait_spec

    wait_spec(RateLimitSpec(str(tmp_path), "tdx_protocol", 0.1, lock_timeout=3.5))

    assert seen["lock_timeout"] == 3.5


def test_source_concurrency_aggregates_slow_calls_and_releases_on_success(tmp_path):
    """A source cap applies to overlapping calls sharing one state directory."""
    limiter = SourceConcurrencyLimiter("eastmoney", 2, tmp_path / "rate_limits")
    active = 0
    peak = 0
    lock = Lock()
    first_pair = Barrier(2)

    def _slow_call(index: int) -> None:
        nonlocal active, peak
        with limiter.slot():
            with lock:
                active += 1
                peak = max(peak, active)
            if index < 2:
                first_pair.wait(timeout=2.0)
            time.sleep(0.04)
            with lock:
                active -= 1

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(_slow_call, range(4)))

    assert peak == 2
    state = json.loads(
        (tmp_path / "rate_limits" / "concurrency-eastmoney.json").read_text(encoding="utf-8")
    )
    assert state["leases"] == []


def test_source_concurrency_releases_slot_when_request_raises(tmp_path):
    limiter = SourceConcurrencyLimiter("cninfo", 1, tmp_path / "rate_limits")

    with pytest.raises(RuntimeError, match="fixture failure"):
        with limiter.slot():
            raise RuntimeError("fixture failure")

    # A leaked lease would make this timeout rather than entering.
    with limiter.slot(timeout=0.2):
        pass


def test_config_source_request_caps_concurrent_calls_across_call_sites(tmp_path):
    cfg = Config(
        data_root=tmp_path / "data",
        workers=4,
        source_intervals={"eastmoney": 0.0},
        source_concurrency={"eastmoney": 2},
    )
    active = 0
    peak = 0
    lock = Lock()
    holder_lock = Lock()
    holder_selected = False
    holder_entered = Event()
    release_holder = Event()

    def _request(_index: int) -> None:
        nonlocal active, peak, holder_selected
        with cfg.source_request("eastmoney"):
            with lock:
                active += 1
                peak = max(peak, active)
            with holder_lock:
                is_holder = not holder_selected
                holder_selected = True
            if is_holder:
                holder_entered.set()
                release_holder.wait(timeout=1.0)
            else:
                time.sleep(0.03)
            with lock:
                active -= 1

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(_request, index) for index in range(4)]
        assert holder_entered.wait(timeout=1.0)
        time.sleep(0.03)
        release_holder.set()
        for future in futures:
            future.result()

    assert 1 <= peak <= 2


def test_the_inflight_cap_holds_at_the_actual_call_boundary(tmp_path):
    """Two requests may overlap under a cap of two, and never three.

    A counter, not a clock: the previous version of this also timed the gap
    between the threads waking up, and a thread descheduled between the
    limiter releasing it and reading the clock records late. That measured a
    gap shorter than the limiter had enforced and failed here twice while the
    cap itself held — the spacing is asserted on the limiter's own reservation
    in the test below instead.
    """
    interval = 0.05
    hold = 0.2  # > interval, so two requests genuinely overlap in the cap
    cfg = Config(
        data_root=tmp_path / "data",
        workers=4,
        source_intervals={"eastmoney": interval},
        source_concurrency={"eastmoney": 2},
    )
    active = 0
    peak = 0
    entered = 0
    lock = Lock()

    def _request(_index: int) -> None:
        nonlocal active, peak, entered
        with cfg.source_request("eastmoney"):
            with lock:
                active += 1
                entered += 1
                peak = max(peak, active)
            try:
                time.sleep(hold)
            finally:
                with lock:
                    active -= 1

    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(_request, range(3)))

    assert entered == 3
    assert peak <= 2


def test_each_request_reserves_the_next_slot_one_interval_later(tmp_path):
    """Spacing is what the limiter *granted*, which scheduling cannot erode.

    Each acquisition takes `slot = max(now, next_allowed_at)` under the shared
    lock and writes `slot + min_interval` back, so the reservation is exact
    arithmetic recorded on disk. Asserting on it says the same thing as timing
    the wake-ups without inheriting their noise: a busy runner can deliver a
    thread late, never early, so the reserved slots cannot come out short.
    """
    interval = 0.2
    cfg = Config(
        data_root=tmp_path / "data",
        workers=4,
        source_intervals={"eastmoney": interval},
    )
    state_path = cfg.meta_root / "rate_limits" / "eastmoney.json"

    reserved: list[float] = []
    for _ in range(3):
        with cfg.source_request("eastmoney"):
            pass
        reserved.append(
            float(json.loads(state_path.read_text(encoding="utf-8"))["next_allowed_at"])
        )

    assert len(reserved) == 3
    # `>=` rather than `==`: a call that arrives after its own reservation has
    # already passed gets the clock instead, which can only push the next slot
    # further out.
    assert reserved[1] - reserved[0] >= interval
    assert reserved[2] - reserved[1] >= interval


def test_source_aliases_share_the_narrowest_configured_vendor_cap(tmp_path):
    cfg = Config(
        data_root=tmp_path / "data",
        workers=8,
        source_concurrency={"ths": 3, "ths_pages": 1, "ths_bonus": 2},
    )

    with cfg.source_slot("ths"):
        state = json.loads(
            (cfg.meta_root / "rate_limits" / "concurrency-ths.json").read_text(encoding="utf-8")
        )
        assert state["limit"] == 1


def test_sina_endpoint_aliases_share_one_vendor_cap(tmp_path):
    cfg = Config(
        data_root=tmp_path / "data",
        workers=8,
        source_concurrency={"sina": 4, "sina_bars": 2},
    )

    with cfg.source_slot("sina_bars"):
        state = json.loads(
            (cfg.meta_root / "rate_limits" / "concurrency-sina.json").read_text(encoding="utf-8")
        )
        assert state["limit"] == 2


def _hold_source_slot(args: tuple[str, float]) -> tuple[float, float]:
    state_dir, delay = args
    limiter = SourceConcurrencyLimiter("tdx_protocol", 1, state_dir)
    started = time.perf_counter()
    with limiter.slot():
        entered = time.perf_counter()
        time.sleep(delay)
    return started, entered


def test_source_concurrency_is_cross_process_for_slow_requests(tmp_path):
    state_dir = str(tmp_path / "rate_limits")
    delay = 0.08
    wall_started = time.perf_counter()
    with ProcessPoolExecutor(max_workers=2) as pool:
        entries = list(pool.map(_hold_source_slot, [(state_dir, delay)] * 2))
    elapsed = time.perf_counter() - wall_started

    # The second process must wait for the first lease. Wall time already
    # requires the two holds not to overlap. The per-process wait uses the
    # same 30% floor as MIN_OBSERVED: Windows CI measured 0.0527 against
    # delay * 0.8 = 0.064, which is still several times a no-op acquire.
    assert elapsed >= delay * 1.6
    assert max(entered - started for started, entered in entries) >= delay * 0.3


def test_dead_process_lease_is_reclaimed(tmp_path):
    """A lease held by a terminated process must not wedge the source forever.

    Regression: on Windows ``os.kill(pid, 0)`` raises plain ``OSError``
    (WinError 87) for a dead pid — not ``ProcessLookupError`` — so the
    liveness check read that as "cannot inspect, assume alive" and every
    stale lease stayed alive forever, permanently exhausting the
    tdx_protocol cap and wedging every later run's first TDX request.
    """
    import subprocess
    import sys

    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait(timeout=30)
    dead = proc.pid

    state_dir = tmp_path / "rate_limits"
    ledger = {
        "version": 1,
        "limit": 1,
        "leases": [
            {
                "token": "abandoned-lease",
                "pid": dead,
                "thread_id": 1,
                "created_at": time.time() - 60,
            }
        ],
    }
    state_dir.mkdir(parents=True)
    (state_dir / "concurrency-tdx_protocol.json").write_text(json.dumps(ledger), encoding="utf-8")

    limiter = SourceConcurrencyLimiter("tdx_protocol", 1, state_dir)
    started = time.perf_counter()
    with limiter.slot(timeout=10.0):
        # Reclaiming is a metadata edit; if it did not happen instantly the
        # slot was blocked behind the dead owner.
        assert time.perf_counter() - started < 5.0
