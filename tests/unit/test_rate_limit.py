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


def test_rate_limiter_enforces_minimum_interval(tmp_path):
    state_dir = tmp_path / "rate_limits"
    limiter = RateLimiter("test", INTERVAL, state_dir)
    limiter.wait()
    t0 = time.perf_counter()
    limiter.wait()
    assert time.perf_counter() - t0 >= MIN_OBSERVED


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


def test_config_source_request_combines_qps_spacing_with_inflight_cap(tmp_path):
    """Pacing and the shared lease both apply at the actual call boundary."""
    cfg = Config(
        data_root=tmp_path / "data",
        workers=4,
        source_intervals={"eastmoney": 0.04},
        source_concurrency={"eastmoney": 2},
    )
    active = 0
    peak = 0
    starts: list[float] = []
    lock = Lock()

    def _request(_index: int) -> None:
        nonlocal active, peak
        with cfg.source_request("eastmoney"):
            with lock:
                starts.append(time.perf_counter())
                active += 1
                peak = max(peak, active)
            try:
                time.sleep(0.12)
            finally:
                with lock:
                    active -= 1

    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(_request, range(3)))

    assert peak <= 2
    assert len(starts) == 3
    ordered = sorted(starts)
    assert ordered[1] - ordered[0] >= 0.04 * 0.6
    assert ordered[2] - ordered[1] >= 0.04 * 0.6


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
