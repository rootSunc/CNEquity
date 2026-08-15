import time
from concurrent.futures import ProcessPoolExecutor

from cn_market_lake.domain.rate_limit import RateLimiter, wait_source

INTERVAL = 0.05

# What the wait must clear. Not `INTERVAL`, because two clocks disagree by a
# little: the limiter computes its sleep from `time.time()` (it has to — the
# deadline is shared across processes through a JSON file, and a monotonic
# clock is not comparable between them), while the assertion measures with
# `perf_counter`. Windows `time.sleep` also returns marginally early.
#
# Measured on CI: 0.03961 against a 0.04 floor — 0.4ms short, on a limiter
# whose job is pacing to ~10 req/s. The margin is generous enough that the
# flake cannot recur and still an order of magnitude tighter than the failure
# it guards against, which is the limiter not sleeping at all.
MIN_OBSERVED = INTERVAL * 0.6


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
