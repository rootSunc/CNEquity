import json
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import contextmanager
from threading import Barrier, Event, Lock

import pytest

from cnequity import file_replace
from cnequity.config import Config
from cnequity.domain.rate_limit import (
    RateLimiter,
    RateLimitSpec,
    SourceConcurrencyLimiter,
    source_request_slot_spec,
    wait_source,
)
from cnequity.file_lock import LockUnavailable

INTERVAL = 0.1


def test_minimal_config_keeps_source_pacing_and_explicit_override(tmp_path):
    from cnequity.adapters.throttle import SourceRateLimiters
    from cnequity.config import load_config
    from cnequity.diagnostics.source_limits import effective_source_policy

    cfg = Config(data_root=tmp_path / "lake")
    assert cfg.tdx_rate_limit_spec().min_interval == 0.1
    limiters = SourceRateLimiters(cfg)
    for source, interval in {
        "cninfo": 1.0,
        "eastmoney_push2": 4.0,
        "ths_pages": 3.0,
        "baostock": 1.0,
    }.items():
        assert limiters._limiters[source].min_interval == interval
        assert (
            effective_source_policy(cfg, source)["pacing_seconds"][source]["configured_seconds"]
            == interval
        )
    for source, family, cap in (
        ("cninfo", "cninfo", 2),
        ("eastmoney_push2", "eastmoney_push2", 1),
        ("ths_pages", "ths", 1),
        ("baostock", "baostock", 1),
    ):
        assert limiters._concurrency[family].limit == cap
        assert effective_source_policy(cfg, source)["configured_max_concurrency"] == cap

    explicit = Config(data_root=tmp_path / "other", source_intervals={"cninfo": 0.0})
    assert SourceRateLimiters(explicit)._limiters["cninfo"].min_interval == 0.0
    minimal_toml = tmp_path / "minimal.toml"
    minimal_toml.write_text('[data]\nroot = "./minimal-lake"\n', encoding="utf-8")
    loaded = load_config(minimal_toml)
    assert loaded.tdx_rate_limit_spec().min_interval == 0.1
    assert SourceRateLimiters(loaded)._limiters["cninfo"].min_interval == 1.0
    assert not cfg.data_root.exists()


@pytest.mark.parametrize(("alias_interval", "expected"), [(0.0, 0.1), (0.25, 0.25)])
def test_tdx_wire_and_request_lanes_use_stricter_interval(tmp_path, alias_interval, expected):
    from cnequity.adapters.throttle import SourceRateLimiters
    from cnequity.diagnostics.source_limits import effective_source_policy

    cfg = Config(
        data_root=tmp_path / "lake",
        tdx_min_interval_ms=100,
        tdx_lock_timeout_sec=7.0,
        source_intervals={"tdx_protocol": alias_interval},
    )
    assert cfg.tdx_rate_limit_spec().min_interval == expected
    request_lane = SourceRateLimiters(cfg)._limiters["tdx_protocol"]
    assert request_lane.min_interval == expected
    assert request_lane.lock_timeout == 7.0
    assert (
        effective_source_policy(cfg, "tdx_protocol")["pacing_seconds"]["tdx_protocol"][
            "configured_seconds"
        ]
        == expected
    )


def test_meter_counts_admitted_scope_once_but_not_local_rejection(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    with cfg.source_request("bse"):
        with cfg.source_request("bse"):
            pass
    meter = cfg.rate_limit_root / "meter-bse.json"
    assert json.loads(meter.read_text())["aliases"] == {"bse": 1}

    cfg.sources["bse"] = False
    with pytest.raises(RuntimeError, match="disabled"):
        with cfg.source_request("bse"):
            pass
    assert json.loads(meter.read_text())["total"] == 1
    from cnequity.diagnostics.source_limits import build_source_limits

    assert build_source_limits(cfg)["sources"]["bse"]["metered_attempts_today"] == {
        "count": 1,
        "aliases": {"bse": 1},
    }


def test_futures_host_attempts_are_visible_in_offline_limits(tmp_path):
    from cnequity.diagnostics.source_limits import build_source_limits

    cfg = Config(data_root=tmp_path, source_intervals={"futures_exchange": 0})
    lane = "futures_exchange_www.shfe.com.cn"
    with cfg.source_request(lane):
        pass
    row = build_source_limits(cfg)["sources"][lane]
    assert row["metered_attempts_today"]["aliases"] == {lane: 1}
    cfg.sources["futures_exchange"] = False
    assert build_source_limits(cfg)["sources"][lane]["enabled"] is False


def test_low_level_wire_spec_is_in_the_same_meter(tmp_path):
    spec = RateLimitSpec(
        state_dir=str(tmp_path),
        source="tdx_protocol",
        min_interval=0,
        concurrency_limit=1,
    )
    with source_request_slot_spec(spec):
        pass
    assert json.loads((tmp_path / "meter-tdx_protocol.json").read_text())["total"] == 1


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


def test_shared_egress_keeps_stricter_interval_from_another_config(tmp_path, monkeypatch):
    now = [100.0]
    slept = []
    monkeypatch.setattr("cnequity.domain.rate_limit.time.time", lambda: now[0])

    def sleep(seconds):
        slept.append(seconds)
        now[0] += seconds

    monkeypatch.setattr("cnequity.domain.rate_limit.time.sleep", sleep)
    RateLimiter("sina", 2.0, tmp_path).wait()
    RateLimiter("sina", 0.0, tmp_path).wait()
    assert slept == [2.0]
    state = json.loads((tmp_path / "sina.json").read_text())
    assert state["min_interval"] == 2.0


def test_shared_egress_keeps_stricter_concurrency_cap(tmp_path):
    narrow = SourceConcurrencyLimiter("sina", 1, tmp_path)
    wide = SourceConcurrencyLimiter("sina", 8, tmp_path)
    token = narrow.acquire()
    with pytest.raises(TimeoutError):
        wide.acquire(timeout=0)
    narrow.release(token)
    token = wide.acquire(timeout=0)
    try:
        state = json.loads((tmp_path / "concurrency-sina.json").read_text())
        assert state["limit"] == 1
    finally:
        wide.release(token)


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


def test_source_concurrency_retries_transient_windows_replace_denial(tmp_path, monkeypatch):
    """A destination briefly held open elsewhere must not fail the ledger write."""
    import cnequity.domain.rate_limit as rate_limit

    real_replace = rate_limit.os.replace
    denials = {"left": 2}

    def _flaky_replace(src, dst):
        if denials["left"]:
            denials["left"] -= 1
            raise PermissionError(5, "Access is denied")
        real_replace(src, dst)

    monkeypatch.setattr(file_replace, "_REPLACE_BACKOFF_SEC", 0.0)
    monkeypatch.setattr(rate_limit.os, "replace", _flaky_replace)
    limiter = SourceConcurrencyLimiter("sina", 1, tmp_path / "rate_limits")

    with limiter.slot(timeout=0.5):
        pass

    assert denials["left"] == 0
    state_dir = tmp_path / "rate_limits"
    assert json.loads((state_dir / "concurrency-sina.json").read_text("utf-8"))["leases"] == []
    assert not list(state_dir.glob("*.tmp"))


def test_source_concurrency_release_denial_does_not_abort_or_leak_slot(tmp_path, monkeypatch):
    """A ledger stuck past the retries logs, and the stale lease is pruned next acquire."""
    import cnequity.domain.rate_limit as rate_limit

    real_replace = rate_limit.os.replace
    deny = {"on": False}

    def _replace(src, dst):
        if deny["on"]:
            raise PermissionError(5, "Access is denied")
        real_replace(src, dst)

    monkeypatch.setattr(file_replace, "_REPLACE_BACKOFF_SEC", 0.0)
    monkeypatch.setattr(rate_limit.os, "replace", _replace)
    limiter = SourceConcurrencyLimiter("sina", 1, tmp_path / "rate_limits")

    with limiter.slot(timeout=0.5):
        deny["on"] = True
    deny["on"] = False

    # Same live thread: without pruning, the leftover lease fills the cap of one.
    with limiter.slot(timeout=0.5):
        pass
    state = json.loads(
        (tmp_path / "rate_limits" / "concurrency-sina.json").read_text(encoding="utf-8")
    )
    assert state["leases"] == []


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


def test_source_aliases_share_one_vendor_request_spacing(tmp_path):
    cfg = Config(
        data_root=tmp_path / "data",
        source_intervals={"ths_pages": 0.01, "ths_bonus": 0.02},
    )
    with cfg.source_request("ths_pages"):
        pass
    path = cfg.rate_limit_root / "ths.json"
    first = json.loads(path.read_text())["next_allowed_at"]
    with cfg.source_request("ths_bonus"):
        pass
    second = json.loads(path.read_text())["next_allowed_at"]
    assert second - first >= 0.9
    assert json.loads(path.read_text())["min_interval"] == 1.0
    from cnequity.diagnostics.source_limits import effective_source_policy

    report = effective_source_policy(cfg, "ths_pages")
    assert report["pacing_seconds"]["ths"]["effective_seconds"] == 1.0


@pytest.mark.parametrize(
    ("family", "slow_alias"),
    [("ths", "ths_pages"), ("sina", "sina_bars")],
)
def test_explicit_family_spacing_does_not_inherit_slow_endpoint(tmp_path, family, slow_alias):
    cfg = Config(
        data_root=tmp_path / family,
        source_intervals={family: 0.001, slow_alias: 0.003},
    )

    with cfg.source_request(family):
        pass
    with cfg.source_request(slow_alias):
        pass

    family_state = json.loads((cfg.rate_limit_root / f"{family}.json").read_text())
    alias_state = json.loads((cfg.rate_limit_root / f"{slow_alias}.json").read_text())
    assert family_state["min_interval"] == 0.001
    assert alias_state["min_interval"] == 0.003

    from cnequity.diagnostics.source_limits import effective_source_policy

    report = effective_source_policy(cfg, slow_alias)["pacing_seconds"]
    assert report[family]["configured_seconds"] == 0.001
    assert report[slow_alias]["configured_seconds"] == 0.003


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


def _write_foreign_lease(state_dir, pid: int) -> None:
    state_dir.mkdir(parents=True)
    (state_dir / "concurrency-tdx_protocol.json").write_text(
        json.dumps(
            {
                "version": 1,
                "limit": 1,
                "leases": [
                    {
                        "token": "foreign-worker",
                        "pid": pid,
                        "thread_id": 1,
                        "created_at": time.time() - 1,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def test_dead_process_lease_is_reclaimed(tmp_path):
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait(timeout=10)
    pid = proc.pid
    del proc  # Release the Windows process handle, as a prior run would have.
    state_dir = tmp_path / "rate_limits"
    _write_foreign_lease(state_dir, pid)

    limiter = SourceConcurrencyLimiter("tdx_protocol", 1, state_dir)
    with limiter.slot(timeout=0.5):
        pass


@pytest.mark.skipif(sys.platform != "win32", reason="Windows process liveness regression")
def test_exited_process_with_still_active_exit_code_is_reclaimed(tmp_path):
    # Keep the process handle open so the liveness check must inspect its
    # signaled state. An exit code of 259 must not mean "still running".
    proc = subprocess.Popen([sys.executable, "-c", "import sys; sys.exit(259)"])
    proc.wait(timeout=10)
    state_dir = tmp_path / "rate_limits"
    _write_foreign_lease(state_dir, proc.pid)

    limiter = SourceConcurrencyLimiter("tdx_protocol", 1, state_dir)
    with limiter.slot(timeout=0.5):
        pass


@pytest.mark.skipif(sys.platform != "win32", reason="Windows process liveness regression")
def test_live_process_lease_is_not_reclaimed_or_terminated(tmp_path):
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        state_dir = tmp_path / "rate_limits"
        _write_foreign_lease(state_dir, proc.pid)
        limiter = SourceConcurrencyLimiter("tdx_protocol", 1, state_dir)

        with pytest.raises(TimeoutError, match="tdx_protocol concurrency slot"):
            with limiter.slot(timeout=0.2):
                pass
        assert proc.poll() is None
    finally:
        if proc.poll() is None:
            proc.terminate()
        proc.wait(timeout=10)


def test_ledger_write_sweeps_temp_files_abandoned_by_killed_writers(tmp_path, monkeypatch):
    import os

    import cnequity.domain.rate_limit as rate_limit

    monkeypatch.setattr(rate_limit, "_SWEPT_TMP_DIRS", set())
    state_dir = tmp_path / "rate_limits"
    state_dir.mkdir()
    abandoned = state_dir / ".concurrency-sina-dead.tmp"
    in_flight = state_dir / ".concurrency-sina-live.tmp"
    abandoned.write_text("{}", encoding="utf-8")
    in_flight.write_text("{}", encoding="utf-8")
    hour_ago = time.time() - rate_limit._STALE_TMP_SECONDS - 60
    os.utime(abandoned, (hour_ago, hour_ago))

    with SourceConcurrencyLimiter("sina", 1, state_dir).slot(timeout=0.5):
        pass

    assert not abandoned.exists()
    assert in_flight.exists()
