import pytest

from cnequity.adapters.tdx_protocol import client as tdx
from cnequity.config import Config


def _install_fake_quotes(monkeypatch, *, factory=None, hosts=None):
    """Stub the quotes facade so these stay offline.

    The TDX wire client is vendored now, so there is no external package to
    fake — only our own factory needs intercepting.
    """
    hosts = hosts or [("9.9.9.9", 7709), ("8.8.8.8", 7709)]
    if factory is None:

        def factory(**kwargs):
            return object()

    from cnequity.adapters.tdx_protocol import hosts as hosts_mod
    from cnequity.adapters.tdx_protocol import quotes as quotes_mod

    monkeypatch.setattr(hosts_mod, "HQ_HOSTS", tuple(hosts))
    monkeypatch.setattr(quotes_mod.Quotes, "factory", staticmethod(factory))


def test_quotes_client_auto_binds_probed_server(monkeypatch):
    seen: dict[str, object] = {}

    def fake_factory(**kwargs):
        seen.update(kwargs)
        return object()

    # auto mode probes a reachable+functional server instead of the slow,
    # flaky bestip scan; stub the probe so the test stays offline.
    _install_fake_quotes(monkeypatch, factory=fake_factory)
    monkeypatch.setattr(
        tdx, "_pick_reachable_server", lambda config=None, timeout=10: ("1.2.3.4", 7709)
    )
    tdx.reset_tdx_server_cache()
    cfg = Config(data_root="/tmp/data")
    cfg.tdx_connect_timeout_sec = 42
    cfg.tdx_servers = "auto"

    tdx._quotes_client(cfg)
    tdx.reset_tdx_server_cache()

    assert seen["timeout"] == 42
    assert seen["server"] == ("1.2.3.4", 7709)
    assert "bestip" not in seen


def test_quotes_client_explicit_server(monkeypatch):
    seen: dict[str, object] = {}

    def fake_factory(**kwargs):
        seen.update(kwargs)
        return object()

    _install_fake_quotes(monkeypatch, factory=fake_factory)
    cfg = Config(data_root="/tmp/data")
    cfg.tdx_servers = "119.147.212.81:7709"
    cfg.tdx_connect_timeout_sec = 15

    tdx._quotes_client(cfg)

    assert seen["server"] == ("119.147.212.81", 7709)
    assert seen["timeout"] == 15
    assert "bestip" not in seen


def test_quotes_client_rejects_invalid_servers(monkeypatch):
    _install_fake_quotes(monkeypatch)
    cfg = Config(data_root="/tmp/data")
    cfg.tdx_servers = "not-a-server"
    with pytest.raises(tdx.TdxSourceError, match="invalid"):
        tdx._quotes_client(cfg)


def test_candidate_servers_prefers_config_pool(monkeypatch):
    _install_fake_quotes(monkeypatch)
    cfg = Config(data_root="/tmp/data")
    cfg.tdx_host_pool = ["1.1.1.1:7709", "2.2.2.2:7709"]
    candidates = tdx._candidate_servers(cfg)
    # configured pool comes first, in order
    assert candidates[0] == ("1.1.1.1", 7709)
    assert candidates[1] == ("2.2.2.2", 7709)
    # bundled hosts appended as fallback
    assert len(candidates) > 2


def test_pick_reachable_server_returns_first_functional(monkeypatch):
    _install_fake_quotes(monkeypatch)
    cfg = Config(data_root="/tmp/data")
    cfg.tdx_host_pool = ["9.9.9.9:7709", "8.8.8.8:7709"]

    # only 8.8.8.8 serves data
    def fake_probe(host, port, timeout):
        return host == "8.8.8.8"

    monkeypatch.setattr(tdx, "_probe", fake_probe)
    monkeypatch.setattr(
        tdx, "_candidate_servers", lambda config: [("9.9.9.9", 7709), ("8.8.8.8", 7709)]
    )
    assert tdx._pick_reachable_server(cfg) == ("8.8.8.8", 7709)


def test_pick_reachable_server_raises_when_none_live(monkeypatch):
    _install_fake_quotes(monkeypatch)
    monkeypatch.setattr(tdx, "_probe", lambda h, p, t: False)
    monkeypatch.setattr(tdx, "_candidate_servers", lambda config: [("9.9.9.9", 7709)])
    with pytest.raises(tdx.TdxSourceError, match="no TDX server responded"):
        tdx._pick_reachable_server(None)


def test_serves_data_true_when_bars_return(monkeypatch):
    class _Client:
        def bars(self, *args, **kwargs):
            return [{"close": 1.0}]

        def close(self):
            self.closed = True

    monkeypatch.setattr(
        "cnequity.adapters.tdx_protocol.quotes.Quotes.factory",
        staticmethod(lambda **kwargs: _Client()),
    )
    assert tdx._serves_data("1.2.3.4", 7709, timeout=5) is True


def test_serves_data_false_on_empty_or_error(monkeypatch):
    class _Empty:
        def bars(self, *args, **kwargs):
            return []

        def close(self):
            pass

    monkeypatch.setattr(
        "cnequity.adapters.tdx_protocol.quotes.Quotes.factory",
        staticmethod(lambda **kwargs: _Empty()),
    )
    assert tdx._serves_data("1.2.3.4", 7709, timeout=5) is False

    def _boom(**kwargs):
        raise OSError("down")

    monkeypatch.setattr(
        "cnequity.adapters.tdx_protocol.quotes.Quotes.factory",
        staticmethod(_boom),
    )
    assert tdx._serves_data("1.2.3.4", 7709, timeout=5) is False


def test_bundled_hosts_start_with_verified_live_set():
    from cnequity.adapters.tdx_protocol.hosts import HQ_HOSTS, VERIFIED_HOSTS

    assert HQ_HOSTS[: len(VERIFIED_HOSTS)] == VERIFIED_HOSTS
    assert len(HQ_HOSTS) > len(VERIFIED_HOSTS)


def test_verified_hosts_are_always_within_the_probed_window():
    """Discovery must not depend on a lucky shuffle.

    Only the first ``_TDX_MAX_CANDIDATES`` entries are probed. When the bundled
    list was shuffled whole, a config without its own host pool — what
    ``cne demo`` writes — filled every probed slot by random draw, so the
    verified hosts were missed outright on a fifth of runs.
    """
    from cnequity.adapters.tdx_protocol.hosts import VERIFIED_HOSTS

    verified = {(host, int(port)) for host, port in VERIFIED_HOSTS}
    for _ in range(50):
        probed = set(tdx._candidate_servers(None)[: tdx._TDX_MAX_CANDIDATES])
        assert verified <= probed


def test_configured_pool_still_outranks_the_verified_hosts(monkeypatch):
    """An operator who lists hosts for their own network is probed first."""

    class _Cfg:
        tdx_host_pool = ["10.0.0.1:7709", "10.0.0.2:7709"]

    candidates = tdx._candidate_servers(_Cfg())

    assert candidates[:2] == [("10.0.0.1", 7709), ("10.0.0.2", 7709)]
    assert len(candidates) == len(set(candidates))


def test_bundled_remainder_is_still_shuffled():
    """Load spreading survives: the tail order varies between calls."""
    tails = {tuple(tdx._candidate_servers(None)[8:]) for _ in range(20)}
    assert len(tails) > 1
