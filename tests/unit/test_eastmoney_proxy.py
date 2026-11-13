"""EastMoneyClient proxy wiring."""

from unittest.mock import patch

import httpx
import pytest

from cnequity.adapters.eastmoney.em_auth import EastMoneyClient
from cnequity.config import Config, load_config
from cnequity.config.bootstrap import path_for_toml


def test_eastmoney_client_passes_config_proxy(tmp_path):
    cfg = Config(data_root=tmp_path / "data", eastmoney_proxy="http://127.0.0.1:7890")
    with patch("cnequity.adapters.eastmoney.em_auth.httpx.Client") as mock_client:
        mock_client.return_value = mock_client
        client = EastMoneyClient(config=cfg)
        client.close()
    kwargs = mock_client.call_args.kwargs
    # httpx>=0.28 uses ``proxy``; older pins used ``proxies``.
    assert kwargs.get("proxy") == "http://127.0.0.1:7890" or kwargs.get("proxies") == (
        "http://127.0.0.1:7890"
    )


def test_eastmoney_client_no_proxy_by_default(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    with patch("cnequity.adapters.eastmoney.em_auth.httpx.Client") as mock_client:
        mock_client.return_value = mock_client
        client = EastMoneyClient(config=cfg)
        client.close()
    kwargs = mock_client.call_args.kwargs
    assert kwargs.get("proxy") is None
    assert kwargs.get("proxies") is None


def test_eastmoney_direct_fallback_config_is_explicit(tmp_path):
    path = tmp_path / "cnequity.toml"
    path.write_text(
        f'''[data]
root = "{path_for_toml(tmp_path / "data")}"
[sources.eastmoney]
enabled = true
proxy = "http://127.0.0.1:7890"
direct_fallback = true
''',
        encoding="utf-8",
    )

    assert load_config(path).eastmoney_direct_fallback is True


def test_push2his_proxy_protocol_failure_retries_direct_once(tmp_path, monkeypatch):
    from cnequity.domain.rate_limit import _read_json

    cfg = Config(
        data_root=tmp_path / "data",
        eastmoney_proxy="http://127.0.0.1:7890",
        eastmoney_direct_fallback=True,
        # These exercise the fallback route itself; the breaker would (by
        # design) refuse the second route after the first push2 refusal.
        eastmoney_push2_breaker=False,
        source_intervals={"eastmoney": 0.0},
    )
    client = EastMoneyClient(config=cfg)
    direct_calls: list[str] = []

    def proxy_get(url, **kwargs):
        raise httpx.RemoteProtocolError("server disconnected")

    class _Direct:
        def get(self, url, **kwargs):
            direct_calls.append(url)
            return httpx.Response(200, request=httpx.Request("GET", url), text="ok")

        def close(self):
            pass

    monkeypatch.setattr(client._client, "get", proxy_get)
    client._direct_client = _Direct()

    response = client.get("https://push2his.eastmoney.com/api/qt/stock/kline/get")
    client.close()

    assert response.status_code == 200
    assert len(direct_calls) == 1
    assert client.last_route_outcome["proxy_failed"] is True
    assert client.last_route_outcome["direct_succeeded"] is True
    assert client.last_route_outcome["direct_failed"] is False
    assert _read_json(cfg.rate_limit_root / "events-eastmoney_push2.json")["fallback"] == 1


def test_push2his_records_when_proxy_and_direct_both_fail(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        eastmoney_proxy="http://127.0.0.1:7890",
        eastmoney_direct_fallback=True,
        # These exercise the fallback route itself; the breaker would (by
        # design) refuse the second route after the first push2 refusal.
        eastmoney_push2_breaker=False,
        source_intervals={"eastmoney": 0.0},
    )
    client = EastMoneyClient(config=cfg)

    class _Direct:
        def get(self, url, **kwargs):
            raise httpx.RemoteProtocolError("direct drop")

        def close(self):
            pass

    monkeypatch.setattr(
        client._client,
        "get",
        lambda url, **kwargs: (_ for _ in ()).throw(httpx.RemoteProtocolError("proxy drop")),
    )
    client._direct_client = _Direct()

    with pytest.raises(httpx.RemoteProtocolError, match="direct drop"):
        client.get("https://push2his.eastmoney.com/api/qt/stock/kline/get")
    assert client.last_route_outcome["proxy_failed"] is True
    assert client.last_route_outcome["direct_failed"] is True
    client.close()


def test_direct_fallback_does_not_bypass_proxy_for_other_hosts(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        eastmoney_proxy="http://127.0.0.1:7890",
        eastmoney_direct_fallback=True,
        # These exercise the fallback route itself; the breaker would (by
        # design) refuse the second route after the first push2 refusal.
        eastmoney_push2_breaker=False,
        source_intervals={"eastmoney": 0.0},
    )
    client = EastMoneyClient(config=cfg)
    monkeypatch.setattr(
        client._client,
        "get",
        lambda url, **kwargs: (_ for _ in ()).throw(httpx.RemoteProtocolError("drop")),
    )

    with pytest.raises(httpx.RemoteProtocolError):
        client.get("https://push2.eastmoney.com/api/qt/clist/get")
    assert client._direct_client is None
    client.close()


def test_direct_fallback_is_opt_in(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        eastmoney_proxy="http://127.0.0.1:7890",
        source_intervals={"eastmoney": 0.0},
    )
    client = EastMoneyClient(config=cfg)
    monkeypatch.setattr(
        client._client,
        "get",
        lambda url, **kwargs: (_ for _ in ()).throw(httpx.RemoteProtocolError("drop")),
    )

    with pytest.raises(httpx.RemoteProtocolError):
        client.get("https://push2his.eastmoney.com/api/qt/stock/kline/get")
    assert client._direct_client is None
    client.close()


def test_push2_paused_config_is_read(tmp_path):
    path = tmp_path / "cnequity.toml"
    path.write_text(
        f'''[data]
root = "{path_for_toml(tmp_path / "data")}"
[sources.eastmoney]
push2_paused = true
''',
        encoding="utf-8",
    )

    assert load_config(path).eastmoney_push2_paused is True


@pytest.mark.parametrize(
    "url",
    [
        "https://push2.eastmoney.com/api/qt/clist/get?pn=1",
        "https://40.push2.eastmoney.com/api/qt/clist/get",
        "https://push2delay.eastmoney.com/api/qt/clist/get",
        "https://push2his.eastmoney.com/api/qt/stock/kline/get",
        "https://91.push2his.eastmoney.com/api/qt/stock/kline/get",
    ],
)
def test_paused_push2_is_refused_before_anything_is_sent(tmp_path, monkeypatch, url):
    from cnequity.adapters.eastmoney.em_auth import Push2PausedError, is_transport_fail_fast

    cfg = Config(data_root=tmp_path / "data", eastmoney_push2_paused=True)
    client = EastMoneyClient(config=cfg)
    sent: list[str] = []
    monkeypatch.setattr(client._client, "get", lambda u, **kw: sent.append(u))
    monkeypatch.setattr(client._client, "post", lambda u, **kw: sent.append(u))

    with pytest.raises(Push2PausedError) as exc:
        client.get(url)
    with pytest.raises(Push2PausedError):
        client.post(url)
    client.close()

    assert sent == []
    # Callers must see a dead route: fail fast, no retry.
    assert is_transport_fail_fast(exc.value)


def test_paused_push2_leaves_datacenter_alone(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        eastmoney_push2_paused=True,
        source_intervals={"eastmoney": 0.0},
    )
    client = EastMoneyClient(config=cfg)
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    monkeypatch.setattr("cnequity.adapters.eastmoney.em_auth.get_nid", lambda *a, **k: "")
    monkeypatch.setattr(
        client._client,
        "get",
        lambda u, **kw: httpx.Response(200, request=httpx.Request("GET", u), text="ok"),
    )

    assert client.get(url).status_code == 200
    client.close()


def test_breaker_blocks_the_direct_retry_after_a_push2his_refusal(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        eastmoney_proxy="http://127.0.0.1:7890",
        eastmoney_direct_fallback=True,
        source_intervals={"eastmoney": 0.0, "eastmoney_push2": 0.0},
    )
    client = EastMoneyClient(config=cfg)
    direct_calls: list[str] = []

    class _Direct:
        def get(self, url, **kwargs):
            direct_calls.append(url)
            return httpx.Response(200, request=httpx.Request("GET", url), text="ok")

        def close(self):
            pass

    monkeypatch.setattr(
        client._client,
        "get",
        lambda url, **kwargs: (_ for _ in ()).throw(httpx.RemoteProtocolError("dropped")),
    )
    client._direct_client = _Direct()

    from cnequity.adapters.eastmoney.em_auth import Push2BreakerOpenError

    with pytest.raises(Push2BreakerOpenError):
        client.get("https://push2his.eastmoney.com/api/qt/stock/kline/get")
    client.close()
    assert direct_calls == []


_PUSH2_ONLY = ("push2.eastmoney.com", "push2his.eastmoney.com", "push2delay.eastmoney.com")


def test_proxy_hosts_config_is_read_and_validated(tmp_path):
    from cnequity.config.errors import ConfigError

    def config(value: str):
        path = tmp_path / "cnequity.toml"
        path.write_text(
            f'''[data]
root = "{path_for_toml(tmp_path / "data")}"
[sources.eastmoney]
proxy = "http://127.0.0.1:7890"
proxy_hosts = {value}
''',
            encoding="utf-8",
        )
        return load_config(path)

    assert config('["Push2.EastMoney.com.", "push2his.eastmoney.com"]').eastmoney_proxy_hosts == (
        "push2.eastmoney.com",
        "push2his.eastmoney.com",
    )
    with pytest.raises(ConfigError, match="host names"):
        config('["https://push2.eastmoney.com"]')
    with pytest.raises(ConfigError, match="list"):
        config('"push2.eastmoney.com"')


def _routed_client(tmp_path, monkeypatch, sent, **overrides):
    cfg = Config(
        data_root=tmp_path / "data",
        eastmoney_proxy="http://127.0.0.1:7890",
        source_intervals={"eastmoney": 0.0, "eastmoney_push2": 0.0, "eastmoney_dc": 0.0},
        **overrides,
    )
    client = EastMoneyClient(config=cfg)
    monkeypatch.setattr("cnequity.adapters.eastmoney.em_auth.get_nid", lambda *a, **k: "")

    def answer(route):
        def get(url, **kwargs):
            sent.append((route, httpx.URL(url).host))
            return httpx.Response(200, request=httpx.Request("GET", url), text="{}")

        return get

    monkeypatch.setattr(client._client, "get", answer("proxy"))
    monkeypatch.setattr(client._get_direct_client(), "get", answer("direct"))
    return client


def test_only_listed_hosts_use_the_proxy(tmp_path, monkeypatch):
    sent: list[tuple[str, str]] = []
    client = _routed_client(tmp_path, monkeypatch, sent, eastmoney_proxy_hosts=_PUSH2_ONLY)
    for url in (
        "https://push2.eastmoney.com/api/qt/clist/get",
        "https://91.push2his.eastmoney.com/api/qt/stock/kline/get",
        "https://datacenter-web.eastmoney.com/api/data/v1/get",
        "https://np-anotice-stock.eastmoney.com/api/security/ann",
    ):
        client.get(url)
    routes = [client.last_route_outcome["primary_route"]]
    client.close()

    assert sent == [
        ("proxy", "push2.eastmoney.com"),
        ("proxy", "91.push2his.eastmoney.com"),
        ("direct", "datacenter-web.eastmoney.com"),
        ("direct", "np-anotice-stock.eastmoney.com"),
    ]
    assert routes == ["direct"]


def test_without_proxy_hosts_every_host_uses_the_proxy(tmp_path, monkeypatch):
    sent: list[tuple[str, str]] = []
    client = _routed_client(tmp_path, monkeypatch, sent)
    client.get("https://datacenter-web.eastmoney.com/api/data/v1/get")
    client.close()
    assert sent == [("proxy", "datacenter-web.eastmoney.com")]


def test_the_direct_client_ignores_environment_proxies(tmp_path):
    cfg = Config(
        data_root=tmp_path / "data",
        eastmoney_proxy="http://127.0.0.1:7890",
        eastmoney_proxy_hosts=_PUSH2_ONLY,
    )
    with patch("cnequity.adapters.eastmoney.em_auth.httpx.Client") as mock_client:
        mock_client.return_value = mock_client
        client = EastMoneyClient(config=cfg)
        client._route("https://datacenter-web.eastmoney.com/api/data/v1/get")
        client.close()
    direct = mock_client.call_args_list[-1].kwargs
    assert direct["trust_env"] is False and "proxy" not in direct


def test_a_refused_direct_host_is_not_blamed_on_the_proxy(tmp_path, monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.em_auth._proxy_is_dead",
        lambda proxy: calls.append(proxy) or True,
    )
    cfg = Config(
        data_root=tmp_path / "data",
        eastmoney_proxy="http://127.0.0.1:7890",
        eastmoney_proxy_hosts=_PUSH2_ONLY,
        eastmoney_datacenter_breaker=False,
        source_intervals={"eastmoney": 0.0, "eastmoney_dc": 0.0},
    )
    client = EastMoneyClient(config=cfg)
    monkeypatch.setattr("cnequity.adapters.eastmoney.em_auth.get_nid", lambda *a, **k: "")
    monkeypatch.setattr(
        client._get_direct_client(),
        "get",
        lambda url, **kwargs: (_ for _ in ()).throw(httpx.ConnectError("refused")),
    )
    with pytest.raises(httpx.ConnectError):
        client.get("https://datacenter-web.eastmoney.com/api/data/v1/get")
    client.close()
    assert calls == []


def test_proxy_hosts_do_not_change_the_config_fingerprint(tmp_path):
    from cnequity.storage.provenance import config_fingerprint

    plain = Config(data_root=tmp_path / "data", eastmoney_proxy="http://127.0.0.1:7890")
    routed = Config(
        data_root=tmp_path / "data",
        eastmoney_proxy="http://127.0.0.1:7890",
        eastmoney_proxy_hosts=_PUSH2_ONLY,
    )
    assert config_fingerprint(plain) == config_fingerprint(routed)
