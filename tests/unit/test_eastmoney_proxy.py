"""EastMoneyClient proxy wiring."""

from unittest.mock import patch

from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient
from cn_market_lake.config import Config


def test_eastmoney_client_passes_config_proxy(tmp_path):
    cfg = Config(data_root=tmp_path / "data", eastmoney_proxy="http://127.0.0.1:7890")
    with patch("cn_market_lake.adapters.eastmoney.em_auth.httpx.Client") as mock_client:
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
    with patch("cn_market_lake.adapters.eastmoney.em_auth.httpx.Client") as mock_client:
        mock_client.return_value = mock_client
        client = EastMoneyClient(config=cfg)
        client.close()
    kwargs = mock_client.call_args.kwargs
    assert kwargs.get("proxy") is None
    assert kwargs.get("proxies") is None
