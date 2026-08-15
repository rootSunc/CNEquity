"""EastMoney valuation metrics fetch (PE/PB/PS/market cap)."""

from __future__ import annotations

from datetime import date

from cn_market_lake.adapters.eastmoney.valuation import fetch_valuation_metrics


class _Client:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def test_fetch_valuation_metrics_maps_fields(monkeypatch):
    raw = [
        {
            "f12": "600519",
            "f13": 1,
            "f9": "35.2",
            "f23": "12.1",
            "f45": "8.4",
            "f20": "2.1e12",
            "f21": "2.0e12",
        }
    ]
    monkeypatch.setattr(
        "cn_market_lake.adapters.eastmoney.valuation.fetch_clist_pages",
        lambda client, fields: raw,
    )
    client = _Client()
    df = fetch_valuation_metrics(date(2024, 6, 28), client=client)
    assert client.closed is False  # caller-provided client must not be closed
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["symbol"] == "600519.SH"
    assert row["trade_date"] == date(2024, 6, 28)
    assert row["pe_ttm"] == 35.2
    assert row["pb"] == 12.1
    assert row["ps_ttm"] == 8.4
    assert row["total_mv"] == 2.1e12
    assert row["float_mv"] == 2.0e12


def test_fetch_valuation_metrics_owns_and_closes_default_client(monkeypatch):
    created: list[_Client] = []

    def _factory(**kwargs):
        client = _Client()
        created.append(client)
        return client

    monkeypatch.setattr("cn_market_lake.adapters.eastmoney.valuation.EastMoneyClient", _factory)
    monkeypatch.setattr(
        "cn_market_lake.adapters.eastmoney.valuation.fetch_clist_pages",
        lambda client, fields: [],
    )
    df = fetch_valuation_metrics(date(2024, 6, 28))
    assert df.is_empty()
    assert created[0].closed is True


def test_fetch_valuation_metrics_empty_when_no_rows(monkeypatch):
    monkeypatch.setattr(
        "cn_market_lake.adapters.eastmoney.valuation.fetch_clist_pages",
        lambda client, fields: [],
    )
    df = fetch_valuation_metrics(date(2024, 6, 28), client=_Client())
    assert df.is_empty()
