"""EastMoney valuation metrics fetch (PE/PB/PS/market cap)."""

from __future__ import annotations

from datetime import date

import pytest

from cnequity.adapters.eastmoney.valuation import fetch_valuation_metrics


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
            "f130": "8.4",
            "f20": "2.1e12",
            "f21": "2.0e12",
        }
    ]
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.valuation.fetch_clist_pages",
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


def test_fetch_valuation_metrics_dedupes_symbols(monkeypatch):
    raw = [
        {
            "f12": "600519",
            "f13": 1,
            "f9": "35.2",
            "f23": "12.1",
            "f130": "8.4",
            "f20": "2.1e12",
            "f21": "2.0e12",
        },
        {
            "f12": "600519",
            "f13": 1,
            "f9": "36.2",
            "f23": "12.2",
            "f130": "8.5",
            "f20": "2.2e12",
            "f21": "2.1e12",
        },
    ]
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.valuation.fetch_clist_pages",
        lambda client, fields: raw,
    )
    df = fetch_valuation_metrics(date(2024, 6, 28), client=_Client())
    assert df.height == 1
    assert df["pe_ttm"][0] == 36.2


def test_fetch_valuation_metrics_owns_and_closes_default_client(monkeypatch):
    created: list[_Client] = []

    def _factory(**kwargs):
        client = _Client()
        created.append(client)
        return client

    monkeypatch.setattr("cnequity.adapters.eastmoney.valuation.EastMoneyClient", _factory)
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.valuation.fetch_clist_pages",
        lambda client, fields: [],
    )
    df = fetch_valuation_metrics(date(2024, 6, 28))
    assert df.is_empty()
    assert created[0].closed is True


def test_fetch_valuation_metrics_empty_when_no_rows(monkeypatch):
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.valuation.fetch_clist_pages",
        lambda client, fields: [],
    )
    df = fetch_valuation_metrics(date(2024, 6, 28), client=_Client())
    assert df.is_empty()


def test_fetch_valuation_metrics_drops_unmappable_non_security_rows(monkeypatch):
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.valuation.fetch_clist_pages",
        lambda client, fields: [{"f12": "600519", "f13": 1}, {"f12": "123456"}],
    )
    out = fetch_valuation_metrics(date(2024, 6, 28), client=_Client())
    assert out["symbol"].to_list() == ["600519.SH"]


def test_fetch_valuation_metrics_closes_owned_client_on_failure(monkeypatch):
    created: list[_Client] = []

    def _factory(**kwargs):
        client = _Client()
        created.append(client)
        return client

    monkeypatch.setattr("cnequity.adapters.eastmoney.valuation.EastMoneyClient", _factory)
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.valuation.fetch_clist_pages",
        lambda client, fields: (_ for _ in ()).throw(RuntimeError("clist down")),
    )
    with pytest.raises(RuntimeError, match="clist down"):
        fetch_valuation_metrics(date(2024, 6, 28))
    assert created[0].closed is True


def test_ps_ttm_reads_the_ratio_field_not_an_amount(monkeypatch):
    """`f45` is a figure in yuan; `f130` is 市销率 TTM.

    Reading f45 put a median of 2.05e7 into `ps_ttm` for every EastMoney row
    while baostock's median for the same column was 3.2. The feed's own numbers
    settle it: total_mv / f132 (营业总收入 TTM) equals f130 exactly — 1.591e12 /
    1.732e11 = 9.184 for 600519 — and a licensed third source reads the same
    name at 9.20.
    """
    from cnequity.adapters.eastmoney import valuation

    assert "f130" in valuation._VALUATION_FIELDS
    assert "f45" not in valuation._VALUATION_FIELDS

    raw = [
        {
            "f12": "600519",
            "f13": 1,
            "f9": "19.54",
            "f23": "6.33",
            "f130": "9.184",
            "f45": "44516880421.86",  # present in the payload, and ignored
            "f20": "1.591041357673e12",
            "f21": "1.591041357673e12",
        }
    ]
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.valuation.fetch_clist_pages",
        lambda client, fields: raw,
    )
    row = fetch_valuation_metrics(date(2026, 9, 15), client=_Client()).row(0, named=True)

    assert row["ps_ttm"] == 9.184
