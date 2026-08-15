"""Unit tests for commodity_bars adapter normalize path."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl

from cn_market_lake.adapters.eastmoney.commodity_bars import (
    CONTINUOUS_CONTRACTS,
    fetch_commodity_bars_range,
)
from cn_market_lake.domain.datasets import DATASETS, get_dataset
from cn_market_lake.domain.schemas import DATASET_SCHEMAS, PRIMARY_KEYS, validate_dataframe


def test_commodity_bars_registered():
    spec = get_dataset("commodity_bars")
    assert spec.partition_col == "trade_date"
    assert spec.fetch_semantics == "by_date"
    assert spec.backfill_source == "eastmoney_kline+sina_global"
    assert spec.required is False
    assert "commodity_bars" in DATASET_SCHEMAS
    assert PRIMARY_KEYS["commodity_bars"] == ["symbol", "trade_date"]


def test_continuous_contract_symbols_unique():
    syms = [c[0] for c in CONTINUOUS_CONTRACTS]
    assert len(syms) == len(set(syms))
    for sym, secid, _name, exch in CONTINUOUS_CONTRACTS:
        assert sym.endswith(f".{exch}")
        assert "." in secid


def test_fetch_commodity_bars_parses_kline():
    kline_body = {
        "data": {
            "name": "沪金主连",
            "klines": [
                "2026-07-20,880.0,885.0,890.0,870.0,1000,123456.0,1.2",
                "2026-07-21,885.0,892.4,893.5,875.0,1286,234567.0,1.1",
            ],
        }
    }

    class FakeResp:
        def raise_for_status(self):
            return None

        def json(self):
            return kline_body

    fake_client = MagicMock()
    fake_client.get.return_value = FakeResp()
    fake_client.__enter__.return_value = fake_client
    fake_client.__exit__.return_value = None

    only = (("AU0.SHF", "113.AUM", "沪金主连", "SHF"),)
    with (
        patch(
            "cn_market_lake.adapters.eastmoney.commodity_bars.EastMoneyClient",
            return_value=fake_client,
        ),
        patch(
            "cn_market_lake.adapters.sina.global_futures.fetch_offshore_commodity_bars_range",
            return_value=pl.DataFrame(),
        ),
    ):
        df = fetch_commodity_bars_range(
            date(2026, 7, 20),
            date(2026, 7, 21),
            contracts=only,
            include_offshore=True,
            config=_EastMoneyOnly(),
        )

    assert df.height == 2
    assert set(df["symbol"].to_list()) == {"AU0.SHF"}
    assert df.filter(pl.col("trade_date") == date(2026, 7, 21))["close"][0] == 892.4
    from cn_market_lake.domain.schemas import with_provenance

    validated = validate_dataframe(
        with_provenance(df, source="eastmoney", data_version="v1"),
        "commodity_bars",
    )
    assert validated.height == 2
    assert "open_interest" in validated.columns


class _EastMoneyOnly:
    """Config stub that opts into the EastMoney path and blocks Sina.

    The domestic source is Sina now; EastMoney is opt-in. These three tests are
    about the EastMoney retry loop, so they turn it on explicitly — and they set
    sources["sina"]=False so a failure cannot fall through to a live network
    call, which is exactly what happened when the reroute first landed.
    """

    sources = {"sina": False, "eastmoney": True}
    eastmoney_proxy = None
    eastmoney_timeout_sec = 15.0
    _commodity_via_eastmoney = True

    def rate_limit(self, source):
        return None


def test_fetch_commodity_bars_empty_on_failure():
    fake_client = MagicMock()
    fake_client.get.side_effect = RuntimeError("boom")
    fake_client.__enter__.return_value = fake_client
    fake_client.__exit__.return_value = None

    only = (("AU0.SHF", "113.AUM", "沪金主连", "SHF"),)
    with (
        patch(
            "cn_market_lake.adapters.eastmoney.commodity_bars.EastMoneyClient",
            return_value=fake_client,
        ),
        patch(
            "cn_market_lake.adapters.sina.global_futures.fetch_offshore_commodity_bars_range",
            return_value=pl.DataFrame(),
        ),
    ):
        df = fetch_commodity_bars_range(
            date(2026, 7, 21),
            date(2026, 7, 21),
            contracts=only,
            include_offshore=False,
            config=_EastMoneyOnly(),
        )
    assert df.is_empty()


def test_transport_failures_are_not_retried():
    """Regression: the fail-fast predicate was inverted in this loop.

    It retried exactly the failures ``is_transport_fail_fast`` says a retry
    cannot fix. With push2his refusing an egress, the 15 domestic contracts
    burned 151s of backoff per daily run to return nothing.
    """
    import httpx

    fake_client = MagicMock()
    fake_client.get.side_effect = httpx.ConnectError("route down")
    fake_client.__enter__.return_value = fake_client
    fake_client.__exit__.return_value = None

    only = (("AU0.SHF", "113.AUM", "沪金主连", "SHF"),)
    with (
        patch(
            "cn_market_lake.adapters.eastmoney.commodity_bars.EastMoneyClient",
            return_value=fake_client,
        ),
        patch("cn_market_lake.adapters.eastmoney.commodity_bars.time.sleep") as slept,
    ):
        df = fetch_commodity_bars_range(
            date(2026, 7, 21),
            date(2026, 7, 21),
            contracts=only,
            include_offshore=False,
            config=_EastMoneyOnly(),
        )
    assert df.is_empty()
    assert fake_client.get.call_count == 1, "a dead route must cost one attempt, not five"
    # The 0.25s pause between contracts still runs; what must not appear is the
    # retry backoff ladder (0.6 / 1.1 / 1.6 / 2.1).
    backoffs = [c.args[0] for c in slept.call_args_list if c.args and c.args[0] > 0.3]
    assert backoffs == [], f"no backoff for a failure retrying cannot fix, got {backoffs}"


def test_transient_failures_still_retry():
    """The other half of the predicate: retryable errors keep their budget."""
    fake_client = MagicMock()
    fake_client.get.side_effect = RuntimeError("transient parse blip")
    fake_client.__enter__.return_value = fake_client
    fake_client.__exit__.return_value = None

    only = (("AU0.SHF", "113.AUM", "沪金主连", "SHF"),)
    with (
        patch(
            "cn_market_lake.adapters.eastmoney.commodity_bars.EastMoneyClient",
            return_value=fake_client,
        ),
        patch("cn_market_lake.adapters.eastmoney.commodity_bars.time.sleep"),
    ):
        df = fetch_commodity_bars_range(
            date(2026, 7, 21),
            date(2026, 7, 21),
            contracts=only,
            include_offshore=False,
            config=_EastMoneyOnly(),
        )
    assert df.is_empty()
    assert fake_client.get.call_count == 5


def test_dataset_count_includes_commodity():
    assert "commodity_bars" in DATASETS


# --- Sina is the domestic source now ----------------------------------------


def test_domestic_defaults_to_sina_not_push2his(monkeypatch):
    """push2his must not be touched on the daily path.

    It refuses requests intermittently in a way nothing here controls (measured
    0/12 direct and through a mainland exit, still failing after seven minutes
    of quiet, TLS and routing healthy throughout), and commodity_bars was its
    only daily consumer — spending every run failing 15 contracts to write the
    one offshore row.
    """
    from cn_market_lake.adapters.eastmoney import commodity_bars as cb

    def _boom(*a, **k):
        raise AssertionError("EastMoney must not be called by default")

    monkeypatch.setattr(cb, "EastMoneyClient", _boom)
    captured = {}

    def _fake_sina(start, end, *, contracts=None, config=None, **k):
        captured["contracts"] = contracts
        return pl.DataFrame(
            [
                {
                    "symbol": "AU0.SHF",
                    "name": "沪金主连",
                    "exchange": "SHF",
                    "trade_date": date(2026, 7, 21),
                    "open": 900.0,
                    "high": 910.0,
                    "low": 895.0,
                    "close": 905.0,
                    "volume": 1000,
                    "amount": None,
                    "open_interest": 50.0,
                    "source": "sina",
                }
            ]
        )

    monkeypatch.setattr(
        "cn_market_lake.adapters.sina.domestic_futures.fetch_domestic_commodity_bars_range",
        _fake_sina,
    )
    df = cb.fetch_commodity_bars_range(
        date(2026, 7, 21),
        date(2026, 7, 21),
        contracts=(("AU0.SHF", "113.AUM", "沪金主连", "SHF"),),
        include_offshore=False,
    )
    assert df.height == 1
    assert df["source"][0] == "sina"
    # The Sina symbol is derived from the lake symbol, not a second table.
    assert captured["contracts"] == (("AU0.SHF", "AU0", "沪金主连", "SHF"),)


def test_sina_contract_mapping_covers_every_contract():
    from cn_market_lake.adapters.eastmoney.commodity_bars import (
        CONTINUOUS_CONTRACTS,
        _sina_contracts,
    )
    from cn_market_lake.adapters.sina.domestic_futures import DOMESTIC_CONTRACTS

    derived = _sina_contracts(CONTINUOUS_CONTRACTS)
    assert len(derived) == len(CONTINUOUS_CONTRACTS)
    # Deriving from the lake symbol must reproduce the hand-written table.
    assert derived == DOMESTIC_CONTRACTS
