"""Unit tests for commodity_bars adapter normalize path."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from cnequity.adapters.eastmoney.commodity_bars import (
    CONTINUOUS_CONTRACTS,
    fetch_commodity_bars_range,
)
from cnequity.domain.datasets import DATASETS, get_dataset
from cnequity.domain.schemas import DATASET_SCHEMAS, PRIMARY_KEYS, validate_dataframe


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
                "2026-07-22,892.4,895.0,900.0,890.0,1200,345678.0,1.1",
                "2026-07-22,nan,892.4,893.5,875.0,1286,234567.0,1.1",
                "2026-07-23,885.0,892.4,893.5,875.0,inf,234567.0,1.1",
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
            "cnequity.adapters.eastmoney.commodity_bars.EastMoneyClient",
            return_value=fake_client,
        ),
        patch(
            "cnequity.adapters.sina.global_futures.fetch_offshore_commodity_bars_range",
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
    from cnequity.domain.schemas import with_provenance

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
            "cnequity.adapters.eastmoney.commodity_bars.EastMoneyClient",
            return_value=fake_client,
        ),
        patch(
            "cnequity.adapters.sina.global_futures.fetch_offshore_commodity_bars_range",
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
            "cnequity.adapters.eastmoney.commodity_bars.EastMoneyClient",
            return_value=fake_client,
        ),
        patch("cnequity.adapters.eastmoney.commodity_bars.time.sleep") as slept,
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
            "cnequity.adapters.eastmoney.commodity_bars.EastMoneyClient",
            return_value=fake_client,
        ),
        patch("cnequity.adapters.eastmoney.commodity_bars.time.sleep"),
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


def test_strict_eastmoney_failure_does_not_return_partial_success():
    fake_client = MagicMock()
    fake_client.get.side_effect = RuntimeError("contract route down")
    fake_client.__enter__.return_value = fake_client
    fake_client.__exit__.return_value = None

    only = (("AU0.SHF", "113.AUM", "沪金主连", "SHF"),)
    with patch(
        "cnequity.adapters.eastmoney.commodity_bars.EastMoneyClient",
        return_value=fake_client,
    ):
        with pytest.raises(RuntimeError, match="commodity_bars failed for AU0.SHF"):
            fetch_commodity_bars_range(
                date(2026, 7, 21),
                date(2026, 7, 21),
                contracts=only,
                include_offshore=False,
                config=_EastMoneyOnly(),
                strict=True,
            )


def test_strict_daily_fetch_rejects_partial_contract_set(monkeypatch):
    contracts = (
        ("AU0.SHF", "113.AUM", "沪金主连", "SHF"),
        ("AG0.SHF", "113.AGM", "沪银主连", "SHF"),
    )

    def fake_sina(start, end, *, contracts=None, config=None, **kwargs):
        return pl.DataFrame(
            [
                {
                    "symbol": "AU0.SHF",
                    "name": "沪金主连",
                    "exchange": "SHF",
                    "trade_date": start,
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
        "cnequity.adapters.sina.domestic_futures.fetch_domestic_commodity_bars_range",
        fake_sina,
    )

    with pytest.raises(RuntimeError, match="missing 1 domestic contract.*AG0.SHF"):
        fetch_commodity_bars_range(
            date(2026, 7, 21),
            date(2026, 7, 21),
            contracts=contracts,
            include_offshore=False,
            strict=True,
        )


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
    from cnequity.adapters.eastmoney import commodity_bars as cb

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
        "cnequity.adapters.sina.domestic_futures.fetch_domestic_commodity_bars_range",
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
    from cnequity.adapters.eastmoney.commodity_bars import (
        CONTINUOUS_CONTRACTS,
        _sina_contracts,
    )
    from cnequity.adapters.sina.domestic_futures import DOMESTIC_CONTRACTS

    derived = _sina_contracts(CONTINUOUS_CONTRACTS)
    assert len(derived) == len(CONTINUOUS_CONTRACTS)
    # Deriving from the lake symbol must reproduce the hand-written table.
    assert derived == DOMESTIC_CONTRACTS


def test_explicit_empty_domestic_contracts_are_a_noop():
    from cnequity.adapters.sina.domestic_futures import fetch_domestic_commodity_bars_range

    client = MagicMock()
    client.get.side_effect = AssertionError("empty contract selection must not fetch defaults")
    df = fetch_domestic_commodity_bars_range(
        date(2026, 7, 21), date(2026, 7, 21), contracts=(), client=client
    )
    assert df.is_empty()


def test_sina_nonfinite_open_interest_is_null():
    from cnequity.adapters.sina.domestic_futures import fetch_domestic_commodity_bars_range

    client = MagicMock()
    client.get.return_value = MagicMock(
        raise_for_status=MagicMock(),
        text=(
            'x([{"d":"2026-07-21","o":"900","h":"910","l":"895","c":"905","v":"1000","p":"inf"}])'
        ),
    )
    df = fetch_domestic_commodity_bars_range(
        date(2026, 7, 21),
        date(2026, 7, 21),
        contracts=(("AU0.SHF", "AU0", "沪金主连", "SHF"),),
        client=client,
    )
    assert df.height == 1
    assert df["open_interest"][0] is None


def test_sina_domestic_skips_non_object_rows_and_keeps_valid_rows():
    from cnequity.adapters.sina.domestic_futures import fetch_domestic_commodity_bars_range

    client = MagicMock()
    client.get.return_value = MagicMock(
        raise_for_status=MagicMock(),
        text=(
            'x([null,{"d":"2026-07-21","o":"900","h":"910",'
            '"l":"895","c":"905","v":"1000","p":"50"}])'
        ),
    )
    df = fetch_domestic_commodity_bars_range(
        date(2026, 7, 21),
        date(2026, 7, 21),
        contracts=(("AU0.SHF", "AU0", "沪金主连", "SHF"),),
        client=client,
    )
    assert df.height == 1


def test_sina_domestic_malformed_jsonp_fails_strict_fetch():
    from cnequity.adapters.sina.domestic_futures import fetch_domestic_commodity_bars_range

    client = MagicMock()
    client.get.return_value = MagicMock(
        raise_for_status=MagicMock(),
        text="<html>rate limited</html>",
    )
    with pytest.raises(RuntimeError, match="domestic commodity_bars failed for AU0.SHF"):
        fetch_domestic_commodity_bars_range(
            date(2026, 7, 21),
            date(2026, 7, 21),
            contracts=(("AU0.SHF", "AU0", "沪金主连", "SHF"),),
            client=client,
            strict=True,
        )


def test_sina_int64_overflow_volume_is_dropped():
    from cnequity.adapters.sina.domestic_futures import fetch_domestic_commodity_bars_range

    client = MagicMock()
    client.get.return_value = MagicMock(
        raise_for_status=MagicMock(),
        text=(
            'x([{"d":"2026-07-21","o":"900","h":"910","l":"895","c":"905","v":"1e300","p":"50"}])'
        ),
    )
    assert fetch_domestic_commodity_bars_range(
        date(2026, 7, 21),
        date(2026, 7, 21),
        contracts=(("AU0.SHF", "AU0", "沪金主连", "SHF"),),
        client=client,
    ).is_empty()


def test_a_sina_rate_limit_cools_every_lane_instead_of_retrying_straight_away():
    """Sina answers HTTP 456 when a sweep exceeds its anti-abuse budget, and the
    budget is vendor-wide.

    This sweep had no rate-limit awareness: the 456 raised straight out and,
    with `strict=True`, one throttled contract failed the whole `commodity_bars`
    step — having asked a vendor that just said "stop" as often as the retry
    policy allowed. Two other Sina sweeps already cool the lane through
    `defer_source`; this one is on the same budget.
    """
    import httpx

    from cnequity.adapters.sina import domestic_futures as df

    deferred: list[tuple[str, float]] = []

    class _Config:
        def defer_source(self, source, seconds):
            deferred.append((source, seconds))

    class _Resp:
        status_code = 456
        text = ""

        def raise_for_status(self):
            raise httpx.HTTPStatusError("456", request=None, response=self)

    class _Client:
        def __init__(self):
            self.calls = 0

        def get(self, url, params=None):
            self.calls += 1
            return _Resp()

    client = _Client()
    with pytest.raises(httpx.HTTPStatusError):
        df._get_with_cooldown(client, "NI0", config=_Config())

    assert client.calls == df.SINA_FETCH_ATTEMPTS
    # Cooled before every retry, and the whole vendor rather than this endpoint.
    assert deferred == [("sina", df.SINA_RATE_LIMIT_COOLDOWN_SECONDS)] * (
        df.SINA_FETCH_ATTEMPTS - 1
    )
