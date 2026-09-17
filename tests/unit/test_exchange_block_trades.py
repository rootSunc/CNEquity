"""The availability route for `block_trades`, and what it must not get wrong.

EastMoney is the only other source. These pin the two things that measurement —
not reading the spec — caught while wiring it.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

import cnequity.steps.capital as cap
from cnequity.adapters.exchange import block_trades as bt

DAY = date(2026, 9, 15)


def _sse_payload(rows):
    import json

    return "cb(" + json.dumps({"pageHelp": {"total": len(rows), "data": rows}}) + ")"


class _Resp:
    def __init__(self, payload, *, text=None):
        self._payload, self.text = payload, text or ""

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


def test_amounts_stay_in_the_units_the_dataset_already_holds(monkeypatch):
    """The unit contract says shares and CNY; the rows say 万股 and 万元.

    EastMoney's 2026-09-15 row for 002422.SZ is 5747.68 against the exchange's
    identical trade, so converting here would have made the backup disagree
    with the primary by exactly 10,000x on every row.
    """
    monkeypatch.setattr(
        bt,
        "_client",
        lambda: type(
            "S",
            (),
            {
                "get": lambda self, url, **kw: _Resp(
                    [
                        {
                            "metadata": {"recordcount": 1, "pagesize": 20},
                            "data": [
                                {
                                    "zqdh": "002422",
                                    "cjjg": "40.65",
                                    "cjgsnew": "141.39",
                                    "cjjenew": "5,747.68",
                                }
                            ],
                        }
                    ]
                ),
                "close": lambda self: None,
            },
        )(),
    )
    frame = bt.fetch_szse_block_trades(DAY)

    assert frame.height == 1
    assert frame["amount"][0] == pytest.approx(5747.68)
    assert frame["volume"][0] == pytest.approx(141.39)


def test_two_identical_trades_are_summed_not_deduplicated(monkeypatch):
    """The primary key cannot tell them apart, and the primary aggregates.

    603382.SH traded twice at one price and size on 2026-09-15; dropping the
    repeat made the exchange row exactly half of EastMoney's, which reads as a
    50% move rather than a missing fill.
    """
    same = {"stockid": "603382", "tradeprice": "20.28", "tradeqty": "10", "tradeamount": "202.8"}
    monkeypatch.setattr(bt, "fetch_szse_block_trades", lambda day, config=None: bt.EMPTY.clone())
    monkeypatch.setattr(
        bt,
        "_client",
        lambda: type(
            "S",
            (),
            {
                "get": lambda self, url, **kw: _Resp(None, text=_sse_payload([same, dict(same)])),
                "close": lambda self: None,
            },
        )(),
    )

    frame = bt.fetch_block_trades_exchange(DAY)

    assert frame.height == 1, "one key, because the key cannot separate them"
    assert frame["amount"][0] == pytest.approx(405.6), "summed, not dropped"
    assert frame["volume"][0] == pytest.approx(20.0)


def test_a_silent_exchange_is_not_filled_in_from_the_other(monkeypatch):
    """They publish different markets: a missing SZSE page is missing SZ rows."""
    sz = pl.DataFrame(
        {
            "symbol": ["000001.SZ"],
            "trade_date": [DAY],
            "price": [10.0],
            "volume": [1.0],
            "amount": [10.0],
        }
    )
    monkeypatch.setattr(bt, "fetch_szse_block_trades", lambda day, config=None: sz)
    monkeypatch.setattr(bt, "fetch_sse_block_trades", lambda day, config=None: bt.EMPTY.clone())

    frame = bt.fetch_block_trades_exchange(DAY)

    assert frame["symbol"].to_list() == ["000001.SZ"]


def test_the_step_falls_back_when_the_vendor_raises(monkeypatch):
    called: list[str] = []

    def _boom(day, **kwargs):
        raise RuntimeError("EastMoney datacenter down")

    def _exchange(day, config=None):
        called.append("exchange")
        return pl.DataFrame(
            {
                "symbol": ["600000.SH"],
                "trade_date": [day],
                "price": [1.0],
                "volume": [1.0],
                "amount": [1.0],
            }
        )

    frame = cap._with_exchange_fallback("block_trades", _boom, _exchange)(DAY, config=None)

    assert called == ["exchange"]
    assert frame["symbol"].to_list() == ["600000.SH"]


def test_a_healthy_vendor_is_not_second_guessed(monkeypatch):
    """The backup is for an outage, not a cross-check — asking both every day
    would double the request budget and invite a disagreement nothing settles."""
    called: list[str] = []
    good = pl.DataFrame(
        {
            "symbol": ["600000.SH"],
            "trade_date": [DAY],
            "price": [1.0],
            "volume": [1.0],
            "amount": [1.0],
        }
    )

    def _exchange(day, config=None):
        called.append("exchange")
        return bt.EMPTY.clone()

    frame = cap._with_exchange_fallback("block_trades", lambda d, **k: good, _exchange)(
        DAY, config=None
    )

    assert called == []
    assert frame.height == 1


def test_a_security_gets_one_row_at_its_weighted_price(monkeypatch):
    """The exchanges publish transactions; the lake holds securities.

    Across 176,093 (day, security) pairs the lake has never held two rows, but
    2026-09-15 came to 29 SZ rows for 17 securities and 56 SH rows for 15. A
    degraded day would have carried several times the rows of its neighbours,
    at prices meaning something else, under a key that includes `price`. The
    weighted average is what the vendor's single row holds — it agreed to
    0.003% over those 32 securities, the width of its four decimals.
    """
    trades = [
        {"stockid": "600000", "tradeprice": "10.00", "tradeqty": "30", "tradeamount": "300"},
        {"stockid": "600000", "tradeprice": "12.00", "tradeqty": "10", "tradeamount": "120"},
    ]
    monkeypatch.setattr(bt, "fetch_szse_block_trades", lambda day, config=None: bt.EMPTY.clone())
    monkeypatch.setattr(
        bt,
        "_client",
        lambda: type(
            "S",
            (),
            {
                "get": lambda self, url, **kw: _Resp(None, text=_sse_payload(trades)),
                "close": lambda self: None,
            },
        )(),
    )

    frame = bt.fetch_block_trades_exchange(DAY)

    assert frame.height == 1, "one row per security, as the primary writes it"
    assert frame["volume"][0] == pytest.approx(40.0)
    assert frame["amount"][0] == pytest.approx(420.0)
    assert frame["price"][0] == pytest.approx(10.5), "weighted, not the first or the last"
