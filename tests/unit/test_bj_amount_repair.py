"""Filling the Beijing turnover Sina never published, from TDX.

Sina exposes no amount for the Beijing board at all: every one of the 505,518
Sina rows in the lake carried a null one. TDX serves the board and does publish
it, but counts volume in lots — so this supplements a stored row rather than
replacing it, and only when TDX is demonstrably describing the same session.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.steps import bars

DAY = date(2026, 9, 15)


def _stored(**over) -> pl.DataFrame:
    row = {
        "symbol": "920001.BJ",
        "trade_date": DAY,
        "open": 10.0,
        "high": 11.0,
        "low": 9.5,
        "close": 10.5,
        "volume": 123_456.0,
        "amount": None,
        "source": "sina",
    }
    row.update(over)
    return pl.DataFrame([row])


def _tdx(monkeypatch, **over) -> None:
    row = {
        "symbol": "920001.BJ",
        "trade_date": DAY,
        "open": 10.0,
        "high": 11.0,
        "low": 9.5,
        "close": 10.5,
        "volume": 123_400.0,
        "amount": 1_296_000.0,
    }
    row.update(over)
    monkeypatch.setattr(
        "cnequity.adapters.tdx_protocol.client.fetch_daily_bars",
        lambda *a, **k: pl.DataFrame([row]),
    )


def _run(cfg, frame):
    return bars._supplement_bj_amounts_from_tdx(cfg, frame, start=DAY, end=DAY)


@pytest.fixture
def cfg(tmp_path):
    return Config(data_root=tmp_path / "lake")


def test_the_stored_volume_survives_the_supplement(cfg, monkeypatch):
    """TDX counts in lots and Sina has been exact since 2026. Taking TDX's row
    whole would coarsen 7,270 of them for a field we did not need."""
    _tdx(monkeypatch)
    updated, _ = _run(cfg, _stored())

    assert updated["amount"][0] == 1_296_000.0
    assert updated["volume"][0] == 123_456.0, "the finer figure stays"
    assert updated["source"][0] == "tdx_protocol"


def test_a_price_that_disagrees_leaves_the_row_alone(cfg, monkeypatch):
    """Open/high/low/close matched to the last digit across every row measured.
    One that does not is a different session, not a rounding difference."""
    _tdx(monkeypatch, close=10.6)
    updated, findings = _run(cfg, _stored())

    assert updated["amount"][0] is None
    assert updated["source"][0] == "sina"
    assert [f["check"] for f in findings if f["severity"] == "warning"] == [
        "daily_bars_tdx_amount_mismatch"
    ]


def test_a_volume_further_than_a_lot_is_not_rounding(cfg, monkeypatch):
    _tdx(monkeypatch, volume=123_456.0 - 100)
    updated, findings = _run(cfg, _stored())

    assert updated["amount"][0] is None
    assert any(f["check"] == "daily_bars_tdx_amount_mismatch" for f in findings)


def test_a_volume_inside_a_lot_is_rounding(cfg, monkeypatch):
    _tdx(monkeypatch, volume=123_456.0 - 99)
    updated, _ = _run(cfg, _stored())

    assert updated["amount"][0] == 1_296_000.0


def test_a_row_that_already_has_turnover_is_not_overwritten(cfg, monkeypatch):
    _tdx(monkeypatch)
    updated, _ = _run(cfg, _stored(amount=999.0, source="bse"))

    assert updated["amount"][0] == 999.0
    assert updated["source"][0] == "bse"


def test_a_key_tdx_never_served_is_counted_not_ignored(cfg, monkeypatch):
    """The board's retired 8xxxxx/430xxx codes are ~218,000 such rows. Counting
    them nowhere is how a repair reports success over rows it never touched."""
    monkeypatch.setattr(
        "cnequity.adapters.tdx_protocol.client.fetch_daily_bars",
        lambda *a, **k: pl.DataFrame(
            [
                {
                    "symbol": "920002.BJ",
                    "trade_date": DAY,
                    "open": 1.0,
                    "high": 1.0,
                    "low": 1.0,
                    "close": 1.0,
                    "volume": 1.0,
                    "amount": 1.0,
                }
            ]
        ),
    )
    updated, findings = _run(cfg, _stored())

    assert updated["amount"][0] is None
    unserved = [f for f in findings if f["check"] == "daily_bars_tdx_amount_unserved"]
    assert unserved and unserved[0]["rows_unserved"] == 1


def test_a_source_that_raises_leaves_every_row_as_it_was(cfg, monkeypatch):
    def _boom(*a, **k):
        raise RuntimeError("TDX returned no bars")

    monkeypatch.setattr("cnequity.adapters.tdx_protocol.client.fetch_daily_bars", _boom)
    updated, findings = _run(cfg, _stored())

    assert updated["amount"][0] is None
    assert [f["check"] for f in findings] == ["daily_bars_tdx_amount_unavailable"]


def test_the_window_is_walked_a_year_at_a_time():
    """Half a million rows is not one staging write."""
    assert bars._yearly_slices(date(2024, 3, 1), date(2026, 2, 1)) == [
        (date(2024, 3, 1), date(2024, 12, 31)),
        (date(2025, 1, 1), date(2025, 12, 31)),
        (date(2026, 1, 1), date(2026, 2, 1)),
    ]
