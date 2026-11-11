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
    return bars.amounts._supplement_bj_amounts_from_tdx(cfg, frame, start=DAY, end=DAY)


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


def test_a_sina_sh_row_uses_the_same_gate(cfg, monkeypatch):
    """沪深新浪历史行和北交所走同一条核对：价格一致才补成交额，成交量留原值。"""
    _tdx(monkeypatch, symbol="600005.SH")
    updated, _ = _run(cfg, _stored(symbol="600005.SH"))

    assert updated["amount"][0] == 1_296_000.0
    assert updated["volume"][0] == 123_456.0
    assert updated["source"][0] == "tdx_protocol"


def test_sina_history_repair_asks_tdx_only_for_null_sina_rows(tmp_path, monkeypatch):
    from datetime import datetime, timezone

    from cnequity.query import load
    from cnequity.steps.finalize import step_compact
    from cnequity.storage.revisions import RevisionStore

    day = date(2015, 1, 5)
    fetched = datetime(2026, 7, 23, tzinfo=timezone.utc)
    lake = Config(data_root=tmp_path)
    path = lake.curated_root / f"daily_bars/trade_date={day}/part-merged.parquet"
    path.parent.mkdir(parents=True)

    def row(symbol, source, amount, volume=1000):
        return {
            "symbol": symbol,
            "trade_date": day,
            "open": 10.0,
            "high": 11.0,
            "low": 9.5,
            "close": 10.5,
            "volume": volume,
            "amount": amount,
            "source": source,
            "data_version": "v2",
            "fetched_at": fetched,
        }

    pl.DataFrame(
        [
            row("600005.SH", "sina", None),
            row("600270.SH", "ths", None),
            row("000001.SZ", "sina", 80_000.0),
            row("920001.BJ", "sina", None),
        ],
        schema_overrides={"amount": pl.Float64},
    ).write_parquet(path)
    RevisionStore(lake.meta_root, lake.curated_root).commit(
        "daily_bars",
        run_id="seed",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    asked: list[list[str]] = []

    def fetch(symbols, start, end, **_kwargs):
        asked.append(list(symbols))
        return pl.DataFrame(
            [
                {
                    "symbol": "600005.SH",
                    "trade_date": day,
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.5,
                    "close": 10.5,
                    "volume": 1000,
                    "amount": 50_000.0,
                }
            ]
        )

    monkeypatch.setattr("cnequity.adapters.tdx_protocol.client.fetch_daily_bars", fetch)
    result = bars.repair_sina_history_amounts_from_tdx(lake, day, day, "repair", None)

    assert asked == [["600005.SH", "920001.BJ"]]
    assert result["rows_written"] == 1
    step_compact(lake, day, "repair", {})
    # The fixture's Beijing row predates the exchange; read the lake as stored.
    stored = {
        r["symbol"]: r
        for r in load("daily_bars", config=lake, include_otc=True).iter_rows(named=True)
    }
    assert stored["600005.SH"]["amount"] == 50_000.0
    assert stored["600005.SH"]["source"] == "tdx_protocol"
    assert stored["600005.SH"]["volume"] == 1000
    assert stored["600270.SH"]["source"] == "ths"
    assert stored["600270.SH"]["amount"] is None
    assert stored["000001.SZ"]["amount"] == 80_000.0
    assert stored["920001.BJ"]["amount"] is None
    assert stored["920001.BJ"]["source"] == "sina"


def test_the_window_is_walked_a_year_at_a_time():
    """Half a million rows is not one staging write."""
    assert bars._base._yearly_slices(date(2024, 3, 1), date(2026, 2, 1)) == [
        (date(2024, 3, 1), date(2024, 12, 31)),
        (date(2025, 1, 1), date(2025, 12, 31)),
        (date(2026, 1, 1), date(2026, 2, 1)),
    ]
