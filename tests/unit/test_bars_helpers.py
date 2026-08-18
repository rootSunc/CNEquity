"""Offline coverage for bars planning helpers."""

from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace

import polars as pl
import pytest

from cnequity.steps import bars


def test_backfill_window_defaults_and_overrides(tmp_path):
    cfg = SimpleNamespace(_backfill_start=None, _backfill_end=None)
    start, end = bars._backfill_window(cfg, date(2025, 1, 10))
    assert end == date(2025, 1, 10)
    assert start.year <= 2016

    cfg2 = SimpleNamespace(_backfill_start=date(2024, 1, 1), _backfill_end=date(2024, 6, 1))
    assert bars._backfill_window(cfg2, date(2025, 1, 10)) == (date(2024, 1, 1), date(2024, 6, 1))


def test_history_plan_filters_etf_placeholder_and_future_listings(tmp_path, monkeypatch):
    curated = tmp_path / "curated"
    inst = curated / "instruments" / "year=2025"
    inst.mkdir(parents=True)
    pl.DataFrame(
        [
            {"symbol": "600519.SH", "list_date": date(2001, 8, 27), "asset_type": "stock"},
            {"symbol": "510300.SH", "list_date": date(2012, 5, 28), "asset_type": "etf"},
            {"symbol": "589430.SH", "list_date": None, "asset_type": "etf"},
            {"symbol": "688001.SH", "list_date": date(2024, 6, 1), "asset_type": "stock"},
            {"symbol": "301001.SZ", "list_date": date(2026, 1, 1), "asset_type": "stock"},
            {"symbol": "920001.BJ", "list_date": date(2020, 1, 1), "asset_type": "stock"},
            {"symbol": "830001.BJ", "list_date": date(2020, 1, 1), "asset_type": "stock"},
        ]
    ).write_parquet(inst / "part.parquet")

    cfg = SimpleNamespace(curated_root=curated)
    monkeypatch.setattr(
        bars,
        "load_symbols",
        lambda config: [
            "600519.SH",
            "510300.SH",
            "589430.SH",
            "688001.SH",
            "301001.SZ",
            "920001.BJ",
            "830001.BJ",
        ],
    )
    plan = bars._history_plan(cfg, date(2020, 1, 1), date(2025, 1, 1))
    by_sym = dict(plan)
    assert "600519.SH" in by_sym
    assert by_sym["600519.SH"] == date(2020, 1, 1)
    assert "688001.SH" in by_sym
    assert by_sym["688001.SH"] == date(2024, 1, 1)  # listing year Jan 1
    assert "510300.SH" in by_sym
    assert by_sym["510300.SH"] == date(2020, 1, 1)  # etf now included
    assert "589430.SH" not in by_sym  # unlisted etf placeholder
    assert "301001.SZ" not in by_sym  # listed after window
    assert "920001.BJ" not in by_sym  # BJ prefix filtered
    assert "830001.BJ" not in by_sym  # legacy BJ code must be filtered too


def test_history_plan_without_instruments_falls_back(tmp_path, monkeypatch):
    cfg = SimpleNamespace(curated_root=tmp_path / "missing")
    monkeypatch.setattr(bars, "load_symbols", lambda config: ["600519.SH", "920001.BJ"])
    plan = bars._history_plan(cfg, date(2020, 1, 1), date(2025, 1, 1))
    assert plan == [("600519.SH", date(2020, 1, 1))]


def test_history_plan_dedupes_nested_instrument_fragments(tmp_path, monkeypatch):
    curated = tmp_path / "curated"
    root = curated / "instruments"
    root.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["688001.SH"],
            "list_date": [date(2020, 6, 1)],
            "asset_type": ["stock"],
            "fetched_at": [datetime(2024, 6, 1, tzinfo=timezone.utc)],
        }
    ).write_parquet(root / "part-merged.parquet")
    nested = root / ".old-fragments"
    nested.mkdir()
    pl.DataFrame(
        {
            "symbol": ["688001.SH"],
            "list_date": [date(2024, 6, 1)],
            "asset_type": ["stock"],
            "fetched_at": [datetime(2024, 6, 2, tzinfo=timezone.utc)],
        }
    ).write_parquet(nested / "part-old.parquet")

    cfg = SimpleNamespace(curated_root=curated)
    monkeypatch.setattr(bars, "load_symbols", lambda config: ["688001.SH"])

    plan = bars._history_plan(cfg, date(2020, 1, 1), date(2025, 1, 1))

    assert plan == [("688001.SH", date(2024, 1, 1))]


def test_sweep_stock_bars_planned_abort_streak(monkeypatch):
    plan = [(f"{i:06d}.SZ", date(2024, 1, 1)) for i in range(12)]
    calls = {"n": 0}

    def boom(symbol, start, end, *, config=None):
        calls["n"] += 1
        raise RuntimeError("down")

    monkeypatch.setattr("cnequity.adapters.ths.stock_bars.fetch_stock_bars", boom)
    batches = []
    failed = bars.sweep_stock_bars_planned(
        plan,
        date(2024, 1, 10),
        config=SimpleNamespace(),
        on_batch=lambda rows, codes: batches.append(list(codes)),
        batch_size=50,
    )
    assert len(failed) == 10  # aborts after 10 consecutive
    assert calls["n"] == 10


@pytest.mark.parametrize(
    "row",
    [
        {
            "symbol": "000001.SZ",
            "trade_date": date(2024, 1, 5),
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": 1,
            "amount": 1.0,
        },
        {
            "symbol": "600519.SH",
            "trade_date": date(2023, 12, 31),
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": 1,
            "amount": 1.0,
        },
    ],
)
def test_sweep_stock_bars_planned_rejects_out_of_scope_rows(monkeypatch, row):
    monkeypatch.setattr(
        "cnequity.adapters.ths.stock_bars.fetch_stock_bars",
        lambda symbol, start, end, *, config=None: [row],
    )
    batches = []
    failed = bars.sweep_stock_bars_planned(
        [("600519.SH", date(2024, 1, 1))],
        date(2024, 1, 31),
        config=SimpleNamespace(),
        on_batch=lambda rows, codes: batches.append((rows, codes)),
    )
    assert failed == ["600519.SH"]
    assert not any(rows for rows, _codes in batches)


def test_sweep_stock_bars_planned_rejects_empty_symbol_result(monkeypatch):
    monkeypatch.setattr(
        "cnequity.adapters.ths.stock_bars.fetch_stock_bars",
        lambda symbol, start, end, *, config=None: [],
    )
    batches = []
    failed = bars.sweep_stock_bars_planned(
        [("600519.SH", date(2001, 1, 1))],
        date(2015, 12, 31),
        config=SimpleNamespace(),
        on_batch=lambda rows, codes: batches.append((rows, codes)),
    )
    assert failed == ["600519.SH"]
    assert batches == [([], [])]


def test_daily_bars_no_trade_amount_is_exactly_zero():
    """A suspended day must store amount=0, not the decoder's denormal.

    TDX's packed-float decoder maps a raw zero to 2**-127 (~5.9e-39).
    ``int()`` hid it on ``volume``; ``amount`` is a float and kept it, so
    ``amount > 0`` came to mean "was quoted" rather than "traded".
    """
    from cnequity.adapters.tdx_protocol.bars import _parse_bar_rows

    denormal = 2.0**-127
    pdf = pl.DataFrame(
        [
            {
                "date": date(2024, 6, 28),
                "open": 12.5,
                "high": 12.5,
                "low": 12.5,
                "close": 12.5,
                "vol": denormal,
                "amount": denormal,
            }
        ]
    )
    rows = _parse_bar_rows(pdf, "600519.SH", date(2024, 6, 1), date(2024, 6, 30))
    assert rows[0]["volume"] == 0
    assert rows[0]["amount"] == 0.0


def test_daily_bars_real_quantities_are_untouched_by_the_zero_snap():
    from cnequity.adapters.tdx_protocol.bars import _parse_bar_rows

    pdf = pl.DataFrame(
        [
            {
                "date": date(2024, 6, 28),
                "open": 12.5,
                "high": 12.5,
                "low": 12.5,
                "close": 12.5,
                "vol": 400,
                "amount": 500_000.0,
            }
        ]
    )
    rows = _parse_bar_rows(pdf, "600519.SH", date(2024, 6, 1), date(2024, 6, 30))
    # 400 手 → 40,000 股 at the adapter boundary; amount unchanged.
    assert rows[0]["volume"] == 40_000
    assert rows[0]["amount"] == 500_000.0


def test_daily_bars_skip_invalid_date_rows_without_losing_valid_rows():
    from cnequity.adapters.tdx_protocol.bars import _parse_bar_rows

    pdf = pl.DataFrame(
        [
            {
                "date": "not-a-date",
                "open": 12.5,
                "high": 12.5,
                "low": 12.5,
                "close": 12.5,
                "vol": 100,
                "amount": 1_000.0,
            },
            {
                "date": "2024-06-28",
                "open": 12.5,
                "high": 12.5,
                "low": 12.5,
                "close": 12.5,
                "vol": 100,
                "amount": 1_000.0,
            },
        ]
    )
    rows = _parse_bar_rows(pdf, "600519.SH", date(2024, 6, 1), date(2024, 6, 30))
    assert [row["trade_date"] for row in rows] == [date(2024, 6, 28)]


def test_daily_bars_skip_int64_overflow_volume():
    from cnequity.adapters.tdx_protocol.bars import _parse_bar_rows

    pdf = pl.DataFrame(
        [
            {
                "date": date(2024, 6, 28),
                "open": 12.5,
                "high": 12.5,
                "low": 12.5,
                "close": 12.5,
                "vol": 1e300,
                "amount": 1_000.0,
            }
        ]
    )
    assert _parse_bar_rows(pdf, "600519.SH", date(2024, 6, 1), date(2024, 6, 30)) == []
