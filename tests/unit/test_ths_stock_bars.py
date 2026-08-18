"""同花顺 per-stock bars — the deep-history source.

What matters here is not the parsing but the two invariants that make older bars
safe to store next to the ones already in the lake: the request must be for
unadjusted prices, and a hfq series derived from raw × factor must run continuous
across the join with no phantom return.
"""

from __future__ import annotations

from datetime import date

import pytest

from cnequity.adapters.ths.stock_bars import (
    _STOCK_KLINE_URL,
    _parse_stock_kline,
    fetch_stock_bars,
)


def test_url_requests_unadjusted_prices():
    """`00` is raw; `01`/`02` are 同花顺's own qfq/hfq.

    Its adjustment convention differs from Sina's by up to ~340bps on
    ex-dividend days, so storing its adjusted prices would plant a break at every
    dividend. The lake derives hfq itself, from raw.
    """
    url = _STOCK_KLINE_URL.format(code="600519", part=2015)
    assert "/00/" in url
    assert "/01/" not in url and "/02/" not in url


def test_parses_a_kline_row():
    payload = {"data": "20150105,100.10,102.50,99.80,101.20,123456,987654321.00"}
    rows = _parse_stock_kline(payload, "600519.SH")
    assert len(rows) == 1
    r = rows[0]
    assert r["symbol"] == "600519.SH"
    assert r["trade_date"] == date(2015, 1, 5)
    assert (r["open"], r["high"], r["low"], r["close"]) == (100.10, 102.50, 99.80, 101.20)
    assert r["volume"] == 123456


def test_skips_malformed_and_short_rows():
    payload = {
        "data": (
            "bad;20150105,1,2,3,4,5,6;20150106,nan,2,3,4,5,6;"
            "20150107,1,2,3,4,1e300,6;2015,1,2,3,4,5,6;;"
        )
    }
    rows = _parse_stock_kline(payload, "600519.SH")
    # Only the well-formed finite 8-digit-stamp row survives.
    assert [r["trade_date"] for r in rows] == [date(2015, 1, 5)]


def test_empty_payload_is_not_an_error():
    assert _parse_stock_kline({"data": ""}, "600519.SH") == []
    assert _parse_stock_kline({}, "600519.SH") == []


def test_missing_year_is_skipped_not_fatal(monkeypatch):
    """A year file absent means the symbol had not listed — normal for a window
    that starts before the IPO, and it must not abort the fetch."""
    from cnequity.adapters.ths import stock_bars as mod

    def fake_get(url, *, config=None, timeout=20.0):
        if "2014" in url:
            raise mod.ThsNotFoundError("404")
        return 'x({"data":"20150105,1,2,3,4,5,6"})'

    monkeypatch.setattr(mod, "_get", fake_get)
    rows = fetch_stock_bars("600519.SH", date(2014, 1, 1), date(2015, 12, 31))
    assert [r["trade_date"] for r in rows] == [date(2015, 1, 5)]


def test_transport_error_is_not_treated_as_a_missing_year(monkeypatch):
    from cnequity.adapters.ths import stock_bars as mod

    monkeypatch.setattr(mod, "_get", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("500")))
    with pytest.raises(RuntimeError, match="500"):
        fetch_stock_bars("600519.SH", date(2015, 1, 1), date(2015, 12, 31))


def test_hfq_derived_from_raw_is_continuous_across_a_seam():
    """The invariant the backfill rests on.

    Splicing older bars in must not create a return at the join. With one factor
    convention on both sides and no corporate action at the seam, the hfq return
    across it has to equal the raw return — verified live at 0.0bps for 600519
    and 600036 over the 2015→2016 boundary; this pins the arithmetic.
    """
    # Two days either side of a seam, same factor (no action between them).
    factor = 7.04377
    last_old_raw, first_new_raw = 218.19, 210.02
    last_old_hfq = last_old_raw * factor
    first_new_hfq = first_new_raw * factor

    seam_hfq = first_new_hfq / last_old_hfq - 1.0
    seam_raw = first_new_raw / last_old_raw - 1.0
    assert seam_hfq == pytest.approx(seam_raw, abs=1e-12)


def test_a_factor_step_at_the_seam_is_a_real_action_not_a_break():
    """When the factor does change at the join, the hfq return is meant to differ
    from raw — that is the dividend being added back, not a discontinuity."""
    last_old_raw, first_new_raw = 100.0, 98.0
    f_before, f_after = 7.0, 7.2  # ex-dividend between the two days

    seam_hfq = (first_new_raw * f_after) / (last_old_raw * f_before) - 1.0
    seam_raw = first_new_raw / last_old_raw - 1.0
    assert seam_hfq > seam_raw  # the payout offsets part of the price drop


def _plan(tmp_path, rows, start=date(2001, 1, 1), end=date(2015, 12, 31), symbols=None):
    """Build a history plan against a throwaway instruments table."""
    import polars as pl

    from cnequity.config import Config
    from cnequity.steps import bars as mod

    root = tmp_path / "curated" / "instruments"
    root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(root / "part-000.parquet")
    cfg = Config(data_root=tmp_path)  # curated_root derives as data_root/curated
    syms = symbols or [r["symbol"] for r in rows]
    import cnequity.steps.bars as b

    orig = b.load_symbols
    b.load_symbols = lambda _c: syms
    try:
        return mod._history_plan(cfg, start, end)
    finally:
        b.load_symbols = orig


def test_plan_skips_etfs_and_symbols_listed_after_the_window(tmp_path):
    """ETF/LOF factors are not reliable, so deeper raw bars could never be
    served as hfq; a 2016 IPO has no pre-2016 history to fetch."""
    rows = [
        {"symbol": "600519.SH", "list_date": date(2001, 8, 27), "asset_type": "stock"},
        {"symbol": "510300.SH", "list_date": date(2012, 5, 4), "asset_type": "etf"},
        {"symbol": "301000.SZ", "list_date": date(2021, 6, 1), "asset_type": "stock"},
    ]
    plan = _plan(tmp_path, rows)
    assert [s for s, _ in plan] == ["600519.SH"]


def test_plan_starts_at_the_listing_year(tmp_path):
    """Fetching a symbol's pre-IPO years is thousands of empty requests."""
    rows = [
        {"symbol": "600519.SH", "list_date": date(2001, 8, 27), "asset_type": "stock"},
        {"symbol": "601969.SH", "list_date": date(2014, 6, 1), "asset_type": "stock"},
    ]
    plan = dict(_plan(tmp_path, rows))
    assert plan["600519.SH"] == date(2001, 1, 1)  # listed before the window
    assert plan["601969.SH"] == date(2014, 1, 1)  # trimmed to its listing year


def test_plan_keeps_stocks_with_unknown_listing_date(tmp_path):
    """A missing list_date must widen the window, not drop the symbol — the
    conservative direction, since the alternative silently loses real history."""
    rows = [{"symbol": "600000.SH", "list_date": None, "asset_type": "stock"}]
    plan = dict(_plan(tmp_path, rows))
    assert plan["600000.SH"] == date(2001, 1, 1)


def test_calendar_derives_sessions_from_bars_and_closes_the_rest(tmp_path):
    """Before the seed's 2016 start, bars are the only evidence of a session.

    `_is_trading_day` alone would keep every weekday, marking 春节 and 国庆
    sessions and inflating 2001-2008 to ~261 days against an actual ~243. Inside
    the bar era a date with no bar anywhere is a closed market.
    """
    import polars as pl

    from cnequity.adapters.calendar.exchange_calendar import build_trading_calendar

    root = tmp_path / "curated" / "daily_bars"
    root.mkdir(parents=True)
    # Sessions around 春节 2010, which fell on 2010-02-15..19 (all weekdays).
    # The bar era must start at or before the queried window, mirroring
    # production where bars reach 2001 and the query starts later.
    sessions = [date(2010, 2, 10), date(2010, 2, 11), date(2010, 2, 12), date(2010, 2, 22)]
    pl.DataFrame({"symbol": ["600519.SH"] * len(sessions), "trade_date": sessions}).write_parquet(
        root / "part-000.parquet"
    )

    cal = build_trading_calendar(
        date(2010, 2, 10), date(2010, 2, 24), curated_root=tmp_path / "curated"
    )
    trading = set(cal.filter(pl.col("is_trading"))["trade_date"].to_list())
    assert trading == set(sessions)
    # 春节 weekdays carry no bar, so they are closed rather than sessions —
    # which is exactly what `_is_trading_day` alone would get wrong.
    assert date(2010, 2, 17) not in trading


def test_seed_still_wins_inside_its_own_range(tmp_path):
    """A stray bar row must not flip a known holiday to a trading day."""
    import polars as pl

    from cnequity.adapters.calendar.exchange_calendar import build_trading_calendar

    root = tmp_path / "curated" / "daily_bars"
    root.mkdir(parents=True)
    # 2024-10-01 is National Day: closed in the seed, but plant a bar on it.
    pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [date(2024, 10, 1)]}).write_parquet(
        root / "part-000.parquet"
    )

    cal = build_trading_calendar(
        date(2024, 9, 30), date(2024, 10, 2), curated_root=tmp_path / "curated"
    )
    row = cal.filter(pl.col("trade_date") == date(2024, 10, 1))
    assert not bool(row["is_trading"][0])


def test_index_map_excludes_the_wrong_csi1000_series():
    """000852.SH must stay out until a code is value-checked.

    Both `399852` and `1B0852` return data, but it closes 2015 at 10614 against
    中证1000's actual ~8378 and starts in 2005 though the index was published in
    2014 — a different series wearing the code. A benchmark that is quietly wrong
    corrupts every excess-return number computed against it, so absence is the
    safer state.
    """
    from cnequity.adapters.ths.index_bars import INDEX_CODE_MAP

    assert "000852.SH" not in INDEX_CODE_MAP
    assert "399852" not in INDEX_CODE_MAP.values()
    assert "1B0852" not in INDEX_CODE_MAP.values()


def test_index_kline_rows_carry_frequency():
    """`index_bars` keys on frequency; the adapter supplies it rather than
    leaving each caller to remember."""
    from cnequity.adapters.ths.index_bars import _parse_index_kline

    rows = _parse_index_kline(
        {
            "data": (
                "20151231,3700.00,3740.00,3690.00,3731.00,123456,9876543210.00;"
                "20160104,nan,3740.00,3690.00,3731.00,123456,9876543210.00;"
                "20160105,3700.00,3740.00,3690.00,3731.00,1e300,9876543210.00"
            )
        },
        "000300.SH",
    )
    assert len(rows) == 1
    assert rows[0]["frequency"] == "1d"
    assert rows[0]["close"] == 3731.00


def test_index_symbol_without_a_code_raises():
    """An unmapped symbol is a caller error — silently fetching nothing would
    read as 'this index has no history'."""
    from cnequity.adapters.ths.index_bars import fetch_index_bars_history

    with pytest.raises(KeyError, match="no 同花顺 index code"):
        fetch_index_bars_history("000852.SH", date(2010, 1, 1), date(2010, 12, 31))
