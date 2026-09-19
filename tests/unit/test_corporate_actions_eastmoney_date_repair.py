"""The scoped EastMoney repair for named historical ex-dates.

The backfill path's primary source is TDX, symbol by symbol; EastMoney only
ever rides along as a peer snapshot. Its own report reaches back to 1991, but
only through the daily equality filter — the backfill floor (2015-09-29) put
everything older out of reach, so 20 arbitration-confirmed gaps from 2001-2006
could be seen in the source and never fetched.
"""

from datetime import date

import polars as pl
import pytest

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.steps import events


def _row(symbol: str, ex_date: date) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [symbol],
            "ex_date": [ex_date],
            "action_type": ["cash_dividend"],
            "cash_dividend": [0.1],
            "bonus_ratio": [0.0],
            "transfer_ratio": [0.0],
            "allotment_ratio": [None],
            "allotment_price": [None],
        },
        schema_overrides={"allotment_ratio": pl.Float64, "allotment_price": pl.Float64},
    )


def _config(tmp_path) -> Config:
    cfg = Config(
        data_root=tmp_path / "lake",
        sources={"eastmoney": True},
        raw_archive_enabled=False,
    )
    cfg._backfill = True
    return cfg


def test_named_ex_dates_are_fetched_and_no_symbol_sweep_runs(tmp_path, monkeypatch):
    """Asking for 18 dates must not cost a 5,500-symbol TDX walk."""
    cfg = _config(tmp_path)
    cfg._corporate_actions_eastmoney_date_repair = [date(2001, 4, 6), date(2002, 5, 31)]

    monkeypatch.setattr(events, "load_symbols", lambda _cfg: ["600110.SH", "000755.SZ"])

    def no_sweep(*args, **kwargs):
        raise AssertionError("the date repair must not sweep symbols through TDX")

    monkeypatch.setattr(events, "fetch_corporate_actions", no_sweep)
    asked: list[date] = []

    def fake_eastmoney(trade_date, **kwargs):
        asked.append(trade_date)
        assert kwargs["backfill"] is False, "history is only reachable by exact date"
        return _row("600110.SH" if trade_date.year == 2001 else "000755.SZ", trade_date)

    monkeypatch.setattr(events, "fetch_corporate_actions_eastmoney", fake_eastmoney)

    result = events.step_corporate_actions(cfg, date(2026, 9, 19), "run-1", {})

    assert asked == [date(2001, 4, 6), date(2002, 5, 31)]
    assert result["rows_written"] == 2
    staged = list((cfg.staging_root / "corporate_actions").glob("**/*.parquet"))
    rows = pl.concat([pl.read_parquet(path) for path in staged], how="diagonal_relaxed")
    assert sorted(rows["ex_date"].to_list()) == [date(2001, 4, 6), date(2002, 5, 31)]
    assert set(rows["source"].to_list()) == {"eastmoney"}


def test_a_date_outside_the_backfill_window_is_refused_before_any_request(tmp_path, monkeypatch):
    """The window guard downstream would reject the rows after paying for them."""
    cfg = _config(tmp_path)
    cfg._backfill_start = date(2005, 1, 1)
    cfg._backfill_end = date(2006, 12, 31)
    cfg._corporate_actions_eastmoney_date_repair = [date(2001, 4, 6)]

    monkeypatch.setattr(events, "load_symbols", lambda _cfg: [])
    monkeypatch.setattr(events, "fetch_corporate_actions", lambda *a, **k: pl.DataFrame())

    def never(*args, **kwargs):
        raise AssertionError("no request may be made for an out-of-window date")

    monkeypatch.setattr(events, "fetch_corporate_actions_eastmoney", never)

    with pytest.raises(RuntimeError, match="outside the backfill window"):
        events.step_corporate_actions(cfg, date(2026, 9, 19), "run-1", {})
