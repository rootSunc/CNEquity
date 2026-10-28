"""delist_date is the last listed session, not baostock's removal date."""

from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.storage.repairs.delist_dates import last_listed_sessions

SESSIONS = [date(2026, 6, 5), date(2026, 6, 8), date(2026, 6, 9), date(2026, 6, 10)]


def _calendar(monkeypatch):
    monkeypatch.setattr(
        "cnequity.query.calendar.list_trading_dates",
        lambda config, start, end, include_staging=False: [
            day for day in SESSIONS if start <= day <= end
        ],
    )


def test_removal_maps_to_the_previous_session(tmp_path, monkeypatch):
    _calendar(monkeypatch)

    sessions = last_listed_sessions(
        Config(data_root=tmp_path),
        {
            "688287.SH": date(2026, 6, 10),
            "000001.SZ": date(2026, 6, 8),  # Monday: the Friday before
            "000002.SZ": date(2026, 6, 6),  # weekend removal
        },
    )

    assert sessions == {
        "688287.SH": date(2026, 6, 9),
        "000001.SZ": date(2026, 6, 5),
        "000002.SZ": date(2026, 6, 5),
    }


def test_removal_before_the_calendar_is_left_out(tmp_path, monkeypatch):
    _calendar(monkeypatch)

    assert last_listed_sessions(Config(data_root=tmp_path), {"000003.SZ": date(2002, 6, 14)}) == {}


def test_baostock_merge_writes_the_last_listed_session(tmp_path, monkeypatch):
    from cnequity.steps.reference import _merge_delisted_instruments

    _calendar(monkeypatch)
    monkeypatch.setattr(
        "cnequity.steps.delisted.write_delisted_identity_evidence", lambda *a, **k: None
    )
    cfg = Config(data_root=tmp_path / "data")
    cfg.sources["baostock"] = True
    live = pl.DataFrame({"symbol": ["600519.SH"], "list_date": [date(2001, 8, 27)]})
    basics = pl.DataFrame(
        {
            "symbol": ["688287.SH"],
            "name": ["退市观典"],
            "exchange": ["SH"],
            "asset_type": ["stock"],
            "list_date": [date(2022, 5, 25)],
            "delist_date": [date(2026, 6, 10)],
            "prev_symbol": [None],
        },
        schema_overrides={"prev_symbol": pl.Utf8},
    )

    merged = _merge_delisted_instruments(cfg, live, basics_source=lambda: basics)

    row = merged.filter(pl.col("symbol") == "688287.SH")
    assert row["delist_date"].to_list() == [date(2026, 6, 9)]


def test_only_rows_after_delisting_are_planned():
    from cnequity.storage.repairs.bar_listing_window import plan_bar_listing_window

    rows = pl.DataFrame(
        {
            "symbol": ["688287.SH", "688287.SH", "600519.SH"],
            "trade_date": [date(2026, 6, 9), date(2026, 6, 10), date(2026, 6, 10)],
            "volume": [0, 0, 100],
        }
    )
    spans = pl.DataFrame(
        {"symbol": ["688287.SH", "600519.SH"], "delist_date": [date(2026, 6, 9), None]},
        schema_overrides={"delist_date": pl.Date},
    )

    plan = plan_bar_listing_window(rows, spans)

    assert plan.select("symbol", "trade_date").rows() == [("688287.SH", date(2026, 6, 10))]
