"""A security listed before the universe carried it still owes those sessions.

Measured on the production lake 2026-09-17: thirteen BSE securities listed
between 07-22 and 09-04 all took their first bar on 09-07, short 366 sessions
between them, every one of which TDX still served. Nothing asked for them
again — the watermark had moved past, and the interior-gap ledger never saw
them because they were never expected.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.steps import bars
from cnequity.storage.layout import init_data_layout
from cnequity.storage.state import StateStore

LISTED = date(2026, 8, 12)
ADMITTED = date(2026, 9, 7)
END = date(2026, 9, 17)
SESSIONS = [date(2026, 8, d) for d in (12, 13, 14)] + [date(2026, 9, d) for d in (7, 8, 17)]


@pytest.fixture
def lake(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake")
    init_data_layout(cfg)
    monkeypatch.setattr(
        bars, "list_trading_dates", lambda _c, lo, hi: [d for d in SESSIONS if lo <= d <= hi]
    )
    return cfg


def _write_bars(cfg, rows: list[tuple[str, date]]) -> None:
    root = cfg.curated_root / "daily_bars"
    for symbol, day in rows:
        part = root / f"trade_date={day.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"symbol": [symbol], "trade_date": [day]}).write_parquet(
            part / f"{symbol}.parquet"
        )


def _spans(monkeypatch, spans: dict):
    monkeypatch.setattr(bars, "_instrument_spans", lambda _c: spans)


def test_the_sessions_before_admission_land_on_the_ledger(lake, monkeypatch):
    _spans(monkeypatch, {"920138.BJ": (LISTED, None, "stock")})
    _write_bars(lake, [("920138.BJ", ADMITTED), ("920138.BJ", END)])

    assert bars._record_late_admissions(lake, "r1", ["920138.BJ"], END) == 3

    owed = StateStore(lake.meta_root).get_outstanding_keys("daily_bars")
    assert [r["trade_date"] for r in owed] == ["2026-08-12", "2026-08-13", "2026-08-14"]
    assert {r["reason"] for r in owed} == {"late_admission"}


def test_a_security_with_no_bar_at_all_owes_from_its_listing(lake, monkeypatch):
    """It may have been admitted today and simply not swept yet."""
    _spans(monkeypatch, {"601091.SH": (date(2026, 9, 8), None, "stock")})
    _write_bars(lake, [("000001.SZ", END)])

    assert bars._record_late_admissions(lake, "r1", ["601091.SH"], END) == 2


def test_a_security_carried_from_its_first_session_owes_nothing(lake, monkeypatch):
    _spans(monkeypatch, {"920138.BJ": (LISTED, None, "stock")})
    _write_bars(lake, [("920138.BJ", LISTED), ("920138.BJ", END)])

    assert bars._record_late_admissions(lake, "r1", ["920138.BJ"], END) == 0


def test_a_quote_code_the_dataset_does_not_carry_is_not_a_debt(lake, monkeypatch):
    """`instruments` also lists ETF and LOF codes. A debt no repair can pay off
    sits at the same weight as a real one and drowns it."""
    _spans(monkeypatch, {"530060.SH": (LISTED, None, "fund")})
    _write_bars(lake, [("000001.SZ", END)])

    assert bars._record_late_admissions(lake, "r1", ["530060.SH"], END) == 0


def test_a_listing_older_than_the_lookback_is_left_alone(lake, monkeypatch):
    """Otherwise every security predating the lake's horizon reads as owed."""
    old = END - bars.timedelta(days=bars._LATE_ADMISSION_LOOKBACK_DAYS + 1)
    _spans(monkeypatch, {"600000.SH": (old, None, "stock")})
    _write_bars(lake, [("600000.SH", END)])

    assert bars._record_late_admissions(lake, "r1", ["600000.SH"], END) == 0


def test_an_empty_dataset_is_not_a_universe_wide_debt(lake, monkeypatch):
    _spans(monkeypatch, {"920138.BJ": (LISTED, None, "stock")})
    assert bars._record_late_admissions(lake, "r1", ["920138.BJ"], END) == 0


def test_a_scoped_repair_reaches_the_whole_window_it_asked_for(lake, monkeypatch):
    """The Beijing backstop truncates to the daily reconciliation lookback,
    which guards a 580-symbol board. Applied to an explicit `--symbols` repair
    it left fourteen securities owing 225 sessions the repair kept declining to
    fetch."""
    lake.bj_history_lookback_days = 1
    lake._backfill_symbols = ["920138.BJ"]
    assert bars._bj_history_start(lake, SESSIONS[0], SESSIONS[-1]) == SESSIONS[0]


def test_a_board_wide_sweep_still_gets_the_lookback(lake, monkeypatch):
    """The scope is what bounds the cost; past that the guard is the point."""
    lake.bj_history_lookback_days = 1
    lake._backfill_symbols = [
        f"9200{i:02d}.BJ" for i in range(bars._BJ_SCOPED_WINDOW_MAX_SYMBOLS + 1)
    ]
    assert bars._bj_history_start(lake, SESSIONS[0], SESSIONS[-1]) == SESSIONS[-1]


def test_an_unscoped_run_is_untouched(lake, monkeypatch):
    lake.bj_history_lookback_days = 1
    assert bars._bj_history_start(lake, SESSIONS[0], SESSIONS[-1]) == SESSIONS[-1]
