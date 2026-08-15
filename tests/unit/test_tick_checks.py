from datetime import date, datetime

import polars as pl
import pytest

from cn_market_lake.config import Config
from cn_market_lake.quality.tick_checks import (
    DATASET,
    daily_reconciliation_findings,
    direction_findings,
    off_session_findings,
    seq_gap_findings,
    trade_date_mismatch_findings,
    trade_ticks_findings,
    truncation_findings,
)

DAY = date(2026, 7, 31)
START, END = date(2026, 7, 24), DAY


def _rows(symbol: str = "600519.SH", count: int = 10, day: date = DAY, **over) -> list[dict]:
    """A tidy session: the 09:25 auction, then continuous-trading records."""
    out = []
    for seq in range(count):
        clock = (9, 25) if seq == 0 else (10, min(59, seq))
        out.append(
            {
                "symbol": symbol,
                "trade_date": day,
                "tick_seq": seq,
                "trade_time": datetime(day.year, day.month, day.day, *clock),
                "price": 100.0,
                "volume": 100,
                "direction": "neutral" if seq == 0 else ("buy" if seq % 2 else "sell"),
            }
        )
    for row in out:
        row.update(over)
    return out


def _frame(rows: list[dict]) -> pl.LazyFrame:
    return pl.DataFrame(rows).lazy()


def _checks(findings: list[dict]) -> set[str]:
    return {f["check"] for f in findings}


def test_a_tidy_session_raises_nothing():
    lf = _frame(_rows())
    assert seq_gap_findings(lf, START, END) == []
    assert off_session_findings(lf, START, END) == []
    assert trade_date_mismatch_findings(lf, START, END) == []
    assert truncation_findings(lf, START, END) == []
    assert direction_findings(lf, START, END) == []


def test_a_hole_in_tick_seq_is_an_error():
    rows = _rows(count=5)
    del rows[2]  # 0,1,3,4 over 4 rows — max is 4, not 3
    findings = seq_gap_findings(_frame(rows), START, END)
    assert [f["severity"] for f in findings] == ["error"]
    assert "not a dense 0..n-1 run" in findings[0]["message"]


def test_duplicate_seq_numbers_are_an_error():
    rows = _rows(count=4)
    rows[3]["tick_seq"] = 2
    findings = seq_gap_findings(_frame(rows), START, END)
    assert [f["check"] for f in findings] == ["trade_ticks_seq_gaps"]


def test_a_sequence_not_starting_at_zero_is_an_error():
    rows = _rows(count=4)
    for row in rows:
        row["tick_seq"] += 1
    assert seq_gap_findings(_frame(rows), START, END)


@pytest.mark.parametrize("clock", [(9, 24), (12, 15), (15, 2), (15, 31)])
def test_records_outside_the_windows_are_an_error(clock):
    rows = _rows(count=3)
    rows[2]["trade_time"] = datetime(DAY.year, DAY.month, DAY.day, *clock)
    findings = off_session_findings(_frame(rows), START, END)
    assert [f["severity"] for f in findings] == ["error"]


@pytest.mark.parametrize("clock", [(9, 25), (11, 30), (13, 0), (15, 0), (15, 30)])
def test_the_four_windows_are_all_legal(clock):
    rows = _rows(count=3)
    rows[2]["trade_time"] = datetime(DAY.year, DAY.month, DAY.day, *clock)
    assert off_session_findings(_frame(rows), START, END) == []


def test_a_partition_date_disagreeing_with_the_timestamp_is_an_error():
    rows = _rows(count=3)
    rows[1]["trade_time"] = datetime(2026, 7, 30, 10, 0)
    findings = trade_date_mismatch_findings(_frame(rows), START, END)
    assert [f["severity"] for f in findings] == ["error"]


def test_sessions_missing_their_auction_warn():
    # Every session starts at 10:00 — the shape a backwards walk that stopped
    # short leaves behind.
    rows = []
    for i in range(10):
        session = _rows(symbol=f"00000{i}.SZ", count=3)
        for row in session:
            row["trade_time"] = datetime(DAY.year, DAY.month, DAY.day, 10, 0)
        rows.extend(session)
    findings = truncation_findings(_frame(rows), START, END)
    assert [f["severity"] for f in findings] == ["warning"]
    assert "do not open with the 09:25 auction" in findings[0]["message"]


def test_a_single_late_opener_is_not_worth_reporting():
    # One name that first trades at 10:00 among twenty is a market fact.
    rows = []
    for i in range(20):
        session = _rows(symbol=f"0000{i:02d}.SZ", count=3)
        if i == 0:
            for row in session:
                row["trade_time"] = datetime(DAY.year, DAY.month, DAY.day, 10, 0)
        rows.extend(session)
    assert truncation_findings(_frame(rows), START, END) == []


def test_an_unknown_direction_code_warns():
    rows = _rows(count=4)
    rows[3]["direction"] = "unknown"
    findings = direction_findings(_frame(rows), START, END)
    assert [f["severity"] for f in findings] == ["warning"]
    assert "does not recognise" in findings[0]["message"]


def test_an_impossible_buy_sell_split_warns():
    rows = _rows(count=21)
    for row in rows[1:]:
        row["direction"] = "buy"
    findings = direction_findings(_frame(rows), START, END)
    assert any("buy share" in f["message"] for f in findings)


def test_after_hours_rows_do_not_count_toward_the_split():
    rows = _rows(count=21)
    for row in rows[1:]:
        row["direction"] = "after_hours"
    assert direction_findings(_frame(rows), START, END) == []


def _lake_with_daily(tmp_path, tick_rows: list[dict], daily: list[dict]) -> Config:
    config = Config(data_root=tmp_path)
    root = config.curated_root / "daily_bars" / f"trade_date={DAY}"
    root.mkdir(parents=True)
    pl.DataFrame(daily).write_parquet(root / "part-0.parquet")
    return config


def test_reconciliation_excludes_after_hours_rows(tmp_path):
    # 25 symbols, each 1,000 shares of real volume plus a 100-share after-hours
    # print the exchange's daily total does not count. Including it would put
    # the ratio at 1.1 and trip the band.
    ticks: list[dict] = []
    daily: list[dict] = []
    for i in range(25):
        symbol = f"6000{i:02d}.SH"
        session = _rows(symbol=symbol, count=2)
        session[0]["volume"] = 1000
        session[1]["volume"] = 100
        session[1]["direction"] = "after_hours"
        session[1]["trade_time"] = datetime(DAY.year, DAY.month, DAY.day, 15, 10)
        ticks.extend(session)
        daily.append(
            {
                "symbol": symbol,
                "trade_date": DAY,
                "volume": 1000,
                "amount": 100_000.0,
                "data_version": "v2",
            }
        )
    config = _lake_with_daily(tmp_path, ticks, daily)
    findings = daily_reconciliation_findings(config, _frame(ticks), START, END)
    assert findings == []


def test_reconciliation_flags_a_unit_slip(tmp_path):
    ticks: list[dict] = []
    daily: list[dict] = []
    for i in range(25):
        symbol = f"6000{i:02d}.SH"
        session = _rows(symbol=symbol, count=1)
        session[0]["volume"] = 10  # lots left unconverted: 100x short
        ticks.extend(session)
        daily.append(
            {
                "symbol": symbol,
                "trade_date": DAY,
                "volume": 1000,
                "amount": 100_000.0,
                "data_version": "v2",
            }
        )
    config = _lake_with_daily(tmp_path, ticks, daily)
    findings = daily_reconciliation_findings(config, _frame(ticks), START, END)
    assert any(f["metric"] == "volume" for f in findings)
    assert all(f["severity"] == "warning" for f in findings)


def test_too_few_comparable_days_is_reported_not_skipped(tmp_path):
    ticks = _rows(count=2)
    daily = [
        {
            "symbol": "600519.SH",
            "trade_date": DAY,
            "volume": 200,
            "amount": 20_000.0,
            "data_version": "v2",
        }
    ]
    config = _lake_with_daily(tmp_path, ticks, daily)
    findings = daily_reconciliation_findings(config, _frame(ticks), START, END)
    assert [f["severity"] for f in findings] == ["info"]


def test_findings_are_empty_when_the_dataset_was_never_used(tmp_path):
    assert trade_ticks_findings(Config(data_root=tmp_path), DAY) == []


def test_findings_run_end_to_end_over_a_written_lake(tmp_path):
    config = Config(data_root=tmp_path)
    root = config.curated_root / DATASET / f"trade_date={DAY}"
    root.mkdir(parents=True)
    rows = _rows(count=6)
    rows[4]["trade_time"] = datetime(DAY.year, DAY.month, DAY.day, 12, 30)
    pl.DataFrame(rows).write_parquet(root / "part-0.parquet")
    assert "trade_ticks_off_session" in _checks(trade_ticks_findings(config, DAY))
