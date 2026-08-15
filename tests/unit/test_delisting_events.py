"""Ending-pattern classification — does a recovered series run through the 退市整理期?

The distinction decides whether a backtest realises a delisting's final loss or
marks the position at its last pre-suspension price. Fixtures mirror shapes
measured against real recovered series.
"""

from datetime import date, timedelta

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.query import load
from cn_market_lake.steps.delisted import classify_ending, write_delisting_events


def _series(closes: list[float], gaps: dict[int, int] | None = None) -> pl.DataFrame:
    """Bars with optional calendar gaps (index -> extra days) to model a halt."""
    gaps = gaps or {}
    day = date(2025, 1, 6)
    days = []
    for i in range(len(closes)):
        day += timedelta(days=1 + gaps.get(i, 0))
        days.append(day)
    return pl.DataFrame({"symbol": ["X.SH"] * len(closes), "trade_date": days, "close": closes})


def _grind(start: float, n: int, step: float) -> list[float]:
    out, px = [], start
    for _ in range(n):
        px *= 1 + step
        out.append(round(px, 2))
    return out


# --- classification ---------------------------------------------------------


def test_consolidation_period_is_detected(tmp_path):
    """600608 shape: 40-day halt, -90% resumption, then a short tail."""
    closes = _grind(3.0, 40, -0.01) + [0.20, 0.18, 0.17, 0.17, 0.18, 0.18, 0.17]
    df = _series(closes, gaps={40: 40})

    out = classify_ending(df)

    assert out["ending_pattern"] == "consolidation"
    assert out["halt_gap_days"] > 10
    assert out["worst_final_return"] < -0.8


def test_abrupt_decline_is_flagged_separately(tmp_path):
    """600355 shape: no halt, -5%/day grind to a sub-1-yuan close."""
    df = _series(_grind(2.2, 45, -0.05))

    out = classify_ending(df)

    assert out["ending_pattern"] == "abrupt_decline"
    assert out["final_close"] < 1.0
    assert out["worst_final_return"] > -0.1, "no crash day — that is the whole point"


def test_merger_ending_is_not_mistaken_for_a_collapse(tmp_path):
    """601989 shape: flat/positive tail at an ordinary price."""
    df = _series(_grind(4.6, 45, +0.002))

    out = classify_ending(df)

    assert out["ending_pattern"] == "abrupt_stable"
    assert out["final_window_return"] > 0


def test_a_halt_without_a_crash_is_not_a_consolidation(tmp_path):
    """A long suspension that resumes flat is an ordinary halt, not a delisting run."""
    df = _series(_grind(10.0, 45, 0.0), gaps={40: 45})

    out = classify_ending(df)

    assert out["halt_gap_days"] > 10
    assert out["ending_pattern"] == "abrupt_stable"


def test_too_short_to_classify(tmp_path):
    out = classify_ending(_series([1.0, 1.1, 1.2]))

    assert out["ending_pattern"] == "insufficient"
    assert out["final_close"] is None
    assert out["bars"] == 3


# --- persistence ------------------------------------------------------------


def _event(symbol: str, pattern: str, last: date) -> dict:
    return {
        "symbol": symbol,
        "first_trade_date": date(2016, 1, 4),
        "last_trade_date": last,
        "ending_pattern": pattern,
        "final_close": 0.5,
        "halt_gap_days": 30,
        "worst_final_return": -0.9,
        "final_window_return": -0.95,
        "bars": 2000,
    }


def test_events_are_queryable_through_load(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    write_delisting_events(
        cfg,
        [
            _event("600608.SH", "consolidation", date(2026, 6, 29)),
            _event("600355.SH", "abrupt_decline", date(2026, 4, 3)),
        ],
    )

    df = load("delisting_events", config=cfg)

    assert set(df["symbol"]) == {"600608.SH", "600355.SH"}
    suspect = df.filter(pl.col("ending_pattern") == "abrupt_decline")
    assert suspect["symbol"].to_list() == ["600355.SH"], "the bucket needing scrutiny is filterable"


def test_rerunning_updates_rather_than_duplicates(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    write_delisting_events(cfg, [_event("600608.SH", "abrupt_decline", date(2026, 6, 29))])
    write_delisting_events(cfg, [_event("600608.SH", "consolidation", date(2026, 6, 29))])

    df = load("delisting_events", config=cfg)

    assert df.height == 1
    assert df["ending_pattern"][0] == "consolidation", "the later classification wins"


def test_date_window_filters_on_last_trade_date(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    write_delisting_events(
        cfg,
        [
            _event("600001.SH", "consolidation", date(2009, 12, 15)),
            _event("600608.SH", "consolidation", date(2026, 6, 29)),
        ],
    )

    df = load("delisting_events", start="2016-01-01", config=cfg)

    assert df["symbol"].to_list() == ["600608.SH"]
