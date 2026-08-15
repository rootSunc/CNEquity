from datetime import date

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.quality.cross_checks import (
    daily_bars_calendar_findings,
    valuation_bars_coverage_findings,
)


def _write_calendar(root, rows):
    base = root / "curated" / "trading_calendar"
    for d, is_trading in rows:
        part = base / f"trade_date={d.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"trade_date": [d], "is_trading": [is_trading]}).write_parquet(
            part / "part.parquet"
        )


def _write_daily(root, dataset, rows):
    """rows: list of (symbol, date)."""
    base = root / "curated" / dataset
    by_day: dict[date, list[str]] = {}
    for sym, d in rows:
        by_day.setdefault(d, []).append(sym)
    for d, syms in by_day.items():
        part = base / f"trade_date={d.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"symbol": syms, "trade_date": [d] * len(syms)}).write_parquet(
            part / "part.parquet"
        )


# --- daily_bars × calendar --------------------------------------------------


def test_daily_bars_clean_when_every_trading_day_has_bars(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    days = [date(2024, 6, 3), date(2024, 6, 4), date(2024, 6, 5)]
    _write_calendar(cfg.data_root, [(d, True) for d in days])
    _write_daily(cfg.data_root, "daily_bars", [(s, d) for d in days for s in ("A", "B")])

    assert daily_bars_calendar_findings(cfg, date(2024, 6, 5)) == []


def test_daily_bars_flags_market_wide_missing_day(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    days = [date(2024, 6, 3), date(2024, 6, 4), date(2024, 6, 5)]
    _write_calendar(cfg.data_root, [(d, True) for d in days])
    # Interior trading day 06-04 has no bars from any symbol.
    _write_daily(cfg.data_root, "daily_bars", [("A", days[0]), ("A", days[2])])

    findings = daily_bars_calendar_findings(cfg, date(2024, 6, 5))
    assert len(findings) == 1
    assert findings[0]["check"] == "daily_bars_calendar_missing_day"
    assert findings[0]["severity"] == "error"
    assert findings[0]["missing_sample"] == ["2024-06-04"]


def test_daily_bars_flags_orphan_bar_on_non_trading_day(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _write_calendar(
        cfg.data_root,
        [(date(2024, 6, 3), True), (date(2024, 6, 4), False), (date(2024, 6, 5), True)],
    )
    _write_daily(
        cfg.data_root,
        "daily_bars",
        [("A", date(2024, 6, 3)), ("A", date(2024, 6, 4)), ("A", date(2024, 6, 5))],
    )

    findings = daily_bars_calendar_findings(cfg, date(2024, 6, 5))
    assert len(findings) == 1
    assert findings[0]["check"] == "daily_bars_calendar_orphan"
    assert findings[0]["severity"] == "error"
    assert findings[0]["orphan_sample"] == ["2024-06-04"]


def test_daily_bars_suspension_gap_is_not_flagged(tmp_path):
    # A single stock missing on a trading day (suspension) must NOT trip the
    # market-wide check as long as some symbol has a bar that day.
    cfg = Config(data_root=tmp_path / "data")
    days = [date(2024, 6, 3), date(2024, 6, 4), date(2024, 6, 5)]
    _write_calendar(cfg.data_root, [(d, True) for d in days])
    rows = [(s, d) for d in days for s in ("A", "B")]
    rows.remove(("B", days[1]))  # B suspended on 06-04, A still trades
    _write_daily(cfg.data_root, "daily_bars", rows)

    assert daily_bars_calendar_findings(cfg, date(2024, 6, 5)) == []


def test_daily_bars_empty_lake_no_findings(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    assert daily_bars_calendar_findings(cfg, date(2024, 6, 5)) == []


# --- valuation × bars -------------------------------------------------------


def test_valuation_clean_when_covered(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    day = date(2024, 6, 5)
    _write_daily(cfg.data_root, "daily_bars", [("A", day), ("B", day)])
    _write_daily(cfg.data_root, "valuation_metrics", [("A", day), ("B", day)])

    assert valuation_bars_coverage_findings(cfg, day) == []


def test_valuation_flags_symbol_with_no_bars_anywhere(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    day = date(2024, 6, 5)
    _write_daily(cfg.data_root, "daily_bars", [("A", day), ("B", day)])
    # DELISTED has valuation but never a bar.
    _write_daily(cfg.data_root, "valuation_metrics", [("A", day), ("B", day), ("DELISTED", day)])

    findings = valuation_bars_coverage_findings(cfg, day)
    checks = {f["check"] for f in findings}
    assert "valuation_bars_orphan_symbol" in checks
    orphan = next(f for f in findings if f["check"] == "valuation_bars_orphan_symbol")
    assert orphan["orphan_sample"] == ["DELISTED"]
    assert orphan["severity"] == "warning"


def test_valuation_flags_low_coverage(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    day = date(2024, 6, 5)
    # 10 symbols trade; valuation prices only 3 (30% < 70% threshold).
    bars = [(f"S{i}", day) for i in range(10)]
    vals = [(f"S{i}", day) for i in range(3)]
    _write_daily(cfg.data_root, "daily_bars", bars)
    _write_daily(cfg.data_root, "valuation_metrics", vals)

    findings = valuation_bars_coverage_findings(cfg, day)
    low = next(f for f in findings if f["check"] == "valuation_bars_low_coverage")
    assert low["covered_symbols"] == 3
    assert low["bars_symbols"] == 10
    assert low["coverage_ratio"] == 0.3


def test_valuation_no_shared_date(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _write_daily(cfg.data_root, "daily_bars", [("A", date(2024, 6, 5))])
    _write_daily(cfg.data_root, "valuation_metrics", [("A", date(2024, 6, 4))])

    findings = valuation_bars_coverage_findings(cfg, date(2024, 6, 5))
    assert any(f["check"] == "valuation_bars_no_shared_date" for f in findings)


def test_valuation_missing_dataset_no_findings(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _write_daily(cfg.data_root, "daily_bars", [("A", date(2024, 6, 5))])
    # No valuation_metrics at all.
    assert valuation_bars_coverage_findings(cfg, date(2024, 6, 5)) == []
