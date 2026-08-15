from datetime import date

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.quality.audit import _index_bars_coverage_findings


def _write_calendar(root, rows):
    base = root / "curated" / "trading_calendar"
    for d, is_trading in rows:
        part = base / f"trade_date={d.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"trade_date": [d], "is_trading": [is_trading]}).write_parquet(
            part / "part.parquet"
        )


def _write_index_bars(root, rows):
    base = root / "curated" / "index_bars"
    by_day: dict[date, list[str]] = {}
    for sym, d in rows:
        by_day.setdefault(d, []).append(sym)
    for d, syms in by_day.items():
        part = base / f"trade_date={d.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"symbol": syms, "trade_date": [d] * len(syms)}).write_parquet(
            part / "part.parquet"
        )


def test_coverage_clean_when_bars_match_calendar(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    days = [date(2024, 6, 3), date(2024, 6, 4), date(2024, 6, 5)]
    _write_calendar(cfg.data_root, [(d, True) for d in days])
    _write_index_bars(cfg.data_root, [("000300.SH", d) for d in days])

    assert _index_bars_coverage_findings(cfg, date(2024, 6, 5)) == []


def test_coverage_flags_missing_trading_day(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    days = [date(2024, 6, 3), date(2024, 6, 4), date(2024, 6, 5)]
    _write_calendar(cfg.data_root, [(d, True) for d in days])
    # Bar missing on the interior day 2024-06-04.
    _write_index_bars(cfg.data_root, [("000300.SH", days[0]), ("000300.SH", days[2])])

    findings = _index_bars_coverage_findings(cfg, date(2024, 6, 5))
    assert len(findings) == 1
    f = findings[0]
    assert f["check"] == "index_bars_calendar_coverage"
    assert f["symbol"] == "000300.SH"
    assert f["missing_count"] == 1
    assert f["missing_sample"] == ["2024-06-04"]
    assert f["orphan_count"] == 0


def test_coverage_flags_orphan_bar_on_non_trading_day(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _write_calendar(
        cfg.data_root,
        [(date(2024, 6, 3), True), (date(2024, 6, 4), False), (date(2024, 6, 5), True)],
    )
    # Bar exists on 2024-06-04 which the calendar marks non-trading.
    _write_index_bars(
        cfg.data_root,
        [
            ("000300.SH", date(2024, 6, 3)),
            ("000300.SH", date(2024, 6, 4)),
            ("000300.SH", date(2024, 6, 5)),
        ],
    )

    findings = _index_bars_coverage_findings(cfg, date(2024, 6, 5))
    assert len(findings) == 1
    assert findings[0]["orphan_count"] == 1
    assert findings[0]["orphan_sample"] == ["2024-06-04"]
    assert findings[0]["missing_count"] == 0


def test_coverage_empty_lake_no_findings(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    assert _index_bars_coverage_findings(cfg, date(2024, 6, 5)) == []
