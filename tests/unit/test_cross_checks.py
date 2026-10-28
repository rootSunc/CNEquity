from datetime import date, datetime, timezone

import polars as pl

from cnequity.config import Config
from cnequity.quality.cross_checks import (
    corporate_action_classification_findings,
    daily_bars_calendar_findings,
    instrument_listing_order_findings,
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


def _write_daily(root, dataset, rows, *, volume_by_symbol=None):
    """rows: list of (symbol, date)."""
    base = root / "curated" / dataset
    by_day: dict[date, list[str]] = {}
    for sym, d in rows:
        by_day.setdefault(d, []).append(sym)
    for d, syms in by_day.items():
        part = base / f"trade_date={d.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        data = {"symbol": syms, "trade_date": [d] * len(syms)}
        if dataset == "daily_bars":
            data["volume"] = [
                100 if volume_by_symbol is None else volume_by_symbol.get(sym, 100) for sym in syms
            ]
        pl.DataFrame(data).write_parquet(part / "part.parquet")


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


def test_daily_bars_lone_history_before_floor_does_not_widen_coverage(tmp_path):
    # Measured 2026-10-29: 000508.SZ bars from 1993 flagged 952 pre-2001 sessions.
    cfg = Config(data_root=tmp_path / "data")
    old = [date(1997, 2, 27), date(1997, 2, 28), date(1997, 3, 3)]
    days = [date(2024, 6, 3), date(2024, 6, 4)]
    _write_calendar(cfg.data_root, [(d, True) for d in old + days])
    _write_daily(cfg.data_root, "daily_bars", [("OLD", old[0])] + [("A", d) for d in days])

    assert daily_bars_calendar_findings(cfg, date(2024, 6, 4)) == []


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


def test_daily_bars_flags_placeholder_only_day_as_missing(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    days = [date(2024, 6, 3), date(2024, 6, 4), date(2024, 6, 5)]
    _write_calendar(cfg.data_root, [(d, True) for d in days])
    _write_daily(cfg.data_root, "daily_bars", [("A", days[0]), ("A", days[2])])
    _write_daily(
        cfg.data_root,
        "daily_bars",
        [("A", days[1])],
        volume_by_symbol={"A": 0},
    )

    findings = daily_bars_calendar_findings(cfg, date(2024, 6, 5))

    assert len(findings) == 1
    assert findings[0]["check"] == "daily_bars_calendar_missing_day"
    assert findings[0]["missing_sample"] == ["2024-06-04"]


def test_daily_bars_canonicalizes_before_market_day_volume_check(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    days = [date(2024, 6, 3), date(2024, 6, 4), date(2024, 6, 5)]
    _write_calendar(cfg.data_root, [(d, True) for d in days])
    _write_daily(cfg.data_root, "daily_bars", [("A", days[0]), ("A", days[2])])

    part = cfg.data_root / "curated" / "daily_bars" / f"trade_date={days[1].isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["A", "A"],
            "trade_date": [days[1], days[1]],
            "volume": [100, 0],
            "fetched_at": [
                datetime(2024, 6, 4, 7, tzinfo=timezone.utc),
                datetime(2024, 6, 4, 8, tzinfo=timezone.utc),
            ],
        }
    ).write_parquet(part / "part-retry.parquet")

    findings = daily_bars_calendar_findings(cfg, date(2024, 6, 5))

    assert len(findings) == 1
    assert findings[0]["check"] == "daily_bars_calendar_missing_day"
    assert findings[0]["missing_sample"] == ["2024-06-04"]


def test_daily_bars_placeholder_only_tail_does_not_extend_coverage(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    days = [date(2024, 6, 3), date(2024, 6, 4), date(2024, 6, 5)]
    _write_calendar(cfg.data_root, [(d, True) for d in days])
    _write_daily(
        cfg.data_root,
        "daily_bars",
        [("A", days[0]), ("A", days[1])],
    )
    _write_daily(
        cfg.data_root,
        "daily_bars",
        [("A", days[2])],
        volume_by_symbol={"A": 0},
    )

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


def test_valuation_coverage_uses_canonical_traded_bar(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    day = date(2024, 6, 5)
    _write_daily(cfg.data_root, "daily_bars", [("B", day)])
    part = cfg.data_root / "curated" / "daily_bars" / f"trade_date={day.isoformat()}"
    pl.DataFrame(
        {
            "symbol": ["A", "A"],
            "trade_date": [day, day],
            "volume": [100, 0],
            "fetched_at": [
                datetime(2024, 6, 5, 7, tzinfo=timezone.utc),
                datetime(2024, 6, 5, 8, tzinfo=timezone.utc),
            ],
        }
    ).write_parquet(part / "part-retry.parquet")
    _write_daily(cfg.data_root, "valuation_metrics", [("A", day), ("B", day)])

    findings = valuation_bars_coverage_findings(cfg, day)

    orphan = next(f for f in findings if f["check"] == "valuation_bars_orphan_symbol")
    assert orphan["orphan_sample"] == ["A"]


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


def test_valuation_coverage_ignores_placeholder_only_bar(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    day = date(2024, 6, 5)
    _write_daily(
        cfg.data_root,
        "daily_bars",
        [("A", day), ("B", day)],
        volume_by_symbol={"A": 0, "B": 100},
    )
    _write_daily(cfg.data_root, "valuation_metrics", [("B", day)])

    assert valuation_bars_coverage_findings(cfg, day) == []


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


# --- corporate action classification ---------------------------------------


def _write_actions(root, ex, rows):
    """rows: list of (symbol, action_type, bonus, transfer, source)."""
    part = root / "curated" / "corporate_actions" / f"ex_date={ex.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": [r[0] for r in rows],
            "ex_date": [ex] * len(rows),
            "action_type": [r[1] for r in rows],
            "bonus_ratio": [r[2] for r in rows],
            "transfer_ratio": [r[3] for r in rows],
            "source": [r[4] for r in rows],
        },
        schema={
            "symbol": pl.Utf8,
            "ex_date": pl.Date,
            "action_type": pl.Utf8,
            "bonus_ratio": pl.Float64,
            "transfer_ratio": pl.Float64,
            "source": pl.Utf8,
        },
    ).write_parquet(part / "part.parquet")


def test_duplicate_classification_counts_events_not_join_pairs(tmp_path):
    """The bonus × transfer join multiplies rows; one event must count once."""
    cfg = Config(data_root=tmp_path / "data")
    ex = date(2024, 6, 28)
    # Two 送 rows and two 转 rows for one ex-date — four cross-source pairs,
    # one event. action_type is in the primary key, so none are deduped away.
    _write_actions(
        cfg.data_root,
        ex,
        [
            ("600519.SH", "bonus", 0.4, 0.0, "tdx_protocol"),
            ("600519.SH", "bonus_plan", 0.4, 0.0, "baostock"),
            ("600519.SH", "transfer", 0.0, 0.4, "eastmoney"),
            ("600519.SH", "transfer_plan", 0.0, 0.4, "sina"),
        ],
    )

    findings = corporate_action_classification_findings(cfg, ex, ex)

    assert len(findings) == 1
    assert findings[0]["check"] == "corporate_action_duplicate_classification"
    assert findings[0]["rows"] == 1
    assert "1 ex-date(s)" in findings[0]["message"]


def test_ratio_scale_flags_the_ten_times_reading(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    ex = date(2024, 6, 28)
    _write_actions(
        cfg.data_root,
        ex,
        [
            ("603538.SH", "bonus", 0.4, 0.0, "tdx_protocol"),
            ("603538.SH", "transfer", 0.0, 4.0, "eastmoney"),
        ],
    )

    findings = corporate_action_classification_findings(cfg, ex, ex)

    assert [f["check"] for f in findings] == ["corporate_action_ratio_scale"]
    assert findings[0]["rows"] == 1
    assert findings[0]["sample"][0]["symbol"] == "603538.SH"


def test_one_source_filing_both_columns_is_not_a_cross_source_conflict(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    ex = date(2024, 6, 28)
    _write_actions(
        cfg.data_root,
        ex,
        [
            ("000001.SZ", "bonus", 0.3, 0.0, "eastmoney"),
            ("000001.SZ", "transfer", 0.0, 0.3, "eastmoney"),
        ],
    )

    assert corporate_action_classification_findings(cfg, ex, ex) == []


# --- instrument listing order ----------------------------------------------


def _write_instruments(root, rows):
    """rows: list of (symbol, list_date, delist_date)."""
    base = root / "curated" / "instruments"
    base.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": [r[0] for r in rows],
            "list_date": [r[1] for r in rows],
            "delist_date": [r[2] for r in rows],
        },
        schema={"symbol": pl.Utf8, "list_date": pl.Date, "delist_date": pl.Date},
    ).write_parquet(base / "part.parquet")


def test_listing_order_flags_a_delist_date_before_the_list_date(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _write_instruments(
        cfg.data_root,
        [
            ("301688.SZ", date(2026, 9, 2), date(2026, 9, 1)),
            ("600001.SH", date(2000, 1, 4), date(2009, 12, 15)),
            ("600519.SH", date(2001, 8, 27), None),
        ],
    )

    findings = instrument_listing_order_findings(cfg)

    assert len(findings) == 1
    assert findings[0]["check"] == "instrument_listing_order"
    assert findings[0]["rows"] == 1
    assert findings[0]["sample"] == [
        {"symbol": "301688.SZ", "list_date": "2026-09-02", "delist_date": "2026-09-01"}
    ]


def test_listing_order_quiet_when_every_pair_is_ordered(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _write_instruments(
        cfg.data_root,
        [
            ("600001.SH", date(2000, 1, 4), date(2009, 12, 15)),
            ("600519.SH", date(2001, 8, 27), None),
        ],
    )

    assert instrument_listing_order_findings(cfg) == []
