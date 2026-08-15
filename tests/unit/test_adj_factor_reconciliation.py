"""Tests for adj_factor_reconciliation_findings.

error ``adj_close_discontinuity`` / warning ``missing_corporate_action``.
Fixtures write raw close + hfq factor; the check reconstructs adj returns.
"""

from datetime import date

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.quality import cross_checks
from cn_market_lake.quality.cross_checks import adj_factor_reconciliation_findings


def _write_bars(root, rows):
    """rows: list of (symbol, date, close)."""
    base = root / "curated" / "daily_bars"
    by_day: dict[date, list[tuple[str, float]]] = {}
    for sym, d, close in rows:
        by_day.setdefault(d, []).append((sym, close))
    for d, entries in by_day.items():
        part = base / f"trade_date={d.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "symbol": [s for s, _ in entries],
                "trade_date": [d] * len(entries),
                "close": [c for _, c in entries],
            }
        ).write_parquet(part / "part.parquet")


def _write_factors(root, rows):
    """rows: list of (symbol, date, factor); adjust_type defaults to hfq."""
    base = root / "derived" / "adj_factors"
    by_day: dict[date, list[tuple[str, float]]] = {}
    for sym, d, f in rows:
        by_day.setdefault(d, []).append((sym, f))
    for d, entries in by_day.items():
        part = base / f"trade_date={d.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "symbol": [s for s, _ in entries],
                "trade_date": [d] * len(entries),
                "adjust_type": ["hfq"] * len(entries),
                "factor": [f for _, f in entries],
            }
        ).write_parquet(part / "part.parquet")


def _write_corp_actions(root, rows):
    """rows: list of (symbol, ex_date)."""
    base = root / "curated" / "corporate_actions"
    by_day: dict[date, list[str]] = {}
    for sym, d in rows:
        by_day.setdefault(d, []).append(sym)
    for d, syms in by_day.items():
        part = base / f"ex_date={d.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "symbol": syms,
                "ex_date": [d] * len(syms),
                "action_type": ["dividend"] * len(syms),
            }
        ).write_parquet(part / "part.parquet")


def _write_instruments(root, rows):
    """rows: list of (symbol, delist_date | None)."""
    base = root / "curated" / "instruments"
    base.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": [s for s, _ in rows],
            "delist_date": [d for _, d in rows],
        },
        schema={"symbol": pl.Utf8, "delist_date": pl.Date},
    ).write_parquet(base / "part-merged.parquet")


def _write_calendar(root, days, *, trading=True):
    """days: list of dates, all marked is_trading=`trading`."""
    base = root / "curated" / "trading_calendar"
    for d in days:
        part = base / f"trade_date={d.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"trade_date": [d], "is_trading": [trading]}).write_parquet(
            part / "part.parquet"
        )


_D = [date(2024, 6, 3), date(2024, 6, 4), date(2024, 6, 5), date(2024, 6, 6)]

# A decoy event on an unrelated symbol so the corporate_actions dataset exists
# (a populated lake always has some) without explaining the symbol under test —
# an absent dataset is a distinct "cannot reconcile" path, covered separately.
_DECOY = [("ZZZZ.SZ", date(2024, 1, 2))]


def _checks(findings):
    return {f["check"] for f in findings}


def test_clean_when_adjustment_tracks_raw(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _write_bars(
        cfg.data_root,
        [("A", _D[0], 10.0), ("A", _D[1], 10.5), ("A", _D[2], 10.2), ("A", _D[3], 10.4)],
    )
    _write_factors(cfg.data_root, [("A", d, 1.0) for d in _D])
    _write_corp_actions(cfg.data_root, _DECOY)
    assert adj_factor_reconciliation_findings(cfg, _D[-1]) == []


def test_normal_down_day_not_flagged(tmp_path):
    # An ~8% down day with a flat factor: adj_ret == raw_ret, no divergence.
    cfg = Config(data_root=tmp_path / "data")
    _write_bars(
        cfg.data_root, [("A", _D[0], 10.0), ("A", _D[1], 9.2), ("A", _D[2], 9.0), ("A", _D[3], 9.1)]
    )
    _write_factors(cfg.data_root, [("A", d, 1.0) for d in _D])
    _write_corp_actions(cfg.data_root, _DECOY)
    assert adj_factor_reconciliation_findings(cfg, _D[-1]) == []


def test_factor_break_is_error(tmp_path):
    # Factor doubles between two consecutive trading days while the raw price is
    # flat: the adjusted series jumps 2x — impossible under the board limit.
    cfg = Config(data_root=tmp_path / "data")
    _write_bars(cfg.data_root, [("A", d, 10.0) for d in _D])
    _write_factors(
        cfg.data_root, [("A", _D[0], 1.0), ("A", _D[1], 1.0), ("A", _D[2], 2.0), ("A", _D[3], 2.0)]
    )
    _write_calendar(cfg.data_root, _D)
    # No corporate_actions dataset at all: an adjustment break is still an error.
    findings = adj_factor_reconciliation_findings(cfg, _D[-1])
    assert len(findings) == 1
    f = findings[0]
    assert f["check"] == "adj_close_discontinuity"
    assert f["severity"] == "error"
    assert f["symbol"] == "A"
    assert f["trade_date"] == "2024-06-05"
    assert f["adj_ret"] == 1.0


def test_break_is_error_even_with_a_corp_action(tmp_path):
    # A corporate action cannot excuse a consecutive-day discontinuous adjustment.
    cfg = Config(data_root=tmp_path / "data")
    _write_bars(cfg.data_root, [("A", d, 10.0) for d in _D])
    _write_factors(
        cfg.data_root, [("A", _D[0], 1.0), ("A", _D[1], 1.0), ("A", _D[2], 2.0), ("A", _D[3], 2.0)]
    )
    _write_calendar(cfg.data_root, _D)
    _write_corp_actions(cfg.data_root, [("A", _D[2])])
    findings = adj_factor_reconciliation_findings(cfg, _D[-1])
    assert _checks(findings) == {"adj_close_discontinuity"}
    assert findings[0]["severity"] == "error"


def test_suspension_resume_reprice_is_not_a_break(tmp_path):
    # 600733-style: bars only on _D[0] and _D[3] (an 8-month halt in reality). The
    # factor steps 3.5x for a bonus during the halt, and the adjusted price
    # genuinely reprices -36% on resume. |adj_ret|>0.35 but the days are NOT
    # consecutive, so it must not be flagged as a break.
    cfg = Config(data_root=tmp_path / "data")
    _write_bars(cfg.data_root, [("A", _D[0], 52.0), ("A", _D[3], 9.5)])
    _write_factors(cfg.data_root, [("A", _D[0], 2.34), ("A", _D[3], 8.19)])
    _write_calendar(cfg.data_root, _D)  # _D[1], _D[2] traded — halt, not adjacency
    _write_corp_actions(cfg.data_root, [("A", _D[2])])  # the bonus during the halt
    assert adj_factor_reconciliation_findings(cfg, _D[-1]) == []


def test_discontinuity_fail_loud_without_calendar(tmp_path):
    # Without a calendar, adjacency cannot be judged, so a discontinuity is still
    # reported (fail-loud) rather than silently dropped.
    cfg = Config(data_root=tmp_path / "data")
    _write_bars(cfg.data_root, [("A", d, 10.0) for d in _D])
    _write_factors(
        cfg.data_root, [("A", _D[0], 1.0), ("A", _D[1], 1.0), ("A", _D[2], 2.0), ("A", _D[3], 2.0)]
    )
    findings = adj_factor_reconciliation_findings(cfg, _D[-1])
    assert _checks(findings) == {"adj_close_discontinuity"}


def test_missing_corp_action_is_warning(tmp_path):
    # 10-for-10 bonus: raw halves, factor doubles, adjustment stays flat. With no
    # corporate action on record it is a completeness warning, not a break.
    # No calendar → adjacency cannot be judged; still warn (fail-loud completeness).
    cfg = Config(data_root=tmp_path / "data")
    _write_bars(
        cfg.data_root,
        [("A", _D[0], 10.0), ("A", _D[1], 10.0), ("A", _D[2], 5.0), ("A", _D[3], 5.0)],
    )
    _write_factors(
        cfg.data_root, [("A", _D[0], 1.0), ("A", _D[1], 1.0), ("A", _D[2], 2.0), ("A", _D[3], 2.0)]
    )
    _write_corp_actions(cfg.data_root, _DECOY)
    findings = adj_factor_reconciliation_findings(cfg, _D[-1])
    assert len(findings) == 1
    f = findings[0]
    assert f["check"] == "missing_corporate_action"
    assert f["severity"] == "warning"
    assert f["dataset"] == "corporate_actions"
    assert f["symbol"] == "A"
    assert f["trade_date"] == "2024-06-05"
    assert f["adj_ret"] == 0.0
    assert f["raw_ret"] == -0.5


def test_missing_corp_action_on_adjacent_days_with_calendar(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _write_bars(
        cfg.data_root,
        [("A", _D[0], 10.0), ("A", _D[1], 10.0), ("A", _D[2], 5.0), ("A", _D[3], 5.0)],
    )
    _write_factors(
        cfg.data_root, [("A", _D[0], 1.0), ("A", _D[1], 1.0), ("A", _D[2], 2.0), ("A", _D[3], 2.0)]
    )
    _write_calendar(cfg.data_root, _D)
    _write_corp_actions(cfg.data_root, _DECOY)
    findings = adj_factor_reconciliation_findings(cfg, _D[-1])
    assert _checks(findings) == {"missing_corporate_action"}


def test_missing_corp_action_on_a_delisted_symbol_is_info_not_warning(tmp_path):
    """tdx_protocol and the eastmoney backup were checked live and neither
    serves corporate-action history for a name once it is off their live
    symbol list — a vendor gap correlated with delist_date, not a code bug.
    Filing one "warning" per delisted symbol (there were 109 of them at once)
    buries the few findings on still-listed names that are worth investigating,
    so a delisted symbol gets bucketed into a single info-level summary."""
    cfg = Config(data_root=tmp_path / "data")
    _write_bars(
        cfg.data_root,
        [("A", _D[0], 10.0), ("A", _D[1], 10.0), ("A", _D[2], 5.0), ("A", _D[3], 5.0)],
    )
    _write_factors(
        cfg.data_root, [("A", _D[0], 1.0), ("A", _D[1], 1.0), ("A", _D[2], 2.0), ("A", _D[3], 2.0)]
    )
    _write_corp_actions(cfg.data_root, _DECOY)
    _write_instruments(cfg.data_root, [("A", date(2024, 6, 5))])
    findings = adj_factor_reconciliation_findings(cfg, _D[-1])
    assert len(findings) == 1
    f = findings[0]
    assert f["check"] == "missing_corporate_action_delisted"
    assert f["severity"] == "info"
    assert f["symbols_total"] == 1
    assert f["sample"] == ["A"]


def test_missing_corp_action_on_a_still_listed_symbol_stays_a_warning(tmp_path):
    """The instruments join must not downgrade a symbol just for existing in
    that dataset — only a non-null delist_date does."""
    cfg = Config(data_root=tmp_path / "data")
    _write_bars(
        cfg.data_root,
        [("A", _D[0], 10.0), ("A", _D[1], 10.0), ("A", _D[2], 5.0), ("A", _D[3], 5.0)],
    )
    _write_factors(
        cfg.data_root, [("A", _D[0], 1.0), ("A", _D[1], 1.0), ("A", _D[2], 2.0), ("A", _D[3], 2.0)]
    )
    _write_corp_actions(cfg.data_root, _DECOY)
    _write_instruments(cfg.data_root, [("A", None)])
    findings = adj_factor_reconciliation_findings(cfg, _D[-1])
    assert len(findings) == 1
    assert findings[0]["check"] == "missing_corporate_action"
    assert findings[0]["severity"] == "warning"


def test_suspension_resume_without_ca_not_missing_warning(tmp_path):
    # Bars only on 06-03 and 06-06 (halt mid-week): raw halves with flat adj.
    # That shape looks like a missing ex-event, but the calendar shows a gap —
    # suspension resume, not an unrecorded dividend.
    cfg = Config(data_root=tmp_path / "data")
    _write_bars(cfg.data_root, [("A", _D[0], 10.0), ("A", _D[3], 5.0)])
    _write_factors(cfg.data_root, [("A", _D[0], 1.0), ("A", _D[3], 2.0)])
    _write_calendar(cfg.data_root, _D)
    _write_corp_actions(cfg.data_root, _DECOY)
    assert adj_factor_reconciliation_findings(cfg, _D[-1]) == []


def test_ex_event_with_matching_corp_action_is_explained(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _write_bars(
        cfg.data_root,
        [("A", _D[0], 10.0), ("A", _D[1], 10.0), ("A", _D[2], 5.0), ("A", _D[3], 5.0)],
    )
    _write_factors(
        cfg.data_root, [("A", _D[0], 1.0), ("A", _D[1], 1.0), ("A", _D[2], 2.0), ("A", _D[3], 2.0)]
    )
    _write_corp_actions(cfg.data_root, [("A", _D[2])])
    assert adj_factor_reconciliation_findings(cfg, _D[-1]) == []


def test_suspension_across_ex_date_is_explained(tmp_path):
    # Suspended 06-04/06-05: the adjustment lands on the resume bar 06-06, but the
    # ex-date 06-05 still lies in the interval (06-03, 06-06], so it is explained.
    cfg = Config(data_root=tmp_path / "data")
    _write_bars(cfg.data_root, [("A", _D[0], 10.0), ("A", _D[3], 5.0)])
    _write_factors(cfg.data_root, [("A", _D[0], 1.0), ("A", _D[3], 2.0)])
    _write_corp_actions(cfg.data_root, [("A", _D[2])])
    assert adj_factor_reconciliation_findings(cfg, _D[-1]) == []


def test_break_symbol_not_double_counted_as_warning(tmp_path):
    # A on 06-04 is a break; on 06-06 it also has a flat-adjustment raw drop that
    # would otherwise warn. The symbol is reported once, as an error.
    cfg = Config(data_root=tmp_path / "data")
    _write_bars(
        cfg.data_root,
        [("A", _D[0], 10.0), ("A", _D[1], 10.0), ("A", _D[2], 10.0), ("A", _D[3], 5.0)],
    )
    _write_factors(
        cfg.data_root, [("A", _D[0], 1.0), ("A", _D[1], 2.0), ("A", _D[2], 2.0), ("A", _D[3], 4.0)]
    )
    _write_calendar(cfg.data_root, _D)
    _write_corp_actions(cfg.data_root, _DECOY)
    findings = adj_factor_reconciliation_findings(cfg, _D[-1])
    assert _checks(findings) == {"adj_close_discontinuity"}
    assert all(f["severity"] == "error" for f in findings)


def test_missing_corp_action_needs_the_dataset_present(tmp_path):
    # Same missing-event shape as above but with no corporate_actions dataset:
    # we cannot assert an event is missing, so no warning (errors would still fire).
    cfg = Config(data_root=tmp_path / "data")
    _write_bars(
        cfg.data_root,
        [("A", _D[0], 10.0), ("A", _D[1], 10.0), ("A", _D[2], 5.0), ("A", _D[3], 5.0)],
    )
    _write_factors(
        cfg.data_root, [("A", _D[0], 1.0), ("A", _D[1], 1.0), ("A", _D[2], 2.0), ("A", _D[3], 2.0)]
    )
    assert adj_factor_reconciliation_findings(cfg, _D[-1]) == []


def test_missing_adj_factors_no_findings(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _write_bars(cfg.data_root, [("A", d, 10.0) for d in _D])
    assert adj_factor_reconciliation_findings(cfg, _D[-1]) == []


def test_overflow_summary_caps_error_findings(tmp_path, monkeypatch):
    monkeypatch.setattr(cross_checks, "_MAX_RECON_FINDINGS", 2)
    cfg = Config(data_root=tmp_path / "data")
    bars, factors = [], []
    for i in range(5):
        sym = f"S{i}"
        bars += [(sym, d, 10.0) for d in _D]
        factors += [(sym, _D[0], 1.0), (sym, _D[1], 1.0), (sym, _D[2], 2.0), (sym, _D[3], 2.0)]
    _write_bars(cfg.data_root, bars)
    _write_factors(cfg.data_root, factors)
    _write_calendar(cfg.data_root, _D)

    findings = adj_factor_reconciliation_findings(cfg, _D[-1])
    per_symbol = [f for f in findings if f["check"] == "adj_close_discontinuity"]
    overflow = [f for f in findings if f["check"] == "adj_close_discontinuity_overflow"]
    assert len(per_symbol) == 2
    assert len(overflow) == 1
    assert overflow[0]["total_symbols"] == 5
    assert overflow[0]["severity"] == "error"
