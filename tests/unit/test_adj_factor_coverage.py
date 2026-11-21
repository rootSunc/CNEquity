"""adj_factors coverage against daily_bars.

The two come from different vendors — factors from Sina, bars from TDX — and
their coverage can diverge for new, retired, or source-unavailable symbols.
Sina does serve 北交所, so a missing BJ factor is not an exchange-wide
exemption. `load(adjust="hfq")` defaults to strict_adj=False, so bars without a
complete factor span come back at factor=1.0, i.e. raw prices inside a result
the caller asked to have adjusted, marked only by an `adj_is_exact` column most
callers never select.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.meta_layout import DECISIONS
from cnequity.quality.cross_checks import adj_factor_coverage_findings
from cnequity.storage.state import StateStore


def _lake(tmp_path, *, stocks, priced, factored, etfs=(), no_trade=(), factor_type="hfq"):
    cfg = Config(data_root=tmp_path / "lake")
    for root in (cfg.curated_root, cfg.derived_root):
        root.mkdir(parents=True, exist_ok=True)

    inst = cfg.curated_root / "instruments"
    inst.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": list(stocks) + list(etfs),
            "asset_type": ["stock"] * len(stocks) + ["etf"] * len(etfs),
        }
    ).write_parquet(inst / "part-0.parquet")

    bars = cfg.curated_root / "daily_bars" / "trade_date=2026-08-07"
    bars.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": list(priced),
            "trade_date": [date(2026, 8, 7)] * len(priced),
            "volume": [0 if symbol in no_trade else 100 for symbol in priced],
        }
    ).write_parquet(bars / "part-0.parquet")

    fac = cfg.derived_root / "adj_factors" / "trade_date=2026-08-07"
    fac.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": list(factored),
            "trade_date": [date(2026, 8, 7)] * len(factored),
            "adjust_type": [factor_type] * len(factored),
            "factor": [1.0] * len(factored),
        }
    ).write_parquet(fac / "part-0.parquet")
    return cfg


def test_uncovered_exchange_is_reported_once_not_per_symbol(tmp_path):
    bj = [f"9200{i:02d}.BJ" for i in range(20)]
    sh = [f"6000{i:02d}.SH" for i in range(20)]
    cfg = _lake(tmp_path, stocks=bj + sh, priced=bj + sh, factored=sh)

    findings = adj_factor_coverage_findings(cfg, date(2026, 8, 7))
    assert len(findings) == 1, "one finding per exchange, not one per symbol"
    f = findings[0]
    assert f["exchange"] == "BJ"
    assert f["symbols_missing"] == 20
    assert f["coverage_ratio"] == 0.0
    assert "strict_adj" in f["message"], "must say how to make it fail loudly"


def test_fully_covered_exchange_is_silent(tmp_path):
    sh = [f"6000{i:02d}.SH" for i in range(20)]
    cfg = _lake(tmp_path, stocks=sh, priced=sh, factored=sh)
    assert adj_factor_coverage_findings(cfg, date(2026, 8, 7)) == []


def test_internal_factor_gap_is_reported_even_when_endpoints_match(tmp_path):
    sh = [f"6000{i:02d}.SH" for i in range(20)]
    cfg = _lake(tmp_path, stocks=sh, priced=sh, factored=sh)
    middle = cfg.curated_root / "daily_bars" / "trade_date=2026-08-06"
    middle.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": sh,
            "trade_date": [date(2026, 8, 6)] * len(sh),
            "volume": [100] * len(sh),
        }
    ).write_parquet(middle / "part-0.parquet")
    # The factor has the first and last dates, but not the middle trading day
    # for one symbol. A min/max-only check would incorrectly call this covered.
    first = cfg.derived_root / "adj_factors" / "trade_date=2026-08-05"
    first.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": sh,
            "trade_date": [date(2026, 8, 5)] * len(sh),
            "adjust_type": ["hfq"] * len(sh),
            "factor": [1.0] * len(sh),
        }
    ).write_parquet(first / "part-0.parquet")
    # Remove the one symbol's middle factor by leaving the other endpoint rows
    # intact; its span still equals its bar span.
    middle_factor = cfg.derived_root / "adj_factors" / "trade_date=2026-08-06"
    middle_factor.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": sh[1:],
            "trade_date": [date(2026, 8, 6)] * (len(sh) - 1),
            "adjust_type": ["hfq"] * (len(sh) - 1),
            "factor": [1.0] * (len(sh) - 1),
        }
    ).write_parquet(middle_factor / "part-0.parquet")

    findings = adj_factor_coverage_findings(cfg, date(2026, 8, 7))

    assert len(findings) == 1
    assert findings[0]["symbols_internal_gaps"] == 1
    assert findings[0]["symbols_partial"] == 1


def test_qfq_only_rows_do_not_count_as_hfq_coverage(tmp_path):
    sh = [f"6000{i:02d}.SH" for i in range(20)]
    cfg = _lake(tmp_path, stocks=sh, priced=sh, factored=sh, factor_type="qfq")

    findings = adj_factor_coverage_findings(cfg, date(2026, 8, 7))

    assert len(findings) == 1
    assert findings[0]["symbols_without_factor"] == len(sh)


def test_partial_factor_span_is_reported_as_incomplete(tmp_path):
    sh = [f"6000{i:02d}.SH" for i in range(20)]
    cfg = _lake(tmp_path, stocks=sh, priced=sh, factored=sh)
    earlier = cfg.curated_root / "daily_bars" / "trade_date=2024-01-02"
    earlier.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": [sh[0]],
            "trade_date": [date(2024, 1, 2)],
            "volume": [100],
        }
    ).write_parquet(earlier / "part-0.parquet")

    findings = adj_factor_coverage_findings(cfg, date(2026, 8, 7))

    assert len(findings) == 1
    finding = findings[0]
    assert finding["symbols_covered"] == 19
    assert finding["symbols_missing"] == 1
    assert finding["symbols_without_factor"] == 0
    assert finding["symbols_partial"] == 1
    assert finding["sample"] == [sh[0]]
    assert "partial" in finding["message"]


def test_a_few_missing_names_stay_below_the_threshold(tmp_path):
    """98% covered is the bar: one delisting mid-refresh must not page anyone."""
    sh = [f"6000{i:02d}.SH" for i in range(100)]
    cfg = _lake(tmp_path, stocks=sh, priced=sh, factored=sh[:99])
    assert adj_factor_coverage_findings(cfg, date(2026, 8, 7)) == []


def test_known_source_unavailable_names_are_visible_below_warning_threshold(tmp_path):
    sh = [f"6000{i:02d}.SH" for i in range(100)]
    cfg = _lake(tmp_path, stocks=sh, priced=sh, factored=sh[:99])
    StateStore(cfg.meta_root, DECISIONS).set_string_set(
        "adj_factors", "source_unavailable_symbols", {sh[-1]}
    )

    findings = adj_factor_coverage_findings(cfg, date(2026, 8, 7))

    assert len(findings) == 1
    assert findings[0]["check"] == "adj_factor_source_unavailable"
    assert findings[0]["severity"] == "info"
    assert findings[0]["symbols_unavailable"] == 1


def test_unfactored_etfs_count_against_coverage(tmp_path):
    """ETFs now carry Sina hfq factors, so a missing one is a real gap."""
    sh = [f"6000{i:02d}.SH" for i in range(20)]
    etf = [f"5100{i:02d}.SH" for i in range(50)]
    cfg = _lake(tmp_path, stocks=sh, priced=sh + etf, factored=sh, etfs=etf)

    findings = adj_factor_coverage_findings(cfg, date(2026, 8, 7))
    assert len(findings) == 1
    assert findings[0]["exchange"] == "SH"
    assert findings[0]["symbols_missing"] == 50


def test_factored_etfs_are_silent(tmp_path):
    sh = [f"6000{i:02d}.SH" for i in range(20)]
    etf = [f"5100{i:02d}.SH" for i in range(50)]
    cfg = _lake(tmp_path, stocks=sh, priced=sh + etf, factored=sh + etf, etfs=etf)
    assert adj_factor_coverage_findings(cfg, date(2026, 8, 7)) == []


def test_placeholder_only_stock_does_not_count_as_priced(tmp_path):
    sh = [f"6000{i:02d}.SH" for i in range(20)]
    placeholder = "600099.SH"
    cfg = _lake(
        tmp_path,
        stocks=sh + [placeholder],
        priced=sh + [placeholder],
        factored=sh,
        no_trade=[placeholder],
    )

    assert adj_factor_coverage_findings(cfg, date(2026, 8, 7)) == []


def test_missing_datasets_are_not_an_error(tmp_path):
    cfg = Config(data_root=tmp_path / "empty")
    assert adj_factor_coverage_findings(cfg, date(2026, 8, 7)) == []
