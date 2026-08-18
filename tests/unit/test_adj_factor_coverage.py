"""adj_factors coverage against daily_bars.

The two come from different vendors — factors from Sina, bars from TDX — and
they do not cover the same market: Sina's factor series essentially skips
北交所. `load(adjust="hfq")` defaults to strict_adj=False, so those bars come
back at factor=1.0, i.e. raw prices inside a result the caller asked to have
adjusted, marked only by an `adj_is_exact` column most callers never select.
Measured on a real lake: 252 of 580 priced BJ stocks, and 10,480 such rows in a
one-year all_a window. Nothing raised and nothing appeared in the audit.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.quality.cross_checks import adj_factor_coverage_findings


def _lake(tmp_path, *, stocks, priced, factored, etfs=(), no_trade=()):
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
        {"symbol": list(factored), "trade_date": [date(2026, 8, 7)] * len(factored)}
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


def test_a_few_missing_names_stay_below_the_threshold(tmp_path):
    """98% covered is the bar: one delisting mid-refresh must not page anyone."""
    sh = [f"6000{i:02d}.SH" for i in range(100)]
    cfg = _lake(tmp_path, stocks=sh, priced=sh, factored=sh[:99])
    assert adj_factor_coverage_findings(cfg, date(2026, 8, 7)) == []


def test_etfs_do_not_count_against_coverage(tmp_path):
    """ETF/LOF factors are not reliable (Sina varies the field per fund and
    omits some ETFs), so they must not bury the stock coverage signal."""
    sh = [f"6000{i:02d}.SH" for i in range(20)]
    etf = [f"5100{i:02d}.SH" for i in range(50)]
    cfg = _lake(tmp_path, stocks=sh, priced=sh + etf, factored=sh, etfs=etf)
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
