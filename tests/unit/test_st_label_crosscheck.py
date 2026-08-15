"""ST label cross-check: trading_status vs the exchange short name (issue #10).

The retired AkShare ST union looked like a second source but queried the same
push2 endpoint with the same `fs` filter as the EastMoney adapter, so it could
never disagree (issue #3). The instrument short name is a real second reading:
it is assigned by the exchange and arrives over the TDX binary protocol, not
over EastMoney HTTP.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.quality.cross_checks import (
    ST_CROSSCHECK_MAX_DISAGREEMENT,
    st_label_crosscheck_findings,
)

TD = date(2026, 8, 1)


def _lake(tmp_path, *, names: dict[str, str], st_labeled: list[str]) -> Config:
    root = tmp_path / "data"

    inst = root / "curated" / "instruments"
    inst.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": list(names),
            "name": list(names.values()),
            "exchange": [s[-2:] for s in names],
            "asset_type": ["stock"] * len(names),
            "source": ["tdx_protocol"] * len(names),
            "data_version": ["v1"] * len(names),
            "fetched_at": [datetime.now(timezone.utc)] * len(names),
        }
    ).write_parquet(inst / "part-0.parquet")

    part = root / "curated" / "trading_status" / f"trade_date={TD.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": list(names),
            "trade_date": [TD] * len(names),
            "is_trading": [True] * len(names),
            "status": ["st" if s in st_labeled else "normal" for s in names],
            "source": ["eastmoney"] * len(names),
            "data_version": ["v1"] * len(names),
            "fetched_at": [datetime.now(timezone.utc)] * len(names),
        }
    ).write_parquet(part / "part-0.parquet")
    return Config(data_root=root)


def _universe(n: int, *, st_named: int, st_labeled: int):
    """n symbols; the first `st_named` carry an ST name, first `st_labeled` the label."""
    syms = [f"{600000 + i:06d}.SH" for i in range(n)]
    names = {s: (f"ST公司{i}" if i < st_named else f"公司{i}") for i, s in enumerate(syms)}
    return names, syms[:st_labeled]


def test_agreeing_feeds_produce_no_finding(tmp_path):
    names, labeled = _universe(50, st_named=10, st_labeled=10)
    assert st_label_crosscheck_findings(_lake(tmp_path, names=names, st_labeled=labeled), TD) == []


def test_star_st_names_count_as_st(tmp_path):
    names, labeled = _universe(20, st_named=0, st_labeled=3)
    for s in labeled:
        names[s] = "*ST退市"
    assert st_label_crosscheck_findings(_lake(tmp_path, names=names, st_labeled=labeled), TD) == []


def test_small_disagreement_is_tolerated_as_naming_lag(tmp_path):
    """instruments and trading_status are captured by different steps in one run."""
    named = 10
    # Label all but `MAX` of the ST-named symbols: exactly at the tolerance.
    names, labeled = _universe(
        50, st_named=named, st_labeled=named - ST_CROSSCHECK_MAX_DISAGREEMENT
    )
    assert st_label_crosscheck_findings(_lake(tmp_path, names=names, st_labeled=labeled), TD) == []

    # One more than the tolerance does get reported.
    names, labeled = _universe(
        50, st_named=named, st_labeled=named - ST_CROSSCHECK_MAX_DISAGREEMENT - 1
    )
    findings = st_label_crosscheck_findings(_lake(tmp_path, names=names, st_labeled=labeled), TD)
    assert len(findings) == 1
    assert findings[0]["named_not_labeled"] == ST_CROSSCHECK_MAX_DISAGREEMENT + 1


def test_board_gone_stale_is_flagged(tmp_path):
    """Names say ST, the board says nothing — the risk-warning query broke."""
    names, _ = _universe(50, st_named=12, st_labeled=0)
    cfg = _lake(tmp_path, names=names, st_labeled=[f"{600000:06d}.SH"])
    findings = st_label_crosscheck_findings(cfg, TD)
    assert len(findings) == 1
    f = findings[0]
    assert f["check"] == "st_label_crosscheck"
    assert f["severity"] == "warning"
    assert f["named_not_labeled"] == 11
    assert f["labeled_not_named"] == 0


def test_labels_without_matching_names_are_flagged(tmp_path):
    """The board lists names the exchange never renamed — wrong board/filter."""
    names, labeled = _universe(50, st_named=0, st_labeled=9)
    findings = st_label_crosscheck_findings(_lake(tmp_path, names=names, st_labeled=labeled), TD)
    assert len(findings) == 1
    assert findings[0]["labeled_not_named"] == 9
    assert findings[0]["named_not_labeled"] == 0


def test_symbols_missing_from_instruments_are_not_st_disagreements(tmp_path):
    """A symbol absent from the instrument list is a universe gap, not an ST bug."""
    names, _ = _universe(20, st_named=0, st_labeled=0)
    cfg = _lake(tmp_path, names=names, st_labeled=[])
    # Label symbols that instruments has never heard of.
    part = cfg.curated_root / "trading_status" / f"trade_date={TD.isoformat()}"
    pl.DataFrame(
        {
            "symbol": [f"{900000 + i:06d}.SH" for i in range(10)],
            "trade_date": [TD] * 10,
            "is_trading": [True] * 10,
            "status": ["st"] * 10,
            "source": ["eastmoney"] * 10,
            "data_version": ["v1"] * 10,
            "fetched_at": [datetime.now(timezone.utc)] * 10,
        }
    ).write_parquet(part / "part-1.parquet")
    assert st_label_crosscheck_findings(cfg, TD) == []


def test_no_st_rows_for_the_day_is_left_to_the_coverage_check(tmp_path):
    names, _ = _universe(20, st_named=5, st_labeled=0)
    assert st_label_crosscheck_findings(_lake(tmp_path, names=names, st_labeled=[]), TD) == []


def test_empty_lake_is_silent(tmp_path):
    assert st_label_crosscheck_findings(Config(data_root=tmp_path / "data"), TD) == []
