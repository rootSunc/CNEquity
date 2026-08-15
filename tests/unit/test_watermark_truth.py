"""The watermark must answer "through what date do we have data", not "when did we run"."""

from datetime import date

import polars as pl

import cn_market_lake.steps  # noqa: F401 — register steps
from cn_market_lake.config import Config
from cn_market_lake.steps.finalize import _reconcile_watermarks, _update_watermarks
from cn_market_lake.storage.state import StateStore

_VAL_SCHEMA_ROWS = ("pe_ttm", "pb", "ps_ttm", "total_mv", "float_mv")


def _write_valuation(cfg: Config, trade_date: date, symbols: int = 3) -> None:
    part = cfg.curated_root / "valuation_metrics" / f"trade_date={trade_date.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    frame = {
        "symbol": [f"60000{i}.SH" for i in range(symbols)],
        "trade_date": [trade_date] * symbols,
        **{col: [1.0] * symbols for col in _VAL_SCHEMA_ROWS},
        "source": ["baostock"] * symbols,
        "data_version": ["v1"] * symbols,
    }
    pl.DataFrame(frame).write_parquet(part / "part-merged.parquet")


def test_snapshot_watermark_follows_the_data_not_the_run_date(tmp_path):
    """A backfill writing an older day must not stamp the watermark with today."""
    cfg = Config(data_root=tmp_path / "data")
    _write_valuation(cfg, date(2026, 7, 20))

    # valuation_metrics is a snapshot dataset; the run happens on the 21st.
    _update_watermarks(cfg, frozenset({"valuation_metrics"}), date(2026, 7, 21))

    assert StateStore(cfg.meta_root).get_date("valuation_metrics") == date(2026, 7, 20)


def test_watermark_advances_when_the_day_actually_landed(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _write_valuation(cfg, date(2026, 7, 20))
    _write_valuation(cfg, date(2026, 7, 21))

    _update_watermarks(cfg, frozenset({"valuation_metrics"}), date(2026, 7, 21))

    assert StateStore(cfg.meta_root).get_date("valuation_metrics") == date(2026, 7, 21)


def test_watermark_never_moves_backward_while_advancing(tmp_path):
    """Advancing is monotonic; only reconciliation may pull a watermark back."""
    cfg = Config(data_root=tmp_path / "data")
    state = StateStore(cfg.meta_root)
    state.set_date("valuation_metrics", date(2026, 7, 21))
    _write_valuation(cfg, date(2026, 7, 20))

    _update_watermarks(cfg, frozenset({"valuation_metrics"}), date(2026, 7, 21))

    assert state.get_date("valuation_metrics") == date(2026, 7, 21)


def test_reconcile_pulls_back_a_watermark_that_claims_missing_data(tmp_path):
    """The outage case: the source went dark, so nothing compacts and nothing advances."""
    cfg = Config(data_root=tmp_path / "data")
    state = StateStore(cfg.meta_root)
    state.set_date("valuation_metrics", date(2026, 7, 21))
    _write_valuation(cfg, date(2026, 7, 20))

    findings = _reconcile_watermarks(cfg)

    assert state.get_date("valuation_metrics") == date(2026, 7, 20)
    assert len(findings) == 1
    assert findings[0]["check"] == "valuation_watermark_coverage_gate"
    assert findings[0]["claimed"] == "2026-07-21"
    assert findings[0]["actual"] == "2026-07-20"


def test_reconcile_leaves_an_honest_watermark_alone(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    state = StateStore(cfg.meta_root)
    _write_valuation(cfg, date(2026, 7, 21))
    state.set_date("valuation_metrics", date(2026, 7, 21))

    assert _reconcile_watermarks(cfg) == []
    assert state.get_date("valuation_metrics") == date(2026, 7, 21)


def test_reconcile_ignores_a_dataset_with_no_watermark_yet(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _write_valuation(cfg, date(2026, 7, 20))

    assert _reconcile_watermarks(cfg) == []


def test_reconcile_ignores_a_dataset_with_no_data_at_all(tmp_path):
    """An empty dataset is reported by the existence check, not by rewinding."""
    cfg = Config(data_root=tmp_path / "data")
    state = StateStore(cfg.meta_root)
    state.set_date("valuation_metrics", date(2026, 7, 21))

    assert _reconcile_watermarks(cfg) == []
    assert state.get_date("valuation_metrics") == date(2026, 7, 21)


def test_an_outage_accumulates_staleness_instead_of_being_masked(tmp_path):
    """The point of the fix: lag has to grow while a source stays dark.

    Stamping the watermark with the run date pinned the lag at zero forever, so
    lake_health called the dataset fresh on day one of an outage and on day ten
    alike. Anchored to the data, each day the source misses adds a day of lag
    until it crosses the staleness tolerance and gets reported.
    """
    from cn_market_lake.domain.datasets import is_stale

    cfg = Config(data_root=tmp_path / "data")
    state = StateStore(cfg.meta_root)
    last_good = date(2026, 7, 20)
    _write_valuation(cfg, last_good)

    lags = []
    for day in (21, 22, 23, 25):
        anchor = date(2026, 7, day)
        # Source is dark: nothing new lands, so nothing compacts.
        state.set_date("valuation_metrics", anchor)  # what the old code did
        _reconcile_watermarks(cfg)
        mark = state.get_date("valuation_metrics")
        lags.append((anchor - mark).days)

    assert lags == [1, 2, 3, 5], "lag must grow with the outage"
    assert is_stale("valuation_metrics", last_good, date(2026, 7, 21)) is False
    assert is_stale("valuation_metrics", last_good, date(2026, 7, 23)) is True
