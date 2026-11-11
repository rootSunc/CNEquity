"""Baostock history must not invent a sparse valuation tip past EastMoney."""

from datetime import date

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.domain.schemas import DAILY_BARS_SCHEMA, VALUATION_METRICS_SCHEMA
from cnequity.quality.cross_checks import (
    last_complete_em_valuation_tip,
    last_dense_valuation_date,
    valuation_day_coverage_ratio,
)
from cnequity.run_scope import set_backfill, set_run_flags
from cnequity.steps.finalize import _reconcile_watermarks, _watermark_date_for
from cnequity.steps.fundamentals import _valuation_history_end
from cnequity.storage.state import StateStore


def _write_day(
    root,
    dataset,
    d: date,
    symbols: list[str],
    *,
    source: str,
    schema: dict,
    partition_value: str | None = None,
    file_name: str = "part-merged.parquet",
):
    part = root / dataset / f"trade_date={partition_value or d.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    n = len(symbols)
    cols = {
        "symbol": symbols,
        "trade_date": [d] * n,
        "source": [source] * n,
        "data_version": ["v1"] * n,
        "fetched_at": ["2026-07-01T00:00:00+00:00"] * n,
    }
    if dataset == "daily_bars":
        cols.update(
            {
                "open": [1.0] * n,
                "high": [1.0] * n,
                "low": [1.0] * n,
                "close": [1.0] * n,
                "volume": [1] * n,
                "amount": [1.0] * n,
            }
        )
    else:
        cols.update(
            {
                "pe_ttm": [10.0] * n,
                "pb": [1.0] * n,
                "ps_ttm": [2.0] * n,
                "total_mv": [1e9] * n,
                "float_mv": [1e9] * n,
            }
        )
    keep = [c for c in schema if c in cols]
    (
        pl.DataFrame({c: cols[c] for c in keep})
        .with_columns(pl.col("fetched_at").str.to_datetime(time_unit="us", time_zone="UTC"))
        .write_parquet(part / file_name)
    )


def _lake(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    cfg.curated_root.mkdir(parents=True, exist_ok=True)
    cfg.meta_root.mkdir(parents=True, exist_ok=True)
    return cfg


def test_coverage_ratio_and_dense_tip(tmp_path):
    cfg = _lake(tmp_path)
    dense = date(2026, 7, 16)
    sparse = date(2026, 7, 22)
    bars = [f"{i:06d}.SH" for i in range(600000, 600010)]
    _write_day(cfg.curated_root, "daily_bars", dense, bars, source="tdx", schema=DAILY_BARS_SCHEMA)
    _write_day(cfg.curated_root, "daily_bars", sparse, bars, source="tdx", schema=DAILY_BARS_SCHEMA)
    _write_day(
        cfg.curated_root,
        "valuation_metrics",
        dense,
        bars,
        source="eastmoney",
        schema=VALUATION_METRICS_SCHEMA,
    )
    _write_day(
        cfg.curated_root,
        "valuation_metrics",
        sparse,
        bars[:2],
        source="baostock",
        schema=VALUATION_METRICS_SCHEMA,
    )

    assert valuation_day_coverage_ratio(cfg, dense) == 1.0
    assert valuation_day_coverage_ratio(cfg, sparse) == 0.2
    assert last_dense_valuation_date(cfg) == dense
    assert last_complete_em_valuation_tip(cfg) == dense


def test_dense_tip_includes_root_legacy_files_in_mixed_layout(tmp_path):
    cfg = _lake(tmp_path)
    old = date(2026, 7, 16)
    latest = date(2026, 7, 22)
    bars = [f"{i:06d}.SH" for i in range(600000, 600005)]
    _write_day(cfg.curated_root, "daily_bars", old, bars, source="tdx", schema=DAILY_BARS_SCHEMA)
    _write_day(
        cfg.curated_root,
        "valuation_metrics",
        old,
        bars,
        source="eastmoney",
        schema=VALUATION_METRICS_SCHEMA,
    )
    _write_day(
        cfg.curated_root,
        "daily_bars",
        latest,
        bars,
        source="tdx",
        schema=DAILY_BARS_SCHEMA,
    )
    _write_day(
        cfg.curated_root,
        "valuation_metrics",
        latest,
        bars,
        source="eastmoney",
        schema=VALUATION_METRICS_SCHEMA,
    )
    latest_dir = cfg.curated_root / "valuation_metrics" / f"trade_date={latest.isoformat()}"
    legacy_path = cfg.curated_root / "valuation_metrics" / "part-legacy.parquet"
    pl.read_parquet(latest_dir / "part-merged.parquet").write_parquet(legacy_path)
    (latest_dir / "part-merged.parquet").unlink()
    latest_dir.rmdir()

    assert last_dense_valuation_date(cfg) == latest


def test_dense_tip_reads_real_dates_from_coarse_partitions(tmp_path):
    cfg = _lake(tmp_path)
    dense = date(2026, 7, 16)
    sparse = date(2026, 7, 22)
    bars = [f"{i:06d}.SH" for i in range(600000, 600005)]
    for d in (dense, sparse):
        _write_day(cfg.curated_root, "daily_bars", d, bars, source="tdx", schema=DAILY_BARS_SCHEMA)
    _write_day(
        cfg.curated_root,
        "valuation_metrics",
        dense,
        bars,
        source="eastmoney",
        schema=VALUATION_METRICS_SCHEMA,
        partition_value="2026-07",
        file_name="dense.parquet",
    )
    _write_day(
        cfg.curated_root,
        "valuation_metrics",
        sparse,
        bars[:1],
        source="baostock",
        schema=VALUATION_METRICS_SCHEMA,
        partition_value="2026-07",
        file_name="sparse.parquet",
    )

    assert last_dense_valuation_date(cfg) == dense


def test_history_end_caps_at_complete_em_tip(tmp_path):
    cfg = _lake(tmp_path)
    tip = date(2026, 7, 16)
    bars = [f"{i:06d}.SH" for i in range(600000, 600005)]
    _write_day(cfg.curated_root, "daily_bars", tip, bars, source="tdx", schema=DAILY_BARS_SCHEMA)
    _write_day(
        cfg.curated_root,
        "valuation_metrics",
        tip,
        bars,
        source="eastmoney",
        schema=VALUATION_METRICS_SCHEMA,
    )

    assert _valuation_history_end(cfg, date(2026, 7, 24)) == tip


def test_watermark_date_ignores_sparse_tip(tmp_path):
    cfg = _lake(tmp_path)
    dense = date(2026, 7, 16)
    sparse = date(2026, 7, 22)
    bars = [f"{i:06d}.SH" for i in range(600000, 600010)]
    for d in (dense, sparse):
        _write_day(cfg.curated_root, "daily_bars", d, bars, source="tdx", schema=DAILY_BARS_SCHEMA)
    _write_day(
        cfg.curated_root,
        "valuation_metrics",
        dense,
        bars,
        source="eastmoney",
        schema=VALUATION_METRICS_SCHEMA,
    )
    _write_day(
        cfg.curated_root,
        "valuation_metrics",
        sparse,
        bars[:2],
        source="baostock",
        schema=VALUATION_METRICS_SCHEMA,
    )

    assert _watermark_date_for(cfg, "valuation_metrics", "trade_date") == dense

    state = StateStore(cfg.meta_root)
    state.set_date("valuation_metrics", sparse)
    findings = _reconcile_watermarks(cfg)
    assert state.get_date("valuation_metrics") == dense
    assert any(f["check"] == "valuation_watermark_coverage_gate" for f in findings)


def test_baostock_single_flight_refuses_overlap(tmp_path):
    from cnequity.orchestrator.run_lock import run_lock
    from cnequity.steps.fundamentals import _backfill_valuation_metrics

    cfg = _lake(tmp_path)
    with run_lock(cfg.meta_root, "baostock"):
        out = _backfill_valuation_metrics(cfg, date(2026, 7, 24), "run-1")
    assert out["status"] == "warning"
    assert out["rows_written"] == 0
    assert "baostock lock" in out["note"]
    assert out["context_updates"]["audit_findings"][0]["check"] == "baostock_single_flight"


def _em_tip_lake(tmp_path, tip: date, symbols: list[str]):
    cfg = _lake(tmp_path)
    _write_day(cfg.curated_root, "daily_bars", tip, symbols, source="tdx", schema=DAILY_BARS_SCHEMA)
    _write_day(
        cfg.curated_root,
        "valuation_metrics",
        tip,
        symbols,
        source="eastmoney",
        schema=VALUATION_METRICS_SCHEMA,
    )
    return cfg


def test_an_outage_fill_reaches_only_the_days_eastmoney_missed(tmp_path):
    """push2 failed every host from 2026-09-22; the last EM day was 09-21."""
    from cnequity.steps.fundamentals import _em_outage_window

    symbols = [f"{i:06d}.SH" for i in range(600000, 600005)]
    cfg = _em_tip_lake(tmp_path, date(2026, 9, 21), symbols)

    # Starts after the last EastMoney day, never on the run day it still owns.
    assert _em_outage_window(cfg, date(2026, 9, 26), date(2026, 9, 1), date(2026, 9, 30)) == (
        date(2026, 9, 22),
        date(2026, 9, 25),
    )


def test_an_outage_fill_needs_an_end_and_an_eastmoney_anchor(tmp_path):
    import pytest

    from cnequity.steps.fundamentals import _em_outage_window

    with pytest.raises(RuntimeError, match="--end"):
        _em_outage_window(_lake(tmp_path), date(2026, 9, 26), date(2026, 9, 22), None)
    with pytest.raises(RuntimeError, match="anchor"):
        _em_outage_window(_lake(tmp_path), date(2026, 9, 26), date(2026, 9, 22), date(2026, 9, 24))


def _outage_lake(tmp_path, symbols: list[str], sessions: list[date]):
    cfg = _em_tip_lake(tmp_path, date(2026, 9, 21), symbols)
    for d in sessions:
        _write_day(
            cfg.curated_root, "daily_bars", d, symbols, source="tdx", schema=DAILY_BARS_SCHEMA
        )
    set_backfill(cfg, start=sessions[0], end=sessions[-1])
    set_run_flags(cfg, valuation_fill_em_outage=True)
    return cfg


def _dc_frame(symbols: list[str], sessions: list[date]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [s for d in sessions for s in symbols],
            "trade_date": [d for d in sessions for _ in symbols],
            "pe_ttm": 20.0,
            "pb": 2.0,
            "ps_ttm": 3.0,
            "total_mv": 1e9,
            "float_mv": 5e8,
        }
    )


_OUTAGE = [date(2026, 9, 22), date(2026, 9, 23), date(2026, 9, 24)]


def test_an_outage_fill_reads_datacenter_including_beijing(tmp_path, monkeypatch):
    from cnequity.steps import fundamentals

    symbols = ["600000.SH", "000001.SZ", "920571.BJ"]
    cfg = _outage_lake(tmp_path, symbols, _OUTAGE)
    monkeypatch.setattr(fundamentals, "load_symbols", lambda _cfg: symbols)
    monkeypatch.setattr(
        "cnequity.storage.repairs.valuation_orphans.purge_valuation_orphan_symbols", lambda _cfg: {}
    )
    asked: list[tuple] = []

    def _dc(start, end=None, *, client=None, config=None):
        asked.append((start, end))
        return _dc_frame(symbols + ["300999.SZ"], _OUTAGE)  # an extra name is ignored

    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.valuation_datacenter.fetch_valuation_datacenter", _dc
    )
    written: list[pl.DataFrame] = []

    def _write(config, run_id, dataset, df, **kwargs):
        written.append(df)
        assert kwargs["source"] == "eastmoney_datacenter"
        return {"rows_read": df.height, "rows_written": df.height}

    monkeypatch.setattr(fundamentals, "write_fetched", _write)
    monkeypatch.setattr(
        "cnequity.adapters.baostock.valuation.fetch_valuation_history",
        lambda *a, **k: pytest.fail("the outage fill must not ask baostock"),
    )

    out = fundamentals._backfill_valuation_metrics_locked(cfg, date(2026, 9, 26), "run-dc")
    assert asked == [(date(2026, 9, 22), date(2026, 9, 24))]
    assert out["rows_written"] == 9 and out["source"] == "eastmoney_datacenter"
    frame = written[0]
    assert set(frame["symbol"]) == set(symbols)
    assert set(frame["source"]) == {"eastmoney_datacenter"}


def test_an_incomplete_outage_fill_retains_valid_rows_and_gap(tmp_path, monkeypatch):
    from cnequity.steps import fundamentals

    symbols = ["600000.SH", "920571.BJ"]
    cfg = _outage_lake(tmp_path, symbols, _OUTAGE)
    monkeypatch.setattr(fundamentals, "load_symbols", lambda _cfg: symbols)
    monkeypatch.setattr(
        "cnequity.storage.repairs.valuation_orphans.purge_valuation_orphan_symbols", lambda _cfg: {}
    )
    # datacenter has not published the last session for one name yet.
    partial = _dc_frame(symbols, _OUTAGE).filter(
        ~((pl.col("symbol") == "920571.BJ") & (pl.col("trade_date") == date(2026, 9, 24)))
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.valuation_datacenter.fetch_valuation_datacenter",
        lambda *a, **k: partial,
    )
    written = []

    def write(config, run, dataset, frame, **kwargs):
        written.append(frame)
        return {"rows_read": frame.height, "rows_written": frame.height}

    monkeypatch.setattr(fundamentals, "write_fetched", write)
    from cnequity.orchestrator.source_gaps import source_gap_scope

    with source_gap_scope(cfg) as gaps:
        result = fundamentals._backfill_valuation_metrics_locked(cfg, date(2026, 9, 26), "run-dc")
    assert result["rows_written"] == 5
    assert written[0].height == 5
    assert gaps[0]["dates"] == ["2026-09-24"]


def test_the_ordinary_backfill_does_not_ask_baostock_for_beijing_names(tmp_path, monkeypatch):
    """baostock has no BJ data; asking fails every BJ symbol after retries."""
    from cnequity.steps import fundamentals

    symbols = ["600000.SH", "000001.SZ", "920229.BJ"]
    cfg = _em_tip_lake(tmp_path, date(2026, 9, 21), symbols)
    set_backfill(cfg, start=date(2026, 9, 14), end=date(2026, 9, 18))
    monkeypatch.setattr(fundamentals, "load_symbols", lambda _cfg: symbols)
    monkeypatch.setattr(
        "cnequity.storage.repairs.valuation_orphans.purge_valuation_orphan_symbols", lambda _cfg: {}
    )
    asked: list[str] = []

    def _fetch(batch, start, end, config=None):
        asked.extend(batch)
        return _dc_frame(batch, [date(2026, 9, 14)]), []

    monkeypatch.setattr("cnequity.adapters.baostock.valuation.fetch_valuation_history", _fetch)
    monkeypatch.setattr(
        fundamentals, "write_fetched", lambda *a, **k: {"rows_read": 2, "rows_written": 2}
    )

    out = fundamentals._backfill_valuation_metrics_locked(cfg, date(2026, 9, 26), "run-bj")
    assert sorted(asked) == ["000001.SZ", "600000.SH"]
    assert out["baostock_unserved"] == 1
    assert "failed_symbols" not in out


# ---- daily push2 → datacenter fallback -----------------------------------------


def test_daily_valuation_reads_datacenter_first(tmp_path, monkeypatch):
    from cnequity.adapters.eastmoney import valuation

    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.valuation_datacenter.fetch_valuation_datacenter",
        lambda d, *a, **k: _dc_frame(["600000.SH", "920571.BJ"], [d]),
    )
    monkeypatch.setattr(
        valuation,
        "_fetch_valuation_push2",
        lambda *a, **k: pytest.fail("push2 must not be asked while datacenter answers"),
    )
    df = valuation.fetch_valuation_metrics(date(2026, 9, 28), config=_lake(tmp_path))
    assert df.height == 2
    assert set(df["source"]) == {"eastmoney_datacenter"}


def test_daily_valuation_falls_back_to_push2_when_datacenter_has_nothing(tmp_path, monkeypatch):
    """datacenter may not have published the session yet at run time."""
    from cnequity.adapters.eastmoney import valuation

    monkeypatch.setattr(valuation, "last_closed_session", lambda: date(2026, 9, 28))
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.valuation_datacenter.fetch_valuation_datacenter",
        lambda *a, **k: _dc_frame([], []),
    )
    monkeypatch.setattr(
        valuation, "_fetch_valuation_push2", lambda d, **k: _dc_frame(["600000.SH"], [d])
    )
    df = valuation.fetch_valuation_metrics(date(2026, 9, 28), config=_lake(tmp_path))
    assert df["symbol"].to_list() == ["600000.SH"]
    assert "source" not in df.columns  # the step stamps push2 rows "eastmoney"


@pytest.mark.parametrize("live_session", [date(2026, 9, 29), None], ids=["later", "open"])
def test_daily_valuation_never_stamps_the_live_snapshot_on_another_session(
    tmp_path, monkeypatch, live_session
):
    """A catch-up over 07-16..07-24 once wrote one live push2 snapshot to every day."""
    from cnequity.adapters.eastmoney import valuation
    from cnequity.orchestrator.outcomes import SourceUnavailableError

    monkeypatch.setattr(valuation, "last_closed_session", lambda: live_session)
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.valuation_datacenter.fetch_valuation_datacenter",
        lambda *a, **k: _dc_frame([], []),
    )
    monkeypatch.setattr(
        valuation,
        "_fetch_valuation_push2",
        lambda *a, **k: pytest.fail("push2 describes the live session only"),
    )
    with pytest.raises(SourceUnavailableError, match="no valuation for 2026-09-28"):
        valuation.fetch_valuation_metrics(date(2026, 9, 28), config=_lake(tmp_path))


def test_daily_valuation_keeps_datacenter_error_when_push2_fails_too(tmp_path, monkeypatch):
    from cnequity.adapters.eastmoney import valuation
    from cnequity.adapters.eastmoney.em_auth import Push2PausedError

    monkeypatch.setattr(valuation, "last_closed_session", lambda: date(2026, 9, 28))

    def _dc(*a, **k):
        raise RuntimeError("datacenter 502")

    def _push2(*a, **k):
        raise Push2PausedError("paused")

    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.valuation_datacenter.fetch_valuation_datacenter", _dc
    )
    monkeypatch.setattr(valuation, "_fetch_valuation_push2", _push2)
    with pytest.raises(RuntimeError, match="datacenter 502"):
        valuation.fetch_valuation_metrics(date(2026, 9, 28), config=_lake(tmp_path))


def test_a_datacenter_day_counts_as_an_eastmoney_day(tmp_path):
    symbols = [f"{i:06d}.SH" for i in range(600000, 600005)]
    cfg = _em_tip_lake(tmp_path, date(2026, 9, 21), symbols)
    d = date(2026, 9, 22)
    _write_day(cfg.curated_root, "daily_bars", d, symbols, source="tdx", schema=DAILY_BARS_SCHEMA)
    _write_day(
        cfg.curated_root,
        "valuation_metrics",
        d,
        symbols,
        source="eastmoney_datacenter",
        schema=VALUATION_METRICS_SCHEMA,
    )
    assert last_complete_em_valuation_tip(cfg) == d


def test_an_outage_fill_only_fills_sessions_the_lake_lacks(tmp_path, monkeypatch):
    """Rows already held for the window stay put, whatever their history looks like."""
    from cnequity.steps import fundamentals

    symbols = ["600000.SH", "920571.BJ"]
    cfg = _outage_lake(tmp_path, symbols, _OUTAGE)
    for d in _OUTAGE:  # baostock already filled SH/SZ for the window
        _write_day(
            cfg.curated_root,
            "valuation_metrics",
            d,
            ["600000.SH"],
            source="baostock",
            schema=VALUATION_METRICS_SCHEMA,
        )
    monkeypatch.setattr(fundamentals, "load_symbols", lambda _cfg: symbols)
    monkeypatch.setattr(
        "cnequity.storage.repairs.valuation_orphans.purge_valuation_orphan_symbols", lambda _cfg: {}
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.valuation_datacenter.fetch_valuation_datacenter",
        lambda *a, **k: _dc_frame(symbols, _OUTAGE),
    )
    written: list[pl.DataFrame] = []
    monkeypatch.setattr(
        fundamentals,
        "write_fetched",
        lambda config, run_id, dataset, df, **k: (
            written.append(df) or {"rows_read": df.height, "rows_written": df.height}
        ),
    )

    out = fundamentals._backfill_valuation_metrics_locked(cfg, date(2026, 9, 26), "run-dc")
    assert out["symbols_todo"] == 1
    assert set(written[0]["symbol"]) == {"920571.BJ"}
    assert written[0].height == 3


# ---- daily run fills missed sessions --------------------------------------------


def _gap_lake(tmp_path):
    symbols = ["600000.SH", "000001.SZ"]
    cfg = _em_tip_lake(tmp_path, date(2026, 9, 21), symbols)
    for d in (date(2026, 9, 22), date(2026, 9, 23), date(2026, 9, 24)):
        _write_day(
            cfg.curated_root, "daily_bars", d, symbols, source="tdx", schema=DAILY_BARS_SCHEMA
        )
    return cfg, symbols


def test_the_daily_run_reads_back_missed_sessions(tmp_path, monkeypatch):
    from cnequity.steps import fundamentals

    cfg, symbols = _gap_lake(tmp_path)
    monkeypatch.setattr(fundamentals, "load_symbols", lambda _cfg: symbols)
    asked: list[tuple] = []

    def _dc(start, end=None, *, client=None, config=None):
        asked.append((start, end))
        return _dc_frame(symbols, [date(2026, 9, 22), date(2026, 9, 23)])

    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.valuation_datacenter.fetch_valuation_datacenter", _dc
    )
    monkeypatch.setattr(
        fundamentals, "write_fetched", lambda *a, **k: {"rows_read": 4, "rows_written": 4}
    )
    gap_result = {
        "rows_written": 2,
        "context_updates": {
            "audit_findings": [{"check": "coverage_gap", "gap_dates": ["2026-09-22", "2026-09-23"]}]
        },
    }
    monkeypatch.setattr(fundamentals, "run_incremental_fetched", lambda *a, **k: dict(gap_result))

    out = fundamentals.step_valuation_metrics(cfg, date(2026, 9, 24), "run-gap", {})
    assert asked == [(date(2026, 9, 22), date(2026, 9, 23))]
    assert out["gap_fill"]["sessions"] == ["2026-09-22", "2026-09-23"]
    assert not (out.get("context_updates") or {}).get("audit_findings")


def test_no_gap_means_no_extra_request(tmp_path, monkeypatch):
    from cnequity.steps import fundamentals

    cfg, symbols = _gap_lake(tmp_path)
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.valuation_datacenter.fetch_valuation_datacenter",
        lambda *a, **k: pytest.fail("no missed session, no read-back"),
    )
    monkeypatch.setattr(
        fundamentals, "run_incremental_fetched", lambda *a, **k: {"rows_written": 2}
    )
    out = fundamentals.step_valuation_metrics(cfg, date(2026, 9, 22), "run-nogap", {})
    assert "gap_fill" not in out


def test_a_failed_read_back_does_not_block_the_day(tmp_path, monkeypatch):
    from cnequity.steps import fundamentals

    cfg, symbols = _gap_lake(tmp_path)
    monkeypatch.setattr(fundamentals, "load_symbols", lambda _cfg: symbols)

    def _dc(*a, **k):
        raise RuntimeError("datacenter 502")

    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.valuation_datacenter.fetch_valuation_datacenter", _dc
    )
    monkeypatch.setattr(
        fundamentals, "run_incremental_fetched", lambda *a, **k: {"rows_written": 2}
    )
    out = fundamentals.step_valuation_metrics(cfg, date(2026, 9, 24), "run-fail", {})
    assert out["rows_written"] == 2
    assert out["context_updates"]["audit_findings"][0]["check"] == "valuation_gap_fill_failed"
