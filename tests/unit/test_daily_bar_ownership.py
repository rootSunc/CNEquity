from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.orchestrator.manifest import Manifest
from cnequity.steps.bars import _record_delegated_ownership_batch, step_daily_bars
from cnequity.steps.common import classify_daily_bar_ownership
from cnequity.storage.layout import init_data_layout


def test_daily_bar_ownership_is_explicit_for_every_symbol():
    symbols = ["600001.SH", "600002.SH", "600003.SH", "600004.SH"]
    spans = {
        "600001.SH": (date(2000, 1, 1), None),
        "600002.SH": (date(2000, 1, 1), date(2015, 12, 31)),
        "600003.SH": (date(2000, 1, 1), date(2020, 6, 1)),
        "600004.SH": (date(2025, 1, 1), None),
    }

    result = classify_daily_bar_ownership(
        symbols,
        spans,
        date(2016, 1, 1),
        date(2024, 12, 31),
    )

    assert result.generic == ["600001.SH"]
    assert result.delegated_delisted == ["600003.SH"]
    assert result.expected_no_data == ["600002.SH", "600004.SH"]
    assert set(result.generic + result.delegated_delisted + result.expected_no_data) == set(symbols)


def test_delisted_etf_is_not_sent_to_stock_recovery_gate():
    result = classify_daily_bar_ownership(
        ["517233.SH", "600003.SH"],
        {
            "517233.SH": (None, date(2026, 8, 18)),
            "600003.SH": (date(2000, 1, 1), date(2026, 8, 18)),
        },
        date(2026, 8, 15),
        date(2026, 8, 21),
    )

    assert result.generic == ["517233.SH"]
    assert result.delegated_delisted == ["600003.SH"]


def test_unlisted_etf_placeholder_is_not_claimed_as_verified_no_data():
    symbols = ["589430.SH", "588200.SH"]
    spans = {
        "589430.SH": (None, None, "etf"),
        "588200.SH": (date(2022, 10, 26), None, "etf"),
    }

    result = classify_daily_bar_ownership(
        symbols,
        spans,
        date(2026, 8, 18),
        date(2026, 8, 18),
        bar_universe={"588200.SH"},
    )

    assert result.placeholder == ["589430.SH"]
    assert result.expected_no_data == []
    assert result.generic == ["588200.SH"]


def test_traded_etf_without_list_date_stays_generic():
    result = classify_daily_bar_ownership(
        ["510300.SH"],
        {"510300.SH": (None, None, "etf")},
        date(2026, 8, 18),
        date(2026, 8, 18),
        bar_universe={"510300.SH"},
    )

    assert result.generic == ["510300.SH"]


def test_unlisted_etf_without_bar_universe_stays_generic():
    """Without a traded-bar universe the classifier stays conservative."""
    result = classify_daily_bar_ownership(
        ["589430.SH"],
        {"589430.SH": (None, None, "etf")},
        date(2026, 8, 18),
        date(2026, 8, 18),
    )

    assert result.generic == ["589430.SH"]


def _etf_retry_lake(tmp_path, **config_kwargs) -> tuple[Config, str]:
    """A lake whose only failed daily_bars batch holds one undated ETF code."""
    cfg = Config(data_root=tmp_path / "data", workers=1, **config_kwargs)
    init_data_layout(cfg)
    instruments = cfg.curated_root / "instruments"
    instruments.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["589430.SH", "600519.SH"],
            "name": ["某基金", "贵州茅台"],
            "asset_type": ["etf", "stock"],
            "list_date": [None, date(2001, 8, 27)],
            "delist_date": [None, None],
        }
    ).write_parquet(instruments / "part-merged.parquet")
    bars = cfg.curated_root / "daily_bars" / "trade_date=2024-06-27"
    bars.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 27)],
            "volume": [100],
        }
    ).write_parquet(bars / "part-0.parquet")

    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily:core")
    manifest.start_batch(
        run_id,
        "placeholder-retry",
        task_id="daily_bars",
        dataset="daily_bars",
        symbols=["589430.SH"],
        window_start="2024-06-28",
        window_end="2024-06-28",
    )
    manifest.finish_batch(
        run_id,
        "placeholder-retry",
        "failed",
        error_message="TDX returned no rows",
    )
    return cfg, run_id


_ETF_RETRY_CONTEXT = {
    "_retry_batch_specs": [
        ("placeholder-retry", ["589430.SH"], date(2024, 6, 28), date(2024, 6, 28))
    ]
}


def test_retry_only_etf_batch_is_superseded_when_outside_the_ingest_universe(tmp_path):
    """The default ingest scope holds no ETF code, so there is nothing to retry.

    Leaving the batch failed would block compaction forever over a code this
    lake no longer fetches, so it is resolved without touching a vendor.
    """
    cfg, run_id = _etf_retry_lake(tmp_path)
    assert cfg.ingest_universe == "all_a"

    result = step_daily_bars(cfg, date(2024, 6, 28), run_id, dict(_ETF_RETRY_CONTEXT))

    batch = Manifest(cfg.manifest_path).get_batch(run_id, "placeholder-retry")
    assert batch["status"] == "superseded"
    assert "ingest-universe-excluded" in (batch["error_message"] or "")
    assert result["rows_written"] == 0


def test_retry_only_etf_placeholder_is_audited_and_unblocks_original_batch(tmp_path):
    """`all_instruments` keeps ETF quotes in scope, so the placeholder audit runs."""
    cfg, run_id = _etf_retry_lake(tmp_path, ingest_universe="all_instruments")

    result = step_daily_bars(cfg, date(2024, 6, 28), run_id, dict(_ETF_RETRY_CONTEXT))

    assert Manifest(cfg.manifest_path).get_batch(run_id, "placeholder-retry")["status"] == (
        "superseded"
    )
    assert result["context_updates"]["daily_bars_ownership"]["placeholder"] == 1
    assert any(
        finding["check"] == "daily_bars_placeholder_skipped"
        for finding in result["context_updates"]["audit_findings"]
    )


def test_incomplete_delisted_ownership_blocks_compaction_and_retries(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    run_id = "run-ownership"
    batch_id = "ownership-retry"
    monkeypatch.setattr("cnequity.steps.delisted.delisted_recovery_covers", lambda *args: False)

    assert (
        _record_delegated_ownership_batch(
            cfg,
            run_id,
            ["600003.SH"],
            date(2016, 1, 1),
            date(2024, 12, 31),
            batch_id=batch_id,
        )
        is False
    )
    manifest = Manifest(cfg.manifest_path)
    first = manifest.get_batch(run_id, batch_id)
    assert first["status"] == "warning"
    assert first["blocks_compaction"] == 1

    monkeypatch.setattr("cnequity.steps.delisted.delisted_recovery_covers", lambda *args: True)
    assert (
        _record_delegated_ownership_batch(
            cfg,
            run_id,
            ["600003.SH"],
            date(2016, 1, 1),
            date(2024, 12, 31),
            batch_id=batch_id,
        )
        is True
    )
    assert manifest.get_batch(run_id, batch_id)["status"] == "success"


def test_a_never_traded_code_is_not_a_coverage_obligation_despite_positive_status():
    """`trading_status` publishes `is_trading` for a whole universe off a
    *suspension list*, so a code that is merely not suspended reads as trading
    normally — including one that has never had a session at all.

    Two such codes (301686.SZ, 688837.SH; TDX published them days early, then
    dropped them, leaving rows no list_date enrichment can reach) held the
    2026-09-15 market snapshot hostage: neither exchange board carried them,
    EastMoney's kline had nothing, and the run refused to checkpoint. Storage
    already declines the mirror image of this — a security that never printed a
    bar cannot have stopped trading — and the same fact settles it here.
    """
    status = pl.DataFrame(
        {
            "symbol": ["301686.SZ", "600519.SH"],
            "trade_date": [date(2026, 9, 15)] * 2,
            "is_trading": [True, True],
        }
    )

    result = classify_daily_bar_ownership(
        ["301686.SZ", "600519.SH"],
        {"301686.SZ": (None, None, "stock"), "600519.SH": (None, None, "stock")},
        date(2026, 9, 15),
        date(2026, 9, 15),
        bar_universe={"600519.SH"},
        trading_status=status,
        trading_sessions=[date(2026, 9, 15)],
    )

    assert result.placeholder == ["301686.SZ"]
    # Never claimed as proven no-data, and a name that has traded before keeps
    # its obligation — a real fetch miss must still block the snapshot.
    assert result.expected_no_data == []
    assert result.generic == ["600519.SH"]


def test_a_dated_code_that_has_never_traded_still_blocks():
    """The escape hatch is only for codes with no listing date. Once a listing
    date exists the span rules decide, so a genuine first-session miss is still
    a coverage obligation rather than a silently skipped placeholder."""
    result = classify_daily_bar_ownership(
        ["301686.SZ"],
        {"301686.SZ": (date(2026, 9, 15), None, "stock")},
        date(2026, 9, 15),
        date(2026, 9, 15),
        bar_universe=set(),
        trading_status=pl.DataFrame(
            {
                "symbol": ["301686.SZ"],
                "trade_date": [date(2026, 9, 15)],
                "is_trading": [True],
            }
        ),
        trading_sessions=[date(2026, 9, 15)],
    )

    assert result.placeholder == []
    assert result.generic == ["301686.SZ"]
