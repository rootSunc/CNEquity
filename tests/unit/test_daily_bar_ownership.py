from datetime import date

from cnequity.config import Config
from cnequity.orchestrator.manifest import Manifest
from cnequity.steps.bars import _record_delegated_ownership_batch
from cnequity.steps.common import classify_daily_bar_ownership


def test_daily_bar_ownership_is_explicit_for_every_symbol():
    symbols = ["600001.SH", "600002.SH", "600003.SH", "600004.SH"]
    spans = {
        "600001.SH": (date(2000, 1, 1), None, "stock"),
        "600002.SH": (date(2000, 1, 1), date(2015, 12, 31), "stock"),
        "600003.SH": (date(2000, 1, 1), date(2020, 6, 1), "stock"),
        "600004.SH": (date(2025, 1, 1), None, "stock"),
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
    assert set(
        result.generic + result.delegated_delisted + result.expected_no_data + result.placeholder
    ) == set(symbols)


def test_unlisted_etf_placeholder_is_separate_bucket():
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
    symbols = ["510300.SH"]
    spans = {"510300.SH": (None, None, "etf")}

    result = classify_daily_bar_ownership(
        symbols,
        spans,
        date(2026, 8, 18),
        date(2026, 8, 18),
        bar_universe={"510300.SH"},
    )

    assert result.generic == ["510300.SH"]


def test_unlisted_etf_without_bar_universe_stays_generic():
    """Without a traded-bar universe the classifier stays conservative."""
    symbols = ["589430.SH"]
    spans = {"589430.SH": (None, None, "etf")}

    result = classify_daily_bar_ownership(
        symbols,
        spans,
        date(2026, 8, 18),
        date(2026, 8, 18),
    )

    assert result.generic == ["589430.SH"]


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
