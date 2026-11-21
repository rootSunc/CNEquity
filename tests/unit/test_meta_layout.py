"""What an older lake kept in ``meta/state/`` moves to each record's own directory."""

from __future__ import annotations

import json
import tarfile
from pathlib import Path

from cnequity.config import Config
from cnequity.storage.lake_record import lake_path, read_lake
from cnequity.storage.repairs.meta_layout import repair_meta_layout


def _old_lake(tmp_path) -> Config:
    cfg = Config(data_root=tmp_path / "data")
    state = cfg.meta_root / "state"
    (state / "scheduler").mkdir(parents=True)
    (state / "scheduler" / "daily-2026-11-20.done").write_text("", encoding="utf-8")
    (state / "web_scheduler").mkdir()
    (state / "web_scheduler" / "settings.json").write_text("{}", encoding="utf-8")
    (state / "web_scheduler" / "timer.out.log").write_text("log\n", encoding="utf-8")
    (state / "historical_st_evidence" / "v2").mkdir(parents=True)
    (state / "historical_st_evidence" / "v2" / "x.json").write_text("{}", encoding="utf-8")

    def write(name: str, payload) -> None:
        (state / name).write_text(json.dumps(payload), encoding="utf-8")

    write("lake.json", {"format": 1, "migrations": {"lake-catalog": {"status": "applied"}}})
    write("daily_bars.json", {"missing_ranges": [], "last_success_trade_date": "2026-11-20"})
    write(
        "instruments.json",
        {"undated_symbols": ["000001.SZ"], "undated_checked_on": "2026-11-20", "updated_at": "x"},
    )
    write("adj_factors.json", {"source_overrides": {"600179.SH": {}}, "coverage_status": "x"})
    write("delisted_catalog.json", {"version": 1})
    write("eastmoney_guard.json", {"day": "2026-11-20"})
    write("sector_bars_backfill_em.json", {"completed": []})
    (state / "daily_bars.lock").write_text("", encoding="utf-8")
    return cfg


def test_the_old_lake_record_moves_on_first_read(tmp_path):
    cfg = _old_lake(tmp_path)
    assert read_lake(cfg)["migrations"]["lake-catalog"]["status"] == "applied"
    assert lake_path(cfg) == cfg.meta_root / "lake.json"
    assert not (cfg.meta_root / "state" / "lake.json").exists()


def test_a_preview_moves_nothing(tmp_path):
    cfg = _old_lake(tmp_path)
    report = repair_meta_layout(cfg)
    assert not report["applied"] and report["rows_changed"] > 0
    assert (cfg.meta_root / "state" / "daily_bars.json").exists()


def test_each_record_moves_to_its_owner_and_the_ledger_goes(tmp_path):
    cfg = _old_lake(tmp_path)
    read_lake(cfg)
    report = repair_meta_layout(cfg, apply=True)
    meta = cfg.meta_root
    assert report["applied"]
    assert (meta / "scheduler" / "markers" / "daily-2026-11-20.done").exists()
    assert (meta / "scheduler" / "web" / "settings.json").exists()
    assert (meta / "source_cache" / "historical_st_evidence" / "v2" / "x.json").exists()
    assert (meta / "source_cache" / "delisted_catalog.json").exists()
    assert (meta / "checkpoints" / "sector_bars_backfill_em.json").exists()
    assert json.loads((meta / "source_cache" / "instruments_undated.json").read_text()) == {
        "undated_symbols": ["000001.SZ"],
        "undated_checked_on": "2026-11-20",
    }
    assert json.loads((meta / "decisions" / "adj_factors.json").read_text()) == {
        "source_overrides": {"600179.SH": {}}
    }
    # The coverage ledger and the stale guard copy are gone; the timer's log stays
    # where its installed unit writes it.
    left = sorted(p.relative_to(meta / "state").as_posix() for p in (meta / "state").rglob("*"))
    assert left == ["web_scheduler", "web_scheduler/timer.out.log"]
    with tarfile.open(meta / report["backup"]) as archive:
        assert "state/daily_bars.json" in archive.getnames()
    assert repair_meta_layout(cfg, apply=True)["rows_changed"] == 0


def test_a_lake_without_meta_state_owes_nothing(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    Path(cfg.meta_root).mkdir(parents=True)
    assert repair_meta_layout(cfg, apply=True)["rows_changed"] == 0
