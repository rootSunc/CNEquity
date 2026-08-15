"""Read-only evidence for historical delisting coverage."""

import json
from datetime import date

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.steps.delisted import catalog_path, delisted_coverage_report


def _cfg(tmp_path, catalog: dict[str, str]) -> Config:
    cfg = Config(data_root=tmp_path / "data", sources={"sina": True})
    path = catalog_path(cfg)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"delisted": catalog, "never_issued": []}))
    return cfg


def _write_bars(cfg: Config, symbol: str, *days: date, volume: int = 100) -> None:
    for day in days:
        part = cfg.curated_root / "daily_bars" / f"trade_date={day.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        path = part / "part-merged.parquet"
        incoming = pl.DataFrame({"symbol": [symbol], "trade_date": [day], "volume": [volume]})
        if path.exists():
            incoming = pl.concat([pl.read_parquet(path), incoming])
        incoming.write_parquet(path)


def _write_instruments(cfg: Config, rows: list[tuple[str, date | None]]) -> None:
    root = cfg.curated_root / "instruments"
    root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": [row[0] for row in rows],
            "delist_date": pl.Series([row[1] for row in rows], dtype=pl.Date),
        }
    ).write_parquet(root / "part-merged.parquet")


def test_coverage_verifies_definite_and_bar_proven_overlap(tmp_path, monkeypatch):
    cfg = _cfg(
        tmp_path,
        {"600001.SH": "2020-01-03", "600002.SH": "2025-02-03"},
    )
    _write_bars(cfg, "600001.SH", date(2019, 1, 2), date(2020, 1, 3))
    _write_bars(cfg, "600001.SH", date(2020, 1, 6), volume=0)
    _write_bars(cfg, "600002.SH", date(2023, 5, 4), date(2024, 12, 31))
    # Anchor catalogue ageing to the lake rather than the wall clock.
    _write_bars(cfg, "600519.SH", date(2026, 7, 24))
    _write_instruments(
        cfg,
        [("600001.SH", date(2020, 1, 3)), ("600002.SH", date(2025, 2, 3))],
    )
    monkeypatch.setattr("cn_market_lake.steps.delisted.pending_codes", lambda cfg: [])

    report = delisted_coverage_report(cfg, date(2019, 1, 1), date(2024, 12, 31))

    assert report["verified"] is True
    assert report["counts"]["catalogue_candidates"] == 2
    assert report["counts"]["proven_overlap"] == 2


def test_coverage_separates_definite_unknown_terminal_and_identity_gaps(tmp_path, monkeypatch):
    cfg = _cfg(
        tmp_path,
        {
            "600001.SH": "2020-01-03",  # definite, no bars or instrument
            "600002.SH": "2025-02-03",  # after window, overlap unknown
            "600003.SH": "2021-06-07",  # observed terminal mismatch
        },
    )
    _write_bars(cfg, "600003.SH", date(2019, 2, 1), date(2021, 6, 4))
    _write_bars(cfg, "600519.SH", date(2026, 7, 24))
    _write_instruments(cfg, [("600003.SH", None)])
    monkeypatch.setattr("cn_market_lake.steps.delisted.pending_codes", lambda cfg: [])

    report = delisted_coverage_report(cfg, date(2019, 1, 1), date(2024, 12, 31))

    assert report["verified"] is False
    assert report["counts"]["missing_bars"] == 1
    assert report["counts"]["unknown_overlap"] == 1
    assert report["counts"]["terminal_mismatch"] == 1
    assert report["counts"]["missing_instrument"] == 1
    assert report["counts"]["invalid_delist_date"] == 1


def test_pending_discovery_blocks_an_otherwise_complete_report(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path, {"600001.SH": "2020-01-03"})
    _write_bars(cfg, "600001.SH", date(2019, 1, 2), date(2020, 1, 3))
    _write_bars(cfg, "600519.SH", date(2026, 7, 24))
    _write_instruments(cfg, [("600001.SH", date(2020, 1, 3))])
    monkeypatch.setattr("cn_market_lake.steps.delisted.pending_codes", lambda cfg: ["600999.SH"])

    report = delisted_coverage_report(cfg, date(2019, 1, 1), date(2024, 12, 31))

    assert report["known_coverage_complete"] is True
    assert report["discovery_complete"] is False
    assert report["verified"] is False
    assert report["samples"]["pending_probe"] == ["600999.SH"]
