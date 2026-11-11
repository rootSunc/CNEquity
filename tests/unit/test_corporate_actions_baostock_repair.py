"""Tests for the explicit Baostock delisted corporate-action repair path."""

from datetime import date

import polars as pl

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.run_scope import set_backfill, set_run_flags
from cnequity.steps import events


def _repair_rows() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["300114.SZ"],
            "ex_date": [date(2020, 8, 11)],
            "action_type": ["cash_dividend"],
            "cash_dividend": [0.049538],
            "bonus_ratio": [0.0],
            "transfer_ratio": [0.0],
            "allotment_ratio": [None],
            "allotment_price": [None],
        },
        schema_overrides={"allotment_ratio": pl.Float64, "allotment_price": pl.Float64},
    )


def test_baostock_repair_is_scoped_to_delisted_sh_sz_and_keeps_provenance(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "lake",
        sources={"baostock": True},
        raw_archive_enabled=False,
    )
    set_backfill(cfg, active=True, start=date(2020, 1, 1), end=date(2020, 12, 31))

    monkeypatch.setattr(
        events,
        "load_symbols",
        lambda _cfg: ["300114.SZ", "430090.BJ", "600000.SH"],
    )
    monkeypatch.setattr(
        events,
        "instrument_metadata",
        lambda _cfg: pl.DataFrame(
            {
                "symbol": ["300114.SZ", "430090.BJ", "600000.SH"],
                "list_date": [date(2010, 1, 1)] * 3,
                "delist_date": [date(2025, 2, 17), date(2025, 9, 30), None],
            }
        ),
    )
    monkeypatch.setattr(events, "fetch_corporate_actions", lambda *args, **kwargs: pl.DataFrame())
    seen = []

    def fake_baostock(symbols, start, end, **kwargs):
        seen.append((symbols, start, end))
        return _repair_rows(), []

    monkeypatch.setattr(events, "fetch_corporate_actions_baostock", fake_baostock)
    set_run_flags(cfg, ca_baostock_repair=True)

    result = events.step_corporate_actions(cfg, date(2020, 12, 31), "run-1", {})

    assert seen == [(["300114.SZ"], date(2020, 1, 1), date(2020, 12, 31))]
    assert result["rows_written"] == 1
    staged = list((cfg.staging_root / "corporate_actions").glob("**/*.parquet"))
    assert len(staged) == 1
    assert pl.read_parquet(staged[0])["source"].to_list() == ["baostock"]
