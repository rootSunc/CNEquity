"""Datasets the audit treats as already behind."""

from datetime import date

import polars as pl


def test_a_dataset_removed_from_the_schedule_counts_as_behind(tmp_path, monkeypatch):
    from cnequity.config import Config
    from cnequity.domain import datasets
    from cnequity.quality import audit
    from cnequity.query import reader

    cfg = Config(data_root=tmp_path / "data")
    monkeypatch.setattr(
        reader,
        "list_datasets",
        lambda config=None: pl.DataFrame(
            {
                "dataset": ["sector_fund_flow", "daily_bars"],
                "has_data": [True, True],
                "watermarked": [True, True],
                "watermark": [date(2026, 9, 21), date(2026, 10, 30)],
                "coverage_end": [date(2026, 9, 21), date(2026, 10, 30)],
            }
        ),
    )
    monkeypatch.setattr(datasets, "is_dataset_enabled", lambda name, config: True)
    monkeypatch.setattr(datasets, "is_stale", lambda name, mark, anchor, config=None: False)
    monkeypatch.setattr(
        datasets, "removed_from_schedule", lambda config, name: name == "sector_fund_flow"
    )
    monkeypatch.setattr(audit, "_last_trading_day", lambda config, day: day)
    assert audit._stale_dataset_names(cfg, date(2026, 10, 30)) == {"sector_fund_flow"}
