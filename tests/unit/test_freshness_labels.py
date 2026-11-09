"""`cne status --datasets` and the dashboard label freshness with one function.

Each used to keep its own copy, and they drifted: a retired source read
``retired`` in the CLI and ``fresh`` on the dashboard, and a sample lake's
synthetic rows were judged as real dates on the dashboard only.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest
from click.testing import CliRunner

from cnequity.cli.main import cli
from cnequity.serve.lake import LakeView

ANCHOR = date(2024, 9, 30)

#: One row per branch of `dataset_freshness`.
INVENTORY = [
    # dataset, has_data, watermarked, watermark
    ("daily_bars", True, True, date(2024, 9, 30)),  # fresh
    ("trading_status", True, True, date(2024, 6, 3)),  # stale
    ("northbound_flows", True, True, date(2024, 8, 16)),  # retired
    ("instruments", False, False, None),  # empty
    ("limit_prices", True, True, date(2024, 6, 3)),  # n/a: optional Tushare backfill
    ("financial_statement_items", True, False, None),  # n/a: no daily watermark
]


def _inventory(config=None) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "dataset": [row[0] for row in INVENTORY],
            "layer": ["curated"] * len(INVENTORY),
            "has_data": [row[1] for row in INVENTORY],
            "watermarked": [row[2] for row in INVENTORY],
            "coverage_start": [row[3] for row in INVENTORY],
            "coverage_end": [row[3] for row in INVENTORY],
            "watermark": [row[3] for row in INVENTORY],
        },
        schema_overrides={"coverage_start": pl.Date, "coverage_end": pl.Date, "watermark": pl.Date},
    )


def _cli_labels(config_path) -> dict[str, str]:
    result = CliRunner().invoke(cli, ["status", "--datasets", "--config", str(config_path)])
    labels = {}
    for line in result.output.splitlines():
        cells = [cell.strip() for cell in line.strip("│ ").split("┆")]
        if len(cells) > 2 and cells[0] in dict.fromkeys(row[0] for row in INVENTORY):
            labels[cells[0]] = cells[2]
    assert labels, result.output
    return labels


def _dashboard_labels(config) -> dict[str, str]:
    view = LakeView(config)
    return {
        row["dataset"]: row["freshness"]
        for row in view._rows()
        if row["dataset"] in dict.fromkeys(r[0] for r in INVENTORY)
    }


@pytest.fixture
def lake(tmp_path, monkeypatch):
    """The test config with the retired source turned on, and its path."""
    from cnequity.config import load_config

    def load(*, profile: str | None = None):
        path = tmp_path / "test.toml"
        text = (tmp_path / "base.toml").read_text(encoding="utf-8")
        if profile:
            text = text.replace("[data]\n", f'[data]\nprofile = "{profile}"\n', 1)
        path.write_text(text + '\n[datasets]\nbest_effort = ["northbound_flows"]\n', "utf-8")
        return path, load_config(path)

    (tmp_path / "base.toml").write_text(
        (tmp_path / "test.toml").read_text(encoding="utf-8"), encoding="utf-8"
    )
    monkeypatch.setattr("cnequity.query.reader.list_datasets", _inventory)
    for module in ("cnequity.orchestrator.run_window", "cnequity.serve.lake"):
        monkeypatch.setattr(f"{module}.current_anchor", lambda config, now=None: ANCHOR)
    return load


def test_the_cli_and_the_dashboard_give_every_dataset_the_same_label(config, lake):
    path, cfg = lake()
    cli_labels = _cli_labels(path)
    dashboard = _dashboard_labels(cfg)

    assert {name: label.lower() for name, label in cli_labels.items()} == dashboard
    assert dashboard == {
        "daily_bars": "fresh",
        "trading_status": "stale",
        "northbound_flows": "retired",
        "instruments": "empty",
        "limit_prices": "n/a",
        "financial_statement_items": "n/a",
    }
    assert cli_labels["trading_status"] == "STALE"


def test_a_sample_lake_is_labelled_sample_in_both(config, lake):
    path, cfg = lake(profile="sample")

    cli_labels = _cli_labels(path)
    dashboard = _dashboard_labels(cfg)

    assert cli_labels == dashboard
    assert dashboard["trading_status"] == "sample"
    assert dashboard["instruments"] == "n/a"


def test_health_counts_a_retired_source_in_its_own_bucket(config, lake):
    _path, cfg = lake()
    body = LakeView(cfg).health()

    assert (body["retired"], body["fresh"], body["stale"]) == (1, 1, 1)
