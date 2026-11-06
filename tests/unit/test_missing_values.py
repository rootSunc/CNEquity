from datetime import date, datetime, timezone

import polars as pl
import pytest
from click.testing import CliRunner

from cnequity.cli.main import cli
from cnequity.config import Config
from cnequity.domain.datasets import DATASETS
from cnequity.domain.schemas import DATASET_SCHEMAS
from cnequity.domain.value_rules import apply_value_rules
from cnequity.query import load
from cnequity.storage.repairs.missing_values import repair_missing_values
from cnequity.storage.revisions import RevisionStore


def test_zero_valuations_are_missing_but_negative_ratios_are_valid():
    columns = DATASETS["valuation_metrics"].value_rules.zero_is_missing
    frame = pl.DataFrame({column: [0.0, None, -2.0, 3.0] for column in columns})
    corrected = apply_value_rules(frame, "valuation_metrics")
    for column in columns:
        assert corrected[column].to_list() == [None, None, -2.0, 3.0]
        assert corrected[column].dtype == pl.Float64


@pytest.mark.parametrize("dataset", ["valuation_metrics", "index_constituents"])
def test_missing_value_repair_is_gated_retained_and_idempotent(tmp_path, dataset):
    cfg = Config(data_root=tmp_path / "lake")
    stamp = datetime(2026, 7, 7, tzinfo=timezone.utc)
    rows = []
    for source in ["eastmoney", "cni" if dataset == "index_constituents" else "baostock", "other"]:
        row = {"symbol": "600519.SH", "source": source, "data_version": "v1", "fetched_at": stamp}
        if dataset == "valuation_metrics":
            row.update(trade_date=date(2026, 7, 7), total_mv=0.0, pb=-2.0, pe_ttm=0.0)
        else:
            row.update(index_symbol="000300.SH", as_of_date=date(2026, 7, 7), weight=0.0)
        rows.append(row)
    column = "trade_date" if dataset == "valuation_metrics" else "as_of_date"
    path = cfg.curated_root / dataset / f"{column}=2026-07" / "part-merged.parquet"
    path.parent.mkdir(parents=True)
    frame = pl.DataFrame(rows, schema=DATASET_SCHEMAS[dataset])
    frame.write_parquet(path)
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    store.commit(
        dataset, run_id="seed", changed_files=[path], schema_version=1, contract_fingerprint="test"
    )
    before = set(cfg.data_root.rglob("*"))
    preview = repair_missing_values(cfg, dataset)
    assert preview["rows"] == (3 if dataset == "valuation_metrics" else 2)
    assert set(cfg.data_root.rglob("*")) == before
    result = repair_missing_values(cfg, dataset, apply=True)
    assert result["applied"] is True
    assert result["publication_audit"]
    raw = pl.read_parquet(list(store.current_root(dataset).rglob("*.parquet")))
    if dataset == "valuation_metrics":
        assert raw["total_mv"].null_count() == 3
        assert raw["pb"].to_list() == [-2.0] * 3
    else:
        assert raw.filter(pl.col("source") != "other")["weight"].null_count() == 2
        assert raw.filter(pl.col("source") == "other")["weight"].to_list() == [0.0]
    assert raw["fetched_at"].to_list() == [stamp] * 3
    # The earlier revision keeps the bytes it was published with; reading it
    # applies today's rules, so the placeholder reads as null either way.
    old_files = list((cfg.meta_root / "revisions").rglob("*.parquet"))
    value = "total_mv" if dataset == "valuation_metrics" else "weight"
    assert any(0.0 in pl.read_parquet(p)[value].to_list() for p in old_files)
    old = load(dataset, config=cfg, revision=1)
    # One row per key survives the read: the "other" source's, whose index
    # weight of 0 is real while a valuation 0 is a placeholder from any source.
    assert old[value].to_list() == ([None] if dataset == "valuation_metrics" else [0.0])
    assert repair_missing_values(cfg, dataset)["rows"] == 0


def test_preview_of_empty_lake_creates_nothing(tmp_path):
    cfg = Config(data_root=tmp_path / "absent")
    assert repair_missing_values(cfg, "index_constituents")["rows"] == 0
    assert not cfg.data_root.exists()


def test_missing_value_cli_restricts_dataset():
    result = CliRunner().invoke(cli, ["repair", "missing-values", "daily_bars"])
    assert result.exit_code == 2


def test_retrying_old_staging_cannot_restore_zero_weights(tmp_path):
    from cnequity.domain.schemas import with_provenance
    from cnequity.storage.parquet import StagingWriter, compact_dataset

    cfg = Config(data_root=tmp_path / "lake")
    staged = with_provenance(
        pl.DataFrame(
            {
                "index_symbol": ["000300.SH"],
                "symbol": ["600519.SH"],
                "as_of_date": [date(2026, 7, 7)],
                "weight": [0.0],
            }
        ),
        source="eastmoney",
        data_version="v1",
    )
    StagingWriter(cfg.staging_root).write_batch("index_constituents", "old", "batch", staged)
    compact_dataset(
        cfg.staging_root, cfg.curated_root, "index_constituents", "old", partition_col="as_of_date"
    )
    result = pl.read_parquet(list((cfg.curated_root / "index_constituents").rglob("*.parquet")))
    assert result["weight"].to_list() == [None]
