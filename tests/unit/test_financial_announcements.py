from datetime import date, datetime, timezone

import polars as pl

from cnequity.config import Config
from cnequity.domain.financial_statements import original_disclosures, reconstruct_disclosures
from cnequity.domain.schemas import DATASET_SCHEMAS
from cnequity.query import load
from cnequity.storage.repairs.financial_announcements import repair_financial_announcements
from cnequity.storage.revisions import RevisionStore

ORIGINAL = date(2017, 3, 27)
LATE = date(2018, 3, 27)
OBSERVED = datetime(2018, 3, 28, tzinfo=timezone.utc)


def _rows():
    common = {
        "symbol": "000002.SZ",
        "report_period": "2016Q4",
        "source": "eastmoney",
        "data_version": "v1",
        "fetched_at": OBSERVED,
    }
    return pl.DataFrame(
        [
            {
                **common,
                "statement_type": "indicator",
                "item_code": "bps",
                "announce_date": ORIGINAL,
                "item_value": 14.6,
            },
            {
                **common,
                "statement_type": "balance",
                "item_code": "total_assets",
                "announce_date": LATE,
                "item_value": 100.0,
            },
        ],
        schema=DATASET_SCHEMAS["financial_statement_items"],
    )


def test_existing_original_vintage_is_never_reconstructed_over():
    frame = _rows()
    existing = frame.filter(pl.col("statement_type") == "balance").with_columns(
        pl.lit(ORIGINAL).alias("announce_date"),
        pl.lit("ths_official").alias("source"),
        pl.lit(90.0).alias("item_value"),
    )
    frame = pl.concat([frame, existing])
    assert reconstruct_disclosures(frame, original_disclosures(frame)).is_empty()


def test_lico_headlines_supply_dates_when_per_share_items_are_missing():
    frame = _rows().with_columns(
        pl.when(pl.col("item_code") == "bps")
        .then(pl.lit("income"))
        .otherwise(pl.col("statement_type"))
        .alias("statement_type"),
        pl.when(pl.col("item_code") == "bps")
        .then(pl.lit("revenue"))
        .otherwise(pl.col("item_code"))
        .alias("item_code"),
    )
    assert original_disclosures(frame).rows() == [("000002.SZ", "2016Q4", ORIGINAL)]
    assert reconstruct_disclosures(frame, original_disclosures(frame)).height == 1
    # A DMSK detail alone is not original-date evidence.
    assert original_disclosures(frame.filter(pl.col("item_code") == "total_assets")).is_empty()


def test_an_old_backfill_can_reconstruct_a_subsequently_verified_earlier_date():
    frame = _rows().with_columns(pl.lit("eastmoney_backfill").alias("source"))
    repaired = reconstruct_disclosures(frame, original_disclosures(frame))
    assert repaired["announce_date"].to_list() == [ORIGINAL]
    assert repaired["source"].to_list() == ["eastmoney_backfill"]
    assert reconstruct_disclosures(
        pl.concat([frame, repaired], how="diagonal_relaxed"), original_disclosures(frame)
    ).is_empty()


def test_late_value_is_visible_only_as_inexact_historical_reconstruction(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    dataset = "financial_statement_items"
    path = cfg.curated_root / dataset / "report_period=2016Q4" / "part-merged.parquet"
    path.parent.mkdir(parents=True)
    _rows().write_parquet(path)
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    store.commit(
        dataset, run_id="seed", changed_files=[path], schema_version=1, contract_fingerprint="test"
    )
    files = set(cfg.data_root.rglob("*"))
    assert repair_financial_announcements(cfg)["rows"] == 1
    assert set(cfg.data_root.rglob("*")) == files
    result = repair_financial_announcements(cfg, apply=True)
    assert result["applied"] is True
    early = load(dataset, config=cfg, as_of=ORIGINAL).filter(pl.col("item_code") == "total_assets")
    assert early["announce_date"].to_list() == [ORIGINAL]
    assert early["source"].to_list() == ["eastmoney_backfill"]
    assert early["item_value"].to_list() == [100.0]
    assert early["pit_is_exact"].to_list() == [False]
    assert early["available_at"].to_list() == [None]
    assert early["fetched_at"].to_list() == [OBSERVED]
    assert load(dataset, config=cfg, as_of=ORIGINAL, pit_mode="strict").is_empty()
    # The late vintage is a new key, so it sits in a file beside the seed one.
    raw = pl.concat(
        [pl.read_parquet(p) for p in sorted(store.current_root(dataset).rglob("*.parquet"))],
        how="diagonal_relaxed",
    )
    assert raw.filter(pl.col("item_code") == "total_assets")["announce_date"].sort().to_list() == [
        ORIGINAL,
        LATE,
    ]
    assert (
        load(dataset, config=cfg, revision=1, as_of=ORIGINAL)
        .filter(pl.col("item_code") == "total_assets")
        .is_empty()
    )
    assert repair_financial_announcements(cfg)["rows"] == 0
