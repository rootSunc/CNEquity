"""Shenwan ↔ EastMoney industry evidence: same-day only, stocks in both only, nothing merged."""

from datetime import date, datetime, timezone

import polars as pl
import pytest
from tests.catalog_seed import commit_tree

from cnequity.config import Config
from cnequity.derive.industry_crosswalk import (
    build_crosswalk,
    derive_industry_crosswalk,
    evidence_dates,
    load_industry_crosswalk,
)
from cnequity.domain.schemas import DATASET_SCHEMAS
from cnequity.storage.revisions import RevisionStore

DAY = date(2026, 10, 30)
FETCHED = datetime(2026, 10, 31, tzinfo=timezone.utc)


def _row(symbol, day, chain, *, system, version="v2"):
    (l1, n1), (l2, n2), (l3, n3) = chain
    return {
        "symbol": symbol,
        "classification_system": system,
        "industry_code": l3,
        "industry_name": n3 or l3,
        "l1_code": l1,
        "l1_name": n1,
        "l2_code": l2,
        "l2_name": n2,
        "l3_code": l3,
        "l3_name": n3,
        "as_of_date": day,
        "source": system,
        "data_version": version,
        "fetched_at": FETCHED,
    }


SW_BANK = (("480000", None), ("480100", None), ("480101", None))
SW_BANK_B = (("480000", None), ("480100", None), ("480102", None))
SW_FOOD = (("340000", None), ("340500", None), ("340501", None))
EM_BANK = (("475", "银行"), ("1283", "银行Ⅱ"), ("1610", "银行Ⅲ"))
EM_FOOD = (("438", "食品饮料"), ("1277", "白酒Ⅱ"), ("1575", "白酒Ⅲ"))

BANKS = [f"6000{i:02d}.SH" for i in range(20)]
FOOD = [f"6005{i:02d}.SH" for i in range(10)]


def _members(day=DAY, *, em_version="v2"):
    rows = []
    for i, s in enumerate(BANKS):
        # Shenwan splits the banks in two at level 3; EastMoney keeps one board.
        rows.append(_row(s, day, SW_BANK if i < 10 else SW_BANK_B, system="sw"))
        rows.append(_row(s, day, EM_BANK, system="eastmoney", version=em_version))
    for s in FOOD:
        rows.append(_row(s, day, SW_FOOD, system="sw"))
        rows.append(_row(s, day, EM_FOOD, system="eastmoney", version=em_version))
    # A stock only Shenwan has classified yet is not disagreement.
    rows.append(_row("600999.SH", day, SW_FOOD, system="sw"))
    return pl.DataFrame(rows, schema=DATASET_SCHEMAS["industry_members"])


def _pairs(table, level):
    rows = table.filter(pl.col("level") == level)
    return {(r["left_id"], r["right_id"]): r for r in rows.iter_rows(named=True)}


def test_equivalent_and_contained_industries_per_level():
    table = build_crosswalk(_members(), DAY)
    l1 = _pairs(table, 1)
    assert l1[("sw:2021:480000", "eastmoney:board:475")]["relation"] == "equivalent"
    assert l1[("sw:2021:480000", "eastmoney:board:475")]["right_name"] == "银行"
    # 600999.SH is Shenwan-only that day, so food still matches exactly.
    food = l1[("sw:2021:340000", "eastmoney:board:438")]
    assert (food["left_members"], food["jaccard"]) == (10, 1.0)
    l3 = _pairs(table, 3)
    half = l3[("sw:2021:480101", "eastmoney:board:1610")]
    assert half["relation"] == "left_within_right"
    assert (half["overlap"], half["left_members"], half["right_members"]) == (10, 10, 20)
    assert half["best_for_left"] and half["best_for_right"]
    # No pair is drawn between unrelated industries.
    assert ("sw:2021:480101", "eastmoney:board:1575") not in l3


def test_only_days_with_full_eastmoney_snapshots_are_evidence():
    old = _members(date(2026, 9, 30), em_version="v1")
    both = pl.concat([old, _members()])
    assert evidence_dates(both) == [DAY]
    sw_only = both.filter(pl.col("classification_system") == "sw")
    assert evidence_dates(sw_only) == []


@pytest.fixture
def lake(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    frame = pl.concat([_members(date(2026, 9, 30)), _members()])
    for (day,), part in frame.partition_by("as_of_date", as_dict=True).items():
        root = cfg.curated_root / "industry_members" / f"as_of_date={day}"
        root.mkdir(parents=True)
        part.write_parquet(root / "part.parquet")
    commit_tree(
        RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root),
        "industry_members",
        run_id="seed",
        changed_files=sorted((cfg.curated_root / "industry_members").rglob("*.parquet")),
        schema_version=2,
        contract_fingerprint="contract",
    )
    return cfg


def test_runs_add_evidence_dates_and_replace_only_their_own(lake):
    first = derive_industry_crosswalk(lake, as_of=date(2026, 10, 1))
    assert first["evidence_date"] == "2026-09-30"
    summary = derive_industry_crosswalk(lake)
    assert summary["evidence_dates"] == ["2026-09-30", "2026-10-30"]
    assert summary["levels"]["L1"]["left_equivalent"] == 2
    assert summary["levels"]["L1"]["stock_agreement"] == 1.0
    again = derive_industry_crosswalk(lake)
    table = load_industry_crosswalk(lake)
    assert again["evidence_dates"] == summary["evidence_dates"]
    assert (
        table.filter(pl.col("evidence_date") == DAY).height
        == table.filter(pl.col("evidence_date") == date(2026, 9, 30)).height
    )


def test_without_same_day_evidence_it_says_what_to_run(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    frame = _members(em_version="v1")
    root = cfg.curated_root / "industry_members" / f"as_of_date={DAY}"
    root.mkdir(parents=True)
    frame.write_parquet(root / "part.parquet")
    commit_tree(
        RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root),
        "industry_members",
        run_id="seed",
        changed_files=[root / "part.parquet"],
        schema_version=2,
        contract_fingerprint="contract",
    )
    with pytest.raises(RuntimeError, match="cne backfill industry_members"):
        derive_industry_crosswalk(cfg)
