"""load_profile: what could be known about each security on each date, and why not."""

from datetime import date, datetime, timezone

import polars as pl
import pytest
from tests.catalog_seed import commit_tree

from cnequity.config import Config
from cnequity.domain.schemas import DATASET_SCHEMAS
from cnequity.query.profile import ProfileError, load_profile
from cnequity.storage.revisions import RevisionStore

S, DELISTED, NEW = "600519.SH", "000406.SZ", "688999.SH"
EARLY = datetime(2024, 4, 1, tzinfo=timezone.utc)  # observed in real time
LATE = datetime(2026, 10, 1, tzinfo=timezone.utc)  # recovered by a backfill


def _write(cfg, dataset, rows, partition, value):
    root = cfg.curated_root / dataset / f"{partition}={value}"
    root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema=DATASET_SCHEMAS[dataset]).write_parquet(root / "part.parquet")
    commit_tree(
        RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root),
        dataset,
        run_id=f"seed-{dataset}",
        changed_files=sorted((cfg.curated_root / dataset).rglob("*.parquet")),
        schema_version=1,
        contract_fingerprint="contract",
    )


def _meta(source, fetched):
    return {"source": source, "data_version": "v1", "fetched_at": fetched}


@pytest.fixture
def lake(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    inst = cfg.curated_root / "instruments"
    inst.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "symbol": S,
                "name": "x",
                "exchange": "SH",
                "asset_type": "stock",
                "list_date": date(2001, 8, 27),
                "delist_date": None,
                "prev_symbol": None,
                **_meta("t", EARLY),
            },
            {
                "symbol": DELISTED,
                "name": "y",
                "exchange": "SZ",
                "asset_type": "stock",
                "list_date": date(1996, 6, 28),
                "delist_date": date(2006, 4, 21),
                "prev_symbol": None,
                **_meta("t", EARLY),
            },
            {
                "symbol": NEW,
                "name": "z",
                "exchange": "SH",
                "asset_type": "stock",
                "list_date": date(2024, 6, 3),
                "delist_date": None,
                "prev_symbol": None,
                **_meta("t", EARLY),
            },
        ],
        schema=DATASET_SCHEMAS["instruments"],
    ).write_parquet(inst / "part.parquet")
    commit_tree(
        RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root),
        "instruments",
        run_id="seed",
        changed_files=[inst / "part.parquet"],
        schema_version=1,
        contract_fingerprint="contract",
    )

    def fsi(period, value, announced, source, fetched):
        return {
            "symbol": S,
            "report_period": period,
            "statement_type": "income",
            "item_code": "net_profit",
            "item_value": value,
            "announce_date": announced,
            **_meta(source, fetched),
        }

    _write(
        cfg,
        "financial_statement_items",
        [
            fsi("2023Q4", 100.0, date(2024, 3, 30), "eastmoney", EARLY),
            # 2023Q4 restated in May: as_of after it sees the new figure.
            fsi("2023Q4", 90.0, date(2024, 5, 20), "eastmoney_backfill", LATE),
            fsi("2024Q1", 30.0, date(2024, 4, 27), "eastmoney_backfill", LATE),
        ],
        "report_period",
        "2024",
    )
    _write(
        cfg,
        "trading_status",
        [
            {
                "symbol": S,
                "trade_date": date(2024, 4, 26),
                "is_trading": True,
                "status": "normal",
                "risk_warning": None,
                **_meta("derived_traded", LATE),
            },
        ],
        "trade_date",
        "2024-04",
    )
    return cfg


def _value(frame, field, as_of, symbol=S):
    row = frame.filter(
        (pl.col("field") == field) & (pl.col("as_of") == as_of) & (pl.col("symbol") == symbol)
    ).row(0, named=True)
    return row


def test_the_latest_visible_period_wins_and_restatements_apply_from_their_date(lake):
    days = [date(2024, 4, 26), date(2024, 4, 30), date(2024, 5, 31)]
    p = load_profile(["fsi:net_profit"], [S], days, config=lake)
    before = _value(p, "fsi:net_profit", days[0])
    assert (before["value_num"], before["value_str"], before["field_date"]) == (
        100.0,
        "2023Q4",
        date(2023, 12, 31),
    )
    assert before["pit_quality"] == "strict"  # first-hand and observed by 2024-04-01
    q1 = _value(p, "fsi:net_profit", days[1])
    assert (q1["value_num"], q1["pit_quality"]) == (30.0, "reconstructed")
    # The 2023Q4 restatement does not displace the newer 2024Q1 period.
    assert _value(p, "fsi:net_profit", days[2])["value_num"] == 30.0


def test_strict_mode_keeps_only_first_hand_rows_observed_in_time(lake):
    p = load_profile(["fsi:net_profit"], [S], [date(2024, 4, 30)], pit_mode="strict", config=lake)
    row = _value(p, "fsi:net_profit", date(2024, 4, 30))
    assert (row["value_num"], row["pit_quality"]) == (100.0, "strict")


def test_missing_values_say_why(lake):
    days = [date(2024, 3, 1), date(2024, 4, 26)]
    p = load_profile(
        ["fsi:net_profit", "status.st", "holders.count", "listed"],
        [S, DELISTED, NEW],
        days,
        config=lake,
    )
    assert _value(p, "fsi:net_profit", days[0])["missing_reason"] == "outside_coverage"
    assert _value(p, "status.st", days[1])["missing_reason"] == "value_unknown"
    assert _value(p, "holders.count", days[1])["missing_reason"] == "outside_coverage"
    gone = _value(p, "fsi:net_profit", days[1], DELISTED)
    assert (gone["value_num"], gone["missing_reason"]) == (None, "after_delisting")
    assert _value(p, "fsi:net_profit", days[1], NEW)["missing_reason"] == "before_listing"
    listed = _value(p, "listed", days[1], NEW)
    assert (listed["value_num"], listed["missing_reason"]) == (0.0, None)


def test_old_values_are_returned_marked_stale(lake):
    p = load_profile(["status.trading"], [S], [date(2024, 6, 28)], config=lake)
    row = _value(p, "status.trading", date(2024, 6, 28))
    assert (row["value_num"], row["age_days"], row["missing_reason"]) == (1.0, 63, "stale")


def test_unknown_fields_are_rejected(lake):
    with pytest.raises(ProfileError, match="unknown profile field"):
        load_profile(["industry.citic.l1"], [S], [date(2024, 4, 26)], config=lake)


def _published_lake(tmp_path):
    """Share counts with and without a recorded publication time."""
    from zoneinfo import ZoneInfo

    sh = ZoneInfo("Asia/Shanghai")
    cfg = Config(data_root=tmp_path / "lake")

    def at(day, hour):
        return datetime(day.year, day.month, day.day, hour, tzinfo=sh).astimezone(timezone.utc)

    rows = [
        # Published 16:00 on 04-26, effective 04-25: after a 15:00 cutoff it counts from 04-29.
        {
            "symbol": S,
            "change_date": date(2024, 4, 25),
            "total_shares": 100.0,
            "float_shares": 80.0,
            "restricted_shares": None,
            "free_float_shares": None,
            "change_reason": None,
            "announce_date": date(2024, 4, 26),
            **_meta("eastmoney", EARLY),
            "available_at": at(date(2024, 4, 26), 16),
        },
        # Published early but effective only on 05-06.
        {
            "symbol": S,
            "change_date": date(2024, 5, 6),
            "total_shares": 120.0,
            "float_shares": 90.0,
            "restricted_shares": None,
            "free_float_shares": None,
            "change_reason": None,
            "announce_date": date(2024, 4, 30),
            **_meta("eastmoney", EARLY),
            "available_at": at(date(2024, 4, 30), 9),
        },
        # No recorded publication time: never visible in published mode.
        {
            "symbol": S,
            "change_date": date(2024, 6, 3),
            "total_shares": 150.0,
            "float_shares": 99.0,
            "restricted_shares": None,
            "free_float_shares": None,
            "change_reason": None,
            "announce_date": date(2024, 6, 3),
            **_meta("eastmoney", EARLY),
            "available_at": None,
        },
    ]
    schema = {**DATASET_SCHEMAS["share_structure"], "available_at": pl.Datetime("us", "UTC")}
    root = cfg.curated_root / "share_structure" / "change_date=2024"
    root.mkdir(parents=True)
    pl.DataFrame(rows, schema=schema).write_parquet(root / "part.parquet")
    commit_tree(
        RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root),
        "share_structure",
        run_id="seed",
        changed_files=[root / "part.parquet"],
        schema_version=1,
        contract_fingerprint="contract",
    )
    return cfg


def test_published_mode_uses_only_recorded_publication_times_cut_at_the_close(tmp_path):
    from datetime import time

    cfg = _published_lake(tmp_path)
    days = [date(2024, 4, 26), date(2024, 4, 29), date(2024, 5, 6), date(2024, 6, 28)]
    p = load_profile(["shares.float"], [S], days, pit_mode="published", cutoff=time(15), config=cfg)
    got = {r["as_of"]: (r["value_num"], r["missing_reason"]) for r in p.iter_rows(named=True)}
    assert got[days[0]] == (None, "no_pit_evidence")  # published after that close
    assert got[days[1]] == (80.0, None)
    assert got[days[2]] == (90.0, None)  # visible once effective, not when announced
    # The June change has no publication time; the May value stands.
    assert got[days[3]] == (90.0, None)
    whole_day = load_profile(["shares.float"], [S], [days[0]], pit_mode="published", config=cfg)
    assert whole_day["value_num"].to_list() == [80.0]
    loose = load_profile(["shares.float"], [S], [days[3]], config=cfg)
    assert loose["value_num"].to_list() == [99.0]


def test_published_mode_has_no_industry_and_leaves_status_alone(lake):
    root = lake.curated_root / "industry_members" / "as_of_date=2024-04-26"
    root.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "symbol": S,
                "classification_system": "sw",
                "industry_code": "340501",
                "industry_name": "340501",
                "l1_code": "340000",
                "l1_name": None,
                "l2_code": "340500",
                "l2_name": None,
                "l3_code": "340501",
                "l3_name": None,
                "as_of_date": date(2024, 4, 26),
                **_meta("sw", EARLY),
            }
        ],
        schema=DATASET_SCHEMAS["industry_members"],
    ).write_parquet(root / "part.parquet")
    commit_tree(
        RevisionStore(lake.meta_root, lake.curated_root, lake.derived_root),
        "industry_members",
        run_id="seed",
        changed_files=[root / "part.parquet"],
        schema_version=2,
        contract_fingerprint="contract",
    )
    day = date(2024, 4, 26)
    p = load_profile(
        ["industry.sw.l3", "status.trading"], [S], [day], pit_mode="published", config=lake
    )
    assert _value(p, "industry.sw.l3", day)["missing_reason"] == "no_pit_evidence"
    assert _value(p, "status.trading", day)["value_num"] == 1.0


def test_canonical_reads_a_renamed_security_under_its_code_on_the_date(tmp_path):
    old, new = "601313.SH", "601360.SH"  # renamed 2018-02-28
    cfg = Config(data_root=tmp_path / "lake")
    root = cfg.curated_root / "trading_status" / "trade_date=2018-01"
    root.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "symbol": new,
                "trade_date": date(2018, 1, 31),
                "is_trading": True,
                "status": "normal",
                "risk_warning": False,
                **_meta("baostock", EARLY),
            }
        ],
        schema=DATASET_SCHEMAS["trading_status"],
    ).write_parquet(root / "part.parquet")
    commit_tree(
        RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root),
        "trading_status",
        run_id="seed",
        changed_files=[root / "part.parquet"],
        schema_version=1,
        contract_fingerprint="contract",
    )
    day = date(2018, 1, 31)
    plain = load_profile(["status.trading"], [old], [day], config=cfg)
    assert plain["value_num"].to_list() == [None]
    mapped = load_profile(["status.trading"], [old], [day], canonical=True, config=cfg)
    assert mapped["value_num"].to_list() == [1.0]
