"""EastMoney lists three industry boards per stock; the lake must keep all three, by level."""

from datetime import date, datetime, timezone

import polars as pl
import pytest
from tests.catalog_seed import commit_tree

from cnequity.adapters.eastmoney import industry as em_industry
from cnequity.config import Config
from cnequity.domain.industry_levels import board_levels, em_member_levels
from cnequity.domain.schemas import DATASET_SCHEMAS
from cnequity.query import load
from cnequity.storage.repairs.industry_levels import fill_levels, repair_industry_levels
from cnequity.storage.revisions import RevisionStore

# Two stocks under one level-1 board. 食品饮料 has one level-2 child per stock;
# each level-2 board has a single level-3 child with the same members, so only the suffix
# can order those two.
BOARDS = [
    ("600519.SH", "438", "食品饮料"),
    ("600519.SH", "1277", "白酒Ⅱ"),
    ("600519.SH", "1575", "白酒Ⅲ"),
    ("603288.SH", "438", "食品饮料"),
    ("603288.SH", "1278", "调味发酵品Ⅱ"),
    ("603288.SH", "1579", "调味发酵品Ⅲ"),
]


def _boards(rows=BOARDS) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=["symbol", "board_code", "board_name"], orient="row")


def test_board_levels_break_equal_member_ties_by_suffix():
    levels = dict(board_levels(_boards()).select("board_code", "level").iter_rows())
    assert levels == {"438": 1, "1277": 2, "1575": 3, "1278": 2, "1579": 3}


def test_each_stock_keeps_all_three_levels_whatever_the_row_order():
    for rows in (BOARDS, list(reversed(BOARDS))):
        got = em_member_levels(_boards(rows)).filter(pl.col("symbol") == "600519.SH")
        assert got.row(0) == ("600519.SH", "438", "食品饮料", "1277", "白酒Ⅱ", "1575", "白酒Ⅲ")


def test_adapter_returns_one_leaf_row_per_stock(monkeypatch):
    raw = [
        {"SECURITY_CODE": s.split(".")[0], "BOARD_CODE": c, "BOARD_NAME": n, "SECUCODE": s}
        for s, c, n in BOARDS
    ]
    monkeypatch.setattr(em_industry, "fetch_datacenter", lambda *a, **k: list(reversed(raw)))
    monkeypatch.setattr(em_industry, "exchange_from_datacenter", lambda item: "SH")

    class Client:
        def close(self):
            pass

    out = em_industry.fetch_industry_members(date(2026, 10, 23), client=Client())
    assert out.height == 2
    row = out.filter(pl.col("symbol") == "603288.SH").row(0, named=True)
    assert (row["industry_code"], row["l1_code"], row["l2_code"], row["l3_code"]) == (
        "1579",
        "438",
        "1278",
        "1579",
    )


def _v1(symbol, system, code, as_of=date(2026, 7, 10)):
    return {
        "symbol": symbol,
        "classification_system": system,
        "industry_code": code,
        "industry_name": code,
        "as_of_date": as_of,
        "source": system,
        "data_version": "v1",
        "fetched_at": datetime(2026, 7, 10, 8, tzinfo=timezone.utc),
    }


def test_fill_levels_never_invents_a_level_below_the_stored_board():
    from cnequity.domain.industry_levels import board_ancestry

    frame = pl.DataFrame(
        [
            _v1("600519.SH", "eastmoney", "438"),
            _v1("603288.SH", "eastmoney", "1579"),
            _v1("000001.SZ", "sw", "480101"),
        ]
    )
    out = fill_levels(frame, board_ancestry(_boards()))
    by = {r["symbol"]: r for r in out.iter_rows(named=True)}
    # A stored level-1 board says nothing about which level-2/3 board it was.
    assert (by["600519.SH"]["l1_code"], by["600519.SH"]["l2_code"]) == ("438", None)
    assert by["600519.SH"]["industry_code"] == "438"
    assert (by["603288.SH"]["l2_code"], by["603288.SH"]["l3_code"]) == ("1278", "1579")
    assert (by["000001.SZ"]["l1_code"], by["000001.SZ"]["l2_code"]) == ("480000", "480100")
    assert by["000001.SZ"]["l3_code"] == "480101"


@pytest.fixture
def lake(tmp_path, settle):
    cfg = Config(data_root=tmp_path / "lake")
    root = cfg.curated_root / "industry_members" / "as_of_date=2026-07-10"
    root.mkdir(parents=True)
    old = {k: v for k, v in DATASET_SCHEMAS["industry_members"].items() if not k.startswith("l")}
    pl.DataFrame(
        [_v1("600519.SH", "eastmoney", "1575"), _v1("000001.SZ", "sw", "480101")], schema=old
    ).write_parquet(root / "part.parquet")
    commit_tree(
        RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root),
        "industry_members",
        run_id="seed",
        changed_files=sorted(root.rglob("*.parquet")),
        schema_version=1,
        contract_fingerprint="contract",
    )
    return settle(cfg)


def test_old_files_read_with_null_levels_and_the_migration_fills_them(lake):
    before = load("industry_members", config=lake)
    assert before["l1_code"].null_count() == 2

    plan = repair_industry_levels(lake, hierarchy=lambda cfg: _boards())
    assert plan == {
        "dataset": "industry_members",
        "applied": False,
        "rows": 2,
        "by_system": {"eastmoney": 1, "sw": 1},
        "eastmoney_boards_not_listed_today": 0,
        "eastmoney_levels": {"3": 1},
    }
    repair_industry_levels(lake, apply=True, hierarchy=lambda cfg: _boards())
    after = load("industry_members", config=lake).sort("symbol")
    assert after["l1_code"].to_list() == ["480000", "438"]
    assert after["fetched_at"].to_list() == before.sort("symbol")["fetched_at"].to_list()
    assert repair_industry_levels(lake, hierarchy=lambda cfg: _boards())["rows"] == 0
