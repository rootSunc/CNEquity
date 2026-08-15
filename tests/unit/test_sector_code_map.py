"""BK ↔ BOARD_CODE identity map."""

from datetime import date

import polars as pl
import pytest

from cn_market_lake.config import Config
from cn_market_lake.derive.sector_code_map import (
    _latest_bars,
    _latest_member_boards,
    bk_to_board_code,
    board_code_to_bk,
    build_sector_code_map,
    derive_sector_code_map,
    load_sector_code_map,
)


def test_bk_board_code_roundtrip():
    assert board_code_to_bk("437") == "BK0437"
    assert board_code_to_bk("1628") == "BK1628"
    assert board_code_to_bk("not-a-number") is None
    assert bk_to_board_code("BK0437") == "437"
    assert bk_to_board_code("BK1628") == "1628"
    assert bk_to_board_code("bad") is None


def test_load_sector_code_map_missing_file_returns_empty(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    assert load_sector_code_map(cfg).is_empty()


def test_load_sector_code_map_reads_persisted_file(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    cfg.meta_root.mkdir(parents=True)
    pl.DataFrame({"sector_code": ["BK0437"]}).write_parquet(
        cfg.meta_root / "sector_code_map.parquet"
    )
    out = load_sector_code_map(cfg)
    assert out["sector_code"].to_list() == ["BK0437"]


def test_latest_bars_empty_when_no_curated_data(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    assert _latest_bars(cfg).is_empty()


def test_latest_bars_selects_latest_trade_date(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    old_part = cfg.curated_root / "sector_bars" / "trade_date=2026-07-13"
    old_part.mkdir(parents=True)
    pl.DataFrame(
        {
            "sector_code": ["BK0001"],
            "sector_name": ["旧板块"],
            "board_type": ["concept"],
            "trade_date": [date(2026, 7, 13)],
        }
    ).write_parquet(old_part / "part-000.parquet")
    new_part = cfg.curated_root / "sector_bars" / "trade_date=2026-07-14"
    new_part.mkdir(parents=True)
    pl.DataFrame(
        {
            "sector_code": ["BK0437"],
            "sector_name": ["煤炭"],
            "board_type": ["industry"],
            "trade_date": [date(2026, 7, 14)],
        }
    ).write_parquet(new_part / "part-000.parquet")

    out = _latest_bars(cfg)
    assert out["trade_date"].unique().to_list() == [date(2026, 7, 14)]
    assert out["sector_code"].to_list() == ["BK0437"]


def test_latest_member_boards_empty_when_no_curated_data(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    out = _latest_member_boards(cfg, "sector_members", "sector_code", "sector_name")
    assert out.is_empty()


def test_latest_member_boards_selects_latest_as_of(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    part = cfg.curated_root / "sector_members" / "as_of_date=2026-07-14"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "sector_code": [896],
            "sector_name": ["白酒"],
            "as_of_date": [date(2026, 7, 14)],
        }
    ).write_parquet(part / "part-000.parquet")

    out = _latest_member_boards(cfg, "sector_members", "sector_code", "sector_name")
    assert out["board_code"].to_list() == ["896"]
    assert out["board_name"].to_list() == ["白酒"]


def test_build_identity_map():
    bars = pl.DataFrame(
        [
            {
                "sector_code": "BK0437",
                "sector_name": "煤炭",
                "board_type": "industry",
                "trade_date": date(2026, 7, 14),
            },
            {
                "sector_code": "BK0896",
                "sector_name": "白酒",
                "board_type": "concept",
                "trade_date": date(2026, 7, 14),
            },
            {
                "sector_code": "BK0636",
                "sector_name": "B股",
                "board_type": "concept",
                "trade_date": date(2026, 7, 14),
            },
        ]
    )
    concept = pl.DataFrame([{"board_code": "896", "board_name": "白酒"}])
    industry = pl.DataFrame([{"board_code": "437", "board_name": "煤炭"}])
    df, summary = build_sector_code_map(bars, concept, industry, as_of=date(2026, 7, 14))
    assert df.height == 3
    hit = df.filter(pl.col("has_members"))
    assert hit.height == 2
    assert set(hit["match_type"].to_list()) == {"identity"}
    orphan = df.filter(~pl.col("has_members"))
    assert orphan["sector_code"][0] == "BK0636"
    assert orphan["board_code"][0] == "636"
    assert summary["has_members"] == 2


def test_build_sector_code_map_all_empty_inputs():
    df, summary = build_sector_code_map(pl.DataFrame(), pl.DataFrame(), pl.DataFrame())
    assert df.is_empty()
    assert summary["mapped_rows"] == 0
    assert summary["match_type_counts"] == {}
    assert summary["board_type_counts"] == {}


def test_build_sector_code_map_skips_non_numeric_member_board_codes():
    bars = pl.DataFrame(
        [
            {
                "sector_code": "BK0896",
                "sector_name": "白酒",
                "board_type": "concept",
                "trade_date": date(2026, 7, 14),
            }
        ]
    )
    # Non-numeric board_code in the member tables must be skipped, not crash.
    concept = pl.DataFrame([{"board_code": "not-numeric", "board_name": "坏数据"}])
    industry = pl.DataFrame([{"board_code": "also-bad", "board_name": "坏数据2"}])
    df, _ = build_sector_code_map(bars, concept, industry, as_of=date(2026, 7, 14))
    assert df["match_type"][0] == "predicted_no_members"


def test_build_sector_code_map_unknown_board_type_has_no_members():
    bars = pl.DataFrame(
        [
            {
                "sector_code": "BK0001",
                "sector_name": "未知类型",
                "board_type": "other",
                "trade_date": date(2026, 7, 14),
            }
        ]
    )
    df, _ = build_sector_code_map(bars, pl.DataFrame(), pl.DataFrame(), as_of=date(2026, 7, 14))
    assert df["match_type"][0] == "predicted_no_members"
    assert df["has_members"][0] is False


def test_derive_sector_code_map_raises_when_curated_bars_empty(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    with pytest.raises(RuntimeError, match="curated/sector_bars empty"):
        derive_sector_code_map(cfg)


def test_derive_sector_code_map_writes_map_and_summary(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    part = cfg.curated_root / "sector_bars" / "trade_date=2026-07-14"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "sector_code": ["BK0437"],
            "sector_name": ["煤炭"],
            "board_type": ["industry"],
            "trade_date": [date(2026, 7, 14)],
        }
    ).write_parquet(part / "part-000.parquet")

    summary = derive_sector_code_map(cfg)
    assert summary["mapped_rows"] == 1
    assert (cfg.meta_root / "sector_code_map.parquet").exists()
    assert (cfg.meta_root / "sector_code_map_summary.json").exists()


def test_identity_name_mismatch_flagged():
    bars = pl.DataFrame(
        [
            {
                "sector_code": "BK0896",
                "sector_name": "白酒概念",
                "board_type": "concept",
                "trade_date": date(2026, 7, 14),
            }
        ]
    )
    # Same id, totally different name → mismatch flag (norm still matches 白酒)
    concept = pl.DataFrame([{"board_code": "896", "board_name": "白酒"}])
    df, _ = build_sector_code_map(bars, concept, pl.DataFrame(), as_of=date(2026, 7, 14))
    assert df["match_type"][0] == "identity"  # 白酒概念 vs 白酒 normalize equal

    concept2 = pl.DataFrame([{"board_code": "896", "board_name": "光伏"}])
    df2, _ = build_sector_code_map(bars, concept2, pl.DataFrame(), as_of=date(2026, 7, 14))
    assert df2["match_type"][0] == "identity_name_mismatch"
