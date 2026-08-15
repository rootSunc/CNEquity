"""BK* (clist) ↔ BOARD_CODE (datacenter members) map — lake-only derive.

EastMoney uses one numeric board id in two spellings:
``sector_bars.sector_code == "BK" + board_code.zfill(4)``.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.derive.sector_routing import norm_sector_name
from cn_market_lake.query.parquet_scan import dataset_has_parquet, scan_parquet_root

MAP_DATASET = "sector_code_map"


def board_code_to_bk(board_code: str) -> str | None:
    text = str(board_code or "").strip()
    if not text.isdigit():
        return None
    return f"BK{text.zfill(4)}"


def bk_to_board_code(sector_code: str) -> str | None:
    text = str(sector_code or "").strip().upper()
    if not text.startswith("BK") or not text[2:].isdigit():
        return None
    return str(int(text[2:]))


def _map_path(config: Config) -> Path:
    return config.meta_root / f"{MAP_DATASET}.parquet"


def _summary_path(config: Config) -> Path:
    return config.meta_root / f"{MAP_DATASET}_summary.json"


def load_sector_code_map(config: Config) -> pl.DataFrame:
    path = _map_path(config)
    if not path.exists():
        return pl.DataFrame()
    return pl.read_parquet(path)


def _latest_bars(config: Config) -> pl.DataFrame:
    root = config.curated_root / "sector_bars"
    if not dataset_has_parquet(root):
        return pl.DataFrame()
    df = scan_parquet_root(root, partition_col="trade_date").collect()
    if df.is_empty():
        return df
    latest = df["trade_date"].max()
    return (
        df.filter(pl.col("trade_date") == latest)
        .select("sector_code", "sector_name", "board_type", "trade_date")
        .unique()
    )


def _latest_member_boards(
    config: Config, dataset: str, code_col: str, name_col: str
) -> pl.DataFrame:
    root = config.curated_root / dataset
    if not dataset_has_parquet(root):
        return pl.DataFrame()
    partition = "as_of_date"
    df = scan_parquet_root(root, partition_col=partition).collect()
    if df.is_empty():
        return df
    latest = df[partition].max()
    return (
        df.filter(pl.col(partition) == latest)
        .select(
            pl.col(code_col).cast(pl.Utf8).alias("board_code"),
            pl.col(name_col).alias("board_name"),
        )
        .unique()
    )


def build_sector_code_map(
    bars: pl.DataFrame,
    concept_boards: pl.DataFrame,
    industry_boards: pl.DataFrame,
    *,
    as_of: date | None = None,
) -> tuple[pl.DataFrame, dict]:
    """Join clist BK boards to datacenter BOARD_CODEs via identity (+ name check)."""
    as_of = as_of or date.today()
    rows: list[dict] = []

    concept_by_bk: dict[str, dict] = {}
    for r in concept_boards.iter_rows(named=True):
        bk = board_code_to_bk(r["board_code"])
        if bk:
            concept_by_bk[bk] = r
    industry_by_bk: dict[str, dict] = {}
    for r in industry_boards.iter_rows(named=True):
        bk = board_code_to_bk(r["board_code"])
        if bk:
            industry_by_bk[bk] = r

    for r in bars.iter_rows(named=True):
        bk = str(r["sector_code"]).strip().upper()
        btype = str(r["board_type"])
        name = str(r["sector_name"] or "")
        board = None
        if btype == "concept":
            board = concept_by_bk.get(bk)
        elif btype == "industry":
            board = industry_by_bk.get(bk)

        predicted = bk_to_board_code(bk)
        if board is not None:
            bn = str(board["board_name"] or "")
            exact = name == bn
            norm_ok = norm_sector_name(name) == norm_sector_name(bn)
            if exact or norm_ok:
                match_type = "identity"
            else:
                match_type = "identity_name_mismatch"
            rows.append(
                {
                    "sector_code": bk,
                    "board_code": str(board["board_code"]),
                    "sector_name": name,
                    "board_name": bn,
                    "board_type": btype,
                    "match_type": match_type,
                    "has_members": True,
                    "as_of": as_of,
                }
            )
        else:
            rows.append(
                {
                    "sector_code": bk,
                    "board_code": predicted,
                    "sector_name": name,
                    "board_name": None,
                    "board_type": btype,
                    "match_type": "predicted_no_members",
                    "has_members": False,
                    "as_of": as_of,
                }
            )

    df = pl.DataFrame(rows) if rows else pl.DataFrame()
    summary: dict = {
        "as_of": as_of.isoformat(),
        "bars": bars.height if not bars.is_empty() else 0,
        "concept_boards": concept_boards.height if not concept_boards.is_empty() else 0,
        "industry_boards": industry_boards.height if not industry_boards.is_empty() else 0,
        "mapped_rows": df.height,
        "has_members": int(df.filter(pl.col("has_members")).height) if not df.is_empty() else 0,
        "match_type_counts": {},
        "board_type_counts": {},
    }
    if not df.is_empty():
        summary["match_type_counts"] = {
            r["match_type"]: r["len"] for r in df.group_by("match_type").len().iter_rows(named=True)
        }
        summary["board_type_counts"] = {
            r["board_type"]: r["len"] for r in df.group_by("board_type").len().iter_rows(named=True)
        }
    return df, summary


def derive_sector_code_map(config: Config, *, as_of: date | None = None) -> dict:
    """Build and persist ``meta/sector_code_map.parquet`` from curated lake only."""
    bars = _latest_bars(config)
    if bars.is_empty():
        raise RuntimeError("sector_code_map: curated/sector_bars empty — run research daily first")
    as_of = as_of or bars["trade_date"].max()
    concept = _latest_member_boards(config, "sector_members", "sector_code", "sector_name")
    industry = _latest_member_boards(config, "industry_members", "industry_code", "industry_name")
    df, summary = build_sector_code_map(bars, concept, industry, as_of=as_of)
    if df.is_empty():
        raise RuntimeError("sector_code_map: empty map")

    config.meta_root.mkdir(parents=True, exist_ok=True)
    df.write_parquet(_map_path(config))
    summary["generated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    _summary_path(config).write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    return summary
