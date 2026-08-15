"""Sector OHLC source routing: EastMoney BK universe × TDX 88xxxx index map.

Optional offline artifact (``cml derive sector_routing``). Does **not** drive
``sector_bars`` ingestion — daily/backfill use EastMoney only.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from cn_market_lake.config import Config

ROUTING_DATASET = "sector_ohlc_routing"
OHLC_TDX = "tdx_protocol"
OHLC_EM = "eastmoney"

# Event / platform concepts that only exist on EastMoney — never route to TDX.
_EM_EXCLUSIVE = re.compile(
    r"2026中报|一季报|东方财富热股|Kimi|DeepSeek|小红书|谷子经济|"
    r"同花顺|热股|预减|预增|首亏|扭亏"
)
_TDX_META = re.compile(r"总市值|流通市值|平均股价|涨跌家数|停板|等权|中位|成交|活筹|新标准券")


def norm_sector_name(name: str) -> str:
    text = re.sub(r"[ⅠⅡⅢIVⅣ\s]", "", str(name or ""))
    return text.replace("概念", "").replace("板块", "")


def is_em_exclusive(name: str) -> bool:
    return bool(_EM_EXCLUSIVE.search(str(name or "")))


def _routing_path(config: Config) -> Path:
    return config.meta_root / f"{ROUTING_DATASET}.parquet"


def _summary_path(config: Config) -> Path:
    return config.meta_root / f"{ROUTING_DATASET}_summary.json"


def load_sector_routing(config: Config) -> pl.DataFrame:
    path = _routing_path(config)
    if not path.exists():
        return pl.DataFrame()
    return pl.read_parquet(path)


def build_sector_routing(
    em_boards: list[dict],
    tdx_indices: list[dict],
    *,
    as_of: date | None = None,
) -> tuple[pl.DataFrame, dict]:
    """Assign each EastMoney BK a canonical OHLC source (TDX or EM).

    ``em_boards``: ``sector_code``, ``sector_name``, ``board_type``
    ``tdx_indices``: ``tdx_code``, ``name``
    """
    as_of = as_of or date.today()
    tdx_rows = [
        {
            "tdx_code": str(r["tdx_code"]).strip(),
            "tdx_name": str(r["name"]).strip().replace("\x00", ""),
        }
        for r in tdx_indices
        if str(r.get("tdx_code", "")).strip() and not _TDX_META.search(str(r.get("name", "")))
    ]

    exact_map: dict[str, list[str]] = {}
    tdx_by_norm: dict[str, list[tuple[str, str]]] = {}
    for row in tdx_rows:
        key = norm_sector_name(row["tdx_name"])
        exact_map.setdefault(key, []).append(row["tdx_code"])
        tdx_by_norm.setdefault(key, []).append((row["tdx_code"], row["tdx_name"]))

    out_rows: list[dict] = []
    tier_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}

    for board in em_boards:
        code = str(board["sector_code"]).strip()
        name = str(board["sector_name"]).strip()
        board_type = str(board.get("board_type") or "concept")
        nname = norm_sector_name(name)

        ohlc_source = OHLC_EM
        tdx_code: str | None = None
        match_type = "none"
        routing_tier = "T4"
        confidence = 0.0
        reason = "no_tdx_match"

        if is_em_exclusive(name):
            reason = "em_exclusive"
        elif nname in exact_map:
            codes = exact_map[nname]
            if len(codes) == 1:
                tdx_code = codes[0]
                match_type = "exact"
                confidence = 1.0
                if board_type == "industry":
                    routing_tier = "T1"
                    reason = "industry_exact"
                else:
                    routing_tier = "T2"
                    reason = "concept_exact"
                ohlc_source = OHLC_TDX
            else:
                reason = "exact_ambiguous"
        else:
            fuzzy_hits = [
                (tc, tn)
                for tn_key, pairs in tdx_by_norm.items()
                for tc, tn in pairs
                if len(nname) >= 2 and (nname in tn_key or tn_key in nname)
            ]
            # dedupe by tdx_code
            seen: set[str] = set()
            unique_hits: list[tuple[str, str]] = []
            for tc, tn in fuzzy_hits:
                if tc in seen:
                    continue
                seen.add(tc)
                unique_hits.append((tc, tn))
            if len(unique_hits) == 1:
                tdx_code, _ = unique_hits[0]
                match_type = "fuzzy"
                confidence = 0.7
                routing_tier = "T3"
                reason = "fuzzy_unique"
                ohlc_source = OHLC_TDX
            elif len(unique_hits) > 1:
                reason = "fuzzy_ambiguous"

        tier_counts[routing_tier] = tier_counts.get(routing_tier, 0) + 1
        source_counts[ohlc_source] = source_counts.get(ohlc_source, 0) + 1
        out_rows.append(
            {
                "sector_code": code,
                "sector_name": name,
                "board_type": board_type,
                "ohlc_source": ohlc_source,
                "tdx_code": tdx_code,
                "match_type": match_type,
                "confidence": confidence,
                "routing_tier": routing_tier,
                "reason": reason,
                "as_of": as_of,
            }
        )

    df = pl.DataFrame(out_rows) if out_rows else pl.DataFrame()
    summary = {
        "as_of": as_of.isoformat(),
        "em_boards": len(em_boards),
        "tdx_indices": len(tdx_rows),
        "tier_counts": tier_counts,
        "ohlc_source_counts": source_counts,
        "tdx_routed": source_counts.get(OHLC_TDX, 0),
        "em_routed": source_counts.get(OHLC_EM, 0),
    }
    return df, summary


def _latest_em_boards_from_lake(config: Config) -> list[dict]:
    root = config.curated_root / "sector_bars"
    if not root.exists():
        return []
    parts = sorted(root.glob("trade_date=*"))
    if not parts:
        return []
    latest = parts[-1]
    files = list(latest.glob("*.parquet"))
    if not files:
        return []
    df = pl.read_parquet(files[0])
    return [
        {
            "sector_code": r["sector_code"],
            "sector_name": r["sector_name"],
            "board_type": r["board_type"],
        }
        for r in df.select("sector_code", "sector_name", "board_type").iter_rows(named=True)
    ]


def _fetch_em_boards_live() -> list[dict]:
    from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient
    from cn_market_lake.adapters.eastmoney.rotation import (
        _CONCEPT_FS,
        _INDUSTRY_FS,
        _dedupe_boards,
        _fetch_board_rows,
    )

    with EastMoneyClient(min_interval=0.2) as client:
        boards = _dedupe_boards(
            _fetch_board_rows(client, _CONCEPT_FS, "concept")
            + _fetch_board_rows(client, _INDUSTRY_FS, "industry")
        )
    return [
        {
            "sector_code": b["sector_code"],
            "sector_name": b["sector_name"],
            "board_type": b["board_type"],
        }
        for b in boards
    ]


def _fetch_tdx_indices_live() -> list[dict]:
    """TDX sector pseudo-indices (88xxxx), read straight off the security list."""
    import re

    from cn_market_lake.adapters.tdx_protocol.client import _quotes_client
    from cn_market_lake.adapters.tdx_protocol.quotes import MARKET_SH, MARKET_SZ

    pattern = re.compile(r"^88\d{4}$")
    client = _quotes_client(None)
    try:
        rows: list[dict] = []
        for market in (MARKET_SZ, MARKET_SH):
            rows.extend(client.stocks(market))
    finally:
        client.close()

    return [
        {"tdx_code": str(r["code"]).strip(), "name": str(r.get("name", "")).strip()}
        for r in rows
        if pattern.match(str(r.get("code", "")).strip())
    ]


def derive_sector_routing(config: Config, *, as_of: date | None = None) -> dict:
    """Build and persist ``meta/sector_ohlc_routing.parquet``."""
    as_of = as_of or date.today()
    em_boards: list[dict] = []
    tdx_indices: list[dict] = []
    notes: list[str] = []

    try:
        em_boards = _fetch_em_boards_live()
    except Exception as exc:
        notes.append(f"em_live_failed:{exc}")
        em_boards = _latest_em_boards_from_lake(config)
        if not em_boards:
            raise RuntimeError(
                "sector_routing: EastMoney clist failed and no lake snapshot"
            ) from exc

    try:
        tdx_indices = _fetch_tdx_indices_live()
    except Exception as exc:
        raise RuntimeError(f"sector_routing: TDX stock_all failed: {exc}") from exc

    df, summary = build_sector_routing(em_boards, tdx_indices, as_of=as_of)
    if df.is_empty():
        raise RuntimeError("sector_routing: empty routing table")

    config.meta_root.mkdir(parents=True, exist_ok=True)
    out = _routing_path(config)
    df.write_parquet(out)
    summary["notes"] = notes
    summary["generated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    _summary_path(config).write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary
