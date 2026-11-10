"""Fill ``industry_members`` level columns on rows written before schema version 2.

Shenwan rows get their levels from the six-digit code alone. A v1 EastMoney row
kept one of the stock's three boards, picked by page order; this fills that
board's level and the levels above it, from the board hierarchy the source
publishes today. The levels below it were never stored and stay null, and
``industry_code`` keeps the board it held: a v1 row is not turned into a
level-3 leaf it cannot prove.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import nullcontext

import polars as pl

from cnequity.config import Config
from cnequity.domain.industry_levels import LEVEL_COLUMNS, board_ancestry, sw_level_columns
from cnequity.file_lock import lake_mutation_lock
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.read_context import ReadContext
from cnequity.storage.repairs.publish import publish_repair
from cnequity.storage.revisions import RevisionStore

DATASET = "industry_members"


def fetch_em_board_hierarchy(config: Config) -> pl.DataFrame:
    """Today's EastMoney industry boards as ``[symbol, board_code, board_name]``."""
    from cnequity.adapters.eastmoney.common import exchange_from_datacenter, symbol_from_em
    from cnequity.adapters.eastmoney.datacenter import fetch_datacenter
    from cnequity.adapters.eastmoney.em_auth import EastMoneyClient
    from cnequity.adapters.eastmoney.industry import (
        _BOARD_COLUMNS,
        _BOARD_REPORT,
        _INDUSTRY_BOARD_TYPE,
    )

    client = EastMoneyClient(config=config)
    try:
        raw = fetch_datacenter(
            client,
            _BOARD_REPORT,
            _BOARD_COLUMNS,
            filter_expr=f'(BOARD_TYPE_NEW="{_INDUSTRY_BOARD_TYPE}")',
            page_size=5000,
            trust_page_size=True,
        )
    finally:
        client.close()
    rows = []
    for item in raw:
        code = str(item.get("SECURITY_CODE", "")).zfill(6)
        exch = exchange_from_datacenter(item)
        sym = symbol_from_em(code, 1 if exch == "SH" else (2 if exch == "BJ" else 0))
        board = str(item.get("BOARD_CODE") or "").strip()
        name = str(item.get("BOARD_NAME") or "").strip()
        if sym and board and name:
            rows.append({"symbol": sym, "board_code": board, "board_name": name})
    return pl.DataFrame(
        rows, schema={"symbol": pl.Utf8, "board_code": pl.Utf8, "board_name": pl.Utf8}
    )


def _needs_levels(frame: pl.DataFrame) -> pl.Expr:
    present = [c for c in LEVEL_COLUMNS if c in frame.columns]
    if not present:
        return pl.lit(True)
    return pl.all_horizontal(pl.col(c).is_null() for c in present)


def fill_levels(frame: pl.DataFrame, ancestry: pl.DataFrame | None) -> pl.DataFrame:
    """``frame`` with level columns filled where they are all null; other rows untouched."""
    out = frame.with_columns(
        pl.lit(None, dtype=pl.Utf8).alias(c) for c in LEVEL_COLUMNS if c not in frame.columns
    )
    todo = _needs_levels(out)
    sw = todo & (pl.col("classification_system") == "sw")
    out = out.with_columns(
        pl.when(sw)
        .then(expr)
        .otherwise(pl.col(expr.meta.output_name()))
        .alias(expr.meta.output_name())
        for expr in sw_level_columns()
    )
    if ancestry is None or ancestry.is_empty():
        return out
    em = todo & (pl.col("classification_system") == "eastmoney")
    joined = out.join(
        ancestry.select("board_code", *(pl.col(c).alias(f"__{c}") for c in LEVEL_COLUMNS)),
        left_on="industry_code",
        right_on="board_code",
        how="left",
    )
    return joined.with_columns(
        pl.when(em).then(pl.col(f"__{c}")).otherwise(pl.col(c)).alias(c) for c in LEVEL_COLUMNS
    ).select(out.columns)


def repair_industry_levels(
    config: Config,
    *,
    apply: bool = False,
    hierarchy: Callable[[Config], pl.DataFrame] = fetch_em_board_hierarchy,
) -> dict:
    """Preview, or publish one gated revision that fills the level columns."""
    with lake_mutation_lock(config.meta_root) if apply else nullcontext():
        store = RevisionStore(
            config.meta_root, config.curated_root, config.derived_root, create=apply
        )
        if apply:
            store.ensure_current(DATASET)
        context = ReadContext.capture(config, (DATASET,), {})
        root = context.roots[DATASET]
        report: dict = {"dataset": DATASET, "applied": False, "rows": 0}
        if not dataset_has_parquet(root, committed=False):
            return report
        frame = scan_parquet_root(root, committed=False)
        names = frame.collect_schema().names()
        missing = frame.filter(
            pl.all_horizontal(pl.col(c).is_null() for c in LEVEL_COLUMNS if c in names)
            if any(c in names for c in LEVEL_COLUMNS)
            else pl.lit(True)
        )
        counts = dict(missing.group_by("classification_system").agg(pl.len()).collect().iter_rows())
        report.update(rows=sum(counts.values()), by_system=counts)
        ancestry = None
        if counts.get("eastmoney"):
            ancestry = board_ancestry(hierarchy(config))
            codes = missing.filter(pl.col("classification_system") == "eastmoney").select(
                "industry_code"
            )
            unknown = (
                codes.join(
                    ancestry.lazy().select(pl.col("board_code").alias("industry_code")),
                    on="industry_code",
                    how="anti",
                )
                .select(pl.len())
                .collect()
                .item()
            )
            report["eastmoney_boards_not_listed_today"] = unknown
            report["eastmoney_levels"] = dict(
                missing.filter(pl.col("classification_system") == "eastmoney")
                .join(
                    ancestry.lazy().select(pl.col("board_code").alias("industry_code"), "level"),
                    on="industry_code",
                    how="left",
                )
                .group_by(pl.col("level").cast(pl.Utf8).fill_null("unknown"))
                .agg(pl.len())
                .collect()
                .iter_rows()
            )
        if not apply or not report["rows"]:
            return report
        report.update(
            publish_repair(
                config,
                store,
                DATASET,
                "as_of_date",
                root,
                keep=lambda part: fill_levels(part, ancestry),
                reason="industry_levels",
                metadata={"by_system": counts},
            )
        )
        report["applied"] = True
        return report
