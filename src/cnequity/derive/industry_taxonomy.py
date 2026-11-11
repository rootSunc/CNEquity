"""Stable industry identities across ``industry_members`` — lake-only derive.

An industry code is not an identity on its own. Shenwan reused code strings
when it replaced its 2014 taxonomy on 2021-07-30: of 161 level-3 code strings in
both versions, only 54% of the stocks under them kept the same string, so
``480101`` names a different industry before and after that date. EastMoney
board codes kept one name each over every snapshot seen (2026-07..10), so a
board code is its own identity.

Each industry gets ``industry_id = "{system}:{version}:{code}"`` — for example
``sw:2021:480101``, ``sw:2014:480101``, ``eastmoney:board:1279`` — with its level,
latest name, parent, and the first and last snapshot it appeared in. Names are
only what the source published: the Shenwan file has none.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from cnequity.config import Config
from cnequity.storage.atomic import write_json_atomic, write_parquet_atomic

TAXONOMY = "industry_taxonomy"
#: First snapshot under Shenwan 2021 codes; 66% of stocks changed code on it.
SW_2021_FROM = date(2021, 7, 30)

SCHEMA = {
    "industry_id": pl.Utf8,
    "classification_system": pl.Utf8,
    "version": pl.Utf8,
    "level": pl.Int8,
    "code": pl.Utf8,
    "name": pl.Utf8,
    "parent_id": pl.Utf8,
    "first_seen": pl.Date,
    "last_seen": pl.Date,
    "snapshots": pl.UInt32,
    "max_members": pl.UInt32,
    "names_seen": pl.UInt32,
}


def _path(config: Config) -> Path:
    return config.meta_root / f"{TAXONOMY}.parquet"


def _summary_path(config: Config) -> Path:
    return config.meta_root / f"{TAXONOMY}_summary.json"


def _version() -> pl.Expr:
    system = pl.col("classification_system")
    return (
        pl.when(system == "sw")
        .then(
            pl.when(pl.col("as_of_date") >= SW_2021_FROM)
            .then(pl.lit("2021"))
            .otherwise(pl.lit("2014"))
        )
        .otherwise(pl.lit("board"))
    )


def _id(code: str) -> pl.Expr:
    return pl.concat_str(
        [pl.col("classification_system"), pl.col("version"), pl.col(code)], separator=":"
    )


def build_industry_taxonomy(members: pl.DataFrame) -> pl.DataFrame:
    """One row per industry identity from ``industry_members`` rows with level columns."""
    if members.is_empty():
        return pl.DataFrame(schema=SCHEMA)
    rows = members.with_columns(_version().alias("version"))
    parts = []
    for level in (1, 2, 3):
        code, name = f"l{level}_code", f"l{level}_name"
        parent = (
            _id(f"l{level - 1}_code").alias("parent_id")
            if level > 1
            else pl.lit(None, dtype=pl.Utf8).alias("parent_id")
        )
        parts.append(
            rows.filter(pl.col(code).is_not_null()).select(
                "classification_system",
                "version",
                pl.lit(level, dtype=pl.Int8).alias("level"),
                pl.col(code).alias("code"),
                pl.col(name).alias("name"),
                parent,
                "as_of_date",
                "symbol",
            )
        )
    long = pl.concat(parts).with_columns(_id("code").alias("industry_id"))
    sizes = long.group_by("industry_id", "as_of_date").agg(pl.col("symbol").n_unique().alias("n"))
    out = (
        long.sort("as_of_date")
        .group_by("industry_id", maintain_order=True)
        .agg(
            pl.col("classification_system").first(),
            pl.col("version").first(),
            pl.col("level").first(),
            pl.col("code").first(),
            pl.col("name").drop_nulls().last(),
            # A parent seen for most of the industry's life, not one stray row.
            pl.col("parent_id").drop_nulls().mode().sort().first(),
            pl.col("as_of_date").min().alias("first_seen"),
            pl.col("as_of_date").max().alias("last_seen"),
            pl.col("as_of_date").n_unique().cast(pl.UInt32).alias("snapshots"),
            pl.col("name").drop_nulls().n_unique().cast(pl.UInt32).alias("names_seen"),
        )
        .join(
            sizes.group_by("industry_id").agg(
                pl.col("n").max().cast(pl.UInt32).alias("max_members")
            ),
            on="industry_id",
        )
    )
    return (
        out.select(list(SCHEMA))
        .cast(SCHEMA)
        .sort("classification_system", "version", "level", "code")
    )


def derive_industry_taxonomy(config: Config) -> dict:
    """Build and persist ``meta/industry_taxonomy.parquet`` from the lake only."""
    from cnequity.query.reader import load

    members = load("industry_members", config=config)
    if members.is_empty():
        raise RuntimeError(
            "industry_taxonomy: industry_members is empty — run the daily update first"
        )
    table = build_industry_taxonomy(members)
    parents = table.filter(pl.col("parent_id").is_not_null())
    orphans = parents.join(
        table.select(pl.col("industry_id").alias("parent_id")), on="parent_id", how="anti"
    )
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "industries": table.height,
        "by_system_version_level": {
            f"{s}:{v}:L{lv}": n
            for s, v, lv, n in table.group_by("classification_system", "version", "level")
            .len()
            .sort("classification_system", "version", "level")
            .iter_rows()
        },
        "parents_missing": orphans.height,
        "renamed": table.filter(pl.col("names_seen") > 1).height,
    }
    config.meta_root.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(_path(config), table)
    write_json_atomic(_summary_path(config), summary, indent=2, default=str)
    return summary
