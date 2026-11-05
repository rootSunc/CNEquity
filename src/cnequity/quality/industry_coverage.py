"""Industry classification completeness: by system, level, industry and snapshot date.

A snapshot is held against the stocks listed on its date, so a classification
that silently loses a slice of the market shows up as missing members, and one
that loses a whole industry shows up as an industry that was there in the
previous snapshot and is gone or sharply smaller in this one.

Levels are compared only between snapshots whose rows all carry them. EastMoney
snapshots written before schema version 2 (``data_version=v1``) kept one board
per stock at whichever level the source page order left last; their per-level
counts move without any change in the market, so they are reported as partial
and left out of the industry comparison.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.config import Config

#: A snapshot covering less than this share of the listed stocks is a gap.
MIN_COVERAGE = 0.99
#: An industry this large in the previous snapshot is worth comparing.
MIN_MEMBERS = 5
#: Losing more than this share of its members between snapshots is a drop.
MAX_SHRINK = 0.5
SAMPLE = 10
#: Listed equity that a classification covers: CDRs (e.g. 689009.SH) are classified too.
CLASSIFIED_TYPES = ("stock", "cdr")
#: Shenwan assigns an IPO's class before its first session; reported, not a gap.
INFORMATIONAL = frozenset({"before_listing"})
#: When more than this share of stocks change level-3 industry between two
#: snapshots, the taxonomy itself was replaced (Shenwan 2014 -> 2021 moved 66%
#: on 2021-07-30; its yearly July reassignments move 1-5%).
RECLASSIFICATION_SHARE = 0.3


def _group() -> pl.Expr:
    return (
        pl.when(pl.col("symbol").str.ends_with(".BJ")).then(pl.lit("BJ")).otherwise(pl.lit("SH_SZ"))
    )


def _instruments(config: Config) -> pl.DataFrame:
    from cnequity.query.reader import load

    return load("instruments", config=config).select(
        "symbol", "asset_type", "list_date", "delist_date"
    )


def _listed_stocks(inst: pl.DataFrame) -> pl.DataFrame:
    return inst.filter(
        pl.col("asset_type").is_in(CLASSIFIED_TYPES) & pl.col("list_date").is_not_null()
    ).select("symbol", "list_date", "delist_date")


def _unexpected(members: pl.DataFrame, inst: pl.DataFrame) -> dict:
    """Rows for securities that were not listed stocks on the snapshot date, by reason."""
    rows = members.select("symbol", "as_of_date").unique().join(inst, on="symbol", how="left")
    reason = (
        pl.when(pl.col("asset_type").is_null())
        .then(pl.lit("unknown_security"))
        .when(~pl.col("asset_type").is_in(CLASSIFIED_TYPES))
        .then(pl.lit("not_a_stock"))
        .when(pl.col("delist_date").is_not_null() & (pl.col("as_of_date") >= pl.col("delist_date")))
        .then(pl.lit("after_delisting"))
        .when(pl.col("list_date").is_null())
        .then(pl.lit("unknown_listing_date"))
        .when(pl.col("as_of_date") < pl.col("list_date"))
        .then(pl.lit("before_listing"))
    )
    hits = rows.with_columns(reason.alias("reason")).filter(pl.col("reason").is_not_null())
    return {
        r["reason"]: {"rows": r["rows"], "symbols": r["symbols"], "sample": r["sample"]}
        for r in hits.group_by("reason")
        .agg(
            pl.len().alias("rows"),
            pl.col("symbol").n_unique().alias("symbols"),
            pl.col("symbol").unique().sort().head(SAMPLE).alias("sample"),
        )
        .sort("reason")
        .iter_rows(named=True)
    }


def _expected(listed: pl.DataFrame, days: list[date]) -> pl.DataFrame:
    grid = listed.join(
        pl.DataFrame({"as_of_date": days}, schema={"as_of_date": pl.Date}), how="cross"
    )
    return grid.filter(
        (pl.col("list_date") <= pl.col("as_of_date"))
        & (pl.col("delist_date").is_null() | (pl.col("delist_date") > pl.col("as_of_date")))
    ).select("symbol", "as_of_date")


def _coverage(members: pl.DataFrame, listed: pl.DataFrame) -> list[dict]:
    days = members["as_of_date"].unique().sort().to_list()
    expected = _expected(listed, days).with_columns(_group().alias("group"))
    present = members.select("symbol", "as_of_date").unique().with_columns(pl.lit(True).alias("_p"))
    joined = expected.join(present, on=["symbol", "as_of_date"], how="left")
    by_day = (
        joined.group_by("as_of_date", "group")
        .agg(
            pl.len().alias("expected"),
            pl.col("_p").sum().alias("present"),
            pl.col("symbol")
            .filter(pl.col("_p").is_null())
            .sort()
            .head(SAMPLE)
            .alias("missing_sample"),
        )
        .with_columns(
            (pl.col("expected") - pl.col("present")).alias("missing"),
            (pl.col("present") / pl.col("expected")).round(4).alias("coverage"),
        )
        .sort("as_of_date", "group")
    )
    return by_day.with_columns(pl.col("as_of_date").cast(pl.Utf8)).to_dicts()


def _partial_days(members: pl.DataFrame) -> list[date]:
    return (
        members.filter(pl.col("data_version") == "v1")
        .filter(pl.col("classification_system") == "eastmoney")["as_of_date"]
        .unique()
        .sort()
        .to_list()
    )


def _snapshot_pairs(complete: pl.DataFrame) -> pl.DataFrame:
    """Consecutive snapshot pairs with the share of common stocks whose level-3 code changed."""
    days = complete["as_of_date"].unique().sort().to_list()
    pairs = pl.DataFrame(
        {"prev_date": days[:-1], "as_of_date": days[1:]},
        schema={"prev_date": pl.Date, "as_of_date": pl.Date},
    )
    leaf = complete.select("symbol", "as_of_date", pl.col("l3_code").alias("code"))
    moved = (
        pairs.join(leaf.rename({"as_of_date": "prev_date", "code": "prev_code"}), on="prev_date")
        .join(leaf, on=["as_of_date", "symbol"])
        .group_by("prev_date", "as_of_date")
        .agg(
            pl.len().alias("common"),
            (pl.col("prev_code") != pl.col("code")).sum().alias("moved"),
        )
    )
    return pairs.join(moved, on=["prev_date", "as_of_date"], how="left").with_columns(
        (pl.col("moved") / pl.col("common")).fill_null(0.0).round(4).alias("moved_share")
    )


def _industry_drops(members: pl.DataFrame, partial: set[date]) -> tuple[list[dict], list[dict]]:
    """Industries that vanish or lose more than ``MAX_SHRINK`` of their members,
    and the snapshot pairs where the taxonomy was replaced (not compared)."""
    complete = members.filter(~pl.col("as_of_date").is_in(sorted(partial)))
    if complete.is_empty():
        return [], []
    pairs = _snapshot_pairs(complete)
    if pairs.is_empty():
        return [], []
    replaced = pairs.filter(pl.col("moved_share") > RECLASSIFICATION_SHARE)
    reclassified = [
        {
            "prev_date": str(r["prev_date"]),
            "as_of_date": str(r["as_of_date"]),
            "moved_share": r["moved_share"],
        }
        for r in replaced.iter_rows(named=True)
    ]
    pairs = pairs.filter(pl.col("moved_share") <= RECLASSIFICATION_SHARE).select(
        "prev_date", "as_of_date"
    )
    out = []
    for level in (1, 2, 3):
        code, name = f"l{level}_code", f"l{level}_name"
        counts = (
            complete.filter(pl.col(code).is_not_null())
            .group_by("as_of_date", code)
            .agg(pl.len().alias("members"), pl.col(name).first().alias("name"))
            .rename({code: "industry_code"})
        )
        prev = pairs.join(
            counts.rename({"as_of_date": "prev_date", "members": "prev_members"}), on="prev_date"
        )
        cmp = prev.join(
            counts.select("as_of_date", "industry_code", "members"),
            on=["as_of_date", "industry_code"],
            how="left",
        ).with_columns(pl.col("members").fill_null(0))
        dropped = cmp.filter(
            (pl.col("prev_members") >= MIN_MEMBERS)
            & (pl.col("members") < pl.col("prev_members") * (1 - MAX_SHRINK))
        )
        out.extend(
            {
                "level": f"L{level}",
                "industry_code": r["industry_code"],
                "name": r["name"],
                "prev_date": str(r["prev_date"]),
                "as_of_date": str(r["as_of_date"]),
                "prev_members": r["prev_members"],
                "members": r["members"],
                "kind": "vanished" if r["members"] == 0 else "shrank",
            }
            for r in dropped.sort("as_of_date", "industry_code").iter_rows(named=True)
        )
    return out, reclassified


def industry_coverage_report(
    config: Config,
    *,
    start: date | None = None,
    end: date | None = None,
    today: date | None = None,
) -> dict:
    """Completeness of ``industry_members`` per classification system; read-only."""
    from cnequity.domain.market_time import last_closed_session
    from cnequity.query.reader import load

    members = load("industry_members", config=config, start=start, end=end)
    inst = _instruments(config)
    listed = _listed_stocks(inst)
    anchor = today or last_closed_session()
    report: dict = {
        "dataset": "industry_members",
        "start": str(start) if start else None,
        "end": str(end) if end else None,
        "thresholds": {
            "min_coverage": MIN_COVERAGE,
            "min_members": MIN_MEMBERS,
            "max_shrink": MAX_SHRINK,
        },
        "systems": {},
        "state": "complete",
    }
    for (system,), part in members.partition_by(
        "classification_system", as_dict=True, maintain_order=False
    ).items():
        partial = _partial_days(part)
        coverage = _coverage(part, listed)
        gaps = [r for r in coverage if r["coverage"] < MIN_COVERAGE]
        drops, reclassified = _industry_drops(part, set(partial))
        unexpected = _unexpected(part, inst)
        last = part["as_of_date"].max()
        levels = {f"L{n}": int(part[f"l{n}_code"].drop_nulls().n_unique()) for n in (1, 2, 3)}
        report["systems"][system] = {
            "snapshots": part["as_of_date"].n_unique(),
            "first": str(part["as_of_date"].min()),
            "last": str(last),
            "days_since_last": (anchor - last).days if anchor else None,
            "industries_by_level": levels,
            "rows_without_levels": int(
                part.filter(
                    pl.all_horizontal(pl.col(f"l{n}_code").is_null() for n in (1, 2, 3))
                ).height
            ),
            "partial_level_snapshots": {
                "count": len(partial),
                "first": str(partial[0]) if partial else None,
                "last": str(partial[-1]) if partial else None,
            },
            "coverage_min": min((r["coverage"] for r in coverage), default=None),
            "coverage_gaps": gaps,
            "unexpected_members": unexpected,
            "industry_drops": drops,
            "reclassifications": reclassified,
        }
        if system == "sw":
            # Dropped from the snapshots, so only the record still shows them.
            from cnequity.storage.never_listed import load_never_listed

            dropped = load_never_listed(config)
            report["systems"][system]["excluded_never_listed"] = {
                "symbols": len(dropped),
                "sample": [r["symbol"] for r in dropped[:SAMPLE]],
            }
        if gaps or drops or set(unexpected) - INFORMATIONAL:
            report["state"] = "gaps"
    return report
