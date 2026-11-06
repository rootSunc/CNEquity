"""Membership sources without weights must represent that absence explicitly."""

from datetime import date

import polars as pl


def index_members_as_of(frame: pl.DataFrame, as_of: date) -> pl.DataFrame:
    """Use one retained snapshot per index, never a member-by-member carry forward."""
    eligible = frame.filter(pl.col("as_of_date") <= as_of)
    dates = eligible.group_by("index_symbol").agg(pl.col("as_of_date").max())
    return eligible.join(dates, on=["index_symbol", "as_of_date"], how="inner")
