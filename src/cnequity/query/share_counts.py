"""Total share counts as the lake knows them, for valuation arithmetic.

The daily valuation step and the valuation-basis repair both read them; it
used to live in the repair module.
"""

from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from cnequity.config import Config
from cnequity.query.parquet_scan import dataset_has_parquet


def load_share_counts(config: Config) -> pl.DataFrame:
    """Latest known total share count per (symbol, change_date), or empty.

    Empty only when the lake holds no share history; a history that cannot
    be read is an error, not a reason to keep estimates silently.
    """
    from cnequity.query import load

    if not dataset_has_parquet(config.curated_root / "share_structure"):
        return pl.DataFrame(
            schema={"symbol": pl.Utf8, "change_date": pl.Date, "total_shares": pl.Float64}
        )
    frame = load("share_structure", as_of=datetime.now(timezone.utc).date(), config=config)
    return frame.select("symbol", "change_date", "total_shares")
