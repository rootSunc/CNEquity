"""Total share counts as the lake knows them, for valuation arithmetic.

The daily valuation step and the valuation-basis repair both read them; it
used to live in the repair module.
"""

from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from cnequity.config import Config
from cnequity.storage.revisions import RevisionStore


def load_share_counts(config: Config) -> pl.DataFrame:
    """Latest known total share count per (symbol, change_date), or empty.

    Empty only when the lake holds no share history; a history that cannot
    be read is an error, not a reason to keep estimates silently.
    """
    from cnequity.query import load

    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root, create=False)
    if (
        store.current_root("share_structure") is None
        and not (config.curated_root / "share_structure").is_dir()
    ):
        return pl.DataFrame(
            schema={"symbol": pl.Utf8, "change_date": pl.Date, "total_shares": pl.Float64}
        )
    frame = load("share_structure", as_of=datetime.now(timezone.utc).date(), config=config)
    return frame.select("symbol", "change_date", "total_shares")
