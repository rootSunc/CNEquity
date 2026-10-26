"""通达信 quarterly financial packages (``tdxfin/gpcw*.zip``)."""

from cnequity.adapters.tdx_fin.packages import (
    BACKFILL_SOURCE,
    FIELD_MAP,
    SOURCE,
    TdxFinPayloadError,
    fetch_index,
    fetch_package,
    package_rows,
    parse_package,
)

__all__ = [
    "BACKFILL_SOURCE",
    "FIELD_MAP",
    "SOURCE",
    "TdxFinPayloadError",
    "fetch_index",
    "fetch_package",
    "package_rows",
    "parse_package",
]
