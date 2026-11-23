"""Official daily files of the domestic futures exchanges.

Kept free of heavy imports: config validation reads ``SUPPORTED_EXCHANGES``.
"""

#: Exchanges this release can read, as lake suffixes, in publication order.
SUPPORTED_EXCHANGES: tuple[str, ...] = ("SHF", "DCE", "CZC", "GFE", "CFE")

#: Exchanges whose rows arrive in another exchange's file: INE's products are
#: published by SHFE, so the SHF reader writes both.
MEMBERS: dict[str, tuple[str, ...]] = {"SHF": ("SHF", "INE")}


def members(publisher: str) -> tuple[str, ...]:
    """Row-level ``exchange`` values one reader produces."""
    return MEMBERS.get(publisher, (publisher,))
