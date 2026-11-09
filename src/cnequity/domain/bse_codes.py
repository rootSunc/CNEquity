"""Beijing Stock Exchange codes: legacy 43/83/87xxx to current 920xxx.

The BSE moved its listed companies to 920xxx codes; vendors still send the old
codes for history, and some send them for current rows too. This is the BSE's
published old/new table (https://www.bse.cn/service/code_mapping.html), 248
pairs, observed 2026-09-26; the rendered table hashed to SHA-256
ed422aaf8f23dadba029c6ad07ab2f9578a94b41103075ec4dd1d9f041c055b2. It is data,
not a suffix rule: pairs such as 870726→920926 and 873305→920505 would send a
suffix guess to another company's history.

Why it has no dates, when ``security_identifiers`` does: the table carries no
conversion date (its listing-date column is not one), and the move renumbered
the same securities, so the lake stores a Beijing company under its current
code whatever date a row describes. Each SH/SZ change there is a separate
event, with an effective date and share ratio from an issuer announcement, and
vendors disagree on which code a past row carries, so the date decides.

Three copies of this table used to exist: a JSON seed read by a private
EastMoney adapter function that ten Tushare adapters imported, and a second
hand-typed copy for the payment-notice search.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

_MAPPING_PATH = Path(__file__).with_name("seeds") / "bse_code_mapping.json"
_PAIRS = 248


@lru_cache(maxsize=1)
def legacy_to_current() -> dict[str, str]:
    """Legacy six-digit code → current 920xxx code."""
    try:
        raw = json.loads(_MAPPING_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            f"BSE legacy/current code mapping is unreadable: {_MAPPING_PATH}"
        ) from exc
    if not isinstance(raw, dict):
        raise RuntimeError(f"BSE legacy/current code mapping is not an object: {_MAPPING_PATH}")
    mapping = {str(old): str(new) for old, new in raw.items()}
    if len(mapping) != _PAIRS or len(set(mapping.values())) != _PAIRS:
        raise RuntimeError("BSE code mapping is incomplete or maps two codes to one")
    return mapping


@lru_cache(maxsize=1)
def current_to_legacy() -> dict[str, str]:
    """Current 920xxx code → legacy code."""
    return {new: old for old, new in legacy_to_current().items()}


@lru_cache(maxsize=1)
def legacy_symbol_map() -> dict[str, str]:
    """``"833454.BJ"`` → ``"920454.BJ"``, for ``Expr.replace`` on vendor symbols."""
    return {f"{old}.BJ": f"{new}.BJ" for old, new in legacy_to_current().items()}
