"""Compatibility import: the holiday table now lives in ``cnequity.domain.holidays_cn``.

It is reference data the registry and schemas need, and ``domain`` must not
depend on ``adapters``. Edit ``domain/holidays_cn.py``; this module only
re-exports it for existing imports.
"""

from cnequity.domain.holidays_cn import CLOSED_DATES, EXTRA_TRADING_DATES

__all__ = ["CLOSED_DATES", "EXTRA_TRADING_DATES"]
