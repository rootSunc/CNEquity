"""同花顺 official API (fuyao.aicubes.cn) — a licensed peer source, never a primary.

Registered client, distinct from the ``ths`` adapter next door: that one reads
10jqka's public pages unauthenticated. See
``docs/zh/datasets/catalog.md`` for usage requirements and
the constraints this source operates under.
"""

from cnequity.adapters.ths_official.client import (
    BASE_URL,
    SOURCE,
    ThsOfficialAuthError,
    ThsOfficialClient,
    ThsOfficialError,
    ThsOfficialParameterError,
    ThsOfficialRateLimited,
    client_from_config,
)

__all__ = [
    "BASE_URL",
    "SOURCE",
    "ThsOfficialAuthError",
    "ThsOfficialClient",
    "ThsOfficialError",
    "ThsOfficialParameterError",
    "ThsOfficialRateLimited",
    "client_from_config",
]
