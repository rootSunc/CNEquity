"""`cml serve` — the read-only lake dashboard."""

from cn_market_lake.serve.app import create_app
from cn_market_lake.serve.lake import LakeView

__all__ = ["LakeView", "create_app"]
