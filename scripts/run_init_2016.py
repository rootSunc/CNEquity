#!/usr/bin/env python3
"""Full init from 2016-01-01: thin wrapper around ``cml init``."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "configs" / "cn-market-lake.toml"
TRADE_DATE = "2026-07-06"


def main() -> int:
    cmd = [
        sys.executable,
        "-m",
        "cn_market_lake.cli.main",
        "init",
        "--config",
        str(CONFIG),
        "--trade-date",
        TRADE_DATE,
    ]
    return subprocess.call(cmd, cwd=ROOT)


if __name__ == "__main__":
    raise SystemExit(main())
