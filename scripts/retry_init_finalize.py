#!/usr/bin/env python3
"""Retry failed init batches, then compact + derive_adj_factors + audit."""

from __future__ import annotations

import json
import logging
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import cn_market_lake.steps  # noqa: F401
from cn_market_lake.config import load_config
from cn_market_lake.orchestrator.engine import JobEngine

RUN_ID = "12dfdb4d-46b6-46e8-b587-baae161e23a1"
TRADE_DATE = date(2026, 7, 6)
CONFIG = ROOT / "configs/cn-market-lake.toml"


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config(CONFIG)
    engine = JobEngine(cfg)

    logging.info("Retry failed batches (backfill=True) run_id=%s", RUN_ID)
    retry = engine.run_job(
        "retry",
        TRADE_DATE,
        retry_failed_only=True,
        run_id=RUN_ID,
        backfill=True,
    )
    logging.info("Retry result: %s", retry.get("status"))

    if retry.get("status") != "success":
        print(json.dumps(retry, indent=2, default=str))
        return 1

    logging.info("Finalize: derive_adj_factors + derive_industry_index + audit")
    finalize = engine.run_job(
        "init-finalize",
        TRADE_DATE,
        steps=["derive_adj_factors", "derive_industry_index", "audit"],
        run_id=RUN_ID,
    )
    out = {"retry": retry, "finalize": finalize}
    print(json.dumps(out, indent=2, default=str))
    return 0 if finalize.get("status") == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
