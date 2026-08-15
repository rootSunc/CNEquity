#!/usr/bin/env python3
"""Real-source single-day smoke test + failure/retry validation."""

from __future__ import annotations

import json
import socket
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cn_market_lake.config import load_config
from cn_market_lake.orchestrator.engine import JobEngine
from cn_market_lake.orchestrator.manifest import Manifest
from cn_market_lake.steps.finalize import step_compact
from cn_market_lake.storage.state import StateStore

CONFIG = ROOT / "configs" / "cn-market-lake.toml"
TRADE_DATE = date(2026, 6, 26)


def _print(title: str, payload: object) -> None:
    print(f"\n=== {title} ===", flush=True)
    if isinstance(payload, (dict, list)):
        print(json.dumps(payload, indent=2, default=str), flush=True)
    else:
        print(payload, flush=True)


def _check_success_run(cfg, run_id: str) -> list[str]:
    errors: list[str] = []
    manifest = Manifest(cfg.manifest_path)
    summary = manifest.run_summary(run_id)
    if summary.get("run", {}).get("status") != "success":
        errors.append(f"run status={summary.get('run', {}).get('status')!r}, expected success")
    if summary.get("batch_counts", {}).get("failed", 0):
        errors.append(f"failed batches: {summary['batch_counts']}")

    for ds, part in (
        ("daily_bars", f"trade_date={TRADE_DATE.isoformat()}"),
        ("trading_status", f"trade_date={TRADE_DATE.isoformat()}"),
    ):
        part_path = cfg.curated_root / ds / part / "part-merged.parquet"
        if not part_path.exists():
            errors.append(f"missing curated partition: {part_path}")

    findings_path = cfg.meta_root / "quality" / "findings" / f"{run_id}.json"
    if not findings_path.exists():
        errors.append(f"missing audit findings: {findings_path}")
    else:
        payload = json.loads(findings_path.read_text(encoding="utf-8"))
        for f in payload.get("findings", []):
            if f.get("severity") == "error" or f.get("check") in ("mock_source", "pk_duplicate"):
                errors.append(f"audit finding: {f}")

    diffs_path = cfg.meta_root / "quality" / "source_diffs" / f"{run_id}.json"
    if not diffs_path.exists():
        errors.append(f"missing source_diffs: {diffs_path}")

    return errors


def main() -> int:
    cfg = load_config(CONFIG)
    engine = JobEngine(cfg)
    state = StateStore(cfg.meta_root)

    _print("Phase 1", "reference wave")
    curated_inst = cfg.curated_root / "instruments" / "part-merged.parquet"
    if curated_inst.exists():
        _print("Phase 1", "skipped (instruments already in curated)")
        ref = {"run_id": "skipped", "status": "success"}
    else:
        ref = engine.run_job(
            "smoke:reference",
            TRADE_DATE,
            steps=["instruments", "trading_calendar", "trading_status"],
        )
        if ref["status"] != "success":
            _print("FAILED", ref)
            return 1
        step_compact(cfg, TRADE_DATE, ref["run_id"], {})

    # True single-day incremental (skip 5-day cold-start window).
    prev = TRADE_DATE - timedelta(days=1)
    state.set_date("daily_bars", prev)
    state.set_date("index_bars", prev)

    _print("Phase 2", f"daily job for {TRADE_DATE} (1-day bar window)")
    daily = engine.run_job("daily", TRADE_DATE)
    _print("daily status", Manifest(cfg.manifest_path).run_summary(daily["run_id"]))
    ok_errors = _check_success_run(cfg, daily["run_id"])
    if ok_errors:
        _print("SUCCESS CHECK FAILED", ok_errors)
        return 1
    _print("SUCCESS CHECK", "passed")

    wm_before_fail = state.get_date("daily_bars")
    _print("watermark before failure test", wm_before_fail)

    _print("Phase 3", "blocked network during daily_bars (new run)")
    state.set_date("daily_bars", prev)
    real_socket = socket.socket

    def _blocked(*args, **kwargs):
        raise OSError("[Errno 51] Network is unreachable")

    socket.socket = _blocked  # type: ignore[misc]
    try:
        fail = engine.run_job(
            "smoke:fail",
            TRADE_DATE,
            steps=["daily_bars", "compact", "audit"],
        )
    finally:
        socket.socket = real_socket

    fail_id = fail["run_id"]
    summary_fail = Manifest(cfg.manifest_path).run_summary(fail_id)
    _print("failure run status", summary_fail)

    wm_after_fail = state.get_date("daily_bars")
    findings = json.loads(
        (cfg.meta_root / "quality" / "findings" / f"{fail_id}.json").read_text(encoding="utf-8")
    )
    skipped = [f for f in findings.get("findings", []) if f.get("check") == "compact_skipped"]

    fail_errors: list[str] = []
    if summary_fail.get("batch_counts", {}).get("failed", 0) == 0:
        fail_errors.append("expected failed daily_bars batches")
    if wm_after_fail != prev:
        fail_errors.append(f"watermark moved to {wm_after_fail!r}, expected {prev!r}")
    if not skipped:
        fail_errors.append("expected compact_skipped audit warning")
    if fail_errors:
        _print("FAILURE CONTRACT CHECK FAILED", fail_errors)
        return 1
    _print("FAILURE CONTRACT CHECK", "passed")

    _print("Phase 4", "cml retry — restore network")
    retry = engine.run_job("retry", retry_failed_only=True, run_id=fail_id, trade_date=TRADE_DATE)
    final = Manifest(cfg.manifest_path).run_summary(fail_id)
    _print("retry result", retry)
    _print("final status", final)

    retry_errors: list[str] = []
    if retry.get("status") != "success":
        retry_errors.append(f"retry status={retry.get('status')!r}")
    if final.get("batch_counts", {}).get("failed", 0):
        retry_errors.append("batches still failed after retry")
    if state.get_date("daily_bars") != TRADE_DATE:
        retry_errors.append(f"watermark={state.get_date('daily_bars')!r}, expected {TRADE_DATE!r}")
    if retry_errors:
        _print("RETRY CHECK FAILED", retry_errors)
        return 1

    _print("ALL CHECKS", "passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
