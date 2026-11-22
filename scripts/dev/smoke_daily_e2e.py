#!/usr/bin/env python3
"""Real-source single-day smoke test + failure/retry validation.

It fetches from live sources and rewinds the ``daily_bars`` / ``index_bars``
watermarks of the lake it runs against, so point it at a throwaway lake only.
There is deliberately no default config: run without ``--config`` and it stops.

    python scripts/dev/smoke_daily_e2e.py --config /tmp/smoke-lake/cnequity.toml
"""

from __future__ import annotations

import argparse
import json
import socket
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from cnequity.config import load_config
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.manifest import Manifest
from cnequity.query.reader import load
from cnequity.query.state import dataset_state
from cnequity.run_scope import set_backfill
from cnequity.steps.finalize import step_compact

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

    for ds in ("daily_bars", "trading_status"):
        frame = load(ds, start=TRADE_DATE, end=TRADE_DATE, config=cfg)
        if frame.is_empty():
            errors.append(f"missing published rows for {ds} on {TRADE_DATE.isoformat()}")

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
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="config of a throwaway lake; this rewinds its watermarks and fetches live data",
    )
    args = parser.parse_args()
    cfg = load_config(args.config)
    engine = JobEngine(cfg)

    _print("Phase 1", "reference wave")
    if dataset_state("instruments", config=cfg).revision_id is not None:
        _print("Phase 1", "skipped (instruments already published)")
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

    _print("Phase 2", f"daily job for {TRADE_DATE} (1-day bar window)")
    set_backfill(cfg, active=True, start=TRADE_DATE, end=TRADE_DATE, redo="all")
    daily = engine.run_job("daily", TRADE_DATE)
    _print("daily status", Manifest(cfg.manifest_path).run_summary(daily["run_id"]))
    ok_errors = _check_success_run(cfg, daily["run_id"])
    if ok_errors:
        _print("SUCCESS CHECK FAILED", ok_errors)
        return 1
    _print("SUCCESS CHECK", "passed")

    rev_before_fail = dataset_state("daily_bars", config=cfg).revision_id
    _print("revision before failure test", rev_before_fail)

    _print("Phase 3", "blocked network during daily_bars (new run)")
    set_backfill(cfg, active=True, start=TRADE_DATE, end=TRADE_DATE, redo="all")
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

    rev_after_fail = dataset_state("daily_bars", config=cfg).revision_id
    findings = json.loads(
        (cfg.meta_root / "quality" / "findings" / f"{fail_id}.json").read_text(encoding="utf-8")
    )
    skipped = [f for f in findings.get("findings", []) if f.get("check") == "compact_skipped"]

    fail_errors: list[str] = []
    if summary_fail.get("batch_counts", {}).get("failed", 0) == 0:
        fail_errors.append("expected failed daily_bars batches")
    if rev_after_fail != rev_before_fail:
        fail_errors.append(f"revision moved to {rev_after_fail!r}, expected {rev_before_fail!r}")
    if not skipped:
        fail_errors.append("expected compact_skipped audit warning")
    if fail_errors:
        _print("FAILURE CONTRACT CHECK FAILED", fail_errors)
        return 1
    _print("FAILURE CONTRACT CHECK", "passed")

    _print("Phase 4", "cne run retry — restore network")
    retry = engine.run_job("retry", retry_failed_only=True, run_id=fail_id, trade_date=TRADE_DATE)
    final = Manifest(cfg.manifest_path).run_summary(fail_id)
    _print("retry result", retry)
    _print("final status", final)

    retry_errors: list[str] = []
    if retry.get("status") != "success":
        retry_errors.append(f"retry status={retry.get('status')!r}")
    if final.get("batch_counts", {}).get("failed", 0):
        retry_errors.append("batches still failed after retry")
    if load("daily_bars", start=TRADE_DATE, end=TRADE_DATE, config=cfg).is_empty():
        retry_errors.append(f"daily_bars missing for {TRADE_DATE!r} after retry")
    if retry_errors:
        _print("RETRY CHECK FAILED", retry_errors)
        return 1

    _print("ALL CHECKS", "passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
