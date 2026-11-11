#!/usr/bin/env python3
"""Catch the core gate up after a missed session, then optionally the rest.

This is composition, not a new capability: everything below is `cne run daily`
against one schedule group at a time, plus the rules for which groups to run and
which failures are allowed to be advisory. It belongs in a script rather
than in the published CLI — a composition is opinionated, changes with how one
lake happens to be operated, and should not be something users depend on the
exact behaviour of.

Runs ``daily:core`` for the target date, then ``market_breadth`` + ``compact``
(unless ``--core-only``). It never passes ``--backfill``: a full corporate-action
scan is fragile on an overseas egress and this path exists to close a gap, not to
re-derive history. ``--extra-group`` / ``--all-groups`` continue past EastMoney
failures so a mainland box can refresh capital/research in one shot.

Exit codes match the gate, not the extras: 1 if core or market_breadth failed,
0 otherwise. An extra group that fails is reported and does not change the exit
code — that is the whole point of it being extra.

Usage::

    python scripts/run_catchup.py                              # latest trading day
    python scripts/run_catchup.py --trade-date 2026-07-17
    python scripts/run_catchup.py --core-only
    python scripts/run_catchup.py --all-groups
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import cnequity.steps  # noqa: F401 — register steps
from cnequity.config import WaveConfig, load_config
from cnequity.domain.market_time import shanghai_today
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.run_lock import RunLockError

DEFAULT_CONFIG = "configs/cnequity.toml"

EXTRA_GROUPS_DEFAULT = (
    "capital",
    "signals",
    "fundamentals",
    "macro_risk",
    "research",
)

OK_STATUSES = ("success", "skipped_non_trading_day", "skipped_already_fresh")


class CatchupError(RuntimeError):
    """A precondition the run cannot proceed past."""


def dataset_watermark(cfg, dataset: str):
    """Latest success date for a gate dataset (StateStore or hive max for adj)."""
    from cnequity.query.parquet_scan import list_hive_partition_dates
    from cnequity.storage.state import StateStore

    state = StateStore(cfg.meta_root)
    wm = state.get_date(dataset)
    if wm is not None:
        return wm
    if dataset == "adj_factors":
        parts = list_hive_partition_dates(cfg.derived_root / "adj_factors", "trade_date")
        return parts[-1] if parts else None
    return None


def gate_fresh(cfg, trade_date: date, *, core_only: bool) -> dict[str, bool]:
    """Which gate pieces are already at/above ``trade_date``."""

    def _ok(name: str) -> bool:
        wm = dataset_watermark(cfg, name)
        return wm is not None and wm >= trade_date

    bars_ok = _ok("daily_bars")
    adj_ok = _ok("adj_factors")
    breadth_ok = True if core_only else _ok("market_breadth")
    return {
        "daily_bars": bars_ok,
        "adj_factors": adj_ok,
        "market_breadth": breadth_ok,
        "core": bars_ok and adj_ok,
        "all": bars_ok and adj_ok and breadth_ok,
    }


def resolve_trade_date(cfg, trade_date_str: str | None) -> date:
    from cnequity.steps.common import is_trading_day, list_trading_dates

    if trade_date_str:
        td = date.fromisoformat(trade_date_str)
        if not is_trading_day(cfg, td):
            raise CatchupError(f"{td.isoformat()} is not a trading day")
        return td

    # Walk back up to ~3 weeks for long holidays.
    end = shanghai_today()
    start = date.fromordinal(end.toordinal() - 21)
    days = list_trading_dates(cfg, start, end)
    if not days:
        raise CatchupError("no trading day found in the last 21 calendar days")
    return days[-1]


def ordered_extras(extra_groups: list[str], all_groups: bool) -> list[str]:
    """Preserve order, drop duplicates and `core` (the gate already handled it)."""
    extras = list(EXTRA_GROUPS_DEFAULT) if all_groups else []
    extras.extend(extra_groups)

    seen: set[str] = set()
    ordered: list[str] = []
    for name in extras:
        if name == "core" or name in seen:
            continue
        seen.add(name)
        ordered.append(name)
    return ordered


def run_catchup(
    config_path: str,
    *,
    trade_date_str: str | None = None,
    core_only: bool = False,
    extra_groups: list[str] | None = None,
    all_groups: bool = False,
) -> int:
    cfg = load_config(Path(config_path))
    td = resolve_trade_date(cfg, trade_date_str)
    extras = ordered_extras(list(extra_groups or []), all_groups)
    fresh = gate_fresh(cfg, td, core_only=core_only)

    def _iso(value):
        return value.isoformat() if value else None

    print(
        json.dumps(
            {
                "trade_date": td.isoformat(),
                "daily_bars_watermark": _iso(dataset_watermark(cfg, "daily_bars")),
                "adj_factors_watermark": _iso(dataset_watermark(cfg, "adj_factors")),
                "market_breadth_watermark": _iso(dataset_watermark(cfg, "market_breadth")),
                "core_only": core_only,
                "extra_groups": extras,
                "already_fresh": fresh,
            },
            indent=2,
        )
    )

    engine = JobEngine(cfg)
    group = cfg.schedule_groups.get("core")
    if not group:
        raise CatchupError("schedule group 'core' missing from config")

    results: dict[str, dict[str, str]] = {}

    def _run(job: str, wave_name: str, steps: list[str]) -> dict:
        return engine.run_job(
            job,
            trade_date=td,
            waves=[WaveConfig(name=wave_name, parallel=False, steps=steps)],
            backfill=False,
        )

    try:
        if fresh["core"]:
            results["core"] = {"run_id": "", "status": "skipped_already_fresh"}
        else:
            core = _run("daily:core", "group:core", group.steps)
            results["core"] = {"run_id": core["run_id"], "status": core["status"]}
            if core["status"] not in OK_STATUSES:
                print(json.dumps(results, indent=2))
                return 1

        if not core_only:
            if fresh["market_breadth"]:
                results["market_breadth"] = {"run_id": "", "status": "skipped_already_fresh"}
            else:
                breadth = _run("daily:market_breadth", "breadth", ["market_breadth", "compact"])
                results["market_breadth"] = {
                    "run_id": breadth["run_id"],
                    "status": breadth["status"],
                }
                if breadth["status"] not in OK_STATUSES:
                    print(json.dumps(results, indent=2))
                    return 1

        for name in extras:
            g = cfg.schedule_groups.get(name)
            if not g:
                results[name] = {"run_id": "", "status": "unknown_group"}
                continue
            out = _run(f"daily:{name}", f"group:{name}", g.steps)
            results[name] = {"run_id": out["run_id"], "status": out["status"]}
    except RunLockError as exc:
        raise CatchupError(str(exc)) from exc

    print(json.dumps(results, indent=2))

    # Gate decides the exit code; extra-group failures are advisory.
    if results["core"]["status"] not in OK_STATUSES:
        return 1
    mb = results.get("market_breadth")
    if mb and mb["status"] not in OK_STATUSES:
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument(
        "--trade-date",
        default=None,
        help="Target trading day YYYY-MM-DD (default: latest trading day on/before today).",
    )
    parser.add_argument(
        "--core-only", action="store_true", help="Skip market_breadth (gate bars/adj only)."
    )
    parser.add_argument(
        "--extra-group",
        action="append",
        default=[],
        help=(
            "Also run this schedule group after the gate catchup (repeatable). "
            "Best-effort: failures are reported but do not change the exit code."
        ),
    )
    parser.add_argument(
        "--all-groups",
        action="store_true",
        help=f"After gate catchup, best-effort run: {' '.join(EXTRA_GROUPS_DEFAULT)}.",
    )
    parser.add_argument("--quiet", action="store_true", help="Warnings and errors only.")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    for noisy in ("httpx", "httpcore", "urllib3", "curl_cffi"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    try:
        return run_catchup(
            args.config,
            trade_date_str=args.trade_date,
            core_only=args.core_only,
            extra_groups=args.extra_group,
            all_groups=args.all_groups,
        )
    except CatchupError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
