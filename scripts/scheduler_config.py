#!/usr/bin/env python3
"""Render and compare launchd jobs, preserving explicit host scheduling choices."""

from __future__ import annotations

import argparse
import os
import plistlib
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_GROUPS = "core capital signals fundamentals macro_risk research"


def read_plist(path: Path) -> dict:
    return plistlib.loads(path.read_bytes()) if path.exists() else {}


def local_time(value: str) -> dict[str, int]:
    if not re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", value):
        raise argparse.ArgumentTypeError("expected local time HH:MM (00:00–23:59)")
    hour, minute = map(int, value.split(":"))
    return {"Hour": hour, "Minute": minute}


def _launchctl_command() -> list[str]:
    """Argv used to (un)load agents.

    ``CNE_LAUNCHCTL`` is normally a single binary (``launchctl``). A path
    ending in ``.py`` is run with this interpreter so a test can stub load
    and unload on Windows, where ``/usr/bin/true`` is not a file CreateProcess
    can execute.
    """
    raw = os.environ.get("CNE_LAUNCHCTL", "launchctl")
    if raw.lower().endswith(".py"):
        return [sys.executable, raw]
    return [raw]


def render_jobs(root: Path, dest: Path, *, groups: str | None, vantage: str | None) -> dict:
    daily = read_plist(dest / "com.cnequity.daily.plist")
    host_env = daily.get("EnvironmentVariables", {})
    groups = groups if groups is not None else host_env.get("CNE_GROUPS", DEFAULT_GROUPS)
    groups = " ".join(dict.fromkeys(groups.replace(",", " ").split()))
    if not groups or any(not re.fullmatch(r"[A-Za-z0-9_-]+", g) for g in groups.split()):
        raise ValueError("CNE_GROUPS must contain valid, non-empty schedule group names")
    vantage = vantage if vantage is not None else host_env.get("CNE_SOURCE_VANTAGE", "local")
    if not re.fullmatch(r"[A-Za-z0-9._-]+", vantage):
        raise ValueError("CNE_SOURCE_VANTAGE must match [A-Za-z0-9._-]+")
    jobs = {}
    # `events-news` is the high-frequency half of the events job. It was a
    # hand-written agent for a while, which is how it ended up as the only one
    # without a descriptor limit — the one job whose compact actually ran out.
    for name in ("daily", "stale", "events", "events-news"):
        label = f"com.cnequity.{name}"
        template = root / "scripts/launchd" / f"{label}.plist.template"
        # Decode before substitution: a checkout path containing & or < is XML text.
        job = plistlib.loads(template.read_bytes())

        def substitute(value):
            if isinstance(value, str):
                return (
                    value.replace("__REPO_ROOT__", str(root))
                    .replace("__SOURCE_VANTAGE__", vantage)
                    .replace("__GROUPS__", groups)
                )
            if isinstance(value, dict):
                return {k: substitute(v) for k, v in value.items()}
            if isinstance(value, list):
                return [substitute(v) for v in value]
            return value

        job = substitute(job)
        old = read_plist(dest / f"{label}.plist")
        # Existing event cadence and host-specific config/proxy overrides are
        # operator choices. Regeneration must not silently erase them.
        for key in ("StartCalendarInterval", "StartInterval"):
            if key in old:
                job.pop("StartCalendarInterval", None)
                job.pop("StartInterval", None)
                job[key] = old[key]
        env = job.setdefault("EnvironmentVariables", {})
        if name == "stale":
            for key in (
                "CNE_CONFIG",
                "CNE_BIN",
                "CNE_LOG_DIR",
                "CNE_DATA_ROOT",
                "CNE_SCHEDULER_LOCK_DIR",
            ):
                if key in host_env:
                    env[key] = host_env[key]
        env.update(old.get("EnvironmentVariables", {}))
        env["CNE_SOURCE_VANTAGE"] = vantage
        if name in {"daily", "stale"}:
            env["CNE_GROUPS"] = groups
        if name == "events-news":
            # The host copy may carry CNE_CONFIG and a retuned interval; both
            # survive above. The group is what makes this agent this agent.
            env.setdefault("CNE_EVENTS_GROUP", "news_wire")
        if name == "daily":
            env["CNE_STALE_RETRY"] = "0"
        jobs[label] = job
    return jobs


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="Report drift without changing anything")
    mode.add_argument("--dry-run", type=Path, metavar="DIRECTORY", help="Render for review only")
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--daily-only", action="store_true", help="Update only the daily agent")
    selection.add_argument(
        "--stale-only", action="store_true", help="Update only the catch-up agent"
    )
    parser.add_argument(
        "--stale-at", type=local_time, metavar="HH:MM", help="Host local catch-up time"
    )
    args = parser.parse_args()
    if args.daily_only and args.stale_at is not None:
        parser.error("--stale-at cannot be combined with --daily-only")
    dest = Path(os.environ.get("CNE_SCHEDULER_DEST_DIR", Path.home() / "Library/LaunchAgents"))
    try:
        jobs = render_jobs(
            ROOT,
            dest,
            groups=os.environ.get("CNE_GROUPS"),
            vantage=os.environ.get("CNE_SOURCE_VANTAGE"),
        )
        if args.stale_at is not None:
            jobs["com.cnequity.stale"].pop("StartInterval", None)
            jobs["com.cnequity.stale"]["StartCalendarInterval"] = args.stale_at
        if args.daily_only:
            jobs = {"com.cnequity.daily": jobs["com.cnequity.daily"]}
        elif args.stale_only:
            jobs = {"com.cnequity.stale": jobs["com.cnequity.stale"]}
        drift = [label for label, job in jobs.items() if read_plist(dest / f"{label}.plist") != job]
        if args.check:
            for label in drift:
                print(f"DRIFT {label}")
            if not drift:
                print("scheduler: installed jobs match templates and host settings")
            return int(bool(drift))
        output = args.dry_run if args.dry_run is not None else dest
        if args.dry_run is not None and output.resolve() == dest.resolve():
            raise ValueError(
                "--dry-run directory must differ from the installed LaunchAgents directory"
            )
        output.mkdir(parents=True, exist_ok=True)
        if args.dry_run is None:
            (ROOT / "data/cnequity/logs").mkdir(parents=True, exist_ok=True)
        for label, job in jobs.items():
            path = output / f"{label}.plist"
            if args.dry_run is None and label not in drift:
                continue
            if path.exists():
                path.with_suffix(".plist.bak").write_bytes(path.read_bytes())
            with tempfile.NamedTemporaryFile(dir=output, delete=False) as handle:
                temporary = Path(handle.name)
                handle.write(plistlib.dumps(job, sort_keys=False))
            try:
                temporary.replace(path)
            finally:
                temporary.unlink(missing_ok=True)
            if args.dry_run is None:
                launchctl = _launchctl_command()
                subprocess.run([*launchctl, "unload", str(path)], capture_output=True, check=False)
                subprocess.run([*launchctl, "load", str(path)], check=True)
            print(f"{'Rendered' if args.dry_run is not None else 'Loaded'} {path}")
        env = jobs[next(iter(jobs))]["EnvironmentVariables"]
        print(f"scheduler: groups={env['CNE_GROUPS']}; vantage={env['CNE_SOURCE_VANTAGE']}")
        return 0
    except (
        OSError,
        ValueError,
        plistlib.InvalidFileException,
        subprocess.CalledProcessError,
    ) as exc:
        print(f"install_scheduler: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
