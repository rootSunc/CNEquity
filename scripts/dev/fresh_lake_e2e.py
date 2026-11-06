#!/usr/bin/env python3
"""Offline acceptance drill for a clean lake and its first incremental update.

The drill deliberately uses a temporary root and deterministic two-exchange
fixtures.  It never opens a real source adapter, and a socket guard turns an
accidental network call into an immediate failure.  The wheel smoke runs in a
throw-away virtual environment and asserts that the CLI imports the installed
wheel rather than the checkout.

Run from a checkout with the development environment active::

    .venv/bin/python scripts/dev/fresh_lake_e2e.py

The wheel smoke can be skipped when ``uv``/a build backend is unavailable::

    .venv/bin/python scripts/dev/fresh_lake_e2e.py --skip-wheel-smoke

This is intentionally a script rather than a pytest fixture: it is a release
operator's one-command recovery drill and leaves no state below ``data/``.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import json
import os
import shutil
import site
import socket
import sqlite3
import subprocess
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import polars as pl

from cnequity.config import Config, load_config, validate_config
from cnequity.config.bootstrap import path_for_toml
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.manifest import Manifest
from cnequity.orchestrator.registry import STEP_REGISTRY, StepEntry
from cnequity.quality.dataset_checks import audit_curated_dataset
from cnequity.query.reader import load
from cnequity.steps.finalize import step_compact
from cnequity.storage.layout import init_data_layout
from cnequity.storage.parquet import StagingWriter
from cnequity.storage.revisions import RevisionStore
from cnequity.storage.snapshots import SnapshotStore
from cnequity.storage.state import StateStore

DAY0 = dt.date(2024, 6, 26)
DAY1 = dt.date(2024, 6, 27)
DAY2 = dt.date(2024, 6, 28)
SYMBOLS = ("600000.SH", "000001.SZ")


def _fail(message: str) -> None:
    raise AssertionError(message)


def _assert(condition: bool, message: str) -> None:
    if not condition:
        _fail(message)


@contextlib.contextmanager
def network_guard() -> Iterator[None]:
    """Make the fixture run fail if any source attempts an outbound socket."""

    # Keep ``socket.socket`` as a class.  Replacing the class itself breaks
    # modules that define a small socket subclass at import time (the TDX wire
    # client does exactly that); patch only the outbound operations instead.
    original_connect = socket.socket.connect
    original_connect_ex = socket.socket.connect_ex
    original_create_connection = socket.create_connection

    def blocked(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise AssertionError("fresh_lake_e2e attempted a network request")

    socket.socket.connect = blocked  # type: ignore[assignment]
    socket.socket.connect_ex = blocked  # type: ignore[assignment]
    socket.create_connection = blocked  # type: ignore[assignment]
    try:
        yield
    finally:
        socket.socket.connect = original_connect  # type: ignore[assignment]
        socket.socket.connect_ex = original_connect_ex  # type: ignore[assignment]
        socket.create_connection = original_create_connection  # type: ignore[assignment]


def _bar_frame(day: dt.date, closes: tuple[float, float]) -> pl.DataFrame:
    """Return a schema-valid, two-exchange daily-bars fixture."""

    return pl.DataFrame(
        {
            "symbol": list(SYMBOLS),
            "trade_date": [day, day],
            "open": [value - 0.5 for value in closes],
            "high": [value + 0.5 for value in closes],
            "low": [value - 1.0 for value in closes],
            "close": list(closes),
            "volume": [100_000, 200_000],
            "amount": [
                value * volume for value, volume in zip(closes, (100_000, 200_000), strict=True)
            ],
            "source": ["fixture", "fixture"],
            "data_version": ["v2", "v2"],
            "fetched_at": [
                dt.datetime.combine(day, dt.time(16), tzinfo=dt.timezone.utc),
                dt.datetime.combine(day, dt.time(16), tzinfo=dt.timezone.utc),
            ],
        }
    )


def _config(root: Path) -> Config:
    cfg = Config(data_root=root, workers=1, tdx_enabled=False, batch_size=2)
    init_data_layout(cfg)
    return cfg


def _write_old_layout(root: Path, day: dt.date) -> None:
    """Create a pre-revision layout with the pre-migration state shape."""

    path = root / "curated" / "daily_bars" / f"trade_date={day.isoformat()}" / "part.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    _bar_frame(day, (10.0, 20.0)).write_parquet(path)
    state = root / "meta" / "state"
    state.mkdir(parents=True, exist_ok=True)
    (state / "daily_bars.json").write_text(
        json.dumps({"last_success_trade_date": day.isoformat()}), encoding="utf-8"
    )


def _legacy_manifest(path: Path) -> None:
    """Write the old two-table manifest schema and let Manifest migrate it."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE ingestion_runs (
                run_id TEXT PRIMARY KEY,
                job_name TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                rows_read INTEGER DEFAULT 0,
                rows_written INTEGER DEFAULT 0,
                error_message TEXT,
                metadata_json TEXT DEFAULT '{}'
            );
            CREATE TABLE ingestion_batches (
                run_id TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                dataset TEXT NOT NULL,
                status TEXT NOT NULL,
                symbols_json TEXT DEFAULT '[]',
                window_start TEXT,
                window_end TEXT,
                rows_read INTEGER DEFAULT 0,
                rows_written INTEGER DEFAULT 0,
                retry_count INTEGER DEFAULT 0,
                started_at TEXT,
                finished_at TEXT,
                error_message TEXT,
                PRIMARY KEY (run_id, batch_id)
            );
            """
        )


def _run_wheel_smoke(work: Path) -> dict[str, Any]:
    """Build and execute the CLI from a wheel in an isolated child venv.

    Runtime dependencies are borrowed through ``PYTHONPATH`` from the active
    development environment.  The package itself is installed with
    ``--no-deps`` into the child venv; the child asserts its module path is
    inside that venv, so an editable ``src`` import cannot satisfy the check.
    This keeps the release gate deterministic and offline.
    """

    wheel_dir = work / "wheel"
    wheel_dir.mkdir()
    build_cmd: list[str]
    if shutil.which("uv"):
        build_cmd = ["uv", "build", "--wheel", "--out-dir", str(wheel_dir)]
    else:
        build_cmd = [sys.executable, "-m", "build", "--wheel", "--outdir", str(wheel_dir)]
    subprocess.run(build_cmd, cwd=ROOT, check=True, capture_output=True)
    wheels = sorted(wheel_dir.glob("cnequity-*.whl"))
    _assert(len(wheels) == 1, f"expected one current wheel, found {wheels}")

    venv = work / "wheel-venv"
    if shutil.which("uv"):
        subprocess.run(["uv", "venv", str(venv), "--python", sys.executable], check=True)
        uv_install = ["uv", "pip", "install", "--python", str(_venv_python(venv)), "--no-deps"]
        subprocess.run([*uv_install, str(wheels[0])], check=True)
    else:
        subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
        subprocess.run(
            [str(_venv_python(venv)), "-m", "pip", "install", "--no-deps", str(wheels[0])],
            check=True,
        )

    child_python = _venv_python(venv)
    dependency_paths = [str(Path(item)) for item in site.getsitepackages() if Path(item).is_dir()]
    child_env = os.environ.copy()
    inherited = (
        child_env.get("PYTHONPATH", "").split(os.pathsep) if child_env.get("PYTHONPATH") else []
    )
    forbidden = {str(ROOT), str(SRC)}
    dependency_paths.extend(
        item
        for item in inherited
        if item and not any(Path(item).resolve() == Path(x).resolve() for x in forbidden)
    )
    child_env["PYTHONPATH"] = os.pathsep.join(dict.fromkeys(dependency_paths))
    child_env.pop("VIRTUAL_ENV", None)

    probe = subprocess.run(
        [
            str(child_python),
            "-c",
            "import cnequity; from pathlib import Path; print(Path(cnequity.__file__).resolve())",
        ],
        cwd=work,
        env=child_env,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    module_path = Path(probe.stdout.strip())
    _assert(module_path.is_relative_to(venv.resolve()), f"wheel smoke imported {module_path}")

    config_path = work / "wheel-config.toml"
    lake = work / "wheel-lake"
    cne = _venv_script(venv, "cne")
    for command in (
        ["config", "create", "--config", str(config_path), "--data-root", str(lake)],
        ["config", "validate", "--config", str(config_path)],
        ["init", "--config", str(config_path), "--layout-only"],
    ):
        completed = subprocess.run(
            [str(cne), *command],
            cwd=work,
            env=child_env,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
        )
        _assert(completed.stdout.strip(), f"wheel CLI produced no output: {command}")
    _assert((lake / "meta" / "manifest.db").is_file(), "wheel layout-only did not create manifest")
    _assert((lake / "curated").is_dir(), "wheel layout-only did not create curated root")
    return {"wheel": wheels[0].name, "module": str(module_path), "lake": str(lake)}


def _venv_python(venv: Path) -> Path:
    return venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def _venv_script(venv: Path, name: str) -> Path:
    return venv / (f"Scripts/{name}.exe" if os.name == "nt" else f"bin/{name}")


def _fixture_pipeline(root: Path) -> dict[str, Any]:
    """Exercise fail-closed staging, retry, compact, revision, query and audit."""

    cfg = _config(root)
    state = StateStore(cfg.meta_root)
    state.set_date("daily_bars", DAY0)
    attempts = {"count": 0}

    def fixture_step(
        config: Config, trade_date: dt.date, run_id: str, context: dict[str, Any]
    ) -> dict[str, Any]:
        _assert(trade_date == DAY1, f"fixture trade date changed unexpectedly: {trade_date}")
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("fixture source outage")
        batch_id = str(context["_batch_id"])
        frame = _bar_frame(DAY1, (10.5, 20.5))
        StagingWriter(config.staging_root).write_batch("daily_bars", run_id, batch_id, frame)
        return {
            "dataset": "daily_bars",
            "rows_read": frame.height,
            "rows_written": frame.height,
        }

    # Keep the registered task name ``daily_bars``.  The production retry
    # ledger uses the task id to upsert/supersede dataset receipts; using a
    # differently named fixture would correctly expose a failed receipt for
    # the synthetic task, but would not model a real daily-bars retry.
    original = STEP_REGISTRY["daily_bars"]
    STEP_REGISTRY["daily_bars"] = StepEntry(
        fn=fixture_step,
        group="core",
        description="deterministic offline daily-bars fixture",
        parallelizable=False,
        requires_workers=False,
    )
    try:
        engine = JobEngine(cfg)
        failed = engine.run_job("fixture", DAY1, steps=["daily_bars"])
        run_id = str(failed["run_id"])
        _assert(failed["status"] == "failed", f"expected fixture failure: {failed}")
        _assert(state.get_date("daily_bars") == DAY0, "failed fetch advanced daily watermark")
        failed_batches = Manifest(cfg.manifest_path).get_failed_batches(run_id)
        _assert(failed_batches, "failed fixture did not leave a retryable batch")

        # ``run_job(..., retry_failed_only=True)`` also auto-runs the optional
        # derive/audit chain.  This fixture intentionally isolates the core
        # fetch/retry contract; use the same durable retry implementation with
        # finalization disabled, then invoke the real compact and audit below.
        retried = engine._retry_run(run_id, DAY1, auto_finalize=False)
        _assert(retried["status"] == "success", f"fixture retry failed: {retried}")
        _assert(state.get_date("daily_bars") == DAY0, "retry advanced watermark before compact")

        compact = engine.run_job(
            "fixture", DAY1, steps=["compact"], run_id=run_id, finalize_run=False
        )
        _assert(
            all(item.get("status") == "success" for item in compact["results"]),
            f"fixture compact failed: {compact}",
        )
        _assert(
            state.get_date("daily_bars") == DAY1, "successful compact did not advance watermark"
        )
        pointer = RevisionStore(cfg.meta_root, cfg.curated_root).current_pointer("daily_bars")
        _assert(
            pointer is not None and pointer["revision"] == 1,
            f"unexpected baseline pointer: {pointer}",
        )

        queried = load("daily_bars", config=cfg, start=DAY1, end=DAY1)
        _assert(queried.height == len(SYMBOLS), f"query returned {queried.height} fixture rows")
        findings = audit_curated_dataset(
            "daily_bars", "trade_date", cfg.curated_root / "daily_bars", DAY1, full=True
        )
        errors = [item for item in findings if item.get("severity") == "error"]
        _assert(not errors, f"baseline audit errors: {errors}")
        return {"config": cfg, "run_id": run_id, "revision": pointer, "query_rows": queried.height}
    finally:
        STEP_REGISTRY["daily_bars"] = original


def _next_day_update(root: Path, packages: Path, baseline: Path) -> dict[str, Any]:
    """Create a revision-2 next-day lake and return its snapshot identity."""

    cfg = _config(root)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("fixture-next-day", {"trade_date": DAY2.isoformat()})
    batch_id = "fixture-next-day-batch"
    frame = _bar_frame(DAY2, (11.0, 21.0))
    manifest.start_batch(
        run_id,
        batch_id,
        task_id="daily_bars",
        dataset="daily_bars",
        symbols=list(SYMBOLS),
        window_start=DAY2.isoformat(),
        window_end=DAY2.isoformat(),
    )
    StagingWriter(cfg.staging_root).write_batch("daily_bars", run_id, batch_id, frame)
    manifest.finish_batch(
        run_id, batch_id, "success", rows_read=frame.height, rows_written=frame.height
    )
    compact = step_compact(cfg, DAY2, run_id, {})
    # DAY1 and DAY2 share 2024-06. Compact rewrites that month file, so the
    # written rows are both sessions, not only the new day.
    _assert(
        compact.get("rows_written") == len(SYMBOLS) * 2,
        f"next-day compact failed: {compact}",
    )
    manifest.finish_run(run_id, "success", rows_written=frame.height)
    pointer = RevisionStore(cfg.meta_root, cfg.curated_root).current_pointer("daily_bars")
    _assert(pointer is not None and pointer["revision"] == 2, f"unexpected next pointer: {pointer}")
    packages_store = SnapshotStore(cfg, packages)
    packages_store.create_delta(
        "after-baseline", baseline=baseline, datasets=["daily_bars"], target=root
    )
    return {"config": cfg, "run_id": run_id, "pointer": pointer}


def _snapshot_and_delta(source_cfg: Config, work: Path) -> dict[str, Any]:
    """Round-trip a baseline, apply next-day delta, and test rollback/idempotency."""

    packages = work / "packages"
    store = SnapshotStore(source_cfg, packages)
    store.create("baseline", ["daily_bars"])
    archive = store.export_archive("baseline", work / "baseline.tar.gz", compression="gzip")

    damaged = work / "damaged.tar.gz"
    damaged_bytes = bytearray(archive.read_bytes())
    # A truncated gzip stream is accepted by a few Python/zlib combinations
    # when the tar reader has already consumed all members.  Flipping a byte
    # in the compressed payload exercises the checksum path consistently.
    corrupt_at = max(10, len(damaged_bytes) // 2)
    damaged_bytes[corrupt_at] ^= 0xFF
    damaged.write_bytes(damaged_bytes)
    try:
        store.import_archive(damaged, name="damaged")
    except Exception as exc:
        # ``tarfile.ReadError`` differs across Python versions; the exact
        # exception is intentionally not part of the compatibility contract.
        del exc
    else:
        _fail("truncated snapshot archive was accepted")
    _assert(not store.path("damaged").exists(), "corrupt archive published a snapshot")

    store.import_archive(archive, name="baseline-imported")
    _assert(store.verify("baseline-imported").passed, "imported baseline failed verification")
    restored = store.restore("baseline-imported", work / "restored")
    restored_cfg = _config(restored)
    base_state = StateStore(restored_cfg.meta_root).get_payload("daily_bars")
    _assert(
        load("daily_bars", config=restored_cfg, start=DAY1, end=DAY1).height == len(SYMBOLS),
        "restored query failed",
    )

    updated = work / "updated"
    shutil.copytree(restored, updated)
    update = _next_day_update(updated, packages, restored)
    update_cfg = update["config"]
    delta = SnapshotStore(update_cfg, packages)
    verification = delta.verify_delta("after-baseline")
    _assert(verification.passed, f"delta verification failed: {verification}")

    # A publish failure after file replacement must restore every overwritten
    # file and leave the baseline fingerprint/state untouched.
    rollback = work / "rollback"
    shutil.copytree(restored, rollback)
    before_rollback = delta._lake_index(rollback, ["daily_bars"])
    import cnequity.storage.snapshots as snapshot_module

    original_write_json = snapshot_module.write_json_atomic

    def fail_receipt(path: Path, payload: Any, **kwargs: Any) -> Any:
        if Path(path).parent.name == "applied-deltas":
            raise OSError("injected delta publish failure")
        return original_write_json(path, payload, **kwargs)

    snapshot_module.write_json_atomic = fail_receipt  # type: ignore[assignment]
    try:
        try:
            delta.apply_delta("after-baseline", rollback)
        except OSError as exc:
            _assert("injected" in str(exc), f"unexpected rollback error: {exc}")
        else:
            _fail("injected delta publish failure did not fail")
    finally:
        snapshot_module.write_json_atomic = original_write_json  # type: ignore[assignment]
    _assert(
        delta._lake_index(rollback, ["daily_bars"]) == before_rollback,
        "delta rollback changed target",
    )

    delta.apply_delta("after-baseline", restored)
    after_first = delta._lake_index(restored, ["daily_bars"])
    delta.apply_delta("after-baseline", restored)
    _assert(
        delta._lake_index(restored, ["daily_bars"]) == after_first, "delta retry was not idempotent"
    )
    _assert(
        after_first == delta._lake_index(updated, ["daily_bars"]),
        "applied delta hash/index differs from source",
    )
    _assert(
        base_state != StateStore(restored_cfg.meta_root).get_payload("daily_bars"),
        "delta did not update state",
    )
    _assert(
        load("daily_bars", config=restored_cfg, start=DAY2, end=DAY2).height == len(SYMBOLS),
        "next-day query failed",
    )
    old_revision = load("daily_bars", config=restored_cfg, start=DAY1, end=DAY1, revision=1)
    _assert(
        old_revision.sort("symbol")["close"].to_list() == [20.5, 10.5],
        "revision-pinned query changed after update",
    )
    current_revision = load("daily_bars", config=restored_cfg, start=DAY2, end=DAY2, revision=2)
    _assert(
        current_revision.sort("symbol")["close"].to_list() == [21.0, 11.0],
        "current revision query is wrong",
    )
    findings = audit_curated_dataset(
        "daily_bars", "trade_date", restored_cfg.curated_root / "daily_bars", DAY2, full=True
    )
    errors = [item for item in findings if item.get("severity") == "error"]
    _assert(not errors, f"post-delta audit errors: {errors}")
    return {
        "archive": str(archive),
        "restored": str(restored),
        "updated": str(updated),
        "delta_verified_files": verification.verified_files,
        "rows_after_delta": load("daily_bars", config=restored_cfg).height,
    }


def _compatibility_drill(work: Path) -> dict[str, Any]:
    """Prove old manifest, old layout and old config remain readable."""

    legacy = work / "legacy"
    _write_old_layout(legacy, DAY1)
    _legacy_manifest(legacy / "meta" / "manifest.db")
    cfg_path = work / "legacy.toml"
    cfg_path.write_text(
        "[data]\n"
        f'root = "{path_for_toml(legacy)}"\n'
        "[orchestrator]\nworkers = 1\n"
        '[[job.daily.waves]]\nname = "legacy"\nsteps = ["instruments"]\n',
        encoding="utf-8",
    )
    cfg = load_config(cfg_path)
    _assert(validate_config(cfg) == [], "old config without new keys no longer validates")
    migrated_manifest = Manifest(cfg.manifest_path)
    run_id = migrated_manifest.start_run("legacy-read")
    _assert(run_id, "old manifest could not be opened/migrated")
    frame = load("daily_bars", config=cfg, start=DAY1, end=DAY1)
    _assert(frame.height == len(SYMBOLS), "old layout could not be queried")
    _assert(
        not (legacy / "meta" / "revisions" / "daily_bars" / "current.json").exists(),
        "legacy read unexpectedly required a revision pointer",
    )
    return {"manifest": str(cfg.manifest_path), "rows": frame.height}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--work-root", type=Path, help="Keep temporary drill state under this directory."
    )
    parser.add_argument(
        "--skip-wheel-smoke", action="store_true", help="Skip build/install/CLI wheel gate."
    )
    args = parser.parse_args()

    owned_temp = args.work_root is None
    context: contextlib.AbstractContextManager[Any]
    if owned_temp:
        context = tempfile.TemporaryDirectory(prefix="cnequity-fresh-lake-")
    else:
        args.work_root.mkdir(parents=True, exist_ok=True)
        context = contextlib.nullcontext(str(args.work_root))

    with context as raw_root:
        work = Path(raw_root)
        results: dict[str, Any] = {}
        if args.skip_wheel_smoke:
            results["wheel_smoke"] = "skipped"
        else:
            results["wheel_smoke"] = _run_wheel_smoke(work)
        with network_guard():
            baseline = _fixture_pipeline(work / "baseline")
            results["fixture_pipeline"] = {
                key: value for key, value in baseline.items() if key not in {"config"}
            }
            results["snapshot_delta"] = _snapshot_and_delta(baseline["config"], work)
            results["compatibility"] = _compatibility_drill(work)
        print(json.dumps({"status": "passed", **results}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
