from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


# ``ingestion_batches`` is the retry ledger.  ``dataset_results`` is the
# smaller, user-facing receipt ledger: one row per logical dataset and stage.
# Keep these values here (rather than sprinkling string literals through the
# engine) so an older manifest can be migrated and callers can validate input
# consistently.
DATASET_RESULT_STAGES = frozenset(
    {"fetch", "stage", "compact", "derive", "audit", "publish_revision"}
)
DATASET_RESULT_STATUSES = frozenset(
    {"success", "warning", "failed", "skipped", "blocked", "degraded"}
)
DATASET_RESULT_CRITICALITIES = frozenset({"core", "research", "advisory"})


@dataclass
class RunRecord:
    run_id: str
    job_name: str
    status: str
    started_at: str
    finished_at: str | None = None
    rows_read: int = 0
    rows_written: int = 0
    error_message: str | None = None
    metadata_json: str = "{}"


@dataclass
class BatchRecord:
    run_id: str
    batch_id: str
    task_id: str
    dataset: str
    status: str
    symbols_json: str = "[]"
    window_start: str | None = None
    window_end: str | None = None
    rows_read: int = 0
    rows_written: int = 0
    retry_count: int = 0
    # ``retry_count`` is orchestrator-owned: a durable retry budget for worker
    # batches and a one-attempt lineage marker for non-worker retry batches.
    # Request retries are adapter-local observations and must not be mixed into
    # it, or a transient HTTP retry could consume a worker retry slot.
    request_retry_count: int = 0
    started_at: str | None = None
    finished_at: str | None = None
    error_message: str | None = None
    blocks_compaction: bool = True


@dataclass
class DatasetResult:
    """One logical dataset/stage receipt in the run manifest.

    This intentionally mirrors the public ``dataset_results`` table rather
    than the more detailed batch ledger.  A row is upserted by
    ``(run_id, dataset, stage)`` as a stage moves from an attempt to its final
    outcome, which keeps status queries deterministic after retries.
    """

    run_id: str
    dataset: str
    stage: str
    status: str
    criticality: str = "core"
    revision_id: str | None = None
    rows_written: int = 0
    error_code: str | None = None
    error_message: str | None = None


class _ClosingConnection(sqlite3.Connection):
    """SQLite context manager that also closes the connection on exit.

    ``sqlite3.Connection.__exit__`` commits or rolls back, but deliberately does
    not close the connection. Manifest methods open short-lived connections, so
    leaving them for garbage collection leaks file descriptors and produces
    ``ResourceWarning`` on Python 3.13 during a full pipeline run.
    """

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


class Manifest:
    def __init__(self, db_path: Path | str):
        # Worker-pool children pass a str path across process boundaries.
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), factory=_ClosingConnection)
        conn.row_factory = sqlite3.Row
        # Concurrent writers (worker processes + engine) need WAL and a
        # bounded wait instead of immediate "database is locked" errors.
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS ingestion_runs (
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
                CREATE TABLE IF NOT EXISTS ingestion_batches (
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
                    request_retry_count INTEGER DEFAULT 0,
                    started_at TEXT,
                    finished_at TEXT,
                    error_message TEXT,
                    heartbeat_at TEXT,
                    blocks_compaction INTEGER DEFAULT 1,
                    PRIMARY KEY (run_id, batch_id)
                );
                CREATE INDEX IF NOT EXISTS idx_batches_run_status
                    ON ingestion_batches(run_id, status);
                CREATE INDEX IF NOT EXISTS idx_batches_dataset
                    ON ingestion_batches(dataset, status);
                CREATE TABLE IF NOT EXISTS dataset_results (
                    run_id TEXT NOT NULL,
                    dataset TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    status TEXT NOT NULL,
                    criticality TEXT NOT NULL DEFAULT 'core',
                    revision_id TEXT,
                    rows_written INTEGER DEFAULT 0,
                    error_code TEXT,
                    error_message TEXT,
                    PRIMARY KEY (run_id, dataset, stage)
                );
                """
            )
            cols = {row[1] for row in conn.execute("PRAGMA table_info(ingestion_batches)")}
            if "heartbeat_at" not in cols:
                conn.execute("ALTER TABLE ingestion_batches ADD COLUMN heartbeat_at TEXT")
            if "blocks_compaction" not in cols:
                conn.execute(
                    "ALTER TABLE ingestion_batches ADD COLUMN blocks_compaction INTEGER DEFAULT 1"
                )
            if "request_retry_count" not in cols:
                # Additive migration for manifests created before request
                # retry telemetry was separated from the orchestrator retry
                # budget.  Existing rows remain explicitly unknown/zero; no
                # historical request retries are inferred from logs.
                conn.execute(
                    "ALTER TABLE ingestion_batches ADD COLUMN request_retry_count INTEGER DEFAULT 0"
                )

            # ``dataset_results`` was added after the original two-table
            # manifest.  Keep the migration deliberately additive: operators
            # may have copied a manifest between releases, and SQLite does
            # not support ``ADD COLUMN IF NOT EXISTS`` on all versions we
            # support.  The defaults make old rows readable without inventing
            # a result for a stage that never ran.
            result_cols = {row[1] for row in conn.execute("PRAGMA table_info(dataset_results)")}
            for name, definition in (
                ("run_id", "TEXT"),
                ("dataset", "TEXT"),
                ("stage", "TEXT"),
                ("status", "TEXT"),
                ("criticality", "TEXT NOT NULL DEFAULT 'core'"),
                ("revision_id", "TEXT"),
                ("rows_written", "INTEGER DEFAULT 0"),
                ("error_code", "TEXT"),
                ("error_message", "TEXT"),
            ):
                if name not in result_cols:
                    conn.execute(f"ALTER TABLE dataset_results ADD COLUMN {name} {definition}")

            # A hand-created pre-release table may not have carried the
            # composite primary key.  The write path below remains safe for
            # that shape, while this index gives normal manifests the same
            # uniqueness guarantee as the declared primary key.
            try:
                conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ux_dataset_results_key "
                    "ON dataset_results(run_id, dataset, stage)"
                )
            except sqlite3.IntegrityError:
                # A hand-created pre-release table may contain duplicate
                # receipts.  Do not make opening an otherwise readable old
                # manifest fail; the UPDATE-first write path remains
                # idempotent and future writes converge all duplicates.
                pass
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_dataset_results_run "
                "ON dataset_results(run_id, status, criticality)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_dataset_results_dataset "
                "ON dataset_results(dataset, stage, status)"
            )

    @staticmethod
    def _batch_activity_at(row: sqlite3.Row) -> datetime | None:
        for field in ("heartbeat_at", "started_at"):
            raw = row[field]
            if raw:
                return datetime.fromisoformat(raw)
        return None

    def start_run(self, job_name: str, metadata: dict[str, Any] | None = None) -> str:
        run_id = str(uuid.uuid4())
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO ingestion_runs (run_id, job_name, status, started_at, metadata_json)
                VALUES (?, ?, 'running', ?, ?)
                """,
                (run_id, job_name, _utcnow(), json.dumps(metadata or {})),
            )
        return run_id

    def finish_run(
        self,
        run_id: str,
        status: str,
        rows_read: int = 0,
        rows_written: int = 0,
        error_message: str | None = None,
    ) -> None:
        # A caller may still pass the legacy ``warning`` spelling or may
        # finalize a run without first asking for its aggregate.  Once logical
        # receipts exist, make the persisted run status obey the same
        # core/research policy as ``aggregate_run_status``.  Runs from before
        # this table was introduced retain their original status.
        if status in {"success", "warning", "degraded", "failed"}:
            aggregate = self.aggregate_run_status(run_id)
            if aggregate["results"]:
                if aggregate["status"] == "failed" or status == "failed":
                    status = "failed"
                elif aggregate["status"] == "degraded" or status in {"warning", "degraded"}:
                    status = "degraded"
                else:
                    status = "success"
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE ingestion_runs
                SET status = ?, finished_at = ?, rows_read = ?, rows_written = ?, error_message = ?
                WHERE run_id = ?
                """,
                (status, _utcnow(), rows_read, rows_written, error_message, run_id),
            )

    def start_batch(
        self,
        run_id: str,
        batch_id: str,
        task_id: str,
        dataset: str,
        symbols: list[str] | None = None,
        window_start: str | None = None,
        window_end: str | None = None,
        blocks_compaction: bool = True,
    ) -> None:
        now = _utcnow()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO ingestion_batches (
                    run_id, batch_id, task_id, dataset, status, symbols_json,
                    window_start, window_end, started_at, heartbeat_at, retry_count,
                    request_retry_count, blocks_compaction
                ) VALUES (?, ?, ?, ?, 'running', ?, ?, ?, ?, ?, 0, 0, ?)
                ON CONFLICT(run_id, batch_id) DO UPDATE SET
                    task_id = excluded.task_id,
                    dataset = excluded.dataset,
                    status = 'running',
                    symbols_json = excluded.symbols_json,
                    window_start = excluded.window_start,
                    window_end = excluded.window_end,
                    rows_read = 0,
                    rows_written = 0,
                    started_at = excluded.started_at,
                    finished_at = NULL,
                    error_message = NULL,
                    heartbeat_at = excluded.heartbeat_at,
                    blocks_compaction = excluded.blocks_compaction
                WHERE ingestion_batches.status <> 'success'
                """,
                (
                    run_id,
                    batch_id,
                    task_id,
                    dataset,
                    json.dumps(symbols or []),
                    window_start,
                    window_end,
                    now,
                    now,
                    int(blocks_compaction),
                ),
            )

    def touch_batch_heartbeat(self, run_id: str, batch_id: str) -> None:
        """Refresh liveness timestamp for a running batch."""
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE ingestion_batches
                SET heartbeat_at = ?
                WHERE run_id = ? AND batch_id = ? AND status = 'running'
                """,
                (_utcnow(), run_id, batch_id),
            )

    def set_batch_dataset(self, run_id: str, batch_id: str, dataset: str) -> None:
        """Set the physical dataset a step's staging batch will compact into."""
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE ingestion_batches
                SET dataset = ?
                WHERE run_id = ? AND batch_id = ?
                """,
                (dataset, run_id, batch_id),
            )

    def set_batch_symbols(self, run_id: str, batch_id: str, symbols: list[str]) -> None:
        """Persist the retry scope discovered by a non-worker step."""
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE ingestion_batches
                SET symbols_json = ?
                WHERE run_id = ? AND batch_id = ?
                """,
                (json.dumps(symbols), run_id, batch_id),
            )

    def finish_batch(
        self,
        run_id: str,
        batch_id: str,
        status: str,
        rows_read: int = 0,
        rows_written: int = 0,
        error_message: str | None = None,
        retry_count: int | None = None,
        request_retry_count: int | None = None,
    ) -> None:
        if request_retry_count is not None:
            try:
                request_retry_count = int(request_retry_count)
            except (TypeError, ValueError) as exc:
                raise ValueError("request_retry_count must be an integer") from exc
            if request_retry_count < 0:
                raise ValueError("request_retry_count must be non-negative")
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE ingestion_batches
                SET status = ?, finished_at = ?, rows_read = ?, rows_written = ?,
                    error_message = ?, retry_count = COALESCE(?, retry_count),
                    request_retry_count = CASE
                        WHEN ? IS NULL THEN request_retry_count
                        ELSE MAX(COALESCE(request_retry_count, 0), ?)
                    END,
                    blocks_compaction = CASE
                        WHEN ? IN ('warning', 'failed', 'stale') THEN 1
                        ELSE blocks_compaction
                    END
                WHERE run_id = ? AND batch_id = ? AND status = 'running'
                """,
                (
                    status,
                    _utcnow(),
                    rows_read,
                    rows_written,
                    error_message,
                    retry_count,
                    request_retry_count,
                    request_retry_count,
                    status,
                    run_id,
                    batch_id,
                ),
            )

    def record_batch_telemetry(
        self,
        run_id: str,
        batch_id: str,
        metrics: Mapping[str, Any] | None = None,
        *,
        request_retry_count: int | None = None,
    ) -> bool:
        """Persist adapter-observed retry telemetry for one batch.

        ``retry_count`` remains the orchestrator-owned requeue counter/budget.
        Adapters report request-level retries through
        ``request_retry_count`` (or the legacy ``retries`` metric when the
        explicit key is absent).  The value is monotonic so a late telemetry
        callback cannot erase an observation made by the worker that finished
        the batch.  This method intentionally does not derive a value from
        logs, response counts, or elapsed time.

        Returns ``True`` when the batch exists and was updated.
        """
        if request_retry_count is None and metrics is not None:
            if "request_retries" in metrics:
                request_retry_count = metrics.get("request_retries")
            elif "orchestrator_retries" not in metrics:
                # Existing adapters use ``retries`` for their own request
                # attempts. Keep that contract while callers migrate to the
                # unambiguous ``request_retries`` spelling.
                request_retry_count = metrics.get("retries")
        if request_retry_count is None:
            return False
        try:
            request_retry_count = int(request_retry_count)
        except (TypeError, ValueError) as exc:
            raise ValueError("request_retry_count must be an integer") from exc
        if request_retry_count < 0:
            raise ValueError("request_retry_count must be non-negative")
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE ingestion_batches
                SET request_retry_count = MAX(COALESCE(request_retry_count, 0), ?)
                WHERE run_id = ? AND batch_id = ?
                """,
                (request_retry_count, run_id, batch_id),
            )
            return bool(cur.rowcount)

    # Clear aliases for callers that describe this as an adapter report.
    record_adapter_telemetry = record_batch_telemetry

    def get_retry_telemetry(self, run_id: str) -> dict[str, int]:
        """Aggregate observed and orchestrator retries for one run.

        The two components are kept separate so operators can distinguish a
        batch being requeued from an adapter retrying a request.  ``retries``
        is their explicit sum, never an estimate.
        """
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COALESCE(SUM(retry_count), 0) AS orchestrator_retries,
                       COALESCE(SUM(request_retry_count), 0) AS request_retries
                FROM ingestion_batches WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
        orchestrator = int(row["orchestrator_retries"] if row is not None else 0)
        request = int(row["request_retries"] if row is not None else 0)
        return {
            "orchestrator_retries": orchestrator,
            "request_retries": request,
            "retries": orchestrator + request,
        }

    # ``retry_telemetry`` is a concise compatibility spelling used by status
    # consumers; both names return the same explicit aggregation.
    retry_telemetry = get_retry_telemetry

    def resolve_failed_batch(
        self,
        run_id: str,
        batch_id: str,
        *,
        error_message: str,
    ) -> None:
        """Close a failed attempt after a step-level recovery succeeds.

        Worker batches are recorded as failed before the step-level secondary
        source gets a chance to repair their symbols. Keeping that terminal
        status would block compaction even when the step verified recovery.
        The failed attempt remains in the audit ledger, but its current
        completion state becomes success.
        """
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE ingestion_batches
                SET status = 'success', finished_at = ?, error_message = ?
                WHERE run_id = ? AND batch_id = ? AND status = 'failed'
                """,
                (_utcnow(), error_message, run_id, batch_id),
            )

    def supersede_batches(
        self,
        run_id: str,
        batch_ids: list[str],
        *,
        superseded_by: str,
    ) -> int:
        """Resolve prior retryable attempts after one verified successful retry.

        Rows remain in the ledger for audit. Only their current-completion
        status changes, so repeated failures do not need to be deleted or
        replayed one by one.
        """
        ids = sorted(set(batch_ids))
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        message = f"superseded by successful retry batch {superseded_by}"
        with self._connect() as conn:
            cur = conn.execute(
                f"""
                UPDATE ingestion_batches
                SET status = 'superseded', error_message = ?
                WHERE run_id = ? AND batch_id IN ({placeholders})
                  AND status IN ('failed', 'warning', 'stale')
                """,
                (message, run_id, *ids),
            )
            return cur.rowcount

    def increment_batch_retry_counts(self, run_id: str, batch_ids: list[str]) -> int:
        """Persist one orchestrator retry attempt for each selected batch."""
        ids = sorted(set(batch_ids))
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        with self._connect() as conn:
            cur = conn.execute(
                f"""
                UPDATE ingestion_batches
                SET retry_count = retry_count + 1
                WHERE run_id = ? AND batch_id IN ({placeholders})
                  AND status IN ('failed', 'warning')
                """,
                (run_id, *ids),
            )
            return cur.rowcount

    def get_retryable_batches(
        self, run_id: str, *, max_retries: int | None = None
    ) -> list[sqlite3.Row]:
        with self._connect() as conn:
            retry_clause = "" if max_retries is None else " AND retry_count < ?"
            params: tuple[object, ...] = (run_id,)
            if max_retries is not None:
                params += (max_retries,)
            cur = conn.execute(
                f"""
                SELECT * FROM ingestion_batches
                WHERE run_id = ? AND status IN ('failed', 'warning')
                {retry_clause}
                ORDER BY started_at, batch_id
                """,
                params,
            )
            return cur.fetchall()

    def exhausted_retry_count(self, run_id: str, *, max_retries: int) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT COUNT(*) AS cnt FROM ingestion_batches
                WHERE run_id = ? AND status IN ('failed', 'warning')
                  AND retry_count >= ?
                """,
                (run_id, max_retries),
            )
            return int(cur.fetchone()["cnt"])

    def get_failed_batches(self, run_id: str) -> list[sqlite3.Row]:
        """Backward-compatible alias for every immediately retryable batch."""
        return self.get_retryable_batches(run_id)

    def incomplete_batch_counts_by_dataset(self, run_id: str) -> dict[str, int]:
        """Count batches not resolved by success or a successful retry."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT dataset, COUNT(*) AS cnt
                FROM ingestion_batches
                WHERE run_id = ? AND status NOT IN ('success', 'superseded')
                  AND blocks_compaction = 1
                GROUP BY dataset
                """,
                (run_id,),
            )
            return {row["dataset"]: row["cnt"] for row in cur.fetchall()}

    def incomplete_batch_count(self, run_id: str) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM ingestion_batches
                WHERE run_id = ? AND status NOT IN ('success', 'superseded')
                """,
                (run_id,),
            )
            return int(cur.fetchone()["cnt"])

    def incomplete_batch_counts_by_status(self, run_id: str) -> dict[str, int]:
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT status, COUNT(*) AS cnt
                FROM ingestion_batches
                WHERE run_id = ? AND status NOT IN ('success', 'superseded')
                GROUP BY status
                """,
                (run_id,),
            )
            return {row["status"]: row["cnt"] for row in cur.fetchall()}

    def mark_batch_stale(self, run_id: str, batch_id: str, error_message: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE ingestion_batches
                SET status = 'stale', error_message = ?
                WHERE run_id = ? AND batch_id = ?
                """,
                (error_message, run_id, batch_id),
            )

    def promote_running_to_stale(self, run_id: str, *, stale_after_seconds: float) -> int:
        """Mark running batches with expired heartbeats as stale."""
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=stale_after_seconds)
        stale_ids: list[str] = []
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT batch_id, started_at, heartbeat_at
                FROM ingestion_batches
                WHERE run_id = ? AND status = 'running'
                """,
                (run_id,),
            )
            for row in cur:
                activity = self._batch_activity_at(row)
                if activity is None or activity <= cutoff:
                    stale_ids.append(row["batch_id"])
            for batch_id in stale_ids:
                conn.execute(
                    """
                    UPDATE ingestion_batches
                    SET status = 'stale', error_message = ?
                    WHERE run_id = ? AND batch_id = ?
                    """,
                    (
                        "batch heartbeat timed out (worker likely stuck or crashed)",
                        run_id,
                        batch_id,
                    ),
                )
        return len(stale_ids)

    def promote_stale_to_failed(self, run_id: str) -> int:
        """Promote stale batches to failed so retry can pick them up."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT batch_id FROM ingestion_batches
                WHERE run_id = ? AND status = 'stale'
                """,
                (run_id,),
            )
            stale_ids = [row["batch_id"] for row in cur]
            for batch_id in stale_ids:
                conn.execute(
                    """
                    UPDATE ingestion_batches
                    SET status = 'failed', finished_at = ?, error_message = ?
                    WHERE run_id = ? AND batch_id = ?
                    """,
                    (
                        _utcnow(),
                        "batch promoted from stale after heartbeat timeout",
                        run_id,
                        batch_id,
                    ),
                )
        return len(stale_ids)

    def advance_batch_timeouts(self, run_id: str, *, stale_after_seconds: float) -> dict[str, int]:
        """Apply running→stale→failed lifecycle for timed-out batches."""
        running_to_stale = self.promote_running_to_stale(
            run_id, stale_after_seconds=stale_after_seconds
        )
        stale_to_failed = self.promote_stale_to_failed(run_id)
        return {
            "running_to_stale": running_to_stale,
            "stale_to_failed": stale_to_failed,
        }

    def _run_activity_at(self, conn: sqlite3.Connection, run_id: str, started_at: str) -> datetime:
        """Latest evidence the run is still alive: batch heartbeat or run start."""
        activity = datetime.fromisoformat(started_at)
        cur = conn.execute(
            """
            SELECT heartbeat_at, started_at FROM ingestion_batches
            WHERE run_id = ? AND status IN ('running', 'stale')
            """,
            (run_id,),
        )
        for row in cur:
            batch_activity = self._batch_activity_at(row)
            if batch_activity is not None and batch_activity > activity:
                activity = batch_activity
        return activity

    def reconcile_orphaned_runs(
        self,
        *,
        stale_after_seconds: float = 300,
        error_message: str = "reconciled: worker exited without finish_run",
        locks_root: Path | None = None,
    ) -> dict[str, int]:
        """Close runs/batches with no activity past *stale_after_seconds*.

        Activity is ``max(run.started_at, max batch heartbeat/started)`` so a
        long-lived job that still heartbeats is not mistaken for a crash.
        Runs whose ``meta/locks/{run_id}.lock`` is held are skipped — another
        process still owns them. Updates are idempotent (``WHERE status=…``).
        """
        from cnequity.orchestrator.run_lock import is_run_locked

        cutoff = datetime.now(timezone.utc) - timedelta(seconds=stale_after_seconds)
        runs_closed = 0
        batches_closed = 0
        skipped_locked = 0
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT run_id, started_at FROM ingestion_runs WHERE status = 'running'"
            )
            candidates = list(cur)
            orphan_run_ids: list[str] = []
            for row in candidates:
                run_id = row["run_id"]
                if locks_root is not None and is_run_locked(locks_root, run_id):
                    skipped_locked += 1
                    continue
                activity = self._run_activity_at(conn, run_id, row["started_at"])
                if activity > cutoff:
                    continue
                orphan_run_ids.append(run_id)
            now = _utcnow()
            for run_id in orphan_run_ids:
                run_cur = conn.execute(
                    """
                    UPDATE ingestion_runs
                    SET status = 'failed', finished_at = ?, error_message = ?
                    WHERE run_id = ? AND status = 'running'
                    """,
                    (now, error_message, run_id),
                )
                if run_cur.rowcount == 0:
                    continue
                runs_closed += 1
                batch_cur = conn.execute(
                    """
                    SELECT batch_id FROM ingestion_batches
                    WHERE run_id = ? AND status IN ('running', 'stale')
                    """,
                    (run_id,),
                )
                for batch_row in batch_cur:
                    bcur = conn.execute(
                        """
                        UPDATE ingestion_batches
                        SET status = 'failed', finished_at = ?, error_message = ?
                        WHERE run_id = ? AND batch_id = ? AND status IN ('running', 'stale')
                        """,
                        (now, error_message, run_id, batch_row["batch_id"]),
                    )
                    batches_closed += bcur.rowcount
        return {
            "runs_closed": runs_closed,
            "batches_closed": batches_closed,
            "skipped_locked": skipped_locked,
        }

    def count_stale_running_runs(
        self,
        *,
        stale_after_seconds: float,
        locks_root: Path | None = None,
    ) -> int:
        """How many ``running`` runs look orphaned (same rules as reconcile)."""
        from cnequity.orchestrator.run_lock import is_run_locked

        cutoff = datetime.now(timezone.utc) - timedelta(seconds=stale_after_seconds)
        n = 0
        with self._connect() as conn:
            for row in conn.execute(
                "SELECT run_id, started_at FROM ingestion_runs WHERE status = 'running'"
            ):
                if locks_root is not None and is_run_locked(locks_root, row["run_id"]):
                    continue
                if self._run_activity_at(conn, row["run_id"], row["started_at"]) <= cutoff:
                    n += 1
        return n

    def mark_stale_running_batches_failed(self, run_id: str, *, stale_after_seconds: float) -> int:
        """Backward-compatible alias: full running→stale→failed promotion."""
        result = self.advance_batch_timeouts(run_id, stale_after_seconds=stale_after_seconds)
        return result["running_to_stale"] + result["stale_to_failed"]

    def demote_success_batches(self, run_id: str, *, reason: str) -> int:
        """Mark fetch batches failed (e.g. staging evicted) so retry refetches them."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE ingestion_batches
                SET status = 'failed', error_message = ?
                WHERE run_id = ? AND status = 'success' AND dataset NOT IN
                    ('compact', 'derive_adj_factors', 'derive_industry_index', 'audit')
                """,
                (reason, run_id),
            )
            return cur.rowcount

    def get_batches_for_run(self, run_id: str) -> list[sqlite3.Row]:
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT * FROM ingestion_batches WHERE run_id = ? ORDER BY batch_id",
                (run_id,),
            )
            return cur.fetchall()

    def get_successful_batches(
        self,
        dataset: str,
        window_start: str,
        window_end: str,
        *,
        exclude_run_id: str | None = None,
    ) -> list[sqlite3.Row]:
        """Find successful batches for an identical date window.

        Daily catchup is resumable across runs: a prior run may have compact
        verified batches before a later batch failed. Those batches are safe to
        reuse, while failed/running attempts are intentionally excluded.
        """
        query = """
            SELECT * FROM ingestion_batches
            WHERE dataset = ? AND status = 'success'
              AND window_start = ? AND window_end = ?
        """
        params: list[object] = [dataset, window_start, window_end]
        if exclude_run_id is not None:
            query += " AND run_id != ?"
            params.append(exclude_run_id)
        query += " ORDER BY finished_at, run_id, batch_id"
        with self._connect() as conn:
            return conn.execute(query, params).fetchall()

    def get_batch(self, run_id: str, batch_id: str) -> sqlite3.Row | None:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT * FROM ingestion_batches
                WHERE run_id = ? AND batch_id = ?
                """,
                (run_id, batch_id),
            ).fetchone()

    def record_dataset_result(
        self,
        run_id: str,
        dataset: str,
        stage: str,
        status: str,
        *,
        criticality: str = "core",
        revision_id: str | None = None,
        rows_written: int = 0,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """Upsert the public result for one dataset/stage.

        ``dataset_results`` is intentionally independent of batch retries.
        A retry updates the same logical receipt, while the detailed failed
        attempt remains in ``ingestion_batches``.  Updating then inserting
        (instead of relying only on ``ON CONFLICT``) also works with manifests
        created by an early migration that had no primary key declaration.
        """
        if stage not in DATASET_RESULT_STAGES:
            raise ValueError(f"invalid dataset result stage: {stage!r}")
        if status not in DATASET_RESULT_STATUSES:
            raise ValueError(f"invalid dataset result status: {status!r}")
        if criticality not in DATASET_RESULT_CRITICALITIES:
            raise ValueError(f"invalid dataset result criticality: {criticality!r}")
        try:
            rows = int(rows_written)
        except (TypeError, ValueError) as exc:
            raise ValueError("rows_written must be an integer") from exc
        if rows < 0:
            raise ValueError("rows_written must be non-negative")

        values = (
            status,
            criticality,
            revision_id,
            rows,
            error_code,
            error_message,
            run_id,
            dataset,
            stage,
        )
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE dataset_results
                SET status = ?, criticality = ?, revision_id = ?, rows_written = ?,
                    error_code = ?, error_message = ?
                WHERE run_id = ? AND dataset = ? AND stage = ?
                """,
                values,
            )
            if cur.rowcount:
                return
            try:
                conn.execute(
                    """
                    INSERT INTO dataset_results (
                        run_id, dataset, stage, status, criticality, revision_id,
                        rows_written, error_code, error_message
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        dataset,
                        stage,
                        status,
                        criticality,
                        revision_id,
                        rows,
                        error_code,
                        error_message,
                    ),
                )
            except sqlite3.IntegrityError:
                # Another worker may have inserted the same logical receipt
                # between UPDATE and INSERT.  Finish with a deterministic
                # update rather than leaking a transient UNIQUE failure.
                conn.execute(
                    """
                    UPDATE dataset_results
                    SET status = ?, criticality = ?, revision_id = ?, rows_written = ?,
                        error_code = ?, error_message = ?
                    WHERE run_id = ? AND dataset = ? AND stage = ?
                    """,
                    values,
                )

    # Synonyms make the small API convenient for callers that describe this as
    # a receipt or an upsert, and keep compatibility with early stage-4
    # consumers that used those names while the schema was being finalized.
    upsert_dataset_result = record_dataset_result
    record_result = record_dataset_result

    def get_dataset_results(
        self,
        run_id: str,
        dataset: str | None = None,
        stage: str | None = None,
    ) -> list[sqlite3.Row]:
        """Read dataset/stage receipts for *run_id* in stable order."""
        clauses = ["run_id = ?"]
        params: list[object] = [run_id]
        if dataset is not None:
            clauses.append("dataset = ?")
            params.append(dataset)
        if stage is not None:
            clauses.append("stage = ?")
            params.append(stage)
        where = " AND ".join(clauses)
        with self._connect() as conn:
            return conn.execute(
                f"""
                SELECT run_id, dataset, stage, status, criticality, revision_id,
                       rows_written, error_code, error_message
                FROM dataset_results
                WHERE {where}
                ORDER BY dataset, stage
                """,
                params,
            ).fetchall()

    list_dataset_results = get_dataset_results
    dataset_results_for_run = get_dataset_results

    def get_dataset_result(self, run_id: str, dataset: str, stage: str) -> sqlite3.Row | None:
        rows = self.get_dataset_results(run_id, dataset=dataset, stage=stage)
        return rows[0] if rows else None

    def aggregate_run_status(self, run_id: str) -> dict[str, Any]:
        """Aggregate logical dataset receipts into success/degraded/failed.

        A core failure (including a blocked core gate) is a failed run.  A
        research/advisory failure, or any warning/degraded result, leaves the
        run usable but degraded.  Explicitly skipped optional stages do not
        affect the aggregate.
        """
        rows = self.get_dataset_results(run_id)
        core_failures: list[dict[str, Any]] = []
        degraded_results: list[dict[str, Any]] = []
        counts: dict[str, int] = {}
        for row in rows:
            status = str(row["status"])
            counts[status] = counts.get(status, 0) + 1
            if status == "skipped":
                continue
            item = dict(row)
            criticality = str(row["criticality"] or "core")
            if criticality == "core" and status in {"failed", "blocked"}:
                core_failures.append(item)
            elif status in {"warning", "degraded", "failed", "blocked"}:
                degraded_results.append(item)
        if core_failures:
            status = "failed"
        elif degraded_results:
            status = "degraded"
        else:
            status = "success"
        return {
            "status": status,
            "counts": counts,
            "core_failures": core_failures,
            "degraded_results": degraded_results,
            "results": [dict(row) for row in rows],
        }

    # ``dataset_status`` reads naturally at call sites that need only the
    # aggregate and is kept as an alias for compatibility with status UIs.
    dataset_status = aggregate_run_status

    def get_run(self, run_id: str) -> sqlite3.Row | None:
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM ingestion_runs WHERE run_id = ?", (run_id,)
            ).fetchone()

    def list_runs(self, job_name: str | None = None) -> list[sqlite3.Row]:
        with self._connect() as conn:
            if job_name:
                cur = conn.execute(
                    """
                    SELECT * FROM ingestion_runs WHERE job_name = ?
                    ORDER BY started_at DESC
                    """,
                    (job_name,),
                )
            else:
                cur = conn.execute("SELECT * FROM ingestion_runs ORDER BY started_at DESC")
            return cur.fetchall()

    def latest_incomplete_init_run(self) -> sqlite3.Row | None:
        from cnequity.orchestrator.init_phases import init_run_complete

        for run in self.list_runs("init"):
            meta = json.loads(run["metadata_json"] or "{}")
            phases = meta.get("phases") or []
            if not phases:
                continue
            batches = self.get_batches_for_run(run["run_id"])
            if init_run_complete(phases, batches):
                continue
            return run
        return None

    def latest_run(self, job_name: str | None = None) -> sqlite3.Row | None:
        with self._connect() as conn:
            if job_name:
                cur = conn.execute(
                    """
                    SELECT * FROM ingestion_runs WHERE job_name = ?
                    ORDER BY started_at DESC LIMIT 1
                    """,
                    (job_name,),
                )
            else:
                cur = conn.execute("SELECT * FROM ingestion_runs ORDER BY started_at DESC LIMIT 1")
            return cur.fetchone()

    def run_summary(self, run_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            run = conn.execute(
                "SELECT * FROM ingestion_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            batches = conn.execute(
                "SELECT status, COUNT(*) as cnt FROM ingestion_batches WHERE run_id = ? GROUP BY status",
                (run_id,),
            ).fetchall()
        dataset_status = self.aggregate_run_status(run_id)
        run_payload = dict(run) if run else None
        public_dataset_status = (
            dataset_status["status"]
            if dataset_status["results"]
            else (run_payload.get("status") if run_payload else None)
        )
        return {
            "run": run_payload,
            "run_id": run_payload.get("run_id") if run_payload else run_id,
            "job_name": run_payload.get("job_name") if run_payload else None,
            "status": run_payload.get("status") if run_payload else None,
            "batch_counts": {row["status"]: row["cnt"] for row in batches},
            "dataset_results": dataset_status["results"],
            "dataset_status": public_dataset_status,
            "dataset_result_counts": dataset_status["counts"],
            "retry_telemetry": self.get_retry_telemetry(run_id),
        }

    def update_run_metadata(self, run_id: str, metadata: dict[str, Any]) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE ingestion_runs SET metadata_json = ? WHERE run_id = ?",
                (json.dumps(metadata), run_id),
            )

    def _mutate_run_metadata(
        self,
        run_id: str,
        mutation: Callable[[sqlite3.Connection, dict[str, Any]], dict[str, Any] | None],
    ) -> dict[str, Any]:
        """Atomically read, mutate, and write one run's metadata document.

        ``metadata_json`` is one JSON document, so every read/modify/write
        must happen while the same SQLite connection owns an ``IMMEDIATE``
        transaction.  This serializes mutations across Manifest instances and
        worker processes, while giving the callback the latest document so it
        cannot overwrite keys added by a concurrent writer.

        The callback must only perform the short in-transaction mutation.  In
        particular, it must not call another Manifest method that opens a
        second connection for the same database, or it could wait on this
        transaction's write lock.
        """
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT metadata_json FROM ingestion_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            raw = json.loads(row["metadata_json"] or "{}") if row else {}
            metadata = raw if isinstance(raw, dict) else {}
            mutated = mutation(conn, metadata)
            if mutated is not None:
                if not isinstance(mutated, dict):
                    raise TypeError("run metadata mutation must return a dict or None")
                metadata = mutated
            if row:
                conn.execute(
                    "UPDATE ingestion_runs SET metadata_json = ? WHERE run_id = ?",
                    (json.dumps(metadata), run_id),
                )
            return metadata

    def mutate_run_metadata(
        self,
        run_id: str,
        mutation: Callable[[dict[str, Any]], dict[str, Any] | None],
    ) -> dict[str, Any]:
        """Apply a metadata-only mutation under the manifest write lock.

        The callback receives the latest metadata mapping and may mutate it in
        place or return a replacement mapping.  Use this for callers that
        would otherwise call ``get_run_metadata`` followed by
        ``update_run_metadata``.
        """
        return self._mutate_run_metadata(
            run_id,
            lambda _conn, metadata: mutation(metadata),
        )

    def record_performance_metrics(
        self,
        run_id: str,
        dataset: str,
        metrics: dict[str, Any],
    ) -> None:
        """Persist source/request performance metrics under run metadata.

        Metrics are deliberately metadata rather than a gate or a second
        mutable table: old manifests remain readable, and benchmark output is
        carried with the same immutable run identity as rows/read receipts.
        The update is best-effort at call sites because losing telemetry must
        never discard a successfully fetched partition.
        """

        def _record(_conn: sqlite3.Connection, metadata: dict[str, Any]) -> None:
            performance = metadata.get("performance")
            if not isinstance(performance, dict):
                performance = {}
                metadata["performance"] = performance
            performance[dataset] = dict(metrics)

        self._mutate_run_metadata(run_id, _record)

    def get_run_metadata(self, run_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT metadata_json FROM ingestion_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        if not row:
            return {}
        return json.loads(row["metadata_json"] or "{}")

    def record_stage_metrics(
        self,
        run_id: str,
        stage: str,
        elapsed_seconds: float,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        """Persist descriptive stage metrics in the run metadata.

        This is intentionally additive and lives in ``metadata_json`` so old
        manifests remain readable without a schema migration.  It is called
        after each completed step; a process interrupted later therefore
        still leaves the metrics for the work that was actually observed.
        """
        from cnequity.diagnostics.metrics import (
            _rss_bytes,
            add_metrics,
            new_metrics,
            stage_metrics,
        )

        stage_payload = stage_metrics(stage, elapsed_seconds=elapsed_seconds, metrics=metrics)
        stage_record = stage_payload["stages"][stage]
        stage_record["peak_memory_bytes"] = max(
            int((metrics or {}).get("peak_memory_bytes", 0) or 0), _rss_bytes()
        )

        def _record(conn: sqlite3.Connection, payload: dict[str, Any]) -> None:
            aggregate = payload.get("metrics")
            if not isinstance(aggregate, dict):
                aggregate = new_metrics()
            # Keep one deterministic latest stage record while accumulating the
            # run-level counters.  Re-recording a stage after retry replaces its
            # stage row; retry counts are read from the batch ledger below.
            stages = aggregate.setdefault("stages", {})
            if not isinstance(stages, dict):
                stages = {}
                aggregate["stages"] = stages
            stages[stage] = stage_record
            # Totals are recalculated from the latest stage records rather than
            # incremented on retries, so a repeated retry cannot double-count a
            # successful stage.
            totals = new_metrics()
            totals["stages"] = {}
            for item in stages.values():
                if isinstance(item, dict):
                    add_metrics(totals, {**item, "stages": {}})
            aggregate.update(
                {
                    key: totals.get(key, 0)
                    for key in totals
                    if key
                    in {
                        "requests",
                        "pages",
                        "cache_hits",
                        "fallback_requests",
                        "retries",
                        "request_retries",
                        "failed_requests",
                        "rows_read",
                        "rows_written",
                        "bytes_read",
                        "bytes_written",
                        "changed_partitions",
                    }
                }
            )
            # Batch retries are tracked independently from stage payloads.  This
            # keeps retry telemetry available even when a non-worker step fails
            # before it can return its own metrics object. Request retries are
            # an adapter observation; orchestrator retries are the retry budget
            # used by the run-level recovery loop.
            retry_row = conn.execute(
                "SELECT COALESCE(SUM(retry_count), 0) AS orchestrator_retries, "
                "COALESCE(SUM(request_retry_count), 0) AS request_retries "
                "FROM ingestion_batches WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            orchestrator_retries = int(
                retry_row["orchestrator_retries"] if retry_row is not None else 0
            )
            request_retries = max(
                int(totals.get("request_retries", 0) or 0),
                int(retry_row["request_retries"] if retry_row is not None else 0),
            )
            aggregate["orchestrator_retries"] = orchestrator_retries
            aggregate["request_retries"] = request_retries
            aggregate["retries"] = max(
                int(totals.get("retries", 0) or 0),
                orchestrator_retries + request_retries,
            )
            aggregate["failed_requests"] = int(totals.get("failed_requests", 0) or 0)
            aggregate["request_seconds"] = float(totals.get("request_seconds", 0.0) or 0.0)
            aggregate["concurrency_wait_seconds"] = float(
                totals.get("concurrency_wait_seconds", 0.0) or 0.0
            )
            aggregate["concurrency_peak"] = int(totals.get("concurrency_peak", 0) or 0)
            aggregate["source_metrics"] = totals.get("source_metrics", {})
            aggregate["peak_memory_bytes"] = max(
                int(aggregate.get("peak_memory_bytes", 0) or 0),
                int(totals.get("peak_memory_bytes", 0) or 0),
                int((metrics or {}).get("peak_memory_bytes", 0) or 0),
                _rss_bytes(),
            )
            aggregate["elapsed_seconds"] = round(
                sum(float(item.get("elapsed_seconds", 0.0) or 0.0) for item in stages.values()),
                6,
            )
            aggregate["throughput_requests_per_second"] = round(
                int(aggregate.get("requests", 0) or 0)
                / max(float(aggregate["elapsed_seconds"] or 0.0), 1e-9),
                3,
            )
            aggregate["updated_at"] = _utcnow()
            payload["metrics"] = aggregate

        self._mutate_run_metadata(run_id, _record)
