from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    started_at: str | None = None
    finished_at: str | None = None
    error_message: str | None = None
    blocks_compaction: bool = True


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
                """
            )
            cols = {row[1] for row in conn.execute("PRAGMA table_info(ingestion_batches)")}
            if "heartbeat_at" not in cols:
                conn.execute("ALTER TABLE ingestion_batches ADD COLUMN heartbeat_at TEXT")
            if "blocks_compaction" not in cols:
                conn.execute(
                    "ALTER TABLE ingestion_batches ADD COLUMN blocks_compaction INTEGER DEFAULT 1"
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
                    blocks_compaction
                ) VALUES (?, ?, ?, ?, 'running', ?, ?, ?, ?, ?, 0, ?)
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
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE ingestion_batches
                SET status = ?, finished_at = ?, rows_read = ?, rows_written = ?,
                    error_message = ?, retry_count = COALESCE(?, retry_count),
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
                    status,
                    run_id,
                    batch_id,
                ),
            )

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
        return {
            "run": dict(run) if run else None,
            "batch_counts": {row["status"]: row["cnt"] for row in batches},
        }

    def update_run_metadata(self, run_id: str, metadata: dict[str, Any]) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE ingestion_runs SET metadata_json = ? WHERE run_id = ?",
                (json.dumps(metadata), run_id),
            )

    def get_run_metadata(self, run_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT metadata_json FROM ingestion_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        if not row:
            return {}
        return json.loads(row["metadata_json"] or "{}")
