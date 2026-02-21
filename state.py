from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _ensure_utc(dt: datetime) -> datetime:
    """Ensure a datetime is timezone-aware (UTC).

    SQLite strips timezone info, so datetimes read back may be naive.
    This helper re-attaches UTC when needed.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


class State:
    """SQLite persistence layer for congressional hearing transcript pipeline."""

    # Class-level cache: skip _init_db() if this db_path was already initialized
    # in this process.  Safe because _init_db() is idempotent (CREATE IF NOT EXISTS).
    _initialized_dbs: set[str] = set()

    def __init__(self, db_path: Path | None = None):
        if db_path is None:
            import config
            db_path = config.DATA_DIR / "state.db"

        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()

        # Initialize database with tables (skip if already done for this path)
        db_key = str(self.db_path.resolve())
        if db_key not in State._initialized_dbs:
            self._init_db()
            State._initialized_dbs.add(db_key)

    def __enter__(self) -> State:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _get_conn(self) -> sqlite3.Connection:
        """Get a thread-local database connection (created once per thread, reused)."""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
        return conn

    def close(self) -> None:
        """Close the current thread's database connection."""
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    def _init_db(self) -> None:
        """Create tables if they don't exist."""
        conn = self._get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS hearings (
                id TEXT PRIMARY KEY,
                committee_key TEXT,
                date TEXT,
                title TEXT,
                slug TEXT,
                sources_json TEXT,
                discovered_at TEXT,
                processed_at TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS processing_steps (
                hearing_id TEXT,
                step TEXT,
                status TEXT,
                started_at TEXT,
                completed_at TEXT,
                error TEXT,
                PRIMARY KEY (hearing_id, step)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS run_costs (
                run_id TEXT PRIMARY KEY,
                started_at TEXT,
                completed_at TEXT,
                hearings_processed INTEGER DEFAULT 0,
                llm_cleanup_usd REAL DEFAULT 0,
                whisper_usd REAL DEFAULT 0,
                total_usd REAL DEFAULT 0
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS scraper_health (
                committee_key TEXT,
                source_type TEXT,
                last_success TEXT,
                last_failure TEXT,
                last_count INTEGER,
                consecutive_failures INTEGER DEFAULT 0,
                PRIMARY KEY (committee_key, source_type)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS cspan_searches (
                committee_key TEXT PRIMARY KEY,
                last_searched TEXT,
                last_result_count INTEGER DEFAULT 0
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS cspan_title_searches (
                hearing_id TEXT PRIMARY KEY,
                searched_at TEXT,
                found INTEGER DEFAULT 0
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS digest_runs (
                run_date TEXT PRIMARY KEY,
                hearings_scanned INTEGER DEFAULT 0,
                quotes_extracted INTEGER DEFAULT 0,
                quotes_selected INTEGER DEFAULT 0,
                cost_usd REAL DEFAULT 0
            )
        """)

        # Queue rollout scaffolding (north-star phases 1+)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS queue_run_audits (
                run_id TEXT PRIMARY KEY,
                role TEXT,
                status TEXT,
                args_json TEXT,
                started_at TEXT,
                completed_at TEXT,
                hearings_discovered INTEGER DEFAULT 0,
                hearings_processed INTEGER DEFAULT 0,
                hearings_failed INTEGER DEFAULT 0,
                error TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS discovery_jobs (
                job_id TEXT PRIMARY KEY,
                run_id TEXT,
                status TEXT,
                payload_json TEXT,
                attempt_count INTEGER DEFAULT 0,
                max_attempts INTEGER DEFAULT 5,
                available_at TEXT,
                claimed_by TEXT,
                lease_expires_at TEXT,
                last_error TEXT,
                enqueued_at TEXT,
                started_at TEXT,
                completed_at TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS hearing_jobs (
                hearing_id TEXT PRIMARY KEY,
                run_id TEXT,
                committee_key TEXT,
                hearing_date TEXT,
                title TEXT,
                status TEXT,
                attempt_count INTEGER DEFAULT 0,
                max_attempts INTEGER DEFAULT 5,
                available_at TEXT,
                claimed_by TEXT,
                lease_expires_at TEXT,
                last_error TEXT,
                enqueued_at TEXT,
                started_at TEXT,
                completed_at TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS stage_tasks (
                task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                hearing_id TEXT NOT NULL,
                stage TEXT NOT NULL,
                publish_version INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL,
                attempt_count INTEGER DEFAULT 0,
                max_attempts INTEGER DEFAULT 5,
                available_at TEXT,
                claimed_by TEXT,
                lease_expires_at TEXT,
                last_error TEXT,
                payload_json TEXT,
                enqueued_at TEXT,
                started_at TEXT,
                completed_at TEXT,
                UNIQUE(hearing_id, stage, publish_version)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS delivery_outbox_items (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                hearing_id TEXT,
                publish_version INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                attempt_count INTEGER DEFAULT 0,
                max_attempts INTEGER DEFAULT 5,
                available_at TEXT,
                claimed_by TEXT,
                lease_expires_at TEXT,
                last_error TEXT,
                enqueued_at TEXT,
                delivered_at TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS dead_letter_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_type TEXT NOT NULL,
                item_key TEXT NOT NULL,
                stage TEXT,
                payload_json TEXT,
                error TEXT,
                attempt_count INTEGER DEFAULT 0,
                first_failed_at TEXT,
                last_failed_at TEXT,
                requeued_at TEXT,
                resolved_at TEXT
            )
        """)

        # Migration: add congress_event_id for cross-run identity matching
        cursor = conn.execute("PRAGMA table_info(hearings)")
        existing_cols = {row["name"] for row in cursor.fetchall()}
        if "congress_event_id" not in existing_cols:
            conn.execute("ALTER TABLE hearings ADD COLUMN congress_event_id TEXT")
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_hearings_congress_event_id
            ON hearings(congress_event_id) WHERE congress_event_id IS NOT NULL
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_discovery_jobs_status_available
            ON discovery_jobs(status, available_at)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_discovery_jobs_lease
            ON discovery_jobs(lease_expires_at)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_hearing_jobs_status_available
            ON hearing_jobs(status, available_at)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_hearing_jobs_lease
            ON hearing_jobs(lease_expires_at)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_stage_tasks_status_available
            ON stage_tasks(status, available_at)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_stage_tasks_lease
            ON stage_tasks(lease_expires_at)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_outbox_status_available
            ON delivery_outbox_items(status, available_at)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_outbox_lease
            ON delivery_outbox_items(lease_expires_at)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_dead_letter_lookup
            ON dead_letter_items(item_type, item_key, stage)
        """)

        conn.commit()

    def is_processed(self, hearing_id: str) -> bool:
        """Check if hearing has been marked as fully processed."""
        conn = self._get_conn()
        cursor = conn.execute(
            "SELECT processed_at FROM hearings WHERE id = ?", (hearing_id,)
        )
        row = cursor.fetchone()
        return row is not None and row['processed_at'] is not None

    def mark_processed(self, hearing_id: str) -> None:
        """Mark a hearing as fully processed."""
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "UPDATE hearings SET processed_at = ? WHERE id = ?",
            (now, hearing_id),
        )
        conn.commit()

    def is_step_done(self, hearing_id: str, step: str) -> bool:
        """Check if a specific step is done for a hearing."""
        conn = self._get_conn()
        cursor = conn.execute("""
            SELECT status
            FROM processing_steps
            WHERE hearing_id = ? AND step = ?
        """, (hearing_id, step))
        row = cursor.fetchone()
        return row is not None and row['status'] == 'done'

    def record_hearing(self, hearing_id: str, committee_key: str, date: str,
                      title: str, slug: str, sources: dict) -> None:
        """Insert or update a hearing record."""
        conn = self._get_conn()
        sources_json = json.dumps(sources)
        now = datetime.now(timezone.utc).isoformat()
        congress_event_id = sources.get("congress_api_event_id")

        # Check if hearing already exists
        cursor = conn.execute("SELECT id FROM hearings WHERE id = ?", (hearing_id,))
        exists = cursor.fetchone() is not None

        if exists:
            conn.execute("""
                UPDATE hearings
                SET committee_key = ?, date = ?, title = ?, slug = ?,
                    sources_json = ?, congress_event_id = COALESCE(?, congress_event_id)
                WHERE id = ?
            """, (committee_key, date, title, slug, sources_json,
                  congress_event_id, hearing_id))
        else:
            conn.execute("""
                INSERT INTO hearings (id, committee_key, date, title, slug,
                                     sources_json, discovered_at, congress_event_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (hearing_id, committee_key, date, title, slug, sources_json,
                  now, congress_event_id))

        conn.commit()

    def find_by_congress_event_id(self, event_id: str) -> dict | None:
        """Look up existing hearing by congress.gov event ID."""
        conn = self._get_conn()
        cursor = conn.execute(
            "SELECT id, committee_key, date, title, processed_at "
            "FROM hearings WHERE congress_event_id = ?", (event_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def find_by_committee_date(self, committee_key: str, date: str) -> list[dict]:
        """Find all hearings for a committee on a given date (for fuzzy reconciliation)."""
        conn = self._get_conn()
        cursor = conn.execute(
            "SELECT id, committee_key, date, title, processed_at, sources_json "
            "FROM hearings WHERE committee_key = ? AND date = ?",
            (committee_key, date))
        rows = []
        for row in cursor.fetchall():
            d = dict(row)
            if d.get("sources_json"):
                d["sources"] = json.loads(d["sources_json"])
            rows.append(d)
        return rows

    def mark_step(self, hearing_id: str, step: str, status: str,
                  error: str | None = None) -> None:
        """Update processing step status. Sets timestamps automatically."""
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()

        # Check if step exists
        cursor = conn.execute("""
            SELECT hearing_id FROM processing_steps
            WHERE hearing_id = ? AND step = ?
        """, (hearing_id, step))
        exists = cursor.fetchone() is not None

        if exists:
            # Update existing step
            if status == 'running':
                conn.execute("""
                    UPDATE processing_steps
                    SET status = ?, started_at = ?, error = NULL
                    WHERE hearing_id = ? AND step = ?
                """, (status, now, hearing_id, step))
            elif status in ('done', 'failed'):
                conn.execute("""
                    UPDATE processing_steps
                    SET status = ?, completed_at = ?, error = ?
                    WHERE hearing_id = ? AND step = ?
                """, (status, now, error, hearing_id, step))
            else:  # pending
                conn.execute("""
                    UPDATE processing_steps
                    SET status = ?, error = ?
                    WHERE hearing_id = ? AND step = ?
                """, (status, error, hearing_id, step))
        else:
            # Insert new step
            if status == 'running':
                conn.execute("""
                    INSERT INTO processing_steps (hearing_id, step, status, started_at)
                    VALUES (?, ?, ?, ?)
                """, (hearing_id, step, status, now))
            elif status in ('done', 'failed'):
                conn.execute("""
                    INSERT INTO processing_steps (hearing_id, step, status, started_at, completed_at, error)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (hearing_id, step, status, now, now, error))
            else:  # pending
                conn.execute("""
                    INSERT INTO processing_steps (hearing_id, step, status, error)
                    VALUES (?, ?, ?, ?)
                """, (hearing_id, step, status, error))

        conn.commit()

    def mark_stage_task(
        self,
        hearing_id: str,
        stage: str,
        status: str,
        error: str | None = None,
        publish_version: int = 1,
        payload: dict | None = None,
    ) -> None:
        """Upsert stage-level queue status for dual-write rollout."""
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        payload_json = json.dumps(payload) if payload is not None else None

        cursor = conn.execute("""
            SELECT task_id FROM stage_tasks
            WHERE hearing_id = ? AND stage = ? AND publish_version = ?
        """, (hearing_id, stage, publish_version))
        exists = cursor.fetchone() is not None

        if exists:
            if status == "running":
                conn.execute("""
                    UPDATE stage_tasks
                    SET status = ?,
                        attempt_count = attempt_count + 1,
                        started_at = ?,
                        completed_at = NULL,
                        last_error = NULL,
                        payload_json = COALESCE(?, payload_json)
                    WHERE hearing_id = ? AND stage = ? AND publish_version = ?
                """, (status, now, payload_json, hearing_id, stage, publish_version))
            elif status in ("done", "failed"):
                conn.execute("""
                    UPDATE stage_tasks
                    SET status = ?,
                        completed_at = ?,
                        last_error = ?,
                        payload_json = COALESCE(?, payload_json)
                    WHERE hearing_id = ? AND stage = ? AND publish_version = ?
                """, (status, now, error, payload_json, hearing_id, stage, publish_version))
            else:
                conn.execute("""
                    UPDATE stage_tasks
                    SET status = ?,
                        last_error = ?,
                        payload_json = COALESCE(?, payload_json)
                    WHERE hearing_id = ? AND stage = ? AND publish_version = ?
                """, (status, error, payload_json, hearing_id, stage, publish_version))
        else:
            attempt_count = 1 if status in ("running", "done", "failed") else 0
            if status == "running":
                conn.execute("""
                    INSERT INTO stage_tasks
                        (hearing_id, stage, publish_version, status, attempt_count,
                         available_at, enqueued_at, started_at, payload_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    hearing_id, stage, publish_version, status, attempt_count,
                    now, now, now, payload_json,
                ))
            elif status in ("done", "failed"):
                conn.execute("""
                    INSERT INTO stage_tasks
                        (hearing_id, stage, publish_version, status, attempt_count,
                         available_at, enqueued_at, started_at, completed_at,
                         last_error, payload_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    hearing_id, stage, publish_version, status, attempt_count,
                    now, now, now, now, error, payload_json,
                ))
            else:
                conn.execute("""
                    INSERT INTO stage_tasks
                        (hearing_id, stage, publish_version, status, attempt_count,
                         available_at, enqueued_at, payload_json, last_error)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    hearing_id, stage, publish_version, status, attempt_count,
                    now, now, payload_json, error,
                ))

        conn.commit()

    def record_scraper_run(self, committee_key: str, source_type: str,
                          count: int, error: str | None = None) -> None:
        """Log a scraper run result for health monitoring."""
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()

        # Check if record exists
        cursor = conn.execute("""
            SELECT consecutive_failures
            FROM scraper_health
            WHERE committee_key = ? AND source_type = ?
        """, (committee_key, source_type))
        row = cursor.fetchone()

        if error is None:
            # Success
            if row is not None:
                conn.execute("""
                    UPDATE scraper_health
                    SET last_success = ?, last_count = ?, consecutive_failures = 0
                    WHERE committee_key = ? AND source_type = ?
                """, (now, count, committee_key, source_type))
            else:
                conn.execute("""
                    INSERT INTO scraper_health (committee_key, source_type, last_success, last_count, consecutive_failures)
                    VALUES (?, ?, ?, ?, 0)
                """, (committee_key, source_type, now, count))
        else:
            # Failure
            consecutive_failures = (row['consecutive_failures'] + 1) if row is not None else 1

            if row is not None:
                conn.execute("""
                    UPDATE scraper_health
                    SET last_failure = ?, consecutive_failures = ?
                    WHERE committee_key = ? AND source_type = ?
                """, (now, consecutive_failures, committee_key, source_type))
            else:
                conn.execute("""
                    INSERT INTO scraper_health (committee_key, source_type, last_failure, consecutive_failures)
                    VALUES (?, ?, ?, ?)
                """, (committee_key, source_type, now, consecutive_failures))

        conn.commit()

    def get_failing_scrapers(self, threshold: int = 3) -> list[dict]:
        """Return scrapers with consecutive_failures >= threshold."""
        conn = self._get_conn()
        cursor = conn.execute("""
            SELECT committee_key, source_type, last_success, last_failure,
                   last_count, consecutive_failures
            FROM scraper_health
            WHERE consecutive_failures >= ?
            ORDER BY consecutive_failures DESC
        """, (threshold,))

        return [dict(row) for row in cursor.fetchall()]

    def record_run(self, run_id: str, started_at: str, completed_at: str,
                   hearings_processed: int, llm_cleanup_usd: float,
                   whisper_usd: float, total_usd: float) -> None:
        """Record a pipeline run with cost breakdown."""
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO run_costs
                (run_id, started_at, completed_at, hearings_processed,
                 llm_cleanup_usd, whisper_usd, total_usd)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (run_id, started_at, completed_at, hearings_processed,
              llm_cleanup_usd, whisper_usd, total_usd))
        conn.commit()

    def get_total_cost(self) -> dict:
        """Return cumulative cost across all runs."""
        conn = self._get_conn()
        cursor = conn.execute("""
            SELECT COUNT(*) as runs,
                   SUM(hearings_processed) as hearings,
                   SUM(llm_cleanup_usd) as llm_cleanup_usd,
                   SUM(whisper_usd) as whisper_usd,
                   SUM(total_usd) as total_usd
            FROM run_costs
        """)
        row = cursor.fetchone()
        return {
            "runs": row["runs"] or 0,
            "hearings": row["hearings"] or 0,
            "llm_cleanup_usd": row["llm_cleanup_usd"] or 0.0,
            "whisper_usd": row["whisper_usd"] or 0.0,
            "total_usd": row["total_usd"] or 0.0,
        }

    # ------------------------------------------------------------------
    # Queue run audit tracking (phase 1 scaffolding)
    # ------------------------------------------------------------------

    def record_queue_run_start(self, run_id: str, role: str, args: dict) -> None:
        """Insert or reset queue audit row for a run."""
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("""
            INSERT INTO queue_run_audits
                (run_id, role, status, args_json, started_at, completed_at,
                 hearings_discovered, hearings_processed, hearings_failed, error)
            VALUES (?, ?, ?, ?, ?, NULL, 0, 0, 0, NULL)
            ON CONFLICT(run_id) DO UPDATE
            SET role = excluded.role,
                status = excluded.status,
                args_json = excluded.args_json,
                started_at = excluded.started_at,
                completed_at = NULL,
                hearings_discovered = 0,
                hearings_processed = 0,
                hearings_failed = 0,
                error = NULL
        """, (run_id, role, "running", json.dumps(args), now))
        conn.commit()

    def record_queue_run_finish(
        self,
        run_id: str,
        status: str,
        hearings_discovered: int,
        hearings_processed: int,
        hearings_failed: int,
        error: str | None = None,
    ) -> None:
        """Finalize queue audit row for a run."""
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("""
            UPDATE queue_run_audits
            SET status = ?,
                completed_at = ?,
                hearings_discovered = ?,
                hearings_processed = ?,
                hearings_failed = ?,
                error = ?
            WHERE run_id = ?
        """, (status, now, hearings_discovered, hearings_processed, hearings_failed, error, run_id))
        conn.commit()

    def get_queue_run(self, run_id: str) -> dict | None:
        """Fetch a queue run audit row by run_id."""
        conn = self._get_conn()
        cursor = conn.execute(
            "SELECT * FROM queue_run_audits WHERE run_id = ?",
            (run_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        result = dict(row)
        args_json = result.get("args_json")
        if args_json:
            result["args"] = json.loads(args_json)
        return result

    # ------------------------------------------------------------------
    # Hearing job queue (phase 3 producer/worker cutover)
    # ------------------------------------------------------------------

    def enqueue_hearing_job(
        self,
        hearing_id: str,
        run_id: str,
        committee_key: str,
        hearing_date: str,
        title: str,
    ) -> bool:
        """Enqueue a hearing for worker processing. Returns True if queued."""
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        cursor = conn.execute(
            "SELECT status FROM hearing_jobs WHERE hearing_id = ?",
            (hearing_id,),
        )
        row = cursor.fetchone()
        if row is not None and row["status"] in ("done", "running"):
            return False

        if row is None:
            conn.execute("""
                INSERT INTO hearing_jobs
                    (hearing_id, run_id, committee_key, hearing_date, title,
                     status, available_at, enqueued_at)
                VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
            """, (hearing_id, run_id, committee_key, hearing_date, title, now, now))
        else:
            conn.execute("""
                UPDATE hearing_jobs
                SET run_id = ?,
                    committee_key = ?,
                    hearing_date = ?,
                    title = ?,
                    status = 'pending',
                    available_at = ?,
                    claimed_by = NULL,
                    lease_expires_at = NULL,
                    last_error = NULL
                WHERE hearing_id = ?
            """, (run_id, committee_key, hearing_date, title, now, hearing_id))
        conn.commit()
        return True

    def reclaim_expired_hearing_job_leases(self) -> int:
        """Move expired running hearing jobs back to pending."""
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        cursor = conn.execute("""
            UPDATE hearing_jobs
            SET status = 'pending',
                claimed_by = NULL,
                lease_expires_at = NULL,
                available_at = ?,
                last_error = COALESCE(last_error, 'lease expired')
            WHERE status = 'running'
              AND lease_expires_at IS NOT NULL
              AND lease_expires_at < ?
        """, (now, now))
        conn.commit()
        return cursor.rowcount

    def claim_hearing_jobs(
        self,
        worker_id: str,
        limit: int = 1,
        lease_seconds: int = 900,
    ) -> list[dict]:
        """Claim pending hearing jobs for a worker, returning claimed rows."""
        conn = self._get_conn()
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat()
        lease_until = (now_dt + timedelta(seconds=max(lease_seconds, 1))).isoformat()

        self.reclaim_expired_hearing_job_leases()

        cursor = conn.execute("""
            SELECT hearing_id
            FROM hearing_jobs
            WHERE status = 'pending'
              AND (available_at IS NULL OR available_at <= ?)
            ORDER BY available_at ASC, hearing_date ASC
            LIMIT ?
        """, (now, limit))
        hearing_ids = [row["hearing_id"] for row in cursor.fetchall()]
        if not hearing_ids:
            return []

        claimed: list[dict] = []
        for hearing_id in hearing_ids:
            update = conn.execute("""
                UPDATE hearing_jobs
                SET status = 'running',
                    claimed_by = ?,
                    lease_expires_at = ?,
                    started_at = COALESCE(started_at, ?),
                    attempt_count = attempt_count + 1
                WHERE hearing_id = ?
                  AND status = 'pending'
                  AND (available_at IS NULL OR available_at <= ?)
            """, (worker_id, lease_until, now, hearing_id, now))
            if update.rowcount == 0:
                continue

            row = conn.execute("""
                SELECT hearing_id, run_id, committee_key, hearing_date, title,
                       status, attempt_count, max_attempts, claimed_by, lease_expires_at
                FROM hearing_jobs
                WHERE hearing_id = ?
            """, (hearing_id,)).fetchone()
            if row:
                claimed.append(dict(row))

        conn.commit()
        return claimed

    def complete_hearing_job(self, hearing_id: str) -> None:
        """Mark a claimed hearing job as done."""
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("""
            UPDATE hearing_jobs
            SET status = 'done',
                completed_at = ?,
                claimed_by = NULL,
                lease_expires_at = NULL,
                last_error = NULL
            WHERE hearing_id = ?
        """, (now, hearing_id))
        conn.commit()

    def fail_hearing_job(self, hearing_id: str, error: str, base_delay_seconds: int = 90) -> None:
        """Record hearing job failure and either retry later or mark terminal failure."""
        conn = self._get_conn()
        row = conn.execute("""
            SELECT attempt_count, max_attempts
            FROM hearing_jobs
            WHERE hearing_id = ?
        """, (hearing_id,)).fetchone()
        if row is None:
            return

        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat()
        attempt_count = int(row["attempt_count"] or 0)
        max_attempts = int(row["max_attempts"] or 5)
        if attempt_count >= max_attempts:
            conn.execute("""
                UPDATE hearing_jobs
                SET status = 'failed',
                    completed_at = ?,
                    claimed_by = NULL,
                    lease_expires_at = NULL,
                    last_error = ?
                WHERE hearing_id = ?
            """, (now, error, hearing_id))
        else:
            delay_seconds = base_delay_seconds * (2 ** max(attempt_count - 1, 0))
            delay_seconds = min(delay_seconds, 3600)
            available_at = (now_dt + timedelta(seconds=delay_seconds)).isoformat()
            conn.execute("""
                UPDATE hearing_jobs
                SET status = 'pending',
                    available_at = ?,
                    claimed_by = NULL,
                    lease_expires_at = NULL,
                    last_error = ?
                WHERE hearing_id = ?
            """, (available_at, error, hearing_id))
        conn.commit()

    def get_hearing(self, hearing_id: str) -> dict | None:
        """Fetch hearing metadata by hearing_id."""
        conn = self._get_conn()
        row = conn.execute("""
            SELECT id, committee_key, date, title, slug, sources_json, congress_event_id
            FROM hearings
            WHERE id = ?
        """, (hearing_id,)).fetchone()
        if row is None:
            return None
        result = dict(row)
        sources_json = result.pop("sources_json", "{}")
        result["sources"] = json.loads(sources_json) if sources_json else {}
        return result

    def get_unprocessed_hearings(self) -> list[dict]:
        """Return hearings that haven't been fully processed."""
        conn = self._get_conn()
        cursor = conn.execute("""
            SELECT h.id, h.committee_key, h.date, h.title, h.slug,
                   h.sources_json, h.discovered_at
            FROM hearings h
            WHERE h.processed_at IS NULL
            ORDER BY h.date DESC
        """)

        hearings = []
        for row in cursor.fetchall():
            hearing = dict(row)
            # Parse sources JSON
            hearing['sources'] = json.loads(hearing['sources_json'])
            del hearing['sources_json']
            hearings.append(hearing)

        return hearings

    # ------------------------------------------------------------------
    # C-SPAN search rotation tracking
    # ------------------------------------------------------------------

    def get_cspan_search_age(self, committee_key: str) -> int | None:
        """Days since last C-SPAN search for this committee. None = never searched."""
        conn = self._get_conn()
        cursor = conn.execute(
            "SELECT last_searched FROM cspan_searches WHERE committee_key = ?",
            (committee_key,),
        )
        row = cursor.fetchone()
        if row is None or row["last_searched"] is None:
            return None
        last = _ensure_utc(datetime.fromisoformat(row["last_searched"]))
        now = datetime.now(timezone.utc)
        return (now - last).days

    def record_cspan_search(self, committee_key: str, result_count: int) -> None:
        """Record that a C-SPAN search was done for this committee."""
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("""
            INSERT INTO cspan_searches (committee_key, last_searched, last_result_count)
            VALUES (?, ?, ?)
            ON CONFLICT(committee_key) DO UPDATE
            SET last_searched = excluded.last_searched,
                last_result_count = excluded.last_result_count
        """, (committee_key, now, result_count))
        conn.commit()

    def get_stale_committees(self, max_age_days: int = 3) -> list[str]:
        """Committees not searched in the last N days, ordered oldest first.

        Returns committee keys that either have never been searched or
        were last searched more than max_age_days ago.
        """
        conn = self._get_conn()
        # Get committees that HAVE been searched but are stale
        cursor = conn.execute("""
            SELECT committee_key, last_searched
            FROM cspan_searches
            ORDER BY last_searched ASC
        """)
        stale = []
        now = datetime.now(timezone.utc)
        for row in cursor.fetchall():
            last = _ensure_utc(datetime.fromisoformat(row["last_searched"]))
            age = (now - last).days
            if age >= max_age_days:
                stale.append(row["committee_key"])
        return stale

    # ------------------------------------------------------------------
    # C-SPAN title search tracking (per-hearing, avoids re-searching)
    # ------------------------------------------------------------------

    def is_cspan_searched(self, hearing_id: str) -> bool:
        """Check if we've already done a title search for this hearing."""
        conn = self._get_conn()
        cursor = conn.execute(
            "SELECT hearing_id FROM cspan_title_searches WHERE hearing_id = ?",
            (hearing_id,),
        )
        return cursor.fetchone() is not None

    def record_cspan_title_search(self, hearing_id: str, found: bool) -> None:
        """Record that a C-SPAN title search was done for this hearing."""
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("""
            INSERT INTO cspan_title_searches (hearing_id, searched_at, found)
            VALUES (?, ?, ?)
            ON CONFLICT(hearing_id) DO UPDATE
            SET searched_at = excluded.searched_at,
                found = excluded.found
        """, (hearing_id, now, 1 if found else 0))
        conn.commit()

    # ------------------------------------------------------------------
    # Hearing ID migration
    # ------------------------------------------------------------------

    def merge_hearing_id(self, old_id: str, new_id: str) -> None:
        """Migrate all DB records from old_id to new_id.

        Copies processing_steps and cspan_title_searches to new_id,
        then deletes old_id records from all tables.
        """
        conn = self._get_conn()

        # Copy processing_steps from old to new
        conn.execute("""
            INSERT OR IGNORE INTO processing_steps
                (hearing_id, step, status, started_at, completed_at, error)
            SELECT ?, step, status, started_at, completed_at, error
            FROM processing_steps WHERE hearing_id = ?
        """, (new_id, old_id))

        # Copy cspan_title_searches
        conn.execute("""
            INSERT OR IGNORE INTO cspan_title_searches (hearing_id, searched_at, found)
            SELECT ?, searched_at, found
            FROM cspan_title_searches WHERE hearing_id = ?
        """, (new_id, old_id))

        # Delete old rows
        conn.execute("DELETE FROM processing_steps WHERE hearing_id = ?", (old_id,))
        conn.execute("DELETE FROM cspan_title_searches WHERE hearing_id = ?", (old_id,))
        conn.execute("DELETE FROM hearings WHERE id = ?", (old_id,))

        conn.commit()

    # ------------------------------------------------------------------
    # Digest tracking
    # ------------------------------------------------------------------

    def record_digest_run(self, run_date: str, hearings_scanned: int,
                          quotes_extracted: int, quotes_selected: int,
                          cost_usd: float) -> None:
        """Record a digest run."""
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO digest_runs
                (run_date, hearings_scanned, quotes_extracted, quotes_selected, cost_usd)
            VALUES (?, ?, ?, ?, ?)
        """, (run_date, hearings_scanned, quotes_extracted, quotes_selected, cost_usd))
        conn.commit()

    def last_digest_date(self) -> str | None:
        """Return the most recent digest run date, or None."""
        conn = self._get_conn()
        cursor = conn.execute(
            "SELECT MAX(run_date) as latest FROM digest_runs"
        )
        row = cursor.fetchone()
        return row["latest"] if row and row["latest"] else None

    def get_last_rotation_time(self) -> datetime | None:
        """Return the most recent C-SPAN committee rotation search time, or None."""
        conn = self._get_conn()
        cursor = conn.execute(
            "SELECT MAX(last_searched) as latest FROM cspan_searches"
        )
        row = cursor.fetchone()
        if row is None or row["latest"] is None:
            return None
        return _ensure_utc(datetime.fromisoformat(row["latest"]))
