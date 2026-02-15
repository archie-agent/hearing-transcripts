from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
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

    def __init__(self, db_path: Path | None = None):
        if db_path is None:
            import config
            db_path = config.DATA_DIR / "state.db"

        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()

        # Initialize database with tables
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        """Get a thread-local database connection (created once per thread, reused)."""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path)
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

        # Migration: add congress_event_id for cross-run identity matching
        cursor = conn.execute("PRAGMA table_info(hearings)")
        existing_cols = {row["name"] for row in cursor.fetchall()}
        if "congress_event_id" not in existing_cols:
            conn.execute("ALTER TABLE hearings ADD COLUMN congress_event_id TEXT")
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_hearings_congress_event_id
            ON hearings(congress_event_id) WHERE congress_event_id IS NOT NULL
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
