from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).parent


class State:
    """SQLite persistence layer for congressional hearing transcript pipeline."""

    def __init__(self, db_path: Path | None = None):
        if db_path is None:
            db_path = ROOT / "data" / "state.db"

        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Initialize database with tables
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        """Get a database connection."""
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        """Create tables if they don't exist."""
        conn = self._get_conn()
        try:
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

            conn.commit()
        finally:
            conn.close()

    def is_processed(self, hearing_id: str) -> bool:
        """Check if hearing has been marked as fully processed."""
        conn = self._get_conn()
        try:
            cursor = conn.execute(
                "SELECT processed_at FROM hearings WHERE id = ?", (hearing_id,)
            )
            row = cursor.fetchone()
            return row is not None and row['processed_at'] is not None
        finally:
            conn.close()

    def mark_processed(self, hearing_id: str) -> None:
        """Mark a hearing as fully processed."""
        conn = self._get_conn()
        try:
            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                "UPDATE hearings SET processed_at = ? WHERE id = ?",
                (now, hearing_id),
            )
            conn.commit()
        finally:
            conn.close()

    def is_step_done(self, hearing_id: str, step: str) -> bool:
        """Check if a specific step is done for a hearing."""
        conn = self._get_conn()
        try:
            cursor = conn.execute("""
                SELECT status
                FROM processing_steps
                WHERE hearing_id = ? AND step = ?
            """, (hearing_id, step))
            row = cursor.fetchone()
            return row is not None and row['status'] == 'done'
        finally:
            conn.close()

    def record_hearing(self, hearing_id: str, committee_key: str, date: str,
                      title: str, slug: str, sources: dict) -> None:
        """Insert or update a hearing record."""
        conn = self._get_conn()
        try:
            sources_json = json.dumps(sources)
            now = datetime.now(timezone.utc).isoformat()

            # Check if hearing already exists
            cursor = conn.execute("SELECT id FROM hearings WHERE id = ?", (hearing_id,))
            exists = cursor.fetchone() is not None

            if exists:
                conn.execute("""
                    UPDATE hearings
                    SET committee_key = ?, date = ?, title = ?, slug = ?, sources_json = ?
                    WHERE id = ?
                """, (committee_key, date, title, slug, sources_json, hearing_id))
            else:
                conn.execute("""
                    INSERT INTO hearings (id, committee_key, date, title, slug, sources_json, discovered_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (hearing_id, committee_key, date, title, slug, sources_json, now))

            conn.commit()
        finally:
            conn.close()

    def mark_step(self, hearing_id: str, step: str, status: str,
                  error: str | None = None) -> None:
        """Update processing step status. Sets timestamps automatically."""
        conn = self._get_conn()
        try:
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
        finally:
            conn.close()

    def record_scraper_run(self, committee_key: str, source_type: str,
                          count: int, error: str | None = None) -> None:
        """Log a scraper run result for health monitoring."""
        conn = self._get_conn()
        try:
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
        finally:
            conn.close()

    def get_failing_scrapers(self, threshold: int = 3) -> list[dict]:
        """Return scrapers with consecutive_failures >= threshold."""
        conn = self._get_conn()
        try:
            cursor = conn.execute("""
                SELECT committee_key, source_type, last_success, last_failure,
                       last_count, consecutive_failures
                FROM scraper_health
                WHERE consecutive_failures >= ?
                ORDER BY consecutive_failures DESC
            """, (threshold,))

            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    def get_unprocessed_hearings(self) -> list[dict]:
        """Return hearings that haven't been fully processed."""
        conn = self._get_conn()
        try:
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
        finally:
            conn.close()
