"""Tests for state.py — SQLite persistence layer."""

import sqlite3
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch

from state import State


class TestState:
    def _make_state(self, tmp_path: Path) -> State:
        return State(db_path=tmp_path / "test.db")

    def test_record_and_lookup(self, tmp_path):
        st = self._make_state(tmp_path)
        st.record_hearing("abc123", "house.judiciary", "2026-02-10",
                          "AI Regulation Hearing", "house-judiciary-ai-regulation",
                          {"youtube_url": "https://youtube.com/watch?v=test"})
        # Not processed yet
        assert not st.is_processed("abc123")

    def test_mark_processed(self, tmp_path):
        st = self._make_state(tmp_path)
        st.record_hearing("abc123", "house.judiciary", "2026-02-10",
                          "AI Regulation Hearing", "house-judiciary-ai-regulation", {})
        st.mark_processed("abc123")
        assert st.is_processed("abc123")

    def test_step_tracking(self, tmp_path):
        st = self._make_state(tmp_path)
        st.record_hearing("h1", "senate.finance", "2026-01-15", "Budget", "slug", {})
        assert not st.is_step_done("h1", "captions")
        st.mark_step("h1", "captions", "running")
        assert not st.is_step_done("h1", "captions")
        st.mark_step("h1", "captions", "done")
        assert st.is_step_done("h1", "captions")

    def test_step_failure(self, tmp_path):
        st = self._make_state(tmp_path)
        st.record_hearing("h1", "senate.finance", "2026-01-15", "Budget", "slug", {})
        st.mark_step("h1", "captions", "failed", error="timeout")
        assert not st.is_step_done("h1", "captions")

    def test_unprocessed_hearings(self, tmp_path):
        st = self._make_state(tmp_path)
        st.record_hearing("h1", "house.judiciary", "2026-02-10", "Hearing 1", "slug1", {})
        st.record_hearing("h2", "senate.finance", "2026-02-11", "Hearing 2", "slug2", {})
        st.mark_processed("h1")
        unprocessed = st.get_unprocessed_hearings()
        assert len(unprocessed) == 1
        assert unprocessed[0]["id"] == "h2"

    def test_scraper_health_success(self, tmp_path):
        st = self._make_state(tmp_path)
        st.record_scraper_run("house.judiciary", "youtube", 5)
        failing = st.get_failing_scrapers(threshold=1)
        assert len(failing) == 0

    def test_scraper_health_failure(self, tmp_path):
        st = self._make_state(tmp_path)
        st.record_scraper_run("house.judiciary", "youtube", 0, error="timeout")
        st.record_scraper_run("house.judiciary", "youtube", 0, error="timeout")
        st.record_scraper_run("house.judiciary", "youtube", 0, error="timeout")
        failing = st.get_failing_scrapers(threshold=3)
        assert len(failing) == 1
        assert failing[0]["consecutive_failures"] == 3

    def test_scraper_health_reset_on_success(self, tmp_path):
        st = self._make_state(tmp_path)
        st.record_scraper_run("house.judiciary", "youtube", 0, error="fail")
        st.record_scraper_run("house.judiciary", "youtube", 0, error="fail")
        st.record_scraper_run("house.judiciary", "youtube", 3)  # success resets
        failing = st.get_failing_scrapers(threshold=1)
        assert len(failing) == 0

    def test_upsert_hearing(self, tmp_path):
        st = self._make_state(tmp_path)
        st.record_hearing("h1", "house.judiciary", "2026-02-10", "Title v1", "slug", {"yt": "a"})
        st.record_hearing("h1", "house.judiciary", "2026-02-10", "Title v2", "slug", {"yt": "a", "web": "b"})
        unprocessed = st.get_unprocessed_hearings()
        assert len(unprocessed) == 1
        assert unprocessed[0]["title"] == "Title v2"


class TestStateContextManager:
    """Test __enter__/__exit__ (with statement)."""

    def test_context_manager_returns_self(self, tmp_path):
        with State(db_path=tmp_path / "test.db") as st:
            assert isinstance(st, State)
            # Verify the connection is usable inside the block
            st.record_hearing("h1", "house.judiciary", "2026-02-10",
                              "Hearing", "slug", {})
            assert not st.is_processed("h1")

    def test_context_manager_closes_connection(self, tmp_path):
        with State(db_path=tmp_path / "test.db") as st:
            # Force connection creation by using it
            st.record_hearing("h1", "house.judiciary", "2026-02-10",
                              "Hearing", "slug", {})
        # After exiting, the thread-local conn should be None
        assert getattr(st._local, "conn", None) is None


class TestInitDbCaching:
    """Test _initialized_dbs class-level cache."""

    def test_second_instantiation_skips_init_db(self, tmp_path):
        db_path = tmp_path / "test.db"
        db_key = str(db_path.resolve())

        # Clean slate: remove this key if it happens to be cached
        State._initialized_dbs.discard(db_key)
        try:
            st1 = State(db_path=db_path)
            # After first instantiation, the key should be in the cache
            assert db_key in State._initialized_dbs

            # Patch _init_db to track whether it gets called
            with patch.object(State, "_init_db") as mock_init:
                st2 = State(db_path=db_path)
                mock_init.assert_not_called()

            # Both instances should be functional
            st1.record_hearing("h1", "house.judiciary", "2026-02-10",
                               "Hearing", "slug", {})
            assert not st2.is_processed("h1")
        finally:
            # Clean up the cache to avoid polluting other tests
            State._initialized_dbs.discard(db_key)


class TestMergeHearingId:
    """Test merge_hearing_id(old_id, new_id)."""

    def test_merge_moves_steps_and_searches(self, tmp_path):
        st = State(db_path=tmp_path / "test.db")

        # Set up old hearing with steps and cspan title search
        st.record_hearing("old-id", "house.judiciary", "2026-02-10",
                          "Old Hearing", "slug", {})
        st.mark_step("old-id", "captions", "done")
        st.mark_step("old-id", "transcript", "done")
        st.record_cspan_title_search("old-id", found=True)

        # Set up new hearing
        st.record_hearing("new-id", "house.judiciary", "2026-02-10",
                          "New Hearing", "slug", {})

        # Merge
        st.merge_hearing_id("old-id", "new-id")

        # Old hearing record should be deleted
        conn = st._get_conn()
        cursor = conn.execute("SELECT id FROM hearings WHERE id = ?", ("old-id",))
        assert cursor.fetchone() is None

        # Processing steps should be under new_id
        assert st.is_step_done("new-id", "captions")
        assert st.is_step_done("new-id", "transcript")

        # Old steps should be gone
        assert not st.is_step_done("old-id", "captions")

        # C-SPAN title search should be under new_id
        assert st.is_cspan_searched("new-id")
        assert not st.is_cspan_searched("old-id")


class TestCspanSearchTracking:
    """Test C-SPAN search rotation tracking methods."""

    def test_record_and_get_search_age(self, tmp_path):
        st = State(db_path=tmp_path / "test.db")
        st.record_cspan_search("house.judiciary", 5)
        age = st.get_cspan_search_age("house.judiciary")
        assert age == 0  # Just recorded, should be 0 days

    def test_search_age_never_searched(self, tmp_path):
        st = State(db_path=tmp_path / "test.db")
        age = st.get_cspan_search_age("house.judiciary")
        assert age is None

    def test_record_and_check_title_search(self, tmp_path):
        st = State(db_path=tmp_path / "test.db")
        assert not st.is_cspan_searched("h1")
        st.record_cspan_title_search("h1", found=True)
        assert st.is_cspan_searched("h1")

    def test_title_search_not_found(self, tmp_path):
        st = State(db_path=tmp_path / "test.db")
        st.record_cspan_title_search("h1", found=False)
        assert st.is_cspan_searched("h1")

    def test_get_stale_committees(self, tmp_path):
        st = State(db_path=tmp_path / "test.db")

        # Insert a search record with a timestamp 5 days in the past
        conn = st._get_conn()
        old_time = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
        conn.execute("""
            INSERT INTO cspan_searches (committee_key, last_searched, last_result_count)
            VALUES (?, ?, ?)
        """, ("house.judiciary", old_time, 3))
        conn.commit()

        stale = st.get_stale_committees(max_age_days=3)
        assert "house.judiciary" in stale

    def test_get_stale_committees_excludes_recent(self, tmp_path):
        st = State(db_path=tmp_path / "test.db")
        # A just-recorded search should NOT be stale
        st.record_cspan_search("house.judiciary", 5)
        stale = st.get_stale_committees(max_age_days=3)
        assert "house.judiciary" not in stale


class TestFindByCongressEventId:
    """Test find_by_congress_event_id lookups."""

    def test_find_existing_event_id(self, tmp_path):
        st = State(db_path=tmp_path / "test.db")
        st.record_hearing("h1", "house.judiciary", "2026-02-10",
                          "AI Hearing", "slug",
                          {"congress_api_event_id": "evt42"})
        result = st.find_by_congress_event_id("evt42")
        assert result is not None
        assert result["id"] == "h1"
        assert result["committee_key"] == "house.judiciary"
        assert result["title"] == "AI Hearing"

    def test_find_nonexistent_event_id(self, tmp_path):
        st = State(db_path=tmp_path / "test.db")
        result = st.find_by_congress_event_id("nonexistent")
        assert result is None


class TestFindByCommitteeDate:
    """Test find_by_committee_date lookups."""

    def test_find_multiple_hearings_same_committee_date(self, tmp_path):
        st = State(db_path=tmp_path / "test.db")
        st.record_hearing("h1", "house.judiciary", "2026-02-10",
                          "Morning Session", "slug1",
                          {"youtube_url": "https://yt.com/1"})
        st.record_hearing("h2", "house.judiciary", "2026-02-10",
                          "Afternoon Session", "slug2",
                          {"youtube_url": "https://yt.com/2"})

        results = st.find_by_committee_date("house.judiciary", "2026-02-10")
        assert len(results) == 2
        ids = {r["id"] for r in results}
        assert ids == {"h1", "h2"}

    def test_returned_dicts_include_parsed_sources(self, tmp_path):
        st = State(db_path=tmp_path / "test.db")
        st.record_hearing("h1", "house.judiciary", "2026-02-10",
                          "Hearing", "slug",
                          {"youtube_url": "https://yt.com/1", "web": "http://example.com"})

        results = st.find_by_committee_date("house.judiciary", "2026-02-10")
        assert len(results) == 1
        assert "sources" in results[0]
        assert results[0]["sources"]["youtube_url"] == "https://yt.com/1"
        assert results[0]["sources"]["web"] == "http://example.com"

    def test_find_by_committee_date_no_match(self, tmp_path):
        st = State(db_path=tmp_path / "test.db")
        results = st.find_by_committee_date("house.judiciary", "2099-01-01")
        assert results == []


class TestDigestTracking:
    """Test digest run recording and retrieval."""

    def test_record_and_retrieve_digest(self, tmp_path):
        st = State(db_path=tmp_path / "test.db")
        st.record_digest_run("2026-02-10", hearings_scanned=5,
                             quotes_extracted=20, quotes_selected=3,
                             cost_usd=0.50)
        assert st.last_digest_date() == "2026-02-10"

    def test_latest_date_returned(self, tmp_path):
        st = State(db_path=tmp_path / "test.db")
        st.record_digest_run("2026-02-08", hearings_scanned=3,
                             quotes_extracted=10, quotes_selected=2,
                             cost_usd=0.30)
        st.record_digest_run("2026-02-10", hearings_scanned=5,
                             quotes_extracted=20, quotes_selected=3,
                             cost_usd=0.50)
        st.record_digest_run("2026-02-09", hearings_scanned=4,
                             quotes_extracted=15, quotes_selected=2,
                             cost_usd=0.40)
        # MAX(run_date) should be the latest date string
        assert st.last_digest_date() == "2026-02-10"

    def test_last_digest_date_empty(self, tmp_path):
        st = State(db_path=tmp_path / "test.db")
        assert st.last_digest_date() is None


class TestQueueScaffolding:
    def test_queue_tables_exist(self, tmp_path):
        st = State(db_path=tmp_path / "test.db")
        conn = st._get_conn()
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        names = {r["name"] for r in rows}
        expected = {
            "queue_run_audits",
            "discovery_jobs",
            "hearing_jobs",
            "stage_tasks",
            "delivery_outbox_items",
            "dead_letter_items",
        }
        assert expected.issubset(names)

    def test_queue_run_audit_lifecycle(self, tmp_path):
        st = State(db_path=tmp_path / "test.db")
        run_id = "2026-02-21T120000"
        st.record_queue_run_start(run_id, role="monolith", args={"days": 1})
        st.record_queue_run_finish(
            run_id,
            status="completed",
            hearings_discovered=7,
            hearings_processed=3,
            hearings_failed=1,
        )
        row = st.get_queue_run(run_id)
        assert row is not None
        assert row["status"] == "completed"
        assert row["hearings_discovered"] == 7
        assert row["hearings_processed"] == 3
        assert row["hearings_failed"] == 1
        assert row["role"] == "monolith"
        assert row["args"]["days"] == 1
