"""Tests for state.py — SQLite persistence layer."""

import tempfile
from pathlib import Path

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
