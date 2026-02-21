"""Tests for run.py — pipeline step functions and hearing ID reconciliation."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import MagicMock, patch

from discover import Hearing


def _make_hearing(**overrides) -> Hearing:
    """Build a Hearing with sensible defaults; override any field via kwargs."""
    defaults = dict(
        committee_key="house.judiciary",
        committee_name="Judiciary Committee",
        title="Test Hearing on AI Regulation",
        date="2026-02-10",
        sources={},
    )
    defaults.update(overrides)
    return Hearing(**defaults)


def _make_state() -> MagicMock:
    """Return a mock State with commonly-used methods pre-configured."""
    state = MagicMock()
    state.is_step_done.return_value = False
    state.find_by_congress_event_id.return_value = None
    state.find_by_committee_date.return_value = []
    return state


# ---------------------------------------------------------------------------
# _hearing_from_state_row
# ---------------------------------------------------------------------------

class TestHearingFromStateRow:

    def test_builds_hearing_with_committee_name_from_config(self):
        from run import _hearing_from_state_row

        row = {
            "id": "h1",
            "committee_key": "house.judiciary",
            "date": "2026-02-10",
            "title": "AI Hearing",
            "sources": {"youtube_url": "https://youtube.com/watch?v=abc123"},
        }

        with patch("run.config.get_committee_meta", return_value={"name": "House Judiciary"}):
            hearing = _hearing_from_state_row(row)

        assert hearing.committee_name == "House Judiciary"
        assert hearing.committee_key == "house.judiciary"
        assert hearing.date == "2026-02-10"
        assert hearing.title == "AI Hearing"
        assert hearing.sources["youtube_url"] == "https://youtube.com/watch?v=abc123"

    def test_falls_back_to_committee_key_when_meta_missing(self):
        from run import _hearing_from_state_row

        row = {
            "id": "h2",
            "committee_key": "unknown.committee",
            "date": "2026-02-11",
            "title": "Unknown Committee Hearing",
            "sources": {},
        }

        with patch("run.config.get_committee_meta", return_value=None):
            hearing = _hearing_from_state_row(row)

        assert hearing.committee_name == "unknown.committee"


# ---------------------------------------------------------------------------
# _emit_transcript_published_event
# ---------------------------------------------------------------------------

class TestEmitTranscriptPublishedEvent:

    def test_enqueues_outbox_event_when_queue_write_enabled(self, monkeypatch, tmp_path):
        from run import _emit_transcript_published_event

        hearing = _make_hearing(
            committee_key="house.judiciary",
            committee_name="House Judiciary",
            date="2026-02-10",
            sources={"youtube_url": "https://youtube.com/watch?v=abc123"},
        )
        state = MagicMock()
        result = {"cost": {"total_usd": 0.12}}
        monkeypatch.setattr("run.config.QUEUE_WRITE_ENABLED", True)
        monkeypatch.setattr("run.config.TRANSCRIPTS_DIR", tmp_path / "transcripts")

        _emit_transcript_published_event(hearing, state, result)

        state.enqueue_outbox_event.assert_called_once()
        kwargs = state.enqueue_outbox_event.call_args.kwargs
        assert kwargs["event_type"] == "transcript_published"
        assert kwargs["hearing_id"] == hearing.id
        assert kwargs["event_id"].startswith(f"transcript_published:{hearing.id}:v1")
        assert kwargs["payload"]["path"] == f"{hearing.committee_key}/{hearing.date}_{hearing.id}"
        assert kwargs["payload"]["cost"]["total_usd"] == 0.12

    def test_noop_when_queue_write_disabled(self, monkeypatch):
        from run import _emit_transcript_published_event

        hearing = _make_hearing()
        state = MagicMock()
        monkeypatch.setattr("run.config.QUEUE_WRITE_ENABLED", False)

        _emit_transcript_published_event(hearing, state, {"cost": {}})

        state.enqueue_outbox_event.assert_not_called()


# ---------------------------------------------------------------------------
# _reconcile_hearing_id
# ---------------------------------------------------------------------------

class TestReconcileHearingId:

    def test_returns_none_when_no_match(self):
        """No event ID, no committee/date match => no migration."""
        from run import _reconcile_hearing_id

        hearing = _make_hearing()
        state = _make_state()

        result = _reconcile_hearing_id(hearing, state)

        assert result is None
        state.find_by_committee_date.assert_called_once_with(
            hearing.committee_key, hearing.date,
        )
        state.merge_hearing_id.assert_not_called()

    def test_migrates_via_congress_event_id(self):
        """Event ID match with different hearing ID => migrate."""
        from run import _reconcile_hearing_id

        hearing = _make_hearing(sources={"congress_api_event_id": "EVT-123"})
        state = _make_state()
        state.find_by_congress_event_id.return_value = {
            "id": "old_id_abc",
            "title": "Old Title",
        }

        with patch("run._migrate_hearing_id") as mock_migrate:
            result = _reconcile_hearing_id(hearing, state)

        assert result == "old_id_abc"
        state.find_by_congress_event_id.assert_called_once_with("EVT-123")
        mock_migrate.assert_called_once_with("old_id_abc", hearing, state)

    def test_no_migration_when_event_id_matches_same_hearing(self):
        """Event ID found but it already points to the same hearing ID => no migration."""
        from run import _reconcile_hearing_id

        hearing = _make_hearing(sources={"congress_api_event_id": "EVT-123"})
        state = _make_state()
        state.find_by_congress_event_id.return_value = {
            "id": hearing.id,  # same ID
            "title": hearing.title,
        }

        result = _reconcile_hearing_id(hearing, state)

        assert result is None
        state.merge_hearing_id.assert_not_called()

    def test_migrates_via_fuzzy_title_match(self):
        """Committee/date match with similar title => migrate."""
        from run import _reconcile_hearing_id

        hearing = _make_hearing(
            title="Test Hearing on AI Regulation",
            sources={},
        )
        state = _make_state()
        state.find_by_committee_date.return_value = [
            {"id": "old_fuzzy_id", "title": "Test Hearing on AI Regulation (Updated)"},
        ]

        with patch("run._migrate_hearing_id") as mock_migrate, \
             patch("run.title_similarity", return_value=0.85):
            result = _reconcile_hearing_id(hearing, state)

        assert result == "old_fuzzy_id"
        mock_migrate.assert_called_once_with("old_fuzzy_id", hearing, state)

    def test_no_migration_when_titles_too_different(self):
        """Committee/date match but title similarity below threshold => no migration."""
        from run import _reconcile_hearing_id

        hearing = _make_hearing(sources={})
        state = _make_state()
        state.find_by_committee_date.return_value = [
            {"id": "candidate_id", "title": "Completely Unrelated Topic"},
        ]

        with patch("run.title_similarity", return_value=0.10):
            result = _reconcile_hearing_id(hearing, state)

        assert result is None
        state.merge_hearing_id.assert_not_called()


# ---------------------------------------------------------------------------
# _step_youtube_captions
# ---------------------------------------------------------------------------

class TestStepYoutubeCaptions:

    def test_processes_when_youtube_url_present(self, tmp_path):
        """With a YouTube URL and step not done, should call process_hearing_audio."""
        from run import _step_youtube_captions

        hearing = _make_hearing(
            sources={"youtube_url": "https://youtube.com/watch?v=abc123"},
        )
        state = _make_state()
        hearing_dir = tmp_path / "hearing"
        hearing_dir.mkdir()
        result = {"outputs": {}}
        cost = {"llm_cleanup_usd": 0.0, "whisper_usd": 0.0}

        mock_audio_result = {
            "captions": "/path/to/captions.txt",
            "cleaned_transcript": "/path/to/cleaned.txt",
            "cleanup_cost_usd": 0.05,
            "whisper_cost_usd": 0.10,
        }

        with patch("run.process_hearing_audio", return_value=mock_audio_result) as mock_audio:
            _step_youtube_captions(hearing, state, hearing_dir, result, cost)

        mock_audio.assert_called_once_with(
            "https://youtube.com/watch?v=abc123",
            hearing_dir,
            hearing_title=hearing.title,
            committee_name=hearing.committee_name,
        )
        assert result["outputs"]["audio"] == mock_audio_result
        assert cost["llm_cleanup_usd"] == 0.05
        assert cost["whisper_usd"] == 0.10
        state.mark_step.assert_any_call(hearing.id, "captions", "done")
        state.mark_step.assert_any_call(hearing.id, "cleanup", "done")

    def test_skips_when_already_done(self, tmp_path):
        """If captions step is done, should not call process_hearing_audio."""
        from run import _step_youtube_captions

        hearing = _make_hearing(
            sources={"youtube_url": "https://youtube.com/watch?v=abc123"},
        )
        state = _make_state()
        state.is_step_done.return_value = True
        hearing_dir = tmp_path / "hearing"
        hearing_dir.mkdir()
        result = {"outputs": {}}
        cost = {"llm_cleanup_usd": 0.0, "whisper_usd": 0.0}

        with patch("run.process_hearing_audio") as mock_audio:
            _step_youtube_captions(hearing, state, hearing_dir, result, cost)

        mock_audio.assert_not_called()
        assert "audio" not in result["outputs"]

    def test_marks_done_when_no_youtube_url(self, tmp_path):
        """No YouTube URL => mark captions and cleanup as done (intentional skip)."""
        from run import _step_youtube_captions

        hearing = _make_hearing(sources={})
        state = _make_state()
        hearing_dir = tmp_path / "hearing"
        hearing_dir.mkdir()
        result = {"outputs": {}}
        cost = {"llm_cleanup_usd": 0.0, "whisper_usd": 0.0}

        _step_youtube_captions(hearing, state, hearing_dir, result, cost)

        state.mark_step.assert_any_call(hearing.id, "captions", "done")
        state.mark_step.assert_any_call(hearing.id, "cleanup", "done")

    def test_marks_failed_on_error(self, tmp_path):
        """process_hearing_audio raises => mark captions as failed."""
        from run import _step_youtube_captions

        hearing = _make_hearing(
            sources={"youtube_url": "https://youtube.com/watch?v=abc123"},
        )
        state = _make_state()
        hearing_dir = tmp_path / "hearing"
        hearing_dir.mkdir()
        result = {"outputs": {}}
        cost = {"llm_cleanup_usd": 0.0, "whisper_usd": 0.0}

        with patch("run.process_hearing_audio", side_effect=OSError("disk full")):
            _step_youtube_captions(hearing, state, hearing_dir, result, cost)

        state.mark_step.assert_any_call(
            hearing.id, "captions", "failed", error="disk full",
        )


# ---------------------------------------------------------------------------
# _step_govinfo_transcript
# ---------------------------------------------------------------------------

class TestStepGovinfoTranscript:

    def test_fetches_when_govinfo_id_present(self, tmp_path):
        """With a govinfo ID and step not done, should call fetch_govinfo_transcript."""
        from run import _step_govinfo_transcript

        hearing = _make_hearing(
            sources={"govinfo_package_id": "CHRG-119shrg12345"},
        )
        state = _make_state()
        hearing_dir = tmp_path / "hearing"
        hearing_dir.mkdir()
        result = {"outputs": {}}
        cost = {"llm_cleanup_usd": 0.0, "whisper_usd": 0.0}

        fake_path = hearing_dir / "govinfo_transcript.txt"
        fake_path.write_text("transcript text")

        with patch("run.fetch_govinfo_transcript", return_value=fake_path) as mock_fetch:
            _step_govinfo_transcript(hearing, state, hearing_dir, result, cost)

        mock_fetch.assert_called_once_with("CHRG-119shrg12345", hearing_dir)
        assert result["outputs"]["govinfo_transcript"] == str(fake_path)
        state.mark_step.assert_any_call(hearing.id, "govinfo", "done")

    def test_marks_done_when_no_govinfo_id(self, tmp_path):
        """No govinfo package ID => mark govinfo as done (intentional skip)."""
        from run import _step_govinfo_transcript

        hearing = _make_hearing(sources={})
        state = _make_state()
        hearing_dir = tmp_path / "hearing"
        hearing_dir.mkdir()
        result = {"outputs": {}}
        cost = {"llm_cleanup_usd": 0.0, "whisper_usd": 0.0}

        _step_govinfo_transcript(hearing, state, hearing_dir, result, cost)

        state.mark_step.assert_called_once_with(hearing.id, "govinfo", "done")


# ---------------------------------------------------------------------------
# _step_testimony_pdfs
# ---------------------------------------------------------------------------

class TestStepTestimonyPdfs:

    def test_processes_pdfs_when_urls_present(self, tmp_path):
        """With testimony PDF URLs and step not done, should call process_testimony_pdfs."""
        from run import _step_testimony_pdfs

        pdf_urls = ["https://example.com/testimony1.pdf", "https://example.com/testimony2.pdf"]
        hearing = _make_hearing(sources={"testimony_pdf_urls": pdf_urls})
        state = _make_state()
        hearing_dir = tmp_path / "hearing"
        hearing_dir.mkdir()
        result = {"outputs": {}}
        cost = {"llm_cleanup_usd": 0.0, "whisper_usd": 0.0}

        mock_pdf_results = [{"path": "/p1.txt"}, {"path": "/p2.txt"}]

        with patch("run.process_testimony_pdfs", return_value=mock_pdf_results) as mock_pdfs:
            _step_testimony_pdfs(hearing, state, hearing_dir, result, cost)

        mock_pdfs.assert_called_once_with(pdf_urls, hearing_dir)
        assert result["outputs"]["testimony"] == mock_pdf_results
        state.mark_step.assert_any_call(hearing.id, "testimony", "done")

    def test_marks_done_when_no_pdfs(self, tmp_path):
        """No PDF URLs => mark testimony as done (intentional skip)."""
        from run import _step_testimony_pdfs

        hearing = _make_hearing(sources={})
        state = _make_state()
        hearing_dir = tmp_path / "hearing"
        hearing_dir.mkdir()
        result = {"outputs": {}}
        cost = {"llm_cleanup_usd": 0.0, "whisper_usd": 0.0}

        _step_testimony_pdfs(hearing, state, hearing_dir, result, cost)

        state.mark_step.assert_called_once_with(hearing.id, "testimony", "done")
