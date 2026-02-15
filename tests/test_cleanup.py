"""Tests for cleanup.py — LLM-based transcript cleanup and diarization."""

from cleanup import CleanupResult, _build_cleanup_prompt, _build_diarization_prompt, cleanup_transcript


class TestBuildDiarizationPrompt:
    """Tests for _build_diarization_prompt."""

    def test_includes_hearing_title(self):
        prompt = _build_diarization_prompt("some text", hearing_title="Budget Hearing 2026")
        assert "Hearing: Budget Hearing 2026" in prompt

    def test_includes_committee_name(self):
        prompt = _build_diarization_prompt("some text", committee_name="Senate Finance")
        assert "Committee: Senate Finance" in prompt

    def test_includes_both_title_and_committee(self):
        prompt = _build_diarization_prompt(
            "some text",
            hearing_title="AI Regulation",
            committee_name="House Judiciary",
        )
        assert "Hearing: AI Regulation" in prompt
        assert "Committee: House Judiciary" in prompt

    def test_default_context_when_no_metadata(self):
        prompt = _build_diarization_prompt("some text")
        assert "Congressional hearing" in prompt

    def test_includes_raw_text(self):
        prompt = _build_diarization_prompt("the quick brown fox")
        assert "the quick brown fox" in prompt

    def test_chunk_info_single_chunk(self):
        prompt = _build_diarization_prompt("text", chunk_index=0, total_chunks=1)
        assert "chunk" not in prompt.lower() or "chunk_index" not in prompt

    def test_chunk_info_multi_chunk(self):
        prompt = _build_diarization_prompt("text", chunk_index=2, total_chunks=5)
        assert "chunk 3 of 5" in prompt


class TestBuildCleanupPrompt:
    """Tests for _build_cleanup_prompt (skip-diarization mode)."""

    def test_includes_hearing_title(self):
        prompt = _build_cleanup_prompt("some text", hearing_title="Oversight Hearing")
        assert "Hearing: Oversight Hearing" in prompt

    def test_includes_committee_name(self):
        prompt = _build_cleanup_prompt("some text", committee_name="House Ways and Means")
        assert "Committee: House Ways and Means" in prompt

    def test_preserves_speaker_labels_instruction(self):
        prompt = _build_cleanup_prompt("text")
        assert "PRESERVE all existing speaker labels" in prompt

    def test_no_add_speaker_labels_instruction(self):
        """Cleanup-only prompt should NOT instruct adding new speaker labels."""
        prompt = _build_cleanup_prompt("text")
        assert "Add speaker labels" not in prompt

    def test_chunk_info_multi_chunk(self):
        prompt = _build_cleanup_prompt("text", chunk_index=0, total_chunks=3)
        assert "chunk 1 of 3" in prompt


class TestCleanupTranscript:
    """Tests for cleanup_transcript with mocked LLM calls."""

    def _mock_openrouter_response(self, content: str) -> dict:
        return {
            "choices": [{"message": {"content": content}}],
            "usage": {"prompt_tokens": 100, "completion_tokens": 50},
        }

    def test_returns_cleanup_result(self, monkeypatch):
        monkeypatch.setattr("cleanup.get_api_key", lambda: "fake-key")
        monkeypatch.setattr(
            "cleanup.call_openrouter",
            lambda prompt, model, api_key: self._mock_openrouter_response(
                "Cleaned text here."
            ),
        )
        monkeypatch.setattr(
            "cleanup.calculate_cost", lambda model, inp, out: 0.0042
        )

        result = cleanup_transcript(
            "raw messy text",
            hearing_title="Test Hearing",
            model="google/gemini-2.0-flash-001",
        )

        assert isinstance(result, CleanupResult)
        assert result.text == "Cleaned text here."
        assert result.cost_usd == 0.0042
        assert result.input_tokens == 100
        assert result.output_tokens == 50
        assert result.chunks_processed == 1
        assert result.model == "google/gemini-2.0-flash-001"

    def test_multi_chunk_processing(self, monkeypatch):
        monkeypatch.setattr("cleanup.get_api_key", lambda: "fake-key")

        call_count = 0

        def mock_call(prompt, model, api_key):
            nonlocal call_count
            call_count += 1
            return self._mock_openrouter_response(f"Chunk {call_count}")

        monkeypatch.setattr("cleanup.call_openrouter", mock_call)
        monkeypatch.setattr(
            "cleanup.calculate_cost", lambda model, inp, out: 0.01
        )
        # Force chunking by making estimate_tokens return a large number
        monkeypatch.setattr("cleanup.estimate_tokens", lambda text: 99999)
        monkeypatch.setattr(
            "cleanup.split_into_chunks",
            lambda text: ["chunk a", "chunk b"],
        )

        result = cleanup_transcript("long text", model="google/gemini-2.0-flash-001")

        assert result.chunks_processed == 2
        assert "Chunk 1" in result.text
        assert "Chunk 2" in result.text
        assert result.input_tokens == 200
        assert result.output_tokens == 100

    def test_skip_diarization_uses_cleanup_prompt(self, monkeypatch):
        monkeypatch.setattr("cleanup.get_api_key", lambda: "fake-key")

        captured_prompts = []

        def mock_call(prompt, model, api_key):
            captured_prompts.append(prompt)
            return self._mock_openrouter_response("cleaned")

        monkeypatch.setattr("cleanup.call_openrouter", mock_call)
        monkeypatch.setattr(
            "cleanup.calculate_cost", lambda model, inp, out: 0.0
        )

        cleanup_transcript(
            "text", model="google/gemini-2.0-flash-001", skip_diarization=True
        )

        assert len(captured_prompts) == 1
        # The cleanup-only prompt has this distinctive phrase
        assert "PRESERVE all existing speaker labels" in captured_prompts[0]

    def test_diarization_mode_uses_diarization_prompt(self, monkeypatch):
        monkeypatch.setattr("cleanup.get_api_key", lambda: "fake-key")

        captured_prompts = []

        def mock_call(prompt, model, api_key):
            captured_prompts.append(prompt)
            return self._mock_openrouter_response("cleaned")

        monkeypatch.setattr("cleanup.call_openrouter", mock_call)
        monkeypatch.setattr(
            "cleanup.calculate_cost", lambda model, inp, out: 0.0
        )

        cleanup_transcript(
            "text", model="google/gemini-2.0-flash-001", skip_diarization=False
        )

        assert len(captured_prompts) == 1
        # The diarization prompt has this distinctive phrase
        assert "Add speaker labels in brackets" in captured_prompts[0]
