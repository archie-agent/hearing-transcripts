"""Tests for llm_utils.py — token estimation, chunking, cost calculation, API calls."""

from unittest.mock import MagicMock, patch

import httpx
import pytest

import config
from llm_utils import (
    OPENROUTER_API_URL,
    calculate_cost,
    call_openrouter,
    estimate_tokens,
    get_api_key,
    split_into_chunks,
)


class TestEstimateTokens:
    def test_basic_calculation(self):
        # 1 token ~ 4 characters
        assert estimate_tokens("abcd") == 1

    def test_empty_string(self):
        assert estimate_tokens("") == 0

    def test_rounds_down(self):
        # 7 chars -> 7 // 4 = 1
        assert estimate_tokens("abcdefg") == 1

    def test_longer_string(self):
        text = "a" * 400
        assert estimate_tokens(text) == 100


class TestSplitIntoChunks:
    def test_short_text_returns_single_chunk(self):
        text = "Hello world."
        chunks = split_into_chunks(text, chunk_size=100, overlap=10)
        assert len(chunks) == 1
        assert chunks[0] == text

    def test_empty_string_returns_single_chunk(self):
        chunks = split_into_chunks("", chunk_size=100, overlap=10)
        assert len(chunks) == 1
        assert chunks[0] == ""

    def test_splits_on_paragraph_boundaries(self):
        # Each paragraph is 40 chars = 10 tokens.
        # chunk_size=25 tokens means ~2-3 paragraphs per chunk before splitting.
        para = "a" * 40  # 10 tokens
        text = "\n\n".join([para] * 6)  # 6 paragraphs, 60 tokens total
        chunks = split_into_chunks(text, chunk_size=25, overlap=0)
        assert len(chunks) > 1
        # Every chunk boundary should be a paragraph boundary (no mid-paragraph splits)
        for chunk in chunks:
            assert "a" * 40 in chunk

    def test_overlap_preserves_context(self):
        # 5 distinct paragraphs, each 10 tokens (40 chars)
        paras = [f"{'abcdefghij' * 4}" for _ in range(5)]
        # Make paragraphs distinguishable
        paras = [f"para{i}_" + "x" * 34 for i in range(5)]
        text = "\n\n".join(paras)
        chunks = split_into_chunks(text, chunk_size=25, overlap=12)
        assert len(chunks) >= 2
        # With overlap, the last paragraph(s) of chunk N should appear at the
        # start of chunk N+1
        if len(chunks) >= 2:
            # The overlap paragraph from the end of chunk 0 should appear in chunk 1
            last_para_of_first = chunks[0].split("\n\n")[-1]
            assert last_para_of_first in chunks[1]

    def test_single_paragraph_returns_one_chunk(self):
        text = "No paragraph breaks here, just one long string of text."
        chunks = split_into_chunks(text, chunk_size=5, overlap=2)
        assert len(chunks) == 1
        assert chunks[0] == text


class TestCalculateCost:
    def test_known_model(self):
        # google/gemini-2.0-flash-001: input=0.10, output=0.40 per 1M tokens
        cost = calculate_cost("google/gemini-2.0-flash-001", 1_000_000, 1_000_000)
        assert cost == pytest.approx(0.10 + 0.40)

    def test_zero_tokens(self):
        cost = calculate_cost("google/gemini-2.0-flash-001", 0, 0)
        assert cost == 0.0

    def test_partial_tokens(self):
        # 500k input, 200k output with gemini flash
        cost = calculate_cost("google/gemini-2.0-flash-001", 500_000, 200_000)
        expected = (500_000 / 1_000_000) * 0.10 + (200_000 / 1_000_000) * 0.40
        assert cost == pytest.approx(expected)

    def test_unknown_model_raises(self):
        with pytest.raises(ValueError, match="Unknown model"):
            calculate_cost("nonexistent/model", 100, 100)

    def test_all_configured_models_accepted(self):
        for model in config.MODEL_PRICING:
            # Should not raise
            cost = calculate_cost(model, 1000, 1000)
            assert cost >= 0


class TestGetApiKey:
    def test_returns_key_when_set(self, monkeypatch):
        monkeypatch.setenv("OPENROUTER_API_KEY", "sk-test-key-123")
        assert get_api_key() == "sk-test-key-123"

    def test_raises_when_empty(self, monkeypatch):
        monkeypatch.setenv("OPENROUTER_API_KEY", "")
        with pytest.raises(ValueError, match="OPENROUTER_API_KEY not found"):
            get_api_key()

    def test_raises_when_unset(self, monkeypatch):
        monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
        with pytest.raises(ValueError, match="OPENROUTER_API_KEY not found"):
            get_api_key()


class TestCallOpenrouter:
    def _make_mock_response(self, json_data: dict) -> MagicMock:
        resp = MagicMock(spec=httpx.Response)
        resp.json.return_value = json_data
        resp.raise_for_status.return_value = None
        return resp

    def test_with_provided_client(self):
        expected = {"choices": [{"message": {"content": "hello"}}]}
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.post.return_value = self._make_mock_response(expected)

        result = call_openrouter(
            prompt="Say hello",
            model="google/gemini-2.0-flash-001",
            api_key="sk-test",
            client=mock_client,
        )

        assert result == expected
        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        assert call_args[0][0] == OPENROUTER_API_URL
        payload = call_args[1]["json"]
        assert payload["model"] == "google/gemini-2.0-flash-001"
        assert payload["messages"][0]["role"] == "user"
        assert payload["messages"][0]["content"] == "Say hello"
        headers = call_args[1]["headers"]
        assert headers["Authorization"] == "Bearer sk-test"

    @patch("llm_utils.httpx.Client")
    def test_without_client_creates_one(self, MockClientClass):
        expected = {"choices": [{"message": {"content": "world"}}]}
        mock_client_instance = MagicMock()
        mock_client_instance.post.return_value = self._make_mock_response(expected)
        MockClientClass.return_value.__enter__ = MagicMock(
            return_value=mock_client_instance
        )
        MockClientClass.return_value.__exit__ = MagicMock(return_value=False)

        result = call_openrouter(
            prompt="Say world",
            model="openai/gpt-4o-mini",
            api_key="sk-test-2",
            timeout=60.0,
        )

        assert result == expected
        MockClientClass.assert_called_once_with(timeout=60.0)
        mock_client_instance.post.assert_called_once()
        payload = mock_client_instance.post.call_args[1]["json"]
        assert payload["model"] == "openai/gpt-4o-mini"

    def test_provided_client_raise_for_status_called(self):
        mock_resp = self._make_mock_response({"ok": True})
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.post.return_value = mock_resp

        call_openrouter(
            prompt="test",
            model="test-model",
            api_key="sk-key",
            client=mock_client,
        )

        mock_resp.raise_for_status.assert_called_once()
