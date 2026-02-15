"""Tests for digest.py — hearing transcript digest pipeline."""

import json
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import MagicMock

import httpx
import pytest

from digest import (
    Quote,
    _markdown_to_simple_html,
    compose_digest,
    find_recent_transcripts,
    score_quotes,
)


class TestFindRecentTranscripts:
    """Tests for find_recent_transcripts with mocked filesystem."""

    def _setup_transcripts_dir(self, tmp_path: Path, hearings: list[dict]) -> Path:
        """Create a fake transcripts directory with index.json and transcript files."""
        transcripts_dir = tmp_path / "transcripts"
        transcripts_dir.mkdir()

        index = {"hearings": hearings}
        (transcripts_dir / "index.json").write_text(
            json.dumps(index), encoding="utf-8"
        )

        for h in hearings:
            hearing_dir = transcripts_dir / h["path"]
            hearing_dir.mkdir(parents=True, exist_ok=True)
            (hearing_dir / "transcript.txt").write_text(
                "Some transcript content", encoding="utf-8"
            )
            (hearing_dir / "meta.json").write_text(
                json.dumps({"sources": {"youtube_url": "https://youtube.com/watch?v=abc"}}),
                encoding="utf-8",
            )

        return transcripts_dir

    def test_finds_recent_transcripts(self, tmp_path, monkeypatch):
        hearings = [
            {
                "id": "h1",
                "title": "Recent Hearing",
                "committee": "house.judiciary",
                "date": "2026-02-14",
                "path": "house-judiciary/h1",
            },
        ]
        transcripts_dir = self._setup_transcripts_dir(tmp_path, hearings)
        monkeypatch.setattr("config.TRANSCRIPTS_DIR", transcripts_dir)

        result = find_recent_transcripts(lookback_days=7)

        assert len(result) == 1
        assert result[0]["id"] == "h1"
        assert result[0]["title"] == "Recent Hearing"
        assert result[0]["committee"] == "house.judiciary"

    def test_excludes_old_transcripts(self, tmp_path, monkeypatch):
        hearings = [
            {
                "id": "old",
                "title": "Old Hearing",
                "committee": "senate.finance",
                "date": "2020-01-01",
                "path": "senate-finance/old",
            },
        ]
        transcripts_dir = self._setup_transcripts_dir(tmp_path, hearings)
        monkeypatch.setattr("config.TRANSCRIPTS_DIR", transcripts_dir)

        result = find_recent_transcripts(lookback_days=7)

        assert len(result) == 0

    def test_skips_missing_transcript_file(self, tmp_path, monkeypatch):
        transcripts_dir = tmp_path / "transcripts"
        transcripts_dir.mkdir()

        hearings = [
            {
                "id": "no-file",
                "title": "Missing File",
                "committee": "house.judiciary",
                "date": "2026-02-14",
                "path": "house-judiciary/no-file",
            },
        ]
        index = {"hearings": hearings}
        (transcripts_dir / "index.json").write_text(
            json.dumps(index), encoding="utf-8"
        )
        # Create directory but NOT transcript.txt
        (transcripts_dir / "house-judiciary" / "no-file").mkdir(parents=True)

        monkeypatch.setattr("config.TRANSCRIPTS_DIR", transcripts_dir)

        result = find_recent_transcripts(lookback_days=7)

        assert len(result) == 0

    def test_returns_empty_when_no_index(self, tmp_path, monkeypatch):
        transcripts_dir = tmp_path / "transcripts"
        transcripts_dir.mkdir()
        # No index.json at all
        monkeypatch.setattr("config.TRANSCRIPTS_DIR", transcripts_dir)

        result = find_recent_transcripts(lookback_days=7)

        assert result == []

    def test_deduplicates_by_hearing_id(self, tmp_path, monkeypatch):
        hearings = [
            {
                "id": "dup",
                "title": "Duplicate Hearing",
                "committee": "house.judiciary",
                "date": "2026-02-14",
                "path": "house-judiciary/dup",
            },
            {
                "id": "dup",
                "title": "Duplicate Hearing (copy)",
                "committee": "house.judiciary",
                "date": "2026-02-14",
                "path": "house-judiciary/dup",
            },
        ]
        transcripts_dir = self._setup_transcripts_dir(tmp_path, hearings)
        monkeypatch.setattr("config.TRANSCRIPTS_DIR", transcripts_dir)

        result = find_recent_transcripts(lookback_days=7)

        assert len(result) == 1


class TestMarkdownToSimpleHtml:
    """Tests for _markdown_to_simple_html conversion."""

    def test_header_h1(self):
        html = _markdown_to_simple_html("# Main Title")
        assert "<h1" in html
        assert "Main Title" in html

    def test_header_h2(self):
        html = _markdown_to_simple_html("## Section")
        assert "<h2" in html
        assert "Section" in html

    def test_header_h3(self):
        html = _markdown_to_simple_html("### Subsection")
        assert "<h3" in html
        assert "Subsection" in html

    def test_blockquote(self):
        html = _markdown_to_simple_html("> This is a quote")
        assert "<blockquote" in html
        assert "This is a quote" in html

    def test_multiline_blockquote(self):
        html = _markdown_to_simple_html("> Line one\n> Line two")
        assert html.count("<blockquote") == 1
        assert "Line one" in html
        assert "Line two" in html

    def test_unordered_list(self):
        html = _markdown_to_simple_html("- Item one\n- Item two")
        assert "<ul" in html
        assert "<li" in html
        assert "Item one" in html
        assert "Item two" in html

    def test_horizontal_rule(self):
        html = _markdown_to_simple_html("---")
        assert "<hr" in html

    def test_paragraph(self):
        html = _markdown_to_simple_html("Just a paragraph of text.")
        assert "<p" in html
        assert "Just a paragraph of text." in html

    def test_bold_text(self):
        html = _markdown_to_simple_html("This is **bold** text.")
        assert "<strong" in html
        assert "bold" in html

    def test_link(self):
        html = _markdown_to_simple_html("[Click here](https://example.com)")
        assert 'href="https://example.com"' in html
        assert "Click here" in html

    def test_link_rejects_non_http(self):
        """Non-http(s) links should be stripped to just the text."""
        html = _markdown_to_simple_html("[payload](javascript:alert(1))")
        assert "href" not in html
        assert "payload" in html

    def test_empty_input(self):
        html = _markdown_to_simple_html("")
        assert html == ""

    def test_list_closed_before_header(self):
        html = _markdown_to_simple_html("- item\n## Header")
        # The </ul> must appear before the <h2>
        ul_close = html.index("</ul>")
        h2_open = html.index("<h2")
        assert ul_close < h2_open

    def test_blockquote_closed_on_non_quote_line(self):
        html = _markdown_to_simple_html("> Quote\nParagraph after")
        bq_close = html.index("</blockquote>")
        # Find the <p> that contains "Paragraph after", not the inner blockquote <p>
        p_open = html.index("Paragraph after")
        assert bq_close < p_open


class TestScoreQuotes:
    """Tests for score_quotes with mocked interest model."""

    def _make_quote(self, text: str = "test quote", score: float = 0.0) -> Quote:
        return Quote(
            text=text,
            speaker="Sen. Test",
            context="Some context",
            hearing_title="Test Hearing",
            committee="senate.finance",
            hearing_date="2026-02-14",
            source_url="https://example.com",
            score=score,
        )

    def test_scores_and_filters_quotes(self, monkeypatch):
        @dataclass
        class FakeScoreResult:
            score: float = 0.0
            top_interests: tuple = ()

        class FakeInterestModel:
            def __init__(self):
                self._call_count = 0

            def score(self, text: str) -> FakeScoreResult:
                self._call_count += 1
                if "high" in text:
                    return FakeScoreResult(score=0.9, top_interests=("economics",))
                return FakeScoreResult(score=0.1, top_interests=("other",))

        fake_module = MagicMock()
        fake_module.InterestModel = FakeInterestModel

        import sys
        monkeypatch.setitem(sys.modules, "interest_model", MagicMock())
        monkeypatch.setitem(sys.modules, "interest_model.core", fake_module)
        monkeypatch.setattr("config.DIGEST_SCORE_THRESHOLD", 0.4)

        quotes = [
            self._make_quote(text="high relevance quote"),
            self._make_quote(text="low relevance quote"),
        ]

        filtered, cost = score_quotes(quotes)

        assert len(filtered) == 1
        assert filtered[0].text == "high relevance quote"
        assert filtered[0].score == 0.9
        assert filtered[0].themes == ["economics"]
        assert cost == 0.0

    def test_fallback_when_interest_model_missing(self, monkeypatch):
        """When interest_model is not installed, return quotes unscored."""
        import sys

        # Remove interest_model from sys.modules if present, and make import fail
        monkeypatch.delitem(sys.modules, "interest_model", raising=False)
        monkeypatch.delitem(sys.modules, "interest_model.core", raising=False)

        import builtins
        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "interest_model.core":
                raise ImportError("not installed")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", mock_import)

        quotes = [self._make_quote() for _ in range(5)]
        filtered, cost = score_quotes(quotes)

        assert len(filtered) == 5
        assert cost == 0.0

    def test_sorts_by_score_descending(self, monkeypatch):
        @dataclass
        class FakeScoreResult:
            score: float = 0.0
            top_interests: tuple = ()

        scores_iter = iter([0.5, 0.9, 0.7])

        class FakeInterestModel:
            def score(self, text: str) -> FakeScoreResult:
                return FakeScoreResult(
                    score=next(scores_iter), top_interests=("econ",)
                )

        fake_module = MagicMock()
        fake_module.InterestModel = FakeInterestModel

        import sys
        monkeypatch.setitem(sys.modules, "interest_model", MagicMock())
        monkeypatch.setitem(sys.modules, "interest_model.core", fake_module)
        monkeypatch.setattr("config.DIGEST_SCORE_THRESHOLD", 0.4)

        quotes = [self._make_quote(text=f"q{i}") for i in range(3)]
        filtered, _ = score_quotes(quotes)

        assert [q.score for q in filtered] == [0.9, 0.7, 0.5]


class TestComposeDigest:
    """Tests for compose_digest — LLM composition and error propagation."""

    def _make_quote(self, **kwargs) -> Quote:
        defaults = dict(
            text="The economy is strong",
            speaker="Chairman Powell",
            context="Fed testimony",
            hearing_title="Monetary Policy Hearing",
            committee="senate.banking",
            hearing_date="2026-02-14",
            source_url="https://example.com",
            score=0.85,
            themes=["economics"],
        )
        defaults.update(kwargs)
        return Quote(**defaults)

    def test_returns_markdown_and_cost(self, monkeypatch):
        monkeypatch.setattr(
            "digest.call_openrouter",
            lambda prompt, model, api_key, timeout=120.0: {
                "choices": [{"message": {"content": "## Economics\n> quote here"}}],
                "usage": {"prompt_tokens": 200, "completion_tokens": 100},
            },
        )
        monkeypatch.setattr(
            "digest.calculate_cost", lambda model, inp, out: 0.005
        )

        quotes = [self._make_quote()]
        body, cost = compose_digest(quotes, "fake-key")

        assert "Economics" in body
        assert cost == 0.005

    def test_raises_on_http_error(self, monkeypatch):
        """compose_digest does not catch httpx.HTTPError — it must propagate."""

        def mock_call(prompt, model, api_key, timeout=120.0):
            request = httpx.Request("POST", "https://openrouter.ai/api/v1/chat/completions")
            response = httpx.Response(502, request=request)
            raise httpx.HTTPStatusError(
                "Bad Gateway", request=request, response=response
            )

        monkeypatch.setattr("digest.call_openrouter", mock_call)

        quotes = [self._make_quote()]

        with pytest.raises(httpx.HTTPStatusError):
            compose_digest(quotes, "fake-key")

    def test_raises_on_malformed_response(self, monkeypatch):
        """Missing keys in response should raise ValueError."""
        monkeypatch.setattr(
            "digest.call_openrouter",
            lambda prompt, model, api_key, timeout=120.0: {
                "choices": [],  # empty choices
                "usage": {},
            },
        )

        quotes = [self._make_quote()]

        with pytest.raises(ValueError, match="Unexpected API response shape"):
            compose_digest(quotes, "fake-key")

    def test_groups_quotes_by_theme(self, monkeypatch):
        captured_prompts = []

        def mock_call(prompt, model, api_key, timeout=120.0):
            captured_prompts.append(prompt)
            return {
                "choices": [{"message": {"content": "digest body"}}],
                "usage": {"prompt_tokens": 100, "completion_tokens": 50},
            }

        monkeypatch.setattr("digest.call_openrouter", mock_call)
        monkeypatch.setattr(
            "digest.calculate_cost", lambda model, inp, out: 0.0
        )

        quotes = [
            self._make_quote(themes=["economics"], text="GDP grew 3%"),
            self._make_quote(themes=["defense"], text="Military budget increased"),
        ]
        compose_digest(quotes, "fake-key")

        # The prompt should contain the grouped JSON with both themes
        assert len(captured_prompts) == 1
        assert "economics" in captured_prompts[0]
        assert "defense" in captured_prompts[0]
