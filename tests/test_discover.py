"""Tests for discover.py — dedup logic and Hearing dataclass."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from discover import Hearing, _deduplicate, _normalize_title


class TestNormalizeTitle:
    def test_strips_prefix(self):
        result = _normalize_title("Full Committee Hearing: Some Important Topic")
        assert "full" not in result.lower()

    def test_strips_hearing_notice(self):
        result = _normalize_title("HEARING NOTICE: Budget Review Session 2026 Fiscal Year")
        assert "hearing notice" not in result.lower()

    def test_returns_first_8_words(self):
        result = _normalize_title("one two three four five six seven eight nine ten")
        assert len(result.split()) == 8


class TestHearingId:
    def test_deterministic(self):
        h = Hearing("house.judiciary", "Judiciary Committee", "AI Hearing", "2026-02-10")
        assert h.id == h.id

    def test_length(self):
        h = Hearing("house.judiciary", "Judiciary Committee", "AI Hearing", "2026-02-10")
        assert len(h.id) == 12

    def test_different_committees(self):
        h1 = Hearing("house.judiciary", "Judiciary", "Same Title Here", "2026-02-10")
        h2 = Hearing("senate.judiciary", "Judiciary", "Same Title Here", "2026-02-10")
        assert h1.id != h2.id


class TestHearingSlug:
    def test_basic_slug(self):
        h = Hearing("house.judiciary", "Judiciary", "AI Regulation Hearing", "2026-02-10")
        assert h.slug.startswith("house-judiciary-")
        assert "ai-regulation" in h.slug

    def test_slug_truncation(self):
        long_title = "A" * 200
        h = Hearing("senate.finance", "Finance", long_title, "2026-02-10")
        # Slug title part limited to 80 chars
        assert len(h.slug) <= 120


class TestDeduplicate:
    def test_merges_same_hearing_different_sources(self):
        h1 = Hearing("house.judiciary", "Judiciary", "AI Regulation Hearing", "2026-02-10",
                      sources={"youtube_url": "https://youtube.com/watch?v=abc"})
        h2 = Hearing("house.judiciary", "Judiciary", "AI Regulation Hearing", "2026-02-10",
                      sources={"website_url": "https://judiciary.house.gov/hearing/123"})
        result = _deduplicate([h1, h2])
        assert len(result) == 1
        assert "youtube_url" in result[0].sources
        assert "website_url" in result[0].sources

    def test_keeps_different_date_hearings(self):
        h1 = Hearing("house.judiciary", "Judiciary", "AI Regulation Hearing", "2026-02-10")
        h2 = Hearing("house.judiciary", "Judiciary", "AI Regulation Hearing", "2026-02-11")
        result = _deduplicate([h1, h2])
        assert len(result) == 2

    def test_keeps_same_day_different_topics(self):
        h1 = Hearing("house.judiciary", "Judiciary", "AI Regulation Hearing", "2026-02-10")
        h2 = Hearing("house.judiciary", "Judiciary", "Immigration Reform Discussion", "2026-02-10")
        result = _deduplicate([h1, h2])
        assert len(result) == 2

    def test_prefers_longer_title(self):
        # Both titles normalize to the same 8-word prefix after stripping
        # "Hearing:" prefix → "AI Regulation and Its Impact on Innovation and ..."
        h1 = Hearing("house.judiciary", "Judiciary",
                      "Hearing: AI Regulation and Its Impact on Innovation and Growth",
                      "2026-02-10", sources={"youtube_url": "yt"})
        h2 = Hearing("house.judiciary", "Judiciary",
                      "Full Committee Hearing: AI Regulation and Its Impact on Innovation and Growth in America",
                      "2026-02-10", sources={"website_url": "web"})
        result = _deduplicate([h1, h2])
        assert len(result) == 1
        assert "America" in result[0].title  # longer title wins
