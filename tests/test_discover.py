"""Tests for discover.py — dedup logic and Hearing dataclass."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from discover import (
    Hearing,
    _cross_committee_dedup,
    _deduplicate,
    _normalize_title,
    _title_similarity,
)


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
        # "Hearing:" prefix -> "AI Regulation and Its Impact on Innovation and ..."
        h1 = Hearing("house.judiciary", "Judiciary",
                      "Hearing: AI Regulation and Its Impact on Innovation and Growth",
                      "2026-02-10", sources={"youtube_url": "yt"})
        h2 = Hearing("house.judiciary", "Judiciary",
                      "Full Committee Hearing: AI Regulation and Its Impact on Innovation and Growth in America",
                      "2026-02-10", sources={"website_url": "web"})
        result = _deduplicate([h1, h2])
        assert len(result) == 1
        assert "America" in result[0].title  # longer title wins


class TestTitleSimilarity:
    def test_identical_titles(self):
        sim = _title_similarity("Federal Reserve Monetary Policy", "Federal Reserve Monetary Policy")
        assert sim == 1.0

    def test_completely_different(self):
        sim = _title_similarity("Federal Reserve Monetary Policy", "Immigration Border Security")
        assert sim == 0.0

    def test_partial_overlap(self):
        # "federal reserve monetary policy" vs "monetary policy and the state of the economy"
        # intersection: {monetary, policy} = 2
        # union: {federal, reserve, monetary, policy, and, the, state, of, economy} = 9
        sim = _title_similarity(
            "Full Committee Hearing: Federal Reserve Monetary Policy",
            "MONETARY POLICY AND THE STATE OF THE ECONOMY",
        )
        assert sim > 0.0
        assert sim < 1.0

    def test_empty_title(self):
        assert _title_similarity("", "some title") == 0.0
        assert _title_similarity("some title", "") == 0.0
        assert _title_similarity("", "") == 0.0

    def test_punctuation_stripped(self):
        # Punctuation should not affect similarity
        sim1 = _title_similarity("Budget, Fiscal Year 2026", "Budget Fiscal Year 2026")
        assert sim1 == 1.0


class TestCrossCommitteeDedup:
    def test_cross_dedup_merges_govinfo_with_youtube(self):
        """YouTube hearing (specific key) + GovInfo hearing (generic key) with similar
        titles on the same date should merge into one."""
        yt = Hearing(
            "house.ways_and_means", "House Ways and Means",
            "Full Committee Hearing: Federal Reserve Monetary Policy Report",
            "2026-02-05",
            sources={"youtube_url": "https://youtube.com/watch?v=abc123"},
        )
        gov = Hearing(
            "govinfo.house", "House (via GovInfo)",
            "FEDERAL RESERVE MONETARY POLICY REPORT",
            "2026-02-05",
            sources={"govinfo_package_id": "CHRG-119hhrg12345"},
        )
        result = _cross_committee_dedup([yt, gov])
        assert len(result) == 1
        # Both sources should be merged
        assert "youtube_url" in result[0].sources
        assert "govinfo_package_id" in result[0].sources
        # Specific committee key should win
        assert result[0].committee_key == "house.ways_and_means"

    def test_cross_dedup_keeps_different_topics(self):
        """Two hearings on the same date from different committees with entirely
        different topics should NOT be merged."""
        h1 = Hearing(
            "house.ways_and_means", "House Ways and Means",
            "Full Committee Hearing: Federal Reserve Monetary Policy",
            "2026-02-05",
            sources={"youtube_url": "yt1"},
        )
        h2 = Hearing(
            "govinfo.house", "House (via GovInfo)",
            "BORDER SECURITY AND IMMIGRATION ENFORCEMENT ACT",
            "2026-02-05",
            sources={"govinfo_package_id": "CHRG-119hhrg99999"},
        )
        result = _cross_committee_dedup([h1, h2])
        assert len(result) == 2

    def test_cross_dedup_prefers_specific_key(self):
        """When merging, the result should use the non-govinfo committee key."""
        # Put the govinfo one first to verify it picks the specific key regardless of order
        gov = Hearing(
            "govinfo.house", "House (via GovInfo)",
            "FEDERAL RESERVE MONETARY POLICY REPORT",
            "2026-02-05",
            sources={"govinfo_package_id": "CHRG-119hhrg12345"},
        )
        yt = Hearing(
            "house.ways_and_means", "House Ways and Means",
            "Full Committee Hearing: Federal Reserve Monetary Policy Report",
            "2026-02-05",
            sources={"youtube_url": "https://youtube.com/watch?v=abc123"},
        )
        result = _cross_committee_dedup([gov, yt])
        assert len(result) == 1
        assert result[0].committee_key == "house.ways_and_means"
        assert result[0].committee_name == "House Ways and Means"

    def test_cross_dedup_different_chambers_no_merge(self):
        """House and Senate hearings on the same date with similar titles should
        NOT be merged -- they are different hearings in different chambers."""
        house = Hearing(
            "house.budget", "House Budget",
            "Federal Budget and Fiscal Policy Review",
            "2026-02-05",
            sources={"youtube_url": "yt-house"},
        )
        senate = Hearing(
            "senate.budget", "Senate Budget",
            "Federal Budget and Fiscal Policy Review",
            "2026-02-05",
            sources={"youtube_url": "yt-senate"},
        )
        result = _cross_committee_dedup([house, senate])
        assert len(result) == 2

    def test_cross_dedup_merges_sources(self):
        """When merging, all source URLs from both hearings should be preserved."""
        h1 = Hearing(
            "house.financial_services", "House Financial Services",
            "Hearing on Federal Reserve Monetary Policy Report",
            "2026-02-05",
            sources={
                "youtube_url": "https://youtube.com/watch?v=xyz",
                "website_url": "https://financialservices.house.gov/hearing/456",
            },
        )
        h2 = Hearing(
            "govinfo.house", "House (via GovInfo)",
            "FEDERAL RESERVE MONETARY POLICY REPORT TO CONGRESS",
            "2026-02-05",
            sources={"govinfo_package_id": "CHRG-119hhrg55555"},
        )
        result = _cross_committee_dedup([h1, h2])
        assert len(result) == 1
        assert "youtube_url" in result[0].sources
        assert "website_url" in result[0].sources
        assert "govinfo_package_id" in result[0].sources

    def test_cross_dedup_keeps_longer_title(self):
        """When merging, the longer of the two titles should be kept."""
        short = Hearing(
            "house.ways_and_means", "House Ways and Means",
            "Federal Reserve Monetary Policy Report",
            "2026-02-05",
            sources={"youtube_url": "yt"},
        )
        long_title = Hearing(
            "govinfo.house", "House (via GovInfo)",
            "FEDERAL RESERVE MONETARY POLICY REPORT TO THE CONGRESS -- PART II",
            "2026-02-05",
            sources={"govinfo_package_id": "pkg1"},
        )
        result = _cross_committee_dedup([short, long_title])
        assert len(result) == 1
        # The longer title should win
        assert "PART II" in result[0].title

    def test_cross_dedup_single_hearing_passthrough(self):
        """A single hearing on a date should pass through unchanged."""
        h = Hearing(
            "house.judiciary", "Judiciary",
            "AI Regulation Hearing", "2026-02-10",
            sources={"youtube_url": "yt"},
        )
        result = _cross_committee_dedup([h])
        assert len(result) == 1
        assert result[0] is h

    def test_cross_dedup_same_committee_key_skipped(self):
        """Hearings with the same committee_key should not be merged by cross-dedup
        (that is _deduplicate's job)."""
        h1 = Hearing(
            "house.judiciary", "Judiciary",
            "AI Regulation Hearing Part 1", "2026-02-10",
            sources={"youtube_url": "yt1"},
        )
        h2 = Hearing(
            "house.judiciary", "Judiciary",
            "AI Regulation Hearing Part 2", "2026-02-10",
            sources={"youtube_url": "yt2"},
        )
        result = _cross_committee_dedup([h1, h2])
        assert len(result) == 2
