"""Tests for discover.py — dedup logic and Hearing dataclass."""

import threading

import discover
from discover import (
    Hearing,
    _attach_youtube_clips,
    _chamber_from_package_id,
    _cross_committee_dedup,
    _deduplicate,
    _is_markup_or_procedural,
    _keyword_overlap,
    _merge_adjacent_date_pairs,
    title_similarity,
)
from utils import normalize_title


class TestNormalizeTitle:
    def test_strips_prefix(self):
        result = normalize_title("Full Committee Hearing: Some Important Topic")
        assert "full" not in result.lower()

    def test_strips_hearing_notice(self):
        result = normalize_title("HEARING NOTICE: Budget Review Session 2026 Fiscal Year")
        assert "hearing notice" not in result.lower()

    def test_returns_first_8_words(self):
        result = normalize_title("one two three four five six seven eight nine ten")
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
        sim = title_similarity("Federal Reserve Monetary Policy", "Federal Reserve Monetary Policy")
        assert sim == 1.0

    def test_completely_different(self):
        sim = title_similarity("Federal Reserve Monetary Policy", "Immigration Border Security")
        assert sim == 0.0

    def test_partial_overlap(self):
        # "federal reserve monetary policy" vs "monetary policy and the state of the economy"
        # intersection: {monetary, policy} = 2
        # union: {federal, reserve, monetary, policy, and, the, state, of, economy} = 9
        sim = title_similarity(
            "Full Committee Hearing: Federal Reserve Monetary Policy",
            "MONETARY POLICY AND THE STATE OF THE ECONOMY",
        )
        assert sim > 0.0
        assert sim < 1.0

    def test_empty_title(self):
        assert title_similarity("", "some title") == 0.0
        assert title_similarity("some title", "") == 0.0
        assert title_similarity("", "") == 0.0

    def test_punctuation_stripped(self):
        # Punctuation should not affect similarity
        sim1 = title_similarity("Budget, Fiscal Year 2026", "Budget Fiscal Year 2026")
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

    def test_cross_dedup_keeps_higher_authority_title(self):
        """When merging, higher authority source title should win."""
        yt = Hearing(
            "house.ways_and_means", "House Ways and Means",
            "Full Committee Hearing: Federal Reserve Monetary Policy Report to Congress",
            "2026-02-05",
            sources={"youtube_url": "yt"},
            source_authority=1,
        )
        gov = Hearing(
            "govinfo.house", "House (via GovInfo)",
            "Federal Reserve Monetary Policy Report",
            "2026-02-05",
            sources={"govinfo_package_id": "pkg1"},
            source_authority=3,
        )
        result = _cross_committee_dedup([yt, gov])
        assert len(result) == 1
        # GovInfo (authority=3) title wins over YouTube (authority=1) even though shorter
        assert result[0].title == "Federal Reserve Monetary Policy Report"
        assert result[0].source_authority == 3

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


class TestSourceAuthority:
    """Tests for authority-aware title merging."""

    def test_congress_api_title_wins_in_dedup(self):
        """Congress.gov (authority=4) title should override YouTube (authority=1).
        Titles must normalize to the same 8-word prefix for exact-match dedup."""
        congress = Hearing(
            "house.judiciary", "Judiciary",
            "Oversight of Artificial Intelligence and Technology Policy",
            "2026-02-10",
            sources={"congress_api_event_id": "evt123"},
            source_authority=4,
        )
        youtube = Hearing(
            "house.judiciary", "Judiciary",
            "Hearing: Oversight of Artificial Intelligence and Technology Policy in America and Beyond",
            "2026-02-10",
            sources={"youtube_url": "yt"},
            source_authority=1,
        )
        result = _deduplicate([congress, youtube])
        assert len(result) == 1
        assert result[0].title == "Oversight of Artificial Intelligence and Technology Policy"
        assert result[0].source_authority == 4

    def test_equal_authority_falls_back_to_longer_title(self):
        """When both sources have the same authority, longer title should win.
        Titles normalize to the same 8-word prefix for exact-match dedup."""
        h1 = Hearing(
            "house.judiciary", "Judiciary",
            "Hearing: Oversight of Artificial Intelligence Regulation and Its Impact",
            "2026-02-10",
            sources={"website_url": "web1"},
            source_authority=2,
        )
        h2 = Hearing(
            "house.judiciary", "Judiciary",
            "Hearing: Oversight of Artificial Intelligence Regulation and Its Impact on Innovation in America",
            "2026-02-10",
            sources={"website_url": "web2"},
            source_authority=2,
        )
        result = _deduplicate([h1, h2])
        assert len(result) == 1
        assert "Innovation" in result[0].title

    def test_sort_order_congress_first(self):
        """Higher authority entries should sort first (descending)."""
        hearings = [
            Hearing("house.judiciary", "Judiciary", "Title", "2026-02-10",
                    source_authority=1),
            Hearing("house.judiciary", "Judiciary", "Title", "2026-02-10",
                    source_authority=4),
            Hearing("house.judiciary", "Judiciary", "Title", "2026-02-10",
                    source_authority=2),
        ]
        hearings.sort(key=lambda h: -h.source_authority)
        assert hearings[0].source_authority == 4
        assert hearings[1].source_authority == 2
        assert hearings[2].source_authority == 1

    def test_adjacent_date_merge_prefers_authority(self):
        """In adjacent-date merge, higher authority should be winner."""
        congress = Hearing(
            "house.judiciary", "Judiciary",
            "Oversight of Artificial Intelligence Regulation Policy",
            "2026-02-10",
            sources={"congress_api_event_id": "evt1"},
            source_authority=4,
        )
        youtube = Hearing(
            "house.judiciary", "Judiciary",
            "Hearing: Oversight of Artificial Intelligence Regulation Policy Discussion",
            "2026-02-11",
            sources={"youtube_url": "yt", "youtube_id": "abc"},
            source_authority=1,
        )
        result = _merge_adjacent_date_pairs([congress, youtube])
        assert len(result) == 1
        assert result[0].source_authority == 4
        assert result[0].title == "Oversight of Artificial Intelligence Regulation Policy"
        assert "youtube_url" in result[0].sources

    def test_source_authority_default_zero(self):
        """Hearing with no explicit source_authority should default to 0."""
        h = Hearing("house.judiciary", "Judiciary", "Title", "2026-02-10")
        assert h.source_authority == 0


class TestChamberFromPackageId:
    def test_house_package(self):
        assert _chamber_from_package_id("CHRG-119hhrg12345") == "house"

    def test_senate_package(self):
        assert _chamber_from_package_id("CHRG-119shrg99999") == "senate"

    def test_joint_resolution_unknown(self):
        assert _chamber_from_package_id("CHRG-119jres12345") == "unknown"

    def test_case_insensitive(self):
        assert _chamber_from_package_id("chrg-119HHRG12345") == "house"

    def test_case_insensitive_senate(self):
        assert _chamber_from_package_id("chrg-119SHRG55555") == "senate"


class TestIsMarkupOrProcedural:
    def test_markup_of(self):
        assert _is_markup_or_procedural("Markup of H.R. 1234") is True

    def test_full_committee_markup(self):
        assert _is_markup_or_procedural("Full Committee Markup: Defense Bill") is True

    def test_business_meeting(self):
        assert _is_markup_or_procedural("Business Meeting") is True

    def test_organizational_meeting(self):
        assert _is_markup_or_procedural("Organizational Meeting") is True

    def test_hearing_on_topic(self):
        assert _is_markup_or_procedural("Hearing on AI Regulation") is False

    def test_policy_title(self):
        assert _is_markup_or_procedural("Federal Reserve Monetary Policy") is False

    def test_mark_up_of_variant(self):
        assert _is_markup_or_procedural("Mark Up of Some Bill") is True

    def test_member_day(self):
        assert _is_markup_or_procedural("Member Day") is True

    def test_case_insensitive(self):
        assert _is_markup_or_procedural("BUSINESS MEETING") is True

    def test_empty_string(self):
        assert _is_markup_or_procedural("") is False


class TestKeywordOverlap:
    def test_overlapping_titles(self):
        count = _keyword_overlap(
            "Federal Reserve Monetary Policy Report",
            "Monetary Policy Report to Congress",
        )
        # "monetary", "policy", "report" should overlap (all >= 3 chars, not stopwords)
        assert count >= 2

    def test_completely_different_titles(self):
        count = _keyword_overlap(
            "Artificial Intelligence Regulation",
            "Border Security Immigration Enforcement",
        )
        assert count == 0

    def test_empty_titles(self):
        assert _keyword_overlap("", "some title") == 0
        assert _keyword_overlap("some title", "") == 0
        assert _keyword_overlap("", "") == 0

    def test_stopwords_excluded(self):
        # "the", "and", "of" are stopwords; "hearing" and "committee" are also stopwords
        count = _keyword_overlap(
            "the hearing of the committee",
            "the hearing and the committee",
        )
        assert count == 0

    def test_short_words_excluded(self):
        # Words shorter than 3 chars should be excluded
        count = _keyword_overlap("AI is on", "AI is on")
        assert count == 0

    def test_significant_words_counted(self):
        # "federal", "reserve" are significant (>= 3 chars, not stopwords)
        count = _keyword_overlap("Federal Reserve", "Federal Reserve")
        assert count == 2


class TestAttachYoutubeClips:
    def test_clip_matched_to_hearing(self, monkeypatch):
        """A clip with matching committee, date, and similar title gets attached."""
        clip = {
            "committee_key": "house.judiciary",
            "date": "2026-02-10",
            "title": "AI Regulation Hearing Discussion",
            "youtube_url": "https://youtube.com/watch?v=clip1",
            "youtube_id": "clip1",
            "duration": 300,
        }
        monkeypatch.setattr(discover, "_youtube_clips", [clip])
        monkeypatch.setattr(discover, "_youtube_clips_lock", threading.Lock())

        h = Hearing(
            "house.judiciary", "Judiciary",
            "AI Regulation Hearing", "2026-02-10",
            sources={"website_url": "https://example.com"},
        )
        _attach_youtube_clips([h])

        assert "youtube_url" in h.sources
        assert h.sources["youtube_url"] == "https://youtube.com/watch?v=clip1"
        assert "youtube_clips" in h.sources
        assert len(h.sources["youtube_clips"]) == 1
        # _youtube_clips should be cleared after call
        assert discover._youtube_clips == []

    def test_clip_non_matching_committee(self, monkeypatch):
        """A clip with a different committee should NOT be attached."""
        clip = {
            "committee_key": "senate.judiciary",
            "date": "2026-02-10",
            "title": "AI Regulation Hearing Discussion",
            "youtube_url": "https://youtube.com/watch?v=clip2",
            "youtube_id": "clip2",
            "duration": 300,
        }
        monkeypatch.setattr(discover, "_youtube_clips", [clip])
        monkeypatch.setattr(discover, "_youtube_clips_lock", threading.Lock())

        h = Hearing(
            "house.judiciary", "Judiciary",
            "AI Regulation Hearing", "2026-02-10",
            sources={"website_url": "https://example.com"},
        )
        _attach_youtube_clips([h])

        assert "youtube_url" not in h.sources
        assert "youtube_clips" not in h.sources

    def test_clip_non_matching_date(self, monkeypatch):
        """A clip with a different date should NOT be attached."""
        clip = {
            "committee_key": "house.judiciary",
            "date": "2026-02-15",
            "title": "AI Regulation Hearing Discussion",
            "youtube_url": "https://youtube.com/watch?v=clip3",
            "youtube_id": "clip3",
            "duration": 300,
        }
        monkeypatch.setattr(discover, "_youtube_clips", [clip])
        monkeypatch.setattr(discover, "_youtube_clips_lock", threading.Lock())

        h = Hearing(
            "house.judiciary", "Judiciary",
            "AI Regulation Hearing", "2026-02-10",
            sources={"website_url": "https://example.com"},
        )
        _attach_youtube_clips([h])

        assert "youtube_url" not in h.sources

    def test_does_not_overwrite_existing_youtube_url(self, monkeypatch):
        """If hearing already has a youtube_url, clip should not overwrite it
        but should still be added to youtube_clips list."""
        clip = {
            "committee_key": "house.judiciary",
            "date": "2026-02-10",
            "title": "AI Regulation Hearing Discussion",
            "youtube_url": "https://youtube.com/watch?v=clip4",
            "youtube_id": "clip4",
            "duration": 300,
        }
        monkeypatch.setattr(discover, "_youtube_clips", [clip])
        monkeypatch.setattr(discover, "_youtube_clips_lock", threading.Lock())

        h = Hearing(
            "house.judiciary", "Judiciary",
            "AI Regulation Hearing", "2026-02-10",
            sources={
                "website_url": "https://example.com",
                "youtube_url": "https://youtube.com/watch?v=original",
            },
        )
        _attach_youtube_clips([h])

        # Original youtube_url preserved
        assert h.sources["youtube_url"] == "https://youtube.com/watch?v=original"
        # Clip still recorded in youtube_clips list
        assert "youtube_clips" in h.sources
        assert h.sources["youtube_clips"][0]["url"] == "https://youtube.com/watch?v=clip4"

    def test_empty_clips_list_is_noop(self, monkeypatch):
        """When _youtube_clips is empty, function should return early."""
        monkeypatch.setattr(discover, "_youtube_clips", [])
        monkeypatch.setattr(discover, "_youtube_clips_lock", threading.Lock())

        h = Hearing(
            "house.judiciary", "Judiciary",
            "AI Regulation Hearing", "2026-02-10",
            sources={"website_url": "https://example.com"},
        )
        _attach_youtube_clips([h])

        assert "youtube_url" not in h.sources
        assert "youtube_clips" not in h.sources
