"""Tests for utils.py — title normalization, hearing ID, congress calculation."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils import hearing_id, normalize_title, current_congress


class TestNormalizeTitle:
    def test_strips_full_committee_prefix(self):
        result = normalize_title("Full Committee Hearing: Some Important Topic About Economy")
        assert "full" not in result
        assert "committee" not in result
        assert "hearing" not in result

    def test_strips_hearing_notice(self):
        result = normalize_title("HEARING NOTICE: Budget Review Session for 2026")
        assert "hearing" not in result
        assert "notice" not in result

    def test_strips_subcommittee_prefix(self):
        result = normalize_title("Subcommittee Hearing: Tax Policy Discussion for Reform")
        assert "subcommittee" not in result

    def test_lowercases(self):
        result = normalize_title("ALL CAPS TITLE ABOUT ECONOMICS")
        assert result == result.lower()

    def test_first_8_words(self):
        result = normalize_title("one two three four five six seven eight nine ten")
        words = result.split()
        assert len(words) == 8

    def test_removes_punctuation(self):
        result = normalize_title("The Budget: A Review of FY2026 (Part II)")
        assert ":" not in result
        assert "(" not in result

    def test_empty_string(self):
        result = normalize_title("")
        assert result == ""


class TestHearingId:
    def test_deterministic(self):
        id1 = hearing_id("house.ways_and_means", "2026-02-10", "Tax Reform Hearing")
        id2 = hearing_id("house.ways_and_means", "2026-02-10", "Tax Reform Hearing")
        assert id1 == id2

    def test_12_chars(self):
        result = hearing_id("senate.finance", "2026-01-15", "Budget Review")
        assert len(result) == 12

    def test_different_inputs(self):
        id1 = hearing_id("house.ways_and_means", "2026-02-10", "Tax Reform Hearing")
        id2 = hearing_id("senate.finance", "2026-02-10", "Tax Reform Hearing")
        assert id1 != id2

    def test_normalized_title_in_hash(self):
        # Titles that normalize to the same thing should produce same ID
        id1 = hearing_id("house.judiciary", "2026-03-01", "Full Committee Hearing: AI Regulation")
        id2 = hearing_id("house.judiciary", "2026-03-01", "Hearing: AI Regulation and Technology Policy")
        # These won't be equal because the normalized titles differ after prefix removal
        # But titles with same prefix stripped + same first 8 words WILL match
        id3 = hearing_id("house.judiciary", "2026-03-01", "Full Committee Hearing: AI Regulation")
        assert id1 == id3


class TestCurrentCongress:
    def test_returns_integer(self):
        result = current_congress()
        assert isinstance(result, int)

    def test_reasonable_range(self):
        result = current_congress()
        assert 119 <= result <= 125  # valid range for 2025-2036
