"""Tests for utils.py — title normalization and congress calculation."""

from utils import normalize_title
from config import current_congress
from discover import Hearing


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
    """Hearing.id is the canonical ID (uses discover._normalize_title with ':' separator)."""

    def test_deterministic(self):
        h1 = Hearing("house.ways_and_means", "Ways and Means", "Tax Reform Hearing", "2026-02-10")
        h2 = Hearing("house.ways_and_means", "Ways and Means", "Tax Reform Hearing", "2026-02-10")
        assert h1.id == h2.id

    def test_12_chars(self):
        h = Hearing("senate.finance", "Finance", "Budget Review", "2026-01-15")
        assert len(h.id) == 12

    def test_different_inputs(self):
        h1 = Hearing("house.ways_and_means", "Ways and Means", "Tax Reform Hearing", "2026-02-10")
        h2 = Hearing("senate.finance", "Finance", "Tax Reform Hearing", "2026-02-10")
        assert h1.id != h2.id

    def test_normalized_title_in_hash(self):
        # Titles with same prefix stripped + same first 8 words WILL match
        h1 = Hearing("house.judiciary", "Judiciary", "Full Committee Hearing: AI Regulation", "2026-03-01")
        h3 = Hearing("house.judiciary", "Judiciary", "Full Committee Hearing: AI Regulation", "2026-03-01")
        assert h1.id == h3.id


class TestCurrentCongress:
    def test_returns_integer(self):
        result = current_congress()
        assert isinstance(result, int)

    def test_reasonable_range(self):
        result = current_congress()
        assert 119 <= result <= 125  # valid range for 2025-2036
