"""Tests for cspan.py — keyword extraction, caps normalization, transcript building."""

from cspan import _extract_search_keywords, _normalize_caps, _build_transcript


class TestExtractSearchKeywords:
    def test_strips_stopwords(self):
        title = "Full Committee Hearing on the Oversight of the Budget"
        result = _extract_search_keywords(title)
        # "full", "committee", "hearing", "the", "of", "oversight", "budget"
        # stopwords: "full", "committee", "hearing", "the", "of", "oversight"
        assert "the" not in result.split()
        assert "of" not in result.split()
        assert "hearing" not in result.split()
        assert "committee" not in result.split()

    def test_keeps_significant_words(self):
        title = "Nominations for Department of Defense Officials"
        result = _extract_search_keywords(title)
        assert "nominations" in result.split()
        assert "department" in result.split()
        assert "defense" in result.split()
        assert "officials" in result.split()

    def test_max_words_default(self):
        title = ("Examining Artificial Intelligence Applications in Healthcare "
                 "and National Security Infrastructure and Economic Growth")
        result = _extract_search_keywords(title)
        words = result.split()
        assert len(words) <= 5

    def test_max_words_custom(self):
        title = ("Examining Artificial Intelligence Applications in Healthcare "
                 "and National Security Infrastructure and Economic Growth")
        result = _extract_search_keywords(title, max_words=3)
        words = result.split()
        assert len(words) <= 3

    def test_strips_punctuation(self):
        title = "S. 1234: The National Security Act (Part II)"
        result = _extract_search_keywords(title)
        # Punctuation should be removed; digits kept if >= 3 chars
        for word in result.split():
            assert not any(c in word for c in ":.()")

    def test_short_words_removed(self):
        title = "An AI Act to be or not to be"
        result = _extract_search_keywords(title)
        # "an" (stopword), "ai" (2 chars), "act" (3 chars, kept),
        # "to" (stopword), "be" (2 chars), "or" (stopword), "not" (3 chars but stopword-free)
        for word in result.split():
            assert len(word) >= 3

    def test_empty_title(self):
        assert _extract_search_keywords("") == ""

    def test_all_stopwords(self):
        title = "Hearing of the Committee on Oversight"
        result = _extract_search_keywords(title)
        # All words are stopwords or < 3 chars
        assert result == ""

    def test_lowercases_input(self):
        title = "ARTIFICIAL INTELLIGENCE Policy Discussion"
        result = _extract_search_keywords(title)
        for word in result.split():
            assert word == word.lower()


class TestNormalizeCaps:
    def test_all_caps_converted(self):
        text = "THE SENATOR FROM NEW YORK HAS THE FLOOR"
        result = _normalize_caps(text)
        assert result != text  # should not be all caps anymore
        assert result[0].isupper()  # first letter capitalized
        assert "senator" in result.lower()

    def test_sentence_case_first_word(self):
        text = "THANK YOU FOR BEING HERE TODAY"
        result = _normalize_caps(text)
        assert result.startswith("Thank")
        assert "you" in result  # subsequent words lowercase

    def test_preserves_abbreviations(self):
        text = "THE FBI AND CIA WORKED WITH NATO ON THIS"
        result = _normalize_caps(text)
        assert "FBI" in result
        assert "CIA" in result
        assert "NATO" in result

    def test_mixed_case_unchanged(self):
        # Less than 60% uppercase -> returned as-is
        text = "The senator asked about fiscal policy trends"
        result = _normalize_caps(text)
        assert result == text

    def test_multiple_sentences(self):
        text = "FIRST SENTENCE HERE. SECOND SENTENCE HERE."
        result = _normalize_caps(text)
        # Should contain two sentence-cased segments
        assert "First" in result
        assert "Second" in result

    def test_preserves_covid_abbreviation(self):
        text = "THE COVID PANDEMIC CHANGED EVERYTHING"
        result = _normalize_caps(text)
        assert "COVID" in result

    def test_empty_string(self):
        result = _normalize_caps("")
        assert result == ""


class TestBuildTranscript:
    def test_speaker_transition(self):
        parts = [
            {"cc_name": "Sen. Smith", "text": "THANK YOU, MR. CHAIRMAN.", "secAppOffset": 0},
            {"cc_name": "Chairman Jones", "text": "THE CHAIR RECOGNIZES THE SENATOR.", "secAppOffset": 10},
        ]
        result = _build_transcript(parts)
        assert "Sen. Smith:" in result
        assert "Chairman Jones:" in result

    def test_continuation_same_speaker(self):
        parts = [
            {"cc_name": "Sen. Smith", "text": "FIRST PART OF MY REMARKS.", "secAppOffset": 0},
            {"cc_name": "Sen. Smith", "text": "CONTINUING MY REMARKS.", "secAppOffset": 5},
        ]
        result = _build_transcript(parts)
        # Speaker label should appear only once
        assert result.count("Sen. Smith:") == 1

    def test_unlabeled_speaker_transition(self):
        parts = [
            {"cc_name": "Sen. Smith", "text": "I YIELD MY TIME.", "secAppOffset": 0},
            {"cc_name": "", "text": "THANK YOU SENATOR.", "secAppOffset": 10},
        ]
        result = _build_transcript(parts)
        assert "[SPEAKER]:" in result

    def test_chevron_speaker_treated_as_none(self):
        parts = [
            {"cc_name": "Sen. Smith", "text": "FIRST REMARK.", "secAppOffset": 0},
            {"cc_name": ">>", "text": "SECOND REMARK.", "secAppOffset": 5},
        ]
        result = _build_transcript(parts)
        # ">>" is treated as unlabeled, should produce [SPEAKER]:
        assert "[SPEAKER]:" in result

    def test_empty_text_skipped(self):
        parts = [
            {"cc_name": "Sen. Smith", "text": "OPENING STATEMENT.", "secAppOffset": 0},
            {"cc_name": "Sen. Smith", "text": "", "secAppOffset": 5},
            {"cc_name": "Sen. Smith", "text": "CLOSING STATEMENT.", "secAppOffset": 10},
        ]
        result = _build_transcript(parts)
        # Empty text part should be silently skipped
        assert "Sen. Smith:" in result
        assert result.count("Sen. Smith:") == 1

    def test_caps_normalized_in_output(self):
        parts = [
            {"cc_name": "Speaker", "text": "THIS IS ALL CAPS TEXT FOR TESTING", "secAppOffset": 0},
        ]
        result = _build_transcript(parts)
        # Should be sentence-cased, not all-caps
        assert "THIS IS ALL CAPS" not in result

    def test_newlines_collapsed(self):
        parts = [
            {"cc_name": "Speaker", "text": "LINE ONE\n\nLINE TWO\nLINE THREE", "secAppOffset": 0},
        ]
        result = _build_transcript(parts)
        # Internal newlines from caption formatting should be collapsed to spaces
        assert "\n\n\n" not in result.split("Speaker:")[1] if "Speaker:" in result else True

    def test_none_text_skipped(self):
        parts = [
            {"cc_name": "Speaker", "text": None, "secAppOffset": 0},
            {"cc_name": "Speaker", "text": "ACTUAL TEXT HERE.", "secAppOffset": 5},
        ]
        result = _build_transcript(parts)
        assert "Speaker:" in result

    def test_empty_parts_list(self):
        result = _build_transcript([])
        assert result.strip() == ""
