"""Tests for isvp.py — ISVP URL extraction, subtitle discovery, VTT merging."""

from isvp import extract_isvp_url, _find_subtitle_uri, _merge_vtt_segments, _extract_new_text


class TestExtractIsvpUrl:
    def test_basic_iframe(self):
        html = (
            '<div><iframe src="https://www.senate.gov/isvp/'
            '?comm=foreign&filename=foreign020426&type1=live">'
            '</iframe></div>'
        )
        result = extract_isvp_url(html)
        assert result is not None
        assert result["comm"] == "foreign"
        assert result["filename"] == "foreign020426"
        assert result["type1"] == "live"

    def test_single_quoted_src(self):
        html = (
            "<iframe src='https://www.senate.gov/isvp/"
            "?comm=judiciary&filename=judiciary021126'></iframe>"
        )
        result = extract_isvp_url(html)
        assert result is not None
        assert result["comm"] == "judiciary"
        assert result["filename"] == "judiciary021126"

    def test_no_iframe(self):
        html = "<div><p>No video available for this hearing.</p></div>"
        result = extract_isvp_url(html)
        assert result is None

    def test_non_isvp_iframe(self):
        html = '<iframe src="https://www.youtube.com/embed/abc123"></iframe>'
        result = extract_isvp_url(html)
        assert result is None

    def test_missing_comm(self):
        html = (
            '<iframe src="https://www.senate.gov/isvp/'
            '?filename=foreign020426"></iframe>'
        )
        result = extract_isvp_url(html)
        assert result is None

    def test_missing_filename(self):
        html = (
            '<iframe src="https://www.senate.gov/isvp/'
            '?comm=foreign"></iframe>'
        )
        result = extract_isvp_url(html)
        assert result is None

    def test_extra_params_captured(self):
        html = (
            '<iframe src="https://www.senate.gov/isvp/'
            '?comm=banking&filename=banking013026&type1=live&type2=arch">'
            '</iframe>'
        )
        result = extract_isvp_url(html)
        assert result is not None
        assert result["type1"] == "live"
        assert result["type2"] == "arch"

    def test_iframe_among_other_html(self):
        html = (
            '<html><body><h1>Hearing</h1>'
            '<p>Some text here</p>'
            '<iframe src="https://www.senate.gov/isvp/'
            '?comm=finance&filename=finance020526"></iframe>'
            '<p>More text</p></body></html>'
        )
        result = extract_isvp_url(html)
        assert result is not None
        assert result["comm"] == "finance"


class TestFindSubtitleUri:
    def test_ext_x_media_subtitles(self):
        master = (
            '#EXTM3U\n'
            '#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",'
            'NAME="English",URI="text_1.m3u8"\n'
            '#EXT-X-STREAM-INF:BANDWIDTH=2000000,SUBTITLES="subs"\n'
            'video_1.m3u8\n'
        )
        result = _find_subtitle_uri(master)
        assert result == "text_1.m3u8"

    def test_case_insensitive_type(self):
        master = (
            '#EXTM3U\n'
            '#EXT-X-MEDIA:TYPE=subtitles,GROUP-ID="subs",'
            'NAME="English",URI="text_2.m3u8"\n'
        )
        result = _find_subtitle_uri(master)
        assert result == "text_2.m3u8"

    def test_fallback_bare_text_m3u8(self):
        # No EXT-X-MEDIA with SUBTITLES, but a bare text_*.m3u8 line
        master = (
            '#EXTM3U\n'
            '#EXT-X-STREAM-INF:BANDWIDTH=2000000\n'
            'video_1.m3u8\n'
            'text_1.m3u8\n'
        )
        result = _find_subtitle_uri(master)
        assert result == "text_1.m3u8"

    def test_no_subtitles(self):
        master = (
            '#EXTM3U\n'
            '#EXT-X-STREAM-INF:BANDWIDTH=2000000\n'
            'video_1.m3u8\n'
        )
        result = _find_subtitle_uri(master)
        assert result is None

    def test_subtitle_uri_with_path(self):
        master = (
            '#EXTM3U\n'
            '#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",'
            'NAME="English",URI="subs/text_1.m3u8"\n'
        )
        result = _find_subtitle_uri(master)
        assert result == "subs/text_1.m3u8"

    def test_empty_manifest(self):
        result = _find_subtitle_uri("")
        assert result is None


class TestMergeVttSegments:
    def test_basic_rolling_caption(self):
        """Three consecutive cues simulating rolling captions."""
        seg = (
            "WEBVTT\n"
            "\n"
            "00:00:01.000 --> 00:00:03.000\n"
            "HELLO WORLD\n"
            "\n"
            "00:00:03.000 --> 00:00:05.000\n"
            "HELLO WORLD THIS IS\n"
            "\n"
            "00:00:05.000 --> 00:00:07.000\n"
            "THIS IS A TEST\n"
        )
        result = _merge_vtt_segments([seg])
        # Should contain all the words without duplicating the overlapping parts
        assert "HELLO" in result
        assert "WORLD" in result
        assert "TEST" in result

    def test_deduplicates_identical_cues(self):
        """Segment boundary overlap produces identical cues that should be collapsed."""
        seg1 = (
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:03.000\n"
            "SOME TEXT HERE\n"
        )
        seg2 = (
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:03.000\n"
            "SOME TEXT HERE\n"
            "\n"
            "00:00:03.000 --> 00:00:05.000\n"
            "HERE IS MORE\n"
        )
        result = _merge_vtt_segments([seg1, seg2])
        # "SOME TEXT HERE" should not appear twice as a block
        assert result.count("SOME TEXT HERE") <= 1

    def test_no_false_paragraph_within_continuous_speech(self):
        """Cues close together should NOT produce a paragraph break."""
        seg = (
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:03.000\n"
            "FIRST PART\n"
            "\n"
            "00:00:03.000 --> 00:00:05.000\n"
            "SECOND PART\n"
        )
        result = _merge_vtt_segments([seg])
        # Continuous speech (2s gap) should be a single block, no paragraph break
        assert "\n\n" not in result
        assert "FIRST PART" in result
        assert "SECOND PART" in result

    def test_empty_segments(self):
        result = _merge_vtt_segments([])
        assert result == ""

    def test_html_tags_stripped(self):
        seg = (
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:03.000\n"
            "<c>SOME</c> <c>TEXT</c>\n"
        )
        result = _merge_vtt_segments([seg])
        assert "<c>" not in result
        assert "</c>" not in result
        assert "SOME" in result
        assert "TEXT" in result

    def test_multiple_segments_merge(self):
        """Multiple VTT segments should merge into a single coherent output."""
        seg1 = (
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:03.000\n"
            "FIRST SEGMENT TEXT\n"
        )
        seg2 = (
            "WEBVTT\n\n"
            "00:00:03.000 --> 00:00:05.000\n"
            "SECOND SEGMENT TEXT\n"
        )
        result = _merge_vtt_segments([seg1, seg2])
        assert "FIRST" in result
        assert "SECOND" in result


class TestExtractNewText:
    def test_appended_words(self):
        prev = "THE SENATOR FROM"
        curr = "THE SENATOR FROM NEW YORK"
        result = _extract_new_text(prev, curr)
        assert result == "NEW YORK"

    def test_scrolling_caption(self):
        """Old text scrolls off top, new text appears at bottom."""
        prev = "ENERGY SINCE THE START OF UKRAINE."
        curr = "UKRAINE. BEAR WITH ME, THIS IS CLICKING"
        result = _extract_new_text(prev, curr)
        # "UKRAINE." overlaps, so new text is everything after
        assert "BEAR" in result
        assert "CLICKING" in result

    def test_no_overlap_returns_full_text(self):
        prev = "COMPLETELY DIFFERENT TEXT"
        curr = "BRAND NEW STATEMENT HERE"
        result = _extract_new_text(prev, curr)
        assert result == "BRAND NEW STATEMENT HERE"

    def test_identical_screens_return_empty(self):
        prev = "SAME TEXT ON SCREEN"
        curr = "SAME TEXT ON SCREEN"
        result = _extract_new_text(prev, curr)
        assert result == ""

    def test_empty_curr(self):
        result = _extract_new_text("SOME TEXT", "")
        assert result == ""

    def test_empty_prev(self):
        result = _extract_new_text("", "NEW SCREEN TEXT")
        assert result == "NEW SCREEN TEXT"

    def test_partial_word_overlap(self):
        """CART stenography: partial word in prev completed in curr."""
        prev = "IS NOT MUC"
        curr = "IS NOT MUCH BETTER"
        result = _extract_new_text(prev, curr)
        # "MUC" is a prefix of "MUCH", so overlap should still be detected
        assert "BETTER" in result
        # Should not duplicate "MUCH" or "IS NOT"
        assert "IS NOT" not in result

    def test_single_word_overlap(self):
        prev = "GOOD MORNING"
        curr = "MORNING EVERYONE"
        result = _extract_new_text(prev, curr)
        assert result == "EVERYONE"

    def test_full_overlap_no_new_text(self):
        """Current screen is a subset of previous — overlap covers everything."""
        prev = "ALPHA BETA GAMMA DELTA"
        curr = "GAMMA DELTA"
        result = _extract_new_text(prev, curr)
        # All of curr overlaps with end of prev, so nothing is new
        assert result == ""
