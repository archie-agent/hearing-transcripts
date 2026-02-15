"""Tests for transcribe.py — VTT-to-text conversion."""

from transcribe import _vtt_to_text


SAMPLE_VTT = """\
WEBVTT
Kind: captions
Language: en

00:00:01.000 --> 00:00:04.000
thank you mister chairman

00:00:04.000 --> 00:00:07.000
thank you mister chairman

00:00:07.000 --> 00:00:10.000
i want to welcome our witnesses today

00:00:10.000 --> 00:00:14.000
<c>thank</c><c> you</c><c> for</c><c> being</c><c> here</c>

00:00:14.000 --> 00:00:18.000
yes

00:00:18.000 --> 00:00:22.000
thank you mister chairman

00:00:22.000 --> 00:00:25.000
yes

00:00:25.000 --> 00:00:28.000
i yield back
"""


class TestVttToText:
    def test_strips_vtt_headers(self):
        result = _vtt_to_text(SAMPLE_VTT)
        assert "WEBVTT" not in result
        assert "Kind:" not in result
        assert "Language:" not in result

    def test_strips_timestamps(self):
        result = _vtt_to_text(SAMPLE_VTT)
        assert "-->" not in result

    def test_strips_html_tags(self):
        result = _vtt_to_text(SAMPLE_VTT)
        assert "<c>" not in result
        assert "</c>" not in result

    def test_adjacent_dedup(self):
        """Adjacent identical lines should be collapsed."""
        result = _vtt_to_text(SAMPLE_VTT)
        lines = result.splitlines()
        # "thank you mister chairman" appears at 0:01 and 0:04 (adjacent) — collapsed to one
        # Then appears again at 0:22 (not adjacent) — should be kept
        chairman_lines = [l for l in lines if l == "thank you mister chairman"]
        assert len(chairman_lines) == 2  # once from first run, once from later

    def test_keeps_non_adjacent_repeats(self):
        """Non-adjacent identical lines should be preserved."""
        result = _vtt_to_text(SAMPLE_VTT)
        lines = result.splitlines()
        # "yes" appears at 0:14 and 0:25 (not adjacent) — both kept
        yes_lines = [l for l in lines if l == "yes"]
        assert len(yes_lines) == 2

    def test_empty_input(self):
        result = _vtt_to_text("")
        assert result == ""

    def test_basic_content(self):
        result = _vtt_to_text(SAMPLE_VTT)
        assert "i want to welcome our witnesses today" in result
        assert "i yield back" in result
