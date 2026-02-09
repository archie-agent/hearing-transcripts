"""Senate ISVP caption extraction for congressional hearings.

The Senate Internet Video Player (ISVP) system delivers hearing video via
Akamai HLS streams.  Each stream includes broadcast-quality closed captions
(CART/stenographer style) as VTT subtitle tracks embedded in the HLS manifest.

This module:
1. Parses ISVP iframe URLs from hearing detail page HTML.
2. Resolves the Akamai HLS manifest for a given committee + filename.
3. Downloads, merges, and deduplicates VTT caption segments into clean
   transcript text.

Usage (standalone test)::

    from isvp import fetch_isvp_captions
    text = fetch_isvp_captions("foreign", "foreign020426")
    print(len(text), "chars")
    print(text[:500])
"""

from __future__ import annotations

import logging
import re
from urllib.parse import parse_qs, urljoin, urlparse

from utils import get_http_client

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Akamai stream ID mapping  (comm code -> Akamai live stream ID)
#
# Extracted from the ISVP page JavaScript.  These map Senate committee short
# codes to the numeric stream identifiers used in Akamai HLS URLs.
# ---------------------------------------------------------------------------
_ISVP_STREAMS: dict[str, str] = {
    "ag": "2036803",
    "aging": "2036801",
    "approps": "2036802",
    "armed": "2036800",
    "banking": "2036799",
    "budget": "2036798",
    "commerce": "2036779",
    "energy": "2036797",
    "epw": "2036783",
    "ethics": "2036796",
    "finance": "2036795",
    "foreign": "2036794",
    "govtaff": "2036792",
    "help": "2036793",
    "indian": "2036791",
    "intel": "2036790",
    "judiciary": "2036788",
    "rules": "2036787",
    "smbiz": "2036786",
    "vetaff": "2036785",
    # Aliases — committees.json uses 'hsgac' while the ISVP JS uses 'govtaff'
    "hsgac": "2036792",
}

# Base URL template for Akamai HLS manifests
_AKAMAI_BASE = (
    "https://www-senate-gov-media-srs.akamaized.net"
    "/hls/live/{stream_id}/{comm}/{filename}"
)

# Regex to locate ISVP iframes in hearing detail page HTML
_ISVP_IFRAME_RE = re.compile(
    r"""<iframe[^>]+src=["']([^"']*senate\.gov/isvp/[^"']*)["']""",
    re.IGNORECASE,
)

# Regex to parse VTT timestamp lines: "HH:MM:SS.mmm --> HH:MM:SS.mmm"
_VTT_TIMESTAMP_RE = re.compile(
    r"(\d{2}:\d{2}:\d{2}\.\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2}\.\d{3})"
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def extract_isvp_url(html: str) -> dict | None:
    """Extract ISVP iframe parameters from hearing detail page HTML.

    Parses the first ``<iframe>`` whose ``src`` matches the senate.gov/isvp/
    pattern and returns the query-string parameters.

    Returns:
        ``{"comm": "foreign", "filename": "foreign020426", ...}`` or ``None``
        if no ISVP iframe is found.
    """
    match = _ISVP_IFRAME_RE.search(html)
    if not match:
        return None

    iframe_url = match.group(1)
    parsed = urlparse(iframe_url)
    params = parse_qs(parsed.query)

    comm = params.get("comm", [None])[0]
    filename = params.get("filename", [None])[0]

    if not comm or not filename:
        log.debug("ISVP iframe found but missing comm/filename: %s", iframe_url)
        return None

    result = {"comm": comm, "filename": filename}
    # Capture any other useful params (type1, type2, etc.)
    for key in ("type1", "type2"):
        val = params.get(key, [None])[0]
        if val:
            result[key] = val

    log.debug("Extracted ISVP params: %s", result)
    return result


def fetch_isvp_captions(comm: str, filename: str) -> str | None:
    """Fetch and merge VTT captions from an ISVP HLS stream.

    Steps:
        1. Look up stream ID from ``_ISVP_STREAMS``.
        2. Fetch ``master.m3u8`` to locate the subtitle playlist.
        3. Fetch the subtitle playlist (``text_1.m3u8``).
        4. Download all VTT segments listed in the subtitle playlist.
        5. Merge and deduplicate the rolling CART captions.

    Args:
        comm: ISVP committee code (e.g. ``"foreign"``).
        filename: ISVP filename (e.g. ``"foreign020426"``).

    Returns:
        Cleaned caption text as a string, or ``None`` on failure.
    """
    stream_id = _ISVP_STREAMS.get(comm)
    if not stream_id:
        log.warning("Unknown ISVP comm code: %r", comm)
        return None

    base_url = _AKAMAI_BASE.format(
        stream_id=stream_id, comm=comm, filename=filename
    )
    master_url = f"{base_url}/master.m3u8"

    log.info("Fetching ISVP master playlist: %s", master_url)

    client = get_http_client(retries=2, timeout=30.0)
    with client:
        # ------------------------------------------------------------------
        # Step 1: fetch master manifest
        # ------------------------------------------------------------------
        master_text = _fetch_url(client, master_url)
        if master_text is None:
            return None

        # ------------------------------------------------------------------
        # Step 2: locate subtitle playlist URI in the master manifest
        # ------------------------------------------------------------------
        subtitle_uri = _find_subtitle_uri(master_text)
        if not subtitle_uri:
            log.warning("No subtitle track found in master manifest for %s/%s", comm, filename)
            return None

        subtitle_url = urljoin(master_url, subtitle_uri)
        log.debug("Subtitle playlist URL: %s", subtitle_url)

        # ------------------------------------------------------------------
        # Step 3: fetch subtitle playlist
        # ------------------------------------------------------------------
        subtitle_text = _fetch_url(client, subtitle_url)
        if subtitle_text is None:
            return None

        # ------------------------------------------------------------------
        # Step 4: download VTT segments
        # ------------------------------------------------------------------
        segment_uris = _parse_segment_uris(subtitle_text)
        if not segment_uris:
            log.warning("No VTT segments found in subtitle playlist for %s/%s", comm, filename)
            return None

        log.info("Downloading %d VTT segments for %s/%s", len(segment_uris), comm, filename)

        segments: list[str] = []
        empty_count = 0
        for uri in segment_uris:
            seg_url = urljoin(subtitle_url, uri)
            seg_text = _fetch_url(client, seg_url)
            if seg_text is not None:
                segments.append(seg_text)
                # Track consecutive empties for early-termination logging
                if _is_empty_vtt(seg_text):
                    empty_count += 1
                else:
                    empty_count = 0
            else:
                # A failed segment fetch is not fatal; skip and continue
                log.debug("Skipped segment: %s", seg_url)

    if not segments:
        log.warning("All VTT segments failed for %s/%s", comm, filename)
        return None

    # ------------------------------------------------------------------
    # Step 5: merge and deduplicate
    # ------------------------------------------------------------------
    text = _merge_vtt_segments(segments)
    if not text or len(text.strip()) < 50:
        log.warning(
            "ISVP captions too short (%d chars) for %s/%s — may be empty stream",
            len(text) if text else 0, comm, filename,
        )
        return None

    log.info(
        "ISVP captions: %d chars from %d segments for %s/%s",
        len(text), len(segments), comm, filename,
    )
    return text


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _fetch_url(client, url: str) -> str | None:
    """Fetch a URL and return its text, or None on error."""
    try:
        resp = client.get(url)
        if resp.status_code != 200:
            log.debug("HTTP %d for %s", resp.status_code, url)
            return None
        return resp.text
    except Exception as e:
        log.debug("Fetch error for %s: %s", url, e)
        return None


def _find_subtitle_uri(master_text: str) -> str | None:
    """Parse the master.m3u8 to find the subtitle playlist URI.

    Looks for an ``#EXT-X-MEDIA`` tag with ``TYPE=SUBTITLES`` and extracts
    its ``URI`` attribute.  Falls back to scanning for any ``text_*.m3u8``
    reference.
    """
    # Strategy 1: EXT-X-MEDIA with TYPE=SUBTITLES
    for line in master_text.splitlines():
        if "#EXT-X-MEDIA" in line and "TYPE=SUBTITLES" in line.upper():
            uri_match = re.search(r'URI="([^"]+)"', line)
            if uri_match:
                return uri_match.group(1)

    # Strategy 2: look for a text_*.m3u8 reference anywhere in the manifest
    for line in master_text.splitlines():
        stripped = line.strip()
        if re.match(r".*text_\d+\.m3u8", stripped, re.IGNORECASE):
            # Could be a bare URI line or inside a tag
            uri_match = re.search(r'URI="([^"]*text_\d+\.m3u8[^"]*)"', stripped)
            if uri_match:
                return uri_match.group(1)
            # Bare URI line
            if stripped.endswith(".m3u8"):
                return stripped

    return None


def _parse_segment_uris(playlist_text: str) -> list[str]:
    """Extract VTT segment URIs from a subtitle playlist."""
    uris: list[str] = []
    for line in playlist_text.splitlines():
        stripped = line.strip()
        # Skip empty lines, comments, and HLS tags
        if not stripped or stripped.startswith("#"):
            continue
        # VTT segment URIs typically end in .vtt or .webvtt
        if stripped.endswith((".vtt", ".webvtt")) or "text_" in stripped:
            uris.append(stripped)
    return uris


def _is_empty_vtt(vtt_text: str) -> bool:
    """Check whether a VTT segment contains no actual caption cues."""
    for line in vtt_text.splitlines():
        line = line.strip()
        # Skip headers, empty lines, timestamps, and STYLE blocks
        if not line or line.startswith("WEBVTT") or line.startswith("STYLE"):
            continue
        if line.startswith("X-TIMESTAMP") or line.startswith("NOTE"):
            continue
        if _VTT_TIMESTAMP_RE.match(line):
            continue
        # Any other non-blank line is caption text
        if line and not line.startswith("::cue"):
            return False
    return True


def _parse_vtt_timestamp(ts: str) -> float:
    """Convert "HH:MM:SS.mmm" to seconds as a float."""
    parts = ts.split(":")
    hours = int(parts[0])
    minutes = int(parts[1])
    sec_parts = parts[2].split(".")
    seconds = int(sec_parts[0])
    millis = int(sec_parts[1]) if len(sec_parts) > 1 else 0
    return hours * 3600 + minutes * 60 + seconds + millis / 1000.0


def _merge_vtt_segments(segments: list[str]) -> str:
    """Merge multiple VTT segments into clean transcript text.

    The ISVP VTT uses rolling CART/stenographer captions.  Each cue contains
    the FULL visible screen text at that moment (typically 2-3 lines).  As
    new words arrive, older lines scroll off the top.  For example, three
    consecutive cues might look like::

        00:53:48.000 --> 00:53:51.294
        ENERGY SINCE THE START OF UKRAINE.

        00:53:51.294 --> 00:53:51.394
        ENERGY SINCE THE START OF UKRAINE.
        BEAR

        00:53:51.828 --> 00:53:52.162
        UKRAINE.
        BEAR WITH ME, THIS IS CLICKING

    Strategy — "screen diff":
        1. Parse all cues from all segments, sort chronologically.
        2. Collapse identical consecutive cues (segment boundary overlap).
        3. For each consecutive pair of screen states, extract the NEW text
           that appeared (the suffix of the new state not present in the old).
        4. Concatenate all new-text fragments into a clean transcript.
        5. Insert paragraph breaks at significant time gaps (pauses).

    Returns:
        Plain text transcript with paragraph breaks at natural pauses.
    """
    # Step 1: parse all cues from all segments
    all_cues: list[tuple[float, float, str]] = []
    for seg_text in segments:
        cues = _parse_vtt_cues(seg_text)
        all_cues.extend(cues)

    if not all_cues:
        return ""

    # Sort by end time (the moment this screen state is displayed),
    # then by start time for ties.
    all_cues.sort(key=lambda c: (c[1], c[0]))

    # Step 2: collapse duplicates — keep only distinct screen states in order.
    # Two cues are "same" if their text is identical.
    distinct: list[tuple[float, float, str]] = []
    for cue in all_cues:
        if distinct and cue[2] == distinct[-1][2]:
            continue
        distinct.append(cue)

    if not distinct:
        return ""

    # Step 3: extract new text from each screen transition.
    #
    # Each cue is the full screen.  When the screen changes, the new cue
    # usually shares a suffix with the old cue (old lines still visible)
    # plus new text appended.  OR old text scrolled off and new text
    # appeared at the bottom.
    #
    # We find the longest common suffix between consecutive screen states
    # (measured in words) and emit only the truly new words.

    fragments: list[tuple[float, str]] = []  # (timestamp, new_text)

    # The first cue's text is entirely "new"
    fragments.append((distinct[0][1], distinct[0][2]))

    for i in range(1, len(distinct)):
        prev_text = distinct[i - 1][2]
        curr_text = distinct[i][2]
        timestamp = distinct[i][1]

        new_part = _extract_new_text(prev_text, curr_text)
        if new_part:
            fragments.append((timestamp, new_part))

    if not fragments:
        return ""

    # Step 4: deduplicate consecutive identical fragments
    deduped: list[tuple[float, str]] = []
    for ts, text in fragments:
        clean = text.strip()
        if not clean:
            continue
        if deduped and clean == deduped[-1][1]:
            continue
        deduped.append((ts, clean))

    if not deduped:
        return ""

    # Step 5: assemble into paragraphs with breaks at time gaps
    GAP_THRESHOLD = 8.0  # seconds — pause between speakers/topics
    paragraphs: list[str] = []
    current_words: list[str] = []
    prev_ts = deduped[0][0]

    for ts, text in deduped:
        if current_words and (ts - prev_ts) > GAP_THRESHOLD:
            paragraphs.append(" ".join(current_words))
            current_words = []
        current_words.append(text)
        prev_ts = ts

    if current_words:
        paragraphs.append(" ".join(current_words))

    result = "\n\n".join(paragraphs)

    # Clean up common VTT artifacts
    result = re.sub(r"<[^>]+>", "", result)        # strip HTML tags (e.g. <c>)
    result = re.sub(r"\s{2,}", " ", result)         # collapse multiple spaces
    result = re.sub(r" *\n *\n *", "\n\n", result)  # normalize para breaks
    return result.strip()


def _extract_new_text(prev_screen: str, curr_screen: str) -> str:
    """Given two consecutive screen states, return the text that is new.

    The rolling caption display keeps some old text visible while appending
    new words.  We find the overlap between the end of ``prev_screen`` and
    the start of ``curr_screen`` (measured in words), then return only the
    words in ``curr_screen`` that are genuinely new.

    Handles partial words at boundaries: CART stenography sometimes shows a
    partial word (e.g. "MUC") that becomes complete in the next cue ("MUCH").
    The overlap matcher allows the last word of the prev suffix / first word
    of curr to differ if one is a prefix of the other.

    If there is no word-level overlap (e.g. a completely new screen after a
    long pause), the entire ``curr_screen`` is returned.
    """
    prev_words = prev_screen.split()
    curr_words = curr_screen.split()

    if not curr_words:
        return ""
    if not prev_words:
        return " ".join(curr_words)

    # Find the longest suffix of prev_words that matches a prefix of curr_words.
    # Allow fuzzy matching on the boundary word (partial word from CART).
    max_overlap = min(len(prev_words), len(curr_words))
    best_overlap = 0

    for overlap_len in range(1, max_overlap + 1):
        prev_suffix = prev_words[-overlap_len:]
        curr_prefix = curr_words[:overlap_len]

        if prev_suffix == curr_prefix:
            best_overlap = overlap_len
        elif _fuzzy_word_match(prev_suffix, curr_prefix):
            best_overlap = overlap_len

    if best_overlap > 0:
        # Return only the new words after the overlap
        new_words = curr_words[best_overlap:]
        return " ".join(new_words) if new_words else ""
    else:
        # No exact or fuzzy overlap found — completely new screen
        return " ".join(curr_words)


def _fuzzy_word_match(prev_suffix: list[str], curr_prefix: list[str]) -> bool:
    """Check if prev_suffix ~= curr_prefix, allowing partial words at boundaries.

    The CART stenographer system sometimes displays partial words that get
    completed in the next cue.  For example::

        prev: [..., "IS", "NOT", "MUC"]
        curr: ["IS", "NOT", "MUCH", "BETTER"]

    The interior words must match exactly.  Only the FIRST word of curr_prefix
    is allowed to be a completion of the LAST word of prev_suffix (or vice
    versa), and the LAST word of prev_suffix can be a prefix of the
    corresponding curr_prefix word.

    Additionally, the first word of curr_prefix may be a completion of the
    last word of prev_suffix (prev ends with partial).
    """
    if len(prev_suffix) != len(curr_prefix):
        return False
    if len(prev_suffix) == 0:
        return True

    # Check all words except the boundary where a partial word might be
    # The partial word can be at position 0 (first word of the overlap)
    # meaning the last word of prev could be a partial.
    # It can also be at the last position.

    # Strategy: allow exactly one word position to be a prefix match
    # (either prev[i] is a prefix of curr[i], meaning CART partial).
    # All other positions must match exactly.
    mismatch_count = 0
    for i in range(len(prev_suffix)):
        if prev_suffix[i] == curr_prefix[i]:
            continue
        # Check if one is a prefix of the other (partial word)
        if (prev_suffix[i].startswith(curr_prefix[i]) or
                curr_prefix[i].startswith(prev_suffix[i])):
            mismatch_count += 1
            if mismatch_count > 1:
                return False
        else:
            return False

    return mismatch_count <= 1


def _parse_vtt_cues(vtt_text: str) -> list[tuple[float, float, str]]:
    """Parse a single VTT segment into a list of (start_secs, end_secs, text) cues."""
    cues: list[tuple[float, float, str]] = []
    lines = vtt_text.splitlines()
    i = 0
    n = len(lines)

    while i < n:
        line = lines[i].strip()

        # Look for timestamp lines
        ts_match = _VTT_TIMESTAMP_RE.match(line)
        if ts_match:
            start = _parse_vtt_timestamp(ts_match.group(1))
            end = _parse_vtt_timestamp(ts_match.group(2))

            # Collect all text lines until blank line or next timestamp or EOF
            text_lines: list[str] = []
            i += 1
            while i < n:
                tl = lines[i]
                tl_stripped = tl.strip()
                if not tl_stripped:
                    break
                if _VTT_TIMESTAMP_RE.match(tl_stripped):
                    break
                text_lines.append(tl_stripped)
                i += 1

            text = " ".join(text_lines).strip()
            if text:
                cues.append((start, end, text))
        else:
            i += 1

    return cues
