"""C-SPAN caption discovery and extraction for congressional hearings.

Discovers hearing videos via C-SPAN search (using sponsorid per committee)
and extracts broadcast-quality closed captions via the transcript JSON API.
"""

from __future__ import annotations

import json
import logging
import re
import time as _time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote_plus, urlparse

import config

log = logging.getLogger(__name__)

# Rate limiting between C-SPAN requests
_last_request: dict[str, float] = {}
_MIN_DELAY = 4.0  # be polite to C-SPAN — WAF is aggressive

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def _rate_limit(domain: str = "www.c-span.org") -> None:
    now = _time.monotonic()
    last = _last_request.get(domain, 0)
    wait = _MIN_DELAY - (now - last)
    if wait > 0:
        _time.sleep(wait)
    _last_request[domain] = _time.monotonic()


# ---------------------------------------------------------------------------
# C-SPAN discovery: find hearing videos for our committees via search
# ---------------------------------------------------------------------------

def discover_cspan(committees: dict, days: int = 7,
                    active_keys: set[str] | None = None) -> dict[str, list[dict]]:
    """Search C-SPAN for recent hearing videos per committee.

    Uses the C-SPAN search page with sponsorid filter to find program URLs.
    Each committee with a cspan_id gets one search-page load.

    Args:
        committees: Full committee dict from config
        days: How many days back to search
        active_keys: If provided, only search these committee keys (those with
            hearings from other discovery sources). Drastically reduces requests
            to avoid C-SPAN's aggressive CloudFront WAF.

    Returns:
        {committee_key: [{title, date, url, program_id}, ...]}
    """
    cspan_committees = {
        key: meta for key, meta in committees.items()
        if meta.get("cspan_id")
    }
    # Filter to only active committees if specified
    if active_keys is not None:
        cspan_committees = {
            key: meta for key, meta in cspan_committees.items()
            if key in active_keys
        }

    if not cspan_committees:
        log.info("No committees to check on C-SPAN (active_keys filter applied)")
        return {}

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("playwright not installed, skipping C-SPAN discovery")
        return {}

    cutoff = datetime.now() - timedelta(days=days)
    results: dict[str, list[dict]] = {}

    log.info("C-SPAN discovery: checking %d committees", len(cspan_committees))

    # CloudFront WAF tracks by IP and triggers captcha aggressively.
    # Rotate browser context every _BATCH_SIZE committees and detect WAF.
    _BATCH_SIZE = 5
    _MAX_WAF_RETRIES = 1  # retry once with a new browser after WAF
    waf_hits = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=_UA)
        page = context.new_page()
        pages_loaded = 0

        for key, meta in cspan_committees.items():
            # Rotate context to avoid WAF captcha
            if pages_loaded > 0 and pages_loaded % _BATCH_SIZE == 0:
                context.close()
                context = browser.new_context(user_agent=_UA)
                page = context.new_page()
                log.debug("C-SPAN: rotated browser context after %d pages", pages_loaded)

            cspan_id = meta["cspan_id"]
            search_url = (
                f"https://www.c-span.org/search/?query=&searchtype=Videos"
                f"&sponsorid%5B%5D={cspan_id}&sort=Most+Recent+Event"
            )
            _rate_limit()

            try:
                page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(6000)  # let search results render
                pages_loaded += 1

                # WAF detection: check for captcha page
                body_text = (page.inner_text("body") or "")[:300]
                if "confirm you are human" in body_text.lower():
                    waf_hits += 1
                    log.warning("C-SPAN WAF captcha detected at page %d (%s)",
                                pages_loaded, key)
                    if waf_hits > _MAX_WAF_RETRIES:
                        log.warning("C-SPAN WAF: aborting after %d captcha(s), "
                                    "collected %d hearings so far", waf_hits,
                                    sum(len(v) for v in results.values()))
                        break
                    # Retry: close everything, wait, new browser
                    context.close()
                    browser.close()
                    log.info("C-SPAN: waiting 30s for WAF cooldown...")
                    _time.sleep(30)
                    browser = p.chromium.launch(headless=True)
                    context = browser.new_context(user_agent=_UA)
                    page = context.new_page()
                    pages_loaded = 0
                    # Retry this committee
                    _rate_limit()
                    page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(6000)
                    pages_loaded += 1
                    body_text = (page.inner_text("body") or "")[:300]
                    if "confirm you are human" in body_text.lower():
                        log.warning("C-SPAN WAF still active after cooldown, aborting")
                        break

                hearings = _parse_search_results(page, cutoff)
                if hearings:
                    results[key] = hearings
                    log.info("  C-SPAN %s: %d hearings found", key, len(hearings))
                else:
                    log.debug("  C-SPAN %s: no recent hearings", key)

            except Exception as e:
                log.warning("C-SPAN discovery failed for %s: %s", key, e)
                pages_loaded += 1

        browser.close()

    total = sum(len(v) for v in results.values())
    log.info("C-SPAN discovery: %d hearings across %d committees", total, len(results))
    return results


def _parse_search_results(page, cutoff: datetime) -> list[dict]:
    """Parse program listings from a C-SPAN search results page.

    Search results contain /program/ links with dates in parent text:
        FEBRUARY 5, 2026
        LAST AIRED FEBRUARY 7, 2026
        Treasury Secy. Bessent Testifies Before Congress
    """
    hearings = []
    seen_program_ids = set()

    # Find all program links (each result has two: image + title)
    items = page.query_selector_all("a[href*='/program/']")

    for item in items:
        try:
            href = item.get_attribute("href") or ""
            if "/program/" not in href:
                continue

            # Normalize URL
            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("/"):
                href = "https://www.c-span.org" + href

            # Get title from link text — skip image links (empty text)
            title = (item.inner_text() or "").strip()
            if not title or len(title) < 10:
                continue

            # Extract program ID from URL: /program/.../672588
            prog_match = re.search(r"/program/[^/]+/[^/]+/(\d+)", href)
            if not prog_match:
                continue
            program_id = prog_match.group(1)

            # Dedup by program ID (after title check to skip image links)
            if program_id in seen_program_ids:
                continue
            seen_program_ids.add(program_id)

            # Get date from parent element text
            parent = item.query_selector("xpath=..")
            if not parent:
                continue
            parent_text = (parent.inner_text() or "").strip()

            # First date line is the event date (e.g., "FEBRUARY 5, 2026")
            date_match = re.search(
                r"(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|"
                r"SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s+(\d{1,2}),?\s+(\d{4})",
                parent_text,
            )
            if not date_match:
                continue

            month_str = date_match.group(1).capitalize()
            day = int(date_match.group(2))
            year = int(date_match.group(3))
            try:
                date_obj = datetime.strptime(f"{month_str} {day} {year}", "%B %d %Y")
            except ValueError:
                continue

            if date_obj < cutoff:
                continue

            hearings.append({
                "title": title,
                "date": date_obj.strftime("%Y-%m-%d"),
                "url": href,
                "program_id": program_id,
            })

        except Exception as e:
            log.debug("Error parsing C-SPAN search result: %s", e)
            continue

    return hearings


# ---------------------------------------------------------------------------
# C-SPAN transcript extraction via JSON API
# ---------------------------------------------------------------------------

def fetch_cspan_transcript(
    video_url: str,
    output_dir: Path,
    witnesses: list[dict] | None = None,
) -> Path | None:
    """Fetch C-SPAN transcript via the internal JSON API.

    Loads the program page (to pass CloudFront WAF), then fetches the
    transcript API endpoint from within the page context.

    Args:
        video_url: C-SPAN program URL (e.g., https://www.c-span.org/program/.../672588)
        output_dir: Directory to write transcript file
        witnesses: Optional witness list for speaker identification

    Returns:
        Path to transcript file, or None if transcript not available
    """
    # Extract program ID from URL
    prog_match = re.search(r"/(\d+)/?$", video_url)
    if not prog_match:
        log.warning("Cannot extract program ID from URL: %s", video_url)
        return None
    program_id = prog_match.group(1)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("playwright not installed, cannot fetch C-SPAN transcript")
        return None

    log.info("Fetching C-SPAN transcript for program %s", program_id)
    transcript_json = None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=_UA)
        page = context.new_page()

        try:
            _rate_limit()
            page.goto(video_url, wait_until="domcontentloaded", timeout=45000)
            page.wait_for_timeout(5000)

            # Fetch transcript API from within the page context (same-origin,
            # passes CloudFront WAF cookie automatically)
            transcript_json = page.evaluate("""
                async (programId) => {
                    try {
                        const resp = await fetch(
                            '/common/services/transcript/?videoId=' + programId
                            + '&videoType=program&transcriptType=cc&transcriptQuery='
                        );
                        if (!resp.ok) return null;
                        return await resp.text();
                    } catch (e) {
                        return null;
                    }
                }
            """, program_id)

        except Exception as e:
            log.warning("Error loading C-SPAN page %s: %s", video_url, e)
        finally:
            browser.close()

    if not transcript_json:
        log.info("No transcript available for program %s", program_id)
        return None

    # Parse JSON
    try:
        data = json.loads(transcript_json)
    except (json.JSONDecodeError, TypeError):
        log.warning("Invalid transcript JSON for program %s", program_id)
        return None

    parts = data.get("parts")
    if not parts:
        log.info("Transcript has no parts for program %s", program_id)
        return None

    # Build readable transcript from parts
    transcript = _build_transcript(parts)

    if not transcript.strip():
        log.warning("Empty transcript after processing for program %s", program_id)
        return None

    # Write transcript
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "cspan_transcript.txt"
    output_path.write_text(transcript)

    log.info(
        "C-SPAN transcript: %d chars, %d segments -> %s",
        len(transcript), len(parts), output_path,
    )
    return output_path


# ---------------------------------------------------------------------------
# Transcript processing
# ---------------------------------------------------------------------------

def _build_transcript(parts: list[dict]) -> str:
    """Build a readable transcript from C-SPAN API parts.

    Each part has: cc_name (speaker label), personid, text (ALL CAPS),
    secAppOffset (seconds from start).
    """
    sections = []
    prev_speaker = None

    for part in parts:
        text = (part.get("text") or "").strip()
        if not text:
            continue

        # Clean up the text
        text = _normalize_caps(text)
        # Collapse internal line breaks from caption formatting
        text = re.sub(r"\n+", " ", text)
        text = re.sub(r"\s{2,}", " ", text).strip()

        # Determine speaker label
        speaker = (part.get("cc_name") or "").strip()
        if speaker == ">>" or not speaker:
            speaker = None

        if speaker and speaker != prev_speaker:
            sections.append(f"\n{speaker}:\n{text}")
            prev_speaker = speaker
        elif not speaker and prev_speaker:
            # New unlabeled speaker segment — mark transition
            sections.append(f"\n[SPEAKER]:\n{text}")
            prev_speaker = None
        else:
            # Continuation of same speaker
            if sections:
                sections.append(text)
            else:
                sections.append(text)

    return "\n\n".join(sections)


def _normalize_caps(text: str) -> str:
    """Convert ALL CAPS text to sentence case.

    Preserves common abbreviations and proper nouns.
    """
    upper_ratio = sum(1 for c in text if c.isupper()) / max(len(text), 1)
    if upper_ratio < 0.6:
        return text

    # Known abbreviations to preserve
    preserve = {
        "U.S.", "USA", "GDP", "CBO", "OMB", "GAO", "FBI", "CIA", "NSA",
        "DOD", "DOJ", "EPA", "IRS", "SEC", "FDIC", "FED", "FOMC",
        "NATO", "UN", "EU", "IMF", "WHO", "COVID", "AI", "DOGE",
        "HHS", "HUD", "DHS", "FEMA", "SBA", "NIH", "CDC", "FDA",
        "CFPB", "FHFA", "FSOC", "OCC", "CFTC", "NCUA",
    }

    sentences = re.split(r"([.!?]\s+)", text)
    result = []

    for segment in sentences:
        if re.match(r"^[.!?]\s+$", segment):
            result.append(segment)
            continue

        words = segment.split()
        processed = []
        for j, word in enumerate(words):
            upper_word = word.upper().rstrip(".,;:!?'\"")
            if upper_word in preserve:
                processed.append(word)
            elif j == 0:
                processed.append(word.capitalize())
            else:
                processed.append(word.lower())

        result.append(" ".join(processed))

    return "".join(result)
