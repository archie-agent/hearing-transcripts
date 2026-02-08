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

_MAX_CSPAN_SEARCHES = 8  # WAF triggers captcha after ~2 pages; each retry costs ~70s

# Batch cooldown: pause between groups of searches to look more human-like.
# WAF is very aggressive (~2 pages), so we cool down after every 2 searches.
BATCH_SIZE = 2
BATCH_COOLDOWN = 45  # seconds between batches


def discover_cspan(committees: dict, days: int = 7,
                    active_keys: set[str] | None = None,
                    state=None) -> list[dict]:
    """Search C-SPAN for recent hearing videos using a 2-layer strategy.

    Layer 1 (targeted): Per-committee search for committees with known
             hearings from other sources (active_keys). Sorted by tier.
    Layer 2 (rotation): Search stale committees not searched recently,
             ensuring all committees get checked every ~5 days.

    WAF behavior: CloudFront captcha triggers after ~2 page loads.
    Recovery: close browser, wait 60s, relaunch — consistently works.
    Each search effectively takes ~70s due to captcha + cooldown cycle.
    Batch cooldown every BATCH_SIZE pages to reduce captcha frequency.

    Args:
        committees: Full committee dict from config
        days: How many days back to search
        active_keys: If provided, committees with known hearings needing C-SPAN match.
        state: Optional State instance for rotation tracking.

    Returns:
        [{title, date, url, program_id, committee_key}, ...] — flat list.
        Matching to specific hearings is done by the caller via date + title.
    """
    cspan_committees = [
        (key, meta) for key, meta in committees.items()
        if meta.get("cspan_id")
    ]

    if not cspan_committees:
        log.info("No committees to check on C-SPAN")
        return []

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("playwright not installed, skipping C-SPAN discovery")
        return []

    cutoff = datetime.now() - timedelta(days=days)

    all_results: list[dict] = []
    seen_ids: set[str] = set()
    searches_done = 0
    waf_blocked = False

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=_UA)
        page = context.new_page()

        def _search_committee(cspan_id: str, label: str) -> list[dict]:
            """Search C-SPAN for a single committee's hearings.

            Handles WAF captcha detection with 60s cooldown + retry.
            Returns parsed hearings list, or empty list on failure.
            Sets nonlocal waf_blocked=True if retry also fails.
            """
            nonlocal searches_done, waf_blocked, context, page, browser

            # Cooldown between batches to reduce captcha frequency
            if searches_done > 0 and searches_done % BATCH_SIZE == 0:
                log.info("C-SPAN: cooldown (%ds) after %d searches",
                         BATCH_COOLDOWN, searches_done)
                _time.sleep(BATCH_COOLDOWN)

            search_url = (
                f"https://www.c-span.org/search/?query=&searchtype=Videos"
                f"&sponsorid%5B%5D={cspan_id}&sort=Most+Recent+Event"
            )
            _rate_limit()

            try:
                page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(7000)
                searches_done += 1

                # WAF detection
                body_text = (page.inner_text("body") or "")[:300]
                if "confirm you are human" in body_text.lower():
                    log.info("C-SPAN WAF captcha at search %d (%s), "
                             "cooldown 60s...", searches_done, label)
                    context.close()
                    browser.close()
                    _time.sleep(60)
                    browser = p.chromium.launch(headless=True)
                    context = browser.new_context(user_agent=_UA)
                    page = context.new_page()
                    _rate_limit()
                    page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(7000)
                    body_text = (page.inner_text("body") or "")[:300]
                    if "confirm you are human" in body_text.lower():
                        log.warning("C-SPAN WAF still active after cooldown, "
                                    "aborting (collected %d hearings)",
                                    len(all_results))
                        waf_blocked = True
                        return []

                return _parse_search_results(page, cutoff)

            except Exception as e:
                log.warning("C-SPAN search failed for %s: %s", label, e)
                return []

        # ---------------------------------------------------------------
        # Build search queue: targeted (active) first, then rotation (stale)
        # ---------------------------------------------------------------
        search_queue: list[tuple[str, dict]] = []
        covered_committees: set[str] = set()

        # Layer 1: targeted — committees with known hearings needing C-SPAN
        if active_keys:
            targeted = [
                (key, meta) for key, meta in cspan_committees
                if key in active_keys
            ]
            targeted.sort(key=lambda x: x[1].get("tier", 99))
            search_queue.extend(targeted)

        # Layer 2: rotation — stale or never-searched committees
        if state:
            stale_keys = set(state.get_stale_committees(max_age_days=5))
            all_cspan_keys = {key for key, _ in cspan_committees}
            targeted_keys = {key for key, _ in search_queue}
            never_searched = [
                key for key in all_cspan_keys
                if key not in targeted_keys
                and state.get_cspan_search_age(key) is None
            ]
            stale_not_targeted = [
                k for k in state.get_stale_committees(max_age_days=5)
                if k not in targeted_keys
            ]
            rotation = never_searched + stale_not_targeted
            # Add rotation entries with metadata
            cspan_meta = {key: meta for key, meta in cspan_committees}
            for key in rotation:
                meta = cspan_meta.get(key)
                if meta and (key, meta) not in search_queue:
                    search_queue.append((key, meta))

        # Cap to budget
        if len(search_queue) > _MAX_CSPAN_SEARCHES:
            log.info("C-SPAN: capping queue from %d to %d (WAF budget)",
                     len(search_queue), _MAX_CSPAN_SEARCHES)
            search_queue = search_queue[:_MAX_CSPAN_SEARCHES]

        if not search_queue:
            log.info("C-SPAN: no committees to search")
            browser.close()
            return []

        n_targeted = sum(1 for k, _ in search_queue if active_keys and k in active_keys)
        n_rotation = len(search_queue) - n_targeted
        log.info("C-SPAN discovery: %d committees (%d targeted, %d rotation)",
                 len(search_queue), n_targeted, n_rotation)

        # ---------------------------------------------------------------
        # Execute searches
        # ---------------------------------------------------------------
        consecutive_empty = 0

        for key, meta in search_queue:
            if waf_blocked:
                break

            cspan_id = meta["cspan_id"]
            hearings = _search_committee(cspan_id, key)
            if waf_blocked:
                break

            new = 0
            for h in hearings:
                if h["program_id"] not in seen_ids:
                    seen_ids.add(h["program_id"])
                    h["committee_key"] = key
                    all_results.append(h)
                    new += 1

            covered_committees.add(key)
            if state:
                state.record_cspan_search(key, new)

            if new:
                consecutive_empty = 0
                log.info("  C-SPAN %s: %d hearings", key, new)
            else:
                consecutive_empty += 1
                log.debug("  C-SPAN %s: no recent hearings", key)
                # Silent WAF: page loads but no links rendered
                if consecutive_empty >= 4:
                    raw_links = page.query_selector_all("a[href*='/program/']")
                    if len(raw_links) == 0:
                        log.warning("C-SPAN: %d consecutive empty — "
                                    "likely WAF silent block, aborting",
                                    consecutive_empty)
                        break

        browser.close()

    log.info("C-SPAN discovery: %d hearings from %d searches "
             "(%d targeted, %d rotation)",
             len(all_results), searches_done,
             len(covered_committees & (active_keys or set())),
             len(covered_committees - (active_keys or set())))
    return all_results


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
