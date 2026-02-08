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

_MAX_CSPAN_SEARCHES = 12  # stay under WAF captcha threshold (~15 pages/session)

# Batch cooldown: pause between groups of searches to look more human-like
BATCH_SIZE = 4
BATCH_COOLDOWN = 60  # seconds between batches


def discover_cspan(committees: dict, days: int = 7,
                    active_keys: set[str] | None = None,
                    state=None) -> list[dict]:
    """Search C-SPAN for recent hearing videos using a 3-layer strategy.

    Layer 1: Broad search (1 WAF slot) — unfiltered search for recent hearings
             across all committees.
    Layer 2: Targeted search — per-committee search only for committees with
             known unmatched hearings (from active_keys not covered by Layer 1).
    Layer 3: Rotation — search stale committees (not searched recently) to
             ensure all 36 committees get checked every ~3 days.

    Intra-run cooldown batching: every BATCH_SIZE pages, pause BATCH_COOLDOWN
    seconds to spread requests and look more human-like.

    Args:
        committees: Full committee dict from config
        days: How many days back to search
        active_keys: If provided, committees with known hearings needing C-SPAN match.
        state: Optional State instance for rotation tracking (Layer 3).

    Returns:
        [{title, date, url, program_id}, ...] — flat list.
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
    pages_loaded = 0
    waf_blocked = False

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=_UA)
        page = context.new_page()

        def _rotate_context_if_needed():
            nonlocal context, page, pages_loaded
            if pages_loaded > 0 and pages_loaded % 5 == 0:
                context.close()
                context = browser.new_context(user_agent=_UA)
                page = context.new_page()
                log.debug("C-SPAN: rotated browser context after %d pages", pages_loaded)

        def _batch_cooldown_if_needed():
            nonlocal pages_loaded
            if pages_loaded > 0 and pages_loaded % BATCH_SIZE == 0:
                log.info("C-SPAN: batch cooldown (%ds) after %d pages",
                         BATCH_COOLDOWN, pages_loaded)
                _time.sleep(BATCH_COOLDOWN)

        def _search_page(search_url: str, label: str) -> list[dict]:
            """Load a C-SPAN search URL and return parsed results.

            Returns parsed hearings list, or empty list on failure.
            Sets nonlocal waf_blocked=True if WAF captcha detected and
            retry fails.
            """
            nonlocal pages_loaded, waf_blocked, context, page, browser

            _rotate_context_if_needed()
            _batch_cooldown_if_needed()
            _rate_limit()

            try:
                page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(7000)
                pages_loaded += 1

                # WAF detection
                body_text = (page.inner_text("body") or "")[:300]
                if "confirm you are human" in body_text.lower():
                    log.warning("C-SPAN WAF captcha at page %d (%s), "
                                "retrying with 60s cooldown...", pages_loaded, label)
                    context.close()
                    browser.close()
                    _time.sleep(60)
                    browser = p.chromium.launch(headless=True)
                    context = browser.new_context(user_agent=_UA)
                    page = context.new_page()
                    pages_loaded = 0
                    _rate_limit()
                    page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(7000)
                    pages_loaded += 1
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
                pages_loaded += 1
                return []

        # ---------------------------------------------------------------
        # Layer 1: Broad search (1 WAF slot — all committees)
        # ---------------------------------------------------------------
        log.info("C-SPAN Layer 1: broad search (all committees)")
        broad_url = (
            "https://www.c-span.org/search/?query=&searchtype=Videos"
            "&sort=Most+Recent+Event"
        )
        broad_hearings = _search_page(broad_url, "broad")
        covered_committees: set[str] = set()

        if not waf_blocked:
            for h in broad_hearings:
                if h["program_id"] not in seen_ids:
                    seen_ids.add(h["program_id"])
                    # Broad results have no committee_key — set to None
                    h["committee_key"] = None
                    all_results.append(h)

            log.info("C-SPAN Layer 1: %d hearings from broad search", len(broad_hearings))

        # ---------------------------------------------------------------
        # Layer 2: Targeted per-committee search (unmatched active committees)
        # ---------------------------------------------------------------
        if not waf_blocked and active_keys:
            # Only search committees that have known hearings needing C-SPAN URLs
            # AND weren't covered by broad results (we can't tell from broad results
            # which committees they cover since they lack committee_key, but we can
            # check after matching — for now, search active committees with budget)
            targeted = [
                (key, meta) for key, meta in cspan_committees
                if key in active_keys
            ]
            # Sort by tier
            targeted.sort(key=lambda x: x[1].get("tier", 99))

            remaining_budget = _MAX_CSPAN_SEARCHES - pages_loaded
            if len(targeted) > remaining_budget:
                log.info("C-SPAN Layer 2: capping targeted from %d to %d (budget)",
                         len(targeted), remaining_budget)
                targeted = targeted[:remaining_budget]

            if targeted:
                log.info("C-SPAN Layer 2: targeted search for %d active committees",
                         len(targeted))
                consecutive_empty = 0

                for key, meta in targeted:
                    if waf_blocked:
                        break
                    if pages_loaded >= _MAX_CSPAN_SEARCHES:
                        log.info("C-SPAN: WAF budget exhausted at %d pages", pages_loaded)
                        break

                    cspan_id = meta["cspan_id"]
                    search_url = (
                        f"https://www.c-span.org/search/?query=&searchtype=Videos"
                        f"&sponsorid%5B%5D={cspan_id}&sort=Most+Recent+Event"
                    )
                    hearings = _search_page(search_url, key)
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
                        log.info("  C-SPAN %s: %d hearings found", key, new)
                    else:
                        consecutive_empty += 1
                        log.debug("  C-SPAN %s: no recent hearings", key)
                        if consecutive_empty >= 3:
                            raw_links = page.query_selector_all("a[href*='/program/']")
                            if len(raw_links) == 0:
                                log.warning("C-SPAN: %d consecutive empty pages — "
                                            "likely WAF silent block, aborting",
                                            consecutive_empty)
                                break

        # ---------------------------------------------------------------
        # Layer 3: Stale committee rotation (fill remaining budget)
        # ---------------------------------------------------------------
        if not waf_blocked and state:
            remaining_budget = _MAX_CSPAN_SEARCHES - pages_loaded
            if remaining_budget > 0:
                stale_keys = state.get_stale_committees(max_age_days=3)
                # Also include committees never searched
                all_cspan_keys = {key for key, meta in cspan_committees}
                searched_keys = set(stale_keys) | covered_committees
                # Find never-searched committees
                never_searched = [
                    key for key in all_cspan_keys
                    if key not in covered_committees
                    and state.get_cspan_search_age(key) is None
                ]
                # Combine: never-searched first, then stale (oldest first)
                rotation_queue = never_searched + [
                    k for k in stale_keys if k not in covered_committees
                ]
                rotation_queue = rotation_queue[:remaining_budget]

                if rotation_queue:
                    log.info("C-SPAN Layer 3: rotating %d stale/unsearched committees",
                             len(rotation_queue))
                    # Build lookup for committee metadata
                    cspan_meta = {key: meta for key, meta in cspan_committees}

                    for key in rotation_queue:
                        if waf_blocked:
                            break
                        if pages_loaded >= _MAX_CSPAN_SEARCHES:
                            log.info("C-SPAN: WAF budget exhausted at %d pages",
                                     pages_loaded)
                            break

                        meta = cspan_meta.get(key)
                        if not meta:
                            continue
                        cspan_id = meta["cspan_id"]
                        search_url = (
                            f"https://www.c-span.org/search/?query=&searchtype=Videos"
                            f"&sponsorid%5B%5D={cspan_id}&sort=Most+Recent+Event"
                        )
                        hearings = _search_page(search_url, f"rotate:{key}")
                        if waf_blocked:
                            break

                        new = 0
                        for h in hearings:
                            if h["program_id"] not in seen_ids:
                                seen_ids.add(h["program_id"])
                                h["committee_key"] = key
                                all_results.append(h)
                                new += 1

                        state.record_cspan_search(key, new)
                        if new:
                            log.info("  C-SPAN rotate %s: %d hearings found", key, new)

        browser.close()

    log.info("C-SPAN discovery: %d hearings from %d pages "
             "(L1=broad, L2=%d targeted, L3=rotation)",
             len(all_results), pages_loaded, len(covered_committees))
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
