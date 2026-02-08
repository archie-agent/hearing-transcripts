"""C-SPAN caption discovery and extraction for congressional hearings.

Discovers hearing videos via C-SPAN search (using sponsorid per committee),
DuckDuckGo site-search, and extracts broadcast-quality closed captions via the
transcript JSON API.
"""

from __future__ import annotations

import json
import logging
import re
import time as _time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote_plus

import httpx

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


def _launch_cspan_browser(p):
    """Launch a Playwright browser configured for C-SPAN."""
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(user_agent=_UA)
    page = context.new_page()
    return browser, context, page


def discover_cspan_targeted(
    unmatched_hearings: list[dict],
    state=None,
    max_searches: int = 6,
) -> list[dict]:
    """Search C-SPAN for specific hearings by title keywords. WAF-limited.

    Only searches hearings that don't already have a C-SPAN URL (failed
    Google lookup) and haven't been searched before (tracked in state).

    Args:
        unmatched_hearings: [{id, title, date, committee_key, committee_name}, ...]
        state: State instance for tracking which hearings were searched
        max_searches: WAF budget cap

    Returns:
        [{hearing_id, cspan_url, program_id, title, date, committee_key}, ...]
    """
    if not unmatched_hearings:
        return []

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("playwright not installed, skipping targeted C-SPAN search")
        return []

    # Filter out already-searched hearings
    to_search = []
    for h in unmatched_hearings:
        if state and state.is_cspan_searched(h["id"]):
            continue
        to_search.append(h)

    if not to_search:
        log.info("C-SPAN targeted: all hearings already searched")
        return []

    if len(to_search) > max_searches:
        log.info("C-SPAN targeted: capping from %d to %d (WAF budget)",
                 len(to_search), max_searches)
        to_search = to_search[:max_searches]

    cutoff = datetime.now() - timedelta(days=30)  # generous for title matching
    results: list[dict] = []
    searches_done = 0
    waf_blocked = False

    with sync_playwright() as p:
        browser, context, page = _launch_cspan_browser(p)

        for h in to_search:
            if waf_blocked:
                break

            # Batch cooldown
            if searches_done > 0 and searches_done % BATCH_SIZE == 0:
                log.info("C-SPAN targeted: cooldown (%ds) after %d searches",
                         BATCH_COOLDOWN, searches_done)
                _time.sleep(BATCH_COOLDOWN)

            # Build search query from title keywords
            keywords = _extract_search_keywords(h["title"])
            if not keywords:
                if state:
                    state.record_cspan_title_search(h["id"], found=False)
                continue

            search_url = (
                f"https://www.c-span.org/search/?query={quote_plus(keywords)}"
                f"&searchtype=Videos&sort=Most+Recent+Event"
            )
            _rate_limit()

            try:
                page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(7000)
                searches_done += 1

                # WAF detection
                body_text = (page.inner_text("body") or "")[:300]
                if "confirm you are human" in body_text.lower():
                    log.info("C-SPAN targeted: WAF captcha at search %d, "
                             "cooldown 60s...", searches_done)
                    context.close()
                    browser.close()
                    _time.sleep(60)
                    browser, context, page = _launch_cspan_browser(p)
                    _rate_limit()
                    page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(7000)
                    body_text = (page.inner_text("body") or "")[:300]
                    if "confirm you are human" in body_text.lower():
                        log.warning("C-SPAN targeted: WAF still blocked after cooldown")
                        waf_blocked = True
                        break

                search_results = _parse_search_results(page, cutoff)

                # Match by date + keyword overlap
                found = False
                for sr in search_results:
                    if sr["date"] == h.get("date"):
                        sr["hearing_id"] = h["id"]
                        sr["committee_key"] = h.get("committee_key", "")
                        sr["cspan_url"] = sr["url"]
                        results.append(sr)
                        found = True
                        log.debug("C-SPAN targeted: matched '%s' -> %s",
                                  h["title"][:40], sr["program_id"])
                        break

                if state:
                    state.record_cspan_title_search(h["id"], found=found)

            except Exception as e:
                log.warning("C-SPAN targeted search failed for '%s': %s",
                            h["title"][:40], e)
                if state:
                    state.record_cspan_title_search(h["id"], found=False)

        browser.close()

    log.info("C-SPAN targeted: %d found from %d searches", len(results), searches_done)
    return results


def discover_cspan_rotation(
    committees: dict,
    days: int = 7,
    state=None,
) -> list[dict]:
    """Background committee rotation: search stale committees for new hearings.

    This is the original committee-based C-SPAN search, now demoted to
    weekly background duty. Most C-SPAN URL discovery is handled by
    discover_cspan_google() and discover_cspan_targeted().

    Args:
        committees: Full committee dict from config
        days: How many days back to search
        state: State instance for rotation tracking.

    Returns:
        [{title, date, url, program_id, committee_key}, ...] — flat list.
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
        log.warning("playwright not installed, skipping C-SPAN rotation")
        return []

    cutoff = datetime.now() - timedelta(days=days)

    all_results: list[dict] = []
    seen_ids: set[str] = set()
    searches_done = 0
    waf_blocked = False

    with sync_playwright() as p:
        browser, context, page = _launch_cspan_browser(p)

        def _search_committee(cspan_id: str, label: str) -> list[dict]:
            nonlocal searches_done, waf_blocked, context, page, browser

            if searches_done > 0 and searches_done % BATCH_SIZE == 0:
                log.info("C-SPAN rotation: cooldown (%ds) after %d searches",
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

                body_text = (page.inner_text("body") or "")[:300]
                if "confirm you are human" in body_text.lower():
                    log.info("C-SPAN rotation: WAF captcha at search %d (%s), "
                             "cooldown 60s...", searches_done, label)
                    context.close()
                    browser.close()
                    _time.sleep(60)
                    browser, context, page = _launch_cspan_browser(p)
                    _rate_limit()
                    page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(7000)
                    body_text = (page.inner_text("body") or "")[:300]
                    if "confirm you are human" in body_text.lower():
                        log.warning("C-SPAN rotation: WAF still blocked, aborting")
                        waf_blocked = True
                        return []

                return _parse_search_results(page, cutoff)

            except Exception as e:
                log.warning("C-SPAN rotation search failed for %s: %s", label, e)
                return []

        # Build rotation queue: stale or never-searched committees
        search_queue: list[tuple[str, dict]] = []

        if state:
            all_cspan_keys = {key for key, _ in cspan_committees}
            cspan_meta = {key: meta for key, meta in cspan_committees}

            never_searched = [
                key for key in all_cspan_keys
                if state.get_cspan_search_age(key) is None
            ]
            stale = state.get_stale_committees(max_age_days=5)

            rotation = never_searched + [k for k in stale if k not in never_searched]
            for key in rotation:
                meta = cspan_meta.get(key)
                if meta:
                    search_queue.append((key, meta))
        else:
            # Without state, search all committees (legacy behavior)
            search_queue = list(cspan_committees)

        if len(search_queue) > _MAX_CSPAN_SEARCHES:
            log.info("C-SPAN rotation: capping from %d to %d",
                     len(search_queue), _MAX_CSPAN_SEARCHES)
            search_queue = search_queue[:_MAX_CSPAN_SEARCHES]

        if not search_queue:
            log.info("C-SPAN rotation: no stale committees to search")
            browser.close()
            return []

        log.info("C-SPAN rotation: searching %d stale committees", len(search_queue))

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

            if state:
                state.record_cspan_search(key, new)

            if new:
                consecutive_empty = 0
                log.info("  C-SPAN rotation %s: %d hearings", key, new)
            else:
                consecutive_empty += 1
                log.debug("  C-SPAN rotation %s: no recent hearings", key)
                if consecutive_empty >= 4:
                    raw_links = page.query_selector_all(
                        "a[href*='/program/'], a[href*='/event/']"
                    )
                    if len(raw_links) == 0:
                        log.warning("C-SPAN rotation: %d consecutive empty — "
                                    "likely WAF silent block, aborting",
                                    consecutive_empty)
                        break

        browser.close()

    log.info("C-SPAN rotation: %d hearings from %d searches",
             len(all_results), searches_done)
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

    # Find all program/event links (each result has two: image + title)
    items = page.query_selector_all("a[href*='/program/'], a[href*='/event/']")

    for item in items:
        try:
            href = item.get_attribute("href") or ""
            if "/program/" not in href and "/event/" not in href:
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

            # Extract program ID from URL: /program/.../672588 or /event/.../434689
            prog_match = re.search(r"/(?:program|event)/[^/]+/[^/]+/(\d+)", href)
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
# DuckDuckGo-based C-SPAN URL lookup (zero WAF cost)
# ---------------------------------------------------------------------------

_DDG_DELAY = 4.0  # seconds between DDG searches (avoid 202 rate limits)

_STOPWORDS = {
    "the", "a", "an", "of", "in", "on", "to", "for", "and", "or",
    "at", "by", "is", "it", "as", "be", "was", "are", "its", "with",
    "that", "this", "from", "before", "after", "hearing", "committee",
    "subcommittee", "full", "oversight", "examine", "examining",
    "regarding", "concerning", "review", "united", "states", "senate",
    "house", "congress", "testifies", "testimony", "witnesses",
    "hearings", "focusing",
}


def _extract_search_keywords(title: str, max_words: int = 5) -> str:
    """Extract significant keywords from a hearing title for search."""
    words = re.sub(r"[^a-z0-9\s]", "", title.lower()).split()
    significant = [w for w in words if len(w) >= 3 and w not in _STOPWORDS]
    return " ".join(significant[:max_words])


def discover_cspan_google(
    hearings: list[dict],
    max_searches: int = 20,
) -> list[dict]:
    """Find C-SPAN URLs via DuckDuckGo HTML search. No WAF cost.

    Uses DDG's HTML endpoint which returns real results without JS.
    Query format: ``c-span.org/program {keywords} {year}``

    Args:
        hearings: [{id, title, date, committee_key}, ...]
        max_searches: cap on DDG queries per run

    Returns:
        [{hearing_id, cspan_url, program_id}, ...]
    """
    if not hearings:
        return []

    results: list[dict] = []
    searches = 0

    for h in hearings:
        if searches >= max_searches:
            log.info("DDG C-SPAN: hit search cap (%d)", max_searches)
            break

        keywords = _extract_search_keywords(h["title"])
        if not keywords:
            continue

        # Add year context for relevance
        date_str = h.get("date", "")
        year = ""
        if date_str:
            try:
                year = datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y")
            except ValueError:
                pass

        query = f"c-span.org/program {keywords}"
        if year:
            query += f" {year}"

        _time.sleep(_DDG_DELAY)
        searches += 1

        try:
            resp = httpx.post(
                "https://html.duckduckgo.com/html/",
                data={"q": query},
                headers={"User-Agent": _UA},
                follow_redirects=True,
                timeout=15.0,
            )
            if resp.status_code == 202:
                # DDG returns 202 when rate-limited; back off and retry once
                log.debug("DDG rate-limited (202), backing off 10s...")
                _time.sleep(10)
                resp = httpx.post(
                    "https://html.duckduckgo.com/html/",
                    data={"q": query},
                    headers={"User-Agent": _UA},
                    follow_redirects=True,
                    timeout=15.0,
                )
            if resp.status_code != 200:
                log.debug("DDG search returned %d for '%s'",
                          resp.status_code, keywords[:40])
                continue

            # Extract C-SPAN program/event URLs from DDG HTML results
            raw_urls = re.findall(
                r"https?://www\.c-span\.org/(?:program|event)/[^\s\"'<>&]+",
                resp.text,
            )
            # Decode HTML entities and deduplicate
            seen: set[str] = set()
            cspan_url = None
            program_id = None
            for raw in raw_urls:
                url = raw.replace("&amp;", "&")
                m = re.search(r"/(?:program|event)/[^/]+/[^/]+/(\d+)", url)
                if not m:
                    continue
                pid = m.group(1)
                if pid in seen:
                    continue
                seen.add(pid)
                cspan_url = url
                program_id = pid
                break  # take first (most relevant) result

            if cspan_url and program_id:
                results.append({
                    "hearing_id": h["id"],
                    "cspan_url": cspan_url,
                    "program_id": program_id,
                })
                log.debug("DDG C-SPAN: found %s for '%s'",
                          program_id, h["title"][:50])
            else:
                log.debug("DDG C-SPAN: no match for '%s'", h["title"][:50])

        except Exception as e:
            log.debug("DDG search error for '%s': %s", keywords[:30], e)
            continue

    log.info("DDG C-SPAN: %d found from %d searches (%d hearings queried)",
             len(results), searches, len(hearings))
    return results


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
