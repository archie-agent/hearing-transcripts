"""Discover recent hearings from YouTube channels, committee websites, and GovInfo."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import time as _time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import httpx

import config
import scrapers
from detail_scraper import scrape_hearing_detail

log = logging.getLogger(__name__)

# yt-dlp needs deno on PATH for JS challenge solving, and the venv bin for yt-dlp itself
_VENV_BIN = str(Path(__file__).parent / ".venv" / "bin")
_SYS_BIN = str(Path(sys.executable).parent)
_DENO_DIR = os.path.expanduser("~/.deno/bin")
_YT_DLP_ENV = {**os.environ, "PATH": f"{_VENV_BIN}:{_SYS_BIN}:{_DENO_DIR}:{os.environ.get('PATH', '')}"}

# Rate limiting: track last request time per domain
_last_request: dict[str, float] = {}
_MIN_DELAY = 1.5  # seconds between requests to same domain (DEMO_KEY: 40 req/min)


def _rate_limit(url: str) -> None:
    """Sleep if needed to respect minimum delay between requests to same domain."""
    domain = urlparse(url).netloc
    now = _time.monotonic()
    last = _last_request.get(domain, 0)
    wait = _MIN_DELAY - (now - last)
    if wait > 0:
        _time.sleep(wait)
    _last_request[domain] = _time.monotonic()


# ---------------------------------------------------------------------------
# Hearing dataclass
# ---------------------------------------------------------------------------

@dataclass
class Hearing:
    committee_key: str
    committee_name: str
    title: str
    date: str  # YYYY-MM-DD
    sources: dict = field(default_factory=dict)
    # sources can include:
    #   youtube_url, youtube_id,
    #   website_url, testimony_pdf_urls,
    #   govinfo_package_id

    @property
    def id(self) -> str:
        """Deterministic hearing ID from key fields."""
        normalized = _normalize_title(self.title)
        raw = f"{self.committee_key}:{self.date}:{normalized}"
        return hashlib.sha256(raw.encode()).hexdigest()[:12]

    @property
    def slug(self) -> str:
        safe = re.sub(r"[^a-z0-9]+", "-", self.title.lower())[:80].strip("-")
        parts = self.committee_key.split(".", 1)
        chamber = parts[0]
        committee = parts[1].replace("_", "-") if len(parts) > 1 else "unknown"
        return f"{chamber}-{committee}-{safe}"


# ---------------------------------------------------------------------------
# Title normalization for dedup
# ---------------------------------------------------------------------------

def _normalize_title(title: str) -> str:
    """Normalize a hearing title for comparison/dedup."""
    title = re.sub(
        r"^(Full Committee |Subcommittee )?Hearing:?\s*",
        "", title, flags=re.IGNORECASE,
    )
    title = re.sub(r"^HEARING NOTICE:?\s*", "", title, flags=re.IGNORECASE)
    words = re.sub(r"[^a-z0-9\s]", "", title.lower()).split()[:8]
    return " ".join(words)


# ---------------------------------------------------------------------------
# HTTP client with retries
# ---------------------------------------------------------------------------

def _http_get(url: str, timeout: float = 20.0) -> httpx.Response | None:
    """Fetch a URL with retries, rate limiting, and 429 backoff. Returns None on failure."""
    _rate_limit(url)
    transport = httpx.HTTPTransport(retries=2)
    try:
        with httpx.Client(transport=transport, timeout=timeout,
                          follow_redirects=True,
                          headers={"User-Agent": "Mozilla/5.0 (compatible; HearingBot/1.0)"}) as client:
            resp = client.get(url)
            # Handle 429 Too Many Requests with backoff
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "5"))
                retry_after = min(retry_after, 60)  # cap at 60s
                log.debug("HTTP 429 for %s, waiting %ds", url, retry_after)
                _time.sleep(retry_after)
                resp = client.get(url)
            if resp.status_code != 200:
                log.warning("HTTP %s for %s", resp.status_code, url)
                return None
            return resp
    except Exception as e:
        log.warning("HTTP error for %s: %s", url, e)
        return None


# ---------------------------------------------------------------------------
# YouTube discovery via yt-dlp
# ---------------------------------------------------------------------------

# Minimum video duration (seconds) to consider a YouTube video a real hearing.
# Clips under this are kept separately but won't be promoted as standalone hearings.
_MIN_HEARING_DURATION = 600  # 10 minutes


def discover_youtube(committee_key: str, meta: dict, days: int = 1) -> list[Hearing]:
    """Find recent videos on a committee's YouTube channel.

    Returns two categories:
    - Full hearings (>= 10 min): created as standalone Hearing objects
    - Short clips (< 10 min): stored in _youtube_clips for later matching
    """
    youtube_url = meta.get("youtube")
    if not youtube_url:
        return []

    cutoff = datetime.now() - timedelta(days=days)
    cutoff_str = cutoff.strftime("%Y%m%d")

    try:
        result = subprocess.run(
            [
                "yt-dlp",
                "--remote-components", "ejs:github",
                "--no-download",
                "--print", "%(id)s\t%(title)s\t%(upload_date)s\t%(duration)s",
                "--dateafter", cutoff_str,
                "--playlist-end", "50",
                "--match-filter", "!is_live & !is_upcoming",
                f"{youtube_url}/videos",
            ],
            capture_output=True,
            text=True,
            timeout=120,
            env=_YT_DLP_ENV,
        )
    except FileNotFoundError as e:
        log.error("CRITICAL: yt-dlp not found! %s", e)
        raise
    except subprocess.TimeoutExpired as e:
        log.warning("yt-dlp timed out for %s: %s", committee_key, e)
        return []

    if result.returncode != 0 and not result.stdout.strip():
        stderr = result.stderr.strip()
        if stderr:
            log.warning("yt-dlp errors for %s: %s", committee_key, stderr[:200])

    hearings = []
    for line in result.stdout.strip().splitlines():
        parts = line.split("\t", 3)
        if len(parts) < 4:
            continue
        vid_id, title, upload_date, duration_str = parts
        if not upload_date or upload_date == "NA" or len(upload_date) < 8:
            continue
        if upload_date < cutoff_str:
            continue

        try:
            duration = int(float(duration_str)) if duration_str and duration_str != "NA" else 0
        except (ValueError, TypeError):
            duration = 0

        date_formatted = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:8]}"
        yt_source = {
            "youtube_url": f"https://www.youtube.com/watch?v={vid_id}",
            "youtube_id": vid_id,
            "youtube_duration": duration,
        }

        if duration >= _MIN_HEARING_DURATION:
            # Long enough to be a real hearing
            hearings.append(Hearing(
                committee_key=committee_key,
                committee_name=meta.get("name", committee_key),
                title=title,
                date=date_formatted,
                sources=yt_source,
            ))
            dur_str = f"{duration // 60}m{duration % 60:02d}s"
            log.debug("  YouTube hearing: %s (%s) %s", vid_id, dur_str, title[:60])
        else:
            # Short clip — stash for later matching with website hearings
            _youtube_clips.append({
                "committee_key": committee_key,
                "date": date_formatted,
                "title": title,
                "duration": duration,
                **yt_source,
            })
            dur_str = f"{duration // 60}m{duration % 60:02d}s"
            log.debug("  YouTube clip (skipped): %s (%s) %s", vid_id, dur_str, title[:60])

    return hearings


# Clips shorter than _MIN_HEARING_DURATION, stashed during YouTube discovery
# for later matching with website hearings in _attach_youtube_clips().
_youtube_clips: list[dict] = []


def _attach_youtube_clips(hearings: list[Hearing]) -> None:
    """Match stashed YouTube clips to website hearings by committee + date + title.

    When a committee posts both a full hearing page on their website AND short
    YouTube clips (chairman statements, member interviews, etc.), we attach the
    clip URLs as supplementary metadata on the matching website hearing rather
    than treating the clips as standalone hearings.
    """
    if not _youtube_clips:
        return

    attached = 0
    for clip in _youtube_clips:
        best_match: Hearing | None = None
        best_sim = 0.0

        for h in hearings:
            # Must be same committee
            if h.committee_key != clip["committee_key"]:
                continue
            # Must be same date (or within 1 day to handle upload-date drift)
            if h.date != clip["date"]:
                continue
            sim = _title_similarity(h.title, clip["title"])
            if sim > best_sim:
                best_sim = sim
                best_match = h

        # Require a minimum similarity — clips often have very different titles
        # ("Chairman's Opening Statement") so we use a lower bar than cross-dedup
        if best_match and best_sim >= 0.15:
            # Don't overwrite a real hearing YouTube URL with a clip
            if "youtube_url" not in best_match.sources:
                best_match.sources["youtube_url"] = clip["youtube_url"]
                best_match.sources["youtube_id"] = clip["youtube_id"]
                best_match.sources["youtube_duration"] = clip["duration"]
            # Also store clips list for reference
            clips_list = best_match.sources.setdefault("youtube_clips", [])
            clips_list.append({
                "url": clip["youtube_url"],
                "title": clip["title"],
                "duration": clip["duration"],
            })
            attached += 1
            log.debug("  Clip matched: %s -> %s (sim=%.2f)",
                      clip["title"][:40], best_match.title[:40], best_sim)

    if attached:
        log.info("Attached %d YouTube clips to %s hearing(s)",
                 attached, len({id(h) for h in hearings
                                if "youtube_clips" in h.sources}))
    unmatched = len(_youtube_clips) - attached
    if unmatched:
        log.debug("  %d YouTube clips unmatched (no website hearing found)", unmatched)

    _youtube_clips.clear()


# ---------------------------------------------------------------------------
# Website discovery using scraper registry
# ---------------------------------------------------------------------------

def discover_website(committee_key: str, meta: dict, days: int = 1) -> list[Hearing]:
    """Scrape a committee's hearings page using the appropriate scraper."""
    hearings_url = meta.get("hearings_url")
    scraper_type = meta.get("scraper_type", "youtube_only")
    cutoff = datetime.now() - timedelta(days=days)

    # JS-rendered committees: use the Chrome browser via CDP
    if meta.get("requires_js", False) and hearings_url:
        base_url = hearings_url
        scraped = scrapers.scrape_js_rendered(hearings_url, scraper_type, base_url, cutoff)
        hearings = []
        for s in scraped:
            hearings.append(Hearing(
                committee_key=committee_key,
                committee_name=meta.get("name", committee_key),
                title=s.title,
                date=s.date,
                sources={"website_url": s.url},
            ))
        return hearings

    # Normal static HTML path
    if not hearings_url or scraper_type == "youtube_only":
        return []
    if not meta.get("scrapeable", False):
        return []

    resp = _http_get(hearings_url)
    if not resp:
        return []

    base_url = str(resp.url)

    scraped = scrapers.scrape_website(scraper_type, resp.text, base_url, cutoff)

    hearings = []
    for s in scraped:
        hearings.append(Hearing(
            committee_key=committee_key,
            committee_name=meta.get("name", committee_key),
            title=s.title,
            date=s.date,
            sources={"website_url": s.url},
        ))

    return hearings


# ---------------------------------------------------------------------------
# GovInfo API discovery (official GPO transcripts)
# ---------------------------------------------------------------------------

# Map GovInfo committee codes to our committee keys
_GOVINFO_CODE_MAP: dict[str, str] = {}

# Map normalized name fragments to committee keys for title-based matching.
# Multiple committees can share a fragment (e.g., "judiciary" maps to both
# house.judiciary and senate.judiciary), so values are lists.
_GOVINFO_NAME_MAP: dict[str, list[str]] = {}

def _build_govinfo_map() -> None:
    """Build mapping from GovInfo package codes and name fragments to committee keys."""
    global _GOVINFO_CODE_MAP, _GOVINFO_NAME_MAP
    for key, meta in config.COMMITTEES.items():
        code = meta.get("code", "")
        if code:
            _GOVINFO_CODE_MAP[code] = key

        # Build name fragment lookup for title-based matching.
        # From "House Ways and Means" we extract "ways and means",
        # from "Senate Banking" we extract "banking", etc.
        name = meta.get("name", "")
        chamber = meta.get("chamber", "")
        if name and chamber:
            # Strip chamber prefix and normalize
            stripped = name
            for prefix in ("House ", "Senate "):
                if stripped.startswith(prefix):
                    stripped = stripped[len(prefix):]
                    break
            # Store the lowered fragment -> [keys]
            fragment = stripped.lower().strip()
            if fragment:
                _GOVINFO_NAME_MAP.setdefault(fragment, []).append(key)


def _map_govinfo_to_committee(title: str, chamber: str) -> str | None:
    """Try to extract a committee key from a GovInfo package title.

    Searches for known committee name fragments in the title text.
    GovInfo titles often contain phrases like:
      "HEARING BEFORE THE COMMITTEE ON WAYS AND MEANS"
      "COMMITTEE ON FINANCE--UNITED STATES SENATE"
      "COMMITTEE ON BANKING, HOUSING, AND URBAN AFFAIRS"

    Returns the committee_key if a match is found, None otherwise.
    """
    if not _GOVINFO_NAME_MAP:
        _build_govinfo_map()

    title_upper = title.upper()

    # Try to find "COMMITTEE ON <name>" pattern first
    committee_on_match = re.search(
        r"COMMITTEE\s+ON\s+(.+?)(?:\s*[-\u2014,]\s*(?:UNITED\s+STATES|U\.S\.)|$)",
        title_upper,
    )
    search_text = committee_on_match.group(1).strip() if committee_on_match else title_upper

    # Strip leading "THE "
    search_text_no_article = re.sub(r"^THE\s+", "", search_text, flags=re.IGNORECASE)

    # Build candidate list
    candidates = [search_text_no_article]
    if search_text_no_article != search_text:
        candidates.append(search_text)
    if search_text != title_upper:
        candidates.append(title_upper)

    # Filter to only committees matching the detected chamber
    chamber_prefix = f"{chamber}." if chamber and chamber != "unknown" else ""

    # Try longest fragments first for best specificity
    sorted_fragments = sorted(_GOVINFO_NAME_MAP.keys(), key=len, reverse=True)

    for candidate in candidates:
        candidate_lower = candidate.lower()
        for fragment in sorted_fragments:
            if fragment not in candidate_lower:
                continue
            keys = _GOVINFO_NAME_MAP[fragment]
            for key in keys:
                if chamber_prefix and not key.startswith(chamber_prefix):
                    continue
                return key

    return None


def _fetch_govinfo_committee(package_id: str) -> str | None:
    """Fetch the GovInfo package summary and try to extract a committee key.

    Makes an additional API call to the summary endpoint to get committee metadata.
    Only called when GOVINFO_FETCH_DETAILS=true (default false) and title-based
    mapping failed.

    Returns the committee_key if found, None otherwise.
    """
    if not _GOVINFO_NAME_MAP:
        _build_govinfo_map()

    url = (
        f"https://api.govinfo.gov/packages/{package_id}/summary"
        f"?api_key={config.GOVINFO_API_KEY}"
    )
    resp = _http_get(url, timeout=20)
    if not resp:
        return None

    try:
        data = resp.json()
    except Exception:
        log.warning("GovInfo summary for %s returned non-JSON", package_id)
        return None

    # Detect chamber from packageId
    pkg_lower = package_id.lower()
    if "hhrg" in pkg_lower:
        chamber = "house"
    elif "shrg" in pkg_lower:
        chamber = "senate"
    else:
        chamber = "unknown"

    # Check for "committees" field in the summary JSON
    committees = data.get("committees", [])
    if committees:
        for entry in committees:
            name = entry if isinstance(entry, str) else entry.get("committeeName", "")
            if name:
                mapped = _map_govinfo_to_committee(name, chamber)
                if mapped:
                    return mapped

    # Fallback: try the title from the summary
    summary_title = data.get("title", "")
    if summary_title:
        mapped = _map_govinfo_to_committee(summary_title, chamber)
        if mapped:
            return mapped

    return None


def discover_congress_api(days: int = 7) -> list[Hearing]:
    """Poll congress.gov committee-meeting API for recent hearings.

    Uses the structured congress.gov API to discover hearings with witness
    metadata and meeting status. The API's systemCode matches our committee
    'code' field (e.g., 'ssbk00' for Senate Banking).
    """
    api_key = config.CONGRESS_API_KEY
    congress = config.CONGRESS

    # Build reverse lookup: {systemCode: committee_key}
    code_to_key: dict[str, str] = {}
    for key, meta in config.COMMITTEES.items():
        code = meta.get("code", "")
        if code:
            code_to_key[code] = key

    if not code_to_key:
        log.warning("No committee codes configured, skipping congress.gov API")
        return []

    cutoff = datetime.now() - timedelta(days=days)
    from_dt = cutoff.strftime("%Y-%m-%dT00:00:00Z")

    hearings: list[Hearing] = []

    for chamber in ("house", "senate"):
        offset = 0
        while True:
            list_url = (
                f"https://api.congress.gov/v3/committee-meeting/{congress}/{chamber}"
                f"?fromDateTime={from_dt}&limit=250&offset={offset}"
                f"&format=json&api_key={api_key}"
            )
            resp = _http_get(list_url, timeout=30)
            if not resp:
                break

            try:
                data = resp.json()
            except Exception:
                log.warning("congress.gov API returned non-JSON for %s", chamber)
                break

            meetings = data.get("committeeMeetings", [])
            if not meetings:
                break

            for meeting in meetings:
                event_id = meeting.get("eventId", "")
                detail_url = meeting.get("url", "")
                if not event_id or not detail_url:
                    continue

                # Fetch detail endpoint for full metadata
                if "api_key=" not in detail_url:
                    detail_url += f"&api_key={api_key}" if "?" in detail_url else f"?api_key={api_key}"
                detail_url += "&format=json" if "format=" not in detail_url else ""

                detail_resp = _http_get(detail_url, timeout=20)
                if not detail_resp:
                    continue

                try:
                    detail = detail_resp.json()
                except Exception:
                    continue

                # The detail may be nested under a key or at top level
                meeting_detail = detail.get("committeeMeeting", detail)

                # Skip canceled/postponed meetings
                status = meeting_detail.get("meetingStatus", "")
                if status in ("Canceled", "Postponed"):
                    log.debug("  Skipping %s meeting %s: %s", chamber, event_id, status)
                    continue

                title = meeting_detail.get("title", "")
                if not title:
                    continue

                # Parse date
                date_str = meeting_detail.get("date", "")
                if not date_str:
                    continue
                # date is typically ISO format like "2026-02-05T15:00:00Z"
                try:
                    date_formatted = date_str[:10]  # YYYY-MM-DD
                    meeting_date = datetime.strptime(date_formatted, "%Y-%m-%d")
                    if meeting_date < cutoff:
                        continue
                except (ValueError, IndexError):
                    continue

                # Extract systemCode from committees list.
                # systemCode can be a subcommittee code like "hsif16" --
                # try exact match first, then parent committee (first 4 chars + "00").
                committee_key = None
                committees_list = meeting_detail.get("committees", [])
                for comm in committees_list:
                    sys_code = comm.get("systemCode", "")
                    if not sys_code:
                        continue
                    if sys_code in code_to_key:
                        committee_key = code_to_key[sys_code]
                        break
                    # Try parent committee code (e.g., hsif16 -> hsif00)
                    parent_code = sys_code[:4] + "00"
                    if parent_code in code_to_key:
                        committee_key = code_to_key[parent_code]
                        break

                if not committee_key:
                    log.debug("  No matching committee for meeting %s: %s",
                              event_id, title[:60])
                    continue

                committee_meta = config.COMMITTEES.get(committee_key, {})
                committee_name = committee_meta.get("name", committee_key)

                # Extract witnesses
                witnesses = []
                for w in meeting_detail.get("witnesses", []):
                    witness_info = {}
                    if w.get("name"):
                        witness_info["name"] = w["name"]
                    if w.get("position"):
                        witness_info["position"] = w["position"]
                    if w.get("organization"):
                        witness_info["organization"] = w["organization"]
                    if witness_info:
                        witnesses.append(witness_info)

                sources: dict = {"congress_api_event_id": event_id}
                if witnesses:
                    sources["witnesses"] = witnesses

                hearings.append(Hearing(
                    committee_key=committee_key,
                    committee_name=committee_name,
                    title=title,
                    date=date_formatted,
                    sources=sources,
                ))
                log.debug("  congress.gov: %s %s %s", date_formatted,
                          committee_key, title[:60])

            # Pagination
            pagination = data.get("pagination", {})
            if pagination.get("next"):
                offset += 250
            else:
                break

    log.info("congress.gov API: %d hearings discovered", len(hearings))
    return hearings


def discover_govinfo(days: int = 7) -> list[Hearing]:
    """Poll GovInfo API for recently published hearing transcripts."""
    if not _GOVINFO_CODE_MAP:
        _build_govinfo_map()

    fetch_details = os.environ.get("GOVINFO_FETCH_DETAILS", "false").lower() == "true"

    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z")
    url = (
        f"https://api.govinfo.gov/collections/CHRG/{cutoff}"
        f"?offsetMark=*&pageSize=100&congress={config.CONGRESS}"
        f"&api_key={config.GOVINFO_API_KEY}"
    )

    resp = _http_get(url, timeout=30)
    if not resp:
        return []

    try:
        data = resp.json()
    except Exception:
        log.warning("GovInfo returned non-JSON response")
        return []

    # GovInfo collections API returns packages *modified* since cutoff, not
    # *published* since cutoff.  Filter by dateIssued to drop old transcripts
    # that merely got a metadata update.  GPO transcripts are published 3-6
    # months after the hearing, so a 180-day window is generous.
    date_floor = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")

    hearings = []
    for pkg in data.get("packages", []):
        pkg_id = pkg.get("packageId", "")
        date_issued = pkg.get("dateIssued", "")[:10]
        title = pkg.get("title", pkg_id)

        # Skip packages published more than 180 days ago
        if date_issued < date_floor:
            continue

        # Detect chamber from package ID
        if "hhrg" in pkg_id.lower():
            chamber = "house"
        elif "shrg" in pkg_id.lower():
            chamber = "senate"
        else:
            chamber = "unknown"

        # Step 1: Try title-based mapping (no extra API calls)
        committee_key = _map_govinfo_to_committee(title, chamber)

        # Step 2: If title mapping failed and detail fetching is enabled, try summary
        if not committee_key and fetch_details:
            committee_key = _fetch_govinfo_committee(pkg_id)

        # Step 3: Fall back to generic chamber key
        if not committee_key:
            committee_key = f"govinfo.{chamber}"

        # Resolve committee name from config if we have a real key
        committee_meta = config.COMMITTEES.get(committee_key)
        if committee_meta:
            committee_name = committee_meta.get("name", committee_key)
        else:
            committee_name = f"{chamber.title()} (via GovInfo)"

        hearings.append(Hearing(
            committee_key=committee_key,
            committee_name=committee_name,
            title=title,
            date=date_issued,
            sources={"govinfo_package_id": pkg_id},
        ))

    log.info("GovInfo: %d packages after date filtering (floor: %s)", len(hearings), date_floor)
    return hearings


# ---------------------------------------------------------------------------
# Discovery for a single committee (used in parallel)
# ---------------------------------------------------------------------------

def _discover_committee(key: str, meta: dict, days: int) -> list[Hearing]:
    """Discover hearings for a single committee from all sources."""
    results = []

    # YouTube
    try:
        yt = discover_youtube(key, meta, days=days)
        if yt:
            log.info("  %s YouTube: %d videos", key, len(yt))
            results.extend(yt)
    except Exception as e:
        log.warning("YouTube discovery failed for %s: %s", key, e)

    # Website
    try:
        web = discover_website(key, meta, days=days)
        if web:
            log.info("  %s website: %d hearings", key, len(web))
            results.extend(web)
    except Exception as e:
        log.warning("Website discovery failed for %s: %s", key, e)

    return results


# ---------------------------------------------------------------------------
# Main discovery
# ---------------------------------------------------------------------------

def discover_all(days: int = 1, committees: dict[str, dict] | None = None,
                  state=None) -> list[Hearing]:
    """Run all discovery methods across committees. Parallelized."""
    if committees is None:
        committees = config.get_committees()

    _youtube_clips.clear()  # Reset from any prior call in same process
    all_hearings: list[Hearing] = []

    # Parallel discovery across committees
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {
            pool.submit(_discover_committee, key, meta, days): key
            for key, meta in committees.items()
        }
        for future in as_completed(futures):
            key = futures[future]
            try:
                hearings = future.result()
                all_hearings.extend(hearings)
            except Exception as e:
                log.error("Discovery failed for %s: %s", key, e)

    # GovInfo (catches both chambers, longer lookback)
    try:
        govinfo = discover_govinfo(days=max(days, 7))
        if govinfo:
            log.info("GovInfo: %d packages", len(govinfo))
            all_hearings.extend(govinfo)
    except Exception as e:
        log.warning("GovInfo discovery failed: %s", e)

    # Congress.gov API (structured data with witnesses)
    try:
        congress_api = discover_congress_api(days=max(days, 7))
        if congress_api:
            log.info("congress.gov API: %d hearings", len(congress_api))
            all_hearings.extend(congress_api)
    except Exception as e:
        log.warning("congress.gov API discovery failed: %s", e)

    # Deduplicate (same committee key)
    deduped = _deduplicate(all_hearings)
    # Cross-committee dedup (different keys, same hearing)
    deduped = _cross_committee_dedup(deduped)
    log.info("Total hearings: %d (deduped from %d)", len(deduped), len(all_hearings))

    # Attach YouTube clips to matching website hearings
    _attach_youtube_clips(deduped)

    # C-SPAN discovery: find video URLs for hearings.
    # Uses 3-layer strategy: broad search, targeted for active committees,
    # and rotation for stale committees (requires state for Layer 3).
    try:
        import cspan
        active_keys = {h.committee_key for h in deduped}
        cspan_videos = cspan.discover_cspan(
            committees, days=max(days, 7), active_keys=active_keys,
            state=state,
        )
        if cspan_videos:
            _attach_cspan_urls(deduped, cspan_videos)
    except ImportError:
        log.debug("cspan module not available, skipping C-SPAN discovery")
    except Exception as e:
        log.warning("C-SPAN discovery failed: %s", e)

    # After dedup, enrich with testimony PDFs
    for hearing in deduped:
        website_url = hearing.sources.get("website_url")
        if not website_url:
            continue
        meta = committees.get(hearing.committee_key, {})
        if not meta.get("has_testimony", False) and not meta.get("scrapeable", False):
            continue
        try:
            pdf_urls = scrape_hearing_detail(hearing.committee_key, website_url, meta)
            if pdf_urls:
                hearing.sources["testimony_pdf_urls"] = pdf_urls
        except Exception as e:
            log.warning("PDF extraction failed for %s: %s", website_url, e)

    return deduped


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def _deduplicate(hearings: list[Hearing]) -> list[Hearing]:
    """Merge hearings that appear to be the same event from different sources."""
    merged: dict[str, Hearing] = {}

    for h in hearings:
        # Dedup key includes normalized title prefix to handle same-day hearings
        title_key = _normalize_title(h.title)
        dedup_key = f"{h.committee_key}:{h.date}:{title_key}"

        if dedup_key in merged:
            existing = merged[dedup_key]
            existing.sources.update(h.sources)
            if len(h.title) > len(existing.title):
                existing.title = h.title
        else:
            merged[dedup_key] = h

    return list(merged.values())


# ---------------------------------------------------------------------------
# Cross-committee deduplication (joint hearings & YouTube/GovInfo duplicates)
# ---------------------------------------------------------------------------

_CROSS_DEDUP_THRESHOLD = 0.4


def _title_similarity(title_a: str, title_b: str) -> float:
    """Jaccard similarity of word tokens between two titles."""
    words_a = set(re.sub(r"[^a-z0-9\s]", "", title_a.lower()).split())
    words_b = set(re.sub(r"[^a-z0-9\s]", "", title_b.lower()).split())
    if not words_a or not words_b:
        return 0.0
    return len(words_a & words_b) / len(words_a | words_b)


def _chamber_from_key(committee_key: str) -> str:
    """Extract chamber (house/senate) from a committee key."""
    parts = committee_key.split(".")
    # For govinfo keys like "govinfo.house" or "govinfo.senate", chamber is the second part.
    if parts[0] == "govinfo":
        return parts[1] if len(parts) > 1 else "unknown"
    # For regular keys like "house.judiciary" or "senate.finance", chamber is the first part.
    return parts[0]


def _is_specific_key(committee_key: str) -> bool:
    """Return True if the committee key refers to a real committee (not a generic govinfo fallback)."""
    if committee_key.startswith("govinfo."):
        return False
    return committee_key in config.COMMITTEES


def _preferred_key(key_a: str, key_b: str) -> str:
    """Return whichever committee key is more specific / preferred for merging."""
    a_specific = _is_specific_key(key_a)
    b_specific = _is_specific_key(key_b)
    if a_specific and not b_specific:
        return key_a
    if b_specific and not a_specific:
        return key_b
    # Both specific or both generic -- prefer the one in config.COMMITTEES
    if key_a in config.COMMITTEES:
        return key_a
    return key_b


def _cross_committee_dedup(hearings: list[Hearing]) -> list[Hearing]:
    """Catch duplicates across different committee keys within the same chamber and date.

    This handles cases where the same hearing is discovered from multiple sources
    with different committee_keys (e.g. YouTube with a specific key vs GovInfo
    with a generic 'govinfo.house' key).
    """
    # Group by date
    by_date: dict[str, list[Hearing]] = {}
    for h in hearings:
        by_date.setdefault(h.date, []).append(h)

    result: list[Hearing] = []

    for date, group in by_date.items():
        if len(group) < 2:
            result.extend(group)
            continue

        # Track which indices have been merged away
        merged_into: dict[int, int] = {}  # index -> index it was merged into

        for i in range(len(group)):
            if i in merged_into:
                continue
            for j in range(i + 1, len(group)):
                if j in merged_into:
                    continue

                h_i = group[i]
                h_j = group[j]

                # Skip if same committee key (already handled by _deduplicate)
                if h_i.committee_key == h_j.committee_key:
                    continue

                # Skip cross-chamber comparisons
                chamber_i = _chamber_from_key(h_i.committee_key)
                chamber_j = _chamber_from_key(h_j.committee_key)
                if chamber_i != chamber_j:
                    continue

                # Compare titles
                sim = _title_similarity(h_i.title, h_j.title)
                if sim > _CROSS_DEDUP_THRESHOLD:
                    # Merge j into i
                    winner_key = _preferred_key(h_i.committee_key, h_j.committee_key)
                    if winner_key == h_j.committee_key:
                        h_i.committee_key = h_j.committee_key
                        h_i.committee_name = h_j.committee_name
                    h_i.sources.update(h_j.sources)
                    if len(h_j.title) > len(h_i.title):
                        h_i.title = h_j.title
                    merged_into[j] = i

        for i, h in enumerate(group):
            if i not in merged_into:
                result.append(h)

    return result


def _keyword_overlap(title_a: str, title_b: str) -> int:
    """Count significant keyword overlaps between two titles.

    Strips common stopwords and short words, then counts how many
    remaining words appear in both titles. More tolerant of title format
    differences than Jaccard similarity (which penalizes differing lengths).
    """
    _STOPWORDS = {
        "the", "a", "an", "of", "in", "on", "to", "for", "and", "or",
        "at", "by", "is", "it", "as", "be", "was", "are", "its", "with",
        "that", "this", "from", "before", "after", "hearing", "committee",
        "subcommittee", "full", "oversight", "examine", "examining",
        "regarding", "concerning", "review", "united", "states", "senate",
        "house", "congress", "testifies", "testimony", "witnesses",
    }
    def _significant_words(text: str) -> set[str]:
        words = set(re.sub(r"[^a-z0-9\s]", "", text.lower()).split())
        return {w for w in words if len(w) >= 3 and w not in _STOPWORDS}

    words_a = _significant_words(title_a)
    words_b = _significant_words(title_b)
    if not words_a or not words_b:
        return 0
    return len(words_a & words_b)


def _attach_cspan_urls(hearings: list[Hearing], cspan_videos: list[dict]) -> None:
    """Match C-SPAN video URLs to hearings by committee + date + title.

    Each C-SPAN video has {title, date, url, program_id, committee_key}.
    Primary match: same committee_key + same date.
    Tiebreaker: keyword overlap (when multiple hearings per committee per day).
    Fallback: date-only match with keyword overlap for cross-committee matches.
    """
    attached = 0
    for video in cspan_videos:
        video_date = video.get("date", "")
        video_title = video.get("title", "")
        video_url = video.get("url", "")
        video_committee = video.get("committee_key", "")
        if not video_url or not video_date:
            continue

        # Primary: match by committee + date
        candidates = [
            h for h in hearings
            if h.committee_key == video_committee and h.date == video_date
        ]

        # Fallback: date-only (cross-committee or joint hearings)
        if not candidates:
            candidates = [h for h in hearings if h.date == video_date]

        if not candidates:
            log.debug("  C-SPAN unmatched: %s %s %s",
                      video_committee, video_date, video_title[:40])
            continue

        if len(candidates) == 1:
            best = candidates[0]
        else:
            # Multiple candidates — use keyword overlap as tiebreaker
            scored = [(h, _keyword_overlap(h.title, video_title)) for h in candidates]
            scored.sort(key=lambda x: x[1], reverse=True)
            best = scored[0][0]

        if "cspan_url" not in best.sources:
            best.sources["cspan_url"] = video_url
            attached += 1
            log.debug("  C-SPAN matched: %s -> [%s] %s",
                      video_title[:40], best.committee_key, best.title[:40])

    if attached:
        log.info("Attached %d C-SPAN video URLs to hearings", attached)
    unmatched = len(cspan_videos) - attached
    if unmatched:
        log.debug("  %d C-SPAN videos unmatched", unmatched)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    hearings = discover_all(days=3)
    for h in hearings:
        print(f"[{h.date}] [{h.id}] {h.committee_name}: {h.title}")
        print(f"  Sources: {json.dumps(h.sources, indent=2)}")
        print()
