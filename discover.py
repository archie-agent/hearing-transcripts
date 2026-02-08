"""Discover recent hearings from YouTube channels, committee websites, and GovInfo."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import subprocess
import time as _time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from urllib.parse import urlparse

import httpx

import config
import scrapers

log = logging.getLogger(__name__)

# yt-dlp needs deno on PATH for JS challenge solving
_DENO_DIR = os.path.expanduser("~/.deno/bin")
_YT_DLP_ENV = {**os.environ, "PATH": f"{_DENO_DIR}:{os.environ.get('PATH', '')}"}

# Rate limiting: track last request time per domain
_last_request: dict[str, float] = {}
_MIN_DELAY = 1.0  # seconds between requests to same domain


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
    """Fetch a URL with retries and rate limiting. Returns None on failure."""
    _rate_limit(url)
    transport = httpx.HTTPTransport(retries=2)
    try:
        with httpx.Client(transport=transport, timeout=timeout,
                          follow_redirects=True,
                          headers={"User-Agent": "Mozilla/5.0 (compatible; HearingBot/1.0)"}) as client:
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

def discover_youtube(committee_key: str, meta: dict, days: int = 1) -> list[Hearing]:
    """Find recent videos on a committee's YouTube channel."""
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
                "--print", "%(id)s\t%(title)s\t%(upload_date)s",
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
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        log.warning("yt-dlp failed for %s: %s", committee_key, e)
        return []

    if result.returncode != 0 and not result.stdout.strip():
        stderr = result.stderr.strip()
        if stderr:
            log.warning("yt-dlp errors for %s: %s", committee_key, stderr[:200])

    hearings = []
    for line in result.stdout.strip().splitlines():
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue
        vid_id, title, upload_date = parts
        if not upload_date or upload_date == "NA" or len(upload_date) < 8:
            continue
        if upload_date < cutoff_str:
            continue

        date_formatted = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:8]}"
        hearings.append(Hearing(
            committee_key=committee_key,
            committee_name=meta.get("name", committee_key),
            title=title,
            date=date_formatted,
            sources={
                "youtube_url": f"https://www.youtube.com/watch?v={vid_id}",
                "youtube_id": vid_id,
            },
        ))

    return hearings


# ---------------------------------------------------------------------------
# Website discovery using scraper registry
# ---------------------------------------------------------------------------

def discover_website(committee_key: str, meta: dict, days: int = 1) -> list[Hearing]:
    """Scrape a committee's hearings page using the appropriate scraper."""
    hearings_url = meta.get("hearings_url")
    scraper_type = meta.get("scraper_type", "youtube_only")

    if not hearings_url or scraper_type == "youtube_only":
        return []
    if not meta.get("scrapeable", False):
        return []

    resp = _http_get(hearings_url)
    if not resp:
        return []

    cutoff = datetime.now() - timedelta(days=days)
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

def _build_govinfo_map() -> None:
    """Build mapping from GovInfo package codes to committee keys."""
    global _GOVINFO_CODE_MAP
    for key, meta in config.COMMITTEES.items():
        code = meta.get("code", "")
        if code:
            _GOVINFO_CODE_MAP[code] = key


def discover_govinfo(days: int = 7) -> list[Hearing]:
    """Poll GovInfo API for recently published hearing transcripts."""
    if not _GOVINFO_CODE_MAP:
        _build_govinfo_map()

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

    hearings = []
    for pkg in data.get("packages", []):
        pkg_id = pkg.get("packageId", "")
        date_issued = pkg.get("dateIssued", "")[:10]
        title = pkg.get("title", pkg_id)

        # Try to map to a specific committee via the package ID
        # Package IDs look like CHRG-119hhrg12345 or CHRG-119shrg54321
        committee_key = None
        if "hhrg" in pkg_id.lower():
            chamber = "house"
        elif "shrg" in pkg_id.lower():
            chamber = "senate"
        else:
            chamber = "unknown"

        # If we can't map to a specific committee, use a generic key
        if not committee_key:
            committee_key = f"govinfo.{chamber}"

        hearings.append(Hearing(
            committee_key=committee_key,
            committee_name=f"{chamber.title()} (via GovInfo)",
            title=title,
            date=date_issued,
            sources={"govinfo_package_id": pkg_id},
        ))

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

def discover_all(days: int = 1, committees: dict[str, dict] | None = None) -> list[Hearing]:
    """Run all discovery methods across committees. Parallelized."""
    if committees is None:
        committees = config.get_committees()

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

    # Deduplicate
    deduped = _deduplicate(all_hearings)
    log.info("Total hearings: %d (deduped from %d)", len(deduped), len(all_hearings))

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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    hearings = discover_all(days=3)
    for h in hearings:
        print(f"[{h.date}] [{h.id}] {h.committee_name}: {h.title}")
        print(f"  Sources: {json.dumps(h.sources, indent=2)}")
        print()
