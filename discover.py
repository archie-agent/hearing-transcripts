"""Discover recent hearings from YouTube channels, committee websites, and GovInfo."""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import httpx
from bs4 import BeautifulSoup

import config

log = logging.getLogger(__name__)

# yt-dlp needs deno on PATH for JS challenge solving
_DENO_DIR = os.path.expanduser("~/.deno/bin")
_YT_DLP_ENV = {**os.environ, "PATH": f"{_DENO_DIR}:{os.environ.get('PATH', '')}"}


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
    def slug(self) -> str:
        safe = re.sub(r"[^a-z0-9]+", "-", self.title.lower())[:80].strip("-")
        chamber = self.committee_key.split(".")[0]
        committee = self.committee_key.split(".")[1].replace("_", "-")
        return f"{chamber}-{committee}-{safe}"


# ---------------------------------------------------------------------------
# YouTube discovery via yt-dlp
# ---------------------------------------------------------------------------

def discover_youtube(committee_key: str, days: int = 1) -> list[Hearing]:
    """Find recent videos on a committee's YouTube channel."""
    meta = config.get_committee_meta(committee_key)
    if not meta or not meta.get("youtube"):
        return []

    channel_url = meta["youtube"]
    cutoff = datetime.now() - timedelta(days=days)
    cutoff_str = cutoff.strftime("%Y%m%d")

    try:
        # Use --remote-components for YouTube JS challenge solving (requires deno)
        result = subprocess.run(
            [
                "yt-dlp",
                "--remote-components", "ejs:github",
                "--no-download",
                "--print", "%(id)s\t%(title)s\t%(upload_date)s",
                "--dateafter", cutoff_str,
                "--playlist-end", "20",
                f"{channel_url}/videos",
            ],
            capture_output=True,
            text=True,
            timeout=120,
            env=_YT_DLP_ENV,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        log.warning("yt-dlp failed for %s: %s", committee_key, e)
        return []

    if result.returncode != 0:
        stderr = result.stderr.strip()
        # Partial failures are common (some videos unavailable)
        if "ERROR" in stderr and not result.stdout.strip():
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
        committee_info = config.COMMITTEES.get(committee_key, {})
        hearings.append(Hearing(
            committee_key=committee_key,
            committee_name=committee_info.get("name", committee_key),
            title=title,
            date=date_formatted,
            sources={
                "youtube_url": f"https://www.youtube.com/watch?v={vid_id}",
                "youtube_id": vid_id,
            },
        ))

    return hearings


# ---------------------------------------------------------------------------
# Senate committee website discovery
# ---------------------------------------------------------------------------

# Senate committee websites use Drupal with consistent table/link patterns.
# Map committee keys to their hearings page URL.
_SENATE_HEARINGS_URLS = {
    "senate.finance":           "https://www.finance.senate.gov/hearings",
    "senate.banking":           "https://www.banking.senate.gov/hearings",
    "senate.budget":            "https://www.budget.senate.gov/hearings",
    "senate.commerce":          "https://www.commerce.senate.gov/hearings",
    "senate.appropriations":    "https://www.appropriations.senate.gov/hearings",
    "senate.foreign_relations": "https://www.foreign.senate.gov/hearings",
    "senate.help":              "https://www.help.senate.gov/hearings",
    "senate.intelligence":      "https://www.intelligence.senate.gov/hearings",
    "senate.homeland_security": "https://www.hsgac.senate.gov/hearings",
    "senate.environment":       "https://www.epw.senate.gov/hearings",
    "senate.judiciary":         "https://www.judiciary.senate.gov/hearings",
}

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _parse_date_flexible(text: str) -> str | None:
    """Try to parse a date from various formats. Returns YYYY-MM-DD or None."""
    text = text.strip()
    # MM/DD/YY HH:MMAM
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", text)
    if m:
        month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if year < 100:
            year += 2000
        return f"{year:04d}-{month:02d}-{day:02d}"
    # Month DD, YYYY
    m = re.search(r"(\w+)\s+(\d{1,2}),?\s+(\d{4})", text)
    if m:
        month_name = m.group(1).lower()
        if month_name in _MONTHS:
            return f"{int(m.group(3)):04d}-{_MONTHS[month_name]:02d}-{int(m.group(2)):02d}"
    return None


def discover_senate_website(committee_key: str, days: int = 1) -> list[Hearing]:
    """Scrape a Senate committee's hearings page for recent hearings."""
    url = _SENATE_HEARINGS_URLS.get(committee_key)
    if not url:
        return []

    try:
        resp = httpx.get(url, timeout=20, follow_redirects=True,
                         headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            log.warning("Senate hearings page returned %s for %s", resp.status_code, committee_key)
            return []
    except Exception as e:
        log.warning("Senate hearings page error for %s: %s", committee_key, e)
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    cutoff = datetime.now() - timedelta(days=days)
    committee_info = config.COMMITTEES.get(committee_key, {})
    hearings = []

    # Senate sites use <table> with rows containing hearing info
    # Each row typically has: title link, type, date
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if not cells:
            continue

        # Find a link to the hearing
        link = row.find("a", href=True)
        if not link:
            continue
        title = link.get_text(strip=True)
        href = link.get("href", "")

        # Skip navigation / non-hearing links
        if not title or len(title) < 10:
            continue

        # Try to extract date from URL pattern /hearings/MM/DD/YYYY/... or from cell text
        hearing_date = None

        # Check URL for date: /hearings/02/05/2026/...
        url_date = re.search(r"/hearings?/(\d{2})/(\d{2})/(\d{4})/", href)
        if url_date:
            month, day, year = url_date.groups()
            hearing_date = f"{year}-{month}-{day}"

        # Check cells for date text
        if not hearing_date:
            row_text = row.get_text(" ", strip=True)
            hearing_date = _parse_date_flexible(row_text)

        if not hearing_date:
            continue

        try:
            dt = datetime.strptime(hearing_date, "%Y-%m-%d")
            if dt < cutoff:
                continue
        except ValueError:
            continue

        # Build full URL
        if href.startswith("/"):
            base = str(resp.url).rstrip("/").split("/hearings")[0]
            href = f"{base}{href}"

        hearings.append(Hearing(
            committee_key=committee_key,
            committee_name=committee_info.get("name", committee_key),
            title=title,
            date=hearing_date,
            sources={"website_url": href},
        ))

    return hearings


# ---------------------------------------------------------------------------
# House committee website discovery (WordPress-based sites)
# ---------------------------------------------------------------------------

# Some House committee sites are scrapeable. Map key to hearings URL.
_HOUSE_HEARINGS_URLS = {
    "house.ways_and_means":   "https://waysandmeans.house.gov/hearings/",
    "house.budget":           "https://budget.house.gov/hearings/",
    "house.oversight":        "https://oversight.house.gov/hearing/",
}


def discover_house_website(committee_key: str, days: int = 1) -> list[Hearing]:
    """Scrape a House committee's hearings page. Handles WordPress-style layouts."""
    url = _HOUSE_HEARINGS_URLS.get(committee_key)
    if not url:
        return []

    try:
        resp = httpx.get(url, timeout=20, follow_redirects=True,
                         headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return []
    except Exception as e:
        log.warning("House hearings page error for %s: %s", committee_key, e)
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    cutoff = datetime.now() - timedelta(days=days)
    committee_info = config.COMMITTEES.get(committee_key, {})
    hearings = []

    # Ways & Means style: div.single-event with div.date-block and div.info
    for event in soup.find_all("div", class_="single-event"):
        date_block = event.find("div", class_="date-block")
        info = event.find("div", class_="info")
        if not date_block or not info:
            continue

        month_el = date_block.find("span", class_="month")
        day_el = date_block.find("span", class_="day")
        year_el = date_block.find("span", class_="year")
        if not (month_el and day_el and year_el):
            continue

        month_name = month_el.get_text(strip=True).lower()
        if month_name not in _MONTHS:
            continue
        try:
            hearing_date = (
                f"{int(year_el.get_text(strip=True)):04d}-"
                f"{_MONTHS[month_name]:02d}-"
                f"{int(day_el.get_text(strip=True)):02d}"
            )
            dt = datetime.strptime(hearing_date, "%Y-%m-%d")
            if dt < cutoff:
                continue
        except (ValueError, TypeError):
            continue

        link = info.find("a", class_="name")
        title = link.get_text(strip=True) if link else info.get_text(strip=True)[:120]
        href = link.get("href", "") if link else ""
        if href and not href.startswith("http"):
            href = f"https://{resp.url.host}{href}"

        hearings.append(Hearing(
            committee_key=committee_key,
            committee_name=committee_info.get("name", committee_key),
            title=title,
            date=hearing_date,
            sources={"website_url": href},
        ))

    return hearings


# ---------------------------------------------------------------------------
# GovInfo API discovery (official GPO transcripts)
# ---------------------------------------------------------------------------

def discover_govinfo(days: int = 7) -> list[Hearing]:
    """Poll GovInfo API for recently published hearing transcripts."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z")
    url = (
        f"https://api.govinfo.gov/collections/CHRG/{cutoff}"
        f"?offsetMark=*&pageSize=100&congress=119"
        f"&api_key={config.GOVINFO_API_KEY}"
    )

    try:
        resp = httpx.get(url, timeout=30)
        if resp.status_code != 200:
            log.warning("GovInfo API returned %s", resp.status_code)
            return []
        data = resp.json()
    except Exception as e:
        log.warning("GovInfo API error: %s", e)
        return []

    hearings = []
    for pkg in data.get("packages", []):
        pkg_id = pkg.get("packageId", "")
        date_issued = pkg.get("dateIssued", "")[:10]
        title = pkg.get("title", pkg_id)

        if "hhrg" in pkg_id.lower():
            chamber = "House"
        elif "shrg" in pkg_id.lower():
            chamber = "Senate"
        elif "jhrg" in pkg_id.lower():
            chamber = "Joint"
        else:
            chamber = "Unknown"

        hearings.append(Hearing(
            committee_key=f"govinfo.{chamber.lower()}",
            committee_name=f"{chamber} (via GovInfo)",
            title=title,
            date=date_issued,
            sources={
                "govinfo_package_id": pkg_id,
            },
        ))

    return hearings


# ---------------------------------------------------------------------------
# Main discovery
# ---------------------------------------------------------------------------

def discover_all(days: int = 1) -> list[Hearing]:
    """Run all discovery methods across configured committees."""
    all_hearings: list[Hearing] = []

    for key in config.COMMITTEES:
        log.info("Discovering: %s", key)

        # YouTube (primary for House, some Senate)
        yt = discover_youtube(key, days=days)
        if yt:
            log.info("  YouTube: %d videos", len(yt))
            all_hearings.extend(yt)

        # Senate committee websites
        if key.startswith("senate."):
            web = discover_senate_website(key, days=days)
            if web:
                log.info("  Senate website: %d hearings", len(web))
                all_hearings.extend(web)

        # House committee websites (where supported)
        if key.startswith("house."):
            web = discover_house_website(key, days=days)
            if web:
                log.info("  House website: %d hearings", len(web))
                all_hearings.extend(web)

    # GovInfo (catches both chambers, longer lookback)
    govinfo = discover_govinfo(days=max(days, 7))
    if govinfo:
        log.info("GovInfo: %d packages", len(govinfo))
        all_hearings.extend(govinfo)

    # Deduplicate by (committee_key, date, title similarity)
    deduped = _deduplicate(all_hearings)
    log.info("Total hearings found: %d (deduped from %d)", len(deduped), len(all_hearings))
    return deduped


def _deduplicate(hearings: list[Hearing]) -> list[Hearing]:
    """Merge hearings that appear to be the same event from different sources."""
    merged: dict[str, Hearing] = {}

    for h in hearings:
        # Simple dedup key: committee + date
        dedup_key = f"{h.committee_key}:{h.date}"

        if dedup_key in merged:
            # Merge sources
            existing = merged[dedup_key]
            existing.sources.update(h.sources)
            # Keep the longer title
            if len(h.title) > len(existing.title):
                existing.title = h.title
        else:
            merged[dedup_key] = h

    return list(merged.values())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    hearings = discover_all(days=3)
    for h in hearings:
        print(f"[{h.date}] {h.committee_name}: {h.title}")
        print(f"  Sources: {json.dumps(h.sources, indent=2)}")
        print()
