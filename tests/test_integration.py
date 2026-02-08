"""Integration tests that hit real network endpoints.

Skipped by default. Run with:
    pytest -m integration tests/test_integration.py

Purpose: catch scraper drift when committee websites change their HTML
structure. These tests do NOT exercise LLM cleanup or Whisper (those cost
money). They only verify that discovery and scraping against live sites
still return structurally valid results.
"""

from __future__ import annotations

import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

# Ensure project root is importable.
sys.path.insert(0, str(Path(__file__).parent.parent))

import httpx
from discover import Hearing, discover_youtube
from scrapers import ScrapedHearing, parse_date, scrape_website

# Load committee metadata once for the module.
import config

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_YT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _fetch_html(url: str, timeout: float = 25.0) -> str:
    """GET a URL and return the response body. Raises on failure."""
    transport = httpx.HTTPTransport(retries=2)
    with httpx.Client(
        transport=transport,
        timeout=timeout,
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (compatible; HearingBot/1.0)"},
    ) as client:
        resp = client.get(url)
        resp.raise_for_status()
        return resp.text


# ===================================================================
# 1. YouTube discovery — house.ways_and_means
# ===================================================================

@pytest.mark.integration
class TestDiscoverYouTube:
    """Hit the real Ways & Means YouTube channel via yt-dlp."""

    COMMITTEE_KEY = "house.ways_and_means"

    @pytest.fixture(scope="class")
    def hearings(self) -> list[Hearing]:
        meta = config.COMMITTEES[self.COMMITTEE_KEY]
        return discover_youtube(self.COMMITTEE_KEY, meta, days=7)

    def test_returns_list(self, hearings):
        # The channel might have zero uploads in the last 7 days; that is
        # acceptable.  But the return type must always be a list.
        assert isinstance(hearings, list)

    def test_hearing_fields_valid(self, hearings):
        for h in hearings:
            assert isinstance(h, Hearing)
            assert h.committee_key == self.COMMITTEE_KEY
            assert _DATE_RE.match(h.date), f"bad date format: {h.date}"
            assert len(h.id) == 12, f"bad id length: {h.id}"
            assert h.title  # non-empty

    def test_sources_contain_youtube(self, hearings):
        for h in hearings:
            assert "youtube_url" in h.sources
            assert "youtube_id" in h.sources
            assert _YT_ID_RE.match(h.sources["youtube_id"]), (
                f"malformed youtube_id: {h.sources['youtube_id']}"
            )

    def test_dates_within_window(self, hearings):
        cutoff = datetime.now() - timedelta(days=8)  # 1 day slack
        for h in hearings:
            dt = datetime.strptime(h.date, "%Y-%m-%d")
            assert dt >= cutoff, f"hearing date {h.date} is older than 8-day window"


# ===================================================================
# 2. Website scraping — house.appropriations (evo_framework)
# ===================================================================

@pytest.mark.integration
class TestScrapeWebsite:
    """Fetch a real committee hearings page and run the matching scraper."""

    # House Appropriations uses evo_framework (time[datetime] elements).
    # It reliably has hearings listed across sessions and recesses.
    COMMITTEE_KEY = "house.appropriations"

    @pytest.fixture(scope="class")
    def meta(self) -> dict:
        return config.COMMITTEES[self.COMMITTEE_KEY]

    @pytest.fixture(scope="class")
    def results(self, meta) -> list[ScrapedHearing]:
        url = meta["hearings_url"]
        html = _fetch_html(url)
        # Use a generous cutoff so we get *some* results even during recess.
        cutoff = datetime.now() - timedelta(days=365)
        return scrape_website(meta["scraper_type"], html, url, cutoff)

    def test_returns_nonempty(self, results):
        assert len(results) > 0, (
            "evo_framework scraper returned 0 hearings from House Appropriations "
            "-- HTML structure may have changed"
        )

    def test_result_fields(self, results):
        for r in results:
            assert isinstance(r, ScrapedHearing)
            assert r.title and len(r.title) >= 10
            assert _DATE_RE.match(r.date), f"bad date: {r.date}"
            assert r.url.startswith("http"), f"bad url: {r.url}"

    def test_dates_are_plausible(self, results):
        """All dates should be within 2020..today+30d (no wild misparses)."""
        floor = datetime(2020, 1, 1)
        ceiling = datetime.now() + timedelta(days=30)
        for r in results:
            dt = datetime.strptime(r.date, "%Y-%m-%d")
            assert floor <= dt <= ceiling, f"implausible date: {r.date}"

    def test_no_duplicate_urls(self, results):
        urls = [r.url for r in results]
        assert len(urls) == len(set(urls)), "duplicate URLs in scraper output"


# ===================================================================
# 3. parse_date against real-world date strings
# ===================================================================

# These strings are taken verbatim from committee website HTML as of
# Feb 2026.  They represent the diversity of formats across chambers.
_REAL_DATE_STRINGS: list[tuple[str, str]] = [
    # ISO from time[datetime] attrs (evo_framework, aspnet_card)
    ("2026-02-05T10:00:00-05:00", "2026-02-05"),
    ("2026-01-28T14:30:00Z", "2026-01-28"),
    # MM/DD/YY (Senate Finance, Banking, Appropriations table cells)
    ("01/28/26", "2026-01-28"),
    ("2/5/26", "2026-02-05"),
    ("12/10/25", "2025-12-10"),
    # MM/DD/YYYY (Senate Commerce, HSGAC)
    ("02/04/2026", "2026-02-04"),
    ("1/15/2026", "2026-01-15"),
    # MM.DD.YY (Senate Budget, HELP, Judiciary, Armed Services)
    ("01.28.26", "2026-01-28"),
    ("2.05.26", "2026-02-05"),
    # MM.DD.YYYY (some Senate sites)
    ("01.28.2026", "2026-01-28"),
    # Month DD, YYYY (many House sites, Tribe Events)
    ("February 5, 2026", "2026-02-05"),
    ("January 28, 2026", "2026-01-28"),
    ("December 10, 2025", "2025-12-10"),
    # Abbreviated month (WordPress blog, some Senate sites)
    ("Feb 5, 2026", "2026-02-05"),
    ("Jan 28, 2026", "2026-01-28"),
    ("Sept 15, 2025", "2025-09-15"),
    # Embedded in surrounding text (common in Oversight, Veterans)
    ("Posted: February 5, 2026 - 10:00 AM", "2026-02-05"),
    ("Hearing Date: Jan 28, 2026", "2026-01-28"),
    # M/D/YY  (Senate EPW, Small Business ColdFusion)
    ("2/5/26", "2026-02-05"),
    ("11/3/25", "2025-11-03"),
]


@pytest.mark.integration
class TestParseDateRealStrings:
    """Validate parse_date against date strings collected from live sites."""

    @pytest.mark.parametrize("raw,expected", _REAL_DATE_STRINGS)
    def test_parse(self, raw: str, expected: str):
        result = parse_date(raw)
        assert result == expected, f"parse_date({raw!r}) = {result!r}, expected {expected!r}"
