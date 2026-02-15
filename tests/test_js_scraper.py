"""Tests for JS-rendered page scraping via Chrome CDP."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from scrapers import (
    ScrapedHearing,
    scrape_generic_links,
    scrape_js_rendered,
)


# ---------------------------------------------------------------------------
# scrape_generic_links
# ---------------------------------------------------------------------------

GENERIC_HTML = """
<html><body>
<div class="event-list">
  <div class="event">
    <span>February 10, 2026</span>
    <a href="/hearings/climate-policy-and-economic-impact">Climate Policy and Economic Impact Hearing</a>
  </div>
  <div class="event">
    <span>February 5, 2026</span>
    <a href="/hearings/broadband-infrastructure-review">Broadband Infrastructure Review and Oversight</a>
  </div>
  <div class="event">
    <span>January 3, 2020</span>
    <a href="/hearings/ancient-hearing-topic">Very Old Hearing That Should Be Filtered</a>
  </div>
  <div class="nav">
    <a href="/next-page">Next</a>
  </div>
</div>
</body></html>
"""


class TestGenericLinks:
    def test_finds_recent_hearings(self):
        cutoff = datetime(2026, 1, 1)
        results = scrape_generic_links(GENERIC_HTML, "https://example.house.gov", cutoff)
        assert len(results) == 2

    def test_extracts_titles(self):
        cutoff = datetime(2026, 1, 1)
        results = scrape_generic_links(GENERIC_HTML, "https://example.house.gov", cutoff)
        titles = [r.title for r in results]
        assert "Climate Policy and Economic Impact Hearing" in titles
        assert "Broadband Infrastructure Review and Oversight" in titles

    def test_filters_old_hearings(self):
        cutoff = datetime(2026, 1, 1)
        results = scrape_generic_links(GENERIC_HTML, "https://example.house.gov", cutoff)
        titles = [r.title for r in results]
        assert "Very Old Hearing That Should Be Filtered" not in titles

    def test_filters_nav_links(self):
        cutoff = datetime(2020, 1, 1)
        results = scrape_generic_links(GENERIC_HTML, "https://example.house.gov", cutoff)
        titles = [r.title for r in results]
        # "Next" is too short (< 15 chars) and in skip list, should not appear
        assert all("Next" != t for t in titles)

    def test_absolute_urls(self):
        cutoff = datetime(2026, 1, 1)
        results = scrape_generic_links(GENERIC_HTML, "https://example.house.gov", cutoff)
        for r in results:
            assert r.url.startswith("https://example.house.gov")

    def test_returns_scraped_hearing_type(self):
        cutoff = datetime(2026, 1, 1)
        results = scrape_generic_links(GENERIC_HTML, "https://example.house.gov", cutoff)
        for r in results:
            assert isinstance(r, ScrapedHearing)


GENERIC_HTML_TIME_ELEMENTS = """
<html><body>
<div class="calendar-item">
  <time datetime="2026-02-12T10:00:00">Feb 12</time>
  <div>
    <a href="/event/agricultural-policy-review">Agricultural Policy Review and Farm Bill Discussion</a>
  </div>
</div>
</body></html>
"""


class TestGenericLinksTimeElements:
    def test_finds_hearing_via_time_element(self):
        cutoff = datetime(2026, 1, 1)
        results = scrape_generic_links(GENERIC_HTML_TIME_ELEMENTS, "https://agriculture.house.gov", cutoff)
        assert len(results) == 1
        assert results[0].date == "2026-02-12"
        assert "Agricultural Policy" in results[0].title


# ---------------------------------------------------------------------------
# Helper: create a mock Playwright chain that returns given HTML
# ---------------------------------------------------------------------------

def _mock_browser_returning_html(html):
    """Create a mock sync_playwright() that yields a browser returning the given HTML."""
    mock_page = MagicMock()
    mock_page.content.return_value = html
    mock_page.goto.return_value = None
    mock_page.wait_for_timeout.return_value = None
    mock_page.close.return_value = None

    mock_context = MagicMock()
    mock_context.new_page.return_value = mock_page

    mock_browser = MagicMock()
    mock_browser.contexts = [mock_context]

    mock_pw = MagicMock()
    mock_pw.chromium.connect_over_cdp.return_value = mock_browser
    mock_pw.stop.return_value = None

    # sync_playwright() returns an object whose .start() returns mock_pw
    mock_sync_pw_callable = MagicMock()
    mock_sync_pw_callable.return_value.start.return_value = mock_pw

    return mock_sync_pw_callable, mock_page


# ---------------------------------------------------------------------------
# scrape_js_rendered — browser not available
# ---------------------------------------------------------------------------

class TestScrapeJsRenderedBrowserUnavailable:
    def test_returns_empty_when_playwright_not_installed(self):
        """If playwright is not importable, should return []."""
        import builtins
        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if "playwright" in name:
                raise ImportError("No module named 'playwright'")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            results = scrape_js_rendered(
                "https://energycommerce.house.gov/calendars",
                "generic_links",
                "https://energycommerce.house.gov",
                datetime(2026, 1, 1),
            )
            assert results == []

    def test_returns_empty_when_browser_not_running(self):
        """If CDP connection fails, should log warning and return []."""
        mock_pw = MagicMock()
        mock_pw.chromium.connect_over_cdp.side_effect = ConnectionError(
            "Connection refused"
        )
        mock_pw.stop.return_value = None

        mock_sync_pw_callable = MagicMock()
        mock_sync_pw_callable.return_value.start.return_value = mock_pw

        with patch("playwright.sync_api.sync_playwright", mock_sync_pw_callable):
            results = scrape_js_rendered(
                "https://energycommerce.house.gov/calendars",
                "generic_links",
                "https://energycommerce.house.gov",
                datetime(2026, 1, 1),
            )
            assert results == []


# ---------------------------------------------------------------------------
# scrape_js_rendered — correct scraper dispatch
# ---------------------------------------------------------------------------

ELEMENTOR_HTML = """
<html><body>
<div class="jet-listing-grid">
  <div class="jet-listing-grid__item">
    <a href="/hearings/border-security-review">Border Security Review and Assessment Hearing</a>
    <span>February 6, 2026</span>
  </div>
</div>
</body></html>
"""

EVO_HTML = """
<html><body>
<div class="hearing-item">
  <time datetime="2026-02-08T10:00:00">February 8, 2026</time>
  <div>
    <a href="/events/hearings/energy-policy-oversight">Energy Policy Oversight and Regulation Hearing</a>
  </div>
</div>
</body></html>
"""


class TestScrapeJsRenderedDispatch:
    """Test that rendered HTML is passed to the correct scraper function."""

    def test_dispatches_to_wordpress_elementor(self):
        mock, _ = _mock_browser_returning_html(ELEMENTOR_HTML)

        with patch("playwright.sync_api.sync_playwright", mock):
            results = scrape_js_rendered(
                "https://www.hsgac.senate.gov/hearings",
                "wordpress_elementor",
                "https://www.hsgac.senate.gov",
                datetime(2026, 1, 1),
            )
            assert len(results) == 1
            assert "Border Security" in results[0].title
            assert results[0].date == "2026-02-06"

    def test_dispatches_to_evo_framework(self):
        mock, _ = _mock_browser_returning_html(EVO_HTML)

        with patch("playwright.sync_api.sync_playwright", mock):
            results = scrape_js_rendered(
                "https://energycommerce.house.gov/calendars",
                "evo_framework",
                "https://energycommerce.house.gov",
                datetime(2026, 1, 1),
            )
            assert len(results) == 1
            assert "Energy Policy" in results[0].title
            assert results[0].date == "2026-02-08"

    def test_falls_back_to_generic_links(self):
        """When scraper_type returns no results, falls back to generic_links."""
        mock, _ = _mock_browser_returning_html(GENERIC_HTML)

        with patch("playwright.sync_api.sync_playwright", mock):
            # Use evo_framework which won't find anything in GENERIC_HTML
            # (no time[datetime] elements), so should fall back to generic_links
            results = scrape_js_rendered(
                "https://agriculture.house.gov/calendar/",
                "evo_framework",
                "https://agriculture.house.gov",
                datetime(2026, 1, 1),
            )
            assert len(results) >= 1
            titles = [r.title for r in results]
            assert any("Climate" in t or "Broadband" in t for t in titles)

    def test_page_is_closed_after_scraping(self):
        """Verify page.close() is called even on success."""
        mock, mock_page = _mock_browser_returning_html(ELEMENTOR_HTML)

        with patch("playwright.sync_api.sync_playwright", mock):
            scrape_js_rendered(
                "https://www.hsgac.senate.gov/hearings",
                "wordpress_elementor",
                "https://www.hsgac.senate.gov",
                datetime(2026, 1, 1),
            )
            mock_page.close.assert_called_once()

    def test_output_format(self):
        """Results should be ScrapedHearing namedtuples with title, date, url."""
        mock, _ = _mock_browser_returning_html(ELEMENTOR_HTML)

        with patch("playwright.sync_api.sync_playwright", mock):
            results = scrape_js_rendered(
                "https://www.hsgac.senate.gov/hearings",
                "wordpress_elementor",
                "https://www.hsgac.senate.gov",
                datetime(2026, 1, 1),
            )
            for r in results:
                assert isinstance(r, ScrapedHearing)
                assert r.title
                assert r.date
                assert r.url
