"""Tests for scrapers.py — date parsing and HTML scraper functions."""

import sys
from datetime import datetime, timedelta
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers import (
    ScrapedHearing,
    _is_plausible_hearing_date,
    parse_date,
    scrape_aspnet_card,
    scrape_drupal_table,
    scrape_evo_framework,
    scrape_new_senate_cms,
    scrape_wordpress_single_event,
)


# ---------------------------------------------------------------------------
# parse_date
# ---------------------------------------------------------------------------

class TestParseDate:
    def test_iso_8601(self):
        assert parse_date("2026-02-10") == "2026-02-10"

    def test_iso_8601_with_time(self):
        assert parse_date("2026-02-10T15:30:00Z") == "2026-02-10"

    def test_mm_slash_dd_slash_yyyy(self):
        assert parse_date("02/10/2026") == "2026-02-10"

    def test_mm_slash_dd_slash_yy(self):
        assert parse_date("2/5/26") == "2026-02-05"

    def test_mm_dot_dd_dot_yy(self):
        assert parse_date("02.10.26") == "2026-02-10"

    def test_mm_dot_dd_dot_yyyy(self):
        assert parse_date("02.10.2026") == "2026-02-10"

    def test_month_dd_yyyy(self):
        assert parse_date("February 10, 2026") == "2026-02-10"

    def test_month_abbreviated(self):
        assert parse_date("Feb 10, 2026") == "2026-02-10"

    def test_month_no_comma(self):
        assert parse_date("January 5 2026") == "2026-01-05"

    def test_sept_abbreviation(self):
        assert parse_date("Sept 15, 2025") == "2025-09-15"

    def test_empty_string(self):
        assert parse_date("") is None

    def test_no_date(self):
        assert parse_date("No date here") is None

    def test_embedded_in_text(self):
        assert parse_date("Posted on February 10, 2026 by admin") == "2026-02-10"

    def test_embedded_iso(self):
        assert parse_date("datetime='2026-01-15T09:00:00'") == "2026-01-15"


# ---------------------------------------------------------------------------
# Date plausibility filter
# ---------------------------------------------------------------------------

class TestIsPlausibleHearingDate:
    def test_recent_date_is_plausible(self):
        assert _is_plausible_hearing_date("2026-02-10") is True

    def test_far_future_is_not_plausible(self):
        """Term expiration dates like 'January 19, 2031' should be rejected."""
        assert _is_plausible_hearing_date("2031-01-19") is False

    def test_moderately_future_is_not_plausible(self):
        assert _is_plausible_hearing_date("2027-11-12") is False

    def test_slightly_future_is_plausible(self):
        # A hearing scheduled 3 months from now is fine
        from datetime import datetime, timedelta
        future = (datetime.now() + timedelta(days=90)).strftime("%Y-%m-%d")
        assert _is_plausible_hearing_date(future) is True

    def test_very_old_is_not_plausible(self):
        assert _is_plausible_hearing_date("2020-01-01") is False

    def test_invalid_date_is_not_plausible(self):
        assert _is_plausible_hearing_date("not-a-date") is False


class TestNewSenateCmsDatePlausibility:
    """Regression: Senate Finance titles contain nomination term expirations
    like '...term expiring January 19, 2031' which should not be picked up
    as the hearing date."""

    def test_rejects_term_expiration_date_in_title(self):
        html = """
        <div>
          <a href="/hearings/hearing-to-consider-nominations">
            Hearing to Consider the Nominations of Arjun Mody, of New Jersey,
            to be Deputy Commissioner Social Security for the term expiring
            January 19, 2031
          </a>
        </div>
        """
        cutoff = datetime(2026, 1, 1)
        results = scrape_new_senate_cms(html, "https://finance.senate.gov", cutoff)
        # Should find 0 results because the only parseable date (2031) is implausible
        assert len(results) == 0


# ---------------------------------------------------------------------------
# Scraper: drupal_table
# ---------------------------------------------------------------------------

DRUPAL_TABLE_HTML = """
<table class="table-striped">
<tr>
  <td><a href="/hearings/02/10/2026/fiscal-policy-hearing">Fiscal Policy and Economic Outlook Hearing</a></td>
  <td>February 10, 2026</td>
</tr>
<tr>
  <td><a href="/hearings/01/05/2020/old-hearing">Very Old Hearing From Long Ago</a></td>
  <td>January 5, 2020</td>
</tr>
</table>
"""

class TestDrupalTable:
    def test_finds_recent_hearing(self):
        cutoff = datetime(2026, 1, 1)
        results = scrape_drupal_table(DRUPAL_TABLE_HTML, "https://example.com", cutoff)
        assert len(results) == 1
        assert results[0].title == "Fiscal Policy and Economic Outlook Hearing"
        assert results[0].date == "2026-02-10"

    def test_filters_old_hearings(self):
        cutoff = datetime(2026, 1, 1)
        results = scrape_drupal_table(DRUPAL_TABLE_HTML, "https://example.com", cutoff)
        titles = [r.title for r in results]
        assert "Very Old Hearing From Long Ago" not in titles

    def test_absolute_url(self):
        cutoff = datetime(2026, 1, 1)
        results = scrape_drupal_table(DRUPAL_TABLE_HTML, "https://senate.gov", cutoff)
        assert results[0].url.startswith("https://senate.gov")


# ---------------------------------------------------------------------------
# Scraper: evo_framework
# ---------------------------------------------------------------------------

EVO_HTML = """
<div class="hearing-item">
  <time datetime="2026-02-05T10:00:00">February 5, 2026</time>
  <div>
    <a href="/events/hearings/oversight-of-federal-spending">Oversight of Federal Spending Programs</a>
  </div>
</div>
<div class="hearing-item">
  <time datetime="2024-06-15T14:00:00">June 15, 2024</time>
  <div>
    <a href="/events/hearings/old-one">Old Spending Hearing Discussion Topic</a>
  </div>
</div>
"""

class TestEvoFramework:
    def test_finds_recent(self):
        cutoff = datetime(2026, 1, 1)
        results = scrape_evo_framework(EVO_HTML, "https://house.gov", cutoff)
        assert len(results) == 1
        assert results[0].title == "Oversight of Federal Spending Programs"
        assert results[0].date == "2026-02-05"


# ---------------------------------------------------------------------------
# Scraper: aspnet_card
# ---------------------------------------------------------------------------

ASPNET_HTML = """
<article class="card-h-event">
  <time datetime="2026-02-07">February 7, 2026</time>
  <a href="/hearings/banking-regulation-update">Banking Regulation Update and Reform Discussion</a>
</article>
"""

class TestAspnetCard:
    def test_finds_hearing(self):
        cutoff = datetime(2026, 1, 1)
        results = scrape_aspnet_card(ASPNET_HTML, "https://financialservices.house.gov", cutoff)
        assert len(results) == 1
        assert results[0].date == "2026-02-07"
        assert "Banking" in results[0].title


# ---------------------------------------------------------------------------
# Scraper: wordpress_single_event (Ways & Means)
# ---------------------------------------------------------------------------

WM_HTML = """
<div class="single-event">
  <div class="date-block">
    <span class="month">Feb</span>
    <span class="day">12</span>
    <span class="year">2026</span>
  </div>
  <div class="info">
    <a href="/events/tax-reform-hearing-2026">Tax Reform and Middle Class Impact Hearing</a>
  </div>
</div>
<div class="single-event">
  <div class="date-block">
    <span class="month">Dec</span>
    <span class="day">1</span>
    <span class="year">2025</span>
  </div>
  <div class="info">
    <a href="/events/trade-hearing">Trade Policy and Tariffs Discussion Panel</a>
  </div>
</div>
"""

class TestWordPressSingleEvent:
    def test_finds_recent_and_filters(self):
        cutoff = datetime(2026, 1, 1)
        results = scrape_wordpress_single_event(WM_HTML, "https://waysandmeans.house.gov", cutoff)
        assert len(results) == 1
        assert results[0].title == "Tax Reform and Middle Class Impact Hearing"
        assert results[0].date == "2026-02-12"


# ---------------------------------------------------------------------------
# Scraper: new_senate_cms
# ---------------------------------------------------------------------------

SENATE_CMS_HTML = """
<div class="hearing-list">
  <div class="hearing-item">
    <span>February 8, 2026</span>
    <a href="/hearings/nominations-hearing-02-08-2026">Nominations Hearing for Treasury Department Officials</a>
  </div>
  <div class="hearing-item">
    <span>December 1, 2025</span>
    <a href="/hearings/old-hearing-12-01-2025">Old Hearing Title That Should Be Filtered Out</a>
  </div>
</div>
"""

class TestNewSenateCms:
    def test_finds_recent_by_slug_date(self):
        cutoff = datetime(2026, 1, 1)
        results = scrape_new_senate_cms(SENATE_CMS_HTML, "https://foreign.senate.gov", cutoff)
        assert len(results) == 1
        assert "Nominations" in results[0].title
        assert results[0].date == "2026-02-08"
