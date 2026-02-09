"""Website scrapers for committee hearing pages. One function per scraper_type."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import NamedTuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared date parsing
# ---------------------------------------------------------------------------

_MONTHS = {
    "january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
    "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6, "july": 7, "jul": 7,
    "august": 8, "aug": 8, "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10, "november": 11, "nov": 11, "december": 12, "dec": 12,
}


class ScrapedHearing(NamedTuple):
    title: str
    date: str  # YYYY-MM-DD
    url: str   # absolute URL to hearing detail page


def parse_date(text: str) -> str | None:
    """Parse a date from various formats. Returns YYYY-MM-DD or None."""
    text = text.strip()
    if not text:
        return None

    # ISO 8601: 2026-02-10 or 2026-02-10T15:30:00Z
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # MM/DD/YY or MM/DD/YYYY (with optional time)
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", text)
    if m:
        month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if year < 100:
            year += 2000
        return f"{year:04d}-{month:02d}-{day:02d}"

    # MM.DD.YY or MM.DD.YYYY (Senate Budget, HELP, Judiciary, Armed Services)
    m = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{2,4})", text)
    if m:
        month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if year < 100:
            year += 2000
        return f"{year:04d}-{month:02d}-{day:02d}"

    # Month DD, YYYY or Mon DD, YYYY
    m = re.search(r"(\w+)\s+(\d{1,2}),?\s+(\d{4})", text)
    if m:
        month_name = m.group(1).lower()
        if month_name in _MONTHS:
            return f"{int(m.group(3)):04d}-{_MONTHS[month_name]:02d}-{int(m.group(2)):02d}"

    return None


def _is_plausible_hearing_date(date_str: str) -> bool:
    """Check if a parsed date is plausible for a congressional hearing.

    Rejects dates that are clearly embedded metadata (e.g., nomination term
    expirations like 'January 19, 2031') rather than actual hearing dates.
    """
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        now = datetime.now()
        # Hearing can't be more than 2 years old or more than 6 months in the future
        return (now - timedelta(days=730)) <= dt <= (now + timedelta(days=180))
    except ValueError:
        return False


def _is_recent(date_str: str, cutoff: datetime) -> bool:
    """Check if a YYYY-MM-DD date string is on or after the cutoff and plausible."""
    if not _is_plausible_hearing_date(date_str):
        return False
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt >= cutoff
    except ValueError:
        return False


def _abs_url(href: str, base_url: str) -> str:
    """Convert a relative URL to absolute using the base URL."""
    if href.startswith("http"):
        return href
    return urljoin(base_url, href)


# ---------------------------------------------------------------------------
# Scraper: drupal_table (Senate Finance, Banking, Budget, Appropriations)
# Uses table.table-striped > tr > td
# ---------------------------------------------------------------------------

def scrape_drupal_table(html: str, base_url: str, cutoff: datetime) -> list[ScrapedHearing]:
    """Parse Senate Drupal sites with table.table-striped layout."""
    soup = BeautifulSoup(html, "lxml")
    results = []

    for table in soup.find_all("table", class_=re.compile(r"table-striped|recordList")):
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if not cells:
                continue

            link = row.find("a", href=True)
            if not link:
                continue
            title = link.get_text(strip=True)
            if not title or len(title) < 10:
                continue

            # Extract date from cells or URL
            hearing_date = None
            href = link.get("href", "")

            # Check URL for date: /hearings/MM/DD/YYYY/
            url_date = re.search(r"/hearings?/(\d{2})/(\d{2})/(\d{4})(?:/|$)", href)
            if url_date:
                month, day, year = url_date.groups()
                hearing_date = f"{year}-{month}-{day}"

            # Check cells for date text
            if not hearing_date:
                row_text = row.get_text(" ", strip=True)
                hearing_date = parse_date(row_text)

            if not hearing_date or not _is_recent(hearing_date, cutoff):
                continue

            results.append(ScrapedHearing(
                title=title,
                date=hearing_date,
                url=_abs_url(href, base_url),
            ))

    return results


# ---------------------------------------------------------------------------
# Scraper: coldfusion_table (Senate EPW, Small Business)
# Uses table.recordList > tr > td
# ---------------------------------------------------------------------------

def scrape_coldfusion_table(html: str, base_url: str, cutoff: datetime) -> list[ScrapedHearing]:
    """Parse Senate ColdFusion sites with table.recordList layout."""
    # Same table structure as drupal_table but scoped to recordList class
    return scrape_drupal_table(html, base_url, cutoff)


# ---------------------------------------------------------------------------
# Scraper: new_senate_cms (Foreign Relations, HELP, Judiciary, Armed Services,
#                          Agriculture, Rules)
# Links in body matching /hearings/{slug} pattern
# ---------------------------------------------------------------------------

def scrape_new_senate_cms(html: str, base_url: str, cutoff: datetime) -> list[ScrapedHearing]:
    """Parse New Senate CMS sites with link-based hearing listings."""
    soup = BeautifulSoup(html, "lxml")
    results = []
    seen_urls = set()

    # Find all links that look like hearing detail pages
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        # Match /hearings/*, /committee-activity/hearings/*, but not /hearings alone
        if not re.search(r"/(?:committee-activity/)?hearings/[a-z0-9]", href):
            continue
        # Skip pagination/filter links
        if "?" in href or "#" in href:
            continue

        # New Senate CMS wraps entire card in <a> — prefer title-specific element
        title_el = link.select_one(".LegislationList__title, .ArticleTitle")
        title = title_el.get_text(strip=True) if title_el else link.get_text(strip=True)
        if not title or len(title) < 5:
            continue

        abs_href = _abs_url(href, base_url)
        if abs_href in seen_urls:
            continue
        seen_urls.add(abs_href)

        # Try to extract date from URL slug (e.g., -MM-DD-YYYY suffix)
        hearing_date = None
        slug_date = re.search(r"-(\d{2})-(\d{2})-(\d{4})/?$", href)
        if slug_date:
            month, day, year = slug_date.groups()
            hearing_date = f"{year}-{month}-{day}"

        # Try <time> element inside the link (new Senate CMS)
        if not hearing_date:
            time_el = link.find("time")
            if time_el:
                dt_attr = time_el.get("datetime", "")
                hearing_date = parse_date(dt_attr) or parse_date(time_el.get_text(strip=True))

        # Try surrounding text for dates
        if not hearing_date:
            parent = link.parent
            if parent:
                context = parent.get_text(" ", strip=True)
                hearing_date = parse_date(context)

        # Try the table row if link is in a table
        if not hearing_date:
            row = link.find_parent("tr")
            if row:
                hearing_date = parse_date(row.get_text(" ", strip=True))

        if not hearing_date or not _is_recent(hearing_date, cutoff):
            continue

        results.append(ScrapedHearing(
            title=title,
            date=hearing_date,
            url=abs_href,
        ))

    return results


# ---------------------------------------------------------------------------
# Scraper: drupal_links (Senate Commerce, Energy, Veterans)
# Links with /YYYY/M/ date in URL path
# ---------------------------------------------------------------------------

def scrape_drupal_links(html: str, base_url: str, cutoff: datetime) -> list[ScrapedHearing]:
    """Parse Senate Drupal announcement-style sites with date-in-URL links."""
    soup = BeautifulSoup(html, "lxml")
    results = []
    seen_urls = set()

    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        # Match links with /YYYY/M/ or /YYYY/MM/ in the path
        url_date = re.search(r"/(\d{4})/(\d{1,2})/", href)
        if not url_date:
            continue

        title = link.get_text(strip=True)
        if not title or len(title) < 10:
            continue
        # Skip nav/footer links
        if any(skip in title.lower() for skip in ["next", "previous", "page", "more"]):
            continue

        abs_href = _abs_url(href, base_url)
        if abs_href in seen_urls:
            continue
        seen_urls.add(abs_href)

        year, month = int(url_date.group(1)), int(url_date.group(2))
        if year < 2020 or month < 1 or month > 12:
            continue

        # Approximate the date from the URL (day unknown, use 1st)
        # Try to find a more precise date in surrounding context
        hearing_date = None
        parent = link.parent
        if parent:
            hearing_date = parse_date(parent.get_text(" ", strip=True))
        if not hearing_date:
            hearing_date = f"{year:04d}-{month:02d}-01"

        if not _is_recent(hearing_date, cutoff):
            continue

        results.append(ScrapedHearing(
            title=title,
            date=hearing_date,
            url=abs_href,
        ))

    return results


# ---------------------------------------------------------------------------
# Scraper: wordpress_blog (Senate Intelligence)
# article.et_pb_post elements
# ---------------------------------------------------------------------------

def scrape_wordpress_blog(html: str, base_url: str, cutoff: datetime) -> list[ScrapedHearing]:
    """Parse WordPress blog-style sites (Senate Intelligence)."""
    soup = BeautifulSoup(html, "lxml")
    results = []

    for article in soup.find_all("article", class_=re.compile(r"et_pb_post|post")):
        # Title is in h2 > a
        h2 = article.find("h2")
        if not h2:
            continue
        link = h2.find("a", href=True)
        if not link:
            continue
        title = link.get_text(strip=True)
        href = link.get("href", "")

        # Date from URL path: /YYYY/MM/DD/slug/
        hearing_date = None
        url_date = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", href)
        if url_date:
            hearing_date = f"{url_date.group(1)}-{url_date.group(2)}-{url_date.group(3)}"

        # Fallback: date from article text
        if not hearing_date:
            hearing_date = parse_date(article.get_text(" ", strip=True))

        if not hearing_date or not _is_recent(hearing_date, cutoff):
            continue

        results.append(ScrapedHearing(
            title=title,
            date=hearing_date,
            url=_abs_url(href, base_url),
        ))

    return results


# ---------------------------------------------------------------------------
# Scraper: wordpress_elementor (Senate HSGAC, Indian Affairs)
# jet-listing-grid with jet-listing-grid__item children
# ---------------------------------------------------------------------------

def scrape_wordpress_elementor(html: str, base_url: str, cutoff: datetime) -> list[ScrapedHearing]:
    """Parse WordPress + Elementor + JetEngine sites."""
    soup = BeautifulSoup(html, "lxml")
    results = []

    for item in soup.find_all("div", class_=re.compile(r"jet-listing-grid__item")):
        link = item.find("a", href=True)
        if not link:
            continue
        title = link.get_text(strip=True)
        href = link.get("href", "")
        if not title or len(title) < 10:
            continue

        hearing_date = parse_date(item.get_text(" ", strip=True))
        if not hearing_date or not _is_recent(hearing_date, cutoff):
            continue

        results.append(ScrapedHearing(
            title=title,
            date=hearing_date,
            url=_abs_url(href, base_url),
        ))

    return results


# ---------------------------------------------------------------------------
# Scraper: evo_framework (House Appropriations, Foreign Affairs, Judiciary, Rules)
# time[datetime] elements near links
# ---------------------------------------------------------------------------

def scrape_evo_framework(html: str, base_url: str, cutoff: datetime) -> list[ScrapedHearing]:
    """Parse Drupal evo-framework sites with time[datetime] elements."""
    soup = BeautifulSoup(html, "lxml")
    results = []
    seen_urls = set()

    # Strategy 1: Find time[datetime] elements and look for nearby links
    for time_el in soup.find_all("time", attrs={"datetime": True}):
        dt_str = time_el.get("datetime", "")
        hearing_date = parse_date(dt_str)
        if not hearing_date or not _is_recent(hearing_date, cutoff):
            continue

        # Walk up to find the container with a title link
        container = time_el.parent
        for _ in range(5):  # walk up at most 5 levels
            if container is None:
                break
            link = container.find("a", href=True)
            if link:
                title = link.get_text(strip=True)
                href = link.get("href", "")
                if title and len(title) >= 10 and href:
                    abs_href = _abs_url(href, base_url)
                    if abs_href not in seen_urls:
                        seen_urls.add(abs_href)
                        results.append(ScrapedHearing(
                            title=title,
                            date=hearing_date,
                            url=abs_href,
                        ))
                    break
            container = container.parent

    return results


# ---------------------------------------------------------------------------
# Scraper: aspnet_card (House Financial Services, Armed Services)
# article.card-h-event or article.article-item with time[datetime]
# ---------------------------------------------------------------------------

def scrape_aspnet_card(html: str, base_url: str, cutoff: datetime) -> list[ScrapedHearing]:
    """Parse ASP.NET sites with article.card-h-event or article.article-item elements."""
    soup = BeautifulSoup(html, "lxml")
    results = []

    for article in soup.find_all("article", class_=re.compile(r"card-h-event|article-item")):
        link = article.find("a", href=True)
        time_el = article.find("time")

        if not link:
            continue
        title = link.get_text(strip=True)
        href = link.get("href", "")
        if not title or len(title) < 10:
            continue

        hearing_date = None
        if time_el:
            dt_attr = time_el.get("datetime", "")
            hearing_date = parse_date(dt_attr) or parse_date(time_el.get_text(strip=True))
        if not hearing_date:
            hearing_date = parse_date(article.get_text(" ", strip=True))

        if not hearing_date or not _is_recent(hearing_date, cutoff):
            continue

        results.append(ScrapedHearing(
            title=title,
            date=hearing_date,
            url=_abs_url(href, base_url),
        ))

    return results


# ---------------------------------------------------------------------------
# Scraper: html_table (House Budget, Science, Education & Workforce)
# Generic table rows with title links and date cells
# ---------------------------------------------------------------------------

def scrape_html_table(html: str, base_url: str, cutoff: datetime) -> list[ScrapedHearing]:
    """Parse generic HTML table-based hearing listings."""
    soup = BeautifulSoup(html, "lxml")
    results = []

    # Also try time[datetime] elements first (Budget has both table and time elements)
    evo_results = scrape_evo_framework(html, base_url, cutoff)
    if evo_results:
        return evo_results

    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if not cells:
                continue

            link = row.find("a", href=True)
            if not link:
                continue
            title = link.get_text(strip=True)
            if not title or len(title) < 10:
                continue

            hearing_date = parse_date(row.get_text(" ", strip=True))
            if not hearing_date or not _is_recent(hearing_date, cutoff):
                continue

            results.append(ScrapedHearing(
                title=title,
                date=hearing_date,
                url=_abs_url(link.get("href", ""), base_url),
            ))

    return results


# ---------------------------------------------------------------------------
# Scraper: wordpress_single_event (House Ways & Means)
# div.single-event with span.month/day/year
# ---------------------------------------------------------------------------

def scrape_wordpress_single_event(html: str, base_url: str, cutoff: datetime) -> list[ScrapedHearing]:
    """Parse WordPress sites with div.single-event layout (Ways & Means)."""
    soup = BeautifulSoup(html, "lxml")
    results = []

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
        except (ValueError, TypeError):
            continue

        if not _is_recent(hearing_date, cutoff):
            continue

        link = info.find("a")
        title = link.get_text(strip=True) if link else info.get_text(strip=True)[:120]
        href = link.get("href", "") if link else ""

        results.append(ScrapedHearing(
            title=title,
            date=hearing_date,
            url=_abs_url(href, base_url) if href else "",
        ))

    return results


# ---------------------------------------------------------------------------
# Scraper: tribe_events (House Homeland Security)
# article.tribe-events-calendar-list__event
# ---------------------------------------------------------------------------

def scrape_tribe_events(html: str, base_url: str, cutoff: datetime) -> list[ScrapedHearing]:
    """Parse WordPress Tribe Events calendar sites."""
    soup = BeautifulSoup(html, "lxml")
    results = []

    for article in soup.find_all("article", class_=re.compile(r"tribe-events")):
        link = article.find("a", href=True)
        time_el = article.find("time", attrs={"datetime": True})

        if not link:
            continue
        title = link.get_text(strip=True)
        href = link.get("href", "")
        if not title or len(title) < 10:
            continue

        hearing_date = None
        if time_el:
            hearing_date = parse_date(time_el.get("datetime", ""))
        if not hearing_date:
            hearing_date = parse_date(article.get_text(" ", strip=True))

        if not hearing_date or not _is_recent(hearing_date, cutoff):
            continue

        results.append(ScrapedHearing(
            title=title,
            date=hearing_date,
            url=_abs_url(href, base_url),
        ))

    return results


# ---------------------------------------------------------------------------
# Scraper: wordpress_featured_post (House Oversight)
# div.post.featured-post with dates in link text
# ---------------------------------------------------------------------------

def scrape_wordpress_featured_post(html: str, base_url: str, cutoff: datetime) -> list[ScrapedHearing]:
    """Parse WordPress featured-post listings (Oversight)."""
    soup = BeautifulSoup(html, "lxml")
    results = []

    for post in soup.find_all("div", class_=re.compile(r"post")):
        if not post.get("class") or "post" not in post.get("class", []):
            continue

        link = post.find("a", href=True)
        if not link:
            continue
        title = link.get_text(strip=True)
        href = link.get("href", "")
        if not title or len(title) < 10:
            continue

        # Date is often in the post text
        hearing_date = parse_date(post.get_text(" ", strip=True))
        if not hearing_date or not _is_recent(hearing_date, cutoff):
            continue

        results.append(ScrapedHearing(
            title=title,
            date=hearing_date,
            url=_abs_url(href, base_url),
        ))

    return results


# ---------------------------------------------------------------------------
# Scraper: wordpress_calblocker (House Veterans Affairs)
# article.calblocker elements
# ---------------------------------------------------------------------------

def scrape_wordpress_calblocker(html: str, base_url: str, cutoff: datetime) -> list[ScrapedHearing]:
    """Parse WordPress calendar blocker sites (Veterans Affairs)."""
    soup = BeautifulSoup(html, "lxml")
    results = []

    for article in soup.find_all("article", class_=re.compile(r"calblocker")):
        link = article.find("a", href=True)
        if not link:
            continue
        title = link.get_text(strip=True)
        href = link.get("href", "")
        if not title or len(title) < 10:
            continue

        hearing_date = parse_date(article.get_text(" ", strip=True))
        if not hearing_date or not _is_recent(hearing_date, cutoff):
            continue

        results.append(ScrapedHearing(
            title=title,
            date=hearing_date,
            url=_abs_url(href, base_url),
        ))

    return results


# ---------------------------------------------------------------------------
# Scraper registry — dispatch by scraper_type from committees.json
# ---------------------------------------------------------------------------

SCRAPER_REGISTRY: dict[str, callable] = {
    "drupal_table": scrape_drupal_table,
    "coldfusion_table": scrape_coldfusion_table,
    "new_senate_cms": scrape_new_senate_cms,
    "drupal_links": scrape_drupal_links,
    "wordpress_blog": scrape_wordpress_blog,
    "wordpress_elementor": scrape_wordpress_elementor,
    "evo_framework": scrape_evo_framework,
    "aspnet_card": scrape_aspnet_card,
    "html_table": scrape_html_table,
    "wordpress_single_event": scrape_wordpress_single_event,
    "tribe_events": scrape_tribe_events,
    "wordpress_featured_post": scrape_wordpress_featured_post,
    "wordpress_calblocker": scrape_wordpress_calblocker,
}


def scrape_website(scraper_type: str, html: str, base_url: str, cutoff: datetime) -> list[ScrapedHearing]:
    """Dispatch to the right scraper based on type. Returns [] if type unknown."""
    fn = SCRAPER_REGISTRY.get(scraper_type)
    if not fn:
        if scraper_type != "youtube_only":
            log.warning("Unknown scraper type: %s", scraper_type)
        return []
    try:
        return fn(html, base_url, cutoff)
    except Exception as e:
        log.warning("Scraper %s failed: %s", scraper_type, e)
        return []


# ---------------------------------------------------------------------------
# Generic link extractor — fallback for JS-rendered pages with no
# specific scraper or when the designated scraper returns nothing.
# Looks for links containing "hearing" with nearby date text.
# ---------------------------------------------------------------------------

def scrape_generic_links(html: str, base_url: str, cutoff: datetime) -> list[ScrapedHearing]:
    """Extract hearing links from any page by looking for links with date context."""
    soup = BeautifulSoup(html, "lxml")
    results = []
    seen_urls = set()

    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        title = link.get_text(strip=True)
        if not title or len(title) < 15:
            continue
        # Skip obviously non-hearing links
        if any(skip in title.lower() for skip in [
            "next", "previous", "page", "more", "login", "sign in",
            "contact", "about", "home", "search",
        ]):
            continue

        abs_href = _abs_url(href, base_url)
        if abs_href in seen_urls:
            continue

        # Try to find a date in surrounding context
        hearing_date = None

        # Check the link's parent for date text
        parent = link.parent
        if parent:
            hearing_date = parse_date(parent.get_text(" ", strip=True))

        # Walk up a few levels if needed
        if not hearing_date and parent:
            grandparent = parent.parent
            if grandparent:
                hearing_date = parse_date(grandparent.get_text(" ", strip=True))

        # Check for time elements nearby
        if not hearing_date:
            container = link.parent
            for _ in range(3):
                if container is None:
                    break
                time_el = container.find("time", attrs={"datetime": True})
                if time_el:
                    hearing_date = parse_date(time_el.get("datetime", ""))
                    break
                container = container.parent

        if not hearing_date or not _is_recent(hearing_date, cutoff):
            continue

        seen_urls.add(abs_href)
        results.append(ScrapedHearing(
            title=title,
            date=hearing_date,
            url=abs_href,
        ))

    return results


SCRAPER_REGISTRY["generic_links"] = scrape_generic_links


# ---------------------------------------------------------------------------
# JS-rendered page scraper — connects to clawdbot Chrome via CDP
# ---------------------------------------------------------------------------

_CDP_URL = "http://127.0.0.1:18800"


def scrape_js_rendered(
    hearings_url: str,
    scraper_type: str,
    base_url: str,
    cutoff: datetime,
) -> list[ScrapedHearing]:
    """Fetch a JS-rendered page via the clawdbot Chrome browser and scrape it.

    Connects to the running Chrome instance via CDP on port 18800, navigates
    to hearings_url, waits for JS to render, then passes the rendered HTML
    to the appropriate scraper function.

    Falls back gracefully (returns []) if the browser is not running.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("playwright not installed; cannot scrape JS-rendered page: %s", hearings_url)
        return []

    pw = None
    browser = None
    page = None
    try:
        pw = sync_playwright().start()
        browser = pw.chromium.connect_over_cdp(_CDP_URL)
        context = browser.contexts[0] if browser.contexts else browser.new_context()
        page = context.new_page()

        log.info("JS scraper: navigating to %s", hearings_url)
        page.goto(hearings_url, timeout=30000)
        page.wait_for_timeout(6000)
        html = page.content()

    except Exception as e:
        log.warning("JS scraper: browser connection failed for %s: %s", hearings_url, e)
        return []
    finally:
        if page:
            try:
                page.close()
            except Exception:
                pass
        # Do NOT close the browser — it's the shared clawdbot instance
        if pw:
            try:
                pw.stop()
            except Exception:
                pass

    # Dispatch to the designated scraper
    results = scrape_website(scraper_type, html, base_url, cutoff)

    # If the designated scraper found nothing, try the generic link extractor
    if not results and scraper_type != "generic_links":
        log.info("JS scraper: %s returned 0 results, trying generic_links", scraper_type)
        results = scrape_generic_links(html, base_url, cutoff)

    return results
