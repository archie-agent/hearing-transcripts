"""Scrape hearing detail pages to extract testimony PDF URLs.

Each committee platform embeds testimony documents differently. This module
provides per-platform extractors that parse the detail-page HTML and return
a list of absolute PDF URLs suitable for download by extract.py.

Main entry point:
    scrape_hearing_detail(committee_key, detail_url, committee_meta) -> DetailResult
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, Tag

from isvp import extract_isvp_url
from utils import RateLimiter, get_http_client

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level rate limiter (shared across all detail page fetches)
# ---------------------------------------------------------------------------
_rate_limiter = RateLimiter(min_delay=1.0)

# ---------------------------------------------------------------------------
# Keywords that suggest a link points to testimony / witness statements
# ---------------------------------------------------------------------------
_TESTIMONY_KEYWORDS = re.compile(
    r"testimony|statement|written|prepared|download|witness|"
    r"submitted|oral|opening\s+remarks|full\s+text",
    re.IGNORECASE,
)

# YouTube embed pattern (VIDEO_ID is always exactly 11 chars)
_YOUTUBE_EMBED_RE = re.compile(
    r'youtube\.com/embed/([A-Za-z0-9_-]{11})', re.IGNORECASE
)

# Keywords to exclude -- navigation, procedural, or media links
_EXCLUDE_KEYWORDS = re.compile(
    r"livestream|webcast|video|archive|press\s+release|"
    r"add\s+to\s+calendar|rss|share|print|twitter|facebook|"
    r"instagram|youtube|podcast",
    re.IGNORECASE,
)

# Pre-compiled patterns for link filter functions
_FILE_EXT_RE = re.compile(r"\.\w{2,4}$")
_WP_UPLOAD_PDF_RE = re.compile(r"/wp-content/uploads/\d{4}/\d{2}/[^/]+\.pdf", re.IGNORECASE)
_FILES_SERVE_RE = re.compile(r"files\.serve", re.IGNORECASE)
_FILE_ID_RE = re.compile(r"file_id=", re.IGNORECASE)
_HOUSE_DOC_PATH_RE = re.compile(
    r"/sites/default/files/|/uploads/|/documents/|/files/",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_youtube_embeds(html: str) -> list[dict]:
    """Extract YouTube video IDs from iframe embeds on a detail page."""
    soup = BeautifulSoup(html, "lxml")
    results = []
    seen: set[str] = set()
    for iframe in soup.find_all("iframe", src=True):
        src = iframe.get("src", "")
        m = _YOUTUBE_EMBED_RE.search(src)
        if m and m.group(1) not in seen:
            vid_id = m.group(1)
            seen.add(vid_id)
            results.append({
                "youtube_id": vid_id,
                "youtube_url": f"https://www.youtube.com/watch?v={vid_id}",
            })
    return results


def _abs_url(href: str, base_url: str) -> str:
    """Resolve a potentially relative URL against a base URL."""
    if not href or href.startswith(("#", "javascript:", "mailto:")):
        return ""
    if href.startswith("http"):
        return href
    return urljoin(base_url, href)


def _is_pdf_href(href: str) -> bool:
    """Check whether a URL looks like it points to a PDF resource."""
    if not href:
        return False
    lower = href.lower()
    # Explicit .pdf extension
    if lower.endswith(".pdf"):
        return True
    # Query-string that mentions pdf
    if "format=pdf" in lower:
        return True
    # Senate /download/ pattern (often serves PDFs without .pdf extension)
    if "/download/" in lower:
        return True
    # Senate services/files pattern (GUID-based)
    if "/services/files/" in lower:
        return True
    # ColdFusion file serve pattern
    if "files.serve" in lower:
        return True
    return False


def _has_testimony_signal(tag: Tag) -> bool:
    """Check whether a link tag or its immediate context contains testimony keywords."""
    # Check link text itself
    text = tag.get_text(strip=True)
    if text and _TESTIMONY_KEYWORDS.search(text):
        return True
    # Check title attribute
    title = tag.get("title", "")
    if title and _TESTIMONY_KEYWORDS.search(title):
        return True
    # Check aria-label
    aria = tag.get("aria-label", "")
    if aria and _TESTIMONY_KEYWORDS.search(aria):
        return True
    # Check parent text (one level up)
    parent = tag.parent
    if parent and parent.name not in ("body", "html", "[document]"):
        parent_text = parent.get_text(strip=True)
        if parent_text and _TESTIMONY_KEYWORDS.search(parent_text):
            return True
    return False


def _should_exclude(tag: Tag, href: str) -> bool:
    """Filter out links that are clearly not testimony PDFs."""
    text = tag.get_text(strip=True)
    if text and _EXCLUDE_KEYWORDS.search(text):
        return True
    if href and _EXCLUDE_KEYWORDS.search(href):
        return True
    return False


def _deduplicate_urls(urls: list[str]) -> list[str]:
    """Remove duplicate URLs while preserving order."""
    seen: set[str] = set()
    result: list[str] = []
    for url in urls:
        # Normalize trailing slashes for dedup
        normalized = url.rstrip("/")
        if normalized not in seen:
            seen.add(normalized)
            result.append(url)
    return result


def _fetch_detail_page(url: str) -> str | None:
    """Fetch a hearing detail page with rate limiting. Returns HTML or None."""
    domain = urlparse(url).netloc
    _rate_limiter.wait(domain)

    try:
        client = get_http_client(retries=2, timeout=25.0)
        with client:
            resp = client.get(url)
            if resp.status_code != 200:
                log.warning("Detail page HTTP %s: %s", resp.status_code, url)
                return None
            return resp.text
    except (httpx.HTTPError, OSError) as e:
        log.warning("Detail page fetch error for %s: %s", url, e)
        return None


# ===========================================================================
#  Shared link-extraction engine
#
#  All platform extractors delegate to _extract_links_from_containers,
#  supplying only the per-platform container selector and link filter.
# ===========================================================================


def _accept_drupal_link(link: Tag, href: str) -> bool:
    """Link filter for Senate Drupal / new CMS / drupal_links pages.

    Accepts:
    - Any href recognized by _is_pdf_href (PDF extension, /download/, etc.)
    - Testimony-signalled links with /download/ or /services/files/ paths
    - Testimony-signalled links with a file-like extension
    """
    if _is_pdf_href(href):
        return True
    if _has_testimony_signal(link):
        if "/download/" in href or "/services/files/" in href:
            return True
        if _FILE_EXT_RE.search(href.split("?")[0]):
            return True
    return False


def _accept_wordpress_link(link: Tag, href: str) -> bool:
    """Link filter for WordPress-based committee pages.

    Accepts:
    - /wp-content/uploads/YYYY/MM/*.pdf (WordPress upload pattern)
    - Any href recognized by _is_pdf_href
    - Testimony-signalled links with a file-like extension
    """
    if _WP_UPLOAD_PDF_RE.search(href):
        return True
    if _is_pdf_href(href):
        return True
    if _has_testimony_signal(link):
        if _FILE_EXT_RE.search(href.split("?")[0]):
            return True
    return False


def _accept_coldfusion_link(link: Tag, href: str) -> bool:
    """Link filter for ColdFusion-based Senate pages.

    Accepts:
    - files.serve pattern (ColdFusion file delivery)
    - Any href recognized by _is_pdf_href
    - Testimony-signalled links with file_id= parameter
    """
    if _FILES_SERVE_RE.search(href):
        return True
    if _is_pdf_href(href):
        return True
    if _has_testimony_signal(link):
        if _FILE_ID_RE.search(href):
            return True
    return False


def _accept_house_link(link: Tag, href: str) -> bool:
    """Link filter for House evo_framework pages.

    Accepts:
    - docs.house.gov PDF links unconditionally
    - Other PDFs with testimony signal or from document storage paths
    - docs.house.gov links (even non-PDF) with testimony signal
    """
    if "docs.house.gov" in href and _is_pdf_href(href):
        return True
    if _is_pdf_href(href):
        if _has_testimony_signal(link):
            return True
        if _HOUSE_DOC_PATH_RE.search(href):
            return True
    if "docs.house.gov" in href and _has_testimony_signal(link):
        return True
    return False


def _accept_aspnet_link(link: Tag, href: str) -> bool:
    """Link filter for House ASP.NET card-style pages.

    Same as _accept_house_link but without the non-PDF docs.house.gov
    testimony fallback.
    """
    if "docs.house.gov" in href and _is_pdf_href(href):
        return True
    if _is_pdf_href(href):
        if _has_testimony_signal(link):
            return True
        if _HOUSE_DOC_PATH_RE.search(href):
            return True
    return False


def _extract_links_from_containers(
    html: str,
    base_url: str,
    container_fn: Callable | None = None,
    link_filter_fn: Callable | None = None,
) -> list[str]:
    """Core link extraction logic shared by all platform extractors.

    Args:
        html: Raw HTML of the detail page.
        base_url: Base URL for resolving relative links.
        container_fn: Optional function(soup) -> list[Tag] that returns
            the container elements to search. If None, searches entire
            document.  When containers are returned, each is searched
            independently; duplicates across containers are suppressed.
        link_filter_fn: Optional function(link_tag, href) -> bool that
            decides whether to accept a non-excluded link. If None, uses
            _accept_drupal_link (the most common pattern).
    """
    soup = BeautifulSoup(html, "lxml")
    accept = link_filter_fn if link_filter_fn is not None else _accept_drupal_link

    if container_fn is not None:
        search_areas = container_fn(soup)
        if not search_areas:
            search_areas = [soup]
    else:
        search_areas = [soup]

    urls: list[str] = []
    seen: set[str] = set()

    for area in search_areas:
        for link in area.find_all("a", href=True):
            href = link.get("href", "")
            abs_href = _abs_url(href, base_url)
            if not abs_href or abs_href in seen:
                continue
            seen.add(abs_href)

            if _should_exclude(link, href):
                continue

            if accept(link, href):
                urls.append(abs_href)

    return _deduplicate_urls(urls)


# ===========================================================================
#  Platform-specific extractors (thin wrappers)
#
#  Each function takes (html, base_url) and returns a list of absolute PDF
#  URLs found on that detail page.
# ===========================================================================


# ---------------------------------------------------------------------------
# drupal_table / new_senate_cms
# Senate Finance, Banking, Budget, Appropriations, Foreign Relations, HELP,
# Judiciary, Armed Services, Agriculture, Rules
# ---------------------------------------------------------------------------

def _extract_drupal_senate(html: str, base_url: str) -> list[str]:
    """Extract testimony PDFs from Senate Drupal / new CMS detail pages."""
    return _extract_links_from_containers(html, base_url)


# ---------------------------------------------------------------------------
# drupal_links
# Senate Commerce, Energy, Veterans
# ---------------------------------------------------------------------------

def _extract_drupal_links(html: str, base_url: str) -> list[str]:
    """Extract PDFs from Senate Drupal announcement-style detail pages."""
    return _extract_links_from_containers(html, base_url)


# ---------------------------------------------------------------------------
# wordpress_blog / wordpress_elementor
# Senate Intelligence, HSGAC, Indian Affairs
# ---------------------------------------------------------------------------

def _extract_wordpress(html: str, base_url: str) -> list[str]:
    """Extract PDFs from WordPress-based Senate committee detail pages."""
    def containers(soup: BeautifulSoup) -> list[Tag]:
        return soup.find_all("div", class_=re.compile(
            r"entry-content|post-content|et_pb_text|elementor-widget-text|"
            r"jet-listing-dynamic|page-content|article-body"
        ))
    return _extract_links_from_containers(
        html, base_url,
        container_fn=containers,
        link_filter_fn=_accept_wordpress_link,
    )


# ---------------------------------------------------------------------------
# coldfusion_table
# Senate EPW, Small Business
# ---------------------------------------------------------------------------

def _extract_coldfusion(html: str, base_url: str) -> list[str]:
    """Extract PDFs from ColdFusion-based Senate detail pages."""
    return _extract_links_from_containers(
        html, base_url,
        link_filter_fn=_accept_coldfusion_link,
    )


# ---------------------------------------------------------------------------
# evo_framework
# House Appropriations, Foreign Affairs, Judiciary, Rules
# ---------------------------------------------------------------------------

def _extract_evo_framework(html: str, base_url: str) -> list[str]:
    """Extract PDFs from House evo-framework detail pages."""
    return _extract_links_from_containers(
        html, base_url,
        link_filter_fn=_accept_house_link,
    )


# ---------------------------------------------------------------------------
# aspnet_card
# House Financial Services, Armed Services
# ---------------------------------------------------------------------------

def _extract_aspnet_card(html: str, base_url: str) -> list[str]:
    """Extract PDFs from ASP.NET card-style House detail pages."""
    def containers(soup: BeautifulSoup) -> list[Tag]:
        sections = soup.find_all(
            ["div", "section"],
            class_=re.compile(
                r"document|testimony|witness|statement|download|attachment",
                re.IGNORECASE,
            ),
        )
        # Search matching sections first, then the full page
        return sections + [soup] if sections else []
    return _extract_links_from_containers(
        html, base_url,
        container_fn=containers,
        link_filter_fn=_accept_aspnet_link,
    )


# ---------------------------------------------------------------------------
# Generic fallback extractor
# ---------------------------------------------------------------------------

def _extract_pdf_links(html: str, base_url: str) -> list[str]:
    """Generic fallback: find all PDF links on a page.

    Accepts links where:
    - The href ends in .pdf, OR
    - The href contains /download/, /services/files/, or Files.Serve

    AND at least one of:
    - The link text contains testimony keywords, OR
    - The link is in a section/container with testimony keywords in its class/id

    As a final fallback, if no testimony-signalled PDFs are found, returns
    ALL .pdf links on the page (detail pages usually only have testimony PDFs).
    """
    soup = BeautifulSoup(html, "lxml")
    testimony_urls: list[str] = []
    all_pdf_urls: list[str] = []

    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        abs_href = _abs_url(href, base_url)
        if not abs_href:
            continue

        if _should_exclude(link, href):
            continue

        if not _is_pdf_href(href):
            continue

        all_pdf_urls.append(abs_href)

        # Check for testimony signals
        if _has_testimony_signal(link):
            testimony_urls.append(abs_href)
            continue

        # Check if link is inside a testimony-related container
        for parent in link.parents:
            if parent.name in ("body", "html", "[document]"):
                break
            parent_classes = " ".join(parent.get("class", []))
            parent_id = parent.get("id", "")
            combined = f"{parent_classes} {parent_id}"
            if _TESTIMONY_KEYWORDS.search(combined):
                testimony_urls.append(abs_href)
                break

    # Prefer testimony-signalled PDFs; fall back to all PDFs
    result = testimony_urls if testimony_urls else all_pdf_urls
    return _deduplicate_urls(result)


# ===========================================================================
#  Extractor registry — maps scraper_type to the right detail-page extractor
# ===========================================================================

_EXTRACTOR_REGISTRY: dict[str, Callable] = {
    # Senate platforms
    "drupal_table": _extract_drupal_senate,
    "new_senate_cms": _extract_drupal_senate,
    "drupal_links": _extract_drupal_links,
    "wordpress_blog": _extract_wordpress,
    "wordpress_elementor": _extract_wordpress,
    "coldfusion_table": _extract_coldfusion,
    # House platforms
    "evo_framework": _extract_evo_framework,
    "aspnet_card": _extract_aspnet_card,
    # Other House types fall through to generic
    "html_table": _extract_pdf_links,
    "wordpress_single_event": _extract_pdf_links,
    "tribe_events": _extract_pdf_links,
    "wordpress_featured_post": _extract_pdf_links,
    "wordpress_calblocker": _extract_pdf_links,
}


# ===========================================================================
#  Public API
# ===========================================================================

@dataclass
class DetailResult:
    """Result of scraping a hearing detail page."""

    pdf_urls: list[str] = field(default_factory=list)
    isvp_comm: str | None = None
    isvp_filename: str | None = None
    youtube_url: str | None = None
    youtube_id: str | None = None


def scrape_hearing_detail(
    committee_key: str,
    detail_url: str,
    committee_meta: dict,
) -> DetailResult:
    """Fetch a hearing detail page and extract testimony PDFs + media sources.

    Returns a DetailResult with PDF URLs and any ISVP/YouTube params discovered
    on the page.  The caller is responsible for merging these into hearing.sources.

    Args:
        committee_key: Dotted key like "senate.finance".
        detail_url: The hearing detail page URL (website_url from sources).
        committee_meta: Committee metadata dict from committees.json.

    Returns:
        DetailResult with pdf_urls, isvp_comm/filename, youtube_url/id.
    """
    if not detail_url:
        return DetailResult()

    # Fetch the detail page
    html = _fetch_detail_page(detail_url)
    if not html:
        return DetailResult()

    base_url = detail_url
    result = DetailResult()

    # -------------------------------------------------------------------
    # ISVP iframe detection (Senate committees only)
    # -------------------------------------------------------------------
    if committee_meta.get("chamber") == "senate":
        isvp_params = extract_isvp_url(html)
        if isvp_params:
            result.isvp_comm = isvp_params["comm"]
            result.isvp_filename = isvp_params["filename"]
            log.info(
                "ISVP iframe detected for %s: comm=%s filename=%s",
                committee_key, isvp_params["comm"], isvp_params["filename"],
            )

    # -------------------------------------------------------------------
    # YouTube embed detection (all committees)
    # -------------------------------------------------------------------
    yt_embeds = _extract_youtube_embeds(html)
    if yt_embeds:
        result.youtube_url = yt_embeds[0]["youtube_url"]
        result.youtube_id = yt_embeds[0]["youtube_id"]
        log.info("YouTube embed found for %s: %s", committee_key, yt_embeds[0]["youtube_id"])

    # -------------------------------------------------------------------
    # Testimony PDF extraction
    # -------------------------------------------------------------------
    scraper_type = committee_meta.get("scraper_type", "")
    extractor = _EXTRACTOR_REGISTRY.get(scraper_type)

    if extractor:
        result.pdf_urls = extractor(html, base_url)
        log.debug(
            "Extractor %s found %d PDFs on %s",
            scraper_type, len(result.pdf_urls), detail_url,
        )

    # If the platform extractor found nothing, try the generic fallback
    if not result.pdf_urls:
        result.pdf_urls = _extract_pdf_links(html, base_url)
        if result.pdf_urls:
            log.debug(
                "Generic fallback found %d PDFs on %s",
                len(result.pdf_urls), detail_url,
            )

    if result.pdf_urls:
        log.info(
            "Found %d testimony PDFs for %s: %s",
            len(result.pdf_urls), committee_key, detail_url,
        )

    return result
