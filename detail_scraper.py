"""Scrape hearing detail pages to extract testimony PDF URLs.

Each committee platform embeds testimony documents differently. This module
provides per-platform extractors that parse the detail-page HTML and return
a list of absolute PDF URLs suitable for download by extract.py.

Main entry point:
    scrape_hearing_detail(committee_key, detail_url, committee_meta) -> list[str]
"""

from __future__ import annotations

import logging
import re
from urllib.parse import urljoin, urlparse

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
    except Exception as e:
        log.warning("Detail page fetch error for %s: %s", url, e)
        return None


# ===========================================================================
#  Platform-specific extractors
#
#  Each function takes (html, base_url) and returns a list of absolute PDF
#  URLs found on that detail page.
# ===========================================================================


# ---------------------------------------------------------------------------
# drupal_table / new_senate_cms
# Senate Finance, Banking, Budget, Appropriations, Foreign Relations, HELP,
# Judiciary, Armed Services, Agriculture, Rules
#
# Detail pages have links with text containing "Download", "Testimony",
# "Statement". href ends in .pdf or contains /download/.
# ---------------------------------------------------------------------------

def _extract_drupal_senate(html: str, base_url: str) -> list[str]:
    """Extract testimony PDFs from Senate Drupal / new CMS detail pages.

    These sites typically present testimony documents as:
    - Direct .pdf links under a "Testimony" or "Statements" heading
    - /download/{filename} links served through the CMS
    - /services/files/{GUID} links for older content
    """
    soup = BeautifulSoup(html, "lxml")
    urls: list[str] = []

    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        abs_href = _abs_url(href, base_url)
        if not abs_href:
            continue

        if _should_exclude(link, href):
            continue

        # Strategy 1: href is clearly a PDF resource
        if _is_pdf_href(href):
            # Prefer links with testimony signal, but accept all PDFs
            # on detail pages (they're almost always testimony)
            urls.append(abs_href)
            continue

        # Strategy 2: Link text suggests testimony AND href looks downloadable
        if _has_testimony_signal(link):
            # Even without .pdf extension, /download/ paths are PDFs
            if "/download/" in href or "/services/files/" in href:
                urls.append(abs_href)
                continue
            # Accept if the link href has a file-like pattern
            if re.search(r"\.\w{2,4}$", href.split("?")[0]):
                urls.append(abs_href)

    return _deduplicate_urls(urls)


# ---------------------------------------------------------------------------
# drupal_links
# Senate Commerce, Energy, Veterans
#
# Similar to drupal_senate but files often at /download/{filename} or
# /services/files/{GUID}.
# ---------------------------------------------------------------------------

def _extract_drupal_links(html: str, base_url: str) -> list[str]:
    """Extract PDFs from Senate Drupal announcement-style detail pages.

    These sites use:
    - /download/{filename} paths
    - /services/files/{GUID} paths
    - Direct .pdf links
    """
    soup = BeautifulSoup(html, "lxml")
    urls: list[str] = []

    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        abs_href = _abs_url(href, base_url)
        if not abs_href:
            continue

        if _should_exclude(link, href):
            continue

        # Check for PDF-like hrefs
        if _is_pdf_href(href):
            urls.append(abs_href)
            continue

        # Check for testimony-signalled links with download patterns
        if _has_testimony_signal(link):
            if "/download/" in href or "/services/files/" in href:
                urls.append(abs_href)
                continue
            if re.search(r"\.\w{2,4}$", href.split("?")[0]):
                urls.append(abs_href)

    return _deduplicate_urls(urls)


# ---------------------------------------------------------------------------
# wordpress_blog / wordpress_elementor
# Senate Intelligence, HSGAC, Indian Affairs
#
# PDFs typically at /wp-content/uploads/YYYY/MM/{filename}.pdf
# ---------------------------------------------------------------------------

def _extract_wordpress(html: str, base_url: str) -> list[str]:
    """Extract PDFs from WordPress-based Senate committee detail pages.

    Common patterns:
    - /wp-content/uploads/YYYY/MM/{filename}.pdf
    - Direct .pdf links in the post body
    """
    soup = BeautifulSoup(html, "lxml")
    urls: list[str] = []

    # WordPress content area selectors (try most specific first)
    content_areas = (
        soup.find_all("div", class_=re.compile(
            r"entry-content|post-content|et_pb_text|elementor-widget-text|"
            r"jet-listing-dynamic|page-content|article-body"
        ))
    )
    # Fall back to the whole page if no content area found
    search_scope = content_areas if content_areas else [soup]

    for area in search_scope:
        for link in area.find_all("a", href=True):
            href = link.get("href", "")
            abs_href = _abs_url(href, base_url)
            if not abs_href:
                continue

            if _should_exclude(link, href):
                continue

            # Primary pattern: wp-content/uploads path with .pdf
            if re.search(r"/wp-content/uploads/\d{4}/\d{2}/[^/]+\.pdf", href, re.IGNORECASE):
                urls.append(abs_href)
                continue

            # General PDF links
            if _is_pdf_href(href):
                urls.append(abs_href)
                continue

            # Testimony-signalled links with file extensions
            if _has_testimony_signal(link):
                if re.search(r"\.\w{2,4}$", href.split("?")[0]):
                    urls.append(abs_href)

    return _deduplicate_urls(urls)


# ---------------------------------------------------------------------------
# coldfusion_table
# Senate EPW, Small Business
#
# Files served via /public/?a=Files.Serve&File_id={GUID}
# ---------------------------------------------------------------------------

def _extract_coldfusion(html: str, base_url: str) -> list[str]:
    """Extract PDFs from ColdFusion-based Senate detail pages.

    Primary pattern:
    - /public/?a=Files.Serve&File_id={GUID}
    - Direct .pdf links
    """
    soup = BeautifulSoup(html, "lxml")
    urls: list[str] = []

    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        abs_href = _abs_url(href, base_url)
        if not abs_href:
            continue

        if _should_exclude(link, href):
            continue

        # ColdFusion file serve pattern
        if re.search(r"files\.serve", href, re.IGNORECASE):
            urls.append(abs_href)
            continue

        # Standard PDF links
        if _is_pdf_href(href):
            urls.append(abs_href)
            continue

        # Testimony keyword links
        if _has_testimony_signal(link):
            if re.search(r"file_id=", href, re.IGNORECASE):
                urls.append(abs_href)

    return _deduplicate_urls(urls)


# ---------------------------------------------------------------------------
# evo_framework
# House Appropriations, Foreign Affairs, Judiciary, Rules
#
# Links to docs.house.gov or committee-hosted PDFs containing
# "testimony" or "statement" in the link text or URL.
# ---------------------------------------------------------------------------

def _extract_evo_framework(html: str, base_url: str) -> list[str]:
    """Extract PDFs from House evo-framework detail pages.

    Patterns:
    - Links to docs.house.gov with .pdf extension
    - Committee-hosted PDFs in /sites/default/files/ or /uploads/
    - Links with "testimony" or "statement" in text pointing to PDFs
    """
    soup = BeautifulSoup(html, "lxml")
    urls: list[str] = []

    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        abs_href = _abs_url(href, base_url)
        if not abs_href:
            continue

        if _should_exclude(link, href):
            continue

        # docs.house.gov PDF links
        if "docs.house.gov" in href and _is_pdf_href(href):
            urls.append(abs_href)
            continue

        # General PDF links
        if _is_pdf_href(href):
            # On House detail pages, accept PDFs with testimony signals
            # or from known document paths
            if _has_testimony_signal(link):
                urls.append(abs_href)
                continue
            # Also accept PDFs from common document paths
            if re.search(
                r"/sites/default/files/|/uploads/|/documents/|/files/",
                href, re.IGNORECASE,
            ):
                urls.append(abs_href)
                continue

        # Testimony keyword links to docs.house.gov (may not end in .pdf)
        if "docs.house.gov" in href and _has_testimony_signal(link):
            urls.append(abs_href)

    return _deduplicate_urls(urls)


# ---------------------------------------------------------------------------
# aspnet_card
# House Financial Services, Armed Services
#
# Similar to evo_framework. PDF links in hearing detail sections.
# ---------------------------------------------------------------------------

def _extract_aspnet_card(html: str, base_url: str) -> list[str]:
    """Extract PDFs from ASP.NET card-style House detail pages.

    Similar structure to evo_framework but may have different
    container selectors.
    """
    soup = BeautifulSoup(html, "lxml")
    urls: list[str] = []

    # Look for document/testimony sections
    sections = soup.find_all(
        ["div", "section"],
        class_=re.compile(r"document|testimony|witness|statement|download|attachment",
                          re.IGNORECASE),
    )
    # Also search the full page
    search_areas = sections + [soup] if sections else [soup]

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

            # docs.house.gov PDF links
            if "docs.house.gov" in href and _is_pdf_href(href):
                urls.append(abs_href)
                continue

            # General PDF links with testimony signal
            if _is_pdf_href(href):
                if _has_testimony_signal(link):
                    urls.append(abs_href)
                    continue
                # Accept PDFs from document storage paths
                if re.search(
                    r"/sites/default/files/|/uploads/|/documents/|/files/",
                    href, re.IGNORECASE,
                ):
                    urls.append(abs_href)

    return _deduplicate_urls(urls)


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

_EXTRACTOR_REGISTRY: dict[str, callable] = {
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

def scrape_hearing_detail(
    committee_key: str,
    detail_url: str,
    committee_meta: dict,
    sources: dict | None = None,
) -> list[str]:
    """Fetch a hearing detail page and extract testimony PDF URLs.

    Also checks for Senate ISVP video iframes.  When found, the ISVP
    comm/filename parameters are written into *sources* (if provided) as
    ``isvp_comm`` and ``isvp_filename``.

    Args:
        committee_key: Dotted key like "senate.finance".
        detail_url: The hearing detail page URL (website_url from sources).
        committee_meta: Committee metadata dict from committees.json.
        sources: Optional mutable dict; ISVP params are injected here when
            an ISVP iframe is detected on the page.

    Returns:
        List of absolute PDF URLs found on the detail page, or empty list.
    """
    if not detail_url:
        return []

    # Fetch the detail page
    html = _fetch_detail_page(detail_url)
    if not html:
        return []

    # Determine the base URL (may differ from detail_url after redirects)
    base_url = detail_url

    # -------------------------------------------------------------------
    # ISVP iframe detection (Senate committees only)
    # -------------------------------------------------------------------
    if sources is not None and committee_meta.get("chamber") == "senate":
        isvp_params = extract_isvp_url(html)
        if isvp_params:
            sources["isvp_comm"] = isvp_params["comm"]
            sources["isvp_filename"] = isvp_params["filename"]
            log.info(
                "ISVP iframe detected for %s: comm=%s filename=%s",
                committee_key, isvp_params["comm"], isvp_params["filename"],
            )

    # -------------------------------------------------------------------
    # YouTube embed detection (all committees)
    # -------------------------------------------------------------------
    if sources is not None and not sources.get("youtube_url"):
        yt_embeds = _extract_youtube_embeds(html)
        if yt_embeds:
            sources["youtube_url"] = yt_embeds[0]["youtube_url"]
            sources["youtube_id"] = yt_embeds[0]["youtube_id"]
            log.info("YouTube embed found for %s: %s", committee_key, yt_embeds[0]["youtube_id"])

    # -------------------------------------------------------------------
    # Testimony PDF extraction
    # -------------------------------------------------------------------
    # Select the platform-specific extractor
    scraper_type = committee_meta.get("scraper_type", "")
    extractor = _EXTRACTOR_REGISTRY.get(scraper_type)

    pdf_urls: list[str] = []

    if extractor:
        pdf_urls = extractor(html, base_url)
        log.debug(
            "Extractor %s found %d PDFs on %s",
            scraper_type, len(pdf_urls), detail_url,
        )

    # If the platform extractor found nothing, try the generic fallback
    if not pdf_urls:
        pdf_urls = _extract_pdf_links(html, base_url)
        if pdf_urls:
            log.debug(
                "Generic fallback found %d PDFs on %s",
                len(pdf_urls), detail_url,
            )

    if pdf_urls:
        log.info(
            "Found %d testimony PDFs for %s: %s",
            len(pdf_urls), committee_key, detail_url,
        )

    return pdf_urls
