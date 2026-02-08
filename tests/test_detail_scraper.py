"""Tests for detail_scraper.py -- per-platform testimony PDF extraction."""

import sys
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent.parent))

from detail_scraper import (
    _abs_url,
    _extract_coldfusion,
    _extract_drupal_links,
    _extract_drupal_senate,
    _extract_evo_framework,
    _extract_aspnet_card,
    _extract_pdf_links,
    _extract_wordpress,
    _has_testimony_signal,
    _is_pdf_href,
    _should_exclude,
)

# ===========================================================================
#  Helper function tests
# ===========================================================================


class TestAbsUrl:
    def test_already_absolute(self):
        assert _abs_url("https://example.com/doc.pdf", "https://base.com") == "https://example.com/doc.pdf"

    def test_relative_path(self):
        result = _abs_url("/files/testimony.pdf", "https://senate.gov/hearings/page")
        assert result == "https://senate.gov/files/testimony.pdf"

    def test_relative_no_slash(self):
        result = _abs_url("doc.pdf", "https://senate.gov/hearings/page/")
        assert result == "https://senate.gov/hearings/page/doc.pdf"

    def test_empty_href(self):
        assert _abs_url("", "https://example.com") == ""

    def test_javascript_href(self):
        assert _abs_url("javascript:void(0)", "https://example.com") == ""

    def test_mailto_href(self):
        assert _abs_url("mailto:test@example.com", "https://example.com") == ""

    def test_hash_href(self):
        assert _abs_url("#section", "https://example.com") == ""


class TestIsPdfHref:
    def test_pdf_extension(self):
        assert _is_pdf_href("/files/testimony.pdf") is True

    def test_pdf_extension_uppercase(self):
        assert _is_pdf_href("/files/TESTIMONY.PDF") is True

    def test_download_path(self):
        assert _is_pdf_href("/download/some-document") is True

    def test_services_files(self):
        assert _is_pdf_href("/services/files/ABCD-1234-EF56") is True

    def test_files_serve(self):
        assert _is_pdf_href("/public/?a=Files.Serve&File_id=ABC") is True

    def test_format_pdf_query(self):
        assert _is_pdf_href("/doc?format=pdf") is True

    def test_html_link(self):
        assert _is_pdf_href("/hearings/some-hearing") is False

    def test_empty(self):
        assert _is_pdf_href("") is False

    def test_image(self):
        assert _is_pdf_href("/images/photo.jpg") is False


# ===========================================================================
#  Senate: drupal_table / new_senate_cms extractor
# ===========================================================================

DRUPAL_SENATE_HTML = """
<html><body>
<h1>Hearing: Fiscal Policy and Economic Outlook</h1>
<div class="hearing-content">
    <h2>Testimony</h2>
    <ul>
        <li><a href="/download/testimony-smith.pdf">Download Testimony of John Smith</a></li>
        <li><a href="/download/statement-jones">Statement of Jane Jones</a></li>
        <li><a href="/services/files/A1B2C3D4-EF56-7890">Written Testimony of Bob Lee</a></li>
    </ul>
    <h2>Related</h2>
    <a href="/hearings/next-hearing">Next Hearing</a>
    <a href="https://www.youtube.com/watch?v=12345">Watch Livestream</a>
</div>
</body></html>
"""


class TestExtractDrupalSenate:
    def test_finds_pdf_download_links(self):
        urls = _extract_drupal_senate(DRUPAL_SENATE_HTML, "https://finance.senate.gov")
        # Should find the .pdf download and the /download/ link
        pdf_names = [u for u in urls if "testimony-smith" in u]
        assert len(pdf_names) == 1

    def test_finds_download_path_links(self):
        urls = _extract_drupal_senate(DRUPAL_SENATE_HTML, "https://finance.senate.gov")
        download_links = [u for u in urls if "/download/" in u]
        assert len(download_links) >= 2

    def test_finds_services_files_links(self):
        urls = _extract_drupal_senate(DRUPAL_SENATE_HTML, "https://finance.senate.gov")
        services_links = [u for u in urls if "/services/files/" in u]
        assert len(services_links) == 1

    def test_excludes_youtube_livestream(self):
        urls = _extract_drupal_senate(DRUPAL_SENATE_HTML, "https://finance.senate.gov")
        youtube_links = [u for u in urls if "youtube.com" in u]
        assert len(youtube_links) == 0

    def test_excludes_hearing_nav_links(self):
        urls = _extract_drupal_senate(DRUPAL_SENATE_HTML, "https://finance.senate.gov")
        nav_links = [u for u in urls if "next-hearing" in u]
        assert len(nav_links) == 0

    def test_urls_are_absolute(self):
        urls = _extract_drupal_senate(DRUPAL_SENATE_HTML, "https://finance.senate.gov")
        for url in urls:
            assert url.startswith("https://"), f"URL not absolute: {url}"


# ===========================================================================
#  Senate: drupal_links extractor
# ===========================================================================

DRUPAL_LINKS_HTML = """
<html><body>
<div class="field-content">
    <h3>Witness Testimony</h3>
    <p><a href="/download/2026-testimony-garcia.pdf">Testimony of Maria Garcia</a></p>
    <p><a href="/services/files/GUID-123-456">Prepared Statement of Alex Chen</a></p>
    <p><a href="/download/opening-remarks-taylor">Opening Remarks of Sam Taylor</a></p>
    <p><a href="/press-releases/2026/hearing-notice">Press Release</a></p>
</div>
</body></html>
"""


class TestExtractDrupalLinks:
    def test_finds_pdf_links(self):
        urls = _extract_drupal_links(DRUPAL_LINKS_HTML, "https://commerce.senate.gov")
        pdf_urls = [u for u in urls if "testimony-garcia" in u]
        assert len(pdf_urls) == 1

    def test_finds_services_files(self):
        urls = _extract_drupal_links(DRUPAL_LINKS_HTML, "https://commerce.senate.gov")
        guid_urls = [u for u in urls if "GUID-123" in u]
        assert len(guid_urls) == 1

    def test_finds_download_paths(self):
        urls = _extract_drupal_links(DRUPAL_LINKS_HTML, "https://commerce.senate.gov")
        download_urls = [u for u in urls if "/download/" in u]
        assert len(download_urls) >= 2

    def test_excludes_press_release(self):
        urls = _extract_drupal_links(DRUPAL_LINKS_HTML, "https://commerce.senate.gov")
        pr_urls = [u for u in urls if "press-release" in u.lower()]
        assert len(pr_urls) == 0

    def test_urls_are_absolute(self):
        urls = _extract_drupal_links(DRUPAL_LINKS_HTML, "https://commerce.senate.gov")
        for url in urls:
            assert url.startswith("https://"), f"URL not absolute: {url}"


# ===========================================================================
#  Senate: wordpress (blog / elementor) extractor
# ===========================================================================

WORDPRESS_HTML = """
<html><body>
<div class="entry-content">
    <h2>Witness Testimony</h2>
    <p><a href="/wp-content/uploads/2026/02/smith-testimony.pdf">Testimony of John Smith</a></p>
    <p><a href="/wp-content/uploads/2026/02/jones-statement.pdf">Statement of Jane Jones</a></p>
    <p><a href="/wp-content/uploads/2026/01/hearing-agenda.pdf">Hearing Agenda</a></p>
    <p><a href="/about-the-committee/">About the Committee</a></p>
</div>
</body></html>
"""


class TestExtractWordpress:
    def test_finds_wp_content_pdfs(self):
        urls = _extract_wordpress(WORDPRESS_HTML, "https://intelligence.senate.gov")
        assert len(urls) >= 2
        wp_urls = [u for u in urls if "/wp-content/uploads/" in u]
        assert len(wp_urls) >= 2

    def test_finds_testimony_pdf(self):
        urls = _extract_wordpress(WORDPRESS_HTML, "https://intelligence.senate.gov")
        testimony = [u for u in urls if "smith-testimony" in u]
        assert len(testimony) == 1

    def test_finds_statement_pdf(self):
        urls = _extract_wordpress(WORDPRESS_HTML, "https://intelligence.senate.gov")
        statement = [u for u in urls if "jones-statement" in u]
        assert len(statement) == 1

    def test_excludes_non_pdf_links(self):
        urls = _extract_wordpress(WORDPRESS_HTML, "https://intelligence.senate.gov")
        about_links = [u for u in urls if "about-the-committee" in u]
        assert len(about_links) == 0

    def test_urls_are_absolute(self):
        urls = _extract_wordpress(WORDPRESS_HTML, "https://intelligence.senate.gov")
        for url in urls:
            assert url.startswith("https://"), f"URL not absolute: {url}"


# ===========================================================================
#  Senate: coldfusion extractor
# ===========================================================================

COLDFUSION_HTML = """
<html><body>
<div class="testimony-section">
    <h3>Testimony</h3>
    <table>
        <tr>
            <td><a href="/public/?a=Files.Serve&File_id=ABC123">Testimony of Dr. Patricia Williams</a></td>
        </tr>
        <tr>
            <td><a href="/public/?a=Files.Serve&File_id=DEF456">Statement of Robert Brown</a></td>
        </tr>
        <tr>
            <td><a href="/files/agenda.pdf">Hearing Agenda</a></td>
        </tr>
    </table>
    <a href="/hearings">Back to Hearings</a>
</div>
</body></html>
"""


class TestExtractColdfusion:
    def test_finds_files_serve_links(self):
        urls = _extract_coldfusion(COLDFUSION_HTML, "https://epw.senate.gov")
        serve_urls = [u for u in urls if "Files.Serve" in u]
        assert len(serve_urls) == 2

    def test_finds_direct_pdf(self):
        urls = _extract_coldfusion(COLDFUSION_HTML, "https://epw.senate.gov")
        pdf_urls = [u for u in urls if "agenda.pdf" in u]
        assert len(pdf_urls) == 1

    def test_excludes_nav_links(self):
        urls = _extract_coldfusion(COLDFUSION_HTML, "https://epw.senate.gov")
        # The /hearings link should not appear because it's not a PDF
        back_links = [u for u in urls if u.rstrip("/").endswith("/hearings")]
        assert len(back_links) == 0

    def test_urls_are_absolute(self):
        urls = _extract_coldfusion(COLDFUSION_HTML, "https://epw.senate.gov")
        for url in urls:
            assert url.startswith("https://"), f"URL not absolute: {url}"


# ===========================================================================
#  House: evo_framework extractor
# ===========================================================================

EVO_FRAMEWORK_HTML = """
<html><body>
<div class="hearing-detail">
    <h1>Hearing on AI Safety</h1>
    <div class="witness-panel">
        <h2>Witness Testimony</h2>
        <a href="https://docs.house.gov/meetings/JU/JU00/20260210/testimony-smith.pdf">
            Testimony of Dr. Smith
        </a>
        <a href="https://docs.house.gov/meetings/JU/JU00/20260210/statement-jones.pdf">
            Written Statement of Prof. Jones
        </a>
        <a href="/sites/default/files/documents/hearing-charter.pdf">Hearing Charter</a>
        <a href="https://www.youtube.com/watch?v=abc123">Watch Video</a>
    </div>
</div>
</body></html>
"""


class TestExtractEvoFramework:
    def test_finds_docs_house_gov_pdfs(self):
        urls = _extract_evo_framework(EVO_FRAMEWORK_HTML, "https://judiciary.house.gov")
        docs_urls = [u for u in urls if "docs.house.gov" in u]
        assert len(docs_urls) == 2

    def test_finds_default_files_pdf(self):
        urls = _extract_evo_framework(EVO_FRAMEWORK_HTML, "https://judiciary.house.gov")
        default_urls = [u for u in urls if "/sites/default/files/" in u]
        assert len(default_urls) == 1

    def test_excludes_youtube(self):
        urls = _extract_evo_framework(EVO_FRAMEWORK_HTML, "https://judiciary.house.gov")
        yt_urls = [u for u in urls if "youtube.com" in u]
        assert len(yt_urls) == 0

    def test_urls_are_absolute(self):
        urls = _extract_evo_framework(EVO_FRAMEWORK_HTML, "https://judiciary.house.gov")
        for url in urls:
            assert url.startswith("https://"), f"URL not absolute: {url}"


# ===========================================================================
#  House: aspnet_card extractor
# ===========================================================================

ASPNET_CARD_HTML = """
<html><body>
<div class="hearing-detail">
    <section class="testimony">
        <h2>Written Testimony</h2>
        <a href="https://docs.house.gov/testimony/witness1.pdf">Testimony of Witness 1</a>
        <a href="https://docs.house.gov/testimony/witness2.pdf">Statement of Witness 2</a>
    </section>
    <section class="media">
        <a href="https://www.youtube.com/watch?v=xyz">Watch Livestream</a>
    </section>
    <a href="/calendar/">Back to Calendar</a>
</div>
</body></html>
"""


class TestExtractAspnetCard:
    def test_finds_testimony_pdfs(self):
        urls = _extract_aspnet_card(ASPNET_CARD_HTML, "https://financialservices.house.gov")
        pdf_urls = [u for u in urls if "docs.house.gov" in u]
        assert len(pdf_urls) == 2

    def test_excludes_youtube(self):
        urls = _extract_aspnet_card(ASPNET_CARD_HTML, "https://financialservices.house.gov")
        yt_urls = [u for u in urls if "youtube.com" in u]
        assert len(yt_urls) == 0

    def test_excludes_calendar_nav(self):
        urls = _extract_aspnet_card(ASPNET_CARD_HTML, "https://financialservices.house.gov")
        cal_urls = [u for u in urls if "/calendar/" in u]
        assert len(cal_urls) == 0

    def test_urls_are_absolute(self):
        urls = _extract_aspnet_card(ASPNET_CARD_HTML, "https://financialservices.house.gov")
        for url in urls:
            assert url.startswith("https://"), f"URL not absolute: {url}"


# ===========================================================================
#  Generic fallback extractor
# ===========================================================================

GENERIC_HTML = """
<html><body>
<div class="hearing-content">
    <h1>Hearing on Climate Policy</h1>
    <div class="documents">
        <a href="/files/testimony-witness-a.pdf">Testimony of Witness A</a>
        <a href="/files/statement-witness-b.pdf">Prepared Statement of Witness B</a>
        <a href="/files/exhibit-1.pdf">Exhibit 1</a>
        <a href="/press/release.html">Press Release</a>
        <a href="/images/banner.jpg">Banner Image</a>
    </div>
</div>
</body></html>
"""


class TestExtractPdfLinksGenericFallback:
    def test_finds_testimony_pdfs(self):
        urls = _extract_pdf_links(GENERIC_HTML, "https://example.senate.gov")
        testimony = [u for u in urls if "testimony-witness-a" in u]
        assert len(testimony) == 1

    def test_finds_statement_pdfs(self):
        urls = _extract_pdf_links(GENERIC_HTML, "https://example.senate.gov")
        statement = [u for u in urls if "statement-witness-b" in u]
        assert len(statement) == 1

    def test_excludes_non_pdf_links(self):
        urls = _extract_pdf_links(GENERIC_HTML, "https://example.senate.gov")
        html_links = [u for u in urls if u.endswith(".html")]
        assert len(html_links) == 0
        jpg_links = [u for u in urls if u.endswith(".jpg")]
        assert len(jpg_links) == 0

    def test_urls_are_absolute(self):
        urls = _extract_pdf_links(GENERIC_HTML, "https://example.senate.gov")
        for url in urls:
            assert url.startswith("https://"), f"URL not absolute: {url}"

    def test_falls_back_to_all_pdfs_when_no_testimony_signal(self):
        """When no links have testimony keywords, return all PDF links."""
        html = """
        <html><body>
            <a href="/data/report-2026.pdf">Annual Report</a>
            <a href="/data/appendix.pdf">Appendix</a>
            <a href="/news">News</a>
        </body></html>
        """
        urls = _extract_pdf_links(html, "https://example.gov")
        assert len(urls) == 2
        assert all(u.endswith(".pdf") for u in urls)


# ===========================================================================
#  Edge cases
# ===========================================================================


class TestNonPdfLinksExcluded:
    """Verify that non-PDF links are never returned by any extractor."""

    def test_html_links_excluded_drupal(self):
        html = """
        <html><body>
            <a href="/hearings/next-one.html">Next Hearing</a>
            <a href="/about">About</a>
        </body></html>
        """
        assert _extract_drupal_senate(html, "https://senate.gov") == []

    def test_image_links_excluded_wordpress(self):
        html = """
        <html><body>
        <div class="entry-content">
            <a href="/wp-content/uploads/2026/02/photo.jpg">Photo</a>
            <a href="/wp-content/uploads/2026/02/logo.png">Logo</a>
        </div>
        </body></html>
        """
        assert _extract_wordpress(html, "https://intelligence.senate.gov") == []

    def test_anchor_links_excluded_generic(self):
        html = """
        <html><body>
            <a href="#top">Back to top</a>
            <a href="javascript:void(0)">Click</a>
        </body></html>
        """
        assert _extract_pdf_links(html, "https://example.gov") == []


class TestRelativeUrlResolution:
    """Verify that relative URLs are resolved to absolute."""

    def test_relative_resolved_drupal(self):
        html = """
        <html><body>
            <a href="/download/testimony.pdf">Download Testimony</a>
        </body></html>
        """
        urls = _extract_drupal_senate(html, "https://finance.senate.gov/hearings/detail")
        assert len(urls) == 1
        assert urls[0] == "https://finance.senate.gov/download/testimony.pdf"

    def test_relative_resolved_wordpress(self):
        html = """
        <html><body>
        <div class="entry-content">
            <a href="/wp-content/uploads/2026/02/doc.pdf">Testimony Document</a>
        </div>
        </body></html>
        """
        urls = _extract_wordpress(html, "https://intelligence.senate.gov/hearing/xyz")
        assert len(urls) == 1
        assert urls[0] == "https://intelligence.senate.gov/wp-content/uploads/2026/02/doc.pdf"

    def test_relative_resolved_coldfusion(self):
        html = """
        <html><body>
            <a href="/public/?a=Files.Serve&File_id=XYZ">Testimony PDF</a>
        </body></html>
        """
        urls = _extract_coldfusion(html, "https://epw.senate.gov/hearings/detail")
        assert len(urls) == 1
        assert urls[0].startswith("https://epw.senate.gov/")

    def test_relative_resolved_evo(self):
        html = """
        <html><body>
            <a href="/sites/default/files/testimony.pdf">Testimony of Dr. Smith</a>
        </body></html>
        """
        urls = _extract_evo_framework(html, "https://judiciary.house.gov/hearing/123")
        assert len(urls) == 1
        assert urls[0] == "https://judiciary.house.gov/sites/default/files/testimony.pdf"

    def test_relative_resolved_generic(self):
        html = """
        <html><body>
            <a href="docs/written-testimony.pdf">Written Testimony</a>
        </body></html>
        """
        urls = _extract_pdf_links(html, "https://example.gov/hearing/detail/")
        assert len(urls) == 1
        assert urls[0] == "https://example.gov/hearing/detail/docs/written-testimony.pdf"


class TestDeduplication:
    """Verify that duplicate URLs within a single page are deduplicated."""

    def test_duplicate_urls_removed(self):
        html = """
        <html><body>
            <a href="/download/testimony.pdf">Testimony of Smith</a>
            <a href="/download/testimony.pdf">Download Smith Testimony</a>
            <a href="/download/different.pdf">Testimony of Jones</a>
        </body></html>
        """
        urls = _extract_drupal_senate(html, "https://finance.senate.gov")
        # testimony.pdf should appear only once
        count = sum(1 for u in urls if "testimony.pdf" in u)
        assert count == 1


class TestExcludeKeywords:
    """Verify that social media and media links are excluded."""

    def test_youtube_excluded(self):
        html = """
        <html><body>
            <a href="https://www.youtube.com/watch?v=abc">Watch Livestream</a>
            <a href="/download/testimony.pdf">Download Testimony</a>
        </body></html>
        """
        urls = _extract_drupal_senate(html, "https://senate.gov")
        yt = [u for u in urls if "youtube.com" in u]
        assert len(yt) == 0

    def test_social_media_excluded_generic(self):
        html = """
        <html><body>
            <a href="/share/facebook">Share on Facebook</a>
            <a href="/share/twitter">Share on Twitter</a>
            <a href="/files/testimony.pdf">Testimony PDF</a>
        </body></html>
        """
        urls = _extract_pdf_links(html, "https://example.gov")
        social = [u for u in urls if "facebook" in u or "twitter" in u]
        assert len(social) == 0


class TestEmptyAndMinimalInput:
    """Verify graceful handling of empty or minimal HTML."""

    def test_empty_html(self):
        assert _extract_drupal_senate("", "https://example.com") == []
        assert _extract_wordpress("", "https://example.com") == []
        assert _extract_coldfusion("", "https://example.com") == []
        assert _extract_evo_framework("", "https://example.com") == []
        assert _extract_aspnet_card("", "https://example.com") == []
        assert _extract_drupal_links("", "https://example.com") == []
        assert _extract_pdf_links("", "https://example.com") == []

    def test_no_links(self):
        html = "<html><body><p>No links here.</p></body></html>"
        assert _extract_drupal_senate(html, "https://example.com") == []
        assert _extract_pdf_links(html, "https://example.com") == []

    def test_no_pdf_links(self):
        html = """
        <html><body>
            <a href="/about">About Us</a>
            <a href="/contact">Contact</a>
        </body></html>
        """
        assert _extract_pdf_links(html, "https://example.com") == []
