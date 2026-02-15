"""Tests for extract module: download_pdf, extract_text_from_pdf, fetch_govinfo_transcript."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx

from extract import download_pdf, extract_text_from_pdf, fetch_govinfo_transcript


# ---------------------------------------------------------------------------
# download_pdf
# ---------------------------------------------------------------------------


class TestDownloadPdf:
    """Test PDF downloading with mocked HTTP calls."""

    @patch("extract.httpx.get")
    def test_success_writes_file(self, mock_get, tmp_path):
        resp = MagicMock()
        resp.status_code = 200
        resp.content = b"%PDF-1.4 fake content"
        mock_get.return_value = resp

        result = download_pdf("https://example.com/report.pdf", tmp_path)

        assert result is not None
        assert result.exists()
        assert result.name == "report.pdf"
        assert result.read_bytes() == b"%PDF-1.4 fake content"
        mock_get.assert_called_once_with(
            "https://example.com/report.pdf", timeout=60, follow_redirects=True
        )

    @patch("extract.httpx.get")
    def test_404_returns_none(self, mock_get, tmp_path):
        resp = MagicMock()
        resp.status_code = 404
        mock_get.return_value = resp

        result = download_pdf("https://example.com/missing.pdf", tmp_path)

        assert result is None

    @patch("extract.httpx.get")
    def test_uses_client_when_provided(self, mock_get, tmp_path):
        client = MagicMock(spec=httpx.Client)
        resp = MagicMock()
        resp.status_code = 200
        resp.content = b"%PDF-1.4 data"
        client.get.return_value = resp

        result = download_pdf("https://example.com/doc.pdf", tmp_path, client=client)

        assert result is not None
        client.get.assert_called_once_with("https://example.com/doc.pdf")
        mock_get.assert_not_called()

    @patch("extract.httpx.get")
    def test_derives_filename_from_url(self, mock_get, tmp_path):
        resp = MagicMock()
        resp.status_code = 200
        resp.content = b"data"
        mock_get.return_value = resp

        result = download_pdf("https://example.com/path/to/mydoc", tmp_path)

        assert result is not None
        assert result.name == "mydoc.pdf"

    @patch("extract.httpx.get")
    def test_explicit_filename(self, mock_get, tmp_path):
        resp = MagicMock()
        resp.status_code = 200
        resp.content = b"data"
        mock_get.return_value = resp

        result = download_pdf(
            "https://example.com/doc.pdf", tmp_path, filename="custom.pdf"
        )

        assert result is not None
        assert result.name == "custom.pdf"

    @patch("extract.httpx.get")
    def test_sanitizes_filename(self, mock_get, tmp_path):
        resp = MagicMock()
        resp.status_code = 200
        resp.content = b"data"
        mock_get.return_value = resp

        result = download_pdf(
            "https://example.com/doc.pdf", tmp_path, filename="bad name!@#$.pdf"
        )

        assert result is not None
        # Special chars replaced with underscores, only [a-zA-Z0-9._-] kept
        assert "!" not in result.name
        assert "@" not in result.name

    @patch("extract.httpx.get")
    def test_http_error_returns_none(self, mock_get, tmp_path):
        mock_get.side_effect = httpx.ConnectError("connection refused")

        result = download_pdf("https://example.com/doc.pdf", tmp_path)

        assert result is None


# ---------------------------------------------------------------------------
# extract_text_from_pdf
# ---------------------------------------------------------------------------


def _make_mock_doc(pages_text):
    """Build a mock pymupdf document that yields pages with given text strings."""
    pages = []
    for text in pages_text:
        page = MagicMock()
        page.get_text.return_value = text
        pages.append(page)
    doc = MagicMock()
    doc.__enter__ = MagicMock(return_value=doc)
    doc.__exit__ = MagicMock(return_value=False)
    doc.__iter__ = MagicMock(return_value=iter(pages))
    return doc


class TestExtractTextFromPdf:
    """Test PDF text extraction with pymupdf4llm and pymupdf fallback.

    Both pymupdf4llm and pymupdf are imported locally inside
    extract_text_from_pdf, so we inject fake modules into sys.modules
    before calling the function.
    """

    def test_pymupdf4llm_success(self, tmp_path):
        pdf_path = tmp_path / "test.pdf"
        pdf_path.write_bytes(b"fake")

        mock_pymupdf4llm = MagicMock()
        mock_pymupdf4llm.to_markdown.return_value = "# Heading\n\nSome text"

        with patch.dict(sys.modules, {"pymupdf4llm": mock_pymupdf4llm}):
            result = extract_text_from_pdf(pdf_path)

        assert result == "# Heading\n\nSome text"
        mock_pymupdf4llm.to_markdown.assert_called_once_with(str(pdf_path))

    def test_pymupdf4llm_runtime_error_falls_back_to_pymupdf(self, tmp_path):
        pdf_path = tmp_path / "test.pdf"
        pdf_path.write_bytes(b"fake")

        mock_pymupdf4llm = MagicMock()
        mock_pymupdf4llm.to_markdown.side_effect = RuntimeError("corrupt PDF")

        mock_pymupdf = MagicMock()
        mock_pymupdf.open.return_value = _make_mock_doc(["Page 1 text"])

        with patch.dict(sys.modules, {
            "pymupdf4llm": mock_pymupdf4llm,
            "pymupdf": mock_pymupdf,
        }):
            result = extract_text_from_pdf(pdf_path)

        assert result == "Page 1 text"
        mock_pymupdf4llm.to_markdown.assert_called_once()

    def test_pymupdf4llm_value_error_falls_back(self, tmp_path):
        pdf_path = tmp_path / "test.pdf"
        pdf_path.write_bytes(b"fake")

        mock_pymupdf4llm = MagicMock()
        mock_pymupdf4llm.to_markdown.side_effect = ValueError("bad value")

        mock_pymupdf = MagicMock()
        mock_pymupdf.open.return_value = _make_mock_doc(["Fallback text"])

        with patch.dict(sys.modules, {
            "pymupdf4llm": mock_pymupdf4llm,
            "pymupdf": mock_pymupdf,
        }):
            result = extract_text_from_pdf(pdf_path)

        assert result == "Fallback text"

    def test_fallback_joins_multiple_pages(self, tmp_path):
        pdf_path = tmp_path / "test.pdf"
        pdf_path.write_bytes(b"fake")

        mock_pymupdf4llm = MagicMock()
        mock_pymupdf4llm.to_markdown.side_effect = RuntimeError("fail")

        mock_pymupdf = MagicMock()
        mock_pymupdf.open.return_value = _make_mock_doc(["Page 1", "Page 2"])

        with patch.dict(sys.modules, {
            "pymupdf4llm": mock_pymupdf4llm,
            "pymupdf": mock_pymupdf,
        }):
            result = extract_text_from_pdf(pdf_path)

        assert result == "Page 1\n\nPage 2"


# ---------------------------------------------------------------------------
# fetch_govinfo_transcript
# ---------------------------------------------------------------------------


class TestFetchGovInfoTranscript:
    """Test GovInfo transcript fetching: htm first, then pdf fallback."""

    @patch("extract.config.get_govinfo_api_key", return_value="TEST_KEY")
    @patch("extract.httpx.get")
    def test_htm_success(self, mock_get, mock_api_key, tmp_path):
        """When the htm endpoint returns 200, parse HTML and write text."""
        resp = MagicMock()
        resp.status_code = 200
        resp.text = "<html><body><p>Transcript text here</p></body></html>"
        mock_get.return_value = resp

        result = fetch_govinfo_transcript("CHRG-119hhrg12345", tmp_path)

        assert result is not None
        assert result.name == "govinfo_transcript.txt"
        assert result.exists()
        content = result.read_text()
        assert "Transcript text here" in content
        # Should only call once (htm succeeded, no pdf needed)
        mock_get.assert_called_once_with(
            "https://api.govinfo.gov/packages/CHRG-119hhrg12345/htm",
            params={"api_key": "TEST_KEY"},
            timeout=120,
            follow_redirects=True,
        )

    @patch("extract.config.get_govinfo_api_key", return_value="TEST_KEY")
    @patch("extract.httpx.get")
    def test_htm_fails_falls_back_to_pdf(self, mock_get, mock_api_key, tmp_path):
        """When htm returns 404, tries pdf endpoint."""
        htm_resp = MagicMock()
        htm_resp.status_code = 404

        pdf_resp = MagicMock()
        pdf_resp.status_code = 200
        pdf_resp.content = b"%PDF-1.4 fake"

        mock_get.side_effect = [htm_resp, pdf_resp]

        with patch("extract.extract_text_from_pdf", return_value="Extracted PDF text"):
            result = fetch_govinfo_transcript("CHRG-119shrg99999", tmp_path)

        assert result is not None
        assert result.name == "govinfo_transcript.txt"
        content = result.read_text()
        assert content == "Extracted PDF text"
        assert mock_get.call_count == 2

    @patch("extract.config.get_govinfo_api_key", return_value="TEST_KEY")
    @patch("extract.httpx.get")
    def test_both_fail_returns_none(self, mock_get, mock_api_key, tmp_path):
        """When both htm and pdf return non-200, return None."""
        resp = MagicMock()
        resp.status_code = 404
        mock_get.return_value = resp

        result = fetch_govinfo_transcript("CHRG-119hhrg00000", tmp_path)

        assert result is None
        assert mock_get.call_count == 2

    @patch("extract.config.get_govinfo_api_key", return_value="TEST_KEY")
    def test_uses_client_when_provided(self, mock_api_key, tmp_path):
        """When a client is provided, use client.get instead of httpx.get."""
        client = MagicMock(spec=httpx.Client)
        resp = MagicMock()
        resp.status_code = 200
        resp.text = "<html><body>Transcript</body></html>"
        client.get.return_value = resp

        result = fetch_govinfo_transcript("CHRG-119hhrg12345", tmp_path, client=client)

        assert result is not None
        client.get.assert_called_once_with(
            "https://api.govinfo.gov/packages/CHRG-119hhrg12345/htm",
            params={"api_key": "TEST_KEY"},
        )

    @patch("extract.config.get_govinfo_api_key", return_value="TEST_KEY")
    @patch("extract.httpx.get")
    def test_http_error_continues_to_next_format(self, mock_get, mock_api_key, tmp_path):
        """An httpx.HTTPError on htm should continue to try pdf."""
        mock_get.side_effect = [
            httpx.ConnectError("connection refused"),
            MagicMock(status_code=404),
        ]

        result = fetch_govinfo_transcript("CHRG-119hhrg12345", tmp_path)

        assert result is None
        assert mock_get.call_count == 2
