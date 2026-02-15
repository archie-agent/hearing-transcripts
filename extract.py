"""Download and extract text from hearing-related PDFs."""

from __future__ import annotations

import logging
import os
import re
import tempfile
from pathlib import Path

import httpx

import config

log = logging.getLogger(__name__)


def download_pdf(url: str, output_dir: Path, filename: str | None = None) -> Path | None:
    """Download a PDF from a URL."""
    if not filename:
        # Derive filename from URL
        filename = url.split("/")[-1]
        if not filename.endswith(".pdf"):
            filename += ".pdf"
    filename = re.sub(r"[^a-zA-Z0-9._-]", "_", filename)

    pdf_path = output_dir / filename
    try:
        resp = httpx.get(url, timeout=60, follow_redirects=True)
        if resp.status_code != 200:
            log.warning("PDF download failed (%s): %s", resp.status_code, url)
            return None
        tmp_fd, tmp_path = tempfile.mkstemp(dir=pdf_path.parent, suffix='.tmp')
        try:
            with os.fdopen(tmp_fd, 'wb') as f:
                f.write(resp.content)
            os.replace(tmp_path, pdf_path)
        except Exception:
            os.unlink(tmp_path)
            raise
        log.info("Downloaded PDF: %s (%.1f KB)", pdf_path.name, len(resp.content) / 1024)
        return pdf_path
    except (httpx.HTTPError, OSError) as e:
        log.warning("PDF download error: %s", e)
        return None


def extract_text_from_pdf(pdf_path: Path) -> str:
    """Extract text from a PDF using pymupdf4llm. Handles multi-column GPO layouts.

    Returns the extracted text (may be empty for genuinely blank PDFs).
    Raises on library/unexpected errors so the caller can distinguish
    "valid PDF, no text" from "extraction crashed".
    """
    try:
        import pymupdf4llm
        text = pymupdf4llm.to_markdown(str(pdf_path))
        return text
    except ImportError:
        log.warning("pymupdf4llm not installed, falling back to pymupdf")
    except (RuntimeError, ValueError, TypeError) as e:
        log.warning("pymupdf4llm extraction failed: %s, falling back to pymupdf", e)

    # Fallback to basic pymupdf
    import pymupdf
    with pymupdf.open(str(pdf_path)) as doc:
        pages = [page.get_text() for page in doc]
    return "\n\n".join(pages)


def process_testimony_pdfs(pdf_urls: list[str], output_dir: Path) -> list[dict]:
    """Download and extract text from a list of testimony PDF URLs."""
    testimony_dir = output_dir / "testimony"
    testimony_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for url in pdf_urls:
        pdf_path = download_pdf(url, testimony_dir)
        if not pdf_path:
            continue

        text = extract_text_from_pdf(pdf_path)
        if not text.strip():
            log.warning("Empty text from %s", pdf_path.name)
            continue

        # Save extracted text (atomic: temp file + rename)
        txt_name = pdf_path.stem + ".txt"
        txt_path = testimony_dir / txt_name
        tmp_fd, tmp_path = tempfile.mkstemp(dir=testimony_dir, suffix='.tmp')
        try:
            with os.fdopen(tmp_fd, 'w') as f:
                f.write(text)
            os.replace(tmp_path, txt_path)
        except Exception:
            os.unlink(tmp_path)
            raise

        # Clean up PDF to save disk
        pdf_path.unlink(missing_ok=True)

        results.append({
            "source_url": url,
            "text_file": str(txt_path),
            "chars": len(text),
        })
        log.info("Extracted testimony: %s (%d chars)", txt_name, len(text))

    return results


def fetch_govinfo_transcript(package_id: str, output_dir: Path) -> Path | None:
    """Download the official GPO transcript text from GovInfo API.

    Tries direct package endpoints (htm, then pdf) since the summary download
    links often only include premis/zip/mods but not the actual content links.
    """
    api_key = config.get_govinfo_api_key()

    # Try HTML first (cleanest), then PDF
    for ext in ("htm", "pdf"):
        url = f"https://api.govinfo.gov/packages/{package_id}/{ext}"
        try:
            resp = httpx.get(url, params={"api_key": api_key}, timeout=120, follow_redirects=True)
            if resp.status_code != 200:
                continue

            if ext == "htm":
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(resp.text, "lxml")
                text = soup.get_text(separator="\n")
                txt_path = output_dir / "govinfo_transcript.txt"
                tmp_fd, tmp_path = tempfile.mkstemp(dir=txt_path.parent, suffix='.tmp')
                try:
                    with os.fdopen(tmp_fd, 'w') as f:
                        f.write(text)
                    os.replace(tmp_path, txt_path)
                except Exception:
                    os.unlink(tmp_path)
                    raise
                log.info("GovInfo transcript (HTML): %d chars", len(text))
                return txt_path
            else:
                # PDF — save, extract, clean up
                pdf_path = output_dir / "govinfo_transcript.pdf"
                tmp_fd, tmp_path = tempfile.mkstemp(dir=pdf_path.parent, suffix='.tmp')
                try:
                    with os.fdopen(tmp_fd, 'wb') as f:
                        f.write(resp.content)
                    os.replace(tmp_path, pdf_path)
                except Exception:
                    os.unlink(tmp_path)
                    raise
                text = extract_text_from_pdf(pdf_path)
                txt_path = output_dir / "govinfo_transcript.txt"
                tmp_fd2, tmp_path2 = tempfile.mkstemp(dir=txt_path.parent, suffix='.tmp')
                try:
                    with os.fdopen(tmp_fd2, 'w') as f:
                        f.write(text)
                    os.replace(tmp_path2, txt_path)
                except Exception:
                    os.unlink(tmp_path2)
                    raise
                pdf_path.unlink(missing_ok=True)
                log.info("GovInfo transcript (PDF): %d chars", len(text))
                return txt_path

        except (httpx.HTTPError, OSError) as e:
            log.warning("GovInfo download failed for %s (%s): %s", package_id, ext, e)
            continue

    return None
