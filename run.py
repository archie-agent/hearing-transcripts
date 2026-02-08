#!/usr/bin/env python3
"""Daily hearing transcript pipeline.

Usage:
    python run.py                  # full run (discover + download + transcribe)
    python run.py --discover-only  # just list what's new
    python run.py --days 3         # look back 3 days
    python run.py --tier 1         # only tier-1 (core economics) committees
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Load .env before importing config
load_dotenv()

import config
from discover import Hearing, discover_all
from transcribe import process_hearing_audio
from extract import process_testimony_pdfs, fetch_govinfo_transcript

log = logging.getLogger(__name__)


def process_hearing(hearing: Hearing) -> dict:
    """Process a single hearing: download audio, transcribe, extract PDFs."""
    date_dir = config.OUTPUT_DIR / hearing.date
    hearing_dir = date_dir / hearing.slug
    hearing_dir.mkdir(parents=True, exist_ok=True)

    result = {
        "committee": hearing.committee_name,
        "committee_key": hearing.committee_key,
        "date": hearing.date,
        "title": hearing.title,
        "sources": hearing.sources,
        "outputs": {},
    }

    # 1. Audio transcription (YouTube source)
    youtube_url = hearing.sources.get("youtube_url")
    if youtube_url:
        log.info("Processing audio: %s", youtube_url)
        audio_result = process_hearing_audio(youtube_url, hearing_dir)
        result["outputs"]["audio"] = audio_result

    # 2. Testimony PDFs (docs.house.gov or committee sites)
    pdf_urls = hearing.sources.get("testimony_pdf_urls", [])
    if pdf_urls:
        log.info("Processing %d testimony PDFs", len(pdf_urls))
        pdf_results = process_testimony_pdfs(pdf_urls, hearing_dir)
        result["outputs"]["testimony"] = pdf_results

    # 3. GovInfo official transcript
    govinfo_id = hearing.sources.get("govinfo_package_id")
    if govinfo_id:
        log.info("Fetching GovInfo transcript: %s", govinfo_id)
        gpo_path = fetch_govinfo_transcript(govinfo_id, hearing_dir)
        if gpo_path:
            result["outputs"]["govinfo_transcript"] = str(gpo_path)

    # Write metadata
    meta_path = hearing_dir / "meta.json"
    meta_path.write_text(json.dumps(result, indent=2))

    return result


def main():
    parser = argparse.ArgumentParser(description="Congressional hearing transcript pipeline")
    parser.add_argument("--days", type=int, default=1, help="Days to look back (default: 1)")
    parser.add_argument("--discover-only", action="store_true", help="Just discover, don't process")
    parser.add_argument("--tier", type=int, default=None, help="Only process this tier (1=core, 2=adjacent)")
    parser.add_argument("--committee", type=str, default=None, help="Process only this committee key")
    parser.add_argument("--verbose", "-v", action="store_true", help="Debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # Filter committees if requested
    if args.committee:
        if args.committee not in config.COMMITTEES:
            log.error("Unknown committee: %s", args.committee)
            log.info("Available: %s", ", ".join(sorted(config.COMMITTEES.keys())))
            sys.exit(1)
        active = {args.committee: config.COMMITTEES[args.committee]}
    elif args.tier:
        active = {k: v for k, v in config.COMMITTEES.items() if v["tier"] == args.tier}
    else:
        active = config.COMMITTEES

    log.info("Monitoring %d committees, looking back %d day(s)", len(active), args.days)

    # Temporarily override config.COMMITTEES for discovery
    original = config.COMMITTEES
    config.COMMITTEES = active

    # Discover
    hearings = discover_all(days=args.days)

    config.COMMITTEES = original

    if not hearings:
        log.info("No new hearings found.")
        return

    log.info("Found %d hearings:", len(hearings))
    for h in hearings:
        log.info("  [%s] %s: %s", h.date, h.committee_name, h.title[:80])

    if args.discover_only:
        # Print JSON summary to stdout
        print(json.dumps([{
            "committee": h.committee_name,
            "date": h.date,
            "title": h.title,
            "sources": h.sources,
        } for h in hearings], indent=2))
        return

    # Process each hearing
    results = []
    for i, h in enumerate(hearings):
        log.info("--- Processing %d/%d: %s ---", i + 1, len(hearings), h.title[:60])
        try:
            result = process_hearing(h)
            results.append(result)
        except Exception as e:
            log.error("Failed to process hearing: %s", e, exc_info=True)

    # Summary
    log.info("=== Done ===")
    log.info("Processed %d/%d hearings", len(results), len(hearings))
    for r in results:
        outputs = r.get("outputs", {})
        has_transcript = bool(outputs.get("audio", {}).get("transcript"))
        has_captions = bool(outputs.get("audio", {}).get("captions"))
        n_testimony = len(outputs.get("testimony", []))
        has_govinfo = bool(outputs.get("govinfo_transcript"))
        log.info(
            "  %s | transcript=%s captions=%s testimony=%d govinfo=%s",
            r["title"][:50],
            "yes" if has_transcript else "no",
            "yes" if has_captions else "no",
            n_testimony,
            "yes" if has_govinfo else "no",
        )


if __name__ == "__main__":
    main()
