#!/usr/bin/env python3
"""Daily hearing transcript pipeline.

Usage:
    python run.py                  # full run (discover + download + transcribe)
    python run.py --discover-only  # just list what's new
    python run.py --days 3         # look back 3 days
    python run.py --tier 1         # only tier-1 (core economics) committees
    python run.py --max-cost 2.0   # stop after $2 in LLM cleanup costs
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Load .env before importing config
load_dotenv()

import config
from discover import Hearing, discover_all
from extract import fetch_govinfo_transcript, process_testimony_pdfs
from state import State
from transcribe import process_hearing_audio

log = logging.getLogger(__name__)


def process_hearing(hearing: Hearing, state: State) -> dict:
    """Process a single hearing: captions, cleanup, PDFs, GovInfo."""
    hearing_dir = config.OUTPUT_DIR / hearing.date / hearing.slug
    hearing_dir.mkdir(parents=True, exist_ok=True)

    # Record discovery in state DB
    state.record_hearing(
        hearing.id, hearing.committee_key, hearing.date,
        hearing.title, hearing.slug, hearing.sources,
    )
    state.mark_step(hearing.id, "discover", "done")

    result = {
        "id": hearing.id,
        "committee": hearing.committee_name,
        "committee_key": hearing.committee_key,
        "date": hearing.date,
        "title": hearing.title,
        "slug": hearing.slug,
        "sources": hearing.sources,
        "outputs": {},
        "cost_usd": 0.0,
    }

    # 1. YouTube captions + LLM cleanup
    youtube_url = hearing.sources.get("youtube_url")
    if youtube_url:
        if not state.is_step_done(hearing.id, "captions"):
            state.mark_step(hearing.id, "captions", "running")
            try:
                audio_result = process_hearing_audio(
                    youtube_url, hearing_dir,
                    hearing_title=hearing.title,
                    committee_name=hearing.committee_name,
                )
                result["outputs"]["audio"] = audio_result
                result["cost_usd"] += audio_result.get("cleanup_cost_usd", 0)
                state.mark_step(hearing.id, "captions", "done")
                # Cleanup step tracks the LLM diarization pass
                if audio_result.get("cleaned_transcript"):
                    state.mark_step(hearing.id, "cleanup", "done")
                else:
                    state.mark_step(hearing.id, "cleanup", "done")
            except Exception as e:
                state.mark_step(hearing.id, "captions", "failed", error=str(e))
                log.error("Caption processing failed for %s: %s", hearing.id, e)
        else:
            log.info("Captions already processed for %s", hearing.id)
    else:
        # No YouTube source — mark captions/cleanup as done (nothing to do)
        state.mark_step(hearing.id, "captions", "done")
        state.mark_step(hearing.id, "cleanup", "done")

    # 2. Testimony PDFs
    pdf_urls = hearing.sources.get("testimony_pdf_urls", [])
    if pdf_urls:
        if not state.is_step_done(hearing.id, "testimony"):
            state.mark_step(hearing.id, "testimony", "running")
            try:
                pdf_results = process_testimony_pdfs(pdf_urls, hearing_dir)
                result["outputs"]["testimony"] = pdf_results
                state.mark_step(hearing.id, "testimony", "done")
            except Exception as e:
                state.mark_step(hearing.id, "testimony", "failed", error=str(e))
                log.error("Testimony extraction failed for %s: %s", hearing.id, e)
    else:
        state.mark_step(hearing.id, "testimony", "done")

    # 3. GovInfo official transcript
    govinfo_id = hearing.sources.get("govinfo_package_id")
    if govinfo_id:
        if not state.is_step_done(hearing.id, "govinfo"):
            state.mark_step(hearing.id, "govinfo", "running")
            try:
                gpo_path = fetch_govinfo_transcript(govinfo_id, hearing_dir)
                if gpo_path:
                    result["outputs"]["govinfo_transcript"] = str(gpo_path)
                state.mark_step(hearing.id, "govinfo", "done")
            except Exception as e:
                state.mark_step(hearing.id, "govinfo", "failed", error=str(e))
                log.error("GovInfo fetch failed for %s: %s", hearing.id, e)
    else:
        state.mark_step(hearing.id, "govinfo", "done")

    # Mark hearing as fully processed
    state.mark_processed(hearing.id)

    # Write metadata
    meta = {k: v for k, v in result.items()}
    meta["processed_at"] = datetime.utcnow().isoformat()
    meta_path = hearing_dir / "meta.json"
    meta_path.write_text(json.dumps(meta, indent=2))

    return result


def _update_index(results: list[dict]) -> None:
    """Update output/index.json with new hearing results."""
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    index_path = config.OUTPUT_DIR / "index.json"

    if index_path.exists():
        try:
            existing = json.loads(index_path.read_text())
        except (json.JSONDecodeError, OSError):
            existing = {"hearings": []}
    else:
        existing = {"hearings": []}

    existing_ids = {h["id"] for h in existing["hearings"] if "id" in h}
    for r in results:
        if r["id"] not in existing_ids:
            existing["hearings"].append(r)

    existing["last_updated"] = datetime.utcnow().isoformat()
    index_path.write_text(json.dumps(existing, indent=2))
    log.info("Index updated: %s (%d hearings)", index_path, len(existing["hearings"]))


def main():
    parser = argparse.ArgumentParser(description="Congressional hearing transcript pipeline")
    parser.add_argument("--days", type=int, default=1, help="Days to look back (default: 1)")
    parser.add_argument("--discover-only", action="store_true", help="Just discover, don't process")
    parser.add_argument("--tier", type=int, default=None, help="Max tier to include (1=core, 2=adjacent)")
    parser.add_argument("--committee", type=str, default=None, help="Process only this committee key")
    parser.add_argument("--max-cost", type=float, default=None, help="Max LLM cost per run in USD")
    parser.add_argument("--workers", type=int, default=3, help="Parallel hearing processing workers")
    parser.add_argument("--reprocess", action="store_true", help="Re-process already processed hearings")
    parser.add_argument("--verbose", "-v", action="store_true", help="Debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # Filter committees
    if args.committee:
        if args.committee not in config.COMMITTEES:
            log.error("Unknown committee: %s", args.committee)
            log.info("Available: %s", ", ".join(sorted(config.COMMITTEES.keys())))
            sys.exit(1)
        active = {args.committee: config.COMMITTEES[args.committee]}
    elif args.tier:
        active = config.get_committees(max_tier=args.tier)
    else:
        active = config.get_committees()  # default: tier <= 2

    max_cost = args.max_cost or config.MAX_COST_PER_RUN
    log.info(
        "Monitoring %d committees, looking back %d day(s), max cost $%.2f",
        len(active), args.days, max_cost,
    )

    # Discover — pass committees dict directly, no global mutation
    hearings = discover_all(days=args.days, committees=active)

    if not hearings:
        log.info("No new hearings found.")
        return

    # Filter out already-processed hearings (unless --reprocess)
    state = State()
    if args.reprocess:
        new_hearings = hearings
    else:
        new_hearings = [h for h in hearings if not state.is_processed(h.id)]

    log.info("Found %d hearings (%d new):", len(hearings), len(new_hearings))
    for h in hearings:
        marker = " " if h in new_hearings else "*"
        log.info("  %s [%s] %s: %s", marker, h.date, h.committee_name, h.title[:80])

    if args.discover_only:
        print(json.dumps([{
            "id": h.id,
            "committee": h.committee_name,
            "committee_key": h.committee_key,
            "date": h.date,
            "title": h.title,
            "sources": h.sources,
        } for h in hearings], indent=2))
        return

    if not new_hearings:
        log.info("All hearings already processed.")
        return

    # Process hearings
    results: list[dict] = []
    errors: list[dict] = []
    total_cost = 0.0

    if args.workers <= 1:
        # Sequential processing
        for i, h in enumerate(new_hearings):
            if total_cost >= max_cost:
                log.warning("Cost limit reached ($%.2f >= $%.2f), stopping", total_cost, max_cost)
                break
            log.info("--- Processing %d/%d: %s ---", i + 1, len(new_hearings), h.title[:60])
            try:
                result = process_hearing(h, state)
                results.append(result)
                total_cost += result.get("cost_usd", 0)
            except Exception as e:
                errors.append({"hearing": h.title, "error": str(e)})
                log.error("Failed: %s: %s", h.title[:60], e, exc_info=True)
    else:
        # Parallel processing
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(process_hearing, h, state): h for h in new_hearings}
            for future in as_completed(futures):
                h = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    total_cost += result.get("cost_usd", 0)
                    if total_cost >= max_cost:
                        log.warning(
                            "Cost limit reached ($%.2f >= $%.2f), cancelling remaining",
                            total_cost, max_cost,
                        )
                        for f in futures:
                            f.cancel()
                        break
                except Exception as e:
                    errors.append({"hearing": h.title, "error": str(e)})
                    log.error("Failed: %s: %s", h.title[:60], e, exc_info=True)

    # Update index
    if results:
        _update_index(results)

    # Check scraper health
    failing = state.get_failing_scrapers(threshold=3)
    if failing:
        log.warning("Failing scrapers (3+ consecutive failures):")
        for f in failing:
            log.warning(
                "  %s/%s: %d failures (last success: %s)",
                f["committee_key"], f["source_type"],
                f["consecutive_failures"], f.get("last_success", "never"),
            )

    # Summary
    log.info("=== Done ===")
    log.info("Processed %d/%d hearings, $%.4f total LLM cost", len(results), len(new_hearings), total_cost)
    for r in results:
        outputs = r.get("outputs", {})
        audio = outputs.get("audio", {})
        has_captions = bool(audio.get("captions")) if isinstance(audio, dict) else False
        has_cleaned = bool(audio.get("cleaned_transcript")) if isinstance(audio, dict) else False
        n_testimony = len(outputs.get("testimony", []))
        has_govinfo = bool(outputs.get("govinfo_transcript"))
        log.info(
            "  %s | captions=%s cleaned=%s testimony=%d govinfo=%s",
            r["title"][:50],
            "yes" if has_captions else "no",
            "yes" if has_cleaned else "no",
            n_testimony,
            "yes" if has_govinfo else "no",
        )
    if errors:
        log.warning("%d errors:", len(errors))
        for e in errors:
            log.warning("  %s: %s", e["hearing"][:50], e["error"])


if __name__ == "__main__":
    main()
