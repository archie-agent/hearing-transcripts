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
import shutil
import threading
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

# Load .env before importing config
load_dotenv()

import config
from discover import Hearing, discover_all
from extract import fetch_govinfo_transcript, process_testimony_pdfs
from alerts import check_and_alert
from state import State
from transcribe import process_hearing_audio

log = logging.getLogger(__name__)


def process_hearing(hearing: Hearing, state: State, run_dir: Path) -> dict:
    """Process a single hearing: captions, cleanup, PDFs, GovInfo.

    Writes all artifacts to run_dir/hearings/{hearing.id}/.
    """
    hearing_dir = run_dir / "hearings" / hearing.id
    hearing_dir.mkdir(parents=True, exist_ok=True)

    # Record discovery in state DB
    state.record_hearing(
        hearing.id, hearing.committee_key, hearing.date,
        hearing.title, hearing.slug, hearing.sources,
    )
    state.mark_step(hearing.id, "discover", "done")

    cost = {"llm_cleanup_usd": 0.0, "whisper_usd": 0.0, "total_usd": 0.0}

    result = {
        "id": hearing.id,
        "committee": hearing.committee_name,
        "committee_key": hearing.committee_key,
        "date": hearing.date,
        "title": hearing.title,
        "slug": hearing.slug,
        "sources": hearing.sources,
        "outputs": {},
        "cost": cost,
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
                cost["llm_cleanup_usd"] += audio_result.get("cleanup_cost_usd", 0)
                cost["whisper_usd"] += audio_result.get("whisper_cost_usd", 0)
                state.mark_step(hearing.id, "captions", "done")
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
        state.mark_step(hearing.id, "captions", "done")
        state.mark_step(hearing.id, "cleanup", "done")

    # 1.5. C-SPAN broadcast captions
    cspan_url = hearing.sources.get("cspan_url")
    if cspan_url:
        if not state.is_step_done(hearing.id, "cspan"):
            state.mark_step(hearing.id, "cspan", "running")
            try:
                import cspan
                witnesses = hearing.sources.get("witnesses")
                transcript_path = cspan.fetch_cspan_transcript(
                    cspan_url, hearing_dir, witnesses=witnesses,
                )
                if transcript_path:
                    result["outputs"]["cspan_transcript"] = str(transcript_path)
                    state.mark_step(hearing.id, "cspan_fetched", "done")
                state.mark_step(hearing.id, "cspan", "done")
            except ImportError:
                log.debug("cspan module not available, skipping")
                state.mark_step(hearing.id, "cspan", "done")
            except Exception as e:
                state.mark_step(hearing.id, "cspan", "failed", error=str(e))
                log.error("C-SPAN caption fetch failed for %s: %s", hearing.id, e)
        else:
            log.info("C-SPAN captions already processed for %s", hearing.id)
    # (If no cspan_url, leave cspan step unmarked so it can be retried
    # if a URL is discovered on a future run.)

    # 2. Testimony PDFs
    pdf_urls = hearing.sources.get("testimony_pdf_urls", [])
    if pdf_urls:
        if not state.is_step_done(hearing.id, "testimony"):
            state.mark_step(hearing.id, "testimony", "running")
            try:
                testimony_dir = hearing_dir / "testimony"
                testimony_dir.mkdir(exist_ok=True)
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

    # Compute total cost
    cost["total_usd"] = cost["llm_cleanup_usd"] + cost["whisper_usd"]

    # Mark hearing as fully processed
    state.mark_processed(hearing.id)

    # Write metadata to run dir
    meta = {k: v for k, v in result.items()}
    meta["processed_at"] = datetime.now(timezone.utc).isoformat()
    meta_path = hearing_dir / "meta.json"
    meta_path.write_text(json.dumps(meta, indent=2))

    # Publish to transcripts/ canonical archive
    _publish_to_transcripts(hearing, hearing_dir, result)

    return result


def _publish_to_transcripts(hearing: Hearing, run_hearing_dir: Path, result: dict) -> None:
    """Copy final artifacts to transcripts/{committee_key}/{date}_{id}/."""
    transcript_dir = config.TRANSCRIPTS_DIR / hearing.committee_key / f"{hearing.date}_{hearing.id}"
    transcript_dir.mkdir(parents=True, exist_ok=True)

    # Determine best transcript by priority:
    #   1. GovInfo official transcript (authoritative, months delayed)
    #   2. C-SPAN broadcast captions (professional stenographers, immediate)
    #   3. YouTube + LLM diarized (ASR quality, LLM-improved)
    #   4. Raw YouTube captions (worst)
    best_transcript = None

    # Priority 1: GovInfo official transcript
    govinfo = result.get("outputs", {}).get("govinfo_transcript")
    if govinfo:
        best_transcript = Path(govinfo)

    # Priority 2: C-SPAN broadcast captions
    if best_transcript is None:
        cspan_path = result.get("outputs", {}).get("cspan_transcript")
        if cspan_path:
            best_transcript = Path(cspan_path)

    # Priority 3-4: YouTube cleaned or raw captions
    if best_transcript is None:
        audio = result.get("outputs", {}).get("audio", {})
        if isinstance(audio, dict):
            if audio.get("cleaned_transcript"):
                best_transcript = Path(audio["cleaned_transcript"])
            elif audio.get("captions"):
                best_transcript = Path(audio["captions"])

    # Copy best transcript
    if best_transcript and best_transcript.exists():
        shutil.copy2(best_transcript, transcript_dir / "transcript.txt")

    # Copy testimony files
    src_testimony = run_hearing_dir / "testimony"
    if src_testimony.is_dir():
        dst_testimony = transcript_dir / "testimony"
        if dst_testimony.exists():
            shutil.rmtree(dst_testimony)
        shutil.copytree(src_testimony, dst_testimony)

    # Write meta.json (subset — no raw paths, just metadata + cost)
    meta = {
        "id": hearing.id,
        "committee": hearing.committee_name,
        "committee_key": hearing.committee_key,
        "date": hearing.date,
        "title": hearing.title,
        "sources": hearing.sources,
        "cost": result.get("cost", {}),
        "published_at": datetime.now(timezone.utc).isoformat(),
    }
    # Include witnesses from congress.gov if available
    witnesses = hearing.sources.get("witnesses")
    if witnesses:
        meta["witnesses"] = witnesses
    (transcript_dir / "meta.json").write_text(json.dumps(meta, indent=2))

    log.info("Published to %s", transcript_dir)


def _update_index(results: list[dict]) -> None:
    """Update transcripts/index.json global manifest."""
    config.TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
    index_path = config.TRANSCRIPTS_DIR / "index.json"

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
            entry = {
                "id": r["id"],
                "committee": r["committee"],
                "committee_key": r["committee_key"],
                "date": r["date"],
                "title": r["title"],
                "path": f"{r['committee_key']}/{r['date']}_{r['id']}",
            }
            existing["hearings"].append(entry)

    existing["last_updated"] = datetime.now(timezone.utc).isoformat()
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

    # Generate run ID and create run directory
    run_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%S")
    run_started = datetime.now(timezone.utc).isoformat()
    run_dir = config.RUNS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

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
        "Run %s: monitoring %d committees, looking back %d day(s), max cost $%.2f",
        run_id, len(active), args.days, max_cost,
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
        new_hearings = []
        for h in hearings:
            if not state.is_processed(h.id):
                new_hearings.append(h)
            elif h.sources.get("cspan_url") and not state.is_step_done(h.id, "cspan_fetched"):
                # Re-process hearings that gained a C-SPAN URL since last run
                new_hearings.append(h)
                log.debug("Re-processing %s: new C-SPAN URL", h.id)

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
    n_total = len(new_hearings)

    def _log_result(i: int, r: dict) -> None:
        """Log a one-line progress summary for a completed hearing."""
        audio = r.get("outputs", {}).get("audio", {})
        has_cap = bool(audio.get("captions")) if isinstance(audio, dict) else False
        has_clean = bool(audio.get("cleaned_transcript")) if isinstance(audio, dict) else False
        n_test = len(r.get("outputs", {}).get("testimony", []))
        has_gov = bool(r.get("outputs", {}).get("govinfo_transcript"))
        has_cspan = bool(r.get("outputs", {}).get("cspan_transcript"))
        cost_usd = r.get("cost", {}).get("total_usd", 0)
        log.info(
            "[%d/%d] %s | cap=%s clean=%s cspan=%s testy=%d gov=%s $%.4f",
            i, n_total, r["title"][:50],
            "Y" if has_cap else "-",
            "Y" if has_clean else "-",
            "Y" if has_cspan else "-",
            n_test,
            "Y" if has_gov else "-",
            cost_usd,
        )

    if args.workers <= 1:
        # Sequential processing
        for i, h in enumerate(new_hearings, 1):
            if total_cost >= max_cost:
                log.warning("Cost limit reached ($%.2f >= $%.2f), stopping", total_cost, max_cost)
                break
            log.info("--- [%d/%d] Processing: %s ---", i, n_total, h.title[:60])
            try:
                result = process_hearing(h, state, run_dir)
                results.append(result)
                total_cost += result.get("cost", {}).get("total_usd", 0)
                _log_result(i, result)
            except Exception as e:
                errors.append({"hearing": h.title, "error": str(e)})
                log.error("[%d/%d] FAILED: %s: %s", i, n_total, h.title[:60], e, exc_info=True)
    else:
        # Parallel processing with progress counter
        counter_lock = threading.Lock()
        completed_count = 0

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(process_hearing, h, state, run_dir): h for h in new_hearings}
            for future in as_completed(futures):
                h = futures[future]
                with counter_lock:
                    completed_count += 1
                    i = completed_count
                try:
                    result = future.result()
                    results.append(result)
                    total_cost += result.get("cost", {}).get("total_usd", 0)
                    _log_result(i, result)
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
                    log.error("[%d/%d] FAILED: %s: %s", i, n_total, h.title[:60], e, exc_info=True)

    # Update transcripts/index.json
    if results:
        _update_index(results)

    # Aggregate costs
    total_llm = sum(r.get("cost", {}).get("llm_cleanup_usd", 0) for r in results)
    total_whisper = sum(r.get("cost", {}).get("whisper_usd", 0) for r in results)
    total_all = total_llm + total_whisper

    # Write run_meta.json
    run_completed = datetime.now(timezone.utc).isoformat()
    run_meta = {
        "run_id": run_id,
        "started_at": run_started,
        "completed_at": run_completed,
        "args": vars(args),
        "hearings_discovered": len(hearings),
        "hearings_processed": len(results),
        "hearings_failed": len(errors),
        "cost": {
            "llm_cleanup_usd": total_llm,
            "whisper_usd": total_whisper,
            "total_usd": total_all,
        },
        "hearings": [
            {"id": r["id"], "title": r["title"], "committee_key": r["committee_key"],
             "date": r["date"], "cost": r.get("cost", {})}
            for r in results
        ],
        "errors": errors,
    }
    (run_dir / "run_meta.json").write_text(json.dumps(run_meta, indent=2))

    # Persist cost to state DB
    state.record_run(
        run_id=run_id,
        started_at=run_started,
        completed_at=run_completed,
        hearings_processed=len(results),
        llm_cleanup_usd=total_llm,
        whisper_usd=total_whisper,
        total_usd=total_all,
    )

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
    log.info("=== Run %s Complete ===", run_id)
    log.info("Processed %d/%d hearings", len(results), len(new_hearings))
    log.info("Cost: LLM cleanup $%.4f + Whisper $%.4f = $%.4f total", total_llm, total_whisper, total_all)
    for r in results:
        outputs = r.get("outputs", {})
        audio = outputs.get("audio", {})
        has_captions = bool(audio.get("captions")) if isinstance(audio, dict) else False
        has_cleaned = bool(audio.get("cleaned_transcript")) if isinstance(audio, dict) else False
        has_cspan = bool(outputs.get("cspan_transcript"))
        n_testimony = len(outputs.get("testimony", []))
        has_govinfo = bool(outputs.get("govinfo_transcript"))
        log.info(
            "  %s | captions=%s cleaned=%s cspan=%s testimony=%d govinfo=%s | $%.4f",
            r["title"][:50],
            "yes" if has_captions else "no",
            "yes" if has_cleaned else "no",
            "yes" if has_cspan else "no",
            n_testimony,
            "yes" if has_govinfo else "no",
            r.get("cost", {}).get("total_usd", 0),
        )
    if errors:
        log.warning("%d errors:", len(errors))
        for e in errors:
            log.warning("  %s: %s", e["hearing"][:50], e["error"])

    # Cumulative cost report
    cumulative = state.get_total_cost()
    log.info(
        "Cumulative: %d runs, %d hearings, $%.4f total",
        cumulative["runs"], cumulative["hearings"], cumulative["total_usd"],
    )

    # Alert on persistently failing scrapers
    check_and_alert(state)


if __name__ == "__main__":
    main()
