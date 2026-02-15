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
import os
import shutil
import subprocess
import tempfile
import threading
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv

# Load .env before importing config
load_dotenv()

import config
from discover import Hearing, discover_all
from utils import title_similarity
from extract import fetch_govinfo_transcript, process_testimony_pdfs
from alerts import check_and_alert
from state import State
from cleanup import cleanup_transcript
from transcribe import process_hearing_audio

log = logging.getLogger(__name__)


def _reconcile_hearing_id(hearing: Hearing, state: State) -> str | None:
    """Check if this hearing already exists in state DB under a different ID.

    When a hearing's title changes (e.g. YouTube title replaced by congress.gov
    title), its computed ID changes.  This function detects the old record and
    migrates it to the new ID.

    Returns the old ID if migration happened, None otherwise.
    """
    new_id = hearing.id

    # 1. Look up by congress.gov event ID (strongest signal)
    event_id = hearing.sources.get("congress_api_event_id")
    if event_id:
        existing = state.find_by_congress_event_id(event_id)
        if existing and existing["id"] != new_id:
            old_id = existing["id"]
            _migrate_hearing_id(old_id, hearing, state)
            return old_id

    # 2. Fallback: look up by committee + date with fuzzy title match
    candidates = state.find_by_committee_date(hearing.committee_key, hearing.date)
    for candidate in candidates:
        if candidate["id"] == new_id:
            continue
        if title_similarity(hearing.title, candidate["title"]) >= 0.30:
            old_id = candidate["id"]
            _migrate_hearing_id(old_id, hearing, state)
            return old_id

    return None


def _migrate_hearing_id(old_id: str, hearing: Hearing, state: State) -> None:
    """Migrate state DB records and transcript files from old_id to new hearing ID."""
    new_id = hearing.id
    log.info("Migrating hearing ID: %s -> %s (%s)", old_id, new_id, hearing.title[:60])

    state.merge_hearing_id(old_id, new_id)

    # Rename transcript directory
    for committee_dir in config.TRANSCRIPTS_DIR.glob("*/"):
        old_dir = committee_dir / f"{hearing.date}_{old_id}"
        if old_dir.is_dir():
            new_dir = committee_dir / f"{hearing.date}_{new_id}"
            if new_dir.exists():
                # Merge: copy files from old that don't exist in new
                for f in old_dir.iterdir():
                    dst = new_dir / f.name
                    if not dst.exists():
                        if f.is_dir():
                            shutil.copytree(f, dst)
                        else:
                            shutil.copy2(f, dst)
                shutil.rmtree(old_dir)
            else:
                old_dir.rename(new_dir)
            log.info("  Renamed transcript dir: %s -> %s", old_dir.name, new_dir.name)

    # Update index.json
    index_path = config.TRANSCRIPTS_DIR / "index.json"
    index = _read_index(index_path)
    if index is not None:
        for entry in index.get("hearings", []):
            if entry.get("id") == old_id:
                entry["id"] = new_id
                entry["title"] = hearing.title
                entry["path"] = f"{hearing.committee_key}/{hearing.date}_{new_id}"
                break
        tmp = index_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(index, indent=2))
        os.replace(tmp, index_path)


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

    # Step lifecycle:
    #   "not started" = step not in state DB (is_step_done returns False)
    #   "done"        = step completed (success or intentional skip)
    # Only mark "done" when the step genuinely completed or was intentionally
    # skipped (e.g. no YouTube URL means captions can't run — mark done so we
    # don't retry).  Leave unmarked if a future run might provide the input.

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
                state.mark_step(hearing.id, "cleanup", "done")
            except (subprocess.SubprocessError, httpx.HTTPError, OSError, ValueError) as e:
                state.mark_step(hearing.id, "captions", "failed", error=str(e))
                log.error("Caption processing failed for %s: %s", hearing.id, e)
        else:
            log.info("Captions already processed for %s", hearing.id)
    else:
        state.mark_step(hearing.id, "captions", "done")
        state.mark_step(hearing.id, "cleanup", "done")

    # 1.5. Senate ISVP captions (broadcast-quality stenographer captions)
    isvp_comm = hearing.sources.get("isvp_comm")
    isvp_filename = hearing.sources.get("isvp_filename")
    if isvp_comm and isvp_filename:
        if not state.is_step_done(hearing.id, "isvp_fetched"):
            state.mark_step(hearing.id, "isvp", "running")
            try:
                from isvp import fetch_isvp_captions
                isvp_text = fetch_isvp_captions(isvp_comm, isvp_filename)
                if isvp_text:
                    isvp_path = hearing_dir / "isvp_transcript.txt"
                    tmp_fd, tmp_path = tempfile.mkstemp(dir=isvp_path.parent, suffix='.tmp')
                    try:
                        with os.fdopen(tmp_fd, 'w') as f:
                            f.write(isvp_text)
                        os.replace(tmp_path, isvp_path)
                    except Exception:
                        os.unlink(tmp_path)
                        raise
                    result["outputs"]["isvp_transcript"] = str(isvp_path)
                    state.mark_step(hearing.id, "isvp_fetched", "done")
                    log.info(
                        "ISVP captions: %d chars for %s",
                        len(isvp_text), hearing.id,
                    )
                state.mark_step(hearing.id, "isvp", "done")
            except (httpx.HTTPError, OSError, ValueError) as e:
                state.mark_step(hearing.id, "isvp", "failed", error=str(e))
                log.error("ISVP caption fetch failed for %s: %s", hearing.id, e)
        else:
            log.debug("ISVP transcript already fetched for %s", hearing.id)

        # ISVP cleanup (text quality only, speaker labels already present)
        if config.CLEANUP_MODEL and not state.is_step_done(hearing.id, "isvp_cleanup"):
            isvp_raw_path = hearing_dir / "isvp_transcript.txt"
            if isvp_raw_path.exists():
                try:
                    isvp_raw = isvp_raw_path.read_text()
                    cleanup_result = cleanup_transcript(
                        isvp_raw,
                        hearing_title=hearing.title,
                        committee_name=hearing.committee_name,
                        skip_diarization=True,
                    )
                    cleaned_path = hearing_dir / "isvp_cleaned.txt"
                    tmp_fd, tmp_path = tempfile.mkstemp(dir=cleaned_path.parent, suffix='.tmp')
                    try:
                        with os.fdopen(tmp_fd, 'w') as f:
                            f.write(cleanup_result.text)
                        os.replace(tmp_path, cleaned_path)
                    except Exception:
                        os.unlink(tmp_path)
                        raise
                    result["outputs"]["isvp_cleaned"] = str(cleaned_path)
                    cost["llm_cleanup_usd"] += cleanup_result.cost_usd
                    state.mark_step(hearing.id, "isvp_cleanup", "done")
                    log.info(
                        "ISVP cleanup: %d→%d chars, $%.4f for %s",
                        len(isvp_raw), len(cleanup_result.text),
                        cleanup_result.cost_usd, hearing.id,
                    )
                except (httpx.HTTPError, OSError, ValueError) as e:
                    state.mark_step(hearing.id, "isvp_cleanup", "failed", error=str(e))
                    log.error("ISVP cleanup failed for %s: %s", hearing.id, e)

    # 1.6. C-SPAN broadcast captions
    cspan_url = hearing.sources.get("cspan_url")
    if cspan_url:
        # Use "cspan_fetched" (transcript actually obtained) not "cspan" (step attempted).
        # This allows retry if a previous run attempted but found no transcript.
        if not state.is_step_done(hearing.id, "cspan_fetched"):
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
            except (TimeoutError, OSError, ValueError) as e:
                state.mark_step(hearing.id, "cspan", "failed", error=str(e))
                log.error("C-SPAN caption fetch failed for %s: %s", hearing.id, e)
        else:
            log.debug("C-SPAN transcript already fetched for %s", hearing.id)

        # C-SPAN cleanup (text quality only, speaker labels already present)
        if config.CLEANUP_MODEL and not state.is_step_done(hearing.id, "cspan_cleanup"):
            cspan_raw_path = hearing_dir / "cspan_transcript.txt"
            if cspan_raw_path.exists():
                try:
                    cspan_raw = cspan_raw_path.read_text()
                    cleanup_result = cleanup_transcript(
                        cspan_raw,
                        hearing_title=hearing.title,
                        committee_name=hearing.committee_name,
                        skip_diarization=True,
                    )
                    cleaned_path = hearing_dir / "cspan_cleaned.txt"
                    tmp_fd, tmp_path = tempfile.mkstemp(dir=cleaned_path.parent, suffix='.tmp')
                    try:
                        with os.fdopen(tmp_fd, 'w') as f:
                            f.write(cleanup_result.text)
                        os.replace(tmp_path, cleaned_path)
                    except Exception:
                        os.unlink(tmp_path)
                        raise
                    result["outputs"]["cspan_cleaned"] = str(cleaned_path)
                    cost["llm_cleanup_usd"] += cleanup_result.cost_usd
                    state.mark_step(hearing.id, "cspan_cleanup", "done")
                    log.info(
                        "C-SPAN cleanup: %d→%d chars, $%.4f for %s",
                        len(cspan_raw), len(cleanup_result.text),
                        cleanup_result.cost_usd, hearing.id,
                    )
                except (httpx.HTTPError, OSError, ValueError) as e:
                    state.mark_step(hearing.id, "cspan_cleanup", "failed", error=str(e))
                    log.error("C-SPAN cleanup failed for %s: %s", hearing.id, e)
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
            except (httpx.HTTPError, OSError, ValueError) as e:
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
            except (httpx.HTTPError, OSError, ValueError) as e:
                state.mark_step(hearing.id, "govinfo", "failed", error=str(e))
                log.error("GovInfo fetch failed for %s: %s", hearing.id, e)
    else:
        state.mark_step(hearing.id, "govinfo", "done")

    # Compute total cost
    cost["total_usd"] = cost["llm_cleanup_usd"] + cost["whisper_usd"]

    # Mark hearing as fully processed
    state.mark_processed(hearing.id)

    # Write metadata to run dir
    meta = result.copy()
    meta["processed_at"] = datetime.now(timezone.utc).isoformat()
    meta_path = hearing_dir / "meta.json"
    tmp = meta_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(meta, indent=2))
    os.replace(tmp, meta_path)

    # Publish to transcripts/ canonical archive
    _publish_to_transcripts(hearing, hearing_dir, result)

    return result


# Transcript priority: highest quality first. Each entry is an output key,
# or a tuple (output_key, sub_key) for nested dicts like audio.
TRANSCRIPT_PRIORITY = [
    "govinfo_transcript",
    "cspan_cleaned",
    "cspan_transcript",
    "isvp_cleaned",
    "isvp_transcript",
    ("audio", "cleaned_transcript"),
    ("audio", "captions"),
]


def _select_best_transcript(outputs: dict) -> Path | None:
    """Return the highest-priority transcript path from outputs."""
    for entry in TRANSCRIPT_PRIORITY:
        if isinstance(entry, tuple):
            outer, inner = entry
            container = outputs.get(outer, {})
            if isinstance(container, dict) and container.get(inner):
                return Path(container[inner])
        else:
            val = outputs.get(entry)
            if val:
                return Path(val)
    return None


def _publish_to_transcripts(hearing: Hearing, run_hearing_dir: Path, result: dict) -> None:
    """Copy final artifacts to transcripts/{committee_key}/{date}_{id}/."""
    transcript_dir = config.TRANSCRIPTS_DIR / hearing.committee_key / f"{hearing.date}_{hearing.id}"
    transcript_dir.mkdir(parents=True, exist_ok=True)

    # Select best transcript by priority (highest quality first)
    best_transcript = _select_best_transcript(result.get("outputs", {}))

    # Copy best transcript
    if best_transcript and best_transcript.exists() and best_transcript.stat().st_size > 0:
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
    meta_path = transcript_dir / "meta.json"
    tmp = meta_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(meta, indent=2))
    os.replace(tmp, meta_path)

    log.info("Published to %s", transcript_dir)


def _read_index(index_path: Path) -> dict | None:
    """Read and parse index.json, returning None if absent or corrupt."""
    if not index_path.exists():
        return None
    try:
        return json.loads(index_path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        log.warning("Failed to read %s: %s", index_path, e)
        return None


def _update_index(results: list[dict]) -> None:
    """Update transcripts/index.json global manifest."""
    config.TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
    index_path = config.TRANSCRIPTS_DIR / "index.json"

    existing = _read_index(index_path) or {"hearings": []}

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
    tmp = index_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(existing, indent=2))
    os.replace(tmp, index_path)
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
        all_committees = config.get_all_committees()
        if args.committee not in all_committees:
            log.error("Unknown committee: %s", args.committee)
            log.info("Available: %s", ", ".join(sorted(all_committees.keys())))
            sys.exit(1)
        active = {args.committee: all_committees[args.committee]}
    elif args.tier:
        active = config.get_committees(max_tier=args.tier)
    else:
        active = config.get_committees(max_tier=2)

    max_cost = args.max_cost or config.MAX_COST_PER_RUN
    state = State()
    log.info(
        "Run %s: monitoring %d committees, looking back %d day(s), max cost $%.2f",
        run_id, len(active), args.days, max_cost,
    )

    # Discover — pass committees dict and state (for C-SPAN rotation tracking)
    hearings = discover_all(days=args.days, committees=active, state=state)

    if not hearings:
        log.info("No new hearings found.")
        return

    # Reconcile hearing IDs: if a hearing was previously stored under a
    # different ID (e.g. YouTube title, now replaced by congress.gov title),
    # migrate state DB records and transcript files to the new canonical ID.
    for h in hearings:
        _reconcile_hearing_id(h, state)

    # Filter out already-processed hearings (unless --reprocess)
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
            elif h.sources.get("isvp_comm") and not state.is_step_done(h.id, "isvp_fetched"):
                # Re-process hearings that gained ISVP params since last run
                new_hearings.append(h)
                log.debug("Re-processing %s: new ISVP params", h.id)

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
        has_isvp = bool(r.get("outputs", {}).get("isvp_transcript"))
        cost_usd = r.get("cost", {}).get("total_usd", 0)
        log.info(
            "[%d/%d] %s | cap=%s clean=%s cspan=%s isvp=%s testy=%d gov=%s $%.4f",
            i, n_total, r["title"][:50],
            "Y" if has_cap else "-",
            "Y" if has_clean else "-",
            "Y" if has_cspan else "-",
            "Y" if has_isvp else "-",
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
    run_meta_path = run_dir / "run_meta.json"
    tmp = run_meta_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(run_meta, indent=2))
    os.replace(tmp, run_meta_path)

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
        has_isvp = bool(outputs.get("isvp_transcript"))
        n_testimony = len(outputs.get("testimony", []))
        has_govinfo = bool(outputs.get("govinfo_transcript"))
        log.info(
            "  %s | captions=%s cleaned=%s cspan=%s isvp=%s testimony=%d govinfo=%s | $%.4f",
            r["title"][:50],
            "yes" if has_captions else "no",
            "yes" if has_cleaned else "no",
            "yes" if has_cspan else "no",
            "yes" if has_isvp else "no",
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
    check_and_alert(state, failing=failing)


if __name__ == "__main__":
    main()
