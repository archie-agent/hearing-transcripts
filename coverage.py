#!/usr/bin/env python3
"""Quick coverage analysis: what sources does each hearing have?

By default skips C-SPAN WAF searches (slow). Pass --with-cspan to include them.
"""
from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import config
from discover import discover_all
from state import State

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Hearing source coverage analysis")
    parser.add_argument("--days", type=int, default=14, help="Days to look back (default: 14)")
    parser.add_argument("--skip-cspan", action="store_true", default=True,
                        help="Skip slow C-SPAN WAF searches (default: True)")
    parser.add_argument("--with-cspan", action="store_true",
                        help="Include C-SPAN WAF searches (overrides --skip-cspan)")
    args = parser.parse_args()

    skip_cspan = args.skip_cspan and not args.with_cspan
    days = args.days
    state = State()
    active = config.get_committees()

    log.info("Running fast discovery (YouTube + websites + congress.gov), %d days, %d committees", days, len(active))
    hearings = discover_all(days=days, committees=active, state=state, skip_cspan=skip_cspan)
    log.info("Discovered %d hearings", len(hearings))

    # Filter to past hearings only (today or earlier)
    today = date.today().isoformat()
    past_hearings = [h for h in hearings if h.date <= today]
    future_hearings = [h for h in hearings if h.date > today]
    log.info("Past hearings: %d, Future: %d (excluded)", len(past_hearings), len(future_hearings))
    hearings = past_hearings

    # Analyze sources
    # Per-hearing summary
    committee_stats = defaultdict(lambda: {"total": 0, "youtube": 0, "website": 0,
                                            "congress": 0, "cspan": 0, "isvp": 0,
                                            "govinfo": 0, "testimony": 0, "no_video": 0})
    no_video = []

    for h in hearings:
        s = h.sources
        cs = committee_stats[h.committee_key]
        cs["total"] += 1

        has_yt = bool(s.get("youtube_url"))
        has_web = bool(s.get("website_url"))
        has_cong = bool(s.get("congress_api_event_id"))
        has_cspan = bool(s.get("cspan_url"))
        has_isvp = bool(s.get("isvp_comm"))
        has_gov = bool(s.get("govinfo_package_id"))
        has_test = bool(s.get("testimony_pdf_urls"))

        if has_yt: cs["youtube"] += 1
        if has_web: cs["website"] += 1
        if has_cong: cs["congress"] += 1
        if has_cspan: cs["cspan"] += 1
        if has_isvp: cs["isvp"] += 1
        if has_gov: cs["govinfo"] += 1
        if has_test: cs["testimony"] += 1

        # No video source at all (no YouTube, no C-SPAN, no ISVP)
        if not has_yt and not has_cspan and not has_isvp:
            cs["no_video"] += 1
            no_video.append(h)

    # Print per-committee table
    print("\n" + "=" * 120)
    print(f"{'Committee':<35} {'Total':>5} {'YT':>4} {'Web':>4} {'Cong':>4} {'CSPAN':>5} {'ISVP':>4} {'Gov':>4} {'PDF':>4} {'NoVid':>5}")
    print("-" * 120)

    for ck in sorted(committee_stats.keys()):
        cs = committee_stats[ck]
        print(f"{ck:<35} {cs['total']:>5} {cs['youtube']:>4} {cs['website']:>4} "
              f"{cs['congress']:>4} {cs['cspan']:>5} {cs['isvp']:>4} {cs['govinfo']:>4} "
              f"{cs['testimony']:>4} {cs['no_video']:>5}")

    # Totals
    print("-" * 120)
    totals = {k: sum(cs[k] for cs in committee_stats.values())
              for k in ["total", "youtube", "website", "congress", "cspan", "isvp", "govinfo", "testimony", "no_video"]}
    print(f"{'TOTAL':<35} {totals['total']:>5} {totals['youtube']:>4} {totals['website']:>4} "
          f"{totals['congress']:>4} {totals['cspan']:>5} {totals['isvp']:>4} {totals['govinfo']:>4} "
          f"{totals['testimony']:>4} {totals['no_video']:>5}")
    print("=" * 120)

    # Hearings with no video source
    if no_video:
        print(f"\n--- Hearings with NO video source ({len(no_video)}) ---")
        for h in sorted(no_video, key=lambda x: (x.committee_key, x.date)):
            srcs = []
            if h.sources.get("website_url"): srcs.append("web")
            if h.sources.get("congress_url"): srcs.append("cong")
            if h.sources.get("govinfo_package_id"): srcs.append("gov")
            print(f"  [{h.date}] {h.committee_key:<35} {h.title[:60]:<60} src={','.join(srcs) or 'none'}")

    # Source availability summary
    print(f"\n--- Source Availability ({totals['total']} hearings) ---")
    if totals['total'] > 0:
        print(f"  YouTube video:     {totals['youtube']:>4} ({100*totals['youtube']/totals['total']:.0f}%)")
        print(f"  Committee website: {totals['website']:>4} ({100*totals['website']/totals['total']:.0f}%)")
        print(f"  Congress.gov:      {totals['congress']:>4} ({100*totals['congress']/totals['total']:.0f}%)")
        print(f"  C-SPAN:            {totals['cspan']:>4} ({100*totals['cspan']/totals['total']:.0f}%) [DDG/committee search skipped]")
        print(f"  Senate ISVP:       {totals['isvp']:>4} ({100*totals['isvp']/totals['total']:.0f}%)")
        print(f"  GovInfo official:  {totals['govinfo']:>4} ({100*totals['govinfo']/totals['total']:.0f}%)")
        print(f"  Testimony PDFs:    {totals['testimony']:>4} ({100*totals['testimony']/totals['total']:.0f}%)")
        print(f"  NO video source:   {totals['no_video']:>4} ({100*totals['no_video']/totals['total']:.0f}%)")
    else:
        print("  No hearings found.")


if __name__ == "__main__":
    main()
