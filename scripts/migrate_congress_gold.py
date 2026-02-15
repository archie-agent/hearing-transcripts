#!/usr/bin/env python3
"""One-time migration: backfill congress_event_id and merge duplicate hearings.

This script:
1. Backfills congress_event_id from sources_json for all existing hearings.
2. Finds duplicate pairs (same committee+date, different IDs) where one has
   congress.gov data and the other doesn't.
3. Merges duplicates: migrates processing state, renames transcript dirs,
   updates the index.

Dry-run by default; pass --apply to execute changes.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from state import State


def main():
    parser = argparse.ArgumentParser(description="Backfill congress_event_id and merge duplicates")
    parser.add_argument("--apply", action="store_true", help="Apply changes (default: dry-run)")
    args = parser.parse_args()

    state = State()
    conn = state._get_conn()

    # Step 1: Backfill congress_event_id from sources_json
    print("=== Step 1: Backfill congress_event_id ===")
    cursor = conn.execute(
        "SELECT id, sources_json, congress_event_id FROM hearings"
    )
    rows = cursor.fetchall()
    backfilled = 0
    for row in rows:
        if row["congress_event_id"]:
            continue
        sources = json.loads(row["sources_json"]) if row["sources_json"] else {}
        event_id = sources.get("congress_api_event_id")
        if event_id:
            print(f"  Backfill {row['id']}: congress_event_id = {event_id}")
            if args.apply:
                conn.execute(
                    "UPDATE hearings SET congress_event_id = ? WHERE id = ?",
                    (event_id, row["id"]),
                )
            backfilled += 1
    print(f"  {backfilled} hearings to backfill")
    if args.apply:
        conn.commit()

    # Step 2: Find duplicate pairs
    print("\n=== Step 2: Find duplicate pairs ===")
    cursor = conn.execute("""
        SELECT committee_key, date, GROUP_CONCAT(id, '|') as ids,
               COUNT(*) as cnt
        FROM hearings
        GROUP BY committee_key, date
        HAVING cnt > 1
    """)
    duplicates = cursor.fetchall()
    print(f"  {len(duplicates)} committee+date groups with multiple hearings")

    merges = []
    for dup in duplicates:
        ids = dup["ids"].split("|")
        # Fetch full details for each
        hearings = []
        for hid in ids:
            row = conn.execute(
                "SELECT id, title, sources_json, congress_event_id, processed_at "
                "FROM hearings WHERE id = ?", (hid,)
            ).fetchone()
            if row:
                hearings.append(dict(row))

        # Find the one with congress.gov data
        congress_hearings = [
            h for h in hearings if h.get("congress_event_id")
        ]
        non_congress = [
            h for h in hearings if not h.get("congress_event_id")
        ]

        if congress_hearings and non_congress:
            winner = congress_hearings[0]
            for loser in non_congress:
                merges.append((winner, loser, dup["committee_key"], dup["date"]))
                print(f"  Merge: {loser['id']} ({loser['title'][:50]})")
                print(f"    -> {winner['id']} ({winner['title'][:50]})")

    print(f"\n  {len(merges)} merges to perform")

    if not merges:
        print("\nNo merges needed.")
        return

    if not args.apply:
        print("\nDry run complete. Pass --apply to execute.")
        return

    # Step 3: Execute merges
    print("\n=== Step 3: Execute merges ===")
    for winner, loser, committee_key, date in merges:
        winner_id = winner["id"]
        loser_id = loser["id"]
        print(f"  Merging {loser_id} -> {winner_id}")

        # Merge sources into winner
        winner_sources = json.loads(winner["sources_json"]) if winner["sources_json"] else {}
        loser_sources = json.loads(loser["sources_json"]) if loser["sources_json"] else {}
        winner_sources.update(loser_sources)
        conn.execute(
            "UPDATE hearings SET sources_json = ? WHERE id = ?",
            (json.dumps(winner_sources), winner_id),
        )
        conn.commit()

        # Migrate DB records via State method
        state.merge_hearing_id(loser_id, winner_id)

        # Rename transcript directory
        for committee_dir in config.TRANSCRIPTS_DIR.glob("*/"):
            old_dir = committee_dir / f"{date}_{loser_id}"
            if old_dir.is_dir():
                new_dir = committee_dir / f"{date}_{winner_id}"
                if new_dir.exists():
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
                print(f"    Renamed dir: {old_dir.name} -> {new_dir.name}")

    # Update index.json (atomic write)
    index_path = config.TRANSCRIPTS_DIR / "index.json"
    if index_path.exists():
        index = json.loads(index_path.read_text())
        merged_losers = {loser["id"] for _, loser, _, _ in merges}
        index["hearings"] = [
            h for h in index.get("hearings", []) if h.get("id") not in merged_losers
        ]
        tmp = index_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(index, indent=2))
        os.replace(tmp, index_path)
        print(f"  Updated index.json (removed {len(merged_losers)} merged entries)")

    print(f"\nDone! {len(merges)} merges applied.")


if __name__ == "__main__":
    main()
