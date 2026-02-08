"""Configuration: committees of interest, API settings, paths."""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).parent
OUTPUT_DIR = ROOT / "output"
DATA_DIR = ROOT / "data"
COMMITTEES_JSON = DATA_DIR / "committees.json"

# ---------------------------------------------------------------------------
# API keys (inherited from environment / ~/.env)
# ---------------------------------------------------------------------------
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
GOVINFO_API_KEY = os.environ.get("GOVINFO_API_KEY", "DEMO_KEY")

# ---------------------------------------------------------------------------
# Transcription settings
# ---------------------------------------------------------------------------
# "openai" uses GPT-4o transcribe w/ diarization ($0.36/hr, 25MB chunks)
# "openai-mini" uses GPT-4o mini ($0.18/hr, no diarization)
# "captions-only" just grabs YouTube auto-captions (free)
TRANSCRIPTION_BACKEND = os.environ.get("TRANSCRIPTION_BACKEND", "openai")

# Maximum audio file size for OpenAI API (bytes). Files larger get chunked.
OPENAI_MAX_FILE_BYTES = 25 * 1024 * 1024  # 25 MB

# ---------------------------------------------------------------------------
# Economics-relevant committees
# Keys must match data/committees.json. Tagged by relevance tier:
#   1 = core economics (always process)
#   2 = adjacent (process if interesting)
# ---------------------------------------------------------------------------
COMMITTEES = {
    # House
    "house.ways_and_means":       {"tier": 1, "name": "House Ways & Means"},
    "house.financial_services":   {"tier": 1, "name": "House Financial Services"},
    "house.energy_commerce":      {"tier": 1, "name": "House Energy & Commerce"},
    "house.budget":               {"tier": 1, "name": "House Budget"},
    "house.appropriations":       {"tier": 2, "name": "House Appropriations"},
    "house.agriculture":          {"tier": 2, "name": "House Agriculture"},
    "house.foreign_affairs":      {"tier": 2, "name": "House Foreign Affairs"},
    "house.science":              {"tier": 2, "name": "House Science & Tech"},
    "house.small_business":       {"tier": 2, "name": "House Small Business"},
    "house.transportation":       {"tier": 2, "name": "House Transportation"},
    "house.oversight":            {"tier": 2, "name": "House Oversight"},
    "house.judiciary":            {"tier": 2, "name": "House Judiciary"},

    # Senate
    "senate.finance":             {"tier": 1, "name": "Senate Finance"},
    "senate.banking":             {"tier": 1, "name": "Senate Banking"},
    "senate.budget":              {"tier": 1, "name": "Senate Budget"},
    "senate.commerce":            {"tier": 1, "name": "Senate Commerce"},
    "senate.appropriations":      {"tier": 2, "name": "Senate Appropriations"},
    "senate.foreign_relations":   {"tier": 2, "name": "Senate Foreign Relations"},
    "senate.help":                {"tier": 2, "name": "Senate HELP"},
    "senate.intelligence":        {"tier": 2, "name": "Senate Intelligence"},
    "senate.homeland_security":   {"tier": 2, "name": "Senate Homeland Security"},
    "senate.environment":         {"tier": 2, "name": "Senate Environment & Public Works"},
    "senate.judiciary":           {"tier": 2, "name": "Senate Judiciary"},
}


def get_committee_meta(key: str) -> dict | None:
    """Look up committee info from committees.json by dotted key like 'house.judiciary'."""
    import json
    chamber, slug = key.split(".", 1)
    with open(COMMITTEES_JSON) as f:
        data = json.load(f)
    return data.get(chamber, {}).get(slug)
