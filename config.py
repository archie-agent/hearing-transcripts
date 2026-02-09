"""Configuration: load committees from data/committees.json, API settings, paths."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

log = logging.getLogger(__name__)

# Load .env from the project root (won't override existing env vars)
load_dotenv(Path(__file__).parent / ".env")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(os.environ.get("HEARINGS_ROOT", str(Path(__file__).parent)))
RUNS_DIR = Path(os.environ.get("HEARINGS_RUNS_DIR", str(ROOT / "runs")))
TRANSCRIPTS_DIR = Path(os.environ.get("HEARINGS_TRANSCRIPTS_DIR", str(ROOT / "transcripts")))
DATA_DIR = ROOT / "data"
COMMITTEES_JSON = DATA_DIR / "committees.json"

# ---------------------------------------------------------------------------
# API keys (inherited from environment / ~/.env)
# ---------------------------------------------------------------------------
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
GOVINFO_API_KEY = os.environ.get("GOVINFO_API_KEY", "DEMO_KEY")
CONGRESS_API_KEY = os.environ.get("CONGRESS_API_KEY", GOVINFO_API_KEY)
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")

if GOVINFO_API_KEY == "DEMO_KEY":
    log.warning("Using GovInfo DEMO_KEY (40 req/min, 1000/hr). Register free at api.data.gov")

# ---------------------------------------------------------------------------
# Model choices — single place to change LLM/transcription models
# ---------------------------------------------------------------------------
# OpenRouter pricing per 1M tokens: (input, output)
MODEL_PRICING: dict[str, tuple[float, float]] = {
    "google/gemini-2.0-flash-001":     (0.10,  0.40),
    "google/gemini-2.0-flash-lite-001":(0.075, 0.30),
    "openai/gpt-4o-mini":             (0.15,  0.60),
    "google/gemini-3-flash-preview":   (0.50,  3.00),
    "anthropic/claude-haiku-4-5":      (1.00,  5.00),
}

# LLM cleanup model for diarization + formatting of captions.
# Runs via OpenRouter. Set to "" to skip cleanup.
CLEANUP_MODEL = os.environ.get("CLEANUP_MODEL", "google/gemini-3-flash-preview")

# ---------------------------------------------------------------------------
# Transcription settings
# ---------------------------------------------------------------------------
# "captions-only" grabs YouTube auto-captions (free) — the default.
# "openai" uses Whisper-1 or GPT-4o transcribe ($0.36/hr).
TRANSCRIPTION_BACKEND = os.environ.get("TRANSCRIPTION_BACKEND", "captions-only")

# Maximum cost (USD) per pipeline run. Abort if exceeded.
MAX_COST_PER_RUN = float(os.environ.get("MAX_COST_PER_RUN", "5.0"))

# Maximum audio file size for OpenAI API (bytes). Files larger get chunked.
OPENAI_MAX_FILE_BYTES = 25 * 1024 * 1024  # 25 MB

# Whisper cost estimation: $0.006 per minute of audio
WHISPER_COST_PER_MINUTE = 0.006

# ---------------------------------------------------------------------------
# Congress number
# ---------------------------------------------------------------------------
def current_congress() -> int:
    """Calculate current Congress number from date. 119th = 2025-2026."""
    return (datetime.now().year - 1789) // 2 + 1

CONGRESS = current_congress()

# ---------------------------------------------------------------------------
# Committee data — loaded from committees.json (single source of truth)
# ---------------------------------------------------------------------------
def _load_committees() -> dict[str, dict]:
    """Load committee config from JSON. Returns {key: {...committee data...}}."""
    try:
        with open(COMMITTEES_JSON) as f:
            data = json.load(f)
        return data.get("committees", {})
    except (FileNotFoundError, json.JSONDecodeError) as e:
        log.error("Failed to load committees.json: %s", e)
        return {}

# Global committee registry. Keyed by "chamber.slug" (e.g., "house.ways_and_means").
COMMITTEES: dict[str, dict] = _load_committees()


def get_committees(max_tier: int = 2) -> dict[str, dict]:
    """Return committees filtered by tier. Tier 1 = core economics, 2 = adjacent, 3 = peripheral."""
    return {k: v for k, v in COMMITTEES.items() if v.get("tier", 3) <= max_tier}


def get_committee_meta(key: str) -> dict | None:
    """Look up committee info by dotted key like 'house.judiciary'."""
    return COMMITTEES.get(key)
