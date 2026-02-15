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
# API keys — read from environment each call so tests / late-set vars work
# ---------------------------------------------------------------------------

def get_openai_api_key() -> str:
    return os.environ.get("OPENAI_API_KEY", "")

_demo_key_warned = False


def get_govinfo_api_key() -> str:
    global _demo_key_warned
    key = os.environ.get("GOVINFO_API_KEY", "DEMO_KEY")
    if key == "DEMO_KEY" and not _demo_key_warned:
        _demo_key_warned = True
        log.warning("Using GovInfo DEMO_KEY (40 req/min, 1000/hr). Register free at api.data.gov")
    return key


def get_congress_api_key() -> str:
    return os.environ.get("CONGRESS_API_KEY", get_govinfo_api_key())


def get_openrouter_api_key() -> str:
    return os.environ.get("OPENROUTER_API_KEY", "")

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

# Validate MODEL_PRICING values at import time
for _model_name, (_in_price, _out_price) in MODEL_PRICING.items():
    if not isinstance(_in_price, (int, float)) or _in_price < 0:
        raise ValueError(f"MODEL_PRICING[{_model_name!r}] input price must be non-negative float, got {_in_price!r}")
    if not isinstance(_out_price, (int, float)) or _out_price < 0:
        raise ValueError(f"MODEL_PRICING[{_model_name!r}] output price must be non-negative float, got {_out_price!r}")

# LLM cleanup model for diarization + formatting of captions.
# Runs via OpenRouter. Set to "" to skip cleanup.
CLEANUP_MODEL = os.environ.get("CLEANUP_MODEL", "google/gemini-3-flash-preview")

TRANSCRIPTION_MODEL = "gpt-4o-transcribe"

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
        with open(COMMITTEES_JSON, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        raise ValueError(f"committees.json not found at {COMMITTEES_JSON}")
    except json.JSONDecodeError as e:
        raise ValueError(f"committees.json is corrupt: {e}") from e
    return data.get("committees", {})

# Lazy-loaded committee registry — avoids reading JSON at import time
_committees_cache: dict[str, dict] | None = None


def get_all_committees() -> dict[str, dict]:
    """Return the full committee registry, loading from JSON on first call."""
    global _committees_cache
    if _committees_cache is None:
        _committees_cache = _load_committees()
    return _committees_cache


def get_committees(max_tier: int = 99) -> dict[str, dict]:
    """Return committees filtered by tier. Tier 1 = core economics, 2 = adjacent, 3 = peripheral."""
    return {k: v for k, v in get_all_committees().items() if v.get("tier", 3) <= max_tier}


# ---------------------------------------------------------------------------
# Digest settings
# ---------------------------------------------------------------------------
DIGEST_MODEL = os.environ.get("DIGEST_MODEL", "google/gemini-3-flash-preview")
DIGEST_POLISH_MODEL = os.environ.get("DIGEST_POLISH_MODEL", "anthropic/claude-haiku-4-5")
DIGEST_RECIPIENT = os.environ.get("DIGEST_RECIPIENT", "archiehk98@gmail.com")
DIGEST_SCORE_THRESHOLD = float(os.environ.get("DIGEST_SCORE_THRESHOLD", "0.40"))
DIGEST_LOOKBACK_DAYS = int(os.environ.get("DIGEST_LOOKBACK_DAYS", "4"))

# AgentMail sender address for digest delivery
AGENTMAIL_SENDER = os.environ.get("AGENTMAIL_SENDER", "archie-agent@agentmail.to")


def get_committee_meta(key: str) -> dict | None:
    """Look up committee info by dotted key like 'house.judiciary'."""
    return get_all_committees().get(key)
