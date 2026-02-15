from __future__ import annotations

import hashlib
import os
import re
import sys
import time
from threading import Lock

import httpx


# Common stopwords for hearing title keyword extraction / comparison.
# Shared by discover.py and cspan.py.
TITLE_STOPWORDS = frozenset({
    "the", "a", "an", "of", "in", "on", "to", "for", "and", "or",
    "at", "by", "is", "it", "as", "be", "was", "are", "its", "with",
    "that", "this", "from", "before", "after", "hearing", "committee",
    "subcommittee", "full", "oversight", "examine", "examining",
    "regarding", "concerning", "review", "united", "states", "senate",
    "house", "congress", "testifies", "testimony", "witnesses",
    "hearings", "focusing",
})


# yt-dlp environment setup — include venv bin, sys executable dir, and deno
_VENV_BIN = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".venv", "bin")
_SYS_BIN = os.path.dirname(os.path.abspath(sys.executable)) if hasattr(sys, "executable") and sys.executable else ""
DENO_DIR = os.path.expanduser("~/.deno/bin")
YT_DLP_ENV = {**os.environ, "PATH": f"{_VENV_BIN}:{_SYS_BIN}:{DENO_DIR}:{os.environ.get('PATH', '')}"}


def get_http_client(retries: int = 3, timeout: float = 20.0) -> httpx.Client:
    """Create an httpx client with retry transport and standard headers."""
    transport = httpx.HTTPTransport(retries=retries)
    return httpx.Client(
        transport=transport,
        timeout=timeout,
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (compatible; HearingBot/1.0)"},
    )


class RateLimiter:
    """Enforce minimum delay between requests to the same domain."""

    def __init__(self, min_delay: float = 1.0):
        self.min_delay = min_delay
        self._last_request: dict[str, float] = {}
        self._lock = Lock()

    def wait(self, domain: str) -> None:
        """Sleep if needed to respect rate limit for domain."""
        with self._lock:
            now = time.time()
            if domain in self._last_request:
                elapsed = now - self._last_request[domain]
                if elapsed < self.min_delay:
                    time.sleep(self.min_delay - elapsed)
            self._last_request[domain] = time.time()


def hearing_id(committee_key: str, date: str, title: str) -> str:
    """Generate a stable ID for a hearing from its key fields.
    Uses first 12 chars of SHA256 of normalized (committee_key, date, title_prefix).
    """
    # Normalize title for consistent hashing
    title_normalized = normalize_title(title)

    # Create hash input
    hash_input = f"{committee_key}|{date}|{title_normalized}"

    # Generate SHA256 and return first 12 characters
    hash_digest = hashlib.sha256(hash_input.encode("utf-8")).hexdigest()
    return hash_digest[:12]


# Pre-compiled patterns for normalize_title
_TITLE_PREFIX_RES = [
    re.compile(prefix, re.IGNORECASE)
    for prefix in [
        r"^full committee hearing:\s*",
        r"^hearing notice:\s*",
        r"^subcommittee hearing:\s*",
        r"^markup:\s*",
        r"^business meeting:\s*",
        r"^hearing:\s*",
        r"^notice:\s*",
    ]
]
_PUNCT_RE = re.compile(r"[^\w\s]")
_MULTI_SPACE_RE = re.compile(r"\s+")


def normalize_title(title: str) -> str:
    """Normalize a hearing title for comparison.
    Strips common prefixes ('Full Committee Hearing:', 'HEARING NOTICE:', etc.),
    lowercases, removes punctuation, returns first 8 words.
    """
    normalized = title.lower()
    for pattern in _TITLE_PREFIX_RES:
        normalized = pattern.sub("", normalized)

    normalized = _PUNCT_RE.sub(" ", normalized)
    normalized = _MULTI_SPACE_RE.sub(" ", normalized).strip()

    words = normalized.split()
    return " ".join(words[:8])
