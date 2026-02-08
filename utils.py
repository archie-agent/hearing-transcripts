from __future__ import annotations

import hashlib
import os
import re
import time
from datetime import datetime
from threading import Lock

import httpx


# yt-dlp environment setup
DENO_DIR = os.path.expanduser("~/.deno/bin")
YT_DLP_ENV = {**os.environ, "PATH": f"{DENO_DIR}:{os.environ.get('PATH', '')}"}


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


def current_congress() -> int:
    """Calculate current Congress number from date."""
    year = datetime.now().year
    return (year - 1789) // 2 + 1


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


def normalize_title(title: str) -> str:
    """Normalize a hearing title for comparison.
    Strips common prefixes ('Full Committee Hearing:', 'HEARING NOTICE:', etc.),
    lowercases, removes punctuation, returns first 8 words.
    """
    # Common prefixes to strip
    prefixes = [
        r"^full committee hearing:\s*",
        r"^hearing notice:\s*",
        r"^subcommittee hearing:\s*",
        r"^markup:\s*",
        r"^business meeting:\s*",
        r"^hearing:\s*",
        r"^notice:\s*",
    ]

    # Apply prefix removal
    normalized = title.lower()
    for prefix in prefixes:
        normalized = re.sub(prefix, "", normalized, flags=re.IGNORECASE)

    # Remove punctuation and extra whitespace
    normalized = re.sub(r"[^\w\s]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()

    # Return first 8 words
    words = normalized.split()
    return " ".join(words[:8])
