from __future__ import annotations

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


# Pre-compiled patterns for normalize_title — comprehensive set covering all
# prefix formats seen from YouTube, websites, GovInfo, and congress.gov.
_TITLE_STRIP_RES = [
    re.compile(r"^HEARING NOTICE:?\s*", re.IGNORECASE),
    re.compile(r"^Hearing\s+Entitled:?\s*", re.IGNORECASE),
    re.compile(r"^Oversight\s+Hearing\s*[-:]\s*", re.IGNORECASE),
    re.compile(r"^Hearings?\s*:?\s*", re.IGNORECASE),
    re.compile(r"^(Full Committee |Subcommittee )?Hearing:?\s*", re.IGNORECASE),
    re.compile(r"^[\w&]+\s+Hearing:?\s*", re.IGNORECASE),
    re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}\s*"),
    re.compile(r"^\*+[A-Z\s]+\*+\s*"),
    re.compile(r"^Upcoming\s*:?\s*", re.IGNORECASE),
    re.compile(r"^(An? )?(Oversight )?Hearing[s]?\s+to\s+(examine|consider)\s+", re.IGNORECASE),
    re.compile(r"\s+(Location|Time):.*$", re.IGNORECASE),
    re.compile(r"WASHINGTON,?\s*D\.?C\.?\s*[-–—].*$", re.IGNORECASE),
]
_TITLE_CLEAN_RE = re.compile(r"[^a-z0-9\s]")


def normalize_title(title: str) -> str:
    """Normalize a hearing title for comparison/dedup.

    Strips common prefixes ('Full Committee Hearing:', 'HEARING NOTICE:', etc.),
    lowercases, removes punctuation, returns first 8 words.
    """
    for pattern in _TITLE_STRIP_RES:
        title = pattern.sub("", title)
    words = _TITLE_CLEAN_RE.sub("", title.lower()).split()[:8]
    return " ".join(words)
