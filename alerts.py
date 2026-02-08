"""Simple alerting for failing scrapers.

Writes daily alert files to data/alerts/ and optionally posts to Slack
when scraper health checks exceed the failure threshold.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import httpx

from state import State

log = logging.getLogger(__name__)

ROOT = Path(__file__).parent
ALERTS_DIR = ROOT / "data" / "alerts"


def _format_alert(failing: list[dict]) -> str:
    """Build a human-readable alert message from a list of failing scrapers."""
    lines = [
        f"Scraper Alert — {len(failing)} source(s) failing",
        f"Generated at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC",
        "",
    ]

    for entry in failing:
        committee = entry["committee_key"]
        source = entry["source_type"]
        consecutive = entry["consecutive_failures"]
        last_success = entry.get("last_success") or "never"
        last_failure = entry.get("last_failure") or "unknown"

        lines.append(f"  {committee} / {source}")
        lines.append(f"    Consecutive failures : {consecutive}")
        lines.append(f"    Last success         : {last_success}")
        lines.append(f"    Last failure          : {last_failure}")
        lines.append("")

    return "\n".join(lines)


def _write_alert_file(message: str) -> Path:
    """Write the alert message to data/alerts/YYYY-MM-DD.txt (append)."""
    ALERTS_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    alert_path = ALERTS_DIR / f"{today}.txt"

    with open(alert_path, "a") as f:
        f.write(message)
        f.write("\n---\n")

    log.info("Alert written to %s", alert_path)
    return alert_path


def _post_to_slack(message: str, webhook_url: str) -> None:
    """Post an alert message to a Slack incoming webhook."""
    payload = {"text": f"```\n{message}\n```"}
    try:
        resp = httpx.post(webhook_url, json=payload, timeout=10)
        resp.raise_for_status()
        log.info("Slack alert sent successfully")
    except httpx.HTTPError as exc:
        log.warning("Failed to send Slack alert: %s", exc)


def check_and_alert(state: State, threshold: int = 3) -> list[dict]:
    """Check for failing scrapers and emit alerts if any are found.

    Returns the list of failing scrapers (empty list if all healthy).
    """
    failing = state.get_failing_scrapers(threshold=threshold)

    if not failing:
        log.debug("All scrapers healthy (threshold=%d)", threshold)
        return []

    message = _format_alert(failing)

    # Always write a local file
    _write_alert_file(message)

    # Optionally notify Slack
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if webhook_url:
        _post_to_slack(message, webhook_url)

    return failing
