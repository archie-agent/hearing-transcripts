"""Hearing transcript digest: extract notable quotes, score by interest, email a roundup.

Twice-weekly pipeline that:
1. Finds recent transcripts (last N days)
2. Extracts quotes/claims via Gemini Flash (per chunk)
3. Scores each quote against the interest model
4. Composes a grouped markdown roundup (Gemini Flash)
5. Polishes for readability (Claude Haiku 4.5)
6. Delivers via AgentMail
7. Records the run in state DB
"""

from __future__ import annotations

import argparse
import html as html_mod
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
import httpx

import config
from llm_utils import (
    call_openrouter,
    calculate_cost,
    estimate_tokens,
    get_api_key,
    split_into_chunks,
)
from state import State

logger = logging.getLogger(__name__)

MAX_QUOTES = 30
AGENTMAIL_SENDER = "archie-agent@agentmail.to"


@dataclass
class Quote:
    """A notable quote extracted from a hearing transcript."""

    text: str
    speaker: str
    context: str
    hearing_title: str
    committee: str
    hearing_date: str
    source_url: str
    score: float = 0.0
    themes: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Step 1: Find recent transcripts
# ---------------------------------------------------------------------------


def find_recent_transcripts(lookback_days: int) -> list[dict]:
    """Read index.json and return transcripts from the last N days."""
    index_path = config.TRANSCRIPTS_DIR / "index.json"
    if not index_path.exists():
        logger.error("index.json not found at %s", index_path)
        return []

    with open(index_path) as f:
        data = json.load(f)

    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
    recent = []
    seen_ids: set[str] = set()

    for entry in data.get("hearings", []):
        if entry.get("date", "") < cutoff:
            continue

        # Skip duplicate index entries
        hearing_id = entry.get("id", "")
        if hearing_id in seen_ids:
            continue
        seen_ids.add(hearing_id)

        transcript_dir = config.TRANSCRIPTS_DIR / entry["path"]
        transcript_file = transcript_dir / "transcript.txt"
        meta_file = transcript_dir / "meta.json"

        if not transcript_file.exists():
            logger.debug("No transcript.txt for %s, skipping", entry["id"])
            continue

        meta = {}
        if meta_file.exists():
            with open(meta_file) as f:
                meta = json.load(f)

        recent.append({
            "id": entry["id"],
            "title": entry.get("title", "Untitled Hearing"),
            "committee": entry.get("committee", ""),
            "date": entry.get("date", ""),
            "transcript_path": str(transcript_file),
            "meta": meta,
        })

    logger.info("Found %d transcripts in last %d days", len(recent), lookback_days)
    return recent


def _get_source_url(meta: dict) -> str:
    """Extract best available source URL from meta.json."""
    sources = meta.get("sources", {})
    # Prefer C-SPAN, then YouTube
    if sources.get("cspan_url"):
        return sources["cspan_url"]
    if sources.get("youtube_url"):
        return sources["youtube_url"]
    if sources.get("youtube_id"):
        return f"https://www.youtube.com/watch?v={sources['youtube_id']}"
    return ""


# ---------------------------------------------------------------------------
# Step 2: Extract quotes (Gemini Flash, per chunk)
# ---------------------------------------------------------------------------

EXTRACT_PROMPT = """Extract 3-8 notable quotes, claims, data points, or exchanges from this congressional hearing transcript chunk. Focus on substantive statements — policy positions, statistics, notable exchanges, surprising admissions.

Return a JSON array (no markdown fences) with objects containing:
- "quote": verbatim text, 1-3 sentences
- "speaker": speaker name if identifiable from context (e.g. "Chairman Smith", "Dr. Powell"), or "Unknown"
- "context": one sentence explaining why this quote matters or what it reveals

Transcript chunk:

{text}"""


def extract_quotes_from_transcript(
    hearing: dict, api_key: str
) -> tuple[list[Quote], float]:
    """Extract quotes from a single transcript. Returns (quotes, cost_usd)."""
    transcript_path = hearing["transcript_path"]
    with open(transcript_path) as f:
        text = f.read()

    if not text.strip():
        return [], 0.0

    source_url = _get_source_url(hearing["meta"])
    chunks = split_into_chunks(text, chunk_size=4000, overlap=200)
    logger.info(
        "Extracting quotes from '%s' (%d chunks)",
        hearing["title"][:60],
        len(chunks),
    )

    all_quotes: list[Quote] = []
    total_cost = 0.0

    for i, chunk in enumerate(chunks):
        prompt = EXTRACT_PROMPT.format(text=chunk)
        try:
            response = call_openrouter(prompt, config.DIGEST_MODEL, api_key)

            usage = response.get("usage", {})
            cost = calculate_cost(
                config.DIGEST_MODEL,
                usage.get("prompt_tokens", 0),
                usage.get("completion_tokens", 0),
            )
            total_cost += cost

            raw = response["choices"][0]["message"]["content"]
        except (httpx.HTTPError, KeyError, IndexError) as e:
            logger.warning(
                "Quote extraction failed for chunk %d of %s: %s", i, hearing["id"], e
            )
            continue

        # Strip markdown code fences if present
        raw = re.sub(r"^```(?:json)?\s*", "", raw.strip())
        raw = re.sub(r"\s*```$", "", raw.strip())

        try:
            items = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Failed to parse quote JSON for %s", hearing["id"])
            continue

        if not isinstance(items, list):
            continue

        for item in items:
            if not isinstance(item, dict) or not item.get("quote"):
                continue
            all_quotes.append(Quote(
                text=item["quote"],
                speaker=item.get("speaker", "Unknown"),
                context=item.get("context", ""),
                hearing_title=hearing["title"],
                committee=hearing["committee"],
                hearing_date=hearing["date"],
                source_url=source_url,
            ))

    logger.info(
        "Extracted %d quotes from '%s' ($%.4f)",
        len(all_quotes),
        hearing["title"][:60],
        total_cost,
    )
    return all_quotes, total_cost


# ---------------------------------------------------------------------------
# Step 3: Score against interest model
# ---------------------------------------------------------------------------


def score_quotes(quotes: list[Quote]) -> tuple[list[Quote], float]:
    """Score quotes against interest model, filter, and sort. Returns (filtered, cost)."""
    try:
        from interest_model.core import InterestModel
    except ImportError:
        logger.warning("interest_model not installed — skipping scoring, keeping all quotes")
        return quotes[:MAX_QUOTES], 0.0

    model = InterestModel()
    for q in quotes:
        result = model.score(f"{q.hearing_title}\n\n{q.text}")
        q.score = result.score
        q.themes = list(result.top_interests)

    filtered = [q for q in quotes if q.score >= config.DIGEST_SCORE_THRESHOLD]
    filtered.sort(key=lambda q: q.score, reverse=True)
    filtered = filtered[:MAX_QUOTES]

    logger.info(
        "Scoring: %d total → %d above threshold (%.2f)",
        len(quotes),
        len(filtered),
        config.DIGEST_SCORE_THRESHOLD,
    )
    # Interest model scoring cost is negligible (OpenAI embeddings cached in DB)
    return filtered, 0.0


# ---------------------------------------------------------------------------
# Step 4: Compose digest (Gemini Flash)
# ---------------------------------------------------------------------------

COMPOSE_PROMPT = """You're writing a casual, informative email digest of notable quotes from recent congressional hearings. The reader is a journalist interested in economics, policy, and politics.

Below are quotes grouped by theme. For each quote, include:
- A blockquote with the verbatim text
- Speaker attribution
- Hearing name, committee, and date
- A [Source](url) link if available
- Optional 1-sentence commentary on why it matters

Write in markdown. Group quotes under thematic headers (##). Keep commentary brief and casual — this is a scan-and-read digest, not analysis. Don't add an introduction or conclusion — those will be added separately.

Quotes:

{quotes_json}"""


def compose_digest(quotes: list[Quote], api_key: str) -> tuple[str, float]:
    """Compose markdown digest from scored quotes. Returns (markdown, cost)."""
    # Group by top theme
    grouped: dict[str, list[dict]] = {}
    for q in quotes:
        theme = q.themes[0] if q.themes else "Other"
        entry = {
            "quote": q.text,
            "speaker": q.speaker,
            "hearing": q.hearing_title,
            "committee": q.committee,
            "date": q.hearing_date,
            "source_url": q.source_url,
            "score": round(q.score, 2),
        }
        grouped.setdefault(theme, []).append(entry)

    prompt = COMPOSE_PROMPT.format(quotes_json=json.dumps(grouped, indent=2))

    try:
        response = call_openrouter(prompt, config.DIGEST_MODEL, api_key, timeout=180.0)
        usage = response.get("usage", {})
        cost = calculate_cost(
            config.DIGEST_MODEL,
            usage.get("prompt_tokens", 0),
            usage.get("completion_tokens", 0),
        )
        body = response["choices"][0]["message"]["content"]
    except httpx.HTTPError as e:
        logger.warning("Transient HTTP error composing digest: %s", e)
        return "", 0.0
    except (KeyError, IndexError) as e:
        raise ValueError(f"Unexpected API response shape in compose_digest: {e}") from e

    logger.info("Composed digest (%d chars, $%.4f)", len(body), cost)
    return body, cost


# ---------------------------------------------------------------------------
# Step 5: Polish (Claude Haiku 4.5)
# ---------------------------------------------------------------------------

POLISH_PROMPT = """Lightly polish this congressional hearing quote digest for readability. Your tasks:

1. Add a 2-sentence opener that sets the scene (what period these hearings cover, general vibe)
2. Reorder sections if a different flow reads better
3. Tighten any verbose commentary — keep it punchy
4. Preserve ALL quotes verbatim, all links, all attributions
5. Keep the markdown formatting

Return the polished markdown:

{body}"""


def polish_digest(body: str, api_key: str) -> tuple[str, float]:
    """Polish the digest with Claude Haiku. Returns (polished, cost)."""
    prompt = POLISH_PROMPT.format(body=body)

    cost = 0.0
    try:
        response = call_openrouter(prompt, config.DIGEST_POLISH_MODEL, api_key, timeout=120.0)
        usage = response.get("usage", {})
        cost = calculate_cost(
            config.DIGEST_POLISH_MODEL,
            usage.get("prompt_tokens", 0),
            usage.get("completion_tokens", 0),
        )
        polished = response["choices"][0]["message"]["content"]
    except httpx.HTTPError as e:
        logger.warning("Polish step failed (transient HTTP error), using unpolished version: %s", e)
        return body, cost
    except (KeyError, IndexError) as e:
        logger.warning("Polish step got unexpected response shape, using unpolished version: %s", e)
        return body, cost

    logger.info("Polished digest (%d chars, $%.4f)", len(polished), cost)
    return polished, cost


# ---------------------------------------------------------------------------
# Step 6: Deliver via AgentMail
# ---------------------------------------------------------------------------


def _inline_format(text: str) -> str:
    """Apply inline markdown formatting: bold and links."""
    # Bold
    text = re.sub(
        r"\*\*(.+?)\*\*",
        lambda m: f'<strong style="color: #222;">{html_mod.escape(m.group(1))}</strong>',
        text,
    )
    # Links [text](url)
    def _replace_link(m):
        link_text = html_mod.escape(m.group(1))
        raw_url = m.group(2).strip()
        if not raw_url.lower().startswith(("http://", "https://")):
            return link_text
        url = html_mod.escape(raw_url)
        return (
            f'<a href="{url}" style="color: #2563eb; text-decoration: underline; '
            f'text-decoration-color: #93b4f5;">{link_text}</a>'
        )

    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", _replace_link, text)
    return text


def _markdown_to_simple_html(md: str) -> str:
    """Convert markdown to HTML for email.

    Handles: headers, bold, links, bullet lists, blockquotes, horizontal rules.
    """
    fmt = _inline_format
    lines = md.split("\n")
    html_lines: list[str] = []
    in_list = False
    in_blockquote = False

    for line in lines:
        stripped = line.strip()

        # Blockquote
        if stripped.startswith("> "):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            if not in_blockquote:
                html_lines.append(
                    '<blockquote style="border-left: 3px solid #c5c5c5; margin: 14px 0; '
                    'padding: 4px 16px; color: #444; font-style: italic;">'
                )
                in_blockquote = True
            html_lines.append(
                f'<p style="margin: 4px 0; font-size: 16px; line-height: 1.6;">'
                f"{fmt(stripped[2:])}</p>"
            )
            continue
        elif in_blockquote:
            html_lines.append("</blockquote>")
            in_blockquote = False

        # Headers
        if stripped.startswith("### "):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(
                f'<h3 style="margin: 20px 0 6px; font-size: 16px; font-weight: 600; color: #444;">'
                f"{fmt(stripped[4:])}</h3>"
            )
        elif stripped.startswith("## "):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(
                f'<h2 style="margin: 32px 0 10px; font-size: 18px; font-weight: 600; color: #333; '
                f'border-bottom: 1px solid #e5e5e5; padding-bottom: 6px;">'
                f"{fmt(stripped[3:])}</h2>"
            )
        elif stripped.startswith("# "):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(
                f'<h1 style="margin: 0 0 20px; font-size: 28px; font-weight: normal; line-height: 1.3; '
                f'color: #111;">'
                f"{fmt(stripped[2:])}</h1>"
            )
        elif stripped.startswith("- "):
            if not in_list:
                html_lines.append('<ul style="padding-left: 18px; margin: 8px 0;">')
                in_list = True
            html_lines.append(
                f'<li style="margin-bottom: 5px; font-size: 16px; line-height: 1.7;">'
                f"{fmt(stripped[2:])}</li>"
            )
        elif stripped == "---":
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(
                '<hr style="border: none; border-top: 1px dashed #ddd; margin: 28px 0;">'
            )
        elif stripped:
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(
                f'<p style="margin: 10px 0; font-size: 16px; line-height: 1.75;">'
                f"{fmt(stripped)}</p>"
            )
        else:
            if in_list:
                html_lines.append("</ul>")
                in_list = False

    if in_list:
        html_lines.append("</ul>")
    if in_blockquote:
        html_lines.append("</blockquote>")

    return "\n".join(html_lines)


def _wrap_html(body: str, date_str: str) -> str:
    """Wrap HTML body in a clean email shell."""
    return f"""<html>
<head><meta charset="utf-8"></head>
<body style="margin: 0; padding: 20px 16px; background-color: #fff; font-family: Georgia, 'Times New Roman', serif;">
<div style="max-width: 640px; margin: 0 auto;">
  <div style="margin-bottom: 24px; padding-bottom: 16px; border-bottom: 1px dashed #ccc;">
    <p style="margin: 0 0 2px; font-size: 12px; letter-spacing: 0.5px; color: #aaa; font-family: -apple-system, Helvetica, Arial, sans-serif;">Capitol Quotes \u00b7 {date_str}</p>
  </div>
  <div style="font-size: 16px; line-height: 1.75; color: #333;">
    {body}
  </div>
</div>
</body>
</html>"""


def deliver_digest(
    markdown: str,
    start_date: str,
    end_date: str,
    dry_run: bool = False,
) -> bool:
    """Send digest email via AgentMail. Returns True on success."""
    subject = f"Capitol Quotes \u2014 {start_date} to {end_date}"

    if dry_run:
        print(f"\n{'='*60}")
        print(f"Subject: {subject}")
        print(f"To: {config.DIGEST_RECIPIENT}")
        print(f"{'='*60}\n")
        print(markdown)
        return True

    # Load AgentMail API key from ~/.env.agentmail (won't override existing env vars)
    load_dotenv(Path.home() / ".env.agentmail")
    api_key = os.environ.get("AGENTMAIL_API_KEY")
    if not api_key:
        logger.error("AGENTMAIL_API_KEY not found — cannot send email")
        return False

    try:
        from agentmail import AgentMail

        client = AgentMail(api_key=api_key)
    except ImportError:
        logger.error("agentmail package not installed")
        return False

    html_body = _markdown_to_simple_html(markdown)
    html = _wrap_html(html_body, end_date)

    try:
        client.inboxes.messages.send(
            inbox_id=AGENTMAIL_SENDER,
            to=config.DIGEST_RECIPIENT,
            subject=subject,
            text=markdown,
            html=html,
        )
        logger.info("Digest sent to %s", config.DIGEST_RECIPIENT)
        return True
    except Exception:
        logger.exception("Failed to send digest email")
        return False


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def run_digest(dry_run: bool = False) -> None:
    """Run the full digest pipeline."""
    today = date.today().isoformat()

    # Check if already sent today
    state = State()
    if not dry_run and state.last_digest_date() == today:
        logger.info("Digest already sent today (%s), skipping", today)
        return

    api_key = get_api_key()
    total_cost = 0.0

    # Step 1: Find recent transcripts
    transcripts = find_recent_transcripts(config.DIGEST_LOOKBACK_DAYS)
    if not transcripts:
        logger.info("No recent transcripts found, nothing to digest")
        return

    # Step 2: Extract quotes
    all_quotes: list[Quote] = []
    for t in transcripts:
        quotes, cost = extract_quotes_from_transcript(t, api_key)
        all_quotes.extend(quotes)
        total_cost += cost

    if not all_quotes:
        logger.info("No quotes extracted, nothing to digest")
        return

    logger.info("Total quotes extracted: %d", len(all_quotes))

    # Step 3: Score against interest model
    scored_quotes, score_cost = score_quotes(all_quotes)
    total_cost += score_cost

    if not scored_quotes:
        logger.info("No quotes above threshold (%.2f)", config.DIGEST_SCORE_THRESHOLD)
        return

    # Step 4: Compose digest
    body, compose_cost = compose_digest(scored_quotes, api_key)
    total_cost += compose_cost

    if not body:
        logger.error("Failed to compose digest")
        return

    # Step 5: Polish
    polished, polish_cost = polish_digest(body, api_key)
    total_cost += polish_cost

    # Step 6: Deliver
    start_date = (date.today() - timedelta(days=config.DIGEST_LOOKBACK_DAYS)).isoformat()
    end_date = today
    sent = deliver_digest(polished, start_date, end_date, dry_run=dry_run)

    # Step 7: Record run
    if sent and not dry_run:
        state.record_digest_run(
            run_date=today,
            hearings_scanned=len(transcripts),
            quotes_extracted=len(all_quotes),
            quotes_selected=len(scored_quotes),
            cost_usd=total_cost,
        )

    logger.info(
        "Digest complete: %d hearings, %d quotes extracted, %d selected, $%.4f",
        len(transcripts),
        len(all_quotes),
        len(scored_quotes),
        total_cost,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Hearing transcript digest")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract, score, and compose but print to stdout instead of emailing",
    )
    args = parser.parse_args()

    run_digest(dry_run=args.dry_run)
