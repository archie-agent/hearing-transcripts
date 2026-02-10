"""LLM-based transcript cleanup and diarization for congressional hearings.

This module cleans up raw YouTube auto-captions from congressional hearings by:
- Adding proper punctuation and capitalization
- Fixing transcription errors
- Adding speaker labels (diarization) based on procedural cues
- Handling long transcripts via chunking with overlap
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

import config

logger = logging.getLogger(__name__)

# Default chunking parameters
DEFAULT_CHUNK_SIZE = 3000  # tokens (approximate)
DEFAULT_OVERLAP = 200  # tokens for context continuity

# OpenRouter API endpoint
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"


@dataclass
class CleanupResult:
    """Result of transcript cleanup operation."""

    text: str
    model: str
    input_tokens: int
    output_tokens: int
    cost_usd: float
    chunks_processed: int


def _get_api_key() -> str:
    """Get OpenRouter API key from env var or .env file.

    Returns:
        API key string

    Raises:
        ValueError: If API key cannot be found
    """
    # Try environment variable first
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if api_key:
        return api_key

    # Fall back to reading from .env file (project-local)
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        try:
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("OPENROUTER_API_KEY="):
                        api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
                        if api_key:
                            return api_key
        except Exception as e:
            logger.warning(f"Failed to read .env file: {e}")

    raise ValueError(
        "OPENROUTER_API_KEY not found. Set the environment variable or add it to .env"
    )


def _build_diarization_prompt(
    raw_text: str,
    hearing_title: str = "",
    committee_name: str = "",
    chunk_index: int = 0,
    total_chunks: int = 1,
) -> str:
    """Build the prompt for LLM-based cleanup and diarization.

    Args:
        raw_text: Raw caption text to clean up
        hearing_title: Title of the hearing (optional)
        committee_name: Name of the committee (optional)
        chunk_index: Current chunk index (for multi-chunk processing)
        total_chunks: Total number of chunks

    Returns:
        Formatted prompt string
    """
    context = []
    if hearing_title:
        context.append(f"Hearing: {hearing_title}")
    if committee_name:
        context.append(f"Committee: {committee_name}")

    context_str = "\n".join(context) if context else "Congressional hearing"

    chunk_info = ""
    if total_chunks > 1:
        chunk_info = f"\n\nNote: This is chunk {chunk_index + 1} of {total_chunks}. Maintain consistency with speaker labels."

    prompt = f"""You are transcribing a congressional hearing. Your task is to clean up raw auto-generated captions and add speaker labels (diarization).

{context_str}

Congressional hearings follow a predictable structure:
1. Committee Chair opens and makes opening statement
2. Ranking Member (senior minority party member) makes opening statement
3. Witnesses give prepared testimony
4. Question and Answer rounds with members

Instructions:
1. Fix capitalization, punctuation, and obvious transcription errors
2. Add speaker labels in brackets like [CHAIRMAN SMITH], [RANKING MEMBER JONES], [WITNESS: Dr. Powell], [REP. GARCIA], [SEN. MARTINEZ]
3. Use procedural cues to detect speaker transitions:
   - "I now recognize..." or "The chair recognizes..." (Chair speaking)
   - "I yield back" or "I yield to..." (Current speaker finishing)
   - "Thank you Mr. Chairman" or "Thank you Madam Chair" (New speaker starting)
   - "Without objection" (Chair speaking)
4. Use committee member titles: CHAIRMAN/CHAIRWOMAN, RANKING MEMBER, REP./SEN., or just last name for repeated speakers
5. For witnesses, use format: [WITNESS: Name] or [Dr./Mr./Ms. Last Name]
6. Preserve the flow and content - only add labels and fix errors
7. If you cannot determine the speaker with confidence, use [SPEAKER] or [UNKNOWN]

Raw captions:{chunk_info}

{raw_text}

Provide the cleaned and diarized transcript:"""

    return prompt


def _build_cleanup_prompt(
    raw_text: str,
    hearing_title: str = "",
    committee_name: str = "",
    chunk_index: int = 0,
    total_chunks: int = 1,
) -> str:
    """Build the prompt for cleanup-only (no diarization).

    Used for C-SPAN and ISVP transcripts that already have speaker labels.
    Fixes capitalization, punctuation, transcription errors, and formatting
    artifacts while preserving existing speaker labels exactly as-is.

    Args:
        raw_text: Raw transcript text to clean up
        hearing_title: Title of the hearing (optional)
        committee_name: Name of the committee (optional)
        chunk_index: Current chunk index (for multi-chunk processing)
        total_chunks: Total number of chunks

    Returns:
        Formatted prompt string
    """
    context = []
    if hearing_title:
        context.append(f"Hearing: {hearing_title}")
    if committee_name:
        context.append(f"Committee: {committee_name}")

    context_str = "\n".join(context) if context else "Congressional hearing"

    chunk_info = ""
    if total_chunks > 1:
        chunk_info = f"\n\nNote: This is chunk {chunk_index + 1} of {total_chunks}. Maintain consistency across chunks."

    prompt = f"""You are cleaning up a congressional hearing transcript. The transcript already has speaker labels — your job is ONLY to fix text quality issues.

{context_str}

Instructions:
1. Fix capitalization errors (e.g. ALL CAPS text should be properly recased to standard sentence case)
2. Fix punctuation — add missing periods, commas, question marks where appropriate
3. Fix obvious transcription errors and misspellings
4. Clean up stutters, repeated words/phrases, and rolling caption artifacts (e.g. "the the economy" → "the economy")
5. PRESERVE all existing speaker labels exactly as they appear — do NOT add, remove, rename, or reformat any speaker labels
6. PRESERVE the overall structure and paragraph breaks
7. Do NOT add any new content or commentary — only fix errors in the existing text

Raw transcript:{chunk_info}

{raw_text}

Provide the cleaned transcript:"""

    return prompt


def _estimate_tokens(text: str) -> int:
    """Estimate token count (rough approximation: 1 token ≈ 4 characters).

    Args:
        text: Text to estimate tokens for

    Returns:
        Estimated token count
    """
    return len(text) // 4


def _split_into_chunks(
    text: str,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    overlap: int = DEFAULT_OVERLAP,
) -> list[str]:
    """Split text into chunks on paragraph boundaries with overlap.

    Args:
        text: Text to split
        chunk_size: Target chunk size in tokens
        overlap: Number of tokens to overlap between chunks

    Returns:
        List of text chunks
    """
    # Split on double newlines (paragraph boundaries)
    paragraphs = text.split("\n\n")

    chunks = []
    current_chunk = []
    current_size = 0

    for para in paragraphs:
        para_size = _estimate_tokens(para)

        # If adding this paragraph exceeds chunk size, save current chunk
        if current_size + para_size > chunk_size and current_chunk:
            chunks.append("\n\n".join(current_chunk))

            # Keep last few paragraphs for overlap
            overlap_size = 0
            overlap_paras = []
            for p in reversed(current_chunk):
                p_size = _estimate_tokens(p)
                if overlap_size + p_size > overlap:
                    break
                overlap_paras.insert(0, p)
                overlap_size += p_size

            current_chunk = overlap_paras
            current_size = overlap_size

        current_chunk.append(para)
        current_size += para_size

    # Add remaining chunk
    if current_chunk:
        chunks.append("\n\n".join(current_chunk))

    return chunks if chunks else [text]


def _calculate_cost(
    model: str,
    input_tokens: int,
    output_tokens: int,
) -> float:
    """Calculate cost in USD for API call.

    Args:
        model: Model identifier
        input_tokens: Number of input tokens
        output_tokens: Number of output tokens

    Returns:
        Cost in USD
    """
    if model not in config.MODEL_PRICING:
        logger.warning(f"Unknown model {model}, cannot calculate cost")
        return 0.0

    input_price, output_price = config.MODEL_PRICING[model]

    # Prices are per 1M tokens
    input_cost = (input_tokens / 1_000_000) * input_price
    output_cost = (output_tokens / 1_000_000) * output_price

    return input_cost + output_cost


def _call_openrouter(
    prompt: str,
    model: str,
    api_key: str,
    timeout: float = 120.0,
) -> dict[str, Any]:
    """Call OpenRouter API.

    Args:
        prompt: Prompt to send
        model: Model identifier
        api_key: OpenRouter API key
        timeout: Request timeout in seconds

    Returns:
        API response as dict

    Raises:
        httpx.HTTPError: If API call fails
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/anthropics/claude-code",
    }

    payload = {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": prompt,
            }
        ],
    }

    with httpx.Client(timeout=timeout) as client:
        response = client.post(OPENROUTER_API_URL, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()


def cleanup_transcript(
    raw_text: str,
    hearing_title: str = "",
    committee_name: str = "",
    model: str | None = None,
    skip_diarization: bool = False,
) -> CleanupResult:
    """Clean up raw transcript text, optionally adding speaker diarization.

    Processes congressional hearing transcripts by:
    - Fixing punctuation, capitalization, and transcription errors
    - Adding speaker labels based on procedural cues (unless skip_diarization=True)
    - Chunking long transcripts with overlap for context

    Args:
        raw_text: Raw transcript text
        hearing_title: Title of the hearing (helps with context)
        committee_name: Name of the committee (helps with context)
        model: OpenRouter model to use
        skip_diarization: If True, only clean up text without adding speaker labels.
            Use for transcripts that already have speaker labels (C-SPAN, ISVP).

    Returns:
        CleanupResult with cleaned text and metadata

    Raises:
        ValueError: If API key is not configured
        httpx.HTTPError: If API calls fail
    """
    if model is None:
        model = config.CLEANUP_MODEL
    logger.info(f"Starting cleanup with model: {model}")

    # Get API key (lazy loading)
    api_key = _get_api_key()

    # Check if we need to chunk
    estimated_tokens = _estimate_tokens(raw_text)
    logger.info(f"Estimated tokens: {estimated_tokens}")

    if estimated_tokens > DEFAULT_CHUNK_SIZE:
        chunks = _split_into_chunks(raw_text)
        logger.info(f"Split into {len(chunks)} chunks")
    else:
        chunks = [raw_text]

    # Process each chunk
    cleaned_chunks = []
    total_input_tokens = 0
    total_output_tokens = 0

    for i, chunk in enumerate(chunks):
        logger.info(f"Processing chunk {i + 1}/{len(chunks)}")

        build_prompt = _build_cleanup_prompt if skip_diarization else _build_diarization_prompt
        prompt = build_prompt(
            chunk,
            hearing_title=hearing_title,
            committee_name=committee_name,
            chunk_index=i,
            total_chunks=len(chunks),
        )

        response = _call_openrouter(prompt, model, api_key)

        # Extract cleaned text
        cleaned_text = response["choices"][0]["message"]["content"]
        cleaned_chunks.append(cleaned_text)

        # Track token usage
        usage = response.get("usage", {})
        input_tokens = usage.get("prompt_tokens", 0)
        output_tokens = usage.get("completion_tokens", 0)

        total_input_tokens += input_tokens
        total_output_tokens += output_tokens

        logger.info(
            f"Chunk {i + 1}: {input_tokens} input tokens, "
            f"{output_tokens} output tokens"
        )

    # Combine chunks
    final_text = "\n\n".join(cleaned_chunks)

    # Calculate cost
    cost = _calculate_cost(model, total_input_tokens, total_output_tokens)

    logger.info(
        f"Cleanup complete: {len(chunks)} chunks, "
        f"{total_input_tokens} input tokens, "
        f"{total_output_tokens} output tokens, "
        f"${cost:.4f}"
    )

    return CleanupResult(
        text=final_text,
        model=model,
        input_tokens=total_input_tokens,
        output_tokens=total_output_tokens,
        cost_usd=cost,
        chunks_processed=len(chunks),
    )


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)

    sample_text = """
thank you mister chairman and i want to thank the witnesses for being here today
this is a critical issue for our national security and i look forward to hearing your testimony
doctor smith can you explain the impact of these regulations on small businesses
thank you ranking member jones yes the impact has been significant we estimate
that compliance costs have increased by forty percent over the past three years
i yield back mister chairman
the chair recognizes the gentleman from california mister garcia for five minutes
thank you mister chairman doctor smith following up on that point
"""

    result = cleanup_transcript(
        sample_text,
        hearing_title="Hearing on Regulatory Impact",
        committee_name="House Committee on Small Business",
    )

    print(f"\nCleaned transcript:\n{result.text}")
    print(f"\nModel: {result.model}")
    print(f"Chunks: {result.chunks_processed}")
    print(f"Tokens: {result.input_tokens} in, {result.output_tokens} out")
    print(f"Cost: ${result.cost_usd:.4f}")
