"""Shared LLM helpers for OpenRouter API calls, cost calculation, and text chunking."""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx

import config

log = logging.getLogger(__name__)

# Default chunking parameters
DEFAULT_CHUNK_SIZE = 3000  # tokens (approximate)
DEFAULT_OVERLAP = 200  # tokens for context continuity

# OpenRouter API endpoint
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"


def get_api_key() -> str:
    """Get OpenRouter API key from environment (loaded by config.py via load_dotenv).

    Returns:
        API key string

    Raises:
        ValueError: If API key cannot be found
    """
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if api_key:
        return api_key

    raise ValueError(
        "OPENROUTER_API_KEY not found. Set the environment variable or add it to .env"
    )


def estimate_tokens(text: str) -> int:
    """Estimate token count (rough approximation: 1 token ~ 4 characters).

    Args:
        text: Text to estimate tokens for

    Returns:
        Estimated token count
    """
    return len(text) // 4


def split_into_chunks(
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
        para_size = estimate_tokens(para)

        # If adding this paragraph exceeds chunk size, save current chunk
        if current_size + para_size > chunk_size and current_chunk:
            chunks.append("\n\n".join(current_chunk))

            # Keep last few paragraphs for overlap
            overlap_size = 0
            overlap_paras = []
            for p in reversed(current_chunk):
                p_size = estimate_tokens(p)
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


def calculate_cost(
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
        raise ValueError(f"Unknown model {model!r}, cannot calculate cost")

    input_price, output_price = config.MODEL_PRICING[model]

    # Prices are per 1M tokens
    input_cost = (input_tokens / 1_000_000) * input_price
    output_cost = (output_tokens / 1_000_000) * output_price

    return input_cost + output_cost


def call_openrouter(
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
