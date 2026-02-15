"""Download audio from YouTube and transcribe via captions + optional LLM cleanup."""

from __future__ import annotations

import logging
import os
import re
import subprocess
import tempfile
from pathlib import Path

import httpx

import config
from utils import YT_DLP_ENV

log = logging.getLogger(__name__)


def get_youtube_captions(youtube_url: str, output_dir: Path) -> Path | None:
    """Download YouTube auto-generated captions as a text file. Free, instant."""
    captions_file = output_dir / "captions.txt"
    try:
        result = subprocess.run(
            [
                "yt-dlp",
                "--remote-components", "ejs:github",
                "--write-auto-sub",
                "--sub-lang", "en",
                "--skip-download",
                "--sub-format", "vtt",
                "-o", str(output_dir / "captions"),
                youtube_url,
            ],
            capture_output=True,
            text=True,
            timeout=120,
            env=YT_DLP_ENV,
        )
        if result.returncode != 0:
            log.warning("yt-dlp failed (exit %d): %s", result.returncode, result.stderr[:500] if result.stderr else "no stderr")
            return None
        # Find the downloaded VTT file
        vtt_files = list(output_dir.glob("captions*.vtt"))
        if not vtt_files:
            log.info("No captions available for %s", youtube_url)
            return None

        # Convert VTT to plain text
        raw = vtt_files[0].read_text()
        text = _vtt_to_text(raw)
        tmp_fd, tmp_path = tempfile.mkstemp(dir=output_dir, suffix='.tmp')
        try:
            with os.fdopen(tmp_fd, 'w') as f:
                f.write(text)
            os.replace(tmp_path, captions_file)
        except Exception:
            os.unlink(tmp_path)
            raise
        # Clean up VTT
        for f in vtt_files:
            f.unlink()
        log.info("Captions saved: %s (%d chars)", captions_file, len(text))
        return captions_file

    except FileNotFoundError as e:
        log.error("CRITICAL: yt-dlp not found! Install it or check PATH. %s", e)
        raise
    except (subprocess.SubprocessError, OSError, ValueError) as e:
        log.error("Caption download FAILED for %s: %s", youtube_url, e)
        return None


def _vtt_to_text(vtt_content: str) -> str:
    """Strip VTT formatting to plain text, deduplicating adjacent repeated lines.

    Uses adjacent-line dedup instead of global set to avoid dropping legitimate
    repeated phrases like "Yes", "Thank you", "I agree" that appear throughout
    multi-hour hearings.
    """
    lines: list[str] = []
    prev: str | None = None
    for line in vtt_content.splitlines():
        # Skip VTT headers, timestamps, positioning
        if line.startswith("WEBVTT") or line.startswith("Kind:") or line.startswith("Language:"):
            continue
        if "-->" in line:
            continue
        if not line.strip():
            continue
        # Strip HTML tags (e.g. <c>, </c>, <00:01:23.456>)
        clean = re.sub(r"<[^>]+>", "", line).strip()
        if clean and clean != prev:
            lines.append(clean)
            prev = clean
    return "\n".join(lines)


def download_audio(youtube_url: str, output_dir: Path) -> Path | None:
    """Download audio from YouTube via yt-dlp as mp3."""
    audio_path = output_dir / "audio.mp3"
    try:
        subprocess.run(
            [
                "yt-dlp",
                "--remote-components", "ejs:github",
                "-x",
                "--audio-format", "mp3",
                "--audio-quality", "5",
                "-o", str(output_dir / "audio.%(ext)s"),
                youtube_url,
            ],
            capture_output=True,
            text=True,
            timeout=600,
            env=YT_DLP_ENV,
        )
        if audio_path.exists():
            size_mb = audio_path.stat().st_size / (1024 * 1024)
            log.info("Audio downloaded: %.1f MB", size_mb)
            return audio_path
        # yt-dlp might produce a different extension then convert
        mp3s = list(output_dir.glob("audio.mp3"))
        if mp3s:
            return mp3s[0]
        log.warning("No audio file produced for %s", youtube_url)
        return None
    except (subprocess.SubprocessError, OSError) as e:
        log.warning("Audio download failed: %s", e)
        return None


def transcribe_audio(audio_path: Path) -> str | None:
    """Transcribe audio file via OpenAI Whisper API. Handles chunking for large files."""
    if not config.get_openai_api_key():
        log.warning("No OPENAI_API_KEY set, skipping transcription")
        return None
    if config.TRANSCRIPTION_BACKEND == "captions-only":
        log.info("Backend is captions-only, skipping API transcription")
        return None

    from openai import OpenAI
    client = OpenAI(api_key=config.get_openai_api_key())
    file_size = audio_path.stat().st_size

    if file_size <= config.OPENAI_MAX_FILE_BYTES:
        return _transcribe_single(client, audio_path)

    # Need to chunk the file
    log.info("File is %.1f MB, chunking for API...", file_size / 1024 / 1024)
    chunks = _split_audio(audio_path)
    if not chunks:
        log.warning("Failed to split audio")
        return None

    all_text = []
    for i, chunk_path in enumerate(chunks):
        log.info("Transcribing chunk %d/%d...", i + 1, len(chunks))
        text = _transcribe_single(client, chunk_path)
        if text:
            all_text.append(text)
        chunk_path.unlink()

    return "\n\n".join(all_text) if all_text else None


def _transcribe_single(client, audio_path: Path) -> str | None:
    """Transcribe a single audio file via OpenAI."""
    import openai

    try:
        try:
            result = client.audio.transcriptions.create(
                model=config.TRANSCRIPTION_MODEL,
                file=audio_path,
                response_format="verbose_json",
                include=["logprobs"],
            )
            if hasattr(result, "segments"):
                lines = []
                for seg in result.segments:
                    speaker = getattr(seg, "speaker", None)
                    text = seg.text.strip()
                    if speaker:
                        lines.append(f"[{speaker}] {text}")
                    else:
                        lines.append(text)
                return "\n".join(lines)
            return result.text
        except openai.APIError as e:
            log.info("%s failed (%s), falling back to whisper-1", config.TRANSCRIPTION_MODEL, e)
            result = client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_path,
            )
            return result.text
    except openai.APIError as e:
        log.error("Transcription failed: %s", e)
        return None


def _split_audio(audio_path: Path, chunk_seconds: int = 1200) -> list[Path]:
    """Split audio into chunks using ffmpeg. Default 20-minute chunks."""
    chunk_dir = audio_path.parent / "chunks"
    chunk_dir.mkdir(exist_ok=True)
    try:
        subprocess.run(
            [
                "ffmpeg", "-i", str(audio_path),
                "-f", "segment",
                "-segment_time", str(chunk_seconds),
                "-c", "copy",
                "-reset_timestamps", "1",
                str(chunk_dir / "chunk_%03d.mp3"),
            ],
            capture_output=True,
            timeout=300,
        )
    except (subprocess.SubprocessError, OSError) as e:
        log.error("ffmpeg split failed: %s", e)
        return []
    chunks = sorted(chunk_dir.glob("chunk_*.mp3"))
    log.info("Split into %d chunks", len(chunks))
    return chunks


def process_hearing_audio(
    youtube_url: str,
    output_dir: Path,
    hearing_title: str = "",
    committee_name: str = "",
) -> dict:
    """Full pipeline: captions + optional LLM cleanup + optional Whisper transcription.

    Returns dict with keys: captions, cleaned_transcript, whisper_transcript,
    cleanup_cost_usd, cleanup_model.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    result: dict = {
        "captions": None,
        "cleaned_transcript": None,
        "whisper_transcript": None,
    }

    # Always grab captions first (free, fast)
    captions_path = get_youtube_captions(youtube_url, output_dir)
    if captions_path:
        result["captions"] = str(captions_path)

        # LLM cleanup of captions if configured
        if config.CLEANUP_MODEL:
            try:
                from cleanup import cleanup_transcript
                raw_text = captions_path.read_text()
                cleanup_result = cleanup_transcript(
                    raw_text,
                    hearing_title=hearing_title,
                    committee_name=committee_name,
                    model=config.CLEANUP_MODEL,
                )
                cleaned_path = output_dir / "transcript_cleaned.txt"
                cleaned_path.write_text(cleanup_result.text)
                result["cleaned_transcript"] = str(cleaned_path)
                result["cleanup_cost_usd"] = cleanup_result.cost_usd
                result["cleanup_model"] = cleanup_result.model
                log.info(
                    "LLM cleanup: %d chars, $%.4f (%s)",
                    len(cleanup_result.text), cleanup_result.cost_usd, cleanup_result.model,
                )
            except (httpx.HTTPError, KeyError, ValueError, OSError) as e:
                log.error("LLM cleanup FAILED for '%s': %s", hearing_title[:60], e)

    # Download and transcribe audio only if explicitly configured for Whisper
    if config.TRANSCRIPTION_BACKEND != "captions-only":
        audio = download_audio(youtube_url, output_dir)
        if audio:
            # Estimate whisper cost from file size (~1 MB per minute of mp3 at quality 5)
            size_mb = audio.stat().st_size / (1024 * 1024)
            estimated_minutes = size_mb  # rough: 1 MB ≈ 1 min for mp3
            result["whisper_cost_usd"] = estimated_minutes * config.WHISPER_COST_PER_MINUTE

            text = transcribe_audio(audio)
            if text:
                transcript_path = output_dir / "transcript_whisper.txt"
                transcript_path.write_text(text)
                result["whisper_transcript"] = str(transcript_path)
            audio.unlink(missing_ok=True)

    return result
