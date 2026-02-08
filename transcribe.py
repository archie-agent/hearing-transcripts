"""Download audio from YouTube and transcribe via OpenAI or captions."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
from pathlib import Path

from openai import OpenAI

import config

log = logging.getLogger(__name__)

# yt-dlp needs deno on PATH for JS challenge solving
_DENO_DIR = os.path.expanduser("~/.deno/bin")
_YT_DLP_ENV = {**os.environ, "PATH": f"{_DENO_DIR}:{os.environ.get('PATH', '')}"}


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
            env=_YT_DLP_ENV,
        )
        # Find the downloaded VTT file
        vtt_files = list(output_dir.glob("captions*.vtt"))
        if not vtt_files:
            log.info("No captions available for %s", youtube_url)
            return None

        # Convert VTT to plain text
        raw = vtt_files[0].read_text()
        text = _vtt_to_text(raw)
        captions_file.write_text(text)
        # Clean up VTT
        for f in vtt_files:
            f.unlink()
        log.info("Captions saved: %s (%d chars)", captions_file, len(text))
        return captions_file

    except Exception as e:
        log.warning("Caption download failed for %s: %s", youtube_url, e)
        return None


def _vtt_to_text(vtt_content: str) -> str:
    """Strip VTT formatting to plain text, deduplicating repeated lines."""
    lines = []
    seen = set()
    for line in vtt_content.splitlines():
        # Skip VTT headers, timestamps, positioning
        if line.startswith("WEBVTT") or line.startswith("Kind:") or line.startswith("Language:"):
            continue
        if "-->" in line:
            continue
        if not line.strip():
            continue
        # Strip HTML tags
        import re
        clean = re.sub(r"<[^>]+>", "", line).strip()
        if clean and clean not in seen:
            seen.add(clean)
            lines.append(clean)
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
                "--audio-quality", "5",  # decent quality, smaller files
                "-o", str(output_dir / "audio.%(ext)s"),
                youtube_url,
            ],
            capture_output=True,
            text=True,
            timeout=600,
            env=_YT_DLP_ENV,
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
    except Exception as e:
        log.warning("Audio download failed: %s", e)
        return None


def transcribe_audio(audio_path: Path, output_dir: Path) -> Path | None:
    """Transcribe audio file via OpenAI API. Handles chunking for large files."""
    if not config.OPENAI_API_KEY:
        log.warning("No OPENAI_API_KEY set, skipping transcription")
        return None

    file_size = audio_path.stat().st_size
    transcript_path = output_dir / "transcript.txt"

    if config.TRANSCRIPTION_BACKEND == "captions-only":
        log.info("Backend is captions-only, skipping API transcription")
        return None

    client = OpenAI(api_key=config.OPENAI_API_KEY)

    if file_size <= config.OPENAI_MAX_FILE_BYTES:
        # Single file, no chunking needed
        text = _transcribe_single(client, audio_path)
        if text:
            transcript_path.write_text(text)
            return transcript_path
        return None

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
        chunk_path.unlink()  # Clean up chunk

    if all_text:
        transcript_path.write_text("\n\n".join(all_text))
        log.info("Transcript saved: %d chars from %d chunks", len(transcript_path.read_text()), len(chunks))
        return transcript_path
    return None


def _transcribe_single(client: OpenAI, audio_path: Path) -> str | None:
    """Transcribe a single audio file via OpenAI."""
    try:
        if config.TRANSCRIPTION_BACKEND == "openai":
            # Use GPT-4o transcribe with diarization if available
            try:
                result = client.audio.transcriptions.create(
                    model="gpt-4o-transcribe",
                    file=audio_path,
                    response_format="verbose_json",
                    include=["logprobs"],
                )
                # Try to format with speaker labels if present
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
            except Exception:
                # Fallback to whisper-1
                log.info("Falling back to whisper-1")
                result = client.audio.transcriptions.create(
                    model="whisper-1",
                    file=audio_path,
                )
                return result.text
        else:
            # openai-mini or whisper-1
            model = "whisper-1"
            result = client.audio.transcriptions.create(
                model=model,
                file=audio_path,
            )
            return result.text
    except Exception as e:
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
    except Exception as e:
        log.error("ffmpeg split failed: %s", e)
        return []

    chunks = sorted(chunk_dir.glob("chunk_*.mp3"))
    log.info("Split into %d chunks", len(chunks))
    return chunks


def process_hearing_audio(youtube_url: str, output_dir: Path) -> dict:
    """Full pipeline: captions + audio download + transcription."""
    output_dir.mkdir(parents=True, exist_ok=True)
    result = {"captions": None, "transcript": None}

    # Always grab captions first (free, fast)
    captions = get_youtube_captions(youtube_url, output_dir)
    if captions:
        result["captions"] = str(captions)

    # Download and transcribe audio if configured
    if config.TRANSCRIPTION_BACKEND != "captions-only":
        audio = download_audio(youtube_url, output_dir)
        if audio:
            transcript = transcribe_audio(audio, output_dir)
            if transcript:
                result["transcript"] = str(transcript)
            # Clean up audio file to save disk
            audio.unlink(missing_ok=True)

    return result
