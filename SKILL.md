# hearing-transcripts

Congressional hearing transcript pipeline. Discovers, downloads, and transcribes House, Senate, and joint committee hearings.

## Capabilities

### Audio transcription
- Monitors committee YouTube channels for new hearing videos
- Downloads audio via yt-dlp
- Transcribes via STT API (OpenAI Whisper / GPT-4o Transcribe / TBD)
- Speaker diarization for important hearings

### PDF extraction
- Extracts witness testimony PDFs from committee websites (pre-hearing)
- Parses official GPO transcripts when they become available (post-hearing, weeks later)
- Handles both clean text PDFs and scanned/OCR documents

### Discovery
- Scrapes congress.gov weekly committee schedule
- Maps committees to YouTube channels for video discovery
- Polls GovInfo API for official GPO transcripts (CHRG collection)

## Usage

```bash
# TBD
python run.py --discover       # find new hearings
python run.py --transcribe     # download + transcribe pending
python run.py --ingest-pdfs    # grab witness testimony PDFs
```

## Output

Transcripts stored as structured JSON:
```json
{
  "hearing_id": "...",
  "committee": "House Judiciary",
  "date": "2026-02-05",
  "title": "...",
  "source": "youtube-whisper|govinfo|committee-pdf",
  "speakers": [...],
  "transcript": [
    {"speaker": "Chair Smith", "text": "...", "timestamp": "00:01:23"},
    ...
  ]
}
```

## Dependencies

- yt-dlp (audio download)
- openai (Whisper/GPT-4o Transcribe API)
- pymupdf / pdfplumber (PDF extraction)
- requests / httpx (API calls, scraping)

## Config

API keys and settings in `.env`:
```
OPENAI_API_KEY=...
GOVINFO_API_KEY=...
```
