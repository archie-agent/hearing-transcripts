# hearing-transcripts

Congressional hearing transcript pipeline. Discovers, downloads, and transcribes House, Senate, and joint committee hearings.

## Capabilities

### Audio transcription
- Monitors committee YouTube channels for new hearing videos
- Downloads audio via yt-dlp
- Transcribes via STT API (whisper.cpp local for bulk, AssemblyAI/Soniox for diarized)
- Speaker diarization for important hearings
- Fallback: YouTube auto-captions (free, ~85% accuracy)

### PDF extraction
- Extracts witness testimony PDFs from committee websites (pre-hearing)
  - House: standardized at docs.house.gov (HHRG-format URLs)
  - Senate: per-committee scrapers (banking.senate.gov/download/..., etc.)
- Parses official GPO transcripts when they become available (post-hearing, weeks later)
- Text extraction via pymupdf4llm (handles multi-column GPO layouts)
- OCR fallback via Marker/Surya for scanned documents

### Discovery
- Scrapes congress.gov weekly committee schedule
- Maps committees to YouTube channels for video discovery
  - House: ~18 committees with confirmed YouTube channels
  - Senate: most use internal JW Player webcast (only Banking/Finance/Commerce on YT)
  - C-SPAN: selective coverage, supplement only
  - congress.gov/committees/video as unified aggregation point
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

- yt-dlp (audio download from YouTube)
- openai (Whisper/GPT-4o Transcribe API)
- assemblyai (diarized transcription for important hearings)
- pymupdf4llm (PDF text extraction — handles GPO multi-column layouts)
- pdfplumber (table extraction from PDFs)
- httpx / beautifulsoup4 / lxml (API calls, scraping)

## STT cost estimates

| Provider | $/hr | Diarization | Best for |
|----------|------|------------|----------|
| whisper.cpp (local) | $0 | No | Bulk transcription |
| Groq (free tier) | $0 | No | Quick tests |
| Soniox | $0.10 | Yes | Cheap + diarization |
| AssemblyAI | $0.15 | Yes (excellent) | High-quality diarized |
| OpenAI GPT-4o Mini | $0.18 | No | Cheap cloud |
| OpenAI GPT-4o Transcribe | $0.36 | Yes | Best quality |

Full 119th Congress (~1,500 hearings): ~$150-500 total depending on provider.

## Config

API keys and settings in `.env` (or inherit from `~/.env`):
```
OPENAI_API_KEY=...
GOVINFO_API_KEY=...        # free from api.data.gov
ASSEMBLYAI_API_KEY=...     # optional, for diarized transcription
```
