# hearing-transcripts

Daily pipeline that discovers congressional hearing videos and testimony from economics-relevant committees, downloads captions, cleans them up with LLM diarization, and dumps structured text for downstream parsing.

## Usage

```bash
python run.py                       # full daily run
python run.py --discover-only       # just find new hearings
python run.py --days 3              # look back 3 days
python run.py --tier 1              # only core economics committees
python run.py --committee house.ways_and_means  # single committee
python run.py --max-cost 2.0        # cap LLM spend at $2
python run.py --workers 5           # parallel processing
python run.py --reprocess           # re-process already-done hearings
```

## Output

```
output/
├── index.json                      # manifest of all processed hearings
└── 2026-02-08/
    └── house-financial-services-fed-oversight/
        ├── meta.json               # committee, date, title, sources, cost
        ├── captions.txt            # raw YouTube auto-captions
        ├── transcript_cleaned.txt  # LLM-diarized transcript
        ├── transcript_whisper.txt  # (only if TRANSCRIPTION_BACKEND=openai)
        ├── govinfo_transcript.txt  # official GPO text (if available)
        └── testimony/
            ├── powell.txt          # extracted witness testimony PDFs
            └── yellen.txt
```

## Config

Set in environment or `.env`:
- `OPENAI_API_KEY` — for Whisper transcription (optional, captions-only by default)
- `OPENROUTER_API_KEY` — for LLM cleanup/diarization
- `GOVINFO_API_KEY` — for official GPO transcripts (uses DEMO_KEY if unset)
- `CLEANUP_MODEL` — OpenRouter model (default: `google/gemini-3-flash-preview`)
- `TRANSCRIPTION_BACKEND` — `captions-only` (default) or `openai`
- `MAX_COST_PER_RUN` — cost cap in USD (default: $5.00)
