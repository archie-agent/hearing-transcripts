# hearing-transcripts

Daily pipeline that scrapes congressional hearing audio and testimony PDFs from economics-relevant committees, transcribes them (with diarization where possible), and dumps raw text for downstream parsing.

## Usage

```bash
python run.py                  # full daily run: discover → download → transcribe → output
python run.py --discover-only  # just find new hearings, don't download
python run.py --days 3         # look back 3 days instead of default 1
```

## Output

One directory per hearing in `output/YYYY-MM-DD/`:
```
output/2026-02-08/house-financial-services-fed-oversight/
├── meta.json          # committee, date, title, sources, URLs
├── transcript.txt     # diarized audio transcript (if video found)
├── captions.txt       # YouTube auto-captions fallback
└── testimony/
    ├── powell.txt     # extracted witness testimony PDFs
    └── yellen.txt
```

## Config

Inherits `OPENAI_API_KEY` from `~/.env`. See `config.py` for committee list and settings.
