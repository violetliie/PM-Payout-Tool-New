# Polymarket Creator Payout Tool

Automates payout calculations for Polymarket's short-form video campaigns across TikTok and Instagram.

---

## What It Does

1. Fetches video performance data from the **Shortimize API** for a given date range
2. Maps each video to a creator using a **Google Sheet** handle mapping
3. Matches TikTok and Instagram videos for the same creator (cross-platform pairing)
4. Applies a **tiered payout formula** per video based on view count
5. Generates a structured **.xlsx report** with 3 tabs: Creator Summary, Video Audit, Exceptions

---

## Project Structure

```
Payout Tool/
  backend/
    main.py                       # FastAPI app — POST /api/calculate, GET /api/download
    config.py                     # Environment variables
    models/
      schemas.py                  # Pydantic models (Video, PayoutUnit, CreatorSummary, etc.)
    services/
      shortimize.py               # Shortimize API client (Steps 1–4)
      creator_mapping.py          # Google Sheet ingestion (handle → creator name)
      matcher.py                  # Cross-platform matching (Steps 5–11)
      payout.py                   # Payout tier calculation (Steps A–D)
      excel_export.py             # 3-tab .xlsx report generation
    tests/
      test_matcher.py             # 96 matcher tests
      test_payout.py              # 110 payout tier tests
      test_excel_export.py        # 52 Excel export tests
      test_main.py                # 23 API endpoint tests
      test_full_pipeline.py       # 49 end-to-end pipeline tests (10 creators, 32 videos)
      test_adversarial.py         # 59 adversarial edge-case tests
    requirements.txt
  frontend/
    src/
      App.jsx                     # Main app component
      components/                 # Header, DateRangePicker, ResultsCard, ErrorCard, EmptyState
      services/api.js             # Backend API calls
    index.html
    package.json
    vite.config.js
  SPEC.md                         # Full specification document
  .gitignore
```

---

## Setup

### Prerequisites

- Python 3.11+
- Node.js 18+
- npm or yarn

### Backend

```bash
cd backend

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # macOS/Linux

# Install dependencies
pip install -r requirements.txt

# Create .env file (never commit this)
cat > .env << 'EOF'
SHORTIMIZE_API_KEY=your_api_key_here
SHORTIMIZE_BASE_URL=https://api.shortimize.com
CREATOR_SHEET_CSV_URL=https://docs.google.com/spreadsheets/d/e/2PACX-1vTQcA8MAAhZ4urj_91M7rq80UwsmR3XePus2j2Ky-iZD_j_YSC5U5-kdSf2P1E73fohaAZWqJ6a4i2w/pub?output=csv&gid=651686011
OUTPUT_DIR=/tmp/payout_reports
EOF

# Run the server
uvicorn main:app --reload --port 8000
```

### Frontend

```bash
cd frontend

# Install dependencies
npm install

# Start dev server (proxies API to backend on port 8000)
npm run dev
```

The frontend runs on `http://localhost:5173` and proxies `/api` requests to the backend.

---

## Running Tests

```bash
cd backend

# Install test dependencies
pip install pytest

# Run all tests (389 tests)
python3 -m pytest tests/ -v

# Run specific test file
python3 -m pytest tests/test_matcher.py -v
python3 -m pytest tests/test_full_pipeline.py -v
python3 -m pytest tests/test_adversarial.py -v

# Run with short output
python3 -m pytest tests/ --tb=short
```

### Test Coverage

| File | Tests | What It Covers |
|------|-------|----------------|
| `test_matcher.py` | 96 | Sequence matching, fallback, dedup, edge cases |
| `test_payout.py` | 110 | All tier boundaries, cap, summaries, pipeline |
| `test_excel_export.py` | 52 | 3-tab structure, formatting, sorting, edge cases |
| `test_main.py` | 23 | API endpoints, error handling, pipeline wiring |
| `test_full_pipeline.py` | 49 | End-to-end with 10 creators and 32 videos |
| `test_adversarial.py` | 59 | Timezone bugs, dedup edge cases, double-booking, stress tests |
| **Total** | **389** | |

---

## API Endpoints

### `POST /api/calculate`

Run the payout calculation pipeline for a date range.

**Request:**
```json
{
  "start_date": "2026-02-20",
  "end_date": "2026-02-21"
}
```

**Response:**
```json
{
  "status": "success",
  "filename": "Polymarket Payout Summary 2026-02-20 to 2026-02-21.xlsx",
  "summary": {
    "total_creators": 15,
    "total_payout": 12500.0,
    "total_videos_processed": 120,
    "total_paired": 40,
    "total_unpaired": 10,
    "total_exceptions": 8
  }
}
```

### `GET /api/download/{filename}`

Download a generated `.xlsx` report.

---

## Matching Algorithm

1. **Sequence match (Step 9):** Pair TikTok #1 with Instagram #1, #2 with #2, etc. (sorted by `created_at`). Confirm with exact `video_length` match.
2. **Fallback match (Step 10):** If sequence pair fails length check, search for a match with exact length + same `uploaded_at` date + closest `created_at` within 24 hours.
3. **Unpaired (Step 11):** Remaining unmatched videos become standalone payout units.

---

## Payout Tiers

| Views | Payout |
|-------|--------|
| < 1,000 | $0 |
| 1,000 – 9,999 | $35 |
| 10,000 – 49,999 | $50 |
| 50,000 – 99,999 | $100 |
| 100,000 – 249,999 | $150 |
| 250,000 – 499,999 | $300 |
| 500,000 – 999,999 | $500 |
| 1,000,000 – 1,999,999 | $700 |
| 2,000,000 – 2,999,999 | $900 |
| 3,000,000 – 3,999,999 | $1,100 |
| 4,000,000 – 4,999,999 | $1,300 |
| 5,000,000 – 5,999,999 | $1,500 |
| 6,000,000 – 10,000,000 | $1,500 + $150 x (floor_millions - 5) |

Views are capped at 10,000,000. For paired videos, the higher view count is used.

---

## Deployment

### Backend (Railway / Render)

1. Set environment variables (`SHORTIMIZE_API_KEY`, `CREATOR_SHEET_CSV_URL`)
2. Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
3. Python 3.11+ runtime

### Frontend (Vercel)

1. Root directory: `frontend`
2. Build command: `npm run build`
3. Output directory: `dist`
4. Add environment variable for API URL if backend is on a different domain

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SHORTIMIZE_API_KEY` | Yes | Bearer token for Shortimize API |
| `SHORTIMIZE_BASE_URL` | No | API base URL (default: `https://api.shortimize.com`) |
| `CREATOR_SHEET_CSV_URL` | No | Google Sheet CSV export URL (has default) |
| `OUTPUT_DIR` | No | Directory for generated reports (default: `/tmp/payout_reports`) |
