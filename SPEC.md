# Polymarket Creator Payout Tool — Full Specification

## Overview

A web-based tool that automates the calculation of creator payouts for Polymarket's short-form video campaigns. It pulls video performance data from the **Shortimize API**, matches videos across TikTok and Instagram for the same creator, applies a tiered payout formula **per video** based on views, and outputs a structured `.xlsx` report.

---

## Data Sources

### 1. Shortimize API

- **Base URL**: `https://api.shortimize.com`
- **Endpoint**: `GET /videos`
- **Auth Header**: `Authorization: Bearer <SHORTIMIZE_API_KEY>` (required)
- **Rate Limit**: 30 requests per minute (standard endpoints). Returns `429 Too Many Requests` when exceeded. Check `RateLimit-*` headers for limit status.

#### Query Parameters

| Param | Type | Description | Default |
|---|---|---|---|
| `page` | integer (min: 1) | Page number for pagination | 1 |
| `limit` | integer (min: 1, max: 20000) | Items per page | 20000 |
| `order_by` | string enum | Sort field. Values: `created_at`, `uploaded_at`, `latest_views`, `latest_likes`, `latest_comments`, `latest_bookmarks`, `latest_shares`, `latest_engagement`, `latest_updated_at` | `latest_updated_at` |
| `order_direction` | string enum | `asc` or `desc` | `desc` |
| `username` | string | Filter by specific username | — |
| `linked_account_id` | string (uuid) | Filter by linked account ID | — |
| `uploaded_at_start` | string (date) | Filter videos uploaded on or after this date (inclusive) | — |
| `uploaded_at_end` | string (date) | Filter videos uploaded on or before this date (inclusive) | — |
| `latest_updated_at_start` | string (date-time) | Filter by last updated on or after | — |
| `latest_updated_at_end` | string (date-time) | Filter by last updated on or before | — |
| `ad_info_shop` | boolean | Filter TikTok shop ads | — |
| `ad_product_id_shop` | boolean | Filter TikTok shop product ads | — |
| `has_metrics` | boolean | Only get videos with metrics (not null) | — |
| `collections` | string | Comma-separated collection names (URL encode) | — |

#### Response Schema (200)

```json
{
  "data": [
    {
      "organisation_id": "uuid",
      "ad_id": "uuid",
      "username": "string",
      "platform": "tiktok | instagram | youtube",
      "ad_link": "string (uri)",
      "created_at": "string (date-time)",
      "removed": false,
      "linked_account_id": "uuid",
      "uploaded_at": "string (date) | null",
      "song_name": "string | null",
      "song_link": "string | null",
      "video_length": "number | null (seconds)",
      "title": "string",
      "not_safe": false,
      "private": false,
      "hidden_stats": false,
      "latest_views": 0,
      "latest_likes": 0,
      "latest_comments": 0,
      "latest_bookmarks": 0,
      "latest_shares": 0,
      "latest_engagement": 0,
      "latest_updated_at": "string (date-time)",
      "outlier_multiplier": 0,
      "increase_1d": 0,
      "increase_7d": 0,
      "increase_14d": 0,
      "increase_30d": 0,
      "ad_info_shop": false,
      "ad_is_ad": false,
      "ad_product_id_shop": "string | null",
      "label_ids": ["uuid"] | null,
      "label_names": ["string"] | null
    }
  ],
  "pagination": {
    "total": 0,
    "page": 1,
    "limit": 20000,
    "total_pages": 1,
    "order_by": "latest_updated_at",
    "order_direction": "desc"
  },
  "filters": {
    "username": "string",
    "linked_account_id": "string",
    "uploaded_at_range": {
      "start": "date",
      "end": "date"
    },
    "ad_info_shop": false,
    "has_metrics": false,
    "collections": ["string"]
  }
}
```

#### Sample API Response (real data)

```json
{
  "organisation_id": "4d5b2d7b-660f-41e4-8506-021b7e7aa110",
  "ad_id": "2cd88fa1-46f9-447f-9901-ef9520779347",
  "username": "flow_bruce",
  "platform": "tiktok",
  "ad_link": "https://www.tiktok.com/@flow_bruce/video/7609364728032365845",
  "created_at": "2026-02-22T00:31:05.461255+00:00",
  "removed": false,
  "linked_account_id": "f4367436-a135-45c3-a337-9883ffbb9d05",
  "uploaded_at": "2026-02-21",
  "song_name": "original sound - flow_bruce",
  "song_link": "https://sf16-ies-music-sg.tiktokcdn.com/obj/tiktok-obj/7609364769379797776.mp3",
  "video_length": 13,
  "title": "Are aliens real?? #skit #alien #fyp ",
  "not_safe": false,
  "private": false,
  "hidden_stats": false,
  "ad_info_shop": false,
  "ad_product_id_shop": null,
  "ad_is_ad": false,
  "latest_views": 367,
  "latest_likes": 14,
  "latest_comments": 1,
  "latest_bookmarks": 0,
  "latest_shares": 1,
  "latest_engagement": 16,
  "latest_updated_at": "2026-02-22T00:31:06.859191+00:00",
  "outlier_multiplier": 0,
  "increase_1d": 0,
  "increase_7d": 0,
  "increase_14d": 0,
  "increase_30d": 0,
  "label_ids": [
    "79689376-d475-4e7e-8c96-7993b18d6e55",
    "8dd8b959-6f08-4a84-89d6-6610ac5a7c40"
  ]
}
```

**IMPORTANT NOTES on field types:**
- `video_length` comes as a number (integer) from the API, but the schema says `string | null`. Always parse to integer. Handle null.
- `uploaded_at` is a date string like `"2026-02-21"` (no time), can be null.
- `created_at` is a full datetime with timezone.
- `latest_views` is an integer.

---

### 2. Creator Mapping Google Sheet

- **Format**: Published-to-web Google Sheet (HTML format — must parse accordingly, OR convert URL to CSV export format)
- **URL**: `https://docs.google.com/spreadsheets/d/e/2PACX-1vTQcA8MAAhZ4urj_91M7rq80UwsmR3XePus2j2Ky-iZD_j_YSC5U5-kdSf2P1E73fohaAZWqJ6a4i2w/pubhtml?gid=651686011&single=true`
- **To get CSV format**, replace `pubhtml` with `pub` and add `&output=csv`:
  `https://docs.google.com/spreadsheets/d/e/2PACX-1vTQcA8MAAhZ4urj_91M7rq80UwsmR3XePus2j2Ky-iZD_j_YSC5U5-kdSf2P1E73fohaAZWqJ6a4i2w/pub?gid=651686011&single=true&output=csv`
- **Key columns**:
  - Column B (index 1): `creator_name` (the canonical name used for payouts)
  - Column P (index 15): `instagram_handle`
  - Column Q (index 16): `tiktok_handle`
- **Data starts at row 3** (rows 1-2 are headers/labels — skip them)
- **Purpose**: Maps platform-specific handles to a single creator identity

---

## Core Logic — Full Pipeline

### Step 1: Pull Videos from Shortimize API

Call `GET https://api.shortimize.com/videos` with:
- `uploaded_at_start` = user's selected start date
- `uploaded_at_end` = user's selected end date
- `order_by` = `created_at`
- `order_direction` = `asc`
- `has_metrics` = `true`
- `limit` = `20000` (max per page)

Handle pagination: check `pagination.total_pages`. If more than 1 page, loop through all pages.

Rate limit: 30 req/min. Implement a delay or backoff if hitting rate limits (check for 429 status).

### Step 2: Keep Only Needed Fields

From each API response object, extract:
- `username`, `platform`, `ad_link`, `uploaded_at`, `created_at`, `video_length`, `latest_views`, `latest_updated_at`, `linked_account_id`, `ad_id`, `title`, `private`, `removed`

Ignore all other fields (likes, comments, shares, engagement, song info, etc.)

### Step 3: Standardize Data

For each video:
- Normalize `platform` to lowercase (must be exactly `tiktok` or `instagram`). **Skip `youtube` videos entirely.**
- Ensure `latest_views` is numeric (integer). Default to 0 if null.
- Ensure `video_length` is numeric (integer). Handle null (flag as exception if null).
- Parse `uploaded_at` as date.
- Parse `created_at` as datetime.
- Parse `latest_updated_at` as datetime.

### Step 4: Filter Out Invalid Videos

Remove (and send to Exceptions tab) videos where:
- `private == true` → reason: "video marked private"
- `removed == true` → reason: "video removed"
- `video_length` is null/missing → reason: "missing video length"
- `latest_views` is null → reason: "missing view data"

### Step 5: Map Each Video to a Creator

Using the creator mapping sheet:
- If `platform == tiktok`, match `username` (lowercase, stripped) to `tiktok_handle` (Column Q)
- If `platform == instagram`, match `username` (lowercase, stripped) to `instagram_handle` (Column P)

Add `creator_name` from the mapping sheet to each video.

If no match found:
- Put into Exceptions tab with reason: "not in creator list"
- Exclude from payout

### Step 6: Remove Duplicate Video Rows

Deduplicate using:
- Primary: `ad_link`
- Fallback: `ad_id`

If duplicates exist, keep the row with the most recent `latest_updated_at`.

### Step 7: Group by Creator (Not by Date)

For each `creator_name`, split videos into:
- TikTok list
- Instagram list

### Step 8: Sort Each Platform List by `created_at`

Within each creator:
- Sort TikTok videos by `created_at` ascending
- Sort Instagram videos by `created_at` ascending

This creates a stable posting sequence across the entire pay period.

### Step 9: Primary Match — Sequence + Exact Length + First Frame Confirmation

Try to pair by position in the sorted sequence:
- TikTok #1 ↔ Instagram #1
- TikTok #2 ↔ Instagram #2
- TikTok #3 ↔ Instagram #3
- etc.

Then **confirm** each pair using two checks:

**Check 1 — Exact `video_length` match (required)**:
- Lengths must be exactly equal (no tolerance). If different → reject pair, both go to unmatched pool for Step 10.

**Check 2 — First frame perceptual hash (required)**:
- Extract first frame from both videos using `yt-dlp` (download video) + `ffmpeg` (extract frame 0)
- Compute perceptual hash (`imagehash.phash`) on both frames
- If hamming distance ≤ 10 → confirmed match, pair is accepted
- If hamming distance > 10 → reject pair, both go to unmatched pool for Step 10
- Delete temp video files immediately after frame extraction

If one platform has more videos than the other (e.g., 5 TikToks, 3 Instagrams):
- Pair #1↔#1, #2↔#2, #3↔#3 (with length + phash confirmation for each)
- TikTok #4 and #5 go to unmatched pool

### Step 10: Fallback — Unmatched Pool Matching

After Step 9, collect all unmatched videos (rejected pairs + extras) for each creator.

For each unmatched video on one platform, search for a match on the other platform:
1. **Exact `video_length` match** (required) — find all candidates on the other platform with the same length
2. **First frame perceptual hash confirmation** (required) — for each length-matched candidate, extract first frames and compare phash
3. If phash distance ≤ 10 → matched pair (treated identically to Step 9 matches)
4. If multiple candidates match length, use the one with the lowest phash distance

If no match is found → video stays unpaired.

**IMPORTANT**: A video that was already matched (in Step 9 or earlier in Step 10) must NOT be re-used. Mark matched videos as "used" immediately.

### Step 11: Handle Unmatched Videos

After Steps 9 and 10, any video that remains unpaired:
- **Does NOT receive payout** ($0)
- Goes to Exceptions tab (Tab 3) with reason: "unpaired — no cross-platform match found"
- Does NOT appear in the Video Audit tab (Tab 2)

Only successfully paired videos are eligible for payout.

---

## Views Selection Logic

For each **matched pair**:
- `chosen_views = max(tiktok_latest_views, instagram_latest_views)`
- Store which platform had the higher views (for audit)

Unpaired videos do not receive payout and are excluded from views selection.

---

## Payout Logic (Per Video)

**CRITICAL: Payout is calculated PER VIDEO (per payout unit — a matched pair or an unpaired video), then summed per creator.**

### Step A: Qualification Threshold

If `chosen_views < 1,000`:
- `payout = $0`
- Mark as "not qualified"

### Step B: Apply 10M View Cap

If `chosen_views > 10,000,000`:
- Use `effective_views = 10,000,000` for payout calculation
- Keep original `chosen_views` in the audit for transparency
- Mark as "capped at 10M"

Otherwise:
- `effective_views = chosen_views`

### Step C: Apply Payout Tiers (per video)

Use `effective_views` to determine payout for each video:

| Effective Views | Payout per Video |
|---|---|
| < 1,000 | $0 (not qualified) |
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
| 6,000,000 – 10,000,000 | $1,500 + $150 × (floor_millions − 5) |

Where `floor_millions = floor(effective_views / 1,000,000)`.

**There is NO tier above 10M. Views are always capped at 10M before applying the formula.**

**Payout examples:**
- 500 views → $0 (not qualified)
- 2,500 views → $35
- 35,000 views → $50
- 75,000 views → $100
- 180,000 views → $150
- 400,000 views → $300
- 800,000 views → $500
- 1,500,000 views → $700
- 2,500,000 views → $900
- 6,700,000 views → floor_millions=6 → $1,500 + $150×(6−5) = $1,650
- 9,200,000 views → floor_millions=9 → $1,500 + $150×(9−5) = $2,100
- 10,000,000 views → floor_millions=10 → $1,500 + $150×(10−5) = $2,250
- 12,000,000 views → **capped to 10M** → floor_millions=10 → $2,250

### Step D: Sum Per Creator

For each creator:
- `total_payout = sum of all individual video payouts` (paired videos only)
- `qualified_video_count = count of paired payout units with chosen_views >= 1,000`
- `paired_video_count = number of pairs` (1 pair = count as 1, not 2)
- `exception_count = number of exception videos for this creator` (includes unpaired)

---

## Output: Excel File (.xlsx)

**File name**: `Polymarket Payout Summary yyyy-mm-dd to yyyy-mm-dd.xlsx`

### Tab 1: Creator Payout Summary

One row per creator:

| Column | Description |
|---|---|
| Creator Name | From the mapping sheet |
| Qualified Video Count | Count of **paired payout units** with `chosen_views >= 1,000`. Each matched pair = 1 payout unit. |
| Total Payout | Sum of all per-video payouts for that creator in this period |
| Paired Video Count | Number of **pairs** (not individual videos). 1 matched TikTok+Instagram pair = 1. |
| Exception Count | Count of exception videos for this creator (unpaired + other exceptions) |

Sort by Total Payout descending. Format payout as currency, views with comma separators. Bold header row. Auto-fit column widths.

### Tab 2: Video Audit (one row per payout unit)

One row per **payout unit** — each matched pair is ONE row with both platforms' data side by side. Only paired videos appear here.

| Column | Description |
|---|---|
| Creator Name | Canonical name |
| Uploaded At | `uploaded_at` value |
| Video Length (sec) | Duration in seconds |
| TikTok Link | `ad_link` for TikTok video |
| TikTok Views | `latest_views` for TikTok |
| Instagram Link | `ad_link` for Instagram video |
| Instagram Views | `latest_views` for Instagram |
| Chosen Views | `max(tiktok_views, instagram_views)` |
| Effective Views | After 10M cap (same as chosen_views if under 10M) |
| Payout Amount | Per-video payout from tier table |
| Match Method | "sequence" (Step 9) or "fallback" (Step 10) |
| Match Notes | e.g., "sequence match", "fallback match: same length, phash confirmed" |
| Latest Updated At | For freshness reference |

Sort by Creator Name, then Uploaded At.

### Tab 3: Exceptions / Manual Review

| Column | Description |
|---|---|
| Username | The platform handle |
| Platform | `tiktok` or `instagram` |
| Video Link | `ad_link` |
| Created At | Creation timestamp |
| Latest Views | View count |
| Video Length (sec) | Duration |
| Reason | Why it's an exception |

**Exception reasons include:**
- "not in creator list" — handle not found in mapping file
- "video marked private"
- "video removed"
- "missing video length"
- "missing view data"
- "unpaired — no cross-platform match found"
- "first frame extraction failed"

---

## Frontend UI

A simple, clean web interface with **Polymarket theme** (white background, clean typography):

1. **Date Range Picker** — select start and end dates
2. **"Calculate Payouts" Button** — triggers the backend pipeline
3. **Progress Indicator** — show status while processing (fetching data, matching, calculating...)
4. **Download Button** — download the generated `.xlsx` file

---

## Tech Stack

| Layer | Technology | Reason |
|---|---|---|
| Backend | Python + FastAPI | pandas for data manipulation, openpyxl for Excel |
| Frontend | React + Vite | Simple, fast UI with date pickers |
| HTTP Client | `httpx` or `requests` | Shortimize API calls |
| Excel Generation | `openpyxl` | Multi-tab .xlsx creation |
| Sheet Ingestion | `pandas.read_csv()` or `pandas.read_html()` | Read the published Google Sheet |
| Video Processing | `yt-dlp` + `ffmpeg` | Download videos and extract first frame for matching |
| Image Comparison | `imagehash` + `Pillow` | Perceptual hash (phash) for first frame comparison |
| Deployment | Vercel (frontend) + Railway/Render (backend) | Easy hosting |

### First Frame Extraction Process

For each video that needs matching:
1. Use `yt-dlp` to download the video from `ad_link` to a temp file
2. Use `ffmpeg` to extract frame 0 (first frame) as a JPEG
3. Delete the temp video file immediately
4. Compute `imagehash.phash()` on the extracted frame
5. Compare phash values: hamming distance ≤ 10 = same video, > 10 = different video

**Performance**: ~1.8 seconds per video. For 200 videos, ~6 minutes total.
**Both platforms produce 720x1280 frames** — no normalization needed.

---

## Configuration / Environment Variables

```env
SHORTIMIZE_API_KEY=your_api_key_here
SHORTIMIZE_BASE_URL=https://api.shortimize.com
CREATOR_SHEET_CSV_URL=https://docs.google.com/spreadsheets/d/e/2PACX-1vTQcA8MAAhZ4urj_91M7rq80UwsmR3XePus2j2Ky-iZD_j_YSC5U5-kdSf2P1E73fohaAZWqJ6a4i2w/pub?gid=651686011&single=true&output=csv
```

**IMPORTANT: Never commit API keys to git. Use a `.env` file and add it to `.gitignore`.**

---

## Edge Cases to Handle

1. **Creator has only TikTok or only Instagram** — all videos go to Exceptions as unpaired. No payout without a cross-platform match.
2. **Multiple videos on same day, same platform, same creator** — each is a separate video; sequence matching handles this.
3. **Video posted on one platform but not the other** — goes to Exceptions, no payout.
4. **Views updated after payout period** — use `latest_views` as-is (Shortimize updates asynchronously).
5. **Creator handle changes** — mapping sheet is source of truth; unmatched handle → Exceptions.
6. **Zero qualified videos** — creator still appears in summary with $0 payout.
7. **Video over 10M views** — cap at 10M for payout calculation, keep original in audit.
8. **Unequal video counts across platforms** — pair what you can, extras go to Exceptions.
9. **`video_length` is null** — send to Exceptions, cannot match without length.
10. **Google Sheet URL** — the published URL is in HTML format; either parse HTML with `pandas.read_html()` or convert to CSV export format by modifying the URL.
11. **First frame extraction fails** (yt-dlp timeout, 403, etc.) — send video to Exceptions with reason "first frame extraction failed". Do not pair without visual confirmation.
12. **yt-dlp / ffmpeg not installed on server** — fail fast at startup with clear error message listing required system dependencies.

---

## Key Assumptions (must be documented)

1. Creator identity is determined by the internal handle mapping file.
2. Cross-platform matching uses a two-step process: (a) sequence position sorted by `created_at` + exact `video_length` + first frame phash confirmation, then (b) fallback matching from unmatched pool using exact `video_length` + first frame phash confirmation.
3. Payout uses the higher view count across TikTok and Instagram for matched pairs.
4. **Only paired videos (matched across both platforms) are eligible for payout.** Unpaired videos receive $0 and go to Exceptions.
5. Videos under 1,000 views do not qualify for payout.
6. Payout calculation caps views at 10,000,000 per video.
7. For million-based tiers (6M+), views are **floored**, never rounded up.
8. Payout is calculated **per paired video**, then summed per creator.
9. The date range filter uses `uploaded_at` (what the API supports), but matching/ordering uses `created_at`.
10. First frame comparison is the definitive matching signal. Same video = phash distance 0-10. Different video = phash distance 30+.
11. `yt-dlp` and `ffmpeg` must be available on the server as system dependencies.
