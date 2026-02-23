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
  - Column Q (index 16): `instagram_handle`
  - Column R (index 17): `tiktok_handle`
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

### Step 9: Primary Match — Sequence Match + Length Confirmation

Try to pair by position in the sorted sequence:
- TikTok #1 ↔ Instagram #1
- TikTok #2 ↔ Instagram #2
- TikTok #3 ↔ Instagram #3
- etc.

Then **confirm** each pair using `video_length`:
- **Exact match** → accept. Set `match_confidence = "high"`, `pair_note = "exact match"`
- **Any length mismatch** → reject this pair, go to Step 10 fallback

If one platform has more videos than the other (e.g., 5 TikToks, 3 Instagrams):
- Pair #1↔#1, #2↔#2, #3↔#3 (with length confirmation for each)
- TikTok #4 and #5 remain unpaired (standalone)

### Step 10: Fallback — If Sequence Pair Fails Length Check

When a sequence pair fails the length confirmation:

Search within the **same creator** for a better match on the other platform using:
1. Exact `video_length` match
2. Same `uploaded_at` date (required — if either video has no `uploaded_at`, fallback is skipped)
3. Closest `created_at` (within ±24 hours)

If a fallback match is found:
- Set `match_confidence = "medium"`
- Set `pair_note = "fallback match: same length, same upload date, closest created_at"`

If no fallback match is found:
- Both videos in the failed pair remain **unpaired** (standalone)
- Set `match_confidence = "low / exception"`
- Flag for review in Exceptions

**IMPORTANT**: A video that was already matched (in any pair) must NOT be re-used in a fallback search. Mark matched videos as "used" immediately.

### Step 11: Handle Unmatched Videos

If a video exists on only one platform, or no valid match is found:
- Treat as a valid **single-platform payout row**
- Mark as `unpaired`
- Still include in payout calculation
- Also add to Exceptions/Tab 3 for review (mark as "unpaired — single platform only")

---

## Views Selection Logic

For each **matched pair**:
- `chosen_views = max(tiktok_latest_views, instagram_latest_views)`
- Store which platform had the higher views (for audit)

For each **unpaired video**:
- `chosen_views = latest_views` from the available platform

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
- `total_payout = sum of all individual video payouts`
- `qualified_video_count = count of payout units with chosen_views >= 1,000` (a matched pair = 1 payout unit, an unpaired video = 1 payout unit)
- `paired_video_count = number of pairs` (1 pair = count as 1, not 2)
- `unpaired_video_count = number of standalone unpaired videos`
- `exception_count = number of exception videos for this creator`

---

## Output: Excel File (.xlsx)

**File name**: `Polymarket Payout Summary yyyy-mm-dd to yyyy-mm-dd.xlsx`

### Tab 1: Creator Payout Summary

One row per creator:

| Column | Description |
|---|---|
| Creator Name | From the mapping sheet |
| Qualified Video Count | Count of **payout units** (paired + unpaired) with `chosen_views >= 1,000`. A matched pair counts as 1 payout unit, an unpaired video counts as 1 payout unit. Example: 1 qualified pair + 1 qualified unpaired = 2. |
| Total Payout | Sum of all per-video payouts for that creator in this period |
| Paired Video Count | Number of **pairs** (not individual videos). 1 matched TikTok+Instagram pair = 1. |
| Unpaired Video Count | Number of standalone videos (from either platform) that were not paired |
| Exception Count | Count of exception videos for this creator |

Sort by Total Payout descending. Format payout as currency, views with comma separators. Bold header row. Auto-fit column widths.

### Tab 2: Video Audit (one row per payout unit)

One row per **payout unit** — a matched pair is ONE row with both platforms' data side by side. An unpaired video is also one row.

| Column | Description |
|---|---|
| Creator Name | Canonical name |
| Uploaded At | `uploaded_at` value |
| Video Length (sec) | Duration in seconds |
| TikTok Link | `ad_link` for TikTok video (blank if unpaired Instagram) |
| TikTok Views | `latest_views` for TikTok (blank if unpaired Instagram) |
| Instagram Link | `ad_link` for Instagram video (blank if unpaired TikTok) |
| Instagram Views | `latest_views` for Instagram (blank if unpaired TikTok) |
| Chosen Views | `max(tiktok_views, instagram_views)` or the single platform's views |
| Effective Views | After 10M cap (same as chosen_views if under 10M) |
| Payout Amount | Per-video payout from tier table |
| Paired / Unpaired | "paired" or "unpaired" |
| Match Confidence | "high", "medium", or "low" |
| Match Notes | e.g., "exact match", "fallback match: same length, same upload date, closest created_at", "unpaired — single platform only" |
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
- "no valid cross-platform match found"
- "unpaired — single platform only"
- "duplicate row with conflicting values"
- "ambiguous match (same day + same length + inconsistent counts)"

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
| Deployment | Vercel (frontend) + Railway/Render (backend) | Easy hosting |

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

1. **Creator has only TikTok or only Instagram** — no matching needed, use that platform's views. Videos are still valid payout units.
2. **Multiple videos on same day, same platform, same creator** — each is a separate video; sequence matching handles this.
3. **Video posted on one platform but not the other** — standalone payout, no deduction.
4. **Views updated after payout period** — use `latest_views` as-is (Shortimize updates asynchronously).
5. **Creator handle changes** — mapping sheet is source of truth; unmatched handle → Exceptions.
6. **Zero qualified videos** — creator still appears in summary with $0 payout.
7. **Video over 10M views** — cap at 10M for payout calculation, keep original in audit.
8. **Unequal video counts across platforms** — pair what you can by sequence, leave extras unpaired.
9. **`video_length` is null** — send to exceptions, cannot match without length.
10. **Google Sheet URL** — the published URL is in HTML format; either parse HTML with `pandas.read_html()` or convert to CSV export format by modifying the URL.

---

## Key Assumptions (must be documented)

1. Creator identity is determined by the internal handle mapping file.
2. Cross-platform matching is based on: same creator → sequence position (sorted by created_at) → confirmed by exact video_length. Fallback uses exact length + same uploaded_at date + closest created_at within ±24h.
3. Payout uses the higher view count across TikTok and Instagram for the same video.
4. Videos under 1,000 views do not qualify for payout.
5. Payout calculation caps views at 10,000,000 per video.
6. For million-based tiers (6M+), views are **floored**, never rounded up.
7. Payout is calculated **per video** (per payout unit), then summed per creator.
8. The date range filter uses `uploaded_at` (what the API supports), but matching/ordering uses `created_at`.
