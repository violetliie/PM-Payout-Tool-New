"""
Shortimize API client (SPEC.md Section 1 + Steps 1–4).

Fetches video data from the Shortimize API for a given date range,
standardizes fields, and filters out invalid videos.

API details:
  Endpoint:   GET https://api.shortimize.com/videos
  Auth:       Authorization: Bearer <SHORTIMIZE_API_KEY>
  Rate limit: 30 requests/minute → returns 429 when exceeded
  Pagination: page (min 1), limit (max 20000)
  Filtering:  uploaded_at_start, uploaded_at_end, order_by, order_direction, has_metrics

Pipeline performed here:
  Step 1: Fetch all pages of video data
  Step 2: Extract only needed fields
  Step 3: Standardize (normalize platform, parse types, skip youtube)
  Step 4: Filter invalid → separate into (valid_videos, exceptions)
"""

import logging
import time
from datetime import date, datetime
from typing import Optional

import httpx

import config
from models.schemas import Video, ExceptionVideo

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MAX_LIMIT = 20000          # Maximum items per API page
MAX_RETRIES = 3            # Retry count for network/rate-limit errors
RATE_LIMIT_DELAY = 2.1     # Seconds between paginated requests (30 req/min safe)
RETRY_BACKOFF_BASE = 2.0   # Exponential backoff base (2s, 4s, 8s)
REQUEST_TIMEOUT = 60.0     # HTTP timeout per request in seconds


# ===========================================================================
# Public API
# ===========================================================================

def fetch_videos(
    start_date: date,
    end_date: date,
) -> tuple[list[Video], list[ExceptionVideo]]:
    """
    Fetch all videos from Shortimize API for the given date range.

    Performs SPEC.md Steps 1–4:
      Step 1: Paginated fetch with date filtering
      Step 2: Extract only needed fields
      Step 3: Standardize (platform normalization, type parsing, skip youtube)
      Step 4: Filter out private/removed/null-length/null-views → exceptions

    Args:
        start_date: Payout period start (inclusive, used as uploaded_at_start)
        end_date:   Payout period end (inclusive, used as uploaded_at_end)

    Returns:
        valid_videos:  List of Video objects that passed all filters
        exceptions:    List of ExceptionVideo objects for Tab 3

    Raises:
        RuntimeError: If the API returns a non-retryable error
    """
    logger.info(f"Fetching videos from Shortimize: {start_date} to {end_date}")

    # ------------------------------------------------------------------
    # Step 1: Fetch all pages
    # ------------------------------------------------------------------
    all_raw_items = _fetch_all_pages(start_date, end_date)
    logger.info(f"Step 1 complete: fetched {len(all_raw_items)} raw video items")

    # ------------------------------------------------------------------
    # Step 2 + 3: Parse and standardize each video
    # ------------------------------------------------------------------
    parsed_videos: list[Video] = []
    step3_exceptions: list[ExceptionVideo] = []

    for raw in all_raw_items:
        video, exception = _parse_and_standardize(raw)
        if exception:
            step3_exceptions.append(exception)
        elif video:
            parsed_videos.append(video)

    logger.info(
        f"Step 2-3 complete: {len(parsed_videos)} parsed, "
        f"{len(step3_exceptions)} skipped/exceptions"
    )

    # ------------------------------------------------------------------
    # Step 4: Filter out invalid videos (private, removed, null fields)
    # ------------------------------------------------------------------
    valid_videos, step4_exceptions = _filter_invalid(parsed_videos)
    all_exceptions = step3_exceptions + step4_exceptions

    logger.info(
        f"Step 4 complete: {len(valid_videos)} valid videos, "
        f"{len(step4_exceptions)} filtered out, "
        f"{len(all_exceptions)} total exceptions"
    )

    return valid_videos, all_exceptions


# ===========================================================================
# Step 1: Paginated fetch
# ===========================================================================

def _fetch_all_pages(start_date: date, end_date: date) -> list[dict]:
    """
    Fetch all pages of video data from the API.

    Uses uploaded_at_start/end for date filtering, orders by created_at asc,
    and paginates through all pages.
    """
    all_items: list[dict] = []
    page = 1
    total_pages = 1  # Will be updated from first response

    # httpx client with SSL verification disabled (Cloudflare compat)
    with httpx.Client(verify=False, timeout=REQUEST_TIMEOUT) as client:
        while page <= total_pages:
            logger.info(f"Fetching page {page}/{total_pages}...")

            response_data = _fetch_single_page(
                client, start_date, end_date, page
            )

            if response_data is None:
                logger.error(f"Failed to fetch page {page}, stopping pagination")
                break

            # Extract data and pagination info
            items = response_data.get("data", [])
            pagination = response_data.get("pagination", {})

            all_items.extend(items)

            # Update total_pages from the response (first page tells us)
            total_pages = pagination.get("total_pages", 1)
            total_records = pagination.get("total", 0)

            logger.info(
                f"Page {page}/{total_pages}: got {len(items)} items "
                f"(total records: {total_records})"
            )

            page += 1

            # Rate limit delay between pages (except after the last page)
            if page <= total_pages:
                logger.debug(f"Rate limit delay: {RATE_LIMIT_DELAY}s")
                time.sleep(RATE_LIMIT_DELAY)

    logger.info(f"Pagination complete: {len(all_items)} total items across {total_pages} page(s)")
    return all_items


def _fetch_single_page(
    client: httpx.Client,
    start_date: date,
    end_date: date,
    page: int,
) -> Optional[dict]:
    """
    Fetch a single page from the API with retry logic.

    Retries on:
      - 429 (rate limit): waits RETRY_BACKOFF_BASE * attempt seconds
      - Network errors: retries up to MAX_RETRIES with exponential backoff
      - 5xx server errors: retries with backoff

    Raises RuntimeError on non-retryable errors (4xx other than 429).
    """
    params = {
        "uploaded_at_start": str(start_date),
        "uploaded_at_end": str(end_date),
        "order_by": "created_at",
        "order_direction": "asc",
        "has_metrics": "true",
        "limit": MAX_LIMIT,
        "page": page,
    }
    headers = {
        "Authorization": f"Bearer {config.SHORTIMIZE_API_KEY}",
    }
    url = f"{config.SHORTIMIZE_BASE_URL}/videos"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.get(url, params=params, headers=headers)

            # --- Success ---
            if response.status_code == 200:
                return response.json()

            # --- Rate limited (429) ---
            if response.status_code == 429:
                wait_time = RETRY_BACKOFF_BASE * attempt
                logger.warning(
                    f"Rate limited (429) on page {page}, "
                    f"attempt {attempt}/{MAX_RETRIES}, "
                    f"waiting {wait_time}s..."
                )
                time.sleep(wait_time)
                continue

            # --- Server error (5xx) — retryable ---
            if response.status_code >= 500:
                wait_time = RETRY_BACKOFF_BASE * attempt
                logger.warning(
                    f"Server error {response.status_code} on page {page}, "
                    f"attempt {attempt}/{MAX_RETRIES}, "
                    f"waiting {wait_time}s..."
                )
                time.sleep(wait_time)
                continue

            # --- Client error (4xx, not 429) — not retryable ---
            logger.error(
                f"API error {response.status_code} on page {page}: "
                f"{response.text[:300]}"
            )
            raise RuntimeError(
                f"Shortimize API returned {response.status_code}: {response.text[:200]}"
            )

        except httpx.RequestError as e:
            # Network error — retry with backoff
            wait_time = RETRY_BACKOFF_BASE * attempt
            logger.warning(
                f"Network error on page {page}, "
                f"attempt {attempt}/{MAX_RETRIES}: {e}. "
                f"Retrying in {wait_time}s..."
            )
            if attempt < MAX_RETRIES:
                time.sleep(wait_time)
            else:
                logger.error(f"All {MAX_RETRIES} retries exhausted for page {page}")
                raise RuntimeError(
                    f"Failed to fetch page {page} after {MAX_RETRIES} retries: {e}"
                ) from e

    # All retries exhausted (rate limit / server errors)
    logger.error(f"All {MAX_RETRIES} retries exhausted for page {page}")
    return None


# ===========================================================================
# Steps 2 + 3: Parse and standardize a single raw video
# ===========================================================================

def _parse_and_standardize(raw: dict) -> tuple[Optional[Video], Optional[ExceptionVideo]]:
    """
    Parse a raw API response dict into a Video model (Steps 2 + 3).

    Step 2: Extract only needed fields.
    Step 3: Standardize:
      - Skip youtube videos entirely (return None, no exception)
      - Normalize platform to lowercase
      - Parse video_length to int (null → exception)
      - Parse dates appropriately
      - Default latest_views to 0 if null

    Returns:
        (Video, None)            — successfully parsed
        (None, ExceptionVideo)   — invalid, goes to exceptions
        (None, None)             — silently skipped (e.g., youtube)
    """
    # --- Extract raw field values ---
    platform = str(raw.get("platform", "")).strip().lower()
    username = str(raw.get("username", "")).strip()
    ad_link = str(raw.get("ad_link", "")).strip()

    # --- Step 3a: Skip youtube videos entirely ---
    if platform == "youtube":
        return None, None

    # --- Step 3b: Validate platform is tiktok or instagram ---
    if platform not in ("tiktok", "instagram"):
        return None, ExceptionVideo(
            username=username,
            platform=platform,
            ad_link=ad_link,
            created_at=_parse_datetime(raw.get("created_at")),
            latest_views=_safe_int(raw.get("latest_views")),
            video_length=_safe_int(raw.get("video_length")),
            reason=f"unknown platform: {platform}",
        )

    # --- Step 2: Extract needed fields ---
    video_length = _safe_int(raw.get("video_length"))
    latest_views = _safe_int(raw.get("latest_views"), default=0)
    uploaded_at = _parse_date(raw.get("uploaded_at"))
    created_at = _parse_datetime(raw.get("created_at"))
    latest_updated_at = _parse_datetime(raw.get("latest_updated_at"))

    video = Video(
        username=username,
        platform=platform,
        ad_link=ad_link,
        uploaded_at=uploaded_at,
        created_at=created_at,
        video_length=video_length,
        latest_views=latest_views,
        latest_updated_at=latest_updated_at,
        linked_account_id=raw.get("linked_account_id"),
        ad_id=raw.get("ad_id"),
        title=raw.get("title"),
        private=bool(raw.get("private", False)),
        removed=bool(raw.get("removed", False)),
    )

    return video, None


# ===========================================================================
# Step 4: Filter out invalid videos
# ===========================================================================

def _filter_invalid(
    videos: list[Video],
) -> tuple[list[Video], list[ExceptionVideo]]:
    """
    Filter out invalid videos per SPEC.md Step 4.

    Invalid conditions (each produces an ExceptionVideo):
      - private == True → "video marked private"
      - removed == True → "video removed"
      - video_length is None → "missing video length"
      - latest_views is None → "missing view data"

    Returns:
        valid:      Videos that passed all filters
        exceptions: Videos that failed, with reason
    """
    valid: list[Video] = []
    exceptions: list[ExceptionVideo] = []

    for v in videos:
        reason = _get_filter_reason(v)
        if reason:
            exceptions.append(ExceptionVideo(
                username=v.username,
                platform=v.platform,
                ad_link=v.ad_link,
                created_at=v.created_at,
                latest_views=v.latest_views,
                video_length=v.video_length,
                reason=reason,
            ))
        else:
            valid.append(v)

    return valid, exceptions


def _get_filter_reason(v: Video) -> Optional[str]:
    """
    Check a video against SPEC.md Step 4 filter rules.
    Returns the reason string if the video should be filtered out,
    or None if the video is valid.
    """
    if v.private:
        return "video marked private"
    if v.removed:
        return "video removed"
    if v.video_length is None:
        return "missing video length"
    if v.video_length <= 0:
        return "invalid video length (0 or negative — likely a photo post)"
    if v.latest_views is None:
        return "missing view data"
    return None


# ===========================================================================
# Type parsing helpers
# ===========================================================================

def _safe_int(value, default: Optional[int] = None) -> Optional[int]:
    """
    Safely convert a value to int.
    Returns default if value is None or cannot be converted.
    """
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _parse_date(value) -> Optional[date]:
    """
    Parse a date string like '2026-02-21' into a date object.
    Returns None if value is None or unparseable.
    """
    if value is None:
        return None
    try:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        return date.fromisoformat(str(value)[:10])
    except (ValueError, TypeError):
        logger.debug(f"Could not parse date: {repr(value)}")
        return None


def _parse_datetime(value) -> Optional[datetime]:
    """
    Parse a datetime string like '2026-02-22T00:31:05.461255+00:00' into datetime.
    Handles timezone-aware ISO 8601 strings from the API.
    Returns None if value is None or unparseable.
    """
    if value is None:
        return None
    try:
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(str(value))
    except (ValueError, TypeError):
        logger.debug(f"Could not parse datetime: {repr(value)}")
        return None


# ===========================================================================
# Standalone test — run with: cd backend && python -m services.shortimize
# ===========================================================================

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    sys.path.insert(0, ".")

    # Fetch a small date range for testing
    test_start = date(2026, 2, 20)
    test_end = date(2026, 2, 21)

    try:
        valid_videos, exceptions = fetch_videos(test_start, test_end)
    except RuntimeError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"SHORTIMIZE API TEST RESULTS")
    print(f"{'='*60}")
    print(f"Date range:     {test_start} to {test_end}")
    print(f"Valid videos:   {len(valid_videos)}")
    print(f"Exceptions:     {len(exceptions)}")

    # Show first 3 valid videos
    print(f"\n--- First 3 valid videos ---")
    for v in valid_videos[:3]:
        print(f"  [{v.platform}] @{v.username}: {v.latest_views:,} views, "
              f"{v.video_length}s, uploaded {v.uploaded_at}")
        print(f"    {v.ad_link}")

    # Show platform breakdown
    platforms = {}
    for v in valid_videos:
        platforms[v.platform] = platforms.get(v.platform, 0) + 1
    print(f"\n--- Platform breakdown ---")
    for p, count in sorted(platforms.items()):
        print(f"  {p}: {count}")

    # Show exception reasons
    if exceptions:
        reasons = {}
        for e in exceptions:
            reasons[e.reason] = reasons.get(e.reason, 0) + 1
        print(f"\n--- Exception reasons ---")
        for r, count in sorted(reasons.items()):
            print(f"  {r}: {count}")

    # Show view distribution
    views = [v.latest_views for v in valid_videos if v.latest_views]
    if views:
        print(f"\n--- View stats ---")
        print(f"  Min:    {min(views):,}")
        print(f"  Max:    {max(views):,}")
        print(f"  Median: {sorted(views)[len(views)//2]:,}")
        under_1k = sum(1 for v in views if v < 1000)
        print(f"  Under 1K views: {under_1k} ({under_1k*100//len(views)}%)")
