"""
Creator mapping ingestion from published Google Sheet (SPEC.md Section 2).

Fetches the Google Sheet as CSV and builds lookup dictionaries
mapping platform handles to canonical creator names.

Sheet structure:
  - Column B (index 1): creator_name  ("Name? (Full Name)")
  - Column Q (index 16): instagram_handle  ("Insta Handle")
  - Column R (index 17): tiktok_handle  ("Tiktok Handle")
  - Rows 0-1 are headers — data starts at row index 2

Output:
  - List of Creator objects
  - tiktok_map:   {normalized_handle → creator_name}
  - instagram_map: {normalized_handle → creator_name}
"""

import io
import logging
from typing import Optional

import httpx
import pandas as pd

import config
from models.schemas import Creator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column indices (0-based) — matches the published Google Sheet layout
# ---------------------------------------------------------------------------
COL_CREATOR_NAME = 1       # Column B
COL_INSTAGRAM_HANDLE = 16  # Column Q
COL_TIKTOK_HANDLE = 17     # Column R
DATA_START_ROW = 2         # Rows 0-1 are headers


# ===========================================================================
# Public API
# ===========================================================================

def fetch_creator_mapping() -> tuple[list[Creator], dict[str, str], dict[str, str]]:
    """
    Fetch and parse the creator mapping Google Sheet.

    Returns:
        creators:      List of Creator objects (one per valid row)
        tiktok_map:    {normalized_tiktok_handle: creator_name}
        instagram_map: {normalized_instagram_handle: creator_name}

    Raises:
        RuntimeError: If the sheet cannot be fetched or parsed.
    """
    # ------------------------------------------------------------------
    # Step 1: Fetch the CSV data from Google Sheets
    # ------------------------------------------------------------------
    logger.info("Fetching creator mapping from Google Sheets...")
    df = _fetch_sheet_csv()
    logger.info(f"Fetched sheet with {df.shape[0]} rows, {df.shape[1]} columns")

    # ------------------------------------------------------------------
    # Step 2: Validate we have enough columns
    # ------------------------------------------------------------------
    if df.shape[1] < COL_TIKTOK_HANDLE + 1:
        raise RuntimeError(
            f"Google Sheet has only {df.shape[1]} columns, "
            f"expected at least {COL_TIKTOK_HANDLE + 1} (through Column R). "
            "Check that the correct sheet/tab is published."
        )

    # ------------------------------------------------------------------
    # Step 3: Extract and clean data rows (skip header rows 0 and 1)
    # ------------------------------------------------------------------
    creators: list[Creator] = []
    tiktok_map: dict[str, str] = {}
    instagram_map: dict[str, str] = {}

    skipped_no_name = 0

    for row_idx in range(DATA_START_ROW, df.shape[0]):
        # --- Extract raw values ---
        raw_name = df.iloc[row_idx, COL_CREATOR_NAME]
        raw_ig = df.iloc[row_idx, COL_INSTAGRAM_HANDLE]
        raw_tt = df.iloc[row_idx, COL_TIKTOK_HANDLE]

        # --- Skip rows with empty creator name ---
        creator_name = _clean_string(raw_name)
        if not creator_name:
            skipped_no_name += 1
            continue

        # --- Normalize handles ---
        ig_handle = _normalize_handle(raw_ig)
        tt_handle = _normalize_handle(raw_tt)

        # --- Build Creator object ---
        creator = Creator(
            creator_name=creator_name,
            tiktok_handle=tt_handle,
            instagram_handle=ig_handle,
        )
        creators.append(creator)

        # --- Build lookup dicts (first occurrence wins for duplicates) ---
        if tt_handle:
            if tt_handle in tiktok_map:
                existing = tiktok_map[tt_handle]
                if existing != creator_name:
                    logger.warning(
                        f"Duplicate TikTok handle '{tt_handle}' — "
                        f"already mapped to '{existing}', "
                        f"ignoring duplicate for '{creator_name}' (row {row_idx})"
                    )
            else:
                tiktok_map[tt_handle] = creator_name

        if ig_handle:
            if ig_handle in instagram_map:
                existing = instagram_map[ig_handle]
                if existing != creator_name:
                    logger.warning(
                        f"Duplicate Instagram handle '{ig_handle}' — "
                        f"already mapped to '{existing}', "
                        f"ignoring duplicate for '{creator_name}' (row {row_idx})"
                    )
            else:
                instagram_map[ig_handle] = creator_name

    # ------------------------------------------------------------------
    # Step 4: Log summary
    # ------------------------------------------------------------------
    logger.info(
        f"Creator mapping complete: "
        f"{len(creators)} creators loaded, "
        f"{len(tiktok_map)} TikTok handles, "
        f"{len(instagram_map)} Instagram handles, "
        f"{skipped_no_name} rows skipped (no name)"
    )

    return creators, tiktok_map, instagram_map


# ===========================================================================
# Private helpers
# ===========================================================================

def _fetch_sheet_csv() -> pd.DataFrame:
    """
    Fetch the Google Sheet as CSV, with SSL workaround for macOS.

    The URL in config should already point to the CSV export format:
      ...pub?gid=...&single=true&output=csv

    Returns a pandas DataFrame with NO header row (header=None),
    so all rows including headers are accessible by integer index.
    """
    url = config.CREATOR_SHEET_CSV_URL

    # Ensure URL uses CSV export format
    if "output=csv" not in url:
        logger.warning(
            "CREATOR_SHEET_CSV_URL does not contain 'output=csv'. "
            "Appending '&output=csv' — if this fails, update the URL in .env."
        )
        url = url.rstrip("&") + "&output=csv"

    try:
        logger.debug(f"Fetching: {url}")
        response = httpx.get(url, timeout=30, follow_redirects=True, verify=False)
        response.raise_for_status()
        csv_text = response.text

        # Parse CSV — header=None so we get raw row indices
        df = pd.read_csv(io.StringIO(csv_text), header=None)
        return df

    except Exception as e:
        logger.error(f"Failed to fetch creator mapping sheet: {e}")
        raise RuntimeError(f"Could not fetch creator mapping: {e}") from e


def _normalize_handle(raw_value) -> Optional[str]:
    """
    Normalize a social media handle for consistent matching.

    Steps:
      1. Convert to string, strip whitespace
      2. Skip if empty or NaN
      3. Remove leading @ if present
      4. Lowercase

    Returns None if the handle is empty/NaN.
    """
    if pd.isna(raw_value):
        return None

    handle = str(raw_value).strip()
    if not handle:
        return None

    # Remove leading @ (some sheets include it, ours doesn't, but be safe)
    if handle.startswith("@"):
        handle = handle[1:]

    # Lowercase for case-insensitive matching
    handle = handle.lower()

    return handle if handle else None


def _clean_string(raw_value) -> Optional[str]:
    """
    Clean a string value: convert to str, strip whitespace.
    Returns None if empty or NaN.
    """
    if pd.isna(raw_value):
        return None

    cleaned = str(raw_value).strip()
    return cleaned if cleaned else None


# ===========================================================================
# Standalone test — run with: cd backend && python -m services.creator_mapping
# ===========================================================================

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    sys.path.insert(0, ".")

    try:
        creators, tt_map, ig_map = fetch_creator_mapping()
    except RuntimeError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"CREATOR MAPPING RESULTS")
    print(f"{'='*60}")
    print(f"Total creators:       {len(creators)}")
    print(f"TikTok handles:       {len(tt_map)}")
    print(f"Instagram handles:    {len(ig_map)}")

    # Show first 5 creators with their handles
    print(f"\n--- First 5 creators with handles ---")
    shown = 0
    for c in creators:
        if c.tiktok_handle or c.instagram_handle:
            print(f"  {c.creator_name}")
            print(f"    TikTok:    {c.tiktok_handle or '(none)'}")
            print(f"    Instagram: {c.instagram_handle or '(none)'}")
            shown += 1
            if shown >= 5:
                break

    # Show creators with neither handle (potential data issues)
    no_handles = [c for c in creators if not c.tiktok_handle and not c.instagram_handle]
    if no_handles:
        print(f"\n--- Creators with NO handles ({len(no_handles)}) ---")
        for c in no_handles:
            print(f"  {c.creator_name}")

    # Show a sample lookup from each dict
    print(f"\n--- Sample TikTok lookups ---")
    for handle in list(tt_map.keys())[:3]:
        print(f"  '{handle}' → {tt_map[handle]}")

    print(f"\n--- Sample Instagram lookups ---")
    for handle in list(ig_map.keys())[:3]:
        print(f"  '{handle}' → {ig_map[handle]}")
