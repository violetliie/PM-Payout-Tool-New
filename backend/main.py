"""
Polymarket Creator Payout Tool — FastAPI application.

Wires together all service modules into a full pipeline:

  POST /api/calculate
    1. Validate request (start_date <= end_date)
    2. Fetch creator mapping from Google Sheet (creator_mapping.py)
    3. Fetch videos from Shortimize API (shortimize.py) → valid_videos + api_exceptions
    4. Run matching: map→dedup→pair (matcher.py) → payout_units + match_exceptions
    5. Calculate payouts (payout.py) → payout_units with amounts + creator_summaries
    6. Build exception_counts per creator for CreatorSummary
    7. Generate .xlsx report (excel_export.py)
    8. Return JSON response with summary stats

  GET /api/download/{filename}
    Serve a generated .xlsx file from the output directory.

Error handling:
  - Shortimize API fails → 502
  - Google Sheet fails → 502
  - No videos found → 200 with empty report
  - Date validation fails → 400
"""

import os
import logging
from datetime import date

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from models.schemas import CalculateRequest, CalculateResponse, ExceptionVideo
import config
from services.creator_mapping import fetch_creator_mapping
from services.shortimize import fetch_videos
from services.matcher import match_videos
from services.payout import run_payout_pipeline
from services.excel_export import generate_report
from services.frame_extractor import check_dependencies

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Verify system dependencies (checked in startup event, not at import time)
# ---------------------------------------------------------------------------
def _check_system_dependencies():
    """Check that yt-dlp and ffmpeg are available. Called on app startup."""
    deps_ok, missing_deps = check_dependencies()
    if not deps_ok:
        logger.error(
            f"MISSING SYSTEM DEPENDENCIES: {', '.join(missing_deps)}. "
            f"Install with: "
            f"{'pip install yt-dlp' if 'yt-dlp' in missing_deps else ''}"
            f"{' && ' if len(missing_deps) == 2 else ''}"
            f"{'brew install ffmpeg (Mac) or apt install ffmpeg (Linux)' if 'ffmpeg' in missing_deps else ''}"
        )
        raise RuntimeError(
            f"Required system dependencies not found: {', '.join(missing_deps)}. "
            f"Cannot start without yt-dlp and ffmpeg."
        )
    logger.info("System dependencies verified: yt-dlp and ffmpeg are available")

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Polymarket Creator Payout Tool",
    description="Automates payout calculations for short-form video campaigns",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Run startup checks
@app.on_event("startup")
async def startup_event():
    _check_system_dependencies()

# Ensure output directory exists at startup
os.makedirs(config.OUTPUT_DIR, exist_ok=True)


# ===========================================================================
# POST /api/calculate — Full payout pipeline
# ===========================================================================

@app.post("/api/calculate", response_model=CalculateResponse)
async def calculate_payouts(request: CalculateRequest):
    """
    Run the full payout calculation pipeline for a date range.

    Pipeline steps:
      1. Validate dates
      2. Fetch creator mapping (Google Sheet)
      3. Fetch videos (Shortimize API)
      4. Match videos (Steps 5-11: map → dedup → pair)
      5. Calculate payouts (Steps A-D)
      6. Build exception counts per creator
      7. Generate .xlsx report
      8. Return summary response

    Returns:
        CalculateResponse with status, filename, and summary stats
    """
    start_date = request.start_date
    end_date = request.end_date

    logger.info(f"=" * 60)
    logger.info(f"PAYOUT CALCULATION: {start_date} to {end_date}")
    logger.info(f"=" * 60)

    # ------------------------------------------------------------------
    # Step 1: Validate dates
    # ------------------------------------------------------------------
    if start_date > end_date:
        raise HTTPException(
            status_code=400,
            detail={
                "status": "error",
                "message": f"start_date ({start_date}) must be <= end_date ({end_date})",
            },
        )

    # ------------------------------------------------------------------
    # Step 2: Fetch creator mapping from Google Sheet
    # ------------------------------------------------------------------
    logger.info("Step 2: Fetching creator mapping from Google Sheet...")
    try:
        creators, tiktok_map, instagram_map = fetch_creator_mapping()
    except Exception as e:
        logger.error(f"Failed to fetch creator mapping: {e}")
        raise HTTPException(
            status_code=502,
            detail={
                "status": "error",
                "message": "Failed to fetch creator mapping",
            },
        )

    logger.info(
        f"  Creator mapping loaded: {len(creators)} creators, "
        f"{len(tiktok_map)} TikTok handles, {len(instagram_map)} Instagram handles"
    )

    # ------------------------------------------------------------------
    # Step 3: Fetch videos from Shortimize API (Steps 1-4)
    # ------------------------------------------------------------------
    logger.info("Step 3: Fetching videos from Shortimize API...")
    try:
        valid_videos, api_exceptions = fetch_videos(start_date, end_date)
    except Exception as e:
        logger.error(f"Failed to fetch video data: {e}")
        raise HTTPException(
            status_code=502,
            detail={
                "status": "error",
                "message": "Failed to fetch video data from Shortimize",
            },
        )

    logger.info(
        f"  Videos fetched: {len(valid_videos)} valid, "
        f"{len(api_exceptions)} filtered (exceptions)"
    )

    # ------------------------------------------------------------------
    # Step 4: Match videos — Steps 5-11
    #   (creator mapping → dedup → sequence match + phash → fallback + phash)
    # ------------------------------------------------------------------
    logger.info("Step 4: Running cross-platform matching...")
    payout_units, match_exceptions = match_videos(
        valid_videos, tiktok_map, instagram_map
    )

    logger.info(
        f"  Matching complete: {len(payout_units)} payout units, "
        f"{len(match_exceptions)} match exceptions"
    )

    # ------------------------------------------------------------------
    # Step 5: Calculate payouts (Steps A-D)
    #   First pass without exception_counts (we'll fill those next)
    # ------------------------------------------------------------------
    logger.info("Step 5: Calculating payouts...")
    processed_units, creator_summaries = run_payout_pipeline(payout_units)

    total_payout = sum(s.total_payout for s in creator_summaries)
    logger.info(
        f"  Payouts calculated: {len(creator_summaries)} creators, "
        f"total=${total_payout:,.2f}"
    )

    # ------------------------------------------------------------------
    # Step 6: Combine all exceptions and build per-creator exception counts
    # ------------------------------------------------------------------
    logger.info("Step 6: Building exception counts...")
    all_exceptions = api_exceptions + match_exceptions

    exception_counts = _count_exceptions_per_creator(
        all_exceptions, tiktok_map, instagram_map
    )

    # Rebuild summaries WITH exception counts
    from services.payout import build_creator_summaries
    creator_summaries = build_creator_summaries(processed_units, exception_counts)

    logger.info(
        f"  Total exceptions: {len(all_exceptions)} "
        f"(api={len(api_exceptions)}, match={len(match_exceptions)})"
    )

    # ------------------------------------------------------------------
    # Step 7: Generate .xlsx report
    # ------------------------------------------------------------------
    logger.info("Step 7: Generating Excel report...")
    filepath = generate_report(
        summaries=creator_summaries,
        payout_units=processed_units,
        exceptions=all_exceptions,
        start_date=start_date,
        end_date=end_date,
    )

    filename = os.path.basename(filepath)
    logger.info(f"  Report saved: {filename}")

    # ------------------------------------------------------------------
    # Step 8: Build and return response
    # ------------------------------------------------------------------
    total_paired = len(processed_units)  # All payout units are paired

    summary = {
        "total_creators": len(creator_summaries),
        "total_payout": total_payout,
        "total_videos_processed": len(valid_videos),
        "total_paired": total_paired,
        "total_exceptions": len(all_exceptions),
    }

    logger.info(f"Pipeline complete: {summary}")
    logger.info(f"=" * 60)

    return CalculateResponse(
        status="success",
        filename=filename,
        summary=summary,
    )


# ===========================================================================
# GET /api/download/{filename} — Serve generated .xlsx files
# ===========================================================================

@app.get("/api/download/{filename}")
async def download_report(filename: str):
    """
    Download a generated .xlsx report from the output directory.

    Sets Content-Disposition header for browser download.
    Returns 404 if the file doesn't exist.
    """
    file_path = os.path.join(config.OUTPUT_DIR, filename)

    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=404,
            detail={
                "status": "error",
                "message": f"Report not found: {filename}",
            },
        )

    return FileResponse(
        file_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )


# ===========================================================================
# Helper: Count exceptions per creator
# ===========================================================================

def _count_exceptions_per_creator(
    exceptions: list[ExceptionVideo],
    tiktok_map: dict[str, str],
    instagram_map: dict[str, str],
) -> dict[str, int]:
    """
    Count exception videos per creator_name.

    For each exception, try to resolve the username to a creator_name
    using the platform handle lookup dicts:
      - TikTok exceptions: look up username in tiktok_map
      - Instagram exceptions: look up username in instagram_map
      - If unmappable (e.g., "not in creator list"): skip — these
        exceptions go to Tab 3 but don't increment any creator's count
        (the creator isn't in the system)

    Args:
        exceptions:     Combined list of all ExceptionVideo objects
        tiktok_map:     {normalized_tiktok_handle: creator_name}
        instagram_map:  {normalized_instagram_handle: creator_name}

    Returns:
        {creator_name: exception_count}
    """
    counts: dict[str, int] = {}

    for exc in exceptions:
        # Try to resolve username → creator_name
        normalized = exc.username.strip().lower()
        creator_name = None

        if exc.platform == "tiktok":
            creator_name = tiktok_map.get(normalized)
        elif exc.platform == "instagram":
            creator_name = instagram_map.get(normalized)

        if creator_name:
            counts[creator_name] = counts.get(creator_name, 0) + 1
        else:
            # Unmappable exception — still in Tab 3 but no creator to count under
            logger.debug(
                f"Exception for unmappable user '{exc.username}' "
                f"({exc.platform}): {exc.reason}"
            )

    return counts


# ===========================================================================
# Main entry point
# ===========================================================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
