"""
Pydantic models for the Polymarket Creator Payout Tool.

Models:
  - Video: A single video from the Shortimize API (raw data + creator_name after mapping)
  - Creator: A creator from the mapping sheet (canonical name + platform handles)
  - PayoutUnit: One payout row — a matched TikTok/Instagram pair (only paired videos get payout)
  - CreatorSummary: Aggregated payout summary per creator (Tab 1 of the Excel output)
  - ExceptionVideo: A video flagged for manual review (Tab 3 of the Excel output)
  - CalculateRequest / CalculateResponse: API request/response models
"""

from pydantic import BaseModel
from datetime import date, datetime
from typing import Optional


# ---------------------------------------------------------------------------
# Video — represents a single video fetched from the Shortimize API
# Fields match SPEC.md Step 2. creator_name is added after mapping (Step 5).
# ---------------------------------------------------------------------------
class Video(BaseModel):
    username: str
    platform: str  # "tiktok" or "instagram"
    ad_link: str
    uploaded_at: Optional[date] = None
    created_at: Optional[datetime] = None
    video_length: Optional[int] = None  # seconds; API returns number, can be null
    latest_views: Optional[int] = None
    latest_updated_at: Optional[datetime] = None
    linked_account_id: Optional[str] = None
    ad_id: Optional[str] = None
    title: Optional[str] = None
    private: bool = False
    removed: bool = False
    # Set during processing (Step 5 — creator mapping)
    creator_name: Optional[str] = None


# ---------------------------------------------------------------------------
# Creator — one row from the creator mapping Google Sheet
# ---------------------------------------------------------------------------
class Creator(BaseModel):
    creator_name: str
    tiktok_handle: Optional[str] = None  # Column Q, normalized (lowercase, no @)
    instagram_handle: Optional[str] = None  # Column P, normalized (lowercase, no @)


# ---------------------------------------------------------------------------
# PayoutUnit — one payout row: a matched TikTok + Instagram pair
#
# Only paired videos (both platforms) are eligible for payout.
# Unpaired videos go to Exceptions with $0 payout.
#
# chosen_views = max(tiktok, instagram) for the pair
# effective_views = min(chosen_views, 10_000_000)
# payout_amount = tier calculation on effective_views
# ---------------------------------------------------------------------------
class PayoutUnit(BaseModel):
    creator_name: str
    tiktok_video: Video               # always populated for a valid payout unit
    instagram_video: Video             # always populated for a valid payout unit
    chosen_views: int = 0
    effective_views: int = 0           # after 10M cap
    best_platform: Optional[str] = None  # which platform had higher views (for audit)
    payout_amount: float = 0.0
    match_method: str = "sequence"     # "sequence" (Step 9) or "fallback" (Step 10)
    match_note: Optional[str] = None   # e.g., "sequence match, phash distance: 0"
    phash_distance: Optional[int] = None  # hamming distance between first frames


# ---------------------------------------------------------------------------
# CreatorSummary — aggregated per-creator summary for Tab 1 of the Excel output
#
# Count definitions:
#   qualified_video_count = number of paired payout units with chosen_views >= 1,000
#   paired_video_count    = number of pairs (1 pair = 1, NOT 2)
#   exception_count       = number of exception videos for this creator (includes unpaired)
# ---------------------------------------------------------------------------
class CreatorSummary(BaseModel):
    creator_name: str
    qualified_video_count: int = 0
    total_payout: float = 0.0
    paired_video_count: int = 0
    exception_count: int = 0


# ---------------------------------------------------------------------------
# ExceptionVideo — a video flagged for manual review (Tab 3)
# ---------------------------------------------------------------------------
class ExceptionVideo(BaseModel):
    username: str
    platform: str
    ad_link: str
    uploaded_at: Optional[date] = None
    created_at: Optional[datetime] = None
    latest_views: Optional[int] = None
    video_length: Optional[int] = None
    reason: str  # e.g., "Not in creator status list", "Video unavailable", etc.


# ---------------------------------------------------------------------------
# API request / response models
# ---------------------------------------------------------------------------
class CalculateRequest(BaseModel):
    start_date: date
    end_date: date


class CalculateResponse(BaseModel):
    status: str
    filename: str
    summary: dict
