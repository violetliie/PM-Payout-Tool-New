"""
Pydantic models for the Polymarket Creator Payout Tool.

Models:
  - Video: A single video from the Shortimize API (raw data + creator_name after mapping)
  - Creator: A creator from the mapping sheet (canonical name + platform handles)
  - PayoutUnit: One payout row — either a matched TikTok/Instagram pair or a standalone video
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
# PayoutUnit — one payout row: a matched pair OR an unpaired standalone video
#
# For a matched pair:   tiktok_video + instagram_video both populated, paired=True
# For an unpaired video: one of tiktok_video/instagram_video is None, paired=False
# chosen_views = max(tiktok, instagram) for pairs, or the single platform's views
# effective_views = min(chosen_views, 10_000_000)
# payout_amount = tier calculation on effective_views
# ---------------------------------------------------------------------------
class PayoutUnit(BaseModel):
    creator_name: str
    tiktok_video: Optional[Video] = None
    instagram_video: Optional[Video] = None
    chosen_views: int = 0
    effective_views: int = 0  # after 10M cap
    best_platform: Optional[str] = None  # which platform had higher views (for audit)
    payout_amount: float = 0.0
    paired: bool = False  # True = matched pair, False = standalone
    match_confidence: str = "high"  # "high", "medium", "low"
    pair_note: Optional[str] = None  # "exact match", "fallback match: same length, same upload date, closest created_at", etc.


# ---------------------------------------------------------------------------
# CreatorSummary — aggregated per-creator summary for Tab 1 of the Excel output
#
# Count definitions (per user clarification):
#   qualified_video_count = number of payout units with chosen_views >= 1,000
#                           (1 pair = 1 payout unit, 1 unpaired = 1 payout unit)
#   paired_video_count    = number of pairs (1 pair = 1, NOT 2)
#   unpaired_video_count  = number of standalone unpaired payout units
#   exception_count       = number of exception videos for this creator
# ---------------------------------------------------------------------------
class CreatorSummary(BaseModel):
    creator_name: str
    qualified_video_count: int = 0
    total_payout: float = 0.0
    paired_video_count: int = 0
    unpaired_video_count: int = 0
    exception_count: int = 0


# ---------------------------------------------------------------------------
# ExceptionVideo — a video flagged for manual review (Tab 3)
# ---------------------------------------------------------------------------
class ExceptionVideo(BaseModel):
    username: str
    platform: str
    ad_link: str
    created_at: Optional[datetime] = None
    latest_views: Optional[int] = None
    video_length: Optional[int] = None
    reason: str  # e.g., "not in creator list", "video marked private", etc.


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
