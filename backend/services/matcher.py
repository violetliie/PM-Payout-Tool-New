"""
Cross-platform video matching (SPEC.md Steps 5–11).

This is the most complex module in the pipeline. It takes validated videos
and creator mappings, then performs:
  Step 5:  Map each video to a creator (via handle lookup)
  Step 6:  Deduplicate by ad_link / ad_id (keep most recent latest_updated_at)
  Step 7:  Group videos by creator_name
  Step 8:  Sort each platform list by created_at ascending
  Step 9:  PRIMARY match — sequence position + exact video_length + first frame phash
  Step 10: FALLBACK match — exact length + first frame phash (no date requirements)
  Step 11: Handle unmatched → Exceptions only (no payout for unpaired videos)

Matching algorithm:
  1. Pair by position: TT#1↔IG#1, TT#2↔IG#2, etc.
  2. Confirm each pair with video_length: exact match only
  3. If lengths match, extract first frames and compare phash (distance ≤ 10 = same video)
  4. If length mismatch OR phash > 10 → reject pair, both go to unmatched pool
  5. FALLBACK: search unmatched pool — exact length + phash confirmation
  6. Mark matched videos as "used" to prevent re-use
  7. Remaining unmatched → Exceptions (no payout, not in Tab 2)

Output:
  - list[PayoutUnit]: all paired payout units
  - list[ExceptionVideo]: unmapped + unpaired + extraction-failed videos
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import imagehash

from models.schemas import Video, PayoutUnit, ExceptionVideo
from services.frame_extractor import get_phash, compare_hashes, is_same_video

logger = logging.getLogger(__name__)


# ===========================================================================
# Public API
# ===========================================================================

def match_videos(
    videos: list[Video],
    tiktok_map: dict[str, str],
    instagram_map: dict[str, str],
) -> tuple[list[PayoutUnit], list[ExceptionVideo]]:
    """
    Full matching pipeline: Steps 5–11.

    Takes validated videos (from Steps 1-4) and creator handle mappings,
    then produces payout units and exceptions.

    Args:
        videos:        List of Video objects that passed shortimize.py filters
        tiktok_map:    {normalized_tiktok_handle: creator_name}
        instagram_map: {normalized_instagram_handle: creator_name}

    Returns:
        payout_units: All paired payout units
        exceptions:   Videos that couldn't be mapped, paired, or had extraction failures
    """
    logger.info(f"Starting matching pipeline with {len(videos)} videos")

    # ------------------------------------------------------------------
    # Step 5: Map each video to a creator
    # ------------------------------------------------------------------
    mapped_videos, step5_exceptions = _map_videos_to_creators(
        videos, tiktok_map, instagram_map
    )
    logger.info(
        f"Step 5 complete: {len(mapped_videos)} mapped, "
        f"{len(step5_exceptions)} unmapped (not in creator list)"
    )

    # ------------------------------------------------------------------
    # Step 6: Deduplicate by ad_link / ad_id
    # ------------------------------------------------------------------
    deduped_videos = _deduplicate_videos(mapped_videos)
    logger.info(
        f"Step 6 complete: {len(deduped_videos)} after dedup "
        f"(removed {len(mapped_videos) - len(deduped_videos)} duplicates)"
    )

    # ------------------------------------------------------------------
    # Steps 7–11: Group, sort, match, build payout units
    # ------------------------------------------------------------------
    payout_units, match_exceptions = _match_all_creators(deduped_videos)
    logger.info(
        f"Steps 7-11 complete: {len(payout_units)} payout units, "
        f"{len(match_exceptions)} match exceptions"
    )

    # Combine all exceptions
    all_exceptions = step5_exceptions + match_exceptions
    logger.info(
        f"Matching pipeline complete: "
        f"{len(payout_units)} payout units, "
        f"{len(all_exceptions)} total exceptions"
    )

    return payout_units, all_exceptions


# ===========================================================================
# Step 5: Map each video to a creator name
# ===========================================================================

def _map_videos_to_creators(
    videos: list[Video],
    tiktok_map: dict[str, str],
    instagram_map: dict[str, str],
) -> tuple[list[Video], list[ExceptionVideo]]:
    """
    Map each video's username to a creator_name using the handle lookup dicts.

    - TikTok videos:    look up username in tiktok_map
    - Instagram videos: look up username in instagram_map
    - Unmatched:        → ExceptionVideo with reason "not in creator list"

    Returns:
        mapped:     Videos with creator_name set
        exceptions: Videos with no matching creator
    """
    mapped: list[Video] = []
    exceptions: list[ExceptionVideo] = []

    for video in videos:
        # Normalize username for lookup (lowercase, stripped)
        normalized_username = video.username.strip().lower()

        # Look up in the appropriate platform map
        creator_name = None
        if video.platform == "tiktok":
            creator_name = tiktok_map.get(normalized_username)
        elif video.platform == "instagram":
            creator_name = instagram_map.get(normalized_username)

        if creator_name:
            # Create a copy with creator_name set
            video_with_creator = video.model_copy(
                update={"creator_name": creator_name}
            )
            mapped.append(video_with_creator)
        else:
            exceptions.append(ExceptionVideo(
                username=video.username,
                platform=video.platform,
                ad_link=video.ad_link,
                created_at=video.created_at,
                latest_views=video.latest_views,
                video_length=video.video_length,
                reason="not in creator list",
            ))

    return mapped, exceptions


# ===========================================================================
# Step 6: Deduplicate videos by ad_link / ad_id
# ===========================================================================

def _deduplicate_videos(videos: list[Video]) -> list[Video]:
    """
    Remove duplicate video rows.

    Deduplication keys (in priority order):
      1. ad_link (primary) — exact URL match
      2. ad_id (fallback) — same ad_id

    If duplicates exist, keep the row with the most recent latest_updated_at.
    Videos with no ad_link AND no ad_id are always kept (cannot be deduped).
    """
    # --- Phase 1: Dedup by ad_link ---
    by_ad_link: dict[str, Video] = {}

    for video in videos:
        key = video.ad_link.strip()
        if not key:
            continue

        if key in by_ad_link:
            existing = by_ad_link[key]
            if _is_more_recent(video, existing):
                logger.debug(
                    f"Dedup (ad_link): replacing {existing.username} with "
                    f"{video.username} for {key}"
                )
                by_ad_link[key] = video
        else:
            by_ad_link[key] = video

    deduped_by_link = list(by_ad_link.values())
    no_link_videos = [v for v in videos if not v.ad_link.strip()]
    intermediate = deduped_by_link + no_link_videos

    # --- Phase 2: Dedup by ad_id ---
    by_ad_id: dict[str, Video] = {}
    no_id_videos: list[Video] = []

    for video in intermediate:
        if not video.ad_id:
            no_id_videos.append(video)
            continue

        key = video.ad_id.strip()
        if not key:
            no_id_videos.append(video)
            continue

        if key in by_ad_id:
            existing = by_ad_id[key]
            if _is_more_recent(video, existing):
                logger.debug(
                    f"Dedup (ad_id): replacing {existing.username} with "
                    f"{video.username} for ad_id={key}"
                )
                by_ad_id[key] = video
        else:
            by_ad_id[key] = video

    result = list(by_ad_id.values()) + no_id_videos

    if len(result) < len(videos):
        logger.info(f"Deduplication removed {len(videos) - len(result)} duplicate(s)")

    return result


def _is_more_recent(candidate: Video, existing: Video) -> bool:
    """Return True if candidate has a more recent latest_updated_at than existing."""
    if candidate.latest_updated_at is None:
        return False
    if existing.latest_updated_at is None:
        return True
    return candidate.latest_updated_at > existing.latest_updated_at


# ===========================================================================
# Steps 7–11: Group → Sort → Match → Build PayoutUnits
# ===========================================================================

def _match_all_creators(
    videos: list[Video],
) -> tuple[list[PayoutUnit], list[ExceptionVideo]]:
    """
    Steps 7–11: Group by creator, match within each creator, build payout units.

    Step 7:  Group videos by creator_name
    Steps 8-11: For each creator, run the matching algorithm

    Returns:
        all_payout_units: Combined payout units from all creators (paired only)
        all_exceptions:   Combined exceptions from all creators
    """
    # ------------------------------------------------------------------
    # Step 7: Group by creator_name
    # ------------------------------------------------------------------
    creator_groups: dict[str, list[Video]] = {}
    for video in videos:
        name = video.creator_name or "UNKNOWN"
        if name not in creator_groups:
            creator_groups[name] = []
        creator_groups[name].append(video)

    logger.info(f"Step 7: grouped into {len(creator_groups)} creators")

    # ------------------------------------------------------------------
    # Shared phash cache — each video extracted only once across all creators
    # Only stores 64-bit hashes (~100 bytes each), not full images (~2.7MB each)
    # ------------------------------------------------------------------
    phash_cache: dict[str, Optional[imagehash.ImageHash]] = {}

    # ------------------------------------------------------------------
    # Process each creator
    # ------------------------------------------------------------------
    all_payout_units: list[PayoutUnit] = []
    all_exceptions: list[ExceptionVideo] = []

    for creator_name, creator_videos in sorted(creator_groups.items()):
        tiktok_videos = [v for v in creator_videos if v.platform == "tiktok"]
        instagram_videos = [v for v in creator_videos if v.platform == "instagram"]

        logger.debug(
            f"Creator '{creator_name}': "
            f"{len(tiktok_videos)} TikTok, {len(instagram_videos)} Instagram"
        )

        payout_units, exceptions = _match_creator_videos(
            creator_name, tiktok_videos, instagram_videos, phash_cache
        )

        all_payout_units.extend(payout_units)
        all_exceptions.extend(exceptions)

    return all_payout_units, all_exceptions


def _match_creator_videos(
    creator_name: str,
    tiktok_videos: list[Video],
    instagram_videos: list[Video],
    phash_cache: Optional[dict[str, Optional[imagehash.ImageHash]]] = None,
) -> tuple[list[PayoutUnit], list[ExceptionVideo]]:
    """
    Match videos for a single creator using sequence + length + phash algorithm.

    Steps 8-11:
      Step 8:  Sort both lists by created_at ascending
      Step 9:  PRIMARY — pair by position, confirm with exact length + phash
      Step 10: FALLBACK — unmatched pool, exact length + phash (no date requirements)
      Step 11: REMAINING — unpaired → Exceptions only (no payout)

    Returns:
        payout_units: Paired payout units for this creator
        exceptions:   Unpaired + extraction-failed videos
    """
    if phash_cache is None:
        phash_cache = {}

    # ------------------------------------------------------------------
    # Step 8: Sort by created_at ascending
    # ------------------------------------------------------------------
    tiktok_sorted = sorted(tiktok_videos, key=_sort_key_created_at)
    instagram_sorted = sorted(instagram_videos, key=_sort_key_created_at)

    # ------------------------------------------------------------------
    # Track which videos have been "used" (matched)
    # ------------------------------------------------------------------
    tt_used: set[int] = set()
    ig_used: set[int] = set()

    payout_units: list[PayoutUnit] = []
    exceptions: list[ExceptionVideo] = []

    # ------------------------------------------------------------------
    # Step 9: PRIMARY matching — sequence position + length + phash
    # ------------------------------------------------------------------
    min_count = min(len(tiktok_sorted), len(instagram_sorted))

    for i in range(min_count):
        tt_video = tiktok_sorted[i]
        ig_video = instagram_sorted[i]

        # Check 1: Exact video_length match
        length_diff = _video_length_diff(tt_video, ig_video)
        if length_diff is None or length_diff != 0:
            logger.debug(
                f"  Pair #{i+1}: length mismatch → unmatched pool "
                f"(TT={tt_video.video_length}s, IG={ig_video.video_length}s)"
            )
            continue  # Both stay unmatched for Step 10

        # Check 2: First frame phash comparison
        tt_hash = get_phash(tt_video.ad_link, phash_cache)
        ig_hash = get_phash(ig_video.ad_link, phash_cache)

        if tt_hash is None:
            logger.debug(f"  Pair #{i+1}: TT frame extraction failed")
            exceptions.append(_build_extraction_failed_exception(tt_video))
            tt_used.add(i)  # Mark as used so Step 10 skips it
            continue

        if ig_hash is None:
            logger.debug(f"  Pair #{i+1}: IG frame extraction failed")
            exceptions.append(_build_extraction_failed_exception(ig_video))
            ig_used.add(i)
            continue

        phash_dist = compare_hashes(tt_hash, ig_hash)

        if is_same_video(tt_hash, ig_hash):
            # Confirmed match
            payout_units.append(_build_paired_unit(
                creator_name, tt_video, ig_video,
                method="sequence",
                note=f"sequence match, phash distance: {phash_dist}",
                phash_distance=phash_dist,
            ))
            tt_used.add(i)
            ig_used.add(i)
            logger.debug(
                f"  Pair #{i+1}: matched (length={tt_video.video_length}s, "
                f"phash={phash_dist})"
            )
        else:
            logger.debug(
                f"  Pair #{i+1}: phash mismatch ({phash_dist}) → unmatched pool"
            )
            # Both stay unmatched for Step 10

    # ------------------------------------------------------------------
    # Step 10: FALLBACK — unmatched pool matching (length + phash only)
    # ------------------------------------------------------------------
    # Collect unmatched videos (not used in Step 9, not failed extraction)
    unmatched_tt = [
        (i, tiktok_sorted[i])
        for i in range(len(tiktok_sorted))
        if i not in tt_used
    ]
    unmatched_ig = [
        (i, instagram_sorted[i])
        for i in range(len(instagram_sorted))
        if i not in ig_used
    ]

    # Check for extraction failures in unmatched pool first
    valid_tt = []
    for idx, video in unmatched_tt:
        h = get_phash(video.ad_link, phash_cache)
        if h is None:
            exceptions.append(_build_extraction_failed_exception(video))
            tt_used.add(idx)
        else:
            valid_tt.append((idx, video, h))

    valid_ig = []
    for idx, video in unmatched_ig:
        h = get_phash(video.ad_link, phash_cache)
        if h is None:
            exceptions.append(_build_extraction_failed_exception(video))
            ig_used.add(idx)
        else:
            valid_ig.append((idx, video, h))

    # Build length index for Instagram candidates (for fast lookup)
    ig_by_length: dict[int, list[tuple[int, Video, imagehash.ImageHash]]] = {}
    for idx, video, h in valid_ig:
        if video.video_length is not None:
            length = video.video_length
            if length not in ig_by_length:
                ig_by_length[length] = []
            ig_by_length[length].append((idx, video, h))

    # For each unmatched TikTok, find best phash match among same-length IG
    for tt_idx, tt_video, tt_hash in valid_tt:
        if tt_idx in tt_used:
            continue  # Already matched by a prior fallback iteration
        if tt_video.video_length is None:
            continue

        candidates = ig_by_length.get(tt_video.video_length, [])
        best_ig_idx = None
        best_phash = None

        for ig_idx, ig_video, ig_hash in candidates:
            if ig_idx in ig_used:
                continue

            phash_dist = compare_hashes(tt_hash, ig_hash)
            if phash_dist <= 10:
                if best_phash is None or phash_dist < best_phash:
                    best_ig_idx = ig_idx
                    best_phash = phash_dist

        if best_ig_idx is not None:
            ig_video = instagram_sorted[best_ig_idx]
            payout_units.append(_build_paired_unit(
                creator_name, tt_video, ig_video,
                method="fallback",
                note=f"fallback match: same length, phash distance: {best_phash}",
                phash_distance=best_phash,
            ))
            tt_used.add(tt_idx)
            ig_used.add(best_ig_idx)
            logger.debug(
                f"  Fallback: TT idx={tt_idx} ↔ IG idx={best_ig_idx} "
                f"(length={tt_video.video_length}s, phash={best_phash})"
            )

    # ------------------------------------------------------------------
    # Step 11: Handle unmatched videos → Exceptions only (no payout)
    # ------------------------------------------------------------------
    for i, tt_video in enumerate(tiktok_sorted):
        if i not in tt_used:
            exceptions.append(ExceptionVideo(
                username=tt_video.username,
                platform=tt_video.platform,
                ad_link=tt_video.ad_link,
                created_at=tt_video.created_at,
                latest_views=tt_video.latest_views,
                video_length=tt_video.video_length,
                reason="unpaired — no cross-platform match found",
            ))

    for i, ig_video in enumerate(instagram_sorted):
        if i not in ig_used:
            exceptions.append(ExceptionVideo(
                username=ig_video.username,
                platform=ig_video.platform,
                ad_link=ig_video.ad_link,
                created_at=ig_video.created_at,
                latest_views=ig_video.latest_views,
                video_length=ig_video.video_length,
                reason="unpaired — no cross-platform match found",
            ))

    # Log summary for this creator
    paired_count = len(payout_units)
    logger.debug(
        f"  Creator '{creator_name}': "
        f"{paired_count} paired, {len(exceptions)} exceptions"
    )

    return payout_units, exceptions


# ===========================================================================
# Exception builder
# ===========================================================================

def _build_extraction_failed_exception(video: Video) -> ExceptionVideo:
    """Build an ExceptionVideo for a video whose first frame couldn't be extracted."""
    return ExceptionVideo(
        username=video.username,
        platform=video.platform,
        ad_link=video.ad_link,
        created_at=video.created_at,
        latest_views=video.latest_views,
        video_length=video.video_length,
        reason="first frame extraction failed",
    )


# ===========================================================================
# PayoutUnit construction helper
# ===========================================================================

def _build_paired_unit(
    creator_name: str,
    tt_video: Video,
    ig_video: Video,
    method: str,
    note: str,
    phash_distance: int,
) -> PayoutUnit:
    """
    Build a PayoutUnit for a matched TikTok + Instagram pair.

    chosen_views = max(tiktok.latest_views, instagram.latest_views)
    best_platform = whichever had more views (for audit trail)
    """
    tt_views = tt_video.latest_views or 0
    ig_views = ig_video.latest_views or 0
    chosen_views = max(tt_views, ig_views)

    if tt_views >= ig_views:
        best_platform = "tiktok"
    else:
        best_platform = "instagram"

    return PayoutUnit(
        creator_name=creator_name,
        tiktok_video=tt_video,
        instagram_video=ig_video,
        chosen_views=chosen_views,
        best_platform=best_platform,
        match_method=method,
        match_note=note,
        phash_distance=phash_distance,
    )


# ===========================================================================
# Utility helpers
# ===========================================================================

def _sort_key_created_at(video: Video) -> datetime:
    """
    Sort key for ordering videos by created_at ascending.
    Videos with None created_at are sorted to the end (max datetime).

    Uses UTC-aware datetime to avoid TypeError when comparing with
    timezone-aware created_at values from the Shortimize API.
    """
    if video.created_at is None:
        return datetime(9999, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    return video.created_at


def _video_length_diff(v1: Video, v2: Video) -> Optional[int]:
    """
    Calculate the absolute difference in video_length between two videos.
    Returns None if either video has no video_length (cannot compare).
    """
    if v1.video_length is None or v2.video_length is None:
        return None
    return abs(v1.video_length - v2.video_length)
