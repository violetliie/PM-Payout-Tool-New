"""
Cross-platform video matching (SPEC.md Steps 5–11).

This is the most complex module in the pipeline. It takes validated videos
and creator mappings, then performs:
  Step 5:  Map each video to a creator (via handle lookup)
  Step 6:  Deduplicate by ad_link / ad_id (keep most recent latest_updated_at)
  Step 7:  Group videos by creator_name
  Step 8:  Sort each platform list by created_at ascending
  Step 9:  PRIMARY match — sequence position + exact video_length confirmation
  Step 10: FALLBACK match — exact length + same uploaded_at date + created_at within ±24h
  Step 11: Handle unmatched → standalone PayoutUnit + exception

Matching algorithm:
  1. Pair by position: TT#1↔IG#1, TT#2↔IG#2, etc.
  2. Confirm each pair with video_length: exact match only → high confidence
  3. If length mismatch (any difference) → reject pair, try FALLBACK on both videos
  4. FALLBACK: search all unmatched videos on the other platform for
     exact length AND same uploaded_at date AND closest created_at within ±24h
  5. Mark matched videos as "used" to prevent re-use
  6. Remaining unmatched → unpaired standalone payout units

Output:
  - list[PayoutUnit]: all payout units (paired + unpaired)
  - list[ExceptionVideo]: unmapped videos + unpaired videos flagged for review
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from models.schemas import Video, PayoutUnit, ExceptionVideo

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
FALLBACK_TIME_WINDOW = timedelta(hours=24)  # ±24h for fallback created_at search


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
        payout_units: All payout units (paired matches + unpaired standalones)
        exceptions:   Videos that couldn't be mapped or were unpaired
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
            # No ad_link — skip this dedup phase for this video
            continue

        if key in by_ad_link:
            existing = by_ad_link[key]
            # Keep the one with more recent latest_updated_at
            if _is_more_recent(video, existing):
                logger.debug(
                    f"Dedup (ad_link): replacing {existing.username} with "
                    f"{video.username} for {key}"
                )
                by_ad_link[key] = video
        else:
            by_ad_link[key] = video

    # Collect unique videos from ad_link dedup
    deduped_by_link = list(by_ad_link.values())

    # Add videos with no ad_link (they weren't in the dict)
    no_link_videos = [v for v in videos if not v.ad_link.strip()]
    intermediate = deduped_by_link + no_link_videos

    # --- Phase 2: Dedup by ad_id (for any remaining duplicates) ---
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
        all_payout_units: Combined payout units from all creators
        all_exceptions:   Combined exceptions from all creators (unpaired videos)
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
    # Process each creator
    # ------------------------------------------------------------------
    all_payout_units: list[PayoutUnit] = []
    all_exceptions: list[ExceptionVideo] = []

    for creator_name, creator_videos in sorted(creator_groups.items()):
        # Split into platform lists
        tiktok_videos = [v for v in creator_videos if v.platform == "tiktok"]
        instagram_videos = [v for v in creator_videos if v.platform == "instagram"]

        logger.debug(
            f"Creator '{creator_name}': "
            f"{len(tiktok_videos)} TikTok, {len(instagram_videos)} Instagram"
        )

        payout_units, exceptions = _match_creator_videos(
            creator_name, tiktok_videos, instagram_videos
        )

        all_payout_units.extend(payout_units)
        all_exceptions.extend(exceptions)

    return all_payout_units, all_exceptions


def _match_creator_videos(
    creator_name: str,
    tiktok_videos: list[Video],
    instagram_videos: list[Video],
) -> tuple[list[PayoutUnit], list[ExceptionVideo]]:
    """
    Match videos for a single creator using the sequence + length algorithm.

    Steps 8-11:
      Step 8:  Sort both lists by created_at ascending
      Step 9:  PRIMARY — pair by position, confirm with exact video_length
      Step 10: FALLBACK — exact length + same uploaded_at date + closest created_at within ±24h
      Step 11: REMAINING — unpaired become standalone payout units + exceptions

    Returns:
        payout_units: All payout units for this creator (paired + unpaired)
        exceptions:   Unpaired videos flagged for review
    """
    # ------------------------------------------------------------------
    # Step 8: Sort by created_at ascending
    # ------------------------------------------------------------------
    tiktok_sorted = sorted(tiktok_videos, key=_sort_key_created_at)
    instagram_sorted = sorted(instagram_videos, key=_sort_key_created_at)

    # ------------------------------------------------------------------
    # Track which videos have been "used" (matched)
    # Using index-based tracking: set of indices into the sorted lists
    # ------------------------------------------------------------------
    tt_used: set[int] = set()   # indices into tiktok_sorted
    ig_used: set[int] = set()   # indices into instagram_sorted

    payout_units: list[PayoutUnit] = []

    # ------------------------------------------------------------------
    # Step 9: PRIMARY matching — sequence position + length confirmation
    # ------------------------------------------------------------------
    min_count = min(len(tiktok_sorted), len(instagram_sorted))
    fallback_candidates: list[tuple[int, int]] = []  # (tt_idx, ig_idx) pairs that failed

    for i in range(min_count):
        tt_video = tiktok_sorted[i]
        ig_video = instagram_sorted[i]

        length_diff = _video_length_diff(tt_video, ig_video)

        if length_diff is not None and length_diff == 0:
            # --- Exact length match → HIGH confidence ---
            payout_units.append(_build_paired_unit(
                creator_name, tt_video, ig_video,
                confidence="high",
                note="exact match",
            ))
            tt_used.add(i)
            ig_used.add(i)
            logger.debug(
                f"  Pair #{i+1}: exact match "
                f"(length={tt_video.video_length}s)"
            )

        else:
            # --- Any length mismatch → reject, queue for fallback ---
            fallback_candidates.append((i, i))
            logger.debug(
                f"  Pair #{i+1}: length mismatch → fallback "
                f"(TT={tt_video.video_length}s, IG={ig_video.video_length}s)"
            )

    # ------------------------------------------------------------------
    # Step 10: FALLBACK matching for rejected sequence pairs
    #
    # IMPORTANT: A previous fallback may have already claimed one or both
    # videos from a later failed pair.  We must check the used sets BEFORE
    # running each fallback attempt.
    # ------------------------------------------------------------------
    for tt_idx, ig_idx in fallback_candidates:
        tt_video = tiktok_sorted[tt_idx]
        ig_video = instagram_sorted[ig_idx]

        # Guard: skip videos already claimed by a prior fallback
        tt_already_used = tt_idx in tt_used
        ig_already_used = ig_idx in ig_used

        tt_matched = tt_already_used   # treat as "matched" so Step 11 skips
        ig_matched = ig_already_used

        # --- TikTok fallback search (only if TT video is still free) ---
        if not tt_already_used:
            tt_fallback_match = _find_fallback_match(
                tt_video, instagram_sorted, ig_used
            )
            if tt_fallback_match is not None:
                fb_ig_idx = tt_fallback_match
                payout_units.append(_build_paired_unit(
                    creator_name, tt_video, instagram_sorted[fb_ig_idx],
                    confidence="medium",
                    note="fallback match: same length, same upload date, closest created_at",
                ))
                tt_used.add(tt_idx)
                ig_used.add(fb_ig_idx)
                tt_matched = True
                logger.debug(
                    f"  Fallback: TT idx={tt_idx} ({tt_video.video_length}s) "
                    f"→ IG idx={fb_ig_idx} ({instagram_sorted[fb_ig_idx].video_length}s)"
                )

        # --- Instagram fallback search (only if IG video is still free) ---
        if not ig_already_used:
            ig_fallback_match = _find_fallback_match(
                ig_video, tiktok_sorted, tt_used
            )
            if ig_fallback_match is not None:
                fb_tt_idx = ig_fallback_match
                payout_units.append(_build_paired_unit(
                    creator_name, tiktok_sorted[fb_tt_idx], ig_video,
                    confidence="medium",
                    note="fallback match: same length, same upload date, closest created_at",
                ))
                tt_used.add(fb_tt_idx)
                ig_used.add(ig_idx)
                ig_matched = True
                logger.debug(
                    f"  Fallback: IG idx={ig_idx} ({ig_video.video_length}s) "
                    f"→ TT idx={fb_tt_idx} ({tiktok_sorted[fb_tt_idx].video_length}s)"
                )

        # Log videos that didn't find a fallback
        if not tt_matched:
            logger.debug(
                f"  Fallback failed for TT @{tt_video.username} "
                f"(length={tt_video.video_length}s) — will be unpaired"
            )
        if not ig_matched:
            logger.debug(
                f"  Fallback failed for IG @{ig_video.username} "
                f"(length={ig_video.video_length}s) — will be unpaired"
            )

    # ------------------------------------------------------------------
    # Step 11: Handle unmatched videos → standalone payout units + exceptions
    # ------------------------------------------------------------------
    exceptions: list[ExceptionVideo] = []

    # Unmatched TikTok videos
    for i, tt_video in enumerate(tiktok_sorted):
        if i not in tt_used:
            payout_units.append(_build_unpaired_unit(creator_name, tt_video))
            exceptions.append(ExceptionVideo(
                username=tt_video.username,
                platform=tt_video.platform,
                ad_link=tt_video.ad_link,
                created_at=tt_video.created_at,
                latest_views=tt_video.latest_views,
                video_length=tt_video.video_length,
                reason="unpaired — single platform only",
            ))

    # Unmatched Instagram videos
    for i, ig_video in enumerate(instagram_sorted):
        if i not in ig_used:
            payout_units.append(_build_unpaired_unit(creator_name, ig_video))
            exceptions.append(ExceptionVideo(
                username=ig_video.username,
                platform=ig_video.platform,
                ad_link=ig_video.ad_link,
                created_at=ig_video.created_at,
                latest_views=ig_video.latest_views,
                video_length=ig_video.video_length,
                reason="unpaired — single platform only",
            ))

    # Log summary for this creator
    paired_count = sum(1 for pu in payout_units if pu.paired)
    unpaired_count = sum(1 for pu in payout_units if not pu.paired)
    logger.debug(
        f"  Creator '{creator_name}': "
        f"{paired_count} paired, {unpaired_count} unpaired, "
        f"{len(exceptions)} exceptions"
    )

    return payout_units, exceptions


# ===========================================================================
# Fallback matching helper
# ===========================================================================

def _find_fallback_match(
    source_video: Video,
    candidate_list: list[Video],
    used_indices: set[int],
) -> Optional[int]:
    """
    Search for a fallback match for source_video among unused candidates.

    Criteria (per SPEC.md Step 10):
      1. Exact video_length match
      2. Same uploaded_at date
      3. Closest created_at (must be within ±24 hours)

    Args:
        source_video:   The video looking for a match
        candidate_list: Sorted list of videos on the OTHER platform
        used_indices:   Set of indices already matched (skip these)

    Returns:
        Index into candidate_list of the best fallback match, or None
    """
    # Cannot fallback without length, created_at, or uploaded_at
    if source_video.video_length is None or source_video.created_at is None:
        return None
    if source_video.uploaded_at is None:
        return None

    source_length = source_video.video_length
    source_time = source_video.created_at
    source_upload_date = source_video.uploaded_at

    best_idx: Optional[int] = None
    best_time_diff: Optional[timedelta] = None

    for idx, candidate in enumerate(candidate_list):
        # Skip already-used candidates
        if idx in used_indices:
            continue

        # Skip candidates with missing data
        if candidate.video_length is None or candidate.created_at is None:
            continue

        # Skip candidates with missing or different uploaded_at date
        if candidate.uploaded_at is None or candidate.uploaded_at != source_upload_date:
            continue

        # Check time window: ±24 hours
        time_diff = abs(source_time - candidate.created_at)
        if time_diff > FALLBACK_TIME_WINDOW:
            continue

        # Check exact length match
        if candidate.video_length != source_length:
            continue

        # Track the closest by created_at
        if best_time_diff is None or time_diff < best_time_diff:
            best_idx = idx
            best_time_diff = time_diff

    return best_idx


# ===========================================================================
# PayoutUnit construction helpers
# ===========================================================================

def _build_paired_unit(
    creator_name: str,
    tt_video: Video,
    ig_video: Video,
    confidence: str,
    note: str,
) -> PayoutUnit:
    """
    Build a PayoutUnit for a matched TikTok + Instagram pair.

    chosen_views = max(tiktok.latest_views, instagram.latest_views)
    best_platform = whichever had more views (for audit trail)
    """
    tt_views = tt_video.latest_views or 0
    ig_views = ig_video.latest_views or 0
    chosen_views = max(tt_views, ig_views)

    # Determine which platform had more views
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
        paired=True,
        match_confidence=confidence,
        pair_note=note,
    )


def _build_unpaired_unit(
    creator_name: str,
    video: Video,
) -> PayoutUnit:
    """
    Build a PayoutUnit for an unpaired standalone video.

    chosen_views = the single platform's latest_views
    """
    views = video.latest_views or 0

    if video.platform == "tiktok":
        return PayoutUnit(
            creator_name=creator_name,
            tiktok_video=video,
            instagram_video=None,
            chosen_views=views,
            best_platform="tiktok",
            paired=False,
            match_confidence="low",
            pair_note="unpaired — single platform only",
        )
    else:
        return PayoutUnit(
            creator_name=creator_name,
            tiktok_video=None,
            instagram_video=video,
            chosen_views=views,
            best_platform="instagram",
            paired=False,
            match_confidence="low",
            pair_note="unpaired — single platform only",
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
        # Use a far-future UTC datetime so None sorts last.
        # datetime.max cannot be used directly because it's naive and
        # comparing naive vs aware datetimes raises TypeError.
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


# ===========================================================================
# Standalone test — run with: cd backend && python -m services.matcher
# ===========================================================================

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")
    sys.path.insert(0, ".")

    # Quick sanity test with synthetic data
    from datetime import date as date_type

    def make_video(
        username: str, platform: str, length: int, views: int,
        created_at_str: str, ad_link: str = "",
    ) -> Video:
        return Video(
            username=username,
            platform=platform,
            ad_link=ad_link or f"https://example.com/{username}/{length}",
            uploaded_at=date_type(2026, 2, 20),
            created_at=datetime.fromisoformat(created_at_str),
            video_length=length,
            latest_views=views,
        )

    # Create test videos for one creator
    test_videos = [
        # TikTok videos (3)
        make_video("creator_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "tt1"),
        make_video("creator_tt", "tiktok", 45, 12000, "2026-02-20T14:00:00+00:00", "tt2"),
        make_video("creator_tt", "tiktok", 60, 800, "2026-02-21T09:00:00+00:00", "tt3"),
        # Instagram videos (3)
        make_video("creator_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "ig1"),
        make_video("creator_ig", "instagram", 45, 3000, "2026-02-20T14:30:00+00:00", "ig2"),
        make_video("creator_ig", "instagram", 60, 1500, "2026-02-21T09:30:00+00:00", "ig3"),
    ]

    tt_map = {"creator_tt": "Test Creator"}
    ig_map = {"creator_ig": "Test Creator"}

    payout_units, exceptions = match_videos(test_videos, tt_map, ig_map)

    print(f"\n{'='*60}")
    print(f"MATCHER SANITY TEST")
    print(f"{'='*60}")
    print(f"Payout units: {len(payout_units)}")
    print(f"Exceptions:   {len(exceptions)}")

    for pu in payout_units:
        tt_info = f"TT @{pu.tiktok_video.username} ({pu.tiktok_video.latest_views:,})" if pu.tiktok_video else "—"
        ig_info = f"IG @{pu.instagram_video.username} ({pu.instagram_video.latest_views:,})" if pu.instagram_video else "—"
        print(f"  [{pu.match_confidence}] {tt_info} ↔ {ig_info}")
        print(f"    chosen_views={pu.chosen_views:,}, paired={pu.paired}, note={pu.pair_note}")
