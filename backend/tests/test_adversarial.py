"""
Adversarial edge-case tests for the Polymarket Creator Payout Tool.

These tests go BEYOND normal spec verification to probe hidden assumptions,
race conditions between algorithm steps, surprising input combinations, and
boundary conditions that a developer might overlook during implementation.

Categories:
  1.  Timezone edge cases (naive vs aware mixing, None created_at)
  2.  Deduplication edge cases (cross-platform, empty strings, overlapping keys)
  3.  Fallback double-booking prevention
  4.  Matcher with identical videos (stress on tiebreaking)
  5.  Payout math edge cases (monotonicity, cap boundaries, negative views)
  6.  Creator summary isolation (case sensitivity, large counts)
  7.  Empty and null inputs
  8.  Large-scale stress tests
  9.  Chosen-views selection for pairs
  10. Spec regression tests (qualification threshold, cap preservation)
"""

import sys
import os
import math
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import date, datetime, timezone, timedelta
from models.schemas import Video, PayoutUnit, CreatorSummary, ExceptionVideo
from services.matcher import (
    match_videos,
    _deduplicate_videos,
    _map_videos_to_creators,
)
from services.payout import (
    calculate_effective_views,
    calculate_payout,
    process_payouts,
    build_creator_summaries,
    run_payout_pipeline,
    VIEW_CAP,
    QUALIFICATION_THRESHOLD,
)


# ===========================================================================
# Helper: make_video
# ===========================================================================

def make_video(
    username="testuser",
    platform="tiktok",
    length=30,
    views=5000,
    created_at_str="2026-02-20T10:00:00+00:00",
    ad_link=None,
    ad_id=None,
    uploaded_at_date=None,
    private=False,
    removed=False,
    title=None,
    updated_at_str=None,
    creator_name=None,
):
    return Video(
        username=username,
        platform=platform,
        ad_link=ad_link or f"https://{platform}.com/@{username}/video/{abs(hash(created_at_str)) % 99999}",
        uploaded_at=uploaded_at_date if uploaded_at_date is not None else date(2026, 2, 20),
        created_at=datetime.fromisoformat(created_at_str),
        video_length=length,
        latest_views=views,
        latest_updated_at=datetime.fromisoformat(updated_at_str or created_at_str),
        linked_account_id=None,
        ad_id=ad_id,
        title=title,
        private=private,
        removed=removed,
        creator_name=creator_name,
    )


# ===========================================================================
# 1. TestTimezoneEdgeCases
# ===========================================================================

class TestTimezoneEdgeCases:
    """
    The matcher sorts videos by created_at (Step 8). Mixing naive and
    timezone-aware datetimes in Python raises TypeError during comparison.
    These tests verify the system handles such scenarios.
    """

    def test_naive_vs_aware_datetime_mixing(self):
        """
        If one video has a naive created_at and another has a timezone-aware
        created_at, sorting them together should either work gracefully or
        raise a consistent, understandable error -- not produce corrupt output.

        The sort key function uses created_at directly, so mixing naive and
        aware datetimes will raise TypeError in Python's sort.
        """
        # Naive datetime (no tzinfo)
        naive_video = Video(
            username="tt_user", platform="tiktok",
            ad_link="https://tiktok.com/v1",
            uploaded_at=date(2026, 2, 20),
            created_at=datetime(2026, 2, 20, 10, 0, 0),  # naive
            video_length=30, latest_views=5000,
        )
        naive_video_with_creator = naive_video.model_copy(
            update={"creator_name": "TestCreator"}
        )

        # Aware datetime (UTC)
        aware_video = Video(
            username="tt_user2", platform="tiktok",
            ad_link="https://tiktok.com/v2",
            uploaded_at=date(2026, 2, 20),
            created_at=datetime(2026, 2, 20, 10, 0, 0, tzinfo=timezone.utc),  # aware
            video_length=30, latest_views=5000,
        )
        aware_video_with_creator = aware_video.model_copy(
            update={"creator_name": "TestCreator"}
        )

        # Sorting these two together in _match_creator_videos will compare
        # naive datetime with aware datetime. This should raise TypeError
        # in Python. We test that this either succeeds or raises cleanly.
        from services.matcher import _match_creator_videos

        with pytest.raises(TypeError):
            _match_creator_videos(
                "TestCreator",
                [naive_video_with_creator, aware_video_with_creator],
                [],
            )

    def test_all_aware_datetimes_sort_correctly(self):
        """All timezone-aware datetimes should sort without issue."""
        tt_map = {"user1": "Creator"}
        ig_map = {"user1": "Creator"}

        v1 = make_video(username="user1", platform="tiktok", length=30,
                        created_at_str="2026-02-20T08:00:00+00:00", ad_link="tt1")
        v2 = make_video(username="user1", platform="tiktok", length=30,
                        created_at_str="2026-02-20T12:00:00+00:00", ad_link="tt2")
        v3 = make_video(username="user1", platform="instagram", length=30,
                        created_at_str="2026-02-20T09:00:00+00:00", ad_link="ig1")
        v4 = make_video(username="user1", platform="instagram", length=30,
                        created_at_str="2026-02-20T13:00:00+00:00", ad_link="ig2")

        payout_units, exceptions = match_videos([v1, v2, v3, v4], tt_map, ig_map)
        # Should produce 2 pairs with no errors
        assert len(payout_units) == 2

    def test_created_at_none_sorts_to_end(self):
        """
        A video with created_at=None should sort to the end of the list,
        not crash the sort. The _sort_key_created_at returns a far-future
        datetime for None values.
        """
        tt_map = {"user1": "Creator"}
        ig_map = {"user1": "Creator"}

        normal_video = make_video(username="user1", platform="tiktok", length=30,
                                  created_at_str="2026-02-20T10:00:00+00:00",
                                  ad_link="tt1")
        # Create a video with created_at=None
        none_video = Video(
            username="user1", platform="tiktok",
            ad_link="tt_none",
            uploaded_at=date(2026, 2, 20),
            created_at=None,
            video_length=30, latest_views=5000,
        )
        ig_video = make_video(username="user1", platform="instagram", length=30,
                              created_at_str="2026-02-20T10:00:00+00:00",
                              ad_link="ig1")

        # Should not crash; the None video sorts last
        payout_units, exceptions = match_videos(
            [none_video, normal_video, ig_video], tt_map, ig_map
        )
        # normal TT pairs with IG; None TT goes to exceptions (unpaired)
        assert len(payout_units) == 1
        # The unpaired None-created_at video should be in exceptions
        unpaired_exceptions = [
            e for e in exceptions
            if e.reason == "Only posted on one platform"
        ]
        assert len(unpaired_exceptions) == 1
        assert unpaired_exceptions[0].created_at is None

    def test_fallback_matches_regardless_of_time_distance(self):
        """
        Fallback matching uses exact length + phash only, with no time
        window requirement. Videos 24h apart with same length should match.
        """
        tt_map = {"user1": "Creator"}
        ig_map = {"user1": "Creator"}

        # TT at noon Feb 20, IG at noon Feb 21 -- 24h apart
        tt = make_video(username="user1", platform="tiktok", length=45,
                        created_at_str="2026-02-20T12:00:00+00:00",
                        ad_link="tt1")
        # IG at same position but different length to force fallback
        ig_wrong_len = make_video(username="user1", platform="instagram", length=60,
                                   created_at_str="2026-02-20T10:00:00+00:00",
                                   ad_link="ig_wrong")
        # IG fallback candidate: same length, 24h apart
        ig_fallback = make_video(username="user1", platform="instagram", length=45,
                                  created_at_str="2026-02-21T12:00:00+00:00",
                                  ad_link="ig_fb")

        payout_units, _ = match_videos(
            [tt, ig_wrong_len, ig_fallback], tt_map, ig_map
        )
        # Fallback should succeed: TT(45s) matches IG(45s) via exact length + phash
        assert len(payout_units) >= 1
        # Verify the pair is TT(45s) + IG(45s, fallback)
        found_fallback = any(
            pu.match_note and "fallback" in pu.match_note
            for pu in payout_units
        )
        assert found_fallback, "Same-length videos should match via fallback regardless of time distance"

    def test_fallback_matches_even_far_apart_in_time(self):
        """
        Fallback no longer has a time window. Videos far apart in time
        but with exact same length should still match via fallback + phash.
        """
        tt_map = {"user1": "Creator"}
        ig_map = {"user1": "Creator"}

        tt = make_video(username="user1", platform="tiktok", length=45,
                        created_at_str="2026-02-20T12:00:00+00:00",
                        ad_link="tt1")
        # Force fallback by making sequence pair fail (different length IG at pos 0)
        ig_wrong = make_video(username="user1", platform="instagram", length=99,
                               created_at_str="2026-02-20T10:00:00+00:00",
                               ad_link="ig_wrong")
        # IG candidate: same length, 24h + 1s apart (should still match)
        ig_far = make_video(username="user1", platform="instagram", length=45,
                             created_at_str="2026-02-21T12:00:01+00:00",
                             ad_link="ig_far")

        payout_units, _ = match_videos(
            [tt, ig_wrong, ig_far], tt_map, ig_map
        )
        # Fallback should succeed since time distance is irrelevant now
        fallback_pairs = [
            pu for pu in payout_units
            if pu.match_note and "fallback" in pu.match_note
        ]
        assert len(fallback_pairs) == 1, "Fallback should match regardless of time distance"


# ===========================================================================
# 2. TestDeduplicationEdgeCases
# ===========================================================================

class TestDeduplicationEdgeCases:
    """
    Dedup is by ad_link first, then ad_id. These tests probe surprising
    inputs like cross-platform duplicates, empty strings, and overlapping keys.
    """

    def test_dedup_same_ad_link_different_platforms(self):
        """
        Two videos with the same ad_link but different platforms.
        Dedup should keep only the one with more recent latest_updated_at.
        """
        v1 = make_video(username="user1", platform="tiktok", length=30,
                        ad_link="https://shared-link.com/v1",
                        created_at_str="2026-02-20T10:00:00+00:00",
                        updated_at_str="2026-02-20T10:00:00+00:00",
                        creator_name="Creator")
        v2 = make_video(username="user1", platform="instagram", length=30,
                        ad_link="https://shared-link.com/v1",
                        created_at_str="2026-02-20T11:00:00+00:00",
                        updated_at_str="2026-02-20T12:00:00+00:00",
                        creator_name="Creator")

        result = _deduplicate_videos([v1, v2])
        assert len(result) == 1
        # v2 is more recent, so it should be kept
        assert result[0].platform == "instagram"

    def test_dedup_empty_ad_link(self):
        """
        Videos with empty string ad_link should NOT be deduped away.
        The code strips ad_link and skips empty strings.
        """
        v1 = make_video(username="user1", platform="tiktok", length=30,
                        ad_link="",
                        created_at_str="2026-02-20T10:00:00+00:00",
                        creator_name="Creator")
        v2 = make_video(username="user2", platform="tiktok", length=45,
                        ad_link="",
                        created_at_str="2026-02-20T11:00:00+00:00",
                        creator_name="Creator")

        result = _deduplicate_videos([v1, v2])
        # Both should survive because empty ad_link skips dedup
        assert len(result) == 2

    def test_dedup_whitespace_only_ad_link(self):
        """
        Videos with whitespace-only ad_link should also not be deduped
        since the code strips and checks for empty.
        """
        v1 = make_video(username="user1", platform="tiktok", length=30,
                        ad_link="   ",
                        created_at_str="2026-02-20T10:00:00+00:00",
                        creator_name="Creator")
        v2 = make_video(username="user2", platform="tiktok", length=45,
                        ad_link="  \t ",
                        created_at_str="2026-02-20T11:00:00+00:00",
                        creator_name="Creator")

        result = _deduplicate_videos([v1, v2])
        assert len(result) == 2

    def test_dedup_both_ad_link_and_ad_id_match(self):
        """
        A video that matches on BOTH ad_link AND ad_id should still
        result in only 1 video after dedup.
        """
        v1 = make_video(username="user1", platform="tiktok", length=30,
                        ad_link="https://example.com/same",
                        ad_id="same_id",
                        created_at_str="2026-02-20T10:00:00+00:00",
                        updated_at_str="2026-02-20T10:00:00+00:00",
                        creator_name="Creator")
        v2 = make_video(username="user1", platform="tiktok", length=30,
                        ad_link="https://example.com/same",
                        ad_id="same_id",
                        created_at_str="2026-02-20T11:00:00+00:00",
                        updated_at_str="2026-02-20T12:00:00+00:00",
                        creator_name="Creator")

        result = _deduplicate_videos([v1, v2])
        assert len(result) == 1

    def test_dedup_ad_id_only_no_ad_link(self):
        """
        Videos with no ad_link (empty) but same ad_id should be deduped
        via the ad_id phase.
        """
        v1 = make_video(username="user1", platform="tiktok", length=30,
                        ad_link="",
                        ad_id="shared_ad_id",
                        created_at_str="2026-02-20T10:00:00+00:00",
                        updated_at_str="2026-02-20T10:00:00+00:00",
                        creator_name="Creator")
        v2 = make_video(username="user1", platform="tiktok", length=30,
                        ad_link="",
                        ad_id="shared_ad_id",
                        created_at_str="2026-02-20T11:00:00+00:00",
                        updated_at_str="2026-02-20T12:00:00+00:00",
                        creator_name="Creator")

        result = _deduplicate_videos([v1, v2])
        assert len(result) == 1
        # The more recent one should survive
        assert result[0].latest_updated_at == datetime.fromisoformat("2026-02-20T12:00:00+00:00")

    def test_dedup_preserves_creator_name(self):
        """After dedup, the kept video should still have creator_name set correctly."""
        v1 = make_video(username="user1", platform="tiktok", length=30,
                        ad_link="https://example.com/dup",
                        created_at_str="2026-02-20T10:00:00+00:00",
                        updated_at_str="2026-02-20T10:00:00+00:00",
                        creator_name="Alice Wonderland")
        v2 = make_video(username="user1", platform="tiktok", length=30,
                        ad_link="https://example.com/dup",
                        created_at_str="2026-02-20T11:00:00+00:00",
                        updated_at_str="2026-02-20T14:00:00+00:00",
                        creator_name="Alice Wonderland")

        result = _deduplicate_videos([v1, v2])
        assert len(result) == 1
        assert result[0].creator_name == "Alice Wonderland"

    def test_dedup_different_ad_links_same_ad_id(self):
        """
        Videos with different ad_links but same ad_id. Phase 1 keeps both
        (different ad_links), then phase 2 deduplicates by ad_id, keeping
        the most recent.
        """
        v1 = make_video(username="user1", platform="tiktok", length=30,
                        ad_link="https://example.com/link_a",
                        ad_id="shared_ad_id",
                        created_at_str="2026-02-20T10:00:00+00:00",
                        updated_at_str="2026-02-20T10:00:00+00:00",
                        creator_name="Creator")
        v2 = make_video(username="user1", platform="tiktok", length=30,
                        ad_link="https://example.com/link_b",
                        ad_id="shared_ad_id",
                        created_at_str="2026-02-20T11:00:00+00:00",
                        updated_at_str="2026-02-20T12:00:00+00:00",
                        creator_name="Creator")

        result = _deduplicate_videos([v1, v2])
        # Phase 1 keeps both (different ad_links).
        # Phase 2 deduplicates by ad_id, keeping the most recent.
        assert len(result) == 1
        assert result[0].ad_link == "https://example.com/link_b"

    def test_dedup_none_updated_at_loses(self):
        """
        Between two duplicates, the one with latest_updated_at=None should
        lose to the one with a real timestamp.
        """
        v1 = make_video(username="user1", platform="tiktok", length=30,
                        ad_link="https://example.com/dup",
                        created_at_str="2026-02-20T10:00:00+00:00",
                        creator_name="Creator")
        # Manually set latest_updated_at to None
        v1_none = v1.model_copy(update={"latest_updated_at": None})

        v2 = make_video(username="user1", platform="tiktok", length=30,
                        ad_link="https://example.com/dup",
                        created_at_str="2026-02-20T11:00:00+00:00",
                        updated_at_str="2026-02-20T11:00:00+00:00",
                        creator_name="Creator")

        result = _deduplicate_videos([v1_none, v2])
        assert len(result) == 1
        assert result[0].latest_updated_at is not None


# ===========================================================================
# 3. TestFallbackDoubleBooking
# ===========================================================================

class TestFallbackDoubleBooking:
    """
    The fallback algorithm must track "used" indices to prevent double-booking.
    These tests verify that a video matched by one step is not stolen by another.
    """

    def test_fallback_cannot_steal_already_paired_video(self):
        """
        If IG[0] was paired by sequence match (Step 9), a later fallback
        for a different TT should NOT grab IG[0] again.
        """
        tt_map = {"user1": "Creator"}
        ig_map = {"user1": "Creator"}

        # TT[0] and IG[0] have same length -> sequence match succeeds
        tt0 = make_video(username="user1", platform="tiktok", length=30,
                         created_at_str="2026-02-20T10:00:00+00:00", ad_link="tt0")
        # TT[1] has different length from IG[1] -> sequence fails, fallback
        tt1 = make_video(username="user1", platform="tiktok", length=30,
                         created_at_str="2026-02-20T14:00:00+00:00", ad_link="tt1")

        ig0 = make_video(username="user1", platform="instagram", length=30,
                         created_at_str="2026-02-20T10:30:00+00:00", ad_link="ig0")
        ig1 = make_video(username="user1", platform="instagram", length=45,
                         created_at_str="2026-02-20T14:30:00+00:00", ad_link="ig1")

        payout_units, exceptions = match_videos([tt0, tt1, ig0, ig1], tt_map, ig_map)

        # TT[0]+IG[0] should be a sequence pair
        # TT[1] should NOT re-grab IG[0] via fallback
        assert len(payout_units) == 1  # only TT[0]+IG[0]
        # TT[1] and IG[1] go to exceptions as unpaired
        unpaired_exceptions = [
            e for e in exceptions
            if e.reason == "Only posted on one platform"
        ]
        assert len(unpaired_exceptions) == 2

    def test_three_tt_two_ig_fallback_ordering(self):
        """
        3 TT + 2 IG where ALL sequence matches fail due to length mismatch.
        Fallback should match optimally and not greedily steal candidates.

        TT[0]=30s, TT[1]=45s, TT[2]=60s
        IG[0]=45s, IG[1]=60s

        Sequence: TT[0](30)!=IG[0](45), TT[1](45)!=IG[1](60) -> both fail
        Fallback for TT[0]: looks for 30s IG -> none found
        Fallback for IG[0]: looks for 45s TT -> finds TT[1] -> match
        Fallback for TT[1]: already used by IG[0]'s fallback -> skip
        Fallback for IG[1]: looks for 60s TT -> finds TT[2] -> match
        """
        tt_map = {"user1": "Creator"}
        ig_map = {"user1": "Creator"}

        tt0 = make_video(username="user1", platform="tiktok", length=30,
                         created_at_str="2026-02-20T10:00:00+00:00", ad_link="tt0")
        tt1 = make_video(username="user1", platform="tiktok", length=45,
                         created_at_str="2026-02-20T11:00:00+00:00", ad_link="tt1")
        tt2 = make_video(username="user1", platform="tiktok", length=60,
                         created_at_str="2026-02-20T12:00:00+00:00", ad_link="tt2")

        ig0 = make_video(username="user1", platform="instagram", length=45,
                         created_at_str="2026-02-20T10:30:00+00:00", ad_link="ig0")
        ig1 = make_video(username="user1", platform="instagram", length=60,
                         created_at_str="2026-02-20T12:30:00+00:00", ad_link="ig1")

        payout_units, exceptions = match_videos(
            [tt0, tt1, tt2, ig0, ig1], tt_map, ig_map
        )

        # Expect: TT[1]+IG[0] (45s) and TT[2]+IG[1] (60s) via fallback
        # TT[0] (30s) goes to exceptions as unpaired
        assert len(payout_units) == 2, f"Expected 2 pairs, got {len(payout_units)}"

        # The unpaired one should be TT[0] (30s, no matching IG) in exceptions
        unpaired_exceptions = [
            e for e in exceptions
            if e.reason == "Only posted on one platform"
        ]
        assert len(unpaired_exceptions) == 1, f"Expected 1 unpaired exception, got {len(unpaired_exceptions)}"
        assert unpaired_exceptions[0].video_length == 30

    def test_fallback_both_sides_compete_for_same_candidate(self):
        """
        TT[0] and TT[1] both want IG[0] via fallback (same length, same
        uploaded_at date). The first one processed should win.

        Setup: 2 TT (both 30s) + 1 IG (30s), but sequence only has 1 position.
        TT[0]+IG[0] sequence pair: same length -> they pair directly.
        TT[1] has no sequence partner -> becomes unpaired.
        """
        tt_map = {"user1": "Creator"}
        ig_map = {"user1": "Creator"}

        tt0 = make_video(username="user1", platform="tiktok", length=30,
                         created_at_str="2026-02-20T10:00:00+00:00", ad_link="tt0")
        tt1 = make_video(username="user1", platform="tiktok", length=30,
                         created_at_str="2026-02-20T11:00:00+00:00", ad_link="tt1")
        ig0 = make_video(username="user1", platform="instagram", length=30,
                         created_at_str="2026-02-20T10:30:00+00:00", ad_link="ig0")

        payout_units, exceptions = match_videos([tt0, tt1, ig0], tt_map, ig_map)

        # TT[0]+IG[0] pair by sequence, TT[1] goes to exceptions as unpaired
        assert len(payout_units) == 1
        unpaired_exceptions = [
            e for e in exceptions
            if e.reason == "Only posted on one platform"
        ]
        assert len(unpaired_exceptions) == 1

    def test_fallback_ig_video_not_stolen_after_tt_fallback(self):
        """
        When processing fallback_candidates, if TT's fallback claims an IG,
        the IG's own fallback search should not run for that same IG video
        (it is already marked as used).
        """
        tt_map = {"user1": "Creator"}
        ig_map = {"user1": "Creator"}

        # Sequence: TT[0](30s) vs IG[0](45s) -> mismatch -> fallback
        # TT[0] fallback: look for 30s IG -> finds IG[1] (30s) -> match
        # IG[0] fallback: look for 45s TT -> finds TT[1] (45s) -> match
        tt0 = make_video(username="user1", platform="tiktok", length=30,
                         created_at_str="2026-02-20T10:00:00+00:00", ad_link="tt0")
        tt1 = make_video(username="user1", platform="tiktok", length=45,
                         created_at_str="2026-02-20T14:00:00+00:00", ad_link="tt1")

        ig0 = make_video(username="user1", platform="instagram", length=45,
                         created_at_str="2026-02-20T10:30:00+00:00", ad_link="ig0")
        ig1 = make_video(username="user1", platform="instagram", length=30,
                         created_at_str="2026-02-20T11:00:00+00:00", ad_link="ig1")

        payout_units, exceptions = match_videos([tt0, tt1, ig0, ig1], tt_map, ig_map)

        # TT[0](30s)+IG[1](30s) via fallback, TT[1](45s)+IG[0](45s) via fallback
        # Both should be fallback pairs, total 2 pairs, 0 unpaired
        assert len(payout_units) == 2
        unpaired_exceptions = [
            e for e in exceptions
            if e.reason == "Only posted on one platform"
        ]
        assert len(unpaired_exceptions) == 0


# ===========================================================================
# 4. TestMatcherWithAllIdenticalVideos
# ===========================================================================

class TestMatcherWithAllIdenticalVideos:
    """
    When many videos have identical properties, the matcher must still
    produce correct, deterministic output without confusion or duplicates.
    """

    def test_all_same_length_same_time(self):
        """
        5 TT + 5 IG, ALL 30s, ALL created at the same time.
        Sequence match should pair them 1:1 since all lengths match.
        """
        tt_map = {"user1": "Creator"}
        ig_map = {"user1": "Creator"}

        videos = []
        for i in range(5):
            videos.append(make_video(
                username="user1", platform="tiktok", length=30, views=5000,
                created_at_str="2026-02-20T10:00:00+00:00",
                ad_link=f"tt_{i}",
            ))
            videos.append(make_video(
                username="user1", platform="instagram", length=30, views=3000,
                created_at_str="2026-02-20T10:00:00+00:00",
                ad_link=f"ig_{i}",
            ))

        payout_units, _ = match_videos(videos, tt_map, ig_map)

        assert len(payout_units) == 5
        # All should be sequence matches
        assert all(pu.match_method == "sequence" for pu in payout_units)

    def test_all_same_length_different_times(self):
        """
        3 TT + 3 IG, all 30s but different times.
        Should still pair by sequence position since lengths all match.
        """
        tt_map = {"user1": "Creator"}
        ig_map = {"user1": "Creator"}

        videos = [
            make_video(username="user1", platform="tiktok", length=30,
                       created_at_str="2026-02-20T08:00:00+00:00", ad_link="tt0"),
            make_video(username="user1", platform="tiktok", length=30,
                       created_at_str="2026-02-20T10:00:00+00:00", ad_link="tt1"),
            make_video(username="user1", platform="tiktok", length=30,
                       created_at_str="2026-02-20T14:00:00+00:00", ad_link="tt2"),
            make_video(username="user1", platform="instagram", length=30,
                       created_at_str="2026-02-20T09:00:00+00:00", ad_link="ig0"),
            make_video(username="user1", platform="instagram", length=30,
                       created_at_str="2026-02-20T12:00:00+00:00", ad_link="ig1"),
            make_video(username="user1", platform="instagram", length=30,
                       created_at_str="2026-02-20T15:00:00+00:00", ad_link="ig2"),
        ]

        payout_units, _ = match_videos(videos, tt_map, ig_map)
        assert len(payout_units) == 3

    def test_swapped_order_same_length(self):
        """
        TT created before IG but in different chronological order across
        platforms. After sorting by created_at, same-length pairs should
        still succeed.
        """
        tt_map = {"user1": "Creator"}
        ig_map = {"user1": "Creator"}

        # TT: 08:00, 12:00  |  IG: 06:00, 14:00
        # After sort: TT=[08, 12], IG=[06, 14]
        # Sequence: TT#0(08)+IG#0(06) same length, TT#1(12)+IG#1(14) same length
        videos = [
            make_video(username="user1", platform="tiktok", length=30,
                       created_at_str="2026-02-20T08:00:00+00:00", ad_link="tt0"),
            make_video(username="user1", platform="tiktok", length=30,
                       created_at_str="2026-02-20T12:00:00+00:00", ad_link="tt1"),
            make_video(username="user1", platform="instagram", length=30,
                       created_at_str="2026-02-20T06:00:00+00:00", ad_link="ig0"),
            make_video(username="user1", platform="instagram", length=30,
                       created_at_str="2026-02-20T14:00:00+00:00", ad_link="ig1"),
        ]

        payout_units, _ = match_videos(videos, tt_map, ig_map)
        assert len(payout_units) == 2
        assert all(pu.match_method == "sequence" for pu in payout_units)


# ===========================================================================
# 5. TestPayoutMathEdgeCases
# ===========================================================================

class TestPayoutMathEdgeCases:
    """
    Adversarial tests for the payout tier calculation, probing monotonicity,
    type correctness, boundary conditions, and extreme inputs.
    """

    def test_payout_returns_float_not_int(self):
        """Even for round numbers, payout should be a float (e.g., 35.0 not 35)."""
        result = calculate_payout(5000)
        assert isinstance(result, float), f"Expected float, got {type(result)}"
        assert result == 35.0

    def test_effective_views_never_exceeds_cap(self):
        """
        Property test: for any views value, effective_views should
        never exceed VIEW_CAP (10M).
        """
        test_values = [0, 1, 999, 1000, 10_000, 100_000, 1_000_000,
                       9_999_999, 10_000_000, 10_000_001, 50_000_000,
                       100_000_000, 999_999_999]

        for views in test_values:
            effective = calculate_effective_views(views)
            assert effective <= VIEW_CAP, (
                f"effective_views ({effective}) exceeded VIEW_CAP ({VIEW_CAP}) "
                f"for input {views}"
            )

    def test_payout_monotonically_non_decreasing(self):
        """
        For any views1 < views2, payout(views1) <= payout(views2).
        Higher views should NEVER produce a lower payout.
        """
        # Test a wide range of values, including all tier boundaries
        test_points = sorted(set([
            0, 1, 500, 999, 1000, 1001, 5000, 9999, 10000, 10001,
            49999, 50000, 50001, 99999, 100000, 100001, 249999, 250000,
            250001, 499999, 500000, 500001, 999999, 1000000, 1000001,
            1999999, 2000000, 2000001, 2999999, 3000000, 3000001,
            3999999, 4000000, 4000001, 4999999, 5000000, 5000001,
            5999999, 6000000, 6000001, 6999999, 7000000, 7000001,
            8000000, 9000000, 9999999, 10000000, 10000001, 15000000,
        ]))

        prev_payout = -1.0
        for views in test_points:
            effective = calculate_effective_views(views)
            payout = calculate_payout(effective)
            assert payout >= prev_payout, (
                f"Payout decreased! views={views}, effective={effective}, "
                f"payout={payout} < prev_payout={prev_payout}"
            )
            prev_payout = payout

    def test_view_cap_exactly_at_boundary(self):
        """
        10,000,000 views -> effective=10,000,000 (not capped).
        10,000,001 views -> effective=10,000,000 (capped).
        """
        assert calculate_effective_views(10_000_000) == 10_000_000
        assert calculate_effective_views(10_000_001) == 10_000_000

    def test_view_cap_just_below_boundary(self):
        """9,999,999 views should NOT be capped."""
        assert calculate_effective_views(9_999_999) == 9_999_999

    def test_negative_views_handling(self):
        """
        Negative views should not happen in production but should not crash
        the system. calculate_effective_views should pass them through
        (or handle gracefully), and calculate_payout should return $0.
        """
        effective = calculate_effective_views(-100)
        # Negative views should be below the cap, so passed through
        assert effective == -100

        # Negative effective views are below QUALIFICATION_THRESHOLD -> $0
        payout = calculate_payout(-100)
        assert payout == 0.0

    def test_zero_views(self):
        """Zero views: below qualification, payout is $0."""
        assert calculate_effective_views(0) == 0
        assert calculate_payout(0) == 0.0

    def test_payout_is_deterministic(self):
        """Same input must always produce the same output."""
        for views in [0, 500, 1000, 50000, 1000000, 6500000, 10000000]:
            result1 = calculate_payout(calculate_effective_views(views))
            result2 = calculate_payout(calculate_effective_views(views))
            assert result1 == result2, (
                f"Non-deterministic payout for views={views}: {result1} vs {result2}"
            )

    def test_every_tier_boundary_min_value(self):
        """
        The minimum value of each tier should produce the correct payout.
        Verifies no off-by-one errors at tier transitions.
        """
        boundary_tests = [
            (0, 0.0),          # below qualification
            (999, 0.0),        # just below qualification
            (1000, 35.0),      # first qualified tier
            (10000, 50.0),
            (50000, 100.0),
            (100000, 150.0),
            (250000, 300.0),
            (500000, 500.0),
            (1000000, 700.0),
            (2000000, 900.0),
            (3000000, 1100.0),
            (4000000, 1300.0),
            (5000000, 1500.0),
            (6000000, 1650.0),  # formula: 1500 + 150*(6-5) = 1650
            (7000000, 1800.0),  # formula: 1500 + 150*(7-5) = 1800
            (8000000, 1950.0),
            (9000000, 2100.0),
            (10000000, 2250.0), # formula: 1500 + 150*(10-5) = 2250
        ]
        for views, expected_payout in boundary_tests:
            actual = calculate_payout(views)
            assert actual == expected_payout, (
                f"Tier boundary error: views={views}, "
                f"expected=${expected_payout}, got=${actual}"
            )

    def test_payout_at_6m_formula_tier_intermediate_values(self):
        """
        Between 6M and 7M, floor_millions=6, so payout=1500+150*1=1650.
        At 6,999,999 it should still be 1650 (floor_millions=6).
        """
        assert calculate_payout(6_000_000) == 1650.0
        assert calculate_payout(6_500_000) == 1650.0
        assert calculate_payout(6_999_999) == 1650.0


# ===========================================================================
# 6. TestCreatorSummaryIsolation
# ===========================================================================

class TestCreatorSummaryIsolation:
    """
    Verify that creator summaries are correctly isolated per creator,
    and that edge cases in naming, counts, and aggregation are handled.
    """

    def test_two_creators_same_name_different_case(self):
        """
        'Alice' and 'alice' should be treated as separate creators since
        creator_name is set by the mapping and preserved as-is.
        """
        dummy_tt = make_video(username="u", platform="tiktok", ad_link="tt_dummy")
        dummy_ig = make_video(username="u", platform="instagram", ad_link="ig_dummy")
        units = [
            PayoutUnit(creator_name="Alice", chosen_views=5000,
                       payout_amount=35.0,
                       tiktok_video=dummy_tt, instagram_video=dummy_ig),
            PayoutUnit(creator_name="alice", chosen_views=50000,
                       payout_amount=100.0,
                       tiktok_video=dummy_tt, instagram_video=dummy_ig),
        ]

        summaries = build_creator_summaries(units)
        assert len(summaries) == 2
        names = {s.creator_name for s in summaries}
        assert "Alice" in names
        assert "alice" in names

        alice_upper = next(s for s in summaries if s.creator_name == "Alice")
        alice_lower = next(s for s in summaries if s.creator_name == "alice")

        assert alice_upper.total_payout == 35.0
        assert alice_lower.total_payout == 100.0

    def test_creator_with_100_videos(self):
        """
        One creator with 100 payout units, each worth $35 (1K-9999 views).
        Total should be $3,500.
        """
        dummy_tt = make_video(username="u", platform="tiktok", ad_link="tt_dummy")
        dummy_ig = make_video(username="u", platform="instagram", ad_link="ig_dummy")
        units = []
        for i in range(100):
            units.append(PayoutUnit(
                creator_name="Prolific Creator",
                chosen_views=5000,
                payout_amount=35.0,
                tiktok_video=dummy_tt,
                instagram_video=dummy_ig,
            ))

        summaries = build_creator_summaries(units)
        assert len(summaries) == 1
        summary = summaries[0]
        assert summary.total_payout == 3500.0
        assert summary.qualified_video_count == 100
        assert summary.paired_video_count == 100

    def test_summary_with_only_unqualified_videos(self):
        """
        Creator has 10 videos all with <1K views.
        qualified_video_count=0, total_payout=$0.
        """
        dummy_tt = make_video(username="u", platform="tiktok", ad_link="tt_dummy")
        dummy_ig = make_video(username="u", platform="instagram", ad_link="ig_dummy")
        units = [
            PayoutUnit(creator_name="LowViews", chosen_views=500,
                       payout_amount=0.0,
                       tiktok_video=dummy_tt, instagram_video=dummy_ig)
            for _ in range(10)
        ]

        summaries = build_creator_summaries(units)
        assert len(summaries) == 1
        summary = summaries[0]
        assert summary.qualified_video_count == 0
        assert summary.total_payout == 0.0

    def test_exception_count_does_not_affect_payout(self):
        """
        exception_count on summary is informational only -- it does not
        reduce payout. A creator with exceptions should still get their
        full payout from qualified payout units.
        """
        dummy_tt = make_video(username="u", platform="tiktok", ad_link="tt_dummy")
        dummy_ig = make_video(username="u", platform="instagram", ad_link="ig_dummy")
        units = [
            PayoutUnit(creator_name="Creator", chosen_views=50000,
                       payout_amount=100.0,
                       tiktok_video=dummy_tt, instagram_video=dummy_ig),
            PayoutUnit(creator_name="Creator", chosen_views=100000,
                       payout_amount=150.0,
                       tiktok_video=dummy_tt, instagram_video=dummy_ig),
        ]
        exception_counts = {"Creator": 5}

        summaries = build_creator_summaries(units, exception_counts)
        assert len(summaries) == 1
        summary = summaries[0]
        assert summary.total_payout == 250.0  # 100 + 150
        assert summary.exception_count == 5

    def test_summary_sorted_by_creator_name(self):
        """Summaries should be sorted alphabetically by creator_name."""
        dummy_tt = make_video(username="u", platform="tiktok", ad_link="tt_dummy")
        dummy_ig = make_video(username="u", platform="instagram", ad_link="ig_dummy")
        units = [
            PayoutUnit(creator_name="Zara", chosen_views=5000, payout_amount=35.0,
                       tiktok_video=dummy_tt, instagram_video=dummy_ig),
            PayoutUnit(creator_name="Alice", chosen_views=5000, payout_amount=35.0,
                       tiktok_video=dummy_tt, instagram_video=dummy_ig),
            PayoutUnit(creator_name="Mia", chosen_views=5000, payout_amount=35.0,
                       tiktok_video=dummy_tt, instagram_video=dummy_ig),
        ]

        summaries = build_creator_summaries(units)
        names = [s.creator_name for s in summaries]
        assert names == ["Alice", "Mia", "Zara"]


# ===========================================================================
# 7. TestEmptyAndNullInputs
# ===========================================================================

class TestEmptyAndNullInputs:
    """
    Edge cases around empty lists, empty maps, and None fields that
    could cause IndexError, KeyError, or NoneType errors.
    """

    def test_empty_videos_empty_maps(self):
        """No videos, no creators -> empty results."""
        payout_units, exceptions = match_videos([], {}, {})
        assert payout_units == []
        assert exceptions == []

    def test_videos_but_empty_maps(self):
        """Videos exist but no handle mappings -> all become exceptions."""
        videos = [
            make_video(username="user1", platform="tiktok", ad_link="tt1"),
            make_video(username="user2", platform="instagram", ad_link="ig1"),
        ]

        payout_units, exceptions = match_videos(videos, {}, {})
        assert len(payout_units) == 0
        assert len(exceptions) == 2
        assert all(e.reason == "Not in creator status list" for e in exceptions)

    def test_maps_but_no_videos(self):
        """Creator mappings exist but no videos -> empty results."""
        tt_map = {"handle1": "Creator1"}
        ig_map = {"handle2": "Creator2"}

        payout_units, exceptions = match_videos([], tt_map, ig_map)
        assert payout_units == []
        assert exceptions == []

    def test_video_with_all_none_optional_fields(self):
        """
        Video with only required fields (username, platform, ad_link)
        and all optional fields as None. Should not crash the matcher.
        """
        minimal_video = Video(
            username="user1",
            platform="tiktok",
            ad_link="https://tiktok.com/minimal",
        )
        tt_map = {"user1": "Creator"}

        payout_units, exceptions = match_videos([minimal_video], tt_map, {})
        # No IG to pair with -> 0 payout units, 1 unpaired exception
        assert len(payout_units) == 0
        unpaired_exceptions = [
            e for e in exceptions
            if e.reason == "Only posted on one platform"
        ]
        assert len(unpaired_exceptions) == 1

    def test_video_with_none_video_length_sequence_pair(self):
        """
        If either video in a sequence pair has video_length=None, the pair
        should be rejected (length_diff returns None) and go to fallback.
        Since IG has None length, fallback also can't match. Both go to exceptions.
        """
        tt_map = {"user1": "Creator"}
        ig_map = {"user1": "Creator"}

        tt = make_video(username="user1", platform="tiktok", length=30,
                        created_at_str="2026-02-20T10:00:00+00:00", ad_link="tt0")
        ig = Video(
            username="user1", platform="instagram",
            ad_link="ig0",
            uploaded_at=date(2026, 2, 20),
            created_at=datetime(2026, 2, 20, 10, 30, 0, tzinfo=timezone.utc),
            video_length=None,  # None length
            latest_views=5000,
        )

        payout_units, exceptions = match_videos([tt, ig], tt_map, ig_map)
        # Both should be unpaired → in exceptions, not in payout_units
        assert len(payout_units) == 0
        unpaired_exceptions = [
            e for e in exceptions
            if e.reason == "Only posted on one platform"
        ]
        assert len(unpaired_exceptions) == 2

    def test_empty_process_payouts(self):
        """process_payouts with empty list should return empty list."""
        result = process_payouts([])
        assert result == []

    def test_empty_build_creator_summaries(self):
        """build_creator_summaries with empty list should return empty list."""
        result = build_creator_summaries([])
        assert result == []


# ===========================================================================
# 8. TestLargeScaleStress
# ===========================================================================

class TestLargeScaleStress:
    """
    Large-scale tests to verify correctness at volume and catch
    O(n^2) or worse performance issues.
    """

    def test_100_creators_500_videos(self):
        """
        100 creators, each with 3 TT + 2 IG = 500 total videos.
        All same length so sequence match works. Verify correct totals.
        """
        tt_map = {}
        ig_map = {}
        videos = []

        for c in range(100):
            creator_name = f"Creator_{c:03d}"
            tt_handle = f"tt_handle_{c}"
            ig_handle = f"ig_handle_{c}"
            tt_map[tt_handle] = creator_name
            ig_map[ig_handle] = creator_name

            for v in range(3):
                videos.append(make_video(
                    username=tt_handle, platform="tiktok", length=30,
                    views=5000,
                    created_at_str=f"2026-02-20T{10+v:02d}:00:00+00:00",
                    ad_link=f"tt_{c}_{v}",
                ))
            for v in range(2):
                videos.append(make_video(
                    username=ig_handle, platform="instagram", length=30,
                    views=3000,
                    created_at_str=f"2026-02-20T{10+v:02d}:30:00+00:00",
                    ad_link=f"ig_{c}_{v}",
                ))

        payout_units, exceptions = match_videos(videos, tt_map, ig_map)

        # Per creator: 2 pairs (payout units), 1 unpaired TT → exception
        # 100 creators * 2 = 200 payout units
        assert len(payout_units) == 200

        # 100 unpaired TT videos → exceptions
        unpaired_exceptions = [
            e for e in exceptions
            if e.reason == "Only posted on one platform"
        ]
        assert len(unpaired_exceptions) == 100

    def test_single_creator_50_pairs(self):
        """
        50 TT + 50 IG, all same length -> 50 pairs, 0 unpaired.
        """
        tt_map = {"user1": "BigCreator"}
        ig_map = {"user1": "BigCreator"}

        videos = []
        for i in range(50):
            hour = 8 + (i // 4)
            minute = (i % 4) * 15
            videos.append(make_video(
                username="user1", platform="tiktok", length=30, views=10000,
                created_at_str=f"2026-02-20T{hour:02d}:{minute:02d}:00+00:00",
                ad_link=f"tt_{i}",
            ))
            videos.append(make_video(
                username="user1", platform="instagram", length=30, views=8000,
                created_at_str=f"2026-02-20T{hour:02d}:{minute:02d}:30+00:00",
                ad_link=f"ig_{i}",
            ))

        payout_units, _ = match_videos(videos, tt_map, ig_map)
        assert len(payout_units) == 50
        assert all(pu.match_method == "sequence" for pu in payout_units)


# ===========================================================================
# 9. TestChosenViewsSelection
# ===========================================================================

class TestChosenViewsSelection:
    """
    Tests for the chosen_views calculation on PayoutUnits,
    particularly edge cases around zero, None, and equal views.
    """

    def test_paired_tiktok_zero_views_ig_has_views(self):
        """TT has 0 views, IG has 50K -> chosen_views = 50K (max(0, 50000))."""
        tt_map = {"user1": "Creator"}
        ig_map = {"user1": "Creator"}

        tt = make_video(username="user1", platform="tiktok", length=30,
                        views=0,
                        created_at_str="2026-02-20T10:00:00+00:00", ad_link="tt0")
        ig = make_video(username="user1", platform="instagram", length=30,
                        views=50000,
                        created_at_str="2026-02-20T10:30:00+00:00", ad_link="ig0")

        payout_units, _ = match_videos([tt, ig], tt_map, ig_map)
        assert len(payout_units) == 1
        assert payout_units[0].chosen_views == 50000
        assert payout_units[0].best_platform == "instagram"

    def test_paired_both_none_views(self):
        """
        Both videos have latest_views=None. The code does `video.latest_views or 0`,
        so both become 0. chosen_views = max(0, 0) = 0.
        """
        tt_map = {"user1": "Creator"}
        ig_map = {"user1": "Creator"}

        tt = Video(
            username="user1", platform="tiktok",
            ad_link="tt_none_views",
            uploaded_at=date(2026, 2, 20),
            created_at=datetime(2026, 2, 20, 10, 0, 0, tzinfo=timezone.utc),
            video_length=30,
            latest_views=None,
        )
        ig = Video(
            username="user1", platform="instagram",
            ad_link="ig_none_views",
            uploaded_at=date(2026, 2, 20),
            created_at=datetime(2026, 2, 20, 10, 30, 0, tzinfo=timezone.utc),
            video_length=30,
            latest_views=None,
        )

        payout_units, _ = match_videos([tt, ig], tt_map, ig_map)
        assert len(payout_units) == 1
        assert payout_units[0].chosen_views == 0

    def test_paired_equal_views_picks_tiktok_as_best(self):
        """
        When TT == IG views, best_platform should be 'tiktok' since
        the code uses `if tt_views >= ig_views: best_platform = 'tiktok'`.
        """
        tt_map = {"user1": "Creator"}
        ig_map = {"user1": "Creator"}

        tt = make_video(username="user1", platform="tiktok", length=30,
                        views=10000,
                        created_at_str="2026-02-20T10:00:00+00:00", ad_link="tt0")
        ig = make_video(username="user1", platform="instagram", length=30,
                        views=10000,
                        created_at_str="2026-02-20T10:30:00+00:00", ad_link="ig0")

        payout_units, _ = match_videos([tt, ig], tt_map, ig_map)
        assert len(payout_units) == 1
        assert payout_units[0].best_platform == "tiktok"
        assert payout_units[0].chosen_views == 10000

    def test_unpaired_video_goes_to_exceptions(self):
        """
        An unpaired TT video (no IG partner) goes to exceptions,
        not payout_units. Unpaired videos get no payout.
        """
        tt_map = {"user1": "Creator"}

        tt = make_video(username="user1", platform="tiktok", length=30,
                        views=7777,
                        created_at_str="2026-02-20T10:00:00+00:00", ad_link="tt0")

        payout_units, exceptions = match_videos([tt], tt_map, {})
        assert len(payout_units) == 0
        unpaired_exceptions = [
            e for e in exceptions
            if e.reason == "Only posted on one platform"
        ]
        assert len(unpaired_exceptions) == 1
        assert unpaired_exceptions[0].latest_views == 7777


# ===========================================================================
# 10. TestSpecRegressions
# ===========================================================================

class TestSpecRegressions:
    """
    Regression tests for specific spec requirements that could easily
    be broken by refactoring.
    """

    def test_qualification_threshold_exactly_1000(self):
        """1000 views -> qualified, 999 -> not."""
        assert calculate_payout(1000) == 35.0
        assert calculate_payout(999) == 0.0

    def test_10m_cap_preserves_chosen_views(self):
        """
        After capping, chosen_views on the PayoutUnit should still be the
        original uncapped value. Only effective_views is capped.
        """
        dummy_tt = make_video(username="u", platform="tiktok", ad_link="tt_dummy")
        dummy_ig = make_video(username="u", platform="instagram", ad_link="ig_dummy")
        unit = PayoutUnit(
            creator_name="Creator",
            chosen_views=15_000_000,
            tiktok_video=dummy_tt,
            instagram_video=dummy_ig,
        )
        processed = process_payouts([unit])
        assert processed[0].chosen_views == 15_000_000  # preserved
        assert processed[0].effective_views == 10_000_000  # capped

    def test_unpaired_video_appears_only_in_exceptions(self):
        """
        An unpaired video (no cross-platform match) should appear ONLY
        in exceptions, NOT in payout_units.
        """
        tt_map = {"user1": "Creator"}

        tt = make_video(username="user1", platform="tiktok", length=30,
                        views=5000,
                        created_at_str="2026-02-20T10:00:00+00:00",
                        ad_link="tt_solo")

        payout_units, exceptions = match_videos([tt], tt_map, {})

        # Should NOT be in payout_units (unpaired videos get no payout)
        assert len(payout_units) == 0

        # Should be in exceptions
        assert len(exceptions) == 1
        assert exceptions[0].reason == "Only posted on one platform"
        assert exceptions[0].ad_link == "tt_solo"

    def test_unmapped_video_only_in_exceptions_not_payout_units(self):
        """
        Unmapped videos (no handle in creator list) should appear ONLY in
        exceptions, NOT in payout_units.
        """
        # No mapping for this user
        tt = make_video(username="unknown_user", platform="tiktok", length=30,
                        views=5000,
                        created_at_str="2026-02-20T10:00:00+00:00",
                        ad_link="tt_unmapped")

        payout_units, exceptions = match_videos([tt], {}, {})

        assert len(payout_units) == 0
        assert len(exceptions) == 1
        assert exceptions[0].reason == "Not in creator status list"

    def test_case_insensitive_username_lookup(self):
        """
        Username lookup should be case-insensitive. 'UserOne' should match
        a mapping keyed by 'userone'.
        """
        tt_map = {"userone": "Creator One"}

        tt = make_video(username="UserOne", platform="tiktok", length=30,
                        created_at_str="2026-02-20T10:00:00+00:00",
                        ad_link="tt_case")

        payout_units, exceptions = match_videos([tt], tt_map, {})
        # Should map successfully (not "Not in creator status list"), but unpaired → exception
        assert len(payout_units) == 0
        assert len(exceptions) == 1
        assert exceptions[0].reason == "Only posted on one platform"

    def test_username_with_leading_trailing_whitespace(self):
        """
        Usernames with leading/trailing whitespace should be stripped
        before lookup. The video maps successfully but is unpaired (no IG).
        """
        tt_map = {"user1": "Creator"}

        tt = make_video(username="  user1  ", platform="tiktok", length=30,
                        created_at_str="2026-02-20T10:00:00+00:00",
                        ad_link="tt_whitespace")

        payout_units, exceptions = match_videos([tt], tt_map, {})
        # Mapped successfully (not "Not in creator status list") but unpaired → exception
        assert len(payout_units) == 0
        assert len(exceptions) == 1
        assert exceptions[0].reason == "Only posted on one platform"

    def test_fallback_matches_across_different_uploaded_at_dates(self):
        """
        Fallback matching uses exact length + phash only, with no
        uploaded_at date requirement. Videos with different uploaded_at
        dates but same length should match via fallback.
        """
        tt_map = {"user1": "Creator"}
        ig_map = {"user1": "Creator"}

        tt = make_video(username="user1", platform="tiktok", length=30,
                        created_at_str="2026-02-20T10:00:00+00:00",
                        uploaded_at_date=date(2026, 2, 20),
                        ad_link="tt0")
        # IG at sequence position: different length -> triggers fallback
        ig_wrong_len = make_video(username="user1", platform="instagram", length=60,
                                   created_at_str="2026-02-20T09:00:00+00:00",
                                   uploaded_at_date=date(2026, 2, 20),
                                   ad_link="ig_wrong")
        # IG fallback candidate: same length, different uploaded_at
        ig_diff_date = make_video(username="user1", platform="instagram", length=30,
                                   created_at_str="2026-02-20T10:30:00+00:00",
                                   uploaded_at_date=date(2026, 2, 21),  # different date
                                   ad_link="ig_diff")

        payout_units, _ = match_videos([tt, ig_wrong_len, ig_diff_date], tt_map, ig_map)
        fallback_pairs = [
            pu for pu in payout_units
            if pu.match_note and "fallback" in pu.match_note
        ]
        assert len(fallback_pairs) == 1, (
            "Fallback should match regardless of uploaded_at date differences"
        )

    def test_fallback_works_when_source_has_no_uploaded_at(self):
        """
        Fallback uses exact length + phash only. uploaded_at is not checked,
        so even a video with uploaded_at=None can match via fallback.
        """
        tt_map = {"user1": "Creator"}
        ig_map = {"user1": "Creator"}

        tt = Video(
            username="user1", platform="tiktok",
            ad_link="tt_no_upload_date",
            uploaded_at=None,  # no uploaded_at
            created_at=datetime(2026, 2, 20, 10, 0, 0, tzinfo=timezone.utc),
            video_length=30,
            latest_views=5000,
        )
        # IG at sequence position: different length -> fallback
        ig_wrong = make_video(username="user1", platform="instagram", length=60,
                               created_at_str="2026-02-20T09:00:00+00:00",
                               ad_link="ig_wrong")
        # IG that should match via fallback (same length, phash passes)
        ig_match = make_video(username="user1", platform="instagram", length=30,
                               created_at_str="2026-02-20T10:30:00+00:00",
                               ad_link="ig_match")

        payout_units, _ = match_videos([tt, ig_wrong, ig_match], tt_map, ig_map)
        fallback_pairs = [
            pu for pu in payout_units
            if pu.match_note and "fallback" in pu.match_note
        ]
        assert len(fallback_pairs) == 1, (
            "Fallback should work even when source has no uploaded_at"
        )

    def test_process_payouts_fills_effective_views_and_payout_amount(self):
        """
        process_payouts should fill in both effective_views and
        payout_amount on each PayoutUnit.
        """
        dummy_tt = make_video(username="u", platform="tiktok", ad_link="tt_dummy")
        dummy_ig = make_video(username="u", platform="instagram", ad_link="ig_dummy")
        unit = PayoutUnit(
            creator_name="Creator",
            chosen_views=250_000,
            tiktok_video=dummy_tt,
            instagram_video=dummy_ig,
        )
        assert unit.effective_views == 0
        assert unit.payout_amount == 0.0

        processed = process_payouts([unit])
        assert processed[0].effective_views == 250_000
        assert processed[0].payout_amount == 300.0

    def test_run_payout_pipeline_returns_consistent_data(self):
        """
        run_payout_pipeline should return payout_units with filled values
        AND matching creator summaries.
        """
        dummy_tt = make_video(username="u", platform="tiktok", ad_link="tt_dummy")
        dummy_ig = make_video(username="u", platform="instagram", ad_link="ig_dummy")
        units = [
            PayoutUnit(creator_name="A", chosen_views=50000,
                       tiktok_video=dummy_tt, instagram_video=dummy_ig),
            PayoutUnit(creator_name="A", chosen_views=100000,
                       tiktok_video=dummy_tt, instagram_video=dummy_ig),
            PayoutUnit(creator_name="B", chosen_views=500,
                       tiktok_video=dummy_tt, instagram_video=dummy_ig),
        ]

        processed, summaries = run_payout_pipeline(units, {"B": 1})

        # Check A's summary
        a_summary = next(s for s in summaries if s.creator_name == "A")
        assert a_summary.qualified_video_count == 2
        assert a_summary.total_payout == 100.0 + 150.0  # 50K + 100K tiers
        assert a_summary.paired_video_count == 2

        # Check B's summary
        b_summary = next(s for s in summaries if s.creator_name == "B")
        assert b_summary.qualified_video_count == 0
        assert b_summary.total_payout == 0.0
        assert b_summary.exception_count == 1

    def test_length_tolerance_allows_one_second_diff(self):
        """
        Sequence matching allows ±1 second tolerance.
        30s vs 31s SHOULD pair. 30s vs 32s should NOT.
        """
        tt_map = {"user1": "Creator"}
        ig_map = {"user1": "Creator"}

        # ±1 second: 30 vs 31 → should pair
        tt = make_video(username="user1", platform="tiktok", length=30,
                        created_at_str="2026-02-20T10:00:00+00:00", ad_link="tt0")
        ig = make_video(username="user1", platform="instagram", length=31,
                        created_at_str="2026-02-20T10:30:00+00:00", ad_link="ig0")

        payout_units, exceptions = match_videos([tt, ig], tt_map, ig_map)
        assert len(payout_units) == 1  # 30 vs 31 pairs (within ±1s)

        # >1 second diff: 30 vs 32 → should NOT pair
        tt2 = make_video(username="user1", platform="tiktok", length=30,
                         created_at_str="2026-02-20T10:00:00+00:00", ad_link="tt1")
        ig2 = make_video(username="user1", platform="instagram", length=32,
                         created_at_str="2026-02-20T10:30:00+00:00", ad_link="ig1")

        payout_units2, exceptions2 = match_videos([tt2, ig2], tt_map, ig_map)
        assert len(payout_units2) == 0
        unpaired_exceptions = [
            e for e in exceptions2
            if e.reason == "Only posted on one platform"
        ]
        assert len(unpaired_exceptions) == 2
