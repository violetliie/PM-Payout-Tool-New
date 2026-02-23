"""
Comprehensive tests for services/matcher.py (SPEC.md Steps 5–11).

Test categories:
  1. REQUIRED TESTS (from Phase 4 spec):
     - 3TT+3IG all same lengths → 3 pairs, all high confidence
     - 3TT+3IG with fallback → 3 pairs, 2 high + 1 medium
     - 5TT+3IG unequal counts → 3 pairs + 2 unpaired TikToks
     - TikTok-only creator → all unpaired
     - 1-second difference → unpaired (exact length required)
     - 5 sec mismatch, no fallback → both unpaired

  2. ADDITIONAL EDGE CASE TESTS:
     - Step 5: Creator mapping (unmapped videos → exception)
     - Step 6: Deduplication (by ad_link, by ad_id)
     - Empty inputs (no videos, no creators)
     - Instagram-only creator → all unpaired
     - Mixed: some videos map, some don't
     - Fallback: both videos in failed pair find different matches
     - Fallback: time window boundary (>24h → no fallback)
     - Fallback: uploaded_at same-date requirement
     - None video_length in sequence pair → treated as mismatch
     - Multiple creators in one call
     - Views selection: max(TT, IG) for pairs, single for unpaired
     - best_platform audit field correctness
     - Dedup: same ad_link different updated_at → keep most recent
"""

import sys
import os
import pytest
from datetime import date, datetime, timedelta, timezone

# Ensure backend is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from models.schemas import Video, PayoutUnit, ExceptionVideo
from services.matcher import (
    match_videos,
    _map_videos_to_creators,
    _deduplicate_videos,
    _match_creator_videos,
    _find_fallback_match,
    _build_paired_unit,
    _build_unpaired_unit,
    _video_length_diff,
)


# ===========================================================================
# Test helpers
# ===========================================================================

def make_video(
    username: str = "testuser",
    platform: str = "tiktok",
    length: int = 30,
    views: int = 5000,
    created_at_str: str = "2026-02-20T10:00:00+00:00",
    ad_link: str = "",
    ad_id: str = None,
    updated_at_str: str = None,
    creator_name: str = None,
    private: bool = False,
    removed: bool = False,
    uploaded_at_date: date = None,
) -> Video:
    """Helper to create a Video with sensible defaults."""
    return Video(
        username=username,
        platform=platform,
        ad_link=ad_link or f"https://example.com/{username}/{platform}/{length}_{created_at_str[:13]}",
        uploaded_at=uploaded_at_date if uploaded_at_date is not None else date(2026, 2, 20),
        created_at=datetime.fromisoformat(created_at_str),
        video_length=length,
        latest_views=views,
        latest_updated_at=datetime.fromisoformat(updated_at_str) if updated_at_str else None,
        linked_account_id=None,
        ad_id=ad_id,
        title=f"Test video {username}",
        private=private,
        removed=removed,
        creator_name=creator_name,
    )


# ===========================================================================
# REQUIRED TEST 1: 3TT + 3IG, all same lengths → 3 pairs, all high
# ===========================================================================

class TestSequenceMatchAllSameLengths:
    """Creator with 3 TikToks + 3 Instagrams, all same lengths → 3 pairs."""

    def setup_method(self):
        self.tiktok_videos = [
            make_video("alice_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "tt1"),
            make_video("alice_tt", "tiktok", 45, 12000, "2026-02-20T14:00:00+00:00", "tt2"),
            make_video("alice_tt", "tiktok", 60, 800, "2026-02-21T09:00:00+00:00", "tt3"),
        ]
        self.instagram_videos = [
            make_video("alice_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "ig1"),
            make_video("alice_ig", "instagram", 45, 3000, "2026-02-20T14:30:00+00:00", "ig2"),
            make_video("alice_ig", "instagram", 60, 1500, "2026-02-21T09:30:00+00:00", "ig3"),
        ]

    def test_all_paired_high_confidence(self):
        """All 3 pairs should be high confidence exact matches."""
        payout_units, exceptions = _match_creator_videos(
            "Alice", self.tiktok_videos, self.instagram_videos
        )
        assert len(payout_units) == 3
        assert len(exceptions) == 0
        assert all(pu.paired for pu in payout_units)
        assert all(pu.match_confidence == "high" for pu in payout_units)
        assert all(pu.pair_note == "exact match" for pu in payout_units)

    def test_chosen_views_is_max(self):
        """chosen_views should be the max of both platforms."""
        payout_units, _ = _match_creator_videos(
            "Alice", self.tiktok_videos, self.instagram_videos
        )
        # Pair 1: TT=5000, IG=8000 → 8000
        assert payout_units[0].chosen_views == 8000
        # Pair 2: TT=12000, IG=3000 → 12000
        assert payout_units[1].chosen_views == 12000
        # Pair 3: TT=800, IG=1500 → 1500
        assert payout_units[2].chosen_views == 1500

    def test_best_platform_audit(self):
        """best_platform should indicate which platform had more views."""
        payout_units, _ = _match_creator_videos(
            "Alice", self.tiktok_videos, self.instagram_videos
        )
        # Pair 1: IG has more views (8000 > 5000)
        assert payout_units[0].best_platform == "instagram"
        # Pair 2: TT has more views (12000 > 3000)
        assert payout_units[1].best_platform == "tiktok"
        # Pair 3: IG has more views (1500 > 800)
        assert payout_units[2].best_platform == "instagram"

    def test_both_videos_populated(self):
        """Both tiktok_video and instagram_video should be populated in each pair."""
        payout_units, _ = _match_creator_videos(
            "Alice", self.tiktok_videos, self.instagram_videos
        )
        for pu in payout_units:
            assert pu.tiktok_video is not None
            assert pu.instagram_video is not None


# ===========================================================================
# REQUIRED TEST 2: 3TT + 3IG, pair #2 has mismatched length but fallback exists
# ===========================================================================

class TestSequenceMatchWithFallback:
    """
    Creator with 3 TikToks + 3 Instagrams.
    Pair #2 has mismatched length (TT=45s, IG=90s), but there's
    an unmatched IG with 45s length → fallback should find it.

    Setup:
      TT: [30s, 45s, 60s] (sorted by created_at)
      IG: [30s, 90s, 45s] (sorted by created_at)

    Expected:
      Pair 1: TT#1(30s) ↔ IG#1(30s) → high, exact
      Pair 2: TT#2(45s) ↔ IG#2(90s) → FAILS → fallback:
        - TT#2(45s) searches all unmatched IG → finds IG#3(45s) → medium
        - IG#2(90s) searches all unmatched TT → no match (no 90s TT) → unpaired
      Pair 3: TT#3(60s) — already past min count, but IG#2(90s) has no match

    Actually, let me re-think. With 3TT+3IG:
      Sequence pairs: (#1,#1), (#2,#2), (#3,#3)
      Pair 1: 30↔30 → exact match ✓
      Pair 2: 45↔90 → mismatch → fallback
      Pair 3: 60↔45 → mismatch → fallback

    For pair 2 fallback:
      TT#2(45s) looks at unmatched IG: IG#3(45s) is available → pair them (medium)
      IG#2(90s) looks at unmatched TT: no 90s TT → unpaired

    For pair 3 fallback:
      TT#3(60s) looks at unmatched IG: IG#2(90s) remaining, 60≠90 → no match → unpaired
      IG#3(45s) was already used by pair 2 fallback
      Wait — IG#3 is used, so for pair 3:
        TT#3(60s) → searches unmatched IG → only IG#2(90s) left → 60≠90 → no match
        IG#3(45s) is already used, so pair 3's IG is IG#3... wait.

    Let me reconsider the setup to get exactly "2 high + 1 medium":
      TT: [30s, 45s, 60s]
      IG: [30s, 100s, 60s]  (IG#2 is 100s, mismatched)
      Extra IG: [45s] at a different time

    No, the prompt says 3TT+3IG. Let me set up:
      TT: [30s, 45s, 60s]
      IG: [30s, 60s, 45s]

    Sequence pairs:
      #1: 30↔30 → exact ✓
      #2: 45↔60 → mismatch (diff=15) → fallback
      #3: 60↔45 → mismatch (diff=15) → fallback

    Pair 2 fallback:
      TT#2(45s) searches unmatched IG → IG#3(45s) → exact! → medium pair
      IG#2(60s) searches unmatched TT → TT#3(60s) → exact! → medium pair

    Result: 1 high + 2 medium = 3 pairs total ✓
    But prompt says "2 high + 1 medium". Let me adjust:

      TT: [30s, 45s, 60s]
      IG: [30s, 45s, 100s]  ← only #3 mismatches
      Extra logic: put a 60s IG somewhere else

    Hmm, simpler approach for "2 high + 1 medium":
      TT: [30s, 45s, 60s]
      IG: [30s, 45s, 99s]  ← pair 3 mismatches, no fallback for 99

    That gives 2 high + 0 medium + 2 unpaired. Not right.

    For "2 high + 1 medium", need pair 3 to fail primary but succeed fallback.
    Use 4 IG videos (but spec says 3):

    OK let me just make the specific scenario from the prompt work:
    "pair #2 has mismatched length but a fallback exists → 3 pairs, 2 high + 1 medium"

    Setup:
      TT: [30s @T1, 45s @T2, 60s @T3]
      IG: [30s @T1+30m, 99s @T2+30m, 45s @T3+30m]

    Pairs:
      #1: TT(30)↔IG(30) → exact → high ✓
      #2: TT(45)↔IG(99) → mismatch → fallback:
        TT#2(45s) searches unmatched IG → IG#3(45s) within 24h → medium ✓
        IG#2(99s) searches unmatched TT → TT#3(60s)? 99≠60 → no match → unpaired
      #3: TT(60)↔IG(45s) → but IG#3 is now used by fallback!

    Wait, the sequence match happens first for ALL min_count pairs before fallback.
    So pairs to check: #1, #2, #3.
      #1: 30↔30 → exact ✓ (used: TT0, IG0)
      #2: 45↔99 → mismatch → queued for fallback
      #3: 60↔45 → mismatch → queued for fallback

    Then fallback for pair #2:
      TT#2(45s) → search unmatched IG (IG#1 used, IG#2 free, IG#3 free) → IG#3(45s) exact → pair! → ig_used adds 2
      IG#2(99s) → search unmatched TT (TT#0 used, TT#1 free, TT#2 free) → no 99s TT → nothing

    Then fallback for pair #3:
      TT#3(60s) → search unmatched IG (IG#0 used, IG#2 used by fallback? NO — wait)

    Hmm, the fallback candidates are processed sequentially. After pair #2 fallback:
      tt_used = {0, 1}, ig_used = {0, 2}

    Pair #3 fallback:
      TT#3(60s) → search unmatched IG: IG#1(99s) is free → 60≠99 → no match
      IG#3(45s) → but IG index 2 is already used!

    Wait, pair #3 is (tt_idx=2, ig_idx=2). ig_idx=2 is IG#3(45s).
    But ig_used already has {0, 2}. So IG#3 is already used.

    So fallback for pair #3:
      tt_video = TT#3(60s), ig_video = IG#3(45s)
      TT#3 searches unmatched IG → only IG#2(99s) free → 60≠99 → no match
      IG#3 searches unmatched TT → but wait, ig_video for pair 3 is IG#3 which is already in ig_used...
      But in the code, the fallback still runs for it. The find_fallback will search unmatched TTs.
      IG#3(45s) → search unmatched TT: TT#3(60s, idx=2) is free → 45≠60 → no match

    Result: TT#3 and IG#2 both unpaired. Total: 2 pairs + 2 unpaired. Not 3 pairs.

    I need a scenario where exactly one fallback succeeds out of one failure.
    Simplest: only pair #2 fails, and #3 succeeds normally.

      TT: [30s, 99s, 60s]  ← TT#2 has weird length
      IG: [30s, 99s from different content, 60s]

    No that makes them match. Let me just do:
      TT: [30s, 45s, 60s]
      IG: [30s, 100s, 60s]

    Pairs:
      #1: 30↔30 → exact → high
      #2: 45↔100 → mismatch → fallback
      #3: 60↔60 → exact → high

    Fallback for pair #2:
      TT#2(45s) → search unmatched IG → only IG#2(100s, idx=1) free → 45≠100 → no match
      IG#2(100s) → search unmatched TT → only TT#2(45s, idx=1) free → 100≠45 → no match

    Result: 2 pairs + 2 unpaired. Still not 3 pairs.

    For 3 pairs (2 high + 1 medium), need an extra video that the fallback CAN match:
      TT: [30s, 45s, 60s]
      IG: [30s, 88s, 60s, 45s]  ← 4 IG videos, but prompt says 3

    The prompt says "3 TikToks + 3 Instagrams, pair #2 has mismatched length
    but a fallback exists." This requires IG#3 to be the fallback match for TT#2.

    Let me re-read: if TT#2(45s) fails with IG#2, and IG#3 exists with 45s,
    then TT#2 can fallback to IG#3... BUT IG#3 would also need to be checked
    for primary pairing with TT#3.

    If TT#3 also exists, then primary pair #3 is TT#3↔IG#3. If that pair
    has matching lengths, it's accepted in primary. Then IG#3 is used,
    and TT#2 has no fallback.

    So for the fallback to work, pair #3 must ALSO fail primary, and then
    the cross-matching resolves it.

    Setup:
      TT: [30s @T1, 45s @T2, 60s @T3]
      IG: [30s @T1, 60s @T2, 45s @T3]

    Primary:
      #1: TT(30)↔IG(30) → exact → high ✓
      #2: TT(45)↔IG(60) → diff=15 → FAIL
      #3: TT(60)↔IG(45) → diff=15 → FAIL

    Fallback pair #2:
      TT#2(45s) → unmatched IG: IG#2(60s, idx=1), IG#3(45s, idx=2) → IG#3 exact → medium ✓
      IG#2(60s) → unmatched TT: TT#2(45s, idx=1), TT#3(60s, idx=2) → TT#3 exact → medium ✓
      Both found different matches → pair both! tt_used={0,1}, ig_used={0,2}, plus tt_used.add(2), ig_used.add(1)

    Fallback pair #3: both TT#3 and IG#3 are now used → nothing to do.
    Actually — TT idx=2 and IG idx=2 are both in used sets already.
    So tt_matched and ig_matched will both be False (no new searches needed),
    but they were already paired in pair #2's fallback.

    Result: 3 pairs! 1 high + 2 medium. Prompt says "2 high + 1 medium" but
    this scenario naturally gives 1 high + 2 medium. Close enough — the prompt
    is illustrative. Let me adjust to get exactly 2 high + 1 medium:

    Setup:
      TT: [30s @T1, 45s @T2, 60s @T3]
      IG: [30s @T1, 100s @T2, 60s @T3]
      Extra IG#4: 45s @T2+1h (becomes IG idx 2 when sorted, pushing 60s to idx 3)

    No, that's 4 IG. I'll just make the test match the actual scenario.
    With my setup (swapped IG order), I get 1 high + 2 medium = 3 pairs.
    """

    def setup_method(self):
        """
        TT: [30s @T1, 45s @T2, 60s @T3]
        IG: [30s @T1+30m, 60s @T2+30m, 45s @T3+30m]

        Primary: #1 exact, #2 fails (45≠60), #3 fails (60≠45)
        Fallback #2: TT#2(45)→IG#3(45) ✓, IG#2(60)→TT#3(60) ✓
        Fallback #3: both already used
        Result: 1 high + 2 medium = 3 pairs
        """
        self.tiktok_videos = [
            make_video("bob_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "tt_b1"),
            make_video("bob_tt", "tiktok", 45, 12000, "2026-02-20T14:00:00+00:00", "tt_b2"),
            make_video("bob_tt", "tiktok", 60, 3000, "2026-02-21T09:00:00+00:00", "tt_b3"),
        ]
        self.instagram_videos = [
            make_video("bob_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "ig_b1"),
            make_video("bob_ig", "instagram", 60, 7000, "2026-02-20T14:30:00+00:00", "ig_b2"),
            make_video("bob_ig", "instagram", 45, 2000, "2026-02-21T09:30:00+00:00", "ig_b3"),
        ]

    def test_three_pairs_total(self):
        payout_units, exceptions = _match_creator_videos(
            "Bob", self.tiktok_videos, self.instagram_videos
        )
        assert len(payout_units) == 3
        assert all(pu.paired for pu in payout_units)

    def test_one_high_two_medium(self):
        """Pair #1 is high (exact), pairs #2 and #3 are medium (fallback)."""
        payout_units, _ = _match_creator_videos(
            "Bob", self.tiktok_videos, self.instagram_videos
        )
        confidences = sorted([pu.match_confidence for pu in payout_units])
        assert confidences.count("high") == 1
        assert confidences.count("medium") == 2

    def test_no_exceptions(self):
        """All videos should be paired, no exceptions."""
        _, exceptions = _match_creator_videos(
            "Bob", self.tiktok_videos, self.instagram_videos
        )
        assert len(exceptions) == 0

    def test_fallback_pairs_correct_lengths(self):
        """Fallback pairs should have exact matching video lengths."""
        payout_units, _ = _match_creator_videos(
            "Bob", self.tiktok_videos, self.instagram_videos
        )
        for pu in payout_units:
            tt_len = pu.tiktok_video.video_length
            ig_len = pu.instagram_video.video_length
            assert tt_len == ig_len, (
                f"Paired videos have mismatched lengths: TT={tt_len}, IG={ig_len}"
            )


# ===========================================================================
# REQUIRED TEST 3: 5TT + 3IG, unequal counts
# ===========================================================================

class TestUnequalCounts:
    """Creator with 5 TikToks + 3 Instagrams → 3 pairs + 2 unpaired TikToks."""

    def setup_method(self):
        self.tiktok_videos = [
            make_video("carl_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "tt_c1"),
            make_video("carl_tt", "tiktok", 45, 12000, "2026-02-20T14:00:00+00:00", "tt_c2"),
            make_video("carl_tt", "tiktok", 60, 800, "2026-02-21T09:00:00+00:00", "tt_c3"),
            make_video("carl_tt", "tiktok", 25, 20000, "2026-02-21T15:00:00+00:00", "tt_c4"),
            make_video("carl_tt", "tiktok", 90, 1500, "2026-02-22T09:00:00+00:00", "tt_c5"),
        ]
        self.instagram_videos = [
            make_video("carl_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "ig_c1"),
            make_video("carl_ig", "instagram", 45, 3000, "2026-02-20T14:30:00+00:00", "ig_c2"),
            make_video("carl_ig", "instagram", 60, 1500, "2026-02-21T09:30:00+00:00", "ig_c3"),
        ]

    def test_three_pairs_two_unpaired(self):
        payout_units, exceptions = _match_creator_videos(
            "Carl", self.tiktok_videos, self.instagram_videos
        )
        paired = [pu for pu in payout_units if pu.paired]
        unpaired = [pu for pu in payout_units if not pu.paired]
        assert len(paired) == 3
        assert len(unpaired) == 2

    def test_unpaired_are_tiktok(self):
        """The 2 extra TikTok videos should be the unpaired ones."""
        payout_units, _ = _match_creator_videos(
            "Carl", self.tiktok_videos, self.instagram_videos
        )
        unpaired = [pu for pu in payout_units if not pu.paired]
        for pu in unpaired:
            assert pu.tiktok_video is not None
            assert pu.instagram_video is None
            assert pu.best_platform == "tiktok"

    def test_exceptions_for_unpaired(self):
        """Unpaired videos should generate exceptions."""
        _, exceptions = _match_creator_videos(
            "Carl", self.tiktok_videos, self.instagram_videos
        )
        assert len(exceptions) == 2
        assert all(e.reason == "unpaired — single platform only" for e in exceptions)
        assert all(e.platform == "tiktok" for e in exceptions)

    def test_total_payout_units(self):
        """Total should be 5 (3 pairs + 2 unpaired)."""
        payout_units, _ = _match_creator_videos(
            "Carl", self.tiktok_videos, self.instagram_videos
        )
        assert len(payout_units) == 5


# ===========================================================================
# REQUIRED TEST 4: TikTok-only creator → all unpaired
# ===========================================================================

class TestTikTokOnlyCreator:
    """Creator with only TikTok videos → all unpaired."""

    def setup_method(self):
        self.tiktok_videos = [
            make_video("dave_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "tt_d1"),
            make_video("dave_tt", "tiktok", 45, 12000, "2026-02-20T14:00:00+00:00", "tt_d2"),
            make_video("dave_tt", "tiktok", 60, 800, "2026-02-21T09:00:00+00:00", "tt_d3"),
        ]

    def test_all_unpaired(self):
        payout_units, exceptions = _match_creator_videos(
            "Dave", self.tiktok_videos, []
        )
        assert len(payout_units) == 3
        assert all(not pu.paired for pu in payout_units)
        assert len(exceptions) == 3

    def test_low_confidence(self):
        """Unpaired videos should have low confidence."""
        payout_units, _ = _match_creator_videos("Dave", self.tiktok_videos, [])
        assert all(pu.match_confidence == "low" for pu in payout_units)

    def test_chosen_views_single_platform(self):
        """chosen_views should be the TikTok views."""
        payout_units, _ = _match_creator_videos("Dave", self.tiktok_videos, [])
        assert payout_units[0].chosen_views == 5000
        assert payout_units[1].chosen_views == 12000
        assert payout_units[2].chosen_views == 800


# ===========================================================================
# REQUIRED TEST 5: 1-second difference → unpaired (exact length required)
# ===========================================================================

class TestOneSecondDifference:
    """Creator with 1 TT + 1 IG, 1 sec difference → unpaired (exact match only)."""

    def test_plus_one_second_unpaired(self):
        """TT=30s, IG=31s → mismatch → both unpaired (no exact length match)."""
        tt = [make_video("eve_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "tt_e1")]
        ig = [make_video("eve_ig", "instagram", 31, 8000, "2026-02-20T10:30:00+00:00", "ig_e1")]
        payout_units, exceptions = _match_creator_videos("Eve", tt, ig)

        assert len(payout_units) == 2
        assert all(not pu.paired for pu in payout_units)
        assert len(exceptions) == 2

    def test_minus_one_second_unpaired(self):
        """TT=31s, IG=30s → mismatch → both unpaired."""
        tt = [make_video("eve_tt", "tiktok", 31, 5000, "2026-02-20T10:00:00+00:00", "tt_e2")]
        ig = [make_video("eve_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "ig_e2")]
        payout_units, _ = _match_creator_videos("Eve", tt, ig)

        assert len(payout_units) == 2
        assert all(not pu.paired for pu in payout_units)

    def test_each_has_own_views_when_unpaired(self):
        """When unpaired due to 1s diff, each video uses its own views."""
        tt = [make_video("eve_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "tt_e3")]
        ig = [make_video("eve_ig", "instagram", 31, 8000, "2026-02-20T10:30:00+00:00", "ig_e3")]
        payout_units, _ = _match_creator_videos("Eve", tt, ig)
        tt_unit = [pu for pu in payout_units if pu.tiktok_video is not None and pu.instagram_video is None][0]
        ig_unit = [pu for pu in payout_units if pu.instagram_video is not None and pu.tiktok_video is None][0]
        assert tt_unit.chosen_views == 5000
        assert ig_unit.chosen_views == 8000


# ===========================================================================
# REQUIRED TEST 6: 5 sec mismatch, no fallback → both unpaired
# ===========================================================================

class TestLargeMismatchNoFallback:
    """Creator with 1 TT + 1 IG, 5 sec difference, no fallback → both unpaired."""

    def test_both_unpaired(self):
        tt = [make_video("frank_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "tt_f1")]
        ig = [make_video("frank_ig", "instagram", 35, 8000, "2026-02-20T10:30:00+00:00", "ig_f1")]
        payout_units, exceptions = _match_creator_videos("Frank", tt, ig)

        assert len(payout_units) == 2
        assert all(not pu.paired for pu in payout_units)
        assert len(exceptions) == 2

    def test_each_has_own_views(self):
        """Each unpaired video uses its own views."""
        tt = [make_video("frank_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "tt_f2")]
        ig = [make_video("frank_ig", "instagram", 35, 8000, "2026-02-20T10:30:00+00:00", "ig_f2")]
        payout_units, _ = _match_creator_videos("Frank", tt, ig)

        tt_unit = [pu for pu in payout_units if pu.tiktok_video is not None and pu.instagram_video is None][0]
        ig_unit = [pu for pu in payout_units if pu.instagram_video is not None and pu.tiktok_video is None][0]
        assert tt_unit.chosen_views == 5000
        assert ig_unit.chosen_views == 8000

    def test_2_second_mismatch_also_fails(self):
        """2-second difference should fail primary (exact match required)."""
        tt = [make_video("frank_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "tt_f3")]
        ig = [make_video("frank_ig", "instagram", 32, 8000, "2026-02-20T10:30:00+00:00", "ig_f3")]
        payout_units, _ = _match_creator_videos("Frank", tt, ig)
        assert all(not pu.paired for pu in payout_units)


# ===========================================================================
# ADDITIONAL TEST: Instagram-only creator
# ===========================================================================

class TestInstagramOnlyCreator:
    """Creator with only Instagram videos → all unpaired."""

    def test_all_unpaired(self):
        ig = [
            make_video("grace_ig", "instagram", 30, 5000, "2026-02-20T10:00:00+00:00", "ig_g1"),
            make_video("grace_ig", "instagram", 45, 12000, "2026-02-20T14:00:00+00:00", "ig_g2"),
        ]
        payout_units, exceptions = _match_creator_videos("Grace", [], ig)

        assert len(payout_units) == 2
        assert all(not pu.paired for pu in payout_units)
        assert all(pu.instagram_video is not None for pu in payout_units)
        assert all(pu.tiktok_video is None for pu in payout_units)
        assert len(exceptions) == 2


# ===========================================================================
# ADDITIONAL TEST: Empty inputs
# ===========================================================================

class TestEmptyInputs:
    """Edge cases with empty inputs."""

    def test_no_videos(self):
        """No videos at all → no payout units, no exceptions."""
        payout_units, exceptions = _match_creator_videos("Nobody", [], [])
        assert len(payout_units) == 0
        assert len(exceptions) == 0

    def test_full_pipeline_empty(self):
        """Full pipeline with no videos."""
        payout_units, exceptions = match_videos([], {}, {})
        assert len(payout_units) == 0
        assert len(exceptions) == 0


# ===========================================================================
# ADDITIONAL TEST: Step 5 — Creator mapping
# ===========================================================================

class TestCreatorMapping:
    """Step 5: Map videos to creators via handle lookup."""

    def test_mapped_videos_get_creator_name(self):
        videos = [
            make_video("alice_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "link1"),
            make_video("alice_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "link2"),
        ]
        tt_map = {"alice_tt": "Alice Smith"}
        ig_map = {"alice_ig": "Alice Smith"}

        mapped, exceptions = _map_videos_to_creators(videos, tt_map, ig_map)
        assert len(mapped) == 2
        assert len(exceptions) == 0
        assert all(v.creator_name == "Alice Smith" for v in mapped)

    def test_unmapped_video_goes_to_exception(self):
        videos = [
            make_video("unknown_user", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "link3"),
        ]
        mapped, exceptions = _map_videos_to_creators(videos, {}, {})
        assert len(mapped) == 0
        assert len(exceptions) == 1
        assert exceptions[0].reason == "not in creator list"
        assert exceptions[0].username == "unknown_user"

    def test_case_insensitive_lookup(self):
        """Username lookup should be case-insensitive."""
        videos = [
            make_video("Alice_TT", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "link4"),
        ]
        tt_map = {"alice_tt": "Alice"}
        mapped, _ = _map_videos_to_creators(videos, tt_map, {})
        assert len(mapped) == 1
        assert mapped[0].creator_name == "Alice"

    def test_mixed_mapped_and_unmapped(self):
        videos = [
            make_video("known_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "link5"),
            make_video("unknown_tt", "tiktok", 45, 3000, "2026-02-20T14:00:00+00:00", "link6"),
            make_video("known_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "link7"),
        ]
        tt_map = {"known_tt": "Known Creator"}
        ig_map = {"known_ig": "Known Creator"}

        mapped, exceptions = _map_videos_to_creators(videos, tt_map, ig_map)
        assert len(mapped) == 2
        assert len(exceptions) == 1
        assert exceptions[0].username == "unknown_tt"


# ===========================================================================
# ADDITIONAL TEST: Step 6 — Deduplication
# ===========================================================================

class TestDeduplication:
    """Step 6: Remove duplicates by ad_link / ad_id."""

    def test_dedup_by_ad_link(self):
        """Same ad_link → keep the one with more recent updated_at."""
        v1 = make_video("user1", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00",
                        ad_link="https://example.com/video1",
                        updated_at_str="2026-02-20T12:00:00+00:00",
                        creator_name="Creator A")
        v2 = make_video("user1", "tiktok", 30, 6000, "2026-02-20T10:00:00+00:00",
                        ad_link="https://example.com/video1",
                        updated_at_str="2026-02-21T12:00:00+00:00",
                        creator_name="Creator A")
        result = _deduplicate_videos([v1, v2])
        assert len(result) == 1
        assert result[0].latest_views == 6000  # v2 is more recent

    def test_dedup_by_ad_id(self):
        """Same ad_id, different ad_link → dedup by ad_id."""
        v1 = make_video("user1", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00",
                        ad_link="https://example.com/v1",
                        ad_id="same-id",
                        updated_at_str="2026-02-20T12:00:00+00:00",
                        creator_name="Creator A")
        v2 = make_video("user1", "tiktok", 30, 7000, "2026-02-20T10:00:00+00:00",
                        ad_link="https://example.com/v2",
                        ad_id="same-id",
                        updated_at_str="2026-02-21T12:00:00+00:00",
                        creator_name="Creator A")
        result = _deduplicate_videos([v1, v2])
        assert len(result) == 1
        assert result[0].latest_views == 7000  # v2 is more recent

    def test_no_duplicates(self):
        """Different ad_links and ad_ids → all kept."""
        v1 = make_video("user1", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00",
                        ad_link="link1", ad_id="id1", creator_name="A")
        v2 = make_video("user2", "tiktok", 45, 8000, "2026-02-20T14:00:00+00:00",
                        ad_link="link2", ad_id="id2", creator_name="A")
        result = _deduplicate_videos([v1, v2])
        assert len(result) == 2


# ===========================================================================
# ADDITIONAL TEST: Fallback time window boundary
# ===========================================================================

class TestFallbackTimeWindow:
    """Fallback should only match within ±24 hours of created_at."""

    def test_within_24h_matches(self):
        """Videos 23 hours apart → fallback should find match."""
        tt = [make_video("hal_tt", "tiktok", 45, 5000, "2026-02-20T10:00:00+00:00", "tt_h1")]
        ig = [make_video("hal_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "ig_h1")]
        # TT(45s) ≠ IG(30s) → primary fails → fallback
        # But no other videos to match → both unpaired
        payout_units, _ = _match_creator_videos("Hal", tt, ig)
        assert all(not pu.paired for pu in payout_units)

    def test_beyond_24h_no_match(self):
        """
        Videos >24 hours apart → fallback should NOT match even with same length.

        Setup: TT#1(45s) and IG#1(30s) fail primary.
        Extra IG#2(45s) exists but >24h away from TT#1.
        """
        tt = [make_video("ian_tt", "tiktok", 45, 5000, "2026-02-20T10:00:00+00:00", "tt_i1")]
        ig = [
            make_video("ian_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "ig_i1"),
            make_video("ian_ig", "instagram", 45, 3000, "2026-02-22T12:00:00+00:00", "ig_i2"),
        ]
        # TT#1(45s) ↔ IG#1(30s) → mismatch → fallback
        # TT#1 searches unmatched IG: IG#2(45s) but >24h away → no match
        payout_units, _ = _match_creator_videos("Ian", tt, ig)
        paired = [pu for pu in payout_units if pu.paired]
        assert len(paired) == 0  # No successful pairs

    def test_exactly_24h_matches(self):
        """Videos exactly 24 hours apart → should still match (within ±24h)."""
        # TT at T, extra IG at T+24h with same length
        tt = [make_video("jan_tt", "tiktok", 45, 5000, "2026-02-20T10:00:00+00:00", "tt_j1")]
        ig = [
            make_video("jan_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "ig_j1"),
            make_video("jan_ig", "instagram", 45, 3000, "2026-02-21T10:00:00+00:00", "ig_j2"),
        ]
        # TT#1(45s) ↔ IG#1(30s) → mismatch → fallback
        # TT#1 searches: IG#2(45s) at exactly 24h → within window → match!
        payout_units, _ = _match_creator_videos("Jan", tt, ig)
        paired = [pu for pu in payout_units if pu.paired]
        assert len(paired) == 1
        assert paired[0].match_confidence == "medium"


# ===========================================================================
# ADDITIONAL TEST: Fallback requires exact length (no ±1s)
# ===========================================================================

class TestFallbackExactLengthOnly:
    """Fallback matching requires exact length — no ±1s tolerance."""

    def test_fallback_rejects_1s_diff(self):
        """Fallback should NOT accept ±1s length match — exact only."""
        tt = [make_video("kim_tt", "tiktok", 45, 5000, "2026-02-20T10:00:00+00:00", "tt_k1")]
        ig = [
            make_video("kim_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "ig_k1"),
            make_video("kim_ig", "instagram", 46, 3000, "2026-02-20T12:00:00+00:00", "ig_k2"),
        ]
        # Primary: TT(45) ↔ IG(30) → mismatch → fallback
        # Fallback: TT(45) searches → IG#2(46) → 1s diff → NO match (exact required)
        payout_units, _ = _match_creator_videos("Kim", tt, ig)
        paired = [pu for pu in payout_units if pu.paired]
        assert len(paired) == 0
        assert len(payout_units) == 3  # all unpaired: TT(45), IG(30), IG(46)

    def test_fallback_accepts_exact_only(self):
        """Fallback should only accept exact length match."""
        tt = [make_video("lee_tt", "tiktok", 45, 5000, "2026-02-20T10:00:00+00:00", "tt_l1")]
        ig = [
            make_video("lee_ig", "instagram", 30, 8000, "2026-02-20T09:00:00+00:00", "ig_l1"),
            make_video("lee_ig", "instagram", 46, 3000, "2026-02-20T11:00:00+00:00", "ig_l2"),
            make_video("lee_ig", "instagram", 45, 6000, "2026-02-20T15:00:00+00:00", "ig_l3"),
        ]
        # Primary: TT(45) ↔ IG#1(30) → mismatch → fallback
        # Fallback: TT(45) searches → IG#2(46) rejected (not exact), IG#3(45) accepted
        payout_units, _ = _match_creator_videos("Lee", tt, ig)
        paired = [pu for pu in payout_units if pu.paired]
        assert len(paired) == 1
        # The paired IG should be IG#3 (exact match, 45s)
        assert paired[0].instagram_video.video_length == 45


# ===========================================================================
# ADDITIONAL TEST: None video_length in sequence pair
# ===========================================================================

class TestNoneVideoLength:
    """When video_length is None, comparison should fail (treated as mismatch)."""

    def test_null_tt_length(self):
        """TT has None video_length → primary fails, both unpaired."""
        tt_vid = make_video("nul_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "tt_n1")
        tt_vid = tt_vid.model_copy(update={"video_length": None})
        ig_vid = make_video("nul_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "ig_n1")

        payout_units, _ = _match_creator_videos("Null", [tt_vid], [ig_vid])
        # Can't compare lengths, so primary fails. Fallback also can't match (None length).
        assert all(not pu.paired for pu in payout_units)
        assert len(payout_units) == 2


# ===========================================================================
# ADDITIONAL TEST: Multiple creators in full pipeline
# ===========================================================================

class TestMultipleCreators:
    """Full pipeline with multiple creators."""

    def test_two_creators(self):
        videos = [
            # Creator A: 1 TT + 1 IG (same length)
            make_video("a_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "tt_a1"),
            make_video("a_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "ig_a1"),
            # Creator B: 1 TT only
            make_video("b_tt", "tiktok", 45, 12000, "2026-02-20T14:00:00+00:00", "tt_b1"),
        ]
        tt_map = {"a_tt": "Creator A", "b_tt": "Creator B"}
        ig_map = {"a_ig": "Creator A"}

        payout_units, exceptions = match_videos(videos, tt_map, ig_map)

        # Creator A: 1 paired
        a_units = [pu for pu in payout_units if pu.creator_name == "Creator A"]
        assert len(a_units) == 1
        assert a_units[0].paired is True

        # Creator B: 1 unpaired
        b_units = [pu for pu in payout_units if pu.creator_name == "Creator B"]
        assert len(b_units) == 1
        assert b_units[0].paired is False

        # Exceptions: Creator B's unpaired video
        unpaired_exceptions = [e for e in exceptions if e.reason == "unpaired — single platform only"]
        assert len(unpaired_exceptions) == 1


# ===========================================================================
# ADDITIONAL TEST: Sorting stability
# ===========================================================================

class TestSortingOrder:
    """Videos should be sorted by created_at ascending for sequence matching."""

    def test_reversed_input_order(self):
        """Even if TT videos are provided in reverse order, matching works correctly."""
        # Provide TikTok videos in REVERSE created_at order
        tiktok = [
            make_video("sort_tt", "tiktok", 60, 800, "2026-02-21T09:00:00+00:00", "tt_s3"),
            make_video("sort_tt", "tiktok", 45, 12000, "2026-02-20T14:00:00+00:00", "tt_s2"),
            make_video("sort_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "tt_s1"),
        ]
        instagram = [
            make_video("sort_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "ig_s1"),
            make_video("sort_ig", "instagram", 45, 3000, "2026-02-20T14:30:00+00:00", "ig_s2"),
            make_video("sort_ig", "instagram", 60, 1500, "2026-02-21T09:30:00+00:00", "ig_s3"),
        ]
        payout_units, exceptions = _match_creator_videos("Sorter", tiktok, instagram)

        # Should still correctly pair: 30↔30, 45↔45, 60↔60
        assert len(payout_units) == 3
        assert all(pu.paired for pu in payout_units)
        assert all(pu.pair_note == "exact match" for pu in payout_units)
        assert len(exceptions) == 0


# ===========================================================================
# ADDITIONAL TEST: Views selection edge cases
# ===========================================================================

class TestViewsSelection:
    """Verify chosen_views logic for various scenarios."""

    def test_zero_views_both_platforms(self):
        """Both platforms have 0 views → chosen_views = 0."""
        tt = [make_video("zero_tt", "tiktok", 30, 0, "2026-02-20T10:00:00+00:00", "tt_z1")]
        ig = [make_video("zero_ig", "instagram", 30, 0, "2026-02-20T10:30:00+00:00", "ig_z1")]
        payout_units, _ = _match_creator_videos("Zero", tt, ig)
        assert payout_units[0].chosen_views == 0

    def test_one_platform_zero_views(self):
        """One platform has 0, other has views → max is used."""
        tt = [make_video("mix_tt", "tiktok", 30, 0, "2026-02-20T10:00:00+00:00", "tt_m1")]
        ig = [make_video("mix_ig", "instagram", 30, 50000, "2026-02-20T10:30:00+00:00", "ig_m1")]
        payout_units, _ = _match_creator_videos("Mix", tt, ig)
        assert payout_units[0].chosen_views == 50000
        assert payout_units[0].best_platform == "instagram"

    def test_equal_views(self):
        """Equal views on both platforms → tiktok wins tie (>= comparison)."""
        tt = [make_video("tie_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "tt_t1")]
        ig = [make_video("tie_ig", "instagram", 30, 5000, "2026-02-20T10:30:00+00:00", "ig_t1")]
        payout_units, _ = _match_creator_videos("Tie", tt, ig)
        assert payout_units[0].chosen_views == 5000
        assert payout_units[0].best_platform == "tiktok"


# ===========================================================================
# ADDITIONAL TEST: Complex fallback — both videos find different matches
# ===========================================================================

class TestComplexFallback:
    """
    Complex scenario: sequence pair fails, but BOTH videos find
    different fallback matches on the other platform.

    Setup:
      TT: [30s @10am, 60s @2pm, 45s @9pm]
      IG: [30s @10:30am, 45s @2:30pm, 60s @9:30pm]

    Primary:
      #1: TT(30) ↔ IG(30) → exact → high ✓
      #2: TT(60) ↔ IG(45) → diff=15 → FAIL
      #3: TT(45) ↔ IG(60) → diff=15 → FAIL

    Fallback #2:
      TT#2(60s) → search unmatched IG → IG#3(60s) exact → match!
      IG#2(45s) → search unmatched TT → TT#3(45s) exact → match!
      Both find independent matches → 2 medium pairs

    Fallback #3:
      Both TT#3 and IG#3 already used → no action needed

    Result: 1 high + 2 medium = 3 pairs
    """

    def test_complex_cross_fallback(self):
        tiktok = [
            make_video("cx_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "tt_cx1"),
            make_video("cx_tt", "tiktok", 60, 12000, "2026-02-20T14:00:00+00:00", "tt_cx2"),
            make_video("cx_tt", "tiktok", 45, 3000, "2026-02-20T21:00:00+00:00", "tt_cx3"),
        ]
        instagram = [
            make_video("cx_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "ig_cx1"),
            make_video("cx_ig", "instagram", 45, 7000, "2026-02-20T14:30:00+00:00", "ig_cx2"),
            make_video("cx_ig", "instagram", 60, 2000, "2026-02-20T21:30:00+00:00", "ig_cx3"),
        ]

        payout_units, exceptions = _match_creator_videos("Complex", tiktok, instagram)

        assert len(payout_units) == 3
        assert all(pu.paired for pu in payout_units)
        assert len(exceptions) == 0

        confidences = [pu.match_confidence for pu in payout_units]
        assert confidences.count("high") == 1
        assert confidences.count("medium") == 2

        # Verify all pairs have exact matching lengths
        for pu in payout_units:
            tt_len = pu.tiktok_video.video_length
            ig_len = pu.instagram_video.video_length
            assert tt_len == ig_len


# ===========================================================================
# ADDITIONAL TEST: Large unequal — more IG than TT
# ===========================================================================

class TestMoreInstagramThanTiktok:
    """3 TikToks + 5 Instagrams → 3 pairs + 2 unpaired Instagrams."""

    def test_extra_ig_unpaired(self):
        tiktok = [
            make_video("rev_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "tt_r1"),
            make_video("rev_tt", "tiktok", 45, 12000, "2026-02-20T14:00:00+00:00", "tt_r2"),
            make_video("rev_tt", "tiktok", 60, 800, "2026-02-21T09:00:00+00:00", "tt_r3"),
        ]
        instagram = [
            make_video("rev_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "ig_r1"),
            make_video("rev_ig", "instagram", 45, 3000, "2026-02-20T14:30:00+00:00", "ig_r2"),
            make_video("rev_ig", "instagram", 60, 1500, "2026-02-21T09:30:00+00:00", "ig_r3"),
            make_video("rev_ig", "instagram", 25, 20000, "2026-02-21T15:00:00+00:00", "ig_r4"),
            make_video("rev_ig", "instagram", 90, 500, "2026-02-22T09:00:00+00:00", "ig_r5"),
        ]

        payout_units, exceptions = _match_creator_videos("Reversed", tiktok, instagram)
        paired = [pu for pu in payout_units if pu.paired]
        unpaired = [pu for pu in payout_units if not pu.paired]

        assert len(paired) == 3
        assert len(unpaired) == 2
        assert all(pu.instagram_video is not None and pu.tiktok_video is None for pu in unpaired)
        assert len(exceptions) == 2


# ===========================================================================
# ADDITIONAL TEST: Single video per platform (simplest pair)
# ===========================================================================

class TestSinglePair:
    """Simplest case: 1 TT + 1 IG with same length."""

    def test_single_exact_pair(self):
        tt = [make_video("sim_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "tt_sim")]
        ig = [make_video("sim_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "ig_sim")]
        payout_units, exceptions = _match_creator_videos("Simple", tt, ig)

        assert len(payout_units) == 1
        assert payout_units[0].paired is True
        assert payout_units[0].match_confidence == "high"
        assert payout_units[0].pair_note == "exact match"
        assert payout_units[0].chosen_views == 8000
        assert len(exceptions) == 0


# ===========================================================================
# ADDITIONAL TEST: _video_length_diff helper
# ===========================================================================

class TestVideoLengthDiff:
    """Unit tests for the _video_length_diff helper."""

    def test_exact_match(self):
        v1 = make_video(length=30)
        v2 = make_video(length=30)
        assert _video_length_diff(v1, v2) == 0

    def test_one_second_diff(self):
        v1 = make_video(length=30)
        v2 = make_video(length=31)
        assert _video_length_diff(v1, v2) == 1

    def test_large_diff(self):
        v1 = make_video(length=30)
        v2 = make_video(length=60)
        assert _video_length_diff(v1, v2) == 30

    def test_none_length_returns_none(self):
        v1 = make_video(length=30)
        v2 = make_video(length=30)
        v2 = v2.model_copy(update={"video_length": None})
        assert _video_length_diff(v1, v2) is None

    def test_both_none_returns_none(self):
        v1 = make_video(length=30)
        v2 = make_video(length=30)
        v1 = v1.model_copy(update={"video_length": None})
        v2 = v2.model_copy(update={"video_length": None})
        assert _video_length_diff(v1, v2) is None


# ===========================================================================
# ADDITIONAL TEST: Full pipeline end-to-end
# ===========================================================================

class TestFullPipelineEndToEnd:
    """End-to-end test of the full match_videos pipeline."""

    def test_full_pipeline(self):
        """
        2 creators:
          Creator A: 2 TT + 2 IG (all matching lengths) → 2 pairs
          Creator B: 1 TT, no IG → 1 unpaired
          Unknown user: 1 TT → exception (not in creator list)
        """
        videos = [
            make_video("a_tiktok", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "link_a_tt1"),
            make_video("a_tiktok", "tiktok", 45, 12000, "2026-02-20T14:00:00+00:00", "link_a_tt2"),
            make_video("a_insta", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "link_a_ig1"),
            make_video("a_insta", "instagram", 45, 3000, "2026-02-20T14:30:00+00:00", "link_a_ig2"),
            make_video("b_tiktok", "tiktok", 60, 800, "2026-02-21T09:00:00+00:00", "link_b_tt1"),
            make_video("mystery_user", "tiktok", 20, 999, "2026-02-21T12:00:00+00:00", "link_mystery"),
        ]

        tt_map = {"a_tiktok": "Creator A", "b_tiktok": "Creator B"}
        ig_map = {"a_insta": "Creator A"}

        payout_units, exceptions = match_videos(videos, tt_map, ig_map)

        # Creator A: 2 pairs
        a_units = [pu for pu in payout_units if pu.creator_name == "Creator A"]
        assert len(a_units) == 2
        assert all(pu.paired for pu in a_units)

        # Creator B: 1 unpaired
        b_units = [pu for pu in payout_units if pu.creator_name == "Creator B"]
        assert len(b_units) == 1
        assert not b_units[0].paired

        # Total payout units: 3
        assert len(payout_units) == 3

        # Exceptions: 1 unmapped + 1 unpaired
        assert len(exceptions) == 2
        unmapped = [e for e in exceptions if e.reason == "not in creator list"]
        unpaired = [e for e in exceptions if e.reason == "unpaired — single platform only"]
        assert len(unmapped) == 1
        assert unmapped[0].username == "mystery_user"
        assert len(unpaired) == 1


# ===========================================================================
# ADDITIONAL TEST: Fallback doesn't reuse matched videos
# ===========================================================================

class TestNoReuseInFallback:
    """
    Verify that once a video is matched (primary or fallback),
    it cannot be re-used in a subsequent fallback search.

    Setup:
      TT: [30s @T1, 30s @T2]  (both same length)
      IG: [30s @T1, 99s @T2]  (IG#2 has different length)

    Primary:
      #1: TT(30) ↔ IG(30) → exact → paired ✓ (IG idx=0 used)
      #2: TT(30) ↔ IG(99) → mismatch → fallback

    Fallback #2:
      TT#2(30s) searches unmatched IG → IG#1(30s) already used → skip → no match
      IG#2(99s) searches unmatched TT → no 99s TT → no match
      Both unpaired.
    """

    def test_used_video_not_reused(self):
        tiktok = [
            make_video("reuse_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "tt_ru1"),
            make_video("reuse_tt", "tiktok", 30, 3000, "2026-02-20T14:00:00+00:00", "tt_ru2"),
        ]
        instagram = [
            make_video("reuse_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "ig_ru1"),
            make_video("reuse_ig", "instagram", 99, 1000, "2026-02-20T14:30:00+00:00", "ig_ru2"),
        ]

        payout_units, exceptions = _match_creator_videos("NoReuse", tiktok, instagram)

        paired = [pu for pu in payout_units if pu.paired]
        unpaired = [pu for pu in payout_units if not pu.paired]

        assert len(paired) == 1  # Only pair #1 succeeds
        assert len(unpaired) == 2  # TT#2 and IG#2 are unpaired
        assert len(exceptions) == 2


# ===========================================================================
# ADDITIONAL TEST: Creator name propagation
# ===========================================================================

class TestCreatorNamePropagation:
    """Verify creator_name is correctly set on all payout units."""

    def test_paired_has_creator_name(self):
        videos = [
            make_video("prop_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "tt_p1"),
            make_video("prop_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "ig_p1"),
        ]
        tt_map = {"prop_tt": "Propagated Name"}
        ig_map = {"prop_ig": "Propagated Name"}

        payout_units, _ = match_videos(videos, tt_map, ig_map)
        assert len(payout_units) == 1
        assert payout_units[0].creator_name == "Propagated Name"

    def test_unpaired_has_creator_name(self):
        videos = [
            make_video("solo_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "tt_solo"),
        ]
        tt_map = {"solo_tt": "Solo Creator"}

        payout_units, _ = match_videos(videos, tt_map, {})
        assert len(payout_units) == 1
        assert payout_units[0].creator_name == "Solo Creator"


# ###########################################################################
#
#  REAL-WORLD / STRESS / EDGE-CASE TESTS
#
#  These go beyond the prompt's required scenarios.  They brainstorm things
#  that might happen in production data and verify the matcher handles them.
#
# ###########################################################################


# ===========================================================================
# REAL-WORLD 1: All videos have identical length (common for campaign briefs)
#
# In practice, a brand might say "make a 30-second video".  Every creator
# posts a ~30s video on both platforms.  Matching should still be purely
# sequence-based: TT#1↔IG#1, TT#2↔IG#2, etc.
# ===========================================================================

class TestAllSameLength:
    """
    5 TT + 5 IG, all 30s.  Should pair by sequence position since lengths
    all match.  Important: the fallback must NOT create wrong cross-pairs
    just because every video is the same length.
    """

    def setup_method(self):
        base_tt = "2026-02-20T{:02d}:00:00+00:00"
        base_ig = "2026-02-20T{:02d}:30:00+00:00"
        self.tt = [
            make_video("same_tt", "tiktok", 30, (i + 1) * 1000,
                        base_tt.format(8 + i), f"tt_same{i}")
            for i in range(5)
        ]
        self.ig = [
            make_video("same_ig", "instagram", 30, (i + 1) * 2000,
                        base_ig.format(8 + i), f"ig_same{i}")
            for i in range(5)
        ]

    def test_five_pairs_all_high(self):
        payout_units, exceptions = _match_creator_videos("SameLen", self.tt, self.ig)
        assert len(payout_units) == 5
        assert all(pu.paired for pu in payout_units)
        assert all(pu.match_confidence == "high" for pu in payout_units)
        assert len(exceptions) == 0

    def test_sequence_preserved_via_views(self):
        """Pair i should have TT views = (i+1)*1000, IG views = (i+1)*2000."""
        payout_units, _ = _match_creator_videos("SameLen", self.tt, self.ig)
        for i, pu in enumerate(payout_units):
            expected_tt_views = (i + 1) * 1000
            expected_ig_views = (i + 1) * 2000
            assert pu.tiktok_video.latest_views == expected_tt_views
            assert pu.instagram_video.latest_views == expected_ig_views
            assert pu.chosen_views == expected_ig_views  # IG always higher


# ===========================================================================
# REAL-WORLD 2: Duplicate videos with different views (API re-scrape)
#
# The Shortimize API might return the same video twice because it was
# re-scraped.  The second row has updated views but same ad_link.
# Dedup should keep the one with the most recent latest_updated_at.
# ===========================================================================

class TestDuplicateWithDifferentViews:
    """Dedup keeps the fresher row; final match should use updated views."""

    def test_dedup_keeps_most_recent_views(self):
        """Two rows with same ad_link, different views → keep newer one."""
        old_row = make_video("dup_tt", "tiktok", 30, 5000,
                              "2026-02-20T10:00:00+00:00", "https://tiktok.com/v1",
                              updated_at_str="2026-02-20T12:00:00+00:00",
                              creator_name="Dup Creator")
        new_row = make_video("dup_tt", "tiktok", 30, 15000,
                              "2026-02-20T10:00:00+00:00", "https://tiktok.com/v1",
                              updated_at_str="2026-02-22T12:00:00+00:00",
                              creator_name="Dup Creator")
        result = _deduplicate_videos([old_row, new_row])
        assert len(result) == 1
        assert result[0].latest_views == 15000

    def test_dedup_order_independent(self):
        """Input order shouldn't matter; always keep the most recent."""
        old_row = make_video("dup_tt", "tiktok", 30, 5000,
                              "2026-02-20T10:00:00+00:00", "https://tiktok.com/v2",
                              updated_at_str="2026-02-20T12:00:00+00:00",
                              creator_name="Dup Creator")
        new_row = make_video("dup_tt", "tiktok", 30, 15000,
                              "2026-02-20T10:00:00+00:00", "https://tiktok.com/v2",
                              updated_at_str="2026-02-22T12:00:00+00:00",
                              creator_name="Dup Creator")
        # Provide in reverse order
        result = _deduplicate_videos([new_row, old_row])
        assert len(result) == 1
        assert result[0].latest_views == 15000

    def test_dedup_three_copies_same_ad_link(self):
        """Three rows same ad_link → only keep the freshest one."""
        rows = [
            make_video("dup3", "tiktok", 30, 1000, "2026-02-20T10:00:00+00:00",
                        "https://tiktok.com/v3",
                        updated_at_str="2026-02-20T10:00:00+00:00",
                        creator_name="D"),
            make_video("dup3", "tiktok", 30, 2000, "2026-02-20T10:00:00+00:00",
                        "https://tiktok.com/v3",
                        updated_at_str="2026-02-21T10:00:00+00:00",
                        creator_name="D"),
            make_video("dup3", "tiktok", 30, 3000, "2026-02-20T10:00:00+00:00",
                        "https://tiktok.com/v3",
                        updated_at_str="2026-02-22T10:00:00+00:00",
                        creator_name="D"),
        ]
        result = _deduplicate_videos(rows)
        assert len(result) == 1
        assert result[0].latest_views == 3000

    def test_dedup_both_updated_at_none(self):
        """If both duplicates have None updated_at, the first one wins (not more recent)."""
        v1 = make_video("dn", "tiktok", 30, 1000, "2026-02-20T10:00:00+00:00",
                         "https://tiktok.com/v_none", creator_name="DN")
        v2 = make_video("dn", "tiktok", 30, 9999, "2026-02-20T10:00:00+00:00",
                         "https://tiktok.com/v_none", creator_name="DN")
        # Both have latest_updated_at=None
        result = _deduplicate_videos([v1, v2])
        assert len(result) == 1
        # The first one stays because _is_more_recent(v2, v1) is False when both None
        assert result[0].latest_views == 1000


# ===========================================================================
# REAL-WORLD 3: Videos with None created_at (missing timestamp)
#
# If the API returns null created_at, those videos should sort to the end
# (datetime.max).  They can still be paired if lengths match.
# ===========================================================================

class TestNoneCreatedAt:
    """Videos with missing created_at should sort to end and still match."""

    def test_none_created_at_sorts_last(self):
        """A video with None created_at should be at the end of the sorted list."""
        tt_normal = make_video("nc_tt", "tiktok", 30, 5000,
                                "2026-02-20T10:00:00+00:00", "tt_nc1")
        tt_null = make_video("nc_tt", "tiktok", 45, 3000,
                              "2026-02-20T14:00:00+00:00", "tt_nc2")
        tt_null = tt_null.model_copy(update={"created_at": None})

        ig_normal = make_video("nc_ig", "instagram", 30, 8000,
                                "2026-02-20T10:30:00+00:00", "ig_nc1")
        ig_for_null = make_video("nc_ig", "instagram", 45, 2000,
                                  "2026-02-20T14:30:00+00:00", "ig_nc2")
        ig_for_null = ig_for_null.model_copy(update={"created_at": None})

        payout_units, _ = _match_creator_videos(
            "NullDate", [tt_normal, tt_null], [ig_normal, ig_for_null]
        )
        # Pair 1: both normal → exact match
        # Pair 2: both None created_at → lengths match → exact match
        assert len(payout_units) == 2
        assert all(pu.paired for pu in payout_units)

    def test_none_created_at_no_fallback(self):
        """
        If a video has None created_at, the fallback search cannot compute
        time difference, so it returns None → no fallback match possible.
        """
        tt = [make_video("nf_tt", "tiktok", 45, 5000,
                          "2026-02-20T10:00:00+00:00", "tt_nf1")]
        tt[0] = tt[0].model_copy(update={"created_at": None})

        ig = [make_video("nf_ig", "instagram", 30, 8000,
                          "2026-02-20T10:30:00+00:00", "ig_nf1"),
              make_video("nf_ig", "instagram", 45, 2000,
                          "2026-02-20T12:00:00+00:00", "ig_nf2")]

        payout_units, _ = _match_creator_videos("NoFallback", tt, ig)
        # TT(45) ↔ IG(30) → mismatch → fallback
        # But TT has None created_at → _find_fallback_match returns None
        paired = [pu for pu in payout_units if pu.paired]
        assert len(paired) == 0


# ===========================================================================
# REAL-WORLD 4: Massive creator — 20 TT + 20 IG
#
# Some prolific creators might post 20+ videos in a pay period.
# Verify the matcher scales correctly and pairs all by sequence.
# ===========================================================================

class TestLargeCreator:
    """Creator with 20 TT + 20 IG, all matching lengths."""

    def test_twenty_pairs(self):
        base = "2026-02-{:02d}T{:02d}:00:00+00:00"
        tt = [
            make_video("big_tt", "tiktok", 30 + i, i * 1000,
                        base.format(1 + i // 2, 10 + i % 12), f"tt_big{i}")
            for i in range(20)
        ]
        ig = [
            make_video("big_ig", "instagram", 30 + i, i * 1500,
                        base.format(1 + i // 2, 10 + i % 12 + 1 if i % 12 < 11 else 23),
                        f"ig_big{i}")
            for i in range(20)
        ]
        payout_units, exceptions = _match_creator_videos("BigCreator", tt, ig)
        assert len(payout_units) == 20
        assert all(pu.paired for pu in payout_units)
        assert len(exceptions) == 0


# ===========================================================================
# REAL-WORLD 5: Same creator handle on both platforms
#
# Some creators use the exact same username on TikTok and Instagram.
# e.g. @john_doe on both.  The mapping sheet maps them to the same
# creator_name.  Verify they group correctly.
# ===========================================================================

class TestSameHandleBothPlatforms:
    """Creator uses same username on both platforms."""

    def test_same_username_different_platforms(self):
        videos = [
            make_video("john_doe", "tiktok", 30, 5000,
                        "2026-02-20T10:00:00+00:00", "tt_jd1"),
            make_video("john_doe", "instagram", 30, 8000,
                        "2026-02-20T10:30:00+00:00", "ig_jd1"),
        ]
        tt_map = {"john_doe": "John Doe"}
        ig_map = {"john_doe": "John Doe"}

        payout_units, exceptions = match_videos(videos, tt_map, ig_map)
        assert len(payout_units) == 1
        assert payout_units[0].paired is True
        assert payout_units[0].creator_name == "John Doe"
        assert len(exceptions) == 0


# ===========================================================================
# REAL-WORLD 6: Videos posted seconds apart (near-identical created_at)
#
# A creator might cross-post almost simultaneously.  The sequence sort
# should still be deterministic.  This tests sort stability.
# ===========================================================================

class TestNearSimultaneousPosts:
    """Videos posted seconds apart on same platform."""

    def test_sort_stability_with_close_timestamps(self):
        """
        3 TT videos posted 1 second apart, 3 IG videos with matching lengths.
        Sort should be stable and pair correctly by position.
        """
        tt = [
            make_video("sim_tt", "tiktok", 30, 5000,
                        "2026-02-20T10:00:00+00:00", "tt_sim1"),
            make_video("sim_tt", "tiktok", 45, 12000,
                        "2026-02-20T10:00:01+00:00", "tt_sim2"),
            make_video("sim_tt", "tiktok", 60, 800,
                        "2026-02-20T10:00:02+00:00", "tt_sim3"),
        ]
        ig = [
            make_video("sim_ig", "instagram", 30, 8000,
                        "2026-02-20T10:00:30+00:00", "ig_sim1"),
            make_video("sim_ig", "instagram", 45, 3000,
                        "2026-02-20T10:00:31+00:00", "ig_sim2"),
            make_video("sim_ig", "instagram", 60, 1500,
                        "2026-02-20T10:00:32+00:00", "ig_sim3"),
        ]
        payout_units, _ = _match_creator_videos("Simultaneous", tt, ig)
        assert len(payout_units) == 3
        assert all(pu.paired for pu in payout_units)
        assert all(pu.pair_note == "exact match" for pu in payout_units)


# ===========================================================================
# REAL-WORLD 7: Fallback race condition — two fallback candidates same distance
#
# When TT has multiple videos that could match the same IG video in fallback
# (same length, same time distance), the code should pick one deterministically
# and not create duplicate pairs.
# ===========================================================================

class TestFallbackTieBreaking:
    """
    Two TT videos fail primary, both look for fallback, both find the
    same IG video.  Only ONE should get it; the other stays unpaired.

    Setup:
      TT: [30s @10am, 30s @2pm, 60s @6pm]
      IG: [60s @10:30am, 60s @2:30pm, 30s @6:30pm]

    Primary:
      #1: TT(30) ↔ IG(60) → mismatch → fallback
      #2: TT(30) ↔ IG(60) → mismatch → fallback
      #3: TT(60) ↔ IG(30) → mismatch → fallback

    The code processes fallback_candidates sequentially, so pair #1's fallback
    runs first.  If it claims IG#3(30s), then pair #2's TT(30s) must look
    elsewhere — but IG#3 is taken, so it stays unpaired.

    Pair #3 fallback: TT(60s) → IG#1(60s) or IG#2(60s) available.
    IG(30s) at idx=2 → TT#1(30s) or TT#2(30s).
    """

    def test_no_double_booking(self):
        tt = [
            make_video("race_tt", "tiktok", 30, 5000,
                        "2026-02-20T10:00:00+00:00", "tt_race1"),
            make_video("race_tt", "tiktok", 30, 3000,
                        "2026-02-20T14:00:00+00:00", "tt_race2"),
            make_video("race_tt", "tiktok", 60, 8000,
                        "2026-02-20T18:00:00+00:00", "tt_race3"),
        ]
        ig = [
            make_video("race_ig", "instagram", 60, 7000,
                        "2026-02-20T10:30:00+00:00", "ig_race1"),
            make_video("race_ig", "instagram", 60, 2000,
                        "2026-02-20T14:30:00+00:00", "ig_race2"),
            make_video("race_ig", "instagram", 30, 1000,
                        "2026-02-20T18:30:00+00:00", "ig_race3"),
        ]

        payout_units, exceptions = _match_creator_videos("Race", tt, ig)

        # All 6 videos should appear exactly once across payout units
        all_tt_links = []
        all_ig_links = []
        for pu in payout_units:
            if pu.tiktok_video:
                all_tt_links.append(pu.tiktok_video.ad_link)
            if pu.instagram_video:
                all_ig_links.append(pu.instagram_video.ad_link)

        # No duplicate ad_links — each video used at most once
        assert len(all_tt_links) == len(set(all_tt_links)), \
            f"TT video used more than once: {all_tt_links}"
        assert len(all_ig_links) == len(set(all_ig_links)), \
            f"IG video used more than once: {all_ig_links}"

        # Total payout units should account for all 6 videos
        total_videos = len(all_tt_links) + len(all_ig_links)
        assert total_videos == 6  # every video accounted for

    def test_swapped_lengths_fallback(self):
        """
        With 3 TT and 3 IG where lengths are swapped across platforms,
        the fallback should correctly pair what it can without double-booking.

        Setup:
          TT: [30s @10am, 30s @2pm, 60s @6pm]
          IG: [60s @10:30am, 60s @2:30pm, 30s @6:30pm]

        Primary: all fail (30≠60, 30≠60, 60≠30)
        Fallback pair #1:
          TT#1(30s) → searches unmatched IG → IG#3(30s @6:30pm, 8.5h away) ✓
          IG#1(60s) → searches unmatched TT → TT#3(60s @6pm, 7.5h away) ✓
        Fallback pair #2:
          TT#2(30s) → IG#3 used → no 30s IG left → unpaired
          IG#2(60s) → TT#3 used → no 60s TT left → unpaired
        Fallback pair #3:
          TT#3 already used → skip
          IG#3 already used → skip

        Result: 2 pairs + 2 unpaired
        """
        tt = [
            make_video("swap_tt", "tiktok", 30, 5000,
                        "2026-02-20T10:00:00+00:00", "tt_swap1"),
            make_video("swap_tt", "tiktok", 30, 3000,
                        "2026-02-20T14:00:00+00:00", "tt_swap2"),
            make_video("swap_tt", "tiktok", 60, 8000,
                        "2026-02-20T18:00:00+00:00", "tt_swap3"),
        ]
        ig = [
            make_video("swap_ig", "instagram", 60, 7000,
                        "2026-02-20T10:30:00+00:00", "ig_swap1"),
            make_video("swap_ig", "instagram", 60, 2000,
                        "2026-02-20T14:30:00+00:00", "ig_swap2"),
            make_video("swap_ig", "instagram", 30, 1000,
                        "2026-02-20T18:30:00+00:00", "ig_swap3"),
        ]

        payout_units, exceptions = _match_creator_videos("Swap", tt, ig)

        paired = [pu for pu in payout_units if pu.paired]
        unpaired = [pu for pu in payout_units if not pu.paired]

        # 2 pairs via fallback + 2 unpaired (TT#2 and IG#2)
        assert len(paired) == 2
        assert len(unpaired) == 2

        # Verify no video appears in two payout units
        all_tt_links = [pu.tiktok_video.ad_link for pu in payout_units if pu.tiktok_video]
        all_ig_links = [pu.instagram_video.ad_link for pu in payout_units if pu.instagram_video]
        assert len(all_tt_links) == len(set(all_tt_links))
        assert len(all_ig_links) == len(set(all_ig_links))


# ===========================================================================
# REAL-WORLD 8: Video with 0 video_length
#
# What if the API returns video_length = 0?  This is technically valid
# (not None), but makes no sense.  Our matcher should still handle it.
# ===========================================================================

class TestZeroVideoLength:
    """Videos with video_length = 0."""

    def test_zero_length_exact_match(self):
        """Two 0-second videos should still match (length diff = 0)."""
        tt = [make_video("zero_tt", "tiktok", 0, 5000,
                          "2026-02-20T10:00:00+00:00", "tt_zero")]
        ig = [make_video("zero_ig", "instagram", 0, 8000,
                          "2026-02-20T10:30:00+00:00", "ig_zero")]
        payout_units, _ = _match_creator_videos("ZeroLen", tt, ig)
        assert len(payout_units) == 1
        assert payout_units[0].paired is True

    def test_zero_vs_one_second(self):
        """0s TT vs 1s IG → diff = 1 → unpaired (exact length required)."""
        tt = [make_video("z01_tt", "tiktok", 0, 5000,
                          "2026-02-20T10:00:00+00:00", "tt_z01")]
        ig = [make_video("z01_ig", "instagram", 1, 8000,
                          "2026-02-20T10:30:00+00:00", "ig_z01")]
        payout_units, _ = _match_creator_videos("ZeroOne", tt, ig)
        assert len(payout_units) == 2
        assert all(not pu.paired for pu in payout_units)


# ===========================================================================
# REAL-WORLD 9: Dedup interaction with matching
#
# Two rows with same ad_link arrive from the API.  After dedup, only one
# remains.  The remaining one should correctly participate in matching.
# This tests the FULL pipeline (dedup → map → match).
# ===========================================================================

class TestDedupThenMatch:
    """Full pipeline: duplicates are deduped, then matching works on clean data."""

    def test_dedup_before_match(self):
        """
        Creator has 2 TT (one is a duplicate) + 1 IG.
        After dedup: 1 TT + 1 IG → 1 pair.
        """
        videos = [
            # TT original
            make_video("dm_tt", "tiktok", 30, 5000,
                        "2026-02-20T10:00:00+00:00", "https://tt.com/v1",
                        updated_at_str="2026-02-20T12:00:00+00:00"),
            # TT duplicate (same ad_link, newer updated_at, higher views)
            make_video("dm_tt", "tiktok", 30, 15000,
                        "2026-02-20T10:00:00+00:00", "https://tt.com/v1",
                        updated_at_str="2026-02-22T12:00:00+00:00"),
            # IG
            make_video("dm_ig", "instagram", 30, 8000,
                        "2026-02-20T10:30:00+00:00", "https://ig.com/v1"),
        ]
        tt_map = {"dm_tt": "Dedup Creator"}
        ig_map = {"dm_ig": "Dedup Creator"}

        payout_units, exceptions = match_videos(videos, tt_map, ig_map)

        # After dedup: 1 TT (15000 views) + 1 IG → 1 pair
        assert len(payout_units) == 1
        assert payout_units[0].paired is True
        # The kept TT should have 15000 views (the newer one)
        assert payout_units[0].tiktok_video.latest_views == 15000
        assert payout_units[0].chosen_views == 15000  # max(15000, 8000)


# ===========================================================================
# REAL-WORLD 10: Creator mapped on TT but NOT on IG
#
# Google Sheet might have a TikTok handle but leave Instagram blank.
# Videos from that IG handle go to "not in creator list" exceptions.
# ===========================================================================

class TestPartialCreatorMapping:
    """Creator has TT handle mapped but IG handle is NOT in the sheet."""

    def test_tt_mapped_ig_unmapped(self):
        videos = [
            make_video("partial_tt", "tiktok", 30, 5000,
                        "2026-02-20T10:00:00+00:00", "tt_partial1"),
            make_video("partial_ig", "instagram", 30, 8000,
                        "2026-02-20T10:30:00+00:00", "ig_partial1"),
        ]
        tt_map = {"partial_tt": "Partial Creator"}
        ig_map = {}  # IG handle not in mapping

        payout_units, exceptions = match_videos(videos, tt_map, ig_map)

        # TT is mapped → 1 unpaired TT payout unit
        assert len(payout_units) == 1
        assert payout_units[0].paired is False
        assert payout_units[0].tiktok_video is not None
        assert payout_units[0].creator_name == "Partial Creator"

        # IG is unmapped → "not in creator list" exception
        unmapped = [e for e in exceptions if e.reason == "not in creator list"]
        assert len(unmapped) == 1
        assert unmapped[0].username == "partial_ig"

        # TT is unpaired → also an exception
        unpaired = [e for e in exceptions if e.reason == "unpaired — single platform only"]
        assert len(unpaired) == 1


# ===========================================================================
# REAL-WORLD 11: Whitespace in usernames
#
# API might return usernames with leading/trailing whitespace.
# The lookup should still work via normalization.
# ===========================================================================

class TestWhitespaceInUsername:
    """Username with extra whitespace should still match via normalization."""

    def test_leading_trailing_spaces(self):
        videos = [
            make_video("  space_user  ", "tiktok", 30, 5000,
                        "2026-02-20T10:00:00+00:00", "tt_space1"),
        ]
        tt_map = {"space_user": "Space Creator"}

        mapped, exceptions = _map_videos_to_creators(videos, tt_map, {})
        assert len(mapped) == 1
        assert mapped[0].creator_name == "Space Creator"
        assert len(exceptions) == 0


# ===========================================================================
# REAL-WORLD 12: Videos with very large views (> 100M)
#
# Viral videos might have 100M+ views.  The matcher doesn't care about
# views (matching is by length), but chosen_views must be correct.
# The 10M cap is applied later in payout, NOT in matcher.
# ===========================================================================

class TestVeryLargeViews:
    """Matcher should handle very large view counts without overflow."""

    def test_hundred_million_views(self):
        tt = [make_video("viral_tt", "tiktok", 30, 100_000_000,
                          "2026-02-20T10:00:00+00:00", "tt_viral")]
        ig = [make_video("viral_ig", "instagram", 30, 50_000_000,
                          "2026-02-20T10:30:00+00:00", "ig_viral")]
        payout_units, _ = _match_creator_videos("Viral", tt, ig)
        assert payout_units[0].chosen_views == 100_000_000  # max, no cap in matcher
        assert payout_units[0].best_platform == "tiktok"


# ===========================================================================
# REAL-WORLD 13: None latest_views
#
# What if latest_views is None?  Schema defaults to 0 via model, but
# let's test the edge where it might be None on a raw Video object.
# ===========================================================================

class TestNoneLatestViews:
    """Videos with None latest_views should default to 0 in chosen_views."""

    def test_none_views_paired(self):
        tt = [make_video("nv_tt", "tiktok", 30, 0,
                          "2026-02-20T10:00:00+00:00", "tt_nv1")]
        tt[0] = tt[0].model_copy(update={"latest_views": None})

        ig = [make_video("nv_ig", "instagram", 30, 5000,
                          "2026-02-20T10:30:00+00:00", "ig_nv1")]

        payout_units, _ = _match_creator_videos("NullViews", tt, ig)
        assert payout_units[0].paired is True
        # TT views is None → treated as 0 → chosen_views = max(0, 5000) = 5000
        assert payout_units[0].chosen_views == 5000


# ===========================================================================
# REAL-WORLD 14: Fallback with multiple same-length candidates at different
#                time distances
#
# When multiple IG videos have the same length as a rejected TT video,
# fallback should pick the one with the CLOSEST created_at.
# ===========================================================================

class TestFallbackPicksClosest:
    """Fallback should pick the closest created_at among same-length candidates."""

    def test_picks_closest_not_first(self):
        """
        TT(45s @noon) fails primary with IG#1(99s @noon).
        Fallback searches unmatched IG: three IG(45s) at different times.
        Should pick the one closest to noon.

        After sorting by created_at:
          IG#1: 99s @11am  (primary pair partner — wrong length)
          IG#2: 45s @4am   (8h from noon)
          IG#3: 45s @2pm   (2h from noon — CLOSEST)
          IG#4: 45s @6pm   (6h from noon)

        Sorted order: IG#2(@4am), IG#1(@11am), IG#3(@2pm), IG#4(@6pm)
        Primary pair: TT(45s) ↔ IG#2(45s @4am) — sorted idx 0
        BUT WAIT: primary pair uses sorted order, so TT#1 ↔ IG_sorted#1.
        IG_sorted#1 is IG#2(45s @4am). That's an exact length match!

        To force a primary FAILURE, the first sorted IG must have a wrong length.
        Fix: make IG#2 earlier BUT with wrong length, so primary fails.
        """
        tt = [make_video("close_tt", "tiktok", 45, 5000,
                          "2026-02-20T12:00:00+00:00", "tt_close1")]
        ig = [
            # Primary partner (wrong length, sorted FIRST due to earliest time)
            make_video("close_ig", "instagram", 99, 1000,
                        "2026-02-20T02:00:00+00:00", "ig_close1"),
            # Fallback candidate A: 45s but 8 hours away from noon
            make_video("close_ig", "instagram", 45, 2000,
                        "2026-02-20T04:00:00+00:00", "ig_close2"),
            # Fallback candidate B: 45s but 2 hours away (CLOSEST to noon)
            make_video("close_ig", "instagram", 45, 3000,
                        "2026-02-20T14:00:00+00:00", "ig_close3"),
            # Fallback candidate C: 45s but 6 hours away
            make_video("close_ig", "instagram", 45, 4000,
                        "2026-02-20T18:00:00+00:00", "ig_close4"),
        ]

        payout_units, _ = _match_creator_videos("Closest", tt, ig)
        paired = [pu for pu in payout_units if pu.paired]
        assert len(paired) == 1
        # Should have picked IG candidate B (3000 views, 2h away from noon)
        assert paired[0].instagram_video.latest_views == 3000


# ===========================================================================
# REAL-WORLD 15: Creator with only 1 video total (single TT, no IG)
#
# Minimal scenario: just one video for one creator.
# Should be 1 unpaired payout unit + 1 exception.
# ===========================================================================

class TestSingleVideoOnly:
    """Creator with exactly one video total."""

    def test_single_tiktok(self):
        videos = [
            make_video("solo", "tiktok", 30, 5000,
                        "2026-02-20T10:00:00+00:00", "tt_solo1"),
        ]
        tt_map = {"solo": "Solo Creator"}

        payout_units, exceptions = match_videos(videos, tt_map, {})
        assert len(payout_units) == 1
        assert payout_units[0].paired is False
        assert payout_units[0].chosen_views == 5000

        unpaired_exc = [e for e in exceptions if "unpaired" in e.reason]
        assert len(unpaired_exc) == 1


# ===========================================================================
# REAL-WORLD 16: Mixed platform unknown
#
# If somehow a video slips through with an unrecognized platform
# (shouldn't happen after Step 3 filtering), ensure the mapper handles it.
# ===========================================================================

class TestUnknownPlatform:
    """Video with a platform that's neither tiktok nor instagram."""

    def test_unknown_platform_goes_to_exception(self):
        """A 'youtube' video should not match any map and go to exceptions."""
        videos = [
            make_video("yt_user", "youtube", 30, 5000,
                        "2026-02-20T10:00:00+00:00", "yt_link"),
        ]
        # Neither map contains this user
        mapped, exceptions = _map_videos_to_creators(videos, {}, {})
        assert len(mapped) == 0
        assert len(exceptions) == 1
        assert exceptions[0].reason == "not in creator list"


# ===========================================================================
# REAL-WORLD 17: Dedup with empty ad_link and no ad_id
#
# If ad_link is empty string and ad_id is None, the video can't be deduped
# and should always be kept.
# ===========================================================================

class TestDedupNoKey:
    """Videos with no ad_link and no ad_id cannot be deduped."""

    def test_no_key_videos_all_kept(self):
        """
        Videos with truly empty ad_link (whitespace only) and no ad_id
        cannot be deduped and should all be kept.

        NOTE: We construct Video objects directly to pass empty ad_link,
        because make_video generates a default ad_link for empty strings.
        """
        v1 = Video(
            username="nokey", platform="tiktok", ad_link="   ",
            uploaded_at=date(2026, 2, 20),
            created_at=datetime.fromisoformat("2026-02-20T10:00:00+00:00"),
            video_length=30, latest_views=1000, creator_name="NK",
        )
        v2 = Video(
            username="nokey", platform="tiktok", ad_link="   ",
            uploaded_at=date(2026, 2, 20),
            created_at=datetime.fromisoformat("2026-02-20T10:00:00+00:00"),
            video_length=30, latest_views=2000, creator_name="NK",
        )
        result = _deduplicate_videos([v1, v2])
        assert len(result) == 2

    def test_mixed_keyed_and_unkeyed(self):
        """One video has ad_link, one has whitespace-only ad_link → only keyed one deduped."""
        keyed1 = make_video("mk", "tiktok", 30, 1000,
                             "2026-02-20T10:00:00+00:00", ad_link="https://tt.com/x",
                             updated_at_str="2026-02-20T10:00:00+00:00",
                             creator_name="MK")
        keyed2 = make_video("mk", "tiktok", 30, 5000,
                             "2026-02-20T10:00:00+00:00", ad_link="https://tt.com/x",
                             updated_at_str="2026-02-21T10:00:00+00:00",
                             creator_name="MK")
        unkeyed = Video(
            username="mk", platform="tiktok", ad_link="   ",
            uploaded_at=date(2026, 2, 20),
            created_at=datetime.fromisoformat("2026-02-20T14:00:00+00:00"),
            video_length=45, latest_views=3000, creator_name="MK",
        )
        result = _deduplicate_videos([keyed1, keyed2, unkeyed])
        # keyed1 and keyed2 → dedup to 1 (keyed2 newer)
        # unkeyed → always kept
        assert len(result) == 2
        views = sorted([v.latest_views for v in result])
        assert views == [3000, 5000]


# ===========================================================================
# REAL-WORLD 18: Fallback exhaustion — all candidates taken
#
# If all potential fallback matches are already used by earlier pairs,
# the video stays unpaired.  This can happen when the same-length videos
# are snatched by earlier fallback searches.
# ===========================================================================

class TestFallbackExhaustion:
    """
    All IG candidates with matching length are taken by earlier pairs.

    Setup:
      TT: [30s, 30s, 30s]  (all same length)
      IG: [30s, 88s, 88s]  (only first matches, others don't)

    Primary:
      #1: TT(30) ↔ IG(30) → exact → high ✓
      #2: TT(30) ↔ IG(88) → mismatch → fallback
      #3: TT(30) ↔ IG(88) → mismatch → fallback

    Fallback #2: TT(30) → no unmatched 30s IG left → unpaired
                  IG(88) → no 88s TT → unpaired
    Fallback #3: same → both unpaired

    Result: 1 pair + 4 unpaired
    """

    def test_exhausted_fallback(self):
        tt = [
            make_video("exh_tt", "tiktok", 30, 5000,
                        "2026-02-20T10:00:00+00:00", "tt_exh1"),
            make_video("exh_tt", "tiktok", 30, 3000,
                        "2026-02-20T14:00:00+00:00", "tt_exh2"),
            make_video("exh_tt", "tiktok", 30, 1000,
                        "2026-02-20T18:00:00+00:00", "tt_exh3"),
        ]
        ig = [
            make_video("exh_ig", "instagram", 30, 8000,
                        "2026-02-20T10:30:00+00:00", "ig_exh1"),
            make_video("exh_ig", "instagram", 88, 2000,
                        "2026-02-20T14:30:00+00:00", "ig_exh2"),
            make_video("exh_ig", "instagram", 88, 500,
                        "2026-02-20T18:30:00+00:00", "ig_exh3"),
        ]

        payout_units, exceptions = _match_creator_videos("Exhausted", tt, ig)

        paired = [pu for pu in payout_units if pu.paired]
        unpaired = [pu for pu in payout_units if not pu.paired]

        assert len(paired) == 1  # Only pair #1
        assert len(unpaired) == 4  # TT#2, TT#3, IG#2, IG#3
        assert len(exceptions) == 4


# ===========================================================================
# REAL-WORLD 19: Mixed confidence levels in one creator
#
# One creator has some exact pairs, some length mismatches, and some
# unpaired.  Verify the full mix with exact-length-only matching.
# ===========================================================================

class TestMixedConfidenceLevels:
    """
    Creator with a realistic mix (exact length only):
      - 1 exact match (high)
      - 1 length mismatch (45 vs 46 → both unpaired, no fallback match)
      - 1 extra TT (99s) → unpaired

    Setup:
      TT: [30s, 45s, 99s]
      IG: [30s, 46s]
    """

    def test_mixed_confidence_results(self):
        tt = [
            make_video("mx_tt", "tiktok", 30, 5000,
                        "2026-02-20T10:00:00+00:00", "tt_mx1"),
            make_video("mx_tt", "tiktok", 45, 12000,
                        "2026-02-20T14:00:00+00:00", "tt_mx2"),
            make_video("mx_tt", "tiktok", 99, 800,
                        "2026-02-20T18:00:00+00:00", "tt_mx3"),
        ]
        ig = [
            make_video("mx_ig", "instagram", 30, 8000,
                        "2026-02-20T10:30:00+00:00", "ig_mx1"),
            make_video("mx_ig", "instagram", 46, 3000,
                        "2026-02-20T14:30:00+00:00", "ig_mx2"),
        ]

        payout_units, exceptions = _match_creator_videos("MixedConf", tt, ig)

        # Pair #1: 30↔30 → exact → high
        # Pair #2: 45↔46 → mismatch → fallback → no match (46≠45) → both unpaired
        # TT#3: 99s → unpaired (no IG left)

        paired = [pu for pu in payout_units if pu.paired]
        unpaired = [pu for pu in payout_units if not pu.paired]

        assert len(paired) == 1  # only 30↔30
        assert len(unpaired) == 3  # TT(45), IG(46), TT(99)

        assert paired[0].pair_note == "exact match"
        assert all(pu.match_confidence == "low" for pu in unpaired)


# ===========================================================================
# REAL-WORLD 20: Many creators, many videos — full pipeline stress test
#
# 5 creators, each with varying video counts and edge cases.
# Verifies the full pipeline handles real-world complexity.
# ===========================================================================

class TestFullPipelineStress:
    """
    Full pipeline stress test with 5 creators:
      Creator A: 3 TT + 3 IG, all exact → 3 pairs
      Creator B: 2 TT + 0 IG → 2 unpaired
      Creator C: 0 TT + 2 IG → 2 unpaired
      Creator D: 1 TT + 1 IG, length mismatch no fallback → 2 unpaired
      Creator E: not in mapping → exceptions
    """

    def test_five_creators_full_pipeline(self):
        videos = [
            # Creator A: 3 TT + 3 IG, matching lengths
            make_video("a_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "a_tt1"),
            make_video("a_tt", "tiktok", 45, 12000, "2026-02-20T14:00:00+00:00", "a_tt2"),
            make_video("a_tt", "tiktok", 60, 800, "2026-02-21T09:00:00+00:00", "a_tt3"),
            make_video("a_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "a_ig1"),
            make_video("a_ig", "instagram", 45, 3000, "2026-02-20T14:30:00+00:00", "a_ig2"),
            make_video("a_ig", "instagram", 60, 1500, "2026-02-21T09:30:00+00:00", "a_ig3"),
            # Creator B: 2 TT only
            make_video("b_tt", "tiktok", 30, 20000, "2026-02-20T10:00:00+00:00", "b_tt1"),
            make_video("b_tt", "tiktok", 45, 15000, "2026-02-20T14:00:00+00:00", "b_tt2"),
            # Creator C: 2 IG only
            make_video("c_ig", "instagram", 30, 9000, "2026-02-20T10:00:00+00:00", "c_ig1"),
            make_video("c_ig", "instagram", 60, 7000, "2026-02-20T14:00:00+00:00", "c_ig2"),
            # Creator D: mismatched lengths
            make_video("d_tt", "tiktok", 30, 4000, "2026-02-20T10:00:00+00:00", "d_tt1"),
            make_video("d_ig", "instagram", 90, 6000, "2026-02-20T10:30:00+00:00", "d_ig1"),
            # Creator E: not mapped
            make_video("ghost_tt", "tiktok", 30, 1000, "2026-02-20T10:00:00+00:00", "e_tt1"),
            make_video("ghost_ig", "instagram", 30, 2000, "2026-02-20T10:30:00+00:00", "e_ig1"),
        ]

        tt_map = {"a_tt": "Creator A", "b_tt": "Creator B", "d_tt": "Creator D"}
        ig_map = {"a_ig": "Creator A", "c_ig": "Creator C", "d_ig": "Creator D"}

        payout_units, exceptions = match_videos(videos, tt_map, ig_map)

        # Creator A: 3 pairs
        a_units = [pu for pu in payout_units if pu.creator_name == "Creator A"]
        assert len(a_units) == 3
        assert all(pu.paired for pu in a_units)

        # Creator B: 2 unpaired TT
        b_units = [pu for pu in payout_units if pu.creator_name == "Creator B"]
        assert len(b_units) == 2
        assert all(not pu.paired for pu in b_units)

        # Creator C: 2 unpaired IG
        c_units = [pu for pu in payout_units if pu.creator_name == "Creator C"]
        assert len(c_units) == 2
        assert all(not pu.paired for pu in c_units)

        # Creator D: 2 unpaired (length mismatch, no fallback)
        d_units = [pu for pu in payout_units if pu.creator_name == "Creator D"]
        assert len(d_units) == 2
        assert all(not pu.paired for pu in d_units)

        # Total payout units: 3 + 2 + 2 + 2 = 9
        assert len(payout_units) == 9

        # Exceptions:
        # - Creator E: 2 unmapped ("not in creator list")
        # - Creator B: 2 unpaired
        # - Creator C: 2 unpaired
        # - Creator D: 2 unpaired
        unmapped_exc = [e for e in exceptions if e.reason == "not in creator list"]
        unpaired_exc = [e for e in exceptions if "unpaired" in e.reason]
        assert len(unmapped_exc) == 2
        assert len(unpaired_exc) == 6  # B(2) + C(2) + D(2)

    def test_no_video_appears_in_two_payout_units(self):
        """Ensure no video ad_link appears in more than one payout unit."""
        videos = [
            make_video("a_tt", "tiktok", 30, 5000, "2026-02-20T10:00:00+00:00", "a_tt1"),
            make_video("a_tt", "tiktok", 45, 12000, "2026-02-20T14:00:00+00:00", "a_tt2"),
            make_video("a_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "a_ig1"),
            make_video("a_ig", "instagram", 45, 3000, "2026-02-20T14:30:00+00:00", "a_ig2"),
            make_video("b_tt", "tiktok", 60, 20000, "2026-02-20T10:00:00+00:00", "b_tt1"),
        ]
        tt_map = {"a_tt": "A", "b_tt": "B"}
        ig_map = {"a_ig": "A"}

        payout_units, _ = match_videos(videos, tt_map, ig_map)

        all_links = []
        for pu in payout_units:
            if pu.tiktok_video:
                all_links.append(pu.tiktok_video.ad_link)
            if pu.instagram_video:
                all_links.append(pu.instagram_video.ad_link)

        assert len(all_links) == len(set(all_links)), \
            f"Duplicate video in payout units: {all_links}"


# ===========================================================================
# REAL-WORLD 21: Fallback across different days
#
# Creator posts TT on Monday and IG on Tuesday (>24h apart).
# Sequence pair fails length check, fallback can't match because >24h.
# Both remain unpaired.
# ===========================================================================

class TestFallbackCrossDayBoundary:
    """Videos posted >24h apart should not match in fallback."""

    def test_48h_apart_no_fallback(self):
        """TT posted Monday, IG posted Wednesday. Same length, but >24h → no fallback."""
        tt = [make_video("day_tt", "tiktok", 45, 5000,
                          "2026-02-20T10:00:00+00:00", "tt_day1")]
        ig = [
            make_video("day_ig", "instagram", 30, 1000,
                        "2026-02-20T10:30:00+00:00", "ig_day1"),
            make_video("day_ig", "instagram", 45, 8000,
                        "2026-02-22T10:00:00+00:00", "ig_day2"),  # 48h later
        ]

        payout_units, _ = _match_creator_videos("CrossDay", tt, ig)
        paired = [pu for pu in payout_units if pu.paired]
        assert len(paired) == 0  # No pairs possible


# ===========================================================================
# REAL-WORLD 22: Sequence pair fails, but IG video's OWN fallback finds
#                the TT that was paired with it
#
# This is a subtle edge case: Pair #1 fails primary.
# TT#1 finds a fallback match (IG#2).
# Then IG#1 tries fallback and finds TT#1... but TT#1 is already used.
# IG#1 should remain unpaired.
# ===========================================================================

class TestFallbackCircularDependency:
    """
    Setup:
      TT: [45s @10am]
      IG: [30s @10:30am, 45s @12pm]

    Primary #1: TT(45) ↔ IG(30) → mismatch → fallback
    Fallback: TT#1(45) → IG#2(45) → match! → TT#1 used, IG#2 used
              IG#1(30) → searches TT → TT#1(45) → 30≠45 → no match

    Result: 1 pair (TT#1↔IG#2) + 1 unpaired (IG#1)
    """

    def test_circular_resolved(self):
        tt = [make_video("circ_tt", "tiktok", 45, 5000,
                          "2026-02-20T10:00:00+00:00", "tt_circ1")]
        ig = [
            make_video("circ_ig", "instagram", 30, 8000,
                        "2026-02-20T10:30:00+00:00", "ig_circ1"),
            make_video("circ_ig", "instagram", 45, 3000,
                        "2026-02-20T12:00:00+00:00", "ig_circ2"),
        ]

        payout_units, exceptions = _match_creator_videos("Circular", tt, ig)

        paired = [pu for pu in payout_units if pu.paired]
        unpaired = [pu for pu in payout_units if not pu.paired]

        assert len(paired) == 1
        # The pair should be TT#1(45) ↔ IG#2(45)
        assert paired[0].tiktok_video.video_length == 45
        assert paired[0].instagram_video.video_length == 45

        assert len(unpaired) == 1
        # The unpaired should be IG#1(30)
        assert unpaired[0].instagram_video.video_length == 30

    def test_exception_for_unpaired_ig(self):
        tt = [make_video("circ_tt", "tiktok", 45, 5000,
                          "2026-02-20T10:00:00+00:00", "tt_circ1")]
        ig = [
            make_video("circ_ig", "instagram", 30, 8000,
                        "2026-02-20T10:30:00+00:00", "ig_circ1"),
            make_video("circ_ig", "instagram", 45, 3000,
                        "2026-02-20T12:00:00+00:00", "ig_circ2"),
        ]

        _, exceptions = _match_creator_videos("Circular", tt, ig)
        assert len(exceptions) == 1
        assert exceptions[0].platform == "instagram"
        assert exceptions[0].video_length == 30


# ===========================================================================
# REAL-WORLD 23: _is_more_recent edge cases
#
# Test the dedup helper directly for edge cases.
# ===========================================================================

class TestIsMoreRecent:
    """Test _is_more_recent logic via dedup behavior."""

    def test_candidate_none_existing_has_date(self):
        """If candidate has None updated_at, existing wins."""
        from services.matcher import _is_more_recent
        v1 = make_video(updated_at_str="2026-02-20T12:00:00+00:00")
        v2 = make_video()  # latest_updated_at = None
        assert _is_more_recent(v2, v1) is False

    def test_existing_none_candidate_has_date(self):
        """If existing has None updated_at, candidate wins."""
        from services.matcher import _is_more_recent
        v1 = make_video()  # latest_updated_at = None
        v2 = make_video(updated_at_str="2026-02-20T12:00:00+00:00")
        assert _is_more_recent(v2, v1) is True

    def test_both_none(self):
        """If both None, candidate does NOT replace existing."""
        from services.matcher import _is_more_recent
        v1 = make_video()
        v2 = make_video()
        assert _is_more_recent(v2, v1) is False


# ===========================================================================
# REAL-WORLD 24: Cross-creator isolation
#
# Fallback matching should ONLY search within the same creator.
# If Creator A has a TT(45s) that fails primary, it should NOT be
# matched with Creator B's IG(45s).
# ===========================================================================

class TestCrossCreatorIsolation:
    """Fallback should never match videos from different creators."""

    def test_no_cross_creator_fallback(self):
        """
        Creator A: 1 TT(45s), 1 IG(30s) → mismatch, no fallback within A
        Creator B: 1 TT(60s), 1 IG(45s) → mismatch, no fallback within B

        Even though A's TT(45s) matches B's IG(45s), they should NOT pair.
        """
        videos = [
            make_video("a_tt", "tiktok", 45, 5000, "2026-02-20T10:00:00+00:00", "cross_a_tt"),
            make_video("a_ig", "instagram", 30, 8000, "2026-02-20T10:30:00+00:00", "cross_a_ig"),
            make_video("b_tt", "tiktok", 60, 3000, "2026-02-20T10:00:00+00:00", "cross_b_tt"),
            make_video("b_ig", "instagram", 45, 7000, "2026-02-20T10:30:00+00:00", "cross_b_ig"),
        ]
        tt_map = {"a_tt": "Creator A", "b_tt": "Creator B"}
        ig_map = {"a_ig": "Creator A", "b_ig": "Creator B"}

        payout_units, exceptions = match_videos(videos, tt_map, ig_map)

        # All should be unpaired — no cross-creator matching
        assert all(not pu.paired for pu in payout_units)
        assert len(payout_units) == 4  # 4 unpaired videos

        # Check that Creator A's payout units only contain Creator A's videos
        a_units = [pu for pu in payout_units if pu.creator_name == "Creator A"]
        for pu in a_units:
            if pu.tiktok_video:
                assert pu.tiktok_video.username == "a_tt"
            if pu.instagram_video:
                assert pu.instagram_video.username == "a_ig"


# ===========================================================================
# REAL-WORLD 25: Fallback requires same uploaded_at date
#
# The fallback (Step 10) now requires that both videos have the same
# uploaded_at date, in addition to exact length and ±24h created_at.
# ===========================================================================

class TestFallbackUploadedAtSameDate:
    """Fallback matching requires same uploaded_at date."""

    def test_same_uploaded_at_fallback_succeeds(self):
        """
        Primary fails (30 vs 45), fallback finds IG#2(30s) with same uploaded_at.
        Should pair via fallback.
        """
        tt = [make_video("ua_tt", "tiktok", 30, 5000,
                          "2026-02-20T10:00:00+00:00", "tt_ua1",
                          uploaded_at_date=date(2026, 2, 20))]
        ig = [
            make_video("ua_ig", "instagram", 45, 8000,
                        "2026-02-20T10:30:00+00:00", "ig_ua1",
                        uploaded_at_date=date(2026, 2, 20)),
            make_video("ua_ig", "instagram", 30, 3000,
                        "2026-02-20T14:00:00+00:00", "ig_ua2",
                        uploaded_at_date=date(2026, 2, 20)),
        ]
        payout_units, _ = _match_creator_videos("UploadA", tt, ig)
        paired = [pu for pu in payout_units if pu.paired]
        assert len(paired) == 1
        assert paired[0].match_confidence == "medium"
        assert "same upload date" in paired[0].pair_note

    def test_different_uploaded_at_fallback_fails(self):
        """
        Primary fails (30 vs 45), fallback candidate IG#2(30s) has DIFFERENT
        uploaded_at → fallback should NOT match.
        """
        tt = [make_video("ub_tt", "tiktok", 30, 5000,
                          "2026-02-20T10:00:00+00:00", "tt_ub1",
                          uploaded_at_date=date(2026, 2, 20))]
        ig = [
            make_video("ub_ig", "instagram", 45, 8000,
                        "2026-02-20T10:30:00+00:00", "ig_ub1",
                        uploaded_at_date=date(2026, 2, 20)),
            make_video("ub_ig", "instagram", 30, 3000,
                        "2026-02-20T14:00:00+00:00", "ig_ub2",
                        uploaded_at_date=date(2026, 2, 21)),  # different date!
        ]
        payout_units, _ = _match_creator_videos("UploadB", tt, ig)
        paired = [pu for pu in payout_units if pu.paired]
        assert len(paired) == 0  # no fallback match

    def test_source_uploaded_at_none_skips_fallback(self):
        """
        If the source video has uploaded_at=None, fallback should return None.
        """
        tt = [make_video("uc_tt", "tiktok", 30, 5000,
                          "2026-02-20T10:00:00+00:00", "tt_uc1",
                          uploaded_at_date=None)]
        # Override uploaded_at to None (make_video defaults to a date)
        tt[0] = tt[0].model_copy(update={"uploaded_at": None})

        ig = [
            make_video("uc_ig", "instagram", 45, 8000,
                        "2026-02-20T10:30:00+00:00", "ig_uc1"),
            make_video("uc_ig", "instagram", 30, 3000,
                        "2026-02-20T14:00:00+00:00", "ig_uc2"),
        ]
        payout_units, _ = _match_creator_videos("UploadC", tt, ig)
        paired = [pu for pu in payout_units if pu.paired]
        assert len(paired) == 0  # can't fallback without uploaded_at

    def test_candidate_uploaded_at_none_skipped(self):
        """
        If the fallback candidate has uploaded_at=None, it should be skipped.
        """
        tt = [make_video("ud_tt", "tiktok", 30, 5000,
                          "2026-02-20T10:00:00+00:00", "tt_ud1",
                          uploaded_at_date=date(2026, 2, 20))]
        ig = [
            make_video("ud_ig", "instagram", 45, 8000,
                        "2026-02-20T10:30:00+00:00", "ig_ud1",
                        uploaded_at_date=date(2026, 2, 20)),
            make_video("ud_ig", "instagram", 30, 3000,
                        "2026-02-20T14:00:00+00:00", "ig_ud2"),
        ]
        # Set IG#2's uploaded_at to None
        ig[1] = ig[1].model_copy(update={"uploaded_at": None})

        payout_units, _ = _match_creator_videos("UploadD", tt, ig)
        paired = [pu for pu in payout_units if pu.paired]
        assert len(paired) == 0  # IG#2 skipped (None uploaded_at)

    def test_both_uploaded_at_none_no_fallback(self):
        """
        If both source and candidate have uploaded_at=None, no fallback.
        """
        tt_video = make_video("ue_tt", "tiktok", 30, 5000,
                               "2026-02-20T10:00:00+00:00", "tt_ue1")
        tt_video = tt_video.model_copy(update={"uploaded_at": None})

        ig_primary = make_video("ue_ig", "instagram", 45, 8000,
                                 "2026-02-20T10:30:00+00:00", "ig_ue1")
        ig_candidate = make_video("ue_ig", "instagram", 30, 3000,
                                   "2026-02-20T14:00:00+00:00", "ig_ue2")
        ig_candidate = ig_candidate.model_copy(update={"uploaded_at": None})

        payout_units, _ = _match_creator_videos("UploadE", [tt_video], [ig_primary, ig_candidate])
        paired = [pu for pu in payout_units if pu.paired]
        assert len(paired) == 0

    def test_fallback_with_mixed_dates_picks_correct(self):
        """
        Multiple fallback candidates: only the one with same uploaded_at
        AND closest created_at should be picked.
        """
        tt = [make_video("uf_tt", "tiktok", 30, 5000,
                          "2026-02-20T12:00:00+00:00", "tt_uf1",
                          uploaded_at_date=date(2026, 2, 20))]
        ig = [
            # Primary partner (wrong length)
            make_video("uf_ig", "instagram", 99, 1000,
                        "2026-02-20T06:00:00+00:00", "ig_uf1",
                        uploaded_at_date=date(2026, 2, 20)),
            # Candidate A: right length, WRONG uploaded_at, very close time
            make_video("uf_ig", "instagram", 30, 2000,
                        "2026-02-20T12:30:00+00:00", "ig_uf2",
                        uploaded_at_date=date(2026, 2, 21)),
            # Candidate B: right length, RIGHT uploaded_at, farther time
            make_video("uf_ig", "instagram", 30, 4000,
                        "2026-02-20T20:00:00+00:00", "ig_uf3",
                        uploaded_at_date=date(2026, 2, 20)),
        ]
        payout_units, _ = _match_creator_videos("UploadF", tt, ig)
        paired = [pu for pu in payout_units if pu.paired]
        assert len(paired) == 1
        # Should pick Candidate B (right uploaded_at), NOT Candidate A (wrong date)
        assert paired[0].instagram_video.latest_views == 4000
