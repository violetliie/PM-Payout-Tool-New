"""
Comprehensive tests for services/payout.py (SPEC.md Steps A–D).

Test categories:
  1. REQUIRED TIER TESTS (19 exact expected results from Phase 5 spec)
  2. TIER BOUNDARY TESTS (every boundary value: min, max, off-by-one)
  3. EFFECTIVE VIEWS / CAP TESTS
  4. PROCESS PAYOUTS TESTS (PayoutUnit field population)
  5. CREATOR SUMMARY TESTS (multi-creator aggregation, counts, isolation)
  6. FULL PIPELINE TESTS (run_payout_pipeline end-to-end)
  7. EDGE CASE TESTS (0 views, empty list, single video, etc.)
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from models.schemas import PayoutUnit, Video, CreatorSummary
from services.payout import (
    calculate_effective_views,
    calculate_payout,
    process_payouts,
    build_creator_summaries,
    run_payout_pipeline,
    VIEW_CAP,
    QUALIFICATION_THRESHOLD,
)
from datetime import date, datetime


# ===========================================================================
# Test helpers
# ===========================================================================

def make_payout_unit(
    creator_name: str = "TestCreator",
    chosen_views: int = 5000,
    paired: bool = True,
    best_platform: str = "tiktok",
    match_confidence: str = "high",
) -> PayoutUnit:
    """Helper to create a PayoutUnit with sensible defaults."""
    tt_video = Video(
        username="test_tt", platform="tiktok",
        ad_link="https://tiktok.com/test",
        uploaded_at=date(2026, 2, 20),
        created_at=datetime(2026, 2, 20, 10, 0, 0),
        video_length=30, latest_views=chosen_views,
    )
    ig_video = Video(
        username="test_ig", platform="instagram",
        ad_link="https://instagram.com/test",
        uploaded_at=date(2026, 2, 20),
        created_at=datetime(2026, 2, 20, 10, 30, 0),
        video_length=30, latest_views=chosen_views // 2,
    ) if paired else None

    return PayoutUnit(
        creator_name=creator_name,
        tiktok_video=tt_video,
        instagram_video=ig_video,
        chosen_views=chosen_views,
        best_platform=best_platform,
        paired=paired,
        match_confidence=match_confidence,
        pair_note="exact match" if paired else "unpaired — single platform only",
    )


# ===========================================================================
# 1. REQUIRED TIER TESTS — 19 exact expected results from Phase 5 spec
# ===========================================================================

class TestRequiredTierValues:
    """
    All 19 test cases with EXACT expected results from the Phase 5 spec.
    Tests the full path: chosen_views → effective_views → payout_amount.
    """

    @pytest.mark.parametrize("views,expected_payout", [
        (500,        0.0),       # < 1K → not qualified
        (2_500,      35.0),      # 1K – 9,999
        (35_000,     50.0),      # 10K – 49,999
        (75_000,     100.0),     # 50K – 99,999
        (180_000,    150.0),     # 100K – 249,999
        (400_000,    300.0),     # 250K – 499,999
        (800_000,    500.0),     # 500K – 999,999
        (1_500_000,  700.0),     # 1M – 1,999,999
        (2_500_000,  900.0),     # 2M – 2,999,999
        (3_500_000,  1_100.0),   # 3M – 3,999,999
        (4_500_000,  1_300.0),   # 4M – 4,999,999
        (5_500_000,  1_500.0),   # 5M – 5,999,999
        (6_000_000,  1_650.0),   # 6M: 1500 + 150*(6-5) = 1650
        (6_700_000,  1_650.0),   # 6.7M: floor_millions=6 → 1650
        (7_000_000,  1_800.0),   # 7M: 1500 + 150*(7-5) = 1800
        (9_200_000,  2_100.0),   # 9.2M: floor_millions=9 → 1500+150*(9-5) = 2100
        (10_000_000, 2_250.0),   # 10M: floor_millions=10 → 1500+150*(10-5) = 2250
        (12_000_000, 2_250.0),   # 12M → CAPPED to 10M → $2,250
        (50_000_000, 2_250.0),   # 50M → CAPPED to 10M → $2,250
    ])
    def test_tier_payout(self, views, expected_payout):
        effective = calculate_effective_views(views)
        actual = calculate_payout(effective)
        assert actual == expected_payout, (
            f"views={views:,}: expected ${expected_payout:,.2f}, got ${actual:,.2f}"
        )


# ===========================================================================
# 2. TIER BOUNDARY TESTS — every boundary value
# ===========================================================================

class TestTierBoundaries:
    """
    Test exact boundary values for every tier transition.
    This catches off-by-one errors in the tier table.
    """

    # --- Below qualification ---

    def test_0_views(self):
        assert calculate_payout(0) == 0.0

    def test_999_views(self):
        """Just below qualification threshold."""
        assert calculate_payout(999) == 0.0

    # --- Tier 1: 1,000 – 9,999 → $35 ---

    def test_1000_views_min_boundary(self):
        """Exact qualification threshold."""
        assert calculate_payout(1_000) == 35.0

    def test_9999_views_max_boundary(self):
        assert calculate_payout(9_999) == 35.0

    # --- Tier 2: 10,000 – 49,999 → $50 ---

    def test_10000_views_min_boundary(self):
        assert calculate_payout(10_000) == 50.0

    def test_49999_views_max_boundary(self):
        assert calculate_payout(49_999) == 50.0

    # --- Tier 3: 50,000 – 99,999 → $100 ---

    def test_50000_views_min_boundary(self):
        assert calculate_payout(50_000) == 100.0

    def test_99999_views_max_boundary(self):
        assert calculate_payout(99_999) == 100.0

    # --- Tier 4: 100,000 – 249,999 → $150 ---

    def test_100000_views_min_boundary(self):
        assert calculate_payout(100_000) == 150.0

    def test_249999_views_max_boundary(self):
        assert calculate_payout(249_999) == 150.0

    # --- Tier 5: 250,000 – 499,999 → $300 ---

    def test_250000_views_min_boundary(self):
        assert calculate_payout(250_000) == 300.0

    def test_499999_views_max_boundary(self):
        assert calculate_payout(499_999) == 300.0

    # --- Tier 6: 500,000 – 999,999 → $500 ---

    def test_500000_views_min_boundary(self):
        assert calculate_payout(500_000) == 500.0

    def test_999999_views_max_boundary(self):
        assert calculate_payout(999_999) == 500.0

    # --- Tier 7: 1,000,000 – 1,999,999 → $700 ---

    def test_1000000_views_min_boundary(self):
        assert calculate_payout(1_000_000) == 700.0

    def test_1999999_views_max_boundary(self):
        assert calculate_payout(1_999_999) == 700.0

    # --- Tier 8: 2,000,000 – 2,999,999 → $900 ---

    def test_2000000_views_min_boundary(self):
        assert calculate_payout(2_000_000) == 900.0

    def test_2999999_views_max_boundary(self):
        assert calculate_payout(2_999_999) == 900.0

    # --- Tier 9: 3,000,000 – 3,999,999 → $1,100 ---

    def test_3000000_views_min_boundary(self):
        assert calculate_payout(3_000_000) == 1_100.0

    def test_3999999_views_max_boundary(self):
        assert calculate_payout(3_999_999) == 1_100.0

    # --- Tier 10: 4,000,000 – 4,999,999 → $1,300 ---

    def test_4000000_views_min_boundary(self):
        assert calculate_payout(4_000_000) == 1_300.0

    def test_4999999_views_max_boundary(self):
        assert calculate_payout(4_999_999) == 1_300.0

    # --- Tier 11: 5,000,000 – 5,999,999 → $1,500 ---

    def test_5000000_views_min_boundary(self):
        assert calculate_payout(5_000_000) == 1_500.0

    def test_5999999_views_max_boundary(self):
        assert calculate_payout(5_999_999) == 1_500.0

    # --- Formula tier: 6,000,000 – 10,000,000 ---

    def test_6000000_formula_start(self):
        """6M: 1500 + 150*(6-5) = $1,650."""
        assert calculate_payout(6_000_000) == 1_650.0

    def test_6999999_formula_6m(self):
        """6.999M: floor_millions=6 → $1,650."""
        assert calculate_payout(6_999_999) == 1_650.0

    def test_7000000_formula_7m(self):
        """7M: 1500 + 150*(7-5) = $1,800."""
        assert calculate_payout(7_000_000) == 1_800.0

    def test_8000000_formula_8m(self):
        """8M: 1500 + 150*(8-5) = $1,950."""
        assert calculate_payout(8_000_000) == 1_950.0

    def test_9000000_formula_9m(self):
        """9M: 1500 + 150*(9-5) = $2,100."""
        assert calculate_payout(9_000_000) == 2_100.0

    def test_9999999_just_under_10m(self):
        """9.999M: floor_millions=9 → $2,100."""
        assert calculate_payout(9_999_999) == 2_100.0

    def test_10000000_max_cap(self):
        """10M: floor_millions=10 → $2,250."""
        assert calculate_payout(10_000_000) == 2_250.0


# ===========================================================================
# 3. EFFECTIVE VIEWS / CAP TESTS
# ===========================================================================

class TestEffectiveViews:
    """Test calculate_effective_views (10M cap)."""

    def test_below_cap(self):
        assert calculate_effective_views(5_000_000) == 5_000_000

    def test_at_cap(self):
        assert calculate_effective_views(10_000_000) == 10_000_000

    def test_above_cap(self):
        assert calculate_effective_views(12_000_000) == 10_000_000

    def test_way_above_cap(self):
        assert calculate_effective_views(50_000_000) == 10_000_000

    def test_zero(self):
        assert calculate_effective_views(0) == 0

    def test_one(self):
        assert calculate_effective_views(1) == 1

    def test_just_below_cap(self):
        assert calculate_effective_views(9_999_999) == 9_999_999

    def test_just_above_cap(self):
        assert calculate_effective_views(10_000_001) == 10_000_000

    def test_preserves_original_below_cap(self):
        """Effective views equals chosen_views when below cap."""
        for v in [500, 1_000, 50_000, 999_999, 5_000_000]:
            assert calculate_effective_views(v) == v


# ===========================================================================
# 4. PROCESS PAYOUTS TESTS — PayoutUnit field population
# ===========================================================================

class TestProcessPayouts:
    """Test process_payouts: fills in effective_views and payout_amount on PayoutUnits."""

    def test_single_unit_fields_populated(self):
        """effective_views and payout_amount should be set after processing."""
        unit = make_payout_unit(chosen_views=35_000)
        result = process_payouts([unit])

        assert len(result) == 1
        assert result[0].effective_views == 35_000
        assert result[0].payout_amount == 50.0

    def test_chosen_views_not_modified(self):
        """chosen_views must NOT be changed (audit trail)."""
        unit = make_payout_unit(chosen_views=12_000_000)
        process_payouts([unit])
        assert unit.chosen_views == 12_000_000  # original preserved
        assert unit.effective_views == 10_000_000  # capped

    def test_below_qualification_gets_zero_payout(self):
        """Videos with < 1,000 views get $0 payout."""
        unit = make_payout_unit(chosen_views=500)
        process_payouts([unit])
        assert unit.payout_amount == 0.0
        assert unit.effective_views == 500

    def test_multiple_units(self):
        """All units in the list get processed."""
        units = [
            make_payout_unit(chosen_views=2_500),    # $35
            make_payout_unit(chosen_views=800_000),   # $500
            make_payout_unit(chosen_views=500),        # $0
        ]
        result = process_payouts(units)
        assert len(result) == 3
        assert result[0].payout_amount == 35.0
        assert result[1].payout_amount == 500.0
        assert result[2].payout_amount == 0.0

    def test_returns_same_list(self):
        """process_payouts modifies in place and returns the same list."""
        units = [make_payout_unit(chosen_views=5_000)]
        result = process_payouts(units)
        assert result is units  # same object

    def test_empty_list(self):
        """Empty list → no errors, returns empty list."""
        result = process_payouts([])
        assert result == []

    def test_capped_unit_preserves_chosen_views(self):
        """Capped video: chosen_views=15M, effective=10M, payout=$2,250."""
        unit = make_payout_unit(chosen_views=15_000_000)
        process_payouts([unit])
        assert unit.chosen_views == 15_000_000
        assert unit.effective_views == 10_000_000
        assert unit.payout_amount == 2_250.0


# ===========================================================================
# 5. CREATOR SUMMARY TESTS — multi-creator aggregation
# ===========================================================================

class TestCreatorSummaries:
    """Test build_creator_summaries: aggregation per creator."""

    def test_single_creator_single_video(self):
        """One creator, one video."""
        unit = make_payout_unit("Alice", chosen_views=35_000, paired=True)
        unit.payout_amount = 50.0
        unit.effective_views = 35_000

        summaries = build_creator_summaries([unit])
        assert len(summaries) == 1
        s = summaries[0]
        assert s.creator_name == "Alice"
        assert s.qualified_video_count == 1
        assert s.total_payout == 50.0
        assert s.paired_video_count == 1
        assert s.unpaired_video_count == 0
        assert s.exception_count == 0

    def test_single_creator_multiple_videos(self):
        """One creator with 3 videos → summed payout."""
        units = [
            make_payout_unit("Bob", chosen_views=35_000, paired=True),    # $50
            make_payout_unit("Bob", chosen_views=800_000, paired=True),   # $500
            make_payout_unit("Bob", chosen_views=500, paired=False),      # $0
        ]
        # Simulate process_payouts
        for u in units:
            u.effective_views = calculate_effective_views(u.chosen_views)
            u.payout_amount = calculate_payout(u.effective_views)

        summaries = build_creator_summaries(units)
        assert len(summaries) == 1
        s = summaries[0]
        assert s.creator_name == "Bob"
        assert s.qualified_video_count == 2  # 35K and 800K qualify, 500 doesn't
        assert s.total_payout == 550.0       # $50 + $500 + $0
        assert s.paired_video_count == 2
        assert s.unpaired_video_count == 1

    def test_two_creators_isolated(self):
        """
        Two creators with different videos.
        CRITICAL: payouts must NOT be mixed between creators.
        """
        units = [
            make_payout_unit("Alice", chosen_views=35_000, paired=True),   # $50
            make_payout_unit("Alice", chosen_views=800_000, paired=False), # $500
            make_payout_unit("Bob", chosen_views=2_500_000, paired=True),  # $900
            make_payout_unit("Bob", chosen_views=180_000, paired=True),    # $150
        ]
        for u in units:
            u.effective_views = calculate_effective_views(u.chosen_views)
            u.payout_amount = calculate_payout(u.effective_views)

        summaries = build_creator_summaries(units)
        assert len(summaries) == 2

        alice = next(s for s in summaries if s.creator_name == "Alice")
        bob = next(s for s in summaries if s.creator_name == "Bob")

        # Alice: $50 + $500 = $550
        assert alice.total_payout == 550.0
        assert alice.qualified_video_count == 2
        assert alice.paired_video_count == 1
        assert alice.unpaired_video_count == 1

        # Bob: $900 + $150 = $1,050
        assert bob.total_payout == 1_050.0
        assert bob.qualified_video_count == 2
        assert bob.paired_video_count == 2
        assert bob.unpaired_video_count == 0

    def test_three_creators(self):
        """Three different creators → three separate summaries."""
        units = [
            make_payout_unit("Alice", chosen_views=5_000, paired=True),
            make_payout_unit("Bob", chosen_views=50_000, paired=False),
            make_payout_unit("Charlie", chosen_views=500_000, paired=True),
        ]
        for u in units:
            u.effective_views = calculate_effective_views(u.chosen_views)
            u.payout_amount = calculate_payout(u.effective_views)

        summaries = build_creator_summaries(units)
        assert len(summaries) == 3
        names = [s.creator_name for s in summaries]
        assert names == ["Alice", "Bob", "Charlie"]  # sorted

    def test_exception_counts_applied(self):
        """Exception counts from dict should be set on each creator."""
        unit = make_payout_unit("Alice", chosen_views=5_000, paired=True)
        unit.payout_amount = 35.0
        unit.effective_views = 5_000

        exc_counts = {"Alice": 3, "Bob": 1}
        summaries = build_creator_summaries([unit], exc_counts)
        assert summaries[0].exception_count == 3

    def test_exception_counts_default_zero(self):
        """Creators not in exception_counts dict default to 0."""
        unit = make_payout_unit("Alice", chosen_views=5_000, paired=True)
        unit.payout_amount = 35.0
        unit.effective_views = 5_000

        summaries = build_creator_summaries([unit], {"Bob": 5})
        assert summaries[0].exception_count == 0

    def test_empty_input(self):
        """No payout units → no summaries."""
        summaries = build_creator_summaries([])
        assert len(summaries) == 0

    def test_all_unqualified(self):
        """Creator with all videos under 1K views → qualified=0, payout=$0."""
        units = [
            make_payout_unit("Zero", chosen_views=500, paired=True),
            make_payout_unit("Zero", chosen_views=999, paired=False),
        ]
        for u in units:
            u.effective_views = calculate_effective_views(u.chosen_views)
            u.payout_amount = calculate_payout(u.effective_views)

        summaries = build_creator_summaries(units)
        assert len(summaries) == 1
        s = summaries[0]
        assert s.qualified_video_count == 0
        assert s.total_payout == 0.0
        assert s.paired_video_count == 1
        assert s.unpaired_video_count == 1

    def test_qualified_counts_only_above_threshold(self):
        """
        Mixed qualified and unqualified:
          - 5,000 views → qualified ($35)
          - 999 views → NOT qualified ($0)
          - 1,000 views → qualified ($35)
        """
        units = [
            make_payout_unit("Mix", chosen_views=5_000, paired=True),
            make_payout_unit("Mix", chosen_views=999, paired=False),
            make_payout_unit("Mix", chosen_views=1_000, paired=False),
        ]
        for u in units:
            u.effective_views = calculate_effective_views(u.chosen_views)
            u.payout_amount = calculate_payout(u.effective_views)

        summaries = build_creator_summaries(units)
        s = summaries[0]
        assert s.qualified_video_count == 2  # 5,000 + 1,000
        assert s.total_payout == 70.0        # $35 + $0 + $35

    def test_paired_count_is_per_unit_not_per_video(self):
        """
        1 pair = 1 paired_video_count (NOT 2).
        3 paired PayoutUnits → paired_video_count = 3.
        """
        units = [
            make_payout_unit("Counter", chosen_views=5_000, paired=True),
            make_payout_unit("Counter", chosen_views=10_000, paired=True),
            make_payout_unit("Counter", chosen_views=50_000, paired=True),
        ]
        for u in units:
            u.effective_views = calculate_effective_views(u.chosen_views)
            u.payout_amount = calculate_payout(u.effective_views)

        summaries = build_creator_summaries(units)
        assert summaries[0].paired_video_count == 3
        assert summaries[0].unpaired_video_count == 0

    def test_summaries_sorted_by_name(self):
        """Summaries should be sorted alphabetically by creator_name."""
        units = [
            make_payout_unit("Zoe", chosen_views=5_000, paired=True),
            make_payout_unit("Alice", chosen_views=5_000, paired=True),
            make_payout_unit("Mike", chosen_views=5_000, paired=True),
        ]
        for u in units:
            u.effective_views = 5_000
            u.payout_amount = 35.0

        summaries = build_creator_summaries(units)
        names = [s.creator_name for s in summaries]
        assert names == ["Alice", "Mike", "Zoe"]


# ===========================================================================
# 6. FULL PIPELINE TESTS — run_payout_pipeline end-to-end
# ===========================================================================

class TestRunPayoutPipeline:
    """End-to-end tests for run_payout_pipeline."""

    def test_basic_pipeline(self):
        """Full pipeline: process payouts + build summaries."""
        units = [
            make_payout_unit("Alice", chosen_views=35_000, paired=True),
            make_payout_unit("Alice", chosen_views=800_000, paired=False),
            make_payout_unit("Bob", chosen_views=2_500_000, paired=True),
        ]

        processed, summaries = run_payout_pipeline(units)

        # Verify processed units have payout amounts
        assert processed[0].payout_amount == 50.0
        assert processed[1].payout_amount == 500.0
        assert processed[2].payout_amount == 900.0

        # Verify summaries
        assert len(summaries) == 2
        alice = next(s for s in summaries if s.creator_name == "Alice")
        bob = next(s for s in summaries if s.creator_name == "Bob")
        assert alice.total_payout == 550.0
        assert bob.total_payout == 900.0

    def test_pipeline_with_exception_counts(self):
        """Pipeline passes exception_counts through to summaries."""
        units = [
            make_payout_unit("Alice", chosen_views=5_000, paired=True),
        ]
        exc = {"Alice": 2}

        _, summaries = run_payout_pipeline(units, exc)
        assert summaries[0].exception_count == 2

    def test_pipeline_empty(self):
        """Empty input → empty output."""
        processed, summaries = run_payout_pipeline([])
        assert processed == []
        assert summaries == []

    def test_pipeline_returns_same_units(self):
        """Processed units should be the same list (mutated in place)."""
        units = [make_payout_unit("A", chosen_views=5_000)]
        processed, _ = run_payout_pipeline(units)
        assert processed is units


# ===========================================================================
# 7. MULTI-CREATOR REALISTIC SCENARIO
# ===========================================================================

class TestMultiCreatorRealistic:
    """
    Realistic scenario with 3 creators, multiple videos each.
    Verifies per-creator isolation of payout sums.

    Creator A: 1 paired (50K views → $100) + 1 paired (2M views → $900)
      → qualified=2, total=$1,000, paired=2, unpaired=0

    Creator B: 1 paired (500 views → $0) + 1 unpaired (15K views → $50) + 1 exception
      → qualified=1, total=$50, paired=1, unpaired=1, exceptions=1

    Creator C: 1 unpaired (12M views → capped → $2,250)
      → qualified=1, total=$2,250, paired=0, unpaired=1
    """

    def setup_method(self):
        self.units = [
            make_payout_unit("Creator A", chosen_views=50_000, paired=True),
            make_payout_unit("Creator A", chosen_views=2_000_000, paired=True),
            make_payout_unit("Creator B", chosen_views=500, paired=True),
            make_payout_unit("Creator B", chosen_views=15_000, paired=False),
            make_payout_unit("Creator C", chosen_views=12_000_000, paired=False),
        ]
        self.exc_counts = {"Creator B": 1}

    def test_total_payouts_correct(self):
        processed, summaries = run_payout_pipeline(self.units, self.exc_counts)

        a = next(s for s in summaries if s.creator_name == "Creator A")
        b = next(s for s in summaries if s.creator_name == "Creator B")
        c = next(s for s in summaries if s.creator_name == "Creator C")

        assert a.total_payout == 1_000.0     # $100 + $900
        assert b.total_payout == 50.0         # $0 + $50
        assert c.total_payout == 2_250.0      # 12M capped to 10M → $2,250

    def test_qualified_counts_correct(self):
        _, summaries = run_payout_pipeline(self.units, self.exc_counts)

        a = next(s for s in summaries if s.creator_name == "Creator A")
        b = next(s for s in summaries if s.creator_name == "Creator B")
        c = next(s for s in summaries if s.creator_name == "Creator C")

        assert a.qualified_video_count == 2  # both ≥ 1K
        assert b.qualified_video_count == 1  # only 15K qualifies, 500 doesn't
        assert c.qualified_video_count == 1  # 12M qualifies

    def test_paired_unpaired_counts(self):
        _, summaries = run_payout_pipeline(self.units, self.exc_counts)

        a = next(s for s in summaries if s.creator_name == "Creator A")
        b = next(s for s in summaries if s.creator_name == "Creator B")
        c = next(s for s in summaries if s.creator_name == "Creator C")

        assert a.paired_video_count == 2
        assert a.unpaired_video_count == 0

        assert b.paired_video_count == 1
        assert b.unpaired_video_count == 1

        assert c.paired_video_count == 0
        assert c.unpaired_video_count == 1

    def test_exception_counts(self):
        _, summaries = run_payout_pipeline(self.units, self.exc_counts)

        a = next(s for s in summaries if s.creator_name == "Creator A")
        b = next(s for s in summaries if s.creator_name == "Creator B")
        c = next(s for s in summaries if s.creator_name == "Creator C")

        assert a.exception_count == 0
        assert b.exception_count == 1
        assert c.exception_count == 0

    def test_individual_unit_payouts(self):
        """Verify each PayoutUnit has the correct payout_amount after processing."""
        processed, _ = run_payout_pipeline(self.units, self.exc_counts)

        assert processed[0].payout_amount == 100.0     # 50K → $100
        assert processed[1].payout_amount == 900.0      # 2M → $900
        assert processed[2].payout_amount == 0.0         # 500 → $0
        assert processed[3].payout_amount == 50.0        # 15K → $50
        assert processed[4].payout_amount == 2_250.0     # 12M capped → $2,250

    def test_effective_views_correct(self):
        """Verify effective_views is correctly capped."""
        processed, _ = run_payout_pipeline(self.units, self.exc_counts)

        assert processed[0].effective_views == 50_000
        assert processed[1].effective_views == 2_000_000
        assert processed[2].effective_views == 500
        assert processed[3].effective_views == 15_000
        assert processed[4].effective_views == 10_000_000  # capped from 12M


# ===========================================================================
# 8. ADDITIONAL EDGE CASES
# ===========================================================================

class TestEdgeCases:
    """Various edge case tests."""

    def test_exactly_1000_views(self):
        """1,000 views is the minimum qualification → $35."""
        unit = make_payout_unit(chosen_views=1_000)
        process_payouts([unit])
        assert unit.payout_amount == 35.0

    def test_exactly_999_views(self):
        """999 views is below qualification → $0."""
        unit = make_payout_unit(chosen_views=999)
        process_payouts([unit])
        assert unit.payout_amount == 0.0

    def test_zero_views(self):
        """0 views → $0."""
        unit = make_payout_unit(chosen_views=0)
        process_payouts([unit])
        assert unit.payout_amount == 0.0
        assert unit.effective_views == 0

    def test_exactly_10m_views(self):
        """10M views → not capped, $2,250."""
        unit = make_payout_unit(chosen_views=10_000_000)
        process_payouts([unit])
        assert unit.effective_views == 10_000_000
        assert unit.payout_amount == 2_250.0

    def test_10m_plus_1_views(self):
        """10,000,001 views → capped to 10M, $2,250."""
        unit = make_payout_unit(chosen_views=10_000_001)
        process_payouts([unit])
        assert unit.effective_views == 10_000_000
        assert unit.payout_amount == 2_250.0

    def test_very_large_views(self):
        """100M views → capped to 10M, $2,250."""
        unit = make_payout_unit(chosen_views=100_000_000)
        process_payouts([unit])
        assert unit.effective_views == 10_000_000
        assert unit.payout_amount == 2_250.0

    def test_single_creator_zero_qualified(self):
        """Creator appears in summary even with $0 payout."""
        unit = make_payout_unit("NoViews", chosen_views=100, paired=False)
        unit.effective_views = 100
        unit.payout_amount = 0.0

        summaries = build_creator_summaries([unit])
        assert len(summaries) == 1
        assert summaries[0].creator_name == "NoViews"
        assert summaries[0].total_payout == 0.0
        assert summaries[0].qualified_video_count == 0

    def test_formula_tier_8_500_000(self):
        """8.5M: floor_millions=8 → 1500 + 150*(8-5) = $1,950."""
        effective = calculate_effective_views(8_500_000)
        payout = calculate_payout(effective)
        assert payout == 1_950.0

    def test_formula_does_not_round_up(self):
        """6,999,999 views: floor_millions=6 (not 7) → $1,650 (not $1,800)."""
        assert calculate_payout(6_999_999) == 1_650.0

    def test_process_payouts_idempotent(self):
        """Running process_payouts twice doesn't change the result."""
        unit = make_payout_unit(chosen_views=35_000)
        process_payouts([unit])
        first_payout = unit.payout_amount
        first_effective = unit.effective_views

        process_payouts([unit])
        assert unit.payout_amount == first_payout
        assert unit.effective_views == first_effective


# ===========================================================================
# 9. ADDITIONAL: Verify spec examples from SPEC.md
# ===========================================================================

class TestSpecExamples:
    """
    Verify every payout example listed in SPEC.md "Payout Logic" section.
    These are the exact examples from the spec document.
    """

    @pytest.mark.parametrize("views,expected", [
        (500, 0.0),
        (2_500, 35.0),
        (35_000, 50.0),
        (75_000, 100.0),
        (180_000, 150.0),
        (400_000, 300.0),
        (800_000, 500.0),
        (1_500_000, 700.0),
        (2_500_000, 900.0),
        (6_700_000, 1_650.0),
        (9_200_000, 2_100.0),
        (10_000_000, 2_250.0),
        (12_000_000, 2_250.0),  # capped to 10M
    ])
    def test_spec_example(self, views, expected):
        effective = calculate_effective_views(views)
        result = calculate_payout(effective)
        assert result == expected
