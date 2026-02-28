"""
Payout tier calculation (SPEC.md Steps A–D).

CRITICAL: Payout is calculated PER VIDEO (per PayoutUnit), NOT per creator total.
Each PayoutUnit gets its own payout_amount based on its chosen_views.
Creator summaries are then built by aggregating PayoutUnits by creator_name.

Pipeline:
  1. calculate_effective_views(chosen_views) → apply 10M cap
  2. calculate_payout(effective_views) → tier lookup → dollar amount
  3. process_payouts(payout_units) → fill in effective_views + payout_amount on each unit
  4. build_creator_summaries(payout_units, exception_counts) → aggregate per creator

Tier table (applied to effective_views):
  < 1,000             → $0 (not qualified)
  1,000 – 9,999       → $35
  10,000 – 49,999     → $50
  50,000 – 99,999     → $100
  100,000 – 249,999   → $150
  250,000 – 499,999   → $300
  500,000 – 999,999   → $500
  1,000,000 – 1,999,999 → $700
  2,000,000 – 2,999,999 → $900
  3,000,000 – 3,999,999 → $1,100
  4,000,000 – 4,999,999 → $1,300
  5,000,000 – 5,999,999 → $1,500
  6,000,000 – 10,000,000 → $1,500 + $150 × (floor_millions - 5)

Where floor_millions = floor(effective_views / 1,000,000).
No tier above 10M. Views are ALWAYS capped at 10M before applying the formula.

Count definitions (CreatorSummary):
  qualified_video_count = number of paired payout units with chosen_views >= 1,000
  paired_video_count    = number of paired payout units (1 pair = 1, NOT 2)
  exception_count       = from the exception_counts dict (includes unpaired videos)
"""

import logging
import math
from models.schemas import PayoutUnit, CreatorSummary

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
VIEW_CAP = 10_000_000           # Max views for payout calculation
QUALIFICATION_THRESHOLD = 1_000  # Minimum views to qualify for any payout

# ---------------------------------------------------------------------------
# Payout tier table — (min_views, max_views, payout_amount)
# For tiers below 6M, these are fixed-amount lookups.
# The 6M–10M tier uses a formula, handled separately.
# ---------------------------------------------------------------------------
FIXED_TIERS = [
    # (min_views_inclusive, max_views_inclusive, payout_dollar)
    (1_000,     9_999,      35.0),
    (10_000,    49_999,     50.0),
    (50_000,    99_999,     100.0),
    (100_000,   249_999,    150.0),
    (250_000,   499_999,    300.0),
    (500_000,   999_999,    500.0),
    (1_000_000, 1_999_999,  700.0),
    (2_000_000, 2_999_999,  900.0),
    (3_000_000, 3_999_999,  1_100.0),
    (4_000_000, 4_999_999,  1_300.0),
    (5_000_000, 5_999_999,  1_500.0),
]

# 6M+ tier constants
HIGH_TIER_FLOOR = 6_000_000     # Views at which the formula tier starts
HIGH_TIER_BASE = 1_500.0        # Base payout at 6M+
HIGH_TIER_INCREMENT = 150.0     # Additional $ per million above 5M
HIGH_TIER_MILLION_OFFSET = 5    # Subtract this from floor_millions in the formula


# ===========================================================================
# Step B: Calculate effective views (apply 10M cap)
# ===========================================================================

def calculate_effective_views(chosen_views: int) -> int:
    """
    Apply the 10M view cap.

    Per SPEC.md Step B:
      - If chosen_views > 10,000,000: effective_views = 10,000,000
      - Otherwise: effective_views = chosen_views
      - Original chosen_views is preserved on the PayoutUnit for audit

    Args:
        chosen_views: The raw view count (max of both platforms for pairs,
                      or single platform views for unpaired)

    Returns:
        effective_views: Capped at 10M
    """
    if chosen_views > VIEW_CAP:
        logger.debug(f"Views capped: {chosen_views:,} → {VIEW_CAP:,}")
        return VIEW_CAP
    return chosen_views


# ===========================================================================
# Step A + C: Calculate payout amount from effective views
# ===========================================================================

def calculate_payout(effective_views: int) -> float:
    """
    Calculate the dollar payout for a single video based on its effective_views.

    Per SPEC.md Steps A and C:
      Step A: If effective_views < 1,000 → $0 (not qualified)
      Step C: Apply tier table to determine payout

    For the 6M–10M range, uses the formula:
      payout = $1,500 + $150 × (floor(effective_views / 1,000,000) - 5)

    Args:
        effective_views: View count after 10M cap has been applied

    Returns:
        payout_amount: Dollar amount for this video
    """
    # ------------------------------------------------------------------
    # Step A: Qualification check
    # ------------------------------------------------------------------
    if effective_views < QUALIFICATION_THRESHOLD:
        return 0.0

    # ------------------------------------------------------------------
    # Step C: Fixed tier lookup (1K – 5,999,999)
    # ------------------------------------------------------------------
    for min_views, max_views, payout in FIXED_TIERS:
        if min_views <= effective_views <= max_views:
            return payout

    # ------------------------------------------------------------------
    # Step C: Formula tier (6,000,000 – 10,000,000)
    # payout = $1,500 + $150 × (floor_millions - 5)
    # ------------------------------------------------------------------
    if effective_views >= HIGH_TIER_FLOOR:
        floor_millions = math.floor(effective_views / 1_000_000)
        payout = HIGH_TIER_BASE + HIGH_TIER_INCREMENT * (floor_millions - HIGH_TIER_MILLION_OFFSET)
        return payout

    # Should never reach here if tiers are defined correctly
    logger.warning(f"No tier matched for effective_views={effective_views:,}")
    return 0.0


# ===========================================================================
# Process all PayoutUnits: fill in effective_views + payout_amount
# ===========================================================================

def process_payouts(payout_units: list[PayoutUnit]) -> list[PayoutUnit]:
    """
    Calculate payouts for all payout units in place.

    For each PayoutUnit:
      1. effective_views = min(chosen_views, 10M)
      2. payout_amount = tier lookup on effective_views
      3. Update the PayoutUnit with both values

    Note: PayoutUnit.chosen_views is NOT modified (preserved for audit).

    Args:
        payout_units: List of PayoutUnit objects from the matcher

    Returns:
        The same list with effective_views and payout_amount populated
    """
    total_payout = 0.0
    qualified_count = 0
    capped_count = 0

    for unit in payout_units:
        # ------------------------------------------------------------------
        # Step B: Apply 10M cap
        # ------------------------------------------------------------------
        unit.effective_views = calculate_effective_views(unit.chosen_views)

        if unit.chosen_views > VIEW_CAP:
            capped_count += 1

        # ------------------------------------------------------------------
        # Steps A + C: Calculate payout
        # ------------------------------------------------------------------
        unit.payout_amount = calculate_payout(unit.effective_views)

        if unit.chosen_views >= QUALIFICATION_THRESHOLD:
            qualified_count += 1

        total_payout += unit.payout_amount

        logger.debug(
            f"  [{unit.creator_name}] "
            f"chosen={unit.chosen_views:,} → effective={unit.effective_views:,} → "
            f"${unit.payout_amount:,.2f} "
            f"(method={unit.match_method})"
        )

    logger.info(
        f"Payout processing complete: "
        f"{len(payout_units)} units, "
        f"{qualified_count} qualified, "
        f"{capped_count} capped at 10M, "
        f"total=${total_payout:,.2f}"
    )

    return payout_units


# ===========================================================================
# Step D: Build CreatorSummary — aggregate per creator
# ===========================================================================

def build_creator_summaries(
    payout_units: list[PayoutUnit],
    exception_counts: dict[str, int] | None = None,
) -> list[CreatorSummary]:
    """
    Aggregate per-video payouts into per-creator summaries.

    Groups all PayoutUnits by creator_name, then for each creator:
      - qualified_video_count = count of PayoutUnits with chosen_views >= 1,000
      - total_payout          = sum of payout_amount across ALL PayoutUnits
      - paired_video_count    = count of PayoutUnits (all are paired)
      - exception_count       = from exception_counts dict (defaults to 0)

    IMPORTANT: Do NOT accidentally aggregate different creators' payouts together.
    Each creator gets their own independent CreatorSummary.

    Args:
        payout_units:     List of PayoutUnit objects with payout_amount filled in
        exception_counts: Dict of {creator_name: exception_count}. Optional —
                          defaults to 0 for all creators if not provided.

    Returns:
        List of CreatorSummary objects, sorted by creator_name
    """
    if exception_counts is None:
        exception_counts = {}

    # ------------------------------------------------------------------
    # Group PayoutUnits by creator_name
    # ------------------------------------------------------------------
    creator_units: dict[str, list[PayoutUnit]] = {}
    for unit in payout_units:
        name = unit.creator_name
        if name not in creator_units:
            creator_units[name] = []
        creator_units[name].append(unit)

    # ------------------------------------------------------------------
    # Build a CreatorSummary for each creator
    # ------------------------------------------------------------------
    summaries: list[CreatorSummary] = []

    for creator_name in sorted(creator_units.keys()):
        units = creator_units[creator_name]

        # Count qualified payout units (chosen_views >= 1,000)
        qualified_count = sum(
            1 for u in units if u.chosen_views >= QUALIFICATION_THRESHOLD
        )

        # Sum all payout amounts
        total_payout = sum(u.payout_amount for u in units)

        # All payout units are paired (unpaired go to Exceptions, not PayoutUnits)
        paired_count = len(units)

        # Exception count from the exceptions dict
        exc_count = exception_counts.get(creator_name, 0)

        summary = CreatorSummary(
            creator_name=creator_name,
            qualified_video_count=qualified_count,
            total_payout=total_payout,
            paired_video_count=paired_count,
            exception_count=exc_count,
        )
        summaries.append(summary)

        logger.debug(
            f"  Creator '{creator_name}': "
            f"qualified={qualified_count}, "
            f"total=${total_payout:,.2f}, "
            f"paired={paired_count}, "
            f"exceptions={exc_count}"
        )

    logger.info(
        f"Built {len(summaries)} creator summaries, "
        f"total across all creators: "
        f"${sum(s.total_payout for s in summaries):,.2f}"
    )

    return summaries


# ===========================================================================
# Convenience: full payout pipeline
# ===========================================================================

def run_payout_pipeline(
    payout_units: list[PayoutUnit],
    exception_counts: dict[str, int] | None = None,
) -> tuple[list[PayoutUnit], list[CreatorSummary]]:
    """
    Full payout pipeline: process payouts + build creator summaries.

    Convenience function that runs process_payouts followed by
    build_creator_summaries.

    Args:
        payout_units:     PayoutUnit objects from the matcher
        exception_counts: Optional dict of {creator_name: count}

    Returns:
        Tuple of (processed payout_units, creator_summaries)
    """
    logger.info(f"Running payout pipeline on {len(payout_units)} units")
    processed = process_payouts(payout_units)
    summaries = build_creator_summaries(processed, exception_counts)
    return processed, summaries


# ===========================================================================
# Standalone test — run with: cd backend && python -m services.payout
# ===========================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")

    print("=" * 60)
    print("PAYOUT TIER SANITY CHECK")
    print("=" * 60)

    test_cases = [
        (500, 0.0),
        (2_500, 35.0),
        (35_000, 50.0),
        (75_000, 100.0),
        (180_000, 150.0),
        (400_000, 300.0),
        (800_000, 500.0),
        (1_500_000, 700.0),
        (2_500_000, 900.0),
        (3_500_000, 1_100.0),
        (4_500_000, 1_300.0),
        (5_500_000, 1_500.0),
        (6_000_000, 1_650.0),
        (6_700_000, 1_650.0),
        (7_000_000, 1_800.0),
        (9_200_000, 2_100.0),
        (10_000_000, 2_250.0),
        (12_000_000, 2_250.0),  # capped to 10M
        (50_000_000, 2_250.0),  # capped to 10M
    ]

    all_pass = True
    for views, expected in test_cases:
        effective = calculate_effective_views(views)
        actual = calculate_payout(effective)
        status = "PASS" if actual == expected else "FAIL"
        if status == "FAIL":
            all_pass = False
        print(f"  {status}: {views:>12,} views → effective={effective:>12,} → ${actual:,.2f} (expected ${expected:,.2f})")

    print(f"\n{'All tests passed!' if all_pass else 'SOME TESTS FAILED!'}")
