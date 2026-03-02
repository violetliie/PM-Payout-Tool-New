"""
Comprehensive end-to-end test for the Polymarket Creator Payout Tool.

Tests the FULL pipeline: match_videos() -> run_payout_pipeline() with 10 fake
creators and 30+ videos covering every important scenario.

Key behavioral rules:
  - Only paired videos (both TT + IG matched) become PayoutUnits.
  - Unpaired videos go to Exceptions only (no payout, no PayoutUnit).
  - Creators with 0 PayoutUnits do NOT appear in creator summaries.
  - match_method is "sequence" (Step 9) or "fallback" (Step 10).
  - Fallback requires exact length + phash only (no date requirements).

Creators and their scenarios:
  1. Creator Alpha   -- Perfect pairing: 3 TT + 3 IG, same lengths, same order
  2. Creator Beta    -- TikTok only: 2 TT videos, no IG -> 0 PayoutUnits, 2 exceptions
  3. Creator Gamma   -- Instagram only: 1 IG below 1K -> 0 PayoutUnits, 1 exception
  4. Creator Delta   -- Length mismatch -> fallback success: swapped lengths
  5. Creator Epsilon -- Mixed: 1 pair + 1 unpaired TT -> 1 PayoutUnit, 1 exception
  6. Creator Zeta    -- Unmapped: handles not in creator list -> exceptions only
  7. Creator Eta     -- Deduplication: 2 TT with same ad_link, keeps newer
  8. Creator Theta   -- 10M cap: 15M TT views capped to 10M effective
  9. Creator Iota    -- Fallback fails: mismatched lengths, no same-length candidate
  10. Creator Kappa  -- Zero/sub-1K views: 2 pairs, all below threshold -> $0

Pipeline flow:
  Videos (Shortimize API)
    -> match_videos(videos, tiktok_map, instagram_map)
       -> (payout_units, exceptions)
    -> run_payout_pipeline(payout_units, exception_counts)
       -> (processed_units, creator_summaries)
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import date, datetime, timezone
from models.schemas import Video, PayoutUnit, CreatorSummary, ExceptionVideo
from services.matcher import match_videos
from services.payout import run_payout_pipeline, calculate_effective_views, calculate_payout


# ## =======================================================================
# ## Helper: make_video factory
# ## =======================================================================

def make_video(
    username,
    platform,
    length,
    views,
    created_at_str,
    ad_link=None,
    ad_id=None,
    uploaded_at_date=None,
    private=False,
    removed=False,
    title=None,
):
    """
    Convenience factory for building Video objects in tests.

    Args:
        username:        Platform handle (e.g. "alice_tt")
        platform:        "tiktok" or "instagram"
        length:          Video duration in seconds
        views:           latest_views count
        created_at_str:  ISO 8601 datetime string for created_at
        ad_link:         Override ad_link (auto-generated if None)
        ad_id:           Optional ad_id for dedup testing
        uploaded_at_date: Override uploaded_at (defaults to 2026-02-20)
        private:         Whether the video is marked private
        removed:         Whether the video is marked removed
        title:           Optional video title
    """
    return Video(
        username=username,
        platform=platform,
        ad_link=ad_link or f"https://{platform}.com/@{username}/video/{hash(created_at_str) % 10000}",
        uploaded_at=uploaded_at_date or date(2026, 2, 20),
        created_at=datetime.fromisoformat(created_at_str),
        video_length=length,
        latest_views=views,
        latest_updated_at=datetime.fromisoformat(created_at_str),
        linked_account_id=None,
        ad_id=ad_id,
        title=title,
        private=private,
        removed=removed,
    )


# ## =======================================================================
# ## Shared pipeline setup — run once and store results
# ## =======================================================================

class _PipelineResults:
    """
    Singleton-like container for shared pipeline results.

    All test classes reference this to avoid re-running the pipeline.
    The setup() classmethod builds all videos, runs matching, runs payout,
    and stores every result for assertions.
    """

    _initialized = False

    # Raw inputs
    all_videos: list[Video] = []
    tiktok_map: dict[str, str] = {}
    instagram_map: dict[str, str] = {}

    # Pipeline outputs
    payout_units: list[PayoutUnit] = []
    exceptions: list[ExceptionVideo] = []
    processed_units: list[PayoutUnit] = []
    creator_summaries: list[CreatorSummary] = []

    # Derived lookup tables (populated during setup)
    summary_by_name: dict[str, CreatorSummary] = {}
    units_by_creator: dict[str, list[PayoutUnit]] = {}
    exception_counts: dict[str, int] = {}

    @classmethod
    def setup(cls):
        """Build all test data and run the full pipeline exactly once."""
        if cls._initialized:
            return
        cls._initialized = True

        videos = []

        # --------------------------------------------------------------
        # Creator Alpha: Perfect Pairing (3 TT + 3 IG, same lengths)
        # TT views: 50K, 10K, 800K    IG views: 80K, 15K, 300K
        # Lengths: 30s, 45s, 60s — same order, same lengths
        # Expected: 3 high-confidence exact-match pairs
        # Payouts: max(50K,80K)=80K->$100, max(10K,15K)=15K->$50,
        #          max(800K,300K)=800K->$500 = $650 total
        # --------------------------------------------------------------
        videos.append(make_video("alice_tt", "tiktok", 30, 50_000,
                                 "2026-02-20T10:00:00+00:00"))
        videos.append(make_video("alice_tt", "tiktok", 45, 10_000,
                                 "2026-02-20T11:00:00+00:00"))
        videos.append(make_video("alice_tt", "tiktok", 60, 800_000,
                                 "2026-02-20T12:00:00+00:00"))
        videos.append(make_video("alice_ig", "instagram", 30, 80_000,
                                 "2026-02-20T10:30:00+00:00"))
        videos.append(make_video("alice_ig", "instagram", 45, 15_000,
                                 "2026-02-20T11:30:00+00:00"))
        videos.append(make_video("alice_ig", "instagram", 60, 300_000,
                                 "2026-02-20T12:30:00+00:00"))

        # --------------------------------------------------------------
        # Creator Beta: TikTok Only (2 TT, no IG mapping)
        # Views: 5K, 2.5K -> both unpaired -> exceptions only
        # Expected: 0 PayoutUnits, 2 exceptions, no CreatorSummary
        # --------------------------------------------------------------
        videos.append(make_video("beta_tt", "tiktok", 30, 5_000,
                                 "2026-02-20T09:00:00+00:00"))
        videos.append(make_video("beta_tt", "tiktok", 45, 2_500,
                                 "2026-02-20T10:00:00+00:00"))

        # --------------------------------------------------------------
        # Creator Gamma: Instagram Only (1 IG, no TT mapping)
        # Views: 500 (below 1K) -> exception only
        # Expected: 0 PayoutUnits, 1 exception, no CreatorSummary
        # --------------------------------------------------------------
        videos.append(make_video("gamma_ig", "instagram", 30, 500,
                                 "2026-02-20T08:00:00+00:00"))

        # --------------------------------------------------------------
        # Creator Delta: Length Mismatch -> Fallback Success
        # TT lengths: 30s, 45s   IG lengths: 45s, 30s (swapped!)
        # Same uploaded_at, created_at within 1 hour
        # Sequence match fails (30!=45, 45!=30), fallback finds:
        #   TT[0] 30s -> IG[1] 30s, TT[1] 45s -> IG[0] 45s
        # Views: TT(100K, 250K), IG(50K, 400K)
        # Fallback pairs: TT[0] 30s <-> IG[1] 30s, TT[1] 45s <-> IG[0] 45s
        # Payouts: max(100K,400K)=400K->$300, max(250K,50K)=250K->$300
        # Total: $600
        # --------------------------------------------------------------
        videos.append(make_video("delta_tt", "tiktok", 30, 100_000,
                                 "2026-02-20T10:00:00+00:00"))
        videos.append(make_video("delta_tt", "tiktok", 45, 250_000,
                                 "2026-02-20T11:00:00+00:00"))
        videos.append(make_video("delta_ig", "instagram", 45, 50_000,
                                 "2026-02-20T10:30:00+00:00"))
        videos.append(make_video("delta_ig", "instagram", 30, 400_000,
                                 "2026-02-20T11:30:00+00:00"))

        # --------------------------------------------------------------
        # Creator Epsilon: Mixed (1 pair + 1 unpaired TT)
        # 2 TT (30s, 45s) + 1 IG (30s)
        # TT#1 30s <-> IG#1 30s (sequence match), TT#2 45s -> exception
        # Views: TT(1.5M, 3.5M), IG(2M)
        # Pair: max(1.5M, 2M)=2M->$900, TT#2 45s -> exception (no payout)
        # Total: $900
        # --------------------------------------------------------------
        videos.append(make_video("epsilon_tt", "tiktok", 30, 1_500_000,
                                 "2026-02-20T10:00:00+00:00"))
        videos.append(make_video("epsilon_tt", "tiktok", 45, 3_500_000,
                                 "2026-02-20T11:00:00+00:00"))
        videos.append(make_video("epsilon_ig", "instagram", 30, 2_000_000,
                                 "2026-02-20T10:30:00+00:00"))

        # --------------------------------------------------------------
        # Creator Zeta: Unmapped Videos
        # Handles NOT in tiktok_map/instagram_map -> exception "Not in creator status list"
        # No PayoutUnits created
        # --------------------------------------------------------------
        videos.append(make_video("zeta_tt", "tiktok", 30, 10_000,
                                 "2026-02-20T10:00:00+00:00"))
        videos.append(make_video("zeta_ig", "instagram", 30, 20_000,
                                 "2026-02-20T10:30:00+00:00"))

        # --------------------------------------------------------------
        # Creator Eta: Deduplication
        # 2 TT videos with SAME ad_link but different latest_updated_at.
        # Older (Feb 20 10:00, 4M views) should be dropped.
        # Newer (Feb 20 12:00, 6M views) should be kept.
        # 1 IG video (30s, 4M views).
        # After dedup: 1 TT (30s, 6M) + 1 IG (30s, 4M) -> 1 pair
        # Chosen = max(6M, 4M) = 6M -> $1650
        # --------------------------------------------------------------
        videos.append(make_video("eta_tt", "tiktok", 30, 4_000_000,
                                 "2026-02-20T10:00:00+00:00",
                                 ad_link="https://tiktok.com/@eta_tt/video/DUPLICATE"))
        videos.append(make_video("eta_tt", "tiktok", 30, 6_000_000,
                                 "2026-02-20T12:00:00+00:00",
                                 ad_link="https://tiktok.com/@eta_tt/video/DUPLICATE"))
        videos.append(make_video("eta_ig", "instagram", 30, 4_000_000,
                                 "2026-02-20T10:30:00+00:00"))

        # --------------------------------------------------------------
        # Creator Theta: 10M Cap
        # 1 TT (30s, 15M views) + 1 IG (30s, 8M views)
        # Chosen = max(15M, 8M) = 15M, effective = 10M (capped)
        # Payout at 10M: floor(10M/1M)=10, 1500 + 150*(10-5) = $2250
        # --------------------------------------------------------------
        videos.append(make_video("theta_tt", "tiktok", 30, 15_000_000,
                                 "2026-02-20T10:00:00+00:00"))
        videos.append(make_video("theta_ig", "instagram", 30, 8_000_000,
                                 "2026-02-20T10:30:00+00:00"))

        # --------------------------------------------------------------
        # Creator Iota: Fallback Fails (mismatched lengths, no same-length candidate)
        # TT: 30s, IG: 45s — different lengths on each platform
        # Sequence match fails (30!=45). Fallback also fails because
        # there is no same-length candidate (30!=45). Both -> exceptions.
        # Views: TT(500K), IG(100K)
        # Expected: 0 PayoutUnits, 2 exceptions, no CreatorSummary
        # --------------------------------------------------------------
        videos.append(make_video("iota_tt", "tiktok", 30, 500_000,
                                 "2026-02-20T10:00:00+00:00",
                                 uploaded_at_date=date(2026, 2, 20)))
        videos.append(make_video("iota_ig", "instagram", 45, 100_000,
                                 "2026-02-21T10:30:00+00:00",
                                 uploaded_at_date=date(2026, 2, 21)))

        # --------------------------------------------------------------
        # Creator Kappa: Zero/Sub-Threshold Views
        # 2 TT (30s, 45s) + 2 IG (30s, 45s)
        # Views: TT(0, 999), IG(500, 100)
        # Both pairs exact match: max(0,500)=500->$0, max(999,100)=999->$0
        # Total: $0, qualified=0, paired=2, unpaired=0
        # --------------------------------------------------------------
        videos.append(make_video("kappa_tt", "tiktok", 30, 0,
                                 "2026-02-20T10:00:00+00:00"))
        videos.append(make_video("kappa_tt", "tiktok", 45, 999,
                                 "2026-02-20T11:00:00+00:00"))
        videos.append(make_video("kappa_ig", "instagram", 30, 500,
                                 "2026-02-20T10:30:00+00:00"))
        videos.append(make_video("kappa_ig", "instagram", 45, 100,
                                 "2026-02-20T11:30:00+00:00"))

        cls.all_videos = videos

        # --------------------------------------------------------------
        # Creator mappings
        # Note: Zeta deliberately NOT included in either map.
        # Beta has no IG mapping. Gamma has no TT mapping.
        # --------------------------------------------------------------
        cls.tiktok_map = {
            "alice_tt": "Creator Alpha",
            "beta_tt": "Creator Beta",
            # Gamma has no TikTok handle
            "delta_tt": "Creator Delta",
            "epsilon_tt": "Creator Epsilon",
            # Zeta deliberately omitted
            "eta_tt": "Creator Eta",
            "theta_tt": "Creator Theta",
            "iota_tt": "Creator Iota",
            "kappa_tt": "Creator Kappa",
        }

        cls.instagram_map = {
            "alice_ig": "Creator Alpha",
            # Beta has no Instagram handle
            "gamma_ig": "Creator Gamma",
            "delta_ig": "Creator Delta",
            "epsilon_ig": "Creator Epsilon",
            # Zeta deliberately omitted
            "eta_ig": "Creator Eta",
            "theta_ig": "Creator Theta",
            "iota_ig": "Creator Iota",
            "kappa_ig": "Creator Kappa",
        }

        # --------------------------------------------------------------
        # Run Stage 1: Matching
        # --------------------------------------------------------------
        cls.payout_units, cls.exceptions = match_videos(
            cls.all_videos, cls.tiktok_map, cls.instagram_map
        )

        # Build exception_counts dict by creator_name for the payout pipeline.
        # For unmapped exceptions (Zeta), we use the username as a proxy since
        # they have no creator_name. For unpaired exceptions, the creator_name
        # can be inferred from the payout units.
        cls.exception_counts = {}
        for exc in cls.exceptions:
            # Unpaired exceptions: find the creator name via the payout units
            # that share the same username.
            creator_name = None
            for pu in cls.payout_units:
                if pu.tiktok_video and pu.tiktok_video.username == exc.username:
                    creator_name = pu.creator_name
                    break
                if pu.instagram_video and pu.instagram_video.username == exc.username:
                    creator_name = pu.creator_name
                    break
            if creator_name:
                cls.exception_counts[creator_name] = cls.exception_counts.get(creator_name, 0) + 1

        # --------------------------------------------------------------
        # Run Stage 2: Payout pipeline
        # --------------------------------------------------------------
        cls.processed_units, cls.creator_summaries = run_payout_pipeline(
            cls.payout_units, cls.exception_counts
        )

        # Build lookup tables for easy per-creator assertions
        cls.summary_by_name = {s.creator_name: s for s in cls.creator_summaries}
        cls.units_by_creator = {}
        for pu in cls.processed_units:
            if pu.creator_name not in cls.units_by_creator:
                cls.units_by_creator[pu.creator_name] = []
            cls.units_by_creator[pu.creator_name].append(pu)


# ## =======================================================================
# ## Base test class — ensures pipeline is initialized before any test
# ## =======================================================================

class PipelineTestBase:
    """Base class that initializes the shared pipeline results before tests run."""

    def setup_method(self):
        """Ensure pipeline results are available for every test method."""
        _PipelineResults.setup()
        self.results = _PipelineResults


# ## =======================================================================
# ## Test: Creator Counts and Names
# ## =======================================================================

class TestFullPipelineCreatorCounts(PipelineTestBase):
    """Verify the correct set of creators appears in summaries."""

    def test_total_creator_count(self):
        """Only creators with PayoutUnits get summaries (6 total).

        Missing: Zeta (unmapped), Beta (TT only), Gamma (IG only), Iota (no match).
        """
        assert len(self.results.creator_summaries) == 6

    def test_all_creator_names_present(self):
        """Only creators with paired PayoutUnits appear in summaries."""
        expected_names = {
            "Creator Alpha", "Creator Delta", "Creator Epsilon",
            "Creator Eta", "Creator Theta", "Creator Kappa",
        }
        actual_names = set(self.results.summary_by_name.keys())
        assert actual_names == expected_names

    def test_zeta_not_in_summaries(self):
        """Unmapped Creator Zeta should NOT appear in summaries."""
        assert "Creator Zeta" not in self.results.summary_by_name

    def test_beta_not_in_summaries(self):
        """Single-platform Creator Beta (TT only) has 0 PayoutUnits, no summary."""
        assert "Creator Beta" not in self.results.summary_by_name

    def test_gamma_not_in_summaries(self):
        """Single-platform Creator Gamma (IG only) has 0 PayoutUnits, no summary."""
        assert "Creator Gamma" not in self.results.summary_by_name

    def test_iota_not_in_summaries(self):
        """Creator Iota has mismatched lengths, 0 PayoutUnits, no summary."""
        assert "Creator Iota" not in self.results.summary_by_name


# ## =======================================================================
# ## Test: Creator Alpha (Perfect Pairing)
# ## =======================================================================

class TestFullPipelineAlpha(PipelineTestBase):
    """
    Creator Alpha: 3 TT + 3 IG, all same lengths in same sequence.
    All 3 pairs should be sequence matches.
    Payouts: $100 + $50 + $500 = $650.
    """

    def test_alpha_total_payout(self):
        """Alpha total payout should be $650."""
        summary = self.results.summary_by_name["Creator Alpha"]
        assert summary.total_payout == 650.0

    def test_alpha_qualified_count(self):
        """All 3 pairs qualify (all chosen_views >= 1K)."""
        summary = self.results.summary_by_name["Creator Alpha"]
        assert summary.qualified_video_count == 3

    def test_alpha_paired_count(self):
        """All 3 units should be paired."""
        summary = self.results.summary_by_name["Creator Alpha"]
        assert summary.paired_video_count == 3

    def test_alpha_payout_units_match_method(self):
        """All Alpha pairs should have 'sequence' match_method and appropriate match_note."""
        units = self.results.units_by_creator["Creator Alpha"]
        for unit in units:
            assert unit.match_method == "sequence"
            assert "sequence match" in unit.match_note
            assert "phash distance: 0" in unit.match_note


# ## =======================================================================
# ## Test: Creator Beta (TikTok Only)
# ## =======================================================================

class TestFullPipelineBeta(PipelineTestBase):
    """
    Creator Beta: 2 TikTok videos, no Instagram mapping.
    Both videos are unpaired -> exceptions only, no PayoutUnits, no CreatorSummary.
    """

    def test_beta_no_payout_units(self):
        """Beta should have no payout units (TT only, no IG to match)."""
        assert "Creator Beta" not in self.results.units_by_creator

    def test_beta_exceptions(self):
        """Beta should have 2 exceptions for unpaired TikTok videos."""
        beta_exceptions = [
            e for e in self.results.exceptions
            if e.username == "beta_tt"
        ]
        assert len(beta_exceptions) == 2

    def test_beta_exception_reason(self):
        """Beta exceptions should have the unpaired reason."""
        beta_exceptions = [
            e for e in self.results.exceptions
            if e.username == "beta_tt"
        ]
        for exc in beta_exceptions:
            assert exc.reason == "Only posted on one platform"


# ## =======================================================================
# ## Test: Creator Gamma (Instagram Only, Below Threshold)
# ## =======================================================================

class TestFullPipelineGamma(PipelineTestBase):
    """
    Creator Gamma: 1 Instagram video with 500 views (below 1K threshold).
    Single-platform only -> exception, no PayoutUnit, no CreatorSummary.
    """

    def test_gamma_no_payout_units(self):
        """Gamma should have no payout units (IG only, no TT to match)."""
        assert "Creator Gamma" not in self.results.units_by_creator

    def test_gamma_exception(self):
        """Gamma should have 1 exception for unpaired Instagram video."""
        gamma_exceptions = [
            e for e in self.results.exceptions
            if e.username == "gamma_ig"
        ]
        assert len(gamma_exceptions) == 1

    def test_gamma_exception_reason(self):
        """Gamma exception should have the unpaired reason."""
        gamma_exceptions = [
            e for e in self.results.exceptions
            if e.username == "gamma_ig"
        ]
        assert gamma_exceptions[0].reason == "Only posted on one platform"


# ## =======================================================================
# ## Test: Creator Delta (Fallback Matching)
# ## =======================================================================

class TestFullPipelineDelta(PipelineTestBase):
    """
    Creator Delta: Lengths swapped in sequence -> primary fails, fallback succeeds.
    TT(30s,45s) + IG(45s,30s). Fallback matches by exact length + phash.
    Fallback pairs: TT[0] 30s <-> IG[1] 30s, TT[1] 45s <-> IG[0] 45s.
    Payouts: max(100K,400K)=400K->$300, max(250K,50K)=250K->$300 = $600.
    """

    def test_delta_total_payout(self):
        """Delta total payout should be $600."""
        summary = self.results.summary_by_name["Creator Delta"]
        assert summary.total_payout == 600.0

    def test_delta_both_pairs_fallback_method(self):
        """Both Delta pairs should have 'fallback' match_method."""
        units = self.results.units_by_creator["Creator Delta"]
        assert len(units) == 2
        for unit in units:
            assert unit.match_method == "fallback"

    def test_delta_fallback_notes(self):
        """Both Delta pairs should have fallback match notes."""
        units = self.results.units_by_creator["Creator Delta"]
        for unit in units:
            assert "fallback match" in unit.match_note


# ## =======================================================================
# ## Test: Creator Epsilon (Mixed: 1 Pair + 1 Unpaired)
# ## =======================================================================

class TestFullPipelineEpsilon(PipelineTestBase):
    """
    Creator Epsilon: 2 TT (30s, 45s) + 1 IG (30s).
    TT#1 30s pairs with IG#1 30s (sequence match). TT#2 45s -> exception.
    Pair: max(1.5M, 2M)=2M->$900. Total: $900 (unpaired TT gets no payout).
    """

    def test_epsilon_total_payout(self):
        """Epsilon total payout should be $900 (only the paired unit)."""
        summary = self.results.summary_by_name["Creator Epsilon"]
        assert summary.total_payout == 900.0

    def test_epsilon_paired_count(self):
        """1 pair only (unpaired TT goes to exceptions, not PayoutUnits)."""
        summary = self.results.summary_by_name["Creator Epsilon"]
        assert summary.paired_video_count == 1

    def test_epsilon_qualified_count(self):
        """1 qualified payout unit (the paired one with 2M views)."""
        summary = self.results.summary_by_name["Creator Epsilon"]
        assert summary.qualified_video_count == 1

    def test_epsilon_exception_for_unpaired_tt(self):
        """The unpaired TT#2 (45s) should be in exceptions."""
        epsilon_exceptions = [
            e for e in self.results.exceptions
            if e.username == "epsilon_tt"
        ]
        assert len(epsilon_exceptions) == 1
        assert epsilon_exceptions[0].reason == "Only posted on one platform"


# ## =======================================================================
# ## Test: Creator Zeta (Unmapped)
# ## =======================================================================

class TestFullPipelineZeta(PipelineTestBase):
    """
    Creator Zeta: Handles not in mapping dicts.
    Both videos should appear in exceptions with 'not in creator list'.
    No PayoutUnits created for Zeta.
    """

    def test_zeta_appears_in_exceptions(self):
        """Zeta's videos should be in the exception list."""
        zeta_exceptions = [
            e for e in self.results.exceptions
            if e.username in ("zeta_tt", "zeta_ig")
        ]
        assert len(zeta_exceptions) == 2

    def test_zeta_exception_reason(self):
        """All Zeta exceptions should have reason 'not in creator list'."""
        zeta_exceptions = [
            e for e in self.results.exceptions
            if e.username in ("zeta_tt", "zeta_ig")
        ]
        for exc in zeta_exceptions:
            assert exc.reason == "Not in creator status list"

    def test_zeta_no_payout_units(self):
        """Zeta should have no payout units."""
        assert "Creator Zeta" not in self.results.units_by_creator


# ## =======================================================================
# ## Test: Creator Eta (Deduplication)
# ## =======================================================================

class TestFullPipelineEta(PipelineTestBase):
    """
    Creator Eta: 2 TT with same ad_link (dedup keeps newer with 6M views).
    After dedup: 1 TT (30s, 6M) + 1 IG (30s, 4M) -> 1 pair.
    Chosen = max(6M, 4M) = 6M -> $1650.
    """

    def test_eta_only_one_pair_after_dedup(self):
        """After dedup, Eta should have exactly 1 paired unit."""
        summary = self.results.summary_by_name["Creator Eta"]
        assert summary.paired_video_count == 1

    def test_eta_payout(self):
        """Eta payout should be $1650 (6M views -> floor(6)=6, 1500+150*(6-5)=1650)."""
        summary = self.results.summary_by_name["Creator Eta"]
        assert summary.total_payout == 1650.0

    def test_eta_chosen_views(self):
        """Eta pair chosen_views should be 6M (the deduped TT video's views)."""
        units = self.results.units_by_creator["Creator Eta"]
        assert len(units) == 1
        assert units[0].chosen_views == 6_000_000


# ## =======================================================================
# ## Test: Creator Theta (10M View Cap)
# ## =======================================================================

class TestFullPipelineTheta(PipelineTestBase):
    """
    Creator Theta: TT 15M views + IG 8M views -> chosen=15M, effective=10M.
    Payout at 10M = $2250.
    """

    def test_theta_effective_views_capped(self):
        """Theta effective_views should be capped at 10M."""
        units = self.results.units_by_creator["Creator Theta"]
        assert len(units) == 1
        assert units[0].effective_views == 10_000_000

    def test_theta_payout_2250(self):
        """Theta payout should be $2250 (10M cap tier)."""
        summary = self.results.summary_by_name["Creator Theta"]
        assert summary.total_payout == 2250.0

    def test_theta_chosen_views_preserved(self):
        """Original chosen_views (15M) should be preserved on the PayoutUnit."""
        units = self.results.units_by_creator["Creator Theta"]
        assert units[0].chosen_views == 15_000_000

    def test_theta_is_paired(self):
        """Theta should be a paired unit."""
        summary = self.results.summary_by_name["Creator Theta"]
        assert summary.paired_video_count == 1


# ## =======================================================================
# ## Test: Creator Iota (Fallback Fails)
# ## =======================================================================

class TestFullPipelineIota(PipelineTestBase):
    """
    Creator Iota: TT 30s + IG 45s — mismatched lengths, no same-length candidate.
    Sequence match fails (30!=45). Fallback also fails (no same-length candidate).
    Both -> exceptions. 0 PayoutUnits, no CreatorSummary.
    """

    def test_iota_no_payout_units(self):
        """Iota should have no payout units (lengths don't match)."""
        assert "Creator Iota" not in self.results.units_by_creator

    def test_iota_exceptions(self):
        """Iota should have 2 exceptions for unpaired videos."""
        iota_exceptions = [
            e for e in self.results.exceptions
            if e.username in ("iota_tt", "iota_ig")
        ]
        assert len(iota_exceptions) == 2

    def test_iota_exception_reason(self):
        """Iota exceptions should have the unpaired reason."""
        iota_exceptions = [
            e for e in self.results.exceptions
            if e.username in ("iota_tt", "iota_ig")
        ]
        for exc in iota_exceptions:
            assert exc.reason == "Only posted on one platform"


# ## =======================================================================
# ## Test: Creator Kappa (Zero/Sub-Threshold Views)
# ## =======================================================================

class TestFullPipelineKappa(PipelineTestBase):
    """
    Creator Kappa: 2 TT + 2 IG, all views below 1K.
    Views: TT(0, 999), IG(500, 100).
    Both pairs match (exact lengths), but payouts are all $0.
    """

    def test_kappa_zero_total_payout(self):
        """Kappa total payout should be $0 (all views below 1K)."""
        summary = self.results.summary_by_name["Creator Kappa"]
        assert summary.total_payout == 0.0

    def test_kappa_zero_qualified(self):
        """No qualified videos for Kappa (all below 1K)."""
        summary = self.results.summary_by_name["Creator Kappa"]
        assert summary.qualified_video_count == 0

    def test_kappa_still_paired(self):
        """Both units should still be paired despite zero payout."""
        summary = self.results.summary_by_name["Creator Kappa"]
        assert summary.paired_video_count == 2


# ## =======================================================================
# ## Test: Aggregate Totals Across All Creators
# ## =======================================================================

class TestFullPipelineAggregates(PipelineTestBase):
    """
    Cross-creator aggregate assertions.

    Expected totals (only paired units get PayoutUnits):
      Alpha:   $650   (3 pairs)
      Delta:   $600   (2 pairs)
      Epsilon: $900   (1 pair)
      Eta:     $1650  (1 pair)
      Theta:   $2250  (1 pair)
      Kappa:   $0     (2 pairs)
      -------------------------------------------------
      Total:   $6050  (10 pairs)

    Creators with NO PayoutUnits (not in summaries):
      Beta, Gamma, Iota (single-platform or no match)
      Zeta (unmapped)

    Exceptions:
      Zeta unmapped:          2
      Beta unpaired:          2
      Gamma unpaired:         1
      Epsilon unpaired:       1
      Iota unpaired:          2
      -------------------------------------------------
      Total exceptions:       8
    """

    def test_total_payout_across_all_creators(self):
        """Sum of all creator payouts should be $6050."""
        total = sum(s.total_payout for s in self.results.creator_summaries)
        assert total == 6050.0

    def test_total_paired_count(self):
        """Total paired count across all creators should be 10."""
        total_paired = sum(s.paired_video_count for s in self.results.creator_summaries)
        assert total_paired == 10

    def test_total_exception_count(self):
        """
        Total exceptions: 2 (Zeta unmapped) + 2 (Beta) + 1 (Gamma)
        + 1 (Epsilon) + 2 (Iota) = 8.
        """
        assert len(self.results.exceptions) == 8


# ## =======================================================================
# ## Test: Pair Detail Invariants
# ## =======================================================================

class TestFullPipelinePairDetails(PipelineTestBase):
    """
    Structural invariants that must hold for ALL payout units regardless
    of creator or scenario. All PayoutUnits are paired (unpaired go to exceptions).
    """

    def test_all_units_have_both_videos(self):
        """Every PayoutUnit must have both tiktok_video and instagram_video."""
        for unit in self.results.processed_units:
            assert unit.tiktok_video is not None, (
                f"Unit for {unit.creator_name} missing tiktok_video"
            )
            assert unit.instagram_video is not None, (
                f"Unit for {unit.creator_name} missing instagram_video"
            )

    def test_chosen_views_is_max_of_both_platforms(self):
        """For every unit, chosen_views == max(tt_views, ig_views)."""
        for unit in self.results.processed_units:
            tt_views = unit.tiktok_video.latest_views or 0
            ig_views = unit.instagram_video.latest_views or 0
            expected_chosen = max(tt_views, ig_views)
            assert unit.chosen_views == expected_chosen, (
                f"Unit for {unit.creator_name}: "
                f"chosen_views={unit.chosen_views} != "
                f"max({tt_views}, {ig_views})={expected_chosen}"
            )

    def test_all_payout_amounts_match_tier(self):
        """For every unit, payout_amount matches calculate_payout(calculate_effective_views(chosen_views))."""
        for unit in self.results.processed_units:
            effective = calculate_effective_views(unit.chosen_views)
            expected_payout = calculate_payout(effective)
            assert unit.payout_amount == expected_payout, (
                f"Unit for {unit.creator_name}: "
                f"payout_amount={unit.payout_amount} != "
                f"calculate_payout({effective})={expected_payout}"
            )

    def test_no_negative_payouts(self):
        """All payout_amount values must be >= 0."""
        for unit in self.results.processed_units:
            assert unit.payout_amount >= 0, (
                f"Negative payout for {unit.creator_name}: {unit.payout_amount}"
            )

    def test_no_negative_views(self):
        """All chosen_views and effective_views must be >= 0."""
        for unit in self.results.processed_units:
            assert unit.chosen_views >= 0, (
                f"Negative chosen_views for {unit.creator_name}: {unit.chosen_views}"
            )
            assert unit.effective_views >= 0, (
                f"Negative effective_views for {unit.creator_name}: {unit.effective_views}"
            )

    def test_effective_views_never_exceed_cap(self):
        """No unit should have effective_views > 10M."""
        for unit in self.results.processed_units:
            assert unit.effective_views <= 10_000_000, (
                f"Effective views exceed cap for {unit.creator_name}: "
                f"{unit.effective_views}"
            )

    def test_all_units_have_valid_match_method(self):
        """Every PayoutUnit must have match_method of 'sequence' or 'fallback'."""
        for unit in self.results.processed_units:
            assert unit.match_method in ("sequence", "fallback"), (
                f"Unit for {unit.creator_name}: "
                f"invalid match_method={unit.match_method}"
            )

    def test_all_units_have_match_note(self):
        """Every PayoutUnit must have a non-empty match_note."""
        for unit in self.results.processed_units:
            assert unit.match_note is not None and len(unit.match_note) > 0, (
                f"Unit for {unit.creator_name}: missing match_note"
            )
