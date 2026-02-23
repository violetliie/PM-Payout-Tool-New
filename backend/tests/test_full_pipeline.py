"""
Comprehensive end-to-end test for the Polymarket Creator Payout Tool.

Tests the FULL pipeline: match_videos() -> run_payout_pipeline() with 10 fake
creators and 30+ videos covering every important scenario.

Creators and their scenarios:
  1. Creator Alpha   -- Perfect pairing: 3 TT + 3 IG, same lengths, same order
  2. Creator Beta    -- TikTok only: 2 TT videos, no IG handle mapped
  3. Creator Gamma   -- Instagram only: 1 IG video below 1K views
  4. Creator Delta   -- Length mismatch -> fallback success: swapped lengths
  5. Creator Epsilon -- Mixed: 1 pair + 1 unpaired TT
  6. Creator Zeta    -- Unmapped: handles not in creator list -> exceptions only
  7. Creator Eta     -- Deduplication: 2 TT with same ad_link, keeps newer
  8. Creator Theta   -- 10M cap: 15M TT views capped to 10M effective
  9. Creator Iota    -- Fallback fails: different uploaded_at dates + mismatched lengths
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
        # Views: 5K, 2.5K -> both unpaired -> $35 + $35 = $70
        # Expected: 2 unpaired TT, 2 exceptions
        # --------------------------------------------------------------
        videos.append(make_video("beta_tt", "tiktok", 30, 5_000,
                                 "2026-02-20T09:00:00+00:00"))
        videos.append(make_video("beta_tt", "tiktok", 45, 2_500,
                                 "2026-02-20T10:00:00+00:00"))

        # --------------------------------------------------------------
        # Creator Gamma: Instagram Only (1 IG, no TT mapping)
        # Views: 500 (below 1K) -> $0 payout
        # Expected: 1 unpaired IG, 1 exception
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
        # TT#1 30s <-> IG#1 30s (exact match), TT#2 45s unpaired
        # Views: TT(1.5M, 3.5M), IG(2M)
        # Pair: max(1.5M, 2M)=2M->$900, Unpaired TT: 3.5M->$1100
        # Total: $2000
        # --------------------------------------------------------------
        videos.append(make_video("epsilon_tt", "tiktok", 30, 1_500_000,
                                 "2026-02-20T10:00:00+00:00"))
        videos.append(make_video("epsilon_tt", "tiktok", 45, 3_500_000,
                                 "2026-02-20T11:00:00+00:00"))
        videos.append(make_video("epsilon_ig", "instagram", 30, 2_000_000,
                                 "2026-02-20T10:30:00+00:00"))

        # --------------------------------------------------------------
        # Creator Zeta: Unmapped Videos
        # Handles NOT in tiktok_map/instagram_map -> exception "not in creator list"
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
        # Creator Iota: Fallback Fails (different uploaded_at + different lengths)
        # TT: 30s, uploaded Feb 20, created 10:00
        # IG: 45s, uploaded Feb 21, created 10:30
        # Sequence match fails (30!=45). Fallback fails because uploaded_at
        # dates differ (Feb 20 vs Feb 21). Both become unpaired.
        # Views: TT(500K), IG(100K)
        # Payouts: $500 + $150 = $650
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
        """Exactly 9 creators should have summaries (Zeta is unmapped)."""
        assert len(self.results.creator_summaries) == 9

    def test_all_creator_names_present(self):
        """Every mapped creator should appear in the summaries."""
        expected_names = {
            "Creator Alpha", "Creator Beta", "Creator Gamma",
            "Creator Delta", "Creator Epsilon", "Creator Eta",
            "Creator Theta", "Creator Iota", "Creator Kappa",
        }
        actual_names = set(self.results.summary_by_name.keys())
        assert actual_names == expected_names

    def test_zeta_not_in_summaries(self):
        """Unmapped Creator Zeta should NOT appear in summaries."""
        assert "Creator Zeta" not in self.results.summary_by_name


# ## =======================================================================
# ## Test: Creator Alpha (Perfect Pairing)
# ## =======================================================================

class TestFullPipelineAlpha(PipelineTestBase):
    """
    Creator Alpha: 3 TT + 3 IG, all same lengths in same sequence.
    All 3 pairs should be high-confidence exact matches.
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

    def test_alpha_unpaired_count(self):
        """No unpaired videos for Alpha."""
        summary = self.results.summary_by_name["Creator Alpha"]
        assert summary.unpaired_video_count == 0

    def test_alpha_payout_units_confidence(self):
        """All Alpha pairs should have 'high' confidence and 'exact match' note."""
        units = self.results.units_by_creator["Creator Alpha"]
        for unit in units:
            assert unit.paired is True
            assert unit.match_confidence == "high"
            assert unit.pair_note == "exact match"


# ## =======================================================================
# ## Test: Creator Beta (TikTok Only)
# ## =======================================================================

class TestFullPipelineBeta(PipelineTestBase):
    """
    Creator Beta: 2 TikTok videos, no Instagram mapping.
    Both videos are unpaired. Views: 5K, 2.5K -> $35 + $35 = $70.
    """

    def test_beta_total_payout(self):
        """Beta total payout should be $70 (two $35 tiers)."""
        summary = self.results.summary_by_name["Creator Beta"]
        assert summary.total_payout == 70.0

    def test_beta_qualified_count(self):
        """Both videos qualify (5K >= 1K and 2.5K >= 1K)."""
        summary = self.results.summary_by_name["Creator Beta"]
        assert summary.qualified_video_count == 2

    def test_beta_paired_unpaired(self):
        """0 pairs, 2 unpaired."""
        summary = self.results.summary_by_name["Creator Beta"]
        assert summary.paired_video_count == 0
        assert summary.unpaired_video_count == 2

    def test_beta_exceptions(self):
        """Beta should have 2 exceptions (unpaired single platform only)."""
        summary = self.results.summary_by_name["Creator Beta"]
        assert summary.exception_count == 2


# ## =======================================================================
# ## Test: Creator Gamma (Instagram Only, Below Threshold)
# ## =======================================================================

class TestFullPipelineGamma(PipelineTestBase):
    """
    Creator Gamma: 1 Instagram video with 500 views (below 1K threshold).
    Payout = $0.
    """

    def test_gamma_zero_payout(self):
        """Gamma payout should be $0 (views below 1K)."""
        summary = self.results.summary_by_name["Creator Gamma"]
        assert summary.total_payout == 0.0

    def test_gamma_qualified_count_zero(self):
        """No qualified videos for Gamma (500 < 1K)."""
        summary = self.results.summary_by_name["Creator Gamma"]
        assert summary.qualified_video_count == 0

    def test_gamma_unpaired(self):
        """1 unpaired IG, 0 paired."""
        summary = self.results.summary_by_name["Creator Gamma"]
        assert summary.paired_video_count == 0
        assert summary.unpaired_video_count == 1


# ## =======================================================================
# ## Test: Creator Delta (Fallback Matching)
# ## =======================================================================

class TestFullPipelineDelta(PipelineTestBase):
    """
    Creator Delta: Lengths swapped in sequence -> primary fails, fallback succeeds.
    TT(30s,45s) + IG(45s,30s). Fallback matches by exact length + same upload date.
    Fallback pairs: TT[0] 30s <-> IG[1] 30s, TT[1] 45s <-> IG[0] 45s.
    Payouts: max(100K,400K)=400K->$300, max(250K,50K)=250K->$300 = $600.
    """

    def test_delta_total_payout(self):
        """Delta total payout should be $600."""
        summary = self.results.summary_by_name["Creator Delta"]
        assert summary.total_payout == 600.0

    def test_delta_both_pairs_medium_confidence(self):
        """Both Delta pairs should have 'medium' confidence (fallback match)."""
        units = self.results.units_by_creator["Creator Delta"]
        assert len(units) == 2
        for unit in units:
            assert unit.paired is True
            assert unit.match_confidence == "medium"

    def test_delta_fallback_notes(self):
        """Both Delta pairs should have fallback match notes."""
        units = self.results.units_by_creator["Creator Delta"]
        for unit in units:
            assert "fallback match" in unit.pair_note


# ## =======================================================================
# ## Test: Creator Epsilon (Mixed: 1 Pair + 1 Unpaired)
# ## =======================================================================

class TestFullPipelineEpsilon(PipelineTestBase):
    """
    Creator Epsilon: 2 TT (30s, 45s) + 1 IG (30s).
    TT#1 30s pairs with IG#1 30s (exact match). TT#2 45s is unpaired.
    Pair: max(1.5M, 2M)=2M->$900. Unpaired: 3.5M->$1100. Total: $2000.
    """

    def test_epsilon_total_payout(self):
        """Epsilon total payout should be $2000."""
        summary = self.results.summary_by_name["Creator Epsilon"]
        assert summary.total_payout == 2000.0

    def test_epsilon_paired_unpaired_counts(self):
        """1 pair, 1 unpaired."""
        summary = self.results.summary_by_name["Creator Epsilon"]
        assert summary.paired_video_count == 1
        assert summary.unpaired_video_count == 1

    def test_epsilon_qualified_count(self):
        """Both payout units qualify (both >= 1K views)."""
        summary = self.results.summary_by_name["Creator Epsilon"]
        assert summary.qualified_video_count == 2


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
            assert exc.reason == "not in creator list"

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
        assert summary.unpaired_video_count == 0

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
        assert summary.unpaired_video_count == 0


# ## =======================================================================
# ## Test: Creator Iota (Fallback Fails)
# ## =======================================================================

class TestFullPipelineIota(PipelineTestBase):
    """
    Creator Iota: TT 30s (Feb 20) + IG 45s (Feb 21).
    Sequence match fails (30!=45). Fallback fails (different uploaded_at).
    Both become unpaired.
    Payouts: TT 500K->$500, IG 100K->$150. Total: $650.
    """

    def test_iota_both_unpaired(self):
        """Both Iota videos should be unpaired."""
        summary = self.results.summary_by_name["Creator Iota"]
        assert summary.paired_video_count == 0
        assert summary.unpaired_video_count == 2

    def test_iota_payout(self):
        """Iota total payout should be $650 ($500 + $150)."""
        summary = self.results.summary_by_name["Creator Iota"]
        assert summary.total_payout == 650.0

    def test_iota_exceptions(self):
        """Iota should have 2 exceptions for unpaired videos."""
        summary = self.results.summary_by_name["Creator Iota"]
        assert summary.exception_count == 2


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
        assert summary.unpaired_video_count == 0


# ## =======================================================================
# ## Test: Aggregate Totals Across All Creators
# ## =======================================================================

class TestFullPipelineAggregates(PipelineTestBase):
    """
    Cross-creator aggregate assertions.

    Expected totals:
      Alpha:   $650   (3 pairs, 0 unpaired)
      Beta:    $70    (0 pairs, 2 unpaired)
      Gamma:   $0     (0 pairs, 1 unpaired)
      Delta:   $600   (2 pairs, 0 unpaired)
      Epsilon: $2000  (1 pair,  1 unpaired)
      Eta:     $1650  (1 pair,  0 unpaired)
      Theta:   $2250  (1 pair,  0 unpaired)
      Iota:    $650   (0 pairs, 2 unpaired)
      Kappa:   $0     (2 pairs, 0 unpaired)
      -------------------------------------------------
      Total:   $7870  (10 pairs, 6 unpaired)

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
        """Sum of all creator payouts should be $7870."""
        total = sum(s.total_payout for s in self.results.creator_summaries)
        assert total == 7870.0

    def test_total_paired_count(self):
        """Total paired count across all creators should be 10."""
        total_paired = sum(s.paired_video_count for s in self.results.creator_summaries)
        assert total_paired == 10

    def test_total_unpaired_count(self):
        """Total unpaired count across all creators should be 6."""
        total_unpaired = sum(s.unpaired_video_count for s in self.results.creator_summaries)
        assert total_unpaired == 6

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
    of creator or scenario.
    """

    def test_pair_chosen_views_is_max(self):
        """For every paired unit, chosen_views == max(tt_views, ig_views)."""
        for unit in self.results.processed_units:
            if unit.paired:
                tt_views = unit.tiktok_video.latest_views or 0
                ig_views = unit.instagram_video.latest_views or 0
                expected_chosen = max(tt_views, ig_views)
                assert unit.chosen_views == expected_chosen, (
                    f"Paired unit for {unit.creator_name}: "
                    f"chosen_views={unit.chosen_views} != "
                    f"max({tt_views}, {ig_views})={expected_chosen}"
                )

    def test_unpaired_chosen_views_is_single(self):
        """For every unpaired unit, chosen_views == the single platform's views."""
        for unit in self.results.processed_units:
            if not unit.paired:
                if unit.tiktok_video:
                    expected = unit.tiktok_video.latest_views or 0
                elif unit.instagram_video:
                    expected = unit.instagram_video.latest_views or 0
                else:
                    pytest.fail(f"Unpaired unit for {unit.creator_name} has no video")
                assert unit.chosen_views == expected, (
                    f"Unpaired unit for {unit.creator_name}: "
                    f"chosen_views={unit.chosen_views} != expected={expected}"
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

    def test_paired_units_have_both_videos(self):
        """Every paired unit must have both tiktok_video and instagram_video."""
        for unit in self.results.processed_units:
            if unit.paired:
                assert unit.tiktok_video is not None, (
                    f"Paired unit for {unit.creator_name} missing tiktok_video"
                )
                assert unit.instagram_video is not None, (
                    f"Paired unit for {unit.creator_name} missing instagram_video"
                )

    def test_unpaired_units_have_exactly_one_video(self):
        """Every unpaired unit must have exactly one of tiktok_video or instagram_video."""
        for unit in self.results.processed_units:
            if not unit.paired:
                has_tt = unit.tiktok_video is not None
                has_ig = unit.instagram_video is not None
                assert has_tt != has_ig, (
                    f"Unpaired unit for {unit.creator_name} should have "
                    f"exactly one video, got tt={has_tt}, ig={has_ig}"
                )
