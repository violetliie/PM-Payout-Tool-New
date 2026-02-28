"""
Shared test fixtures for the Polymarket Creator Payout Tool test suite.

The autouse fixture `mock_frame_extraction` patches the frame_extractor
functions imported by matcher.py so all tests use synthetic phash results
instead of downloading real videos via yt-dlp.
"""

import pytest
from unittest.mock import patch
from PIL import Image


@pytest.fixture(autouse=True)
def mock_frame_extraction():
    """
    Auto-mock frame extraction for ALL tests.

    Tests use fake ad_links (e.g., "tt_alice_1") that yt-dlp cannot download.
    This fixture patches the frame_extractor functions so:
      - get_frame() returns a synthetic PIL Image (never None)
      - is_same_video() returns True (all phash checks pass)
      - compare_frames() returns 0 (distance = 0)

    Tests that need specific phash behavior can override by configuring
    the mock's return_value or side_effect within the test body.
    """
    fake_frame = Image.new("RGB", (720, 1280), color="red")

    with patch("services.matcher.get_frame", return_value=fake_frame) as mock_get, \
         patch("services.matcher.is_same_video", return_value=True) as mock_is_same, \
         patch("services.matcher.compare_frames", return_value=0) as mock_compare:
        yield {
            "get_frame": mock_get,
            "is_same_video": mock_is_same,
            "compare_frames": mock_compare,
        }
