"""
First frame extraction and perceptual hash comparison for cross-platform video matching.

Uses yt-dlp to download videos and ffmpeg to extract the first frame,
then computes perceptual hashes (phash) via imagehash for comparison.

Functions:
  extract_first_frame(ad_link) -> Image | None
      Download video, extract frame 0, return as PIL Image.

  compare_frames(img1, img2) -> int
      Compute phash hamming distance between two images.

  is_same_video(img1, img2, threshold=10) -> bool
      True if hamming distance <= threshold.

  get_frame(ad_link, cache) -> Image | None
      Cached wrapper around extract_first_frame.

Performance: ~1.8 seconds per video. Both TikTok and Instagram
produce 720x1280 first frames — no normalization needed.
"""

import logging
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

import imagehash
from PIL import Image

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Timeouts for subprocess calls
# ---------------------------------------------------------------------------
YTDLP_TIMEOUT = 60   # seconds
FFMPEG_TIMEOUT = 15   # seconds

# ---------------------------------------------------------------------------
# Perceptual hash threshold — same video = distance 0-10, different = 30+
# ---------------------------------------------------------------------------
PHASH_THRESHOLD = 10


# ===========================================================================
# System dependency checks
# ===========================================================================

def check_dependencies() -> tuple[bool, list[str]]:
    """
    Verify that yt-dlp and ffmpeg are available on the system.

    Returns:
        (all_ok, missing): True if both found, list of missing tool names.
    """
    missing = []
    if shutil.which("yt-dlp") is None:
        missing.append("yt-dlp")
    if shutil.which("ffmpeg") is None:
        missing.append("ffmpeg")
    return len(missing) == 0, missing


# ===========================================================================
# First frame extraction
# ===========================================================================

def extract_first_frame(ad_link: str) -> Optional[Image.Image]:
    """
    Download a video and extract its first frame as a PIL Image.

    Process:
      1. yt-dlp downloads the video to a temp file (handles CDN auth/redirects)
      2. ffmpeg extracts frame 0 as JPEG
      3. Open JPEG with Pillow, return Image
      4. Both temp files cleaned up in finally block

    TikTok CDN returns 403 if ffmpeg accesses the URL directly,
    so we always download the full video first with yt-dlp.

    Args:
        ad_link: The video URL (TikTok or Instagram)

    Returns:
        PIL Image of the first frame, or None if extraction fails.
    """
    temp_video_path = None
    temp_frame_path = None

    try:
        # Create temp files
        temp_dir = tempfile.mkdtemp(prefix="payout_frame_")
        temp_video_path = str(Path(temp_dir) / "video.mp4")
        temp_frame_path = str(Path(temp_dir) / "frame.jpg")

        # --- Step 1: Download video with yt-dlp ---
        ytdlp_cmd = [
            "yt-dlp",
            "-f", "best[ext=mp4]/best",
            "-o", temp_video_path,
            ad_link,
        ]

        logger.debug(f"Downloading video: {ad_link}")
        result = subprocess.run(
            ytdlp_cmd,
            capture_output=True,
            text=True,
            timeout=YTDLP_TIMEOUT,
        )

        if result.returncode != 0:
            logger.warning(
                f"yt-dlp failed for {ad_link}: {result.stderr[:200]}"
            )
            return None

        # Verify video file exists
        if not Path(temp_video_path).exists():
            # yt-dlp may add an extension — find the actual file
            video_files = list(Path(temp_dir).glob("video.*"))
            if video_files:
                temp_video_path = str(video_files[0])
            else:
                logger.warning(f"yt-dlp produced no output file for {ad_link}")
                return None

        # --- Step 2: Extract first frame with ffmpeg ---
        ffmpeg_cmd = [
            "ffmpeg",
            "-y",
            "-i", temp_video_path,
            "-vframes", "1",
            "-q:v", "2",
            temp_frame_path,
        ]

        logger.debug(f"Extracting first frame: {ad_link}")
        result = subprocess.run(
            ffmpeg_cmd,
            capture_output=True,
            text=True,
            timeout=FFMPEG_TIMEOUT,
        )

        if result.returncode != 0:
            logger.warning(
                f"ffmpeg failed for {ad_link}: {result.stderr[:200]}"
            )
            return None

        # --- Step 3: Open frame with Pillow ---
        if not Path(temp_frame_path).exists():
            logger.warning(f"ffmpeg produced no frame for {ad_link}")
            return None

        img = Image.open(temp_frame_path).copy()  # .copy() to detach from file
        logger.debug(f"Frame extracted: {ad_link} ({img.size})")
        return img

    except subprocess.TimeoutExpired:
        logger.warning(f"Timeout extracting frame from {ad_link}")
        return None
    except Exception as e:
        logger.warning(f"Frame extraction failed for {ad_link}: {e}")
        return None
    finally:
        # --- Step 4: Clean up temp files ---
        if temp_video_path:
            temp_dir = str(Path(temp_video_path).parent)
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass


# ===========================================================================
# Perceptual hash comparison
# ===========================================================================

def compare_frames(img1: Image.Image, img2: Image.Image) -> int:
    """
    Compute the perceptual hash hamming distance between two images.

    Uses imagehash.phash() — a 64-bit perceptual hash that captures
    visual similarity. Distance 0 = identical, 0-10 = same video,
    30+ = different video.

    Args:
        img1: First PIL Image
        img2: Second PIL Image

    Returns:
        Hamming distance (int) between the two phashes.
    """
    hash1 = imagehash.phash(img1)
    hash2 = imagehash.phash(img2)
    return hash1 - hash2


def is_same_video(
    img1: Image.Image,
    img2: Image.Image,
    threshold: int = PHASH_THRESHOLD,
) -> bool:
    """
    Check if two images represent the same video based on phash distance.

    Args:
        img1:      First PIL Image (first frame)
        img2:      Second PIL Image (first frame)
        threshold: Maximum hamming distance to consider a match (default: 10)

    Returns:
        True if the hamming distance <= threshold.
    """
    return compare_frames(img1, img2) <= threshold


# ===========================================================================
# Cached frame extraction
# ===========================================================================

def get_frame(
    ad_link: str,
    cache: dict[str, Optional[Image.Image]],
) -> Optional[Image.Image]:
    """
    Extract a video's first frame, using a cache to avoid re-downloading.

    Each video is only downloaded and processed once per pipeline run.
    Cache is keyed by ad_link.

    Args:
        ad_link: The video URL
        cache:   Shared dict[ad_link → Image | None]

    Returns:
        PIL Image of the first frame, or None if extraction failed.
    """
    if ad_link not in cache:
        cache[ad_link] = extract_first_frame(ad_link)
    return cache[ad_link]
