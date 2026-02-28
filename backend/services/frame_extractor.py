"""
First frame extraction and perceptual hash comparison for cross-platform video matching.

Uses yt-dlp to download videos and ffmpeg to extract the first frame,
then computes perceptual hashes (phash) via imagehash for comparison.

Memory-efficient: images are discarded immediately after computing phash.
Only the 64-bit hash is cached, not the full 720x1280 PIL Image (~2.7MB each).

Functions:
  extract_first_frame(ad_link) -> Image | None
      Download video, extract frame 0, return as PIL Image.

  extract_phash(ad_link) -> ImageHash | None
      Download video, extract frame, compute phash, discard image.

  get_phash(ad_link, cache) -> ImageHash | None
      Cached wrapper around extract_phash.

  compare_hashes(h1, h2) -> int
      Hamming distance between two phash values.

  is_same_video(h1, h2, threshold=10) -> bool
      True if hamming distance <= threshold.

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
# Phash extraction (memory-efficient — discards image immediately)
# ===========================================================================

def extract_phash(ad_link: str) -> Optional[imagehash.ImageHash]:
    """
    Download a video, extract first frame, compute phash, discard image.

    This is the memory-efficient alternative to extract_first_frame().
    The PIL Image (~2.7MB at 720x1280) is discarded immediately after
    computing the 64-bit phash.

    Args:
        ad_link: The video URL (TikTok or Instagram)

    Returns:
        ImageHash of the first frame, or None if extraction fails.
    """
    img = extract_first_frame(ad_link)
    if img is None:
        return None
    phash = imagehash.phash(img)
    # Image is discarded when it goes out of scope here
    del img
    return phash


# ===========================================================================
# Perceptual hash comparison
# ===========================================================================

def compare_hashes(
    hash1: imagehash.ImageHash,
    hash2: imagehash.ImageHash,
) -> int:
    """
    Compute the hamming distance between two perceptual hashes.

    Distance 0 = identical, 0-10 = same video, 30+ = different video.

    Args:
        hash1: First phash
        hash2: Second phash

    Returns:
        Hamming distance (int) between the two phashes.
    """
    return hash1 - hash2


def is_same_video(
    hash1: imagehash.ImageHash,
    hash2: imagehash.ImageHash,
    threshold: int = PHASH_THRESHOLD,
) -> bool:
    """
    Check if two phashes represent the same video.

    Args:
        hash1:     First phash
        hash2:     Second phash
        threshold: Maximum hamming distance to consider a match (default: 10)

    Returns:
        True if the hamming distance <= threshold.
    """
    return compare_hashes(hash1, hash2) <= threshold


# ===========================================================================
# Cached phash extraction
# ===========================================================================

def get_phash(
    ad_link: str,
    cache: dict[str, Optional[imagehash.ImageHash]],
) -> Optional[imagehash.ImageHash]:
    """
    Extract a video's phash, using a cache to avoid re-downloading.

    Each video is only downloaded and processed once per pipeline run.
    Cache stores only the 64-bit phash (~100 bytes), not the full image.

    Args:
        ad_link: The video URL
        cache:   Shared dict[ad_link -> ImageHash | None]

    Returns:
        ImageHash of the first frame, or None if extraction failed.
    """
    if ad_link not in cache:
        cache[ad_link] = extract_phash(ad_link)
    return cache[ad_link]
