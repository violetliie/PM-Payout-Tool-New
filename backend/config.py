import os
from dotenv import load_dotenv

load_dotenv()

SHORTIMIZE_API_KEY = os.getenv("SHORTIMIZE_API_KEY", "")
SHORTIMIZE_BASE_URL = os.getenv("SHORTIMIZE_BASE_URL", "https://api.shortimize.com")
CREATOR_SHEET_CSV_URL = os.getenv(
    "CREATOR_SHEET_CSV_URL",
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vTQcA8MAAhZ4urj_91M7rq80UwsmR3XePus2j2Ky-iZD_j_YSC5U5-kdSf2P1E73fohaAZWqJ6a4i2w/pub?output=csv&gid=651686011",
)
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/tmp/payout_reports")
