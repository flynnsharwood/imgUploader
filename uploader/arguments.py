import argparse
from dotenv import load_dotenv
import os

parser = argparse.ArgumentParser(description="Image uploader")
parser.add_argument(
    "--filelist",
    type=str,
    default="MediaFiles.txt",
    help="Path to file list (default: MediaFiles.txt)",
)
parser.add_argument(
    "--yml",
    action="store_true",
    default=False,
    help="create yml for media?",
)

args = parser.parse_args()
FILELIST_PATH = args.filelist

load_dotenv()
IMG_CHEST_API_KEY = os.getenv("IMG_CHEST_API_KEY")
HEADERS = {"Authorization": f"Bearer {IMG_CHEST_API_KEY}"} if IMG_CHEST_API_KEY else {}
aiven_host = os.getenv("aiven_host")
port = 27191
aiven_password = os.getenv("aiven_password")
if not IMG_CHEST_API_KEY:
    logger.warning("[WARN] IMG_CHEST_API_KEY not set. Imgchest uploads will fail; script will rely on Catbox fallback.")

