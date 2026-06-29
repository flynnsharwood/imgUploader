import os
import yaml
from collections import defaultdict

ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".mp4", ".webp"}


config_path = r"O:\personalScripts\pixboards\configs\upload.yml"
with open(config_path, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

pixboard_directories = config.get("directories", [])

def build_manifest(directories=pixboard_directories):
    folders = defaultdict(list)

    for directory in directories:
        for root, dirs, files in os.walk(directory):
            media = []
            for f in files:
                ext = os.path.splitext(f)[1].lower()
                if ext in ALLOWED_EXTENSIONS:
                    media.append(os.path.join(root, f))

            if media and not dirs:   # leaf folder only
                folders[root].extend(sorted(media))

    return {
        "folders": [
            {"path": folder, "files": files}
            for folder, files in sorted(folders.items())
        ]
    }

def save_manifest(manifest, output_file="../manifest.yml"):
    with open(output_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(manifest, f, allow_unicode=True, sort_keys=False)
