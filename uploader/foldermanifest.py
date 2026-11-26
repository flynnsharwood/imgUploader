import os
import yaml
from collections import defaultdict

ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".mp4", ".webp"}


config_path = r"O:\personalScripts\pixboards\configs\upload.yml"
with open(config_path, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)


print("dirs in config =", config.get("directories"))
pixboard_directories = config.get("directories", [])


def build_manifest(directories=pixboard_directories):
    print(f"dirs in build = {directories}")

    folders = defaultdict(list)

    for directory in directories:

        print(f"SCANNING: {directory}")
        print(f"EXISTS: {os.path.exists(directory)}")

        for root, dirs, files in os.walk(directory):

            # print(
            #     f"ROOT={root}\n"
            #     f"DIRS={dirs}\n"
            #     f"FILES={files}\n"
            # )

            media = []

            for f in files:
                ext = os.path.splitext(f)[1].lower()

                if ext in ALLOWED_EXTENSIONS:
                    media.append(os.path.join(root, f))

            # print(f"MEDIA={len(media)}")

            if media and not dirs:
                # print(f"LEAF FOUND: {root}")
                folders[root].extend(sorted(media))

    # print(f"folders built={folders}")

    return {
        "folders": [
            {"path": folder, "files": files}
            for folder, files in sorted(folders.items())
        ]
    }


def save_manifest(manifest, output_file="manifest.yml"):
    with open(output_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(manifest, f, allow_unicode=True, sort_keys=False)


def read_manifest(path="manifest.yml"):
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    return data