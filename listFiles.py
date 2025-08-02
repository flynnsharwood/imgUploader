import os

# import csv
import yaml

# Allowed media extensions
ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".mp4", ".webp"}


def get_media_files(directories):
    media_files = []
    for directory in directories:
        for root, _, files in os.walk(directory):
            for f in files:
                ext = os.path.splitext(f)[1].lower()
                if ext in ALLOWED_EXTENSIONS:
                    media_files.append(os.path.join(root, f))
    return media_files


# def read_directories_from_csv(csv_path):
#     directories = []
#     with open(csv_path, newline='', encoding='utf-8') as csvfile:
#         reader = csv.reader(csvfile)
#         for row in reader:
#             if row:  # skip empty lines
#                 directories.append(row[0])
#     return directories


def write_file_list_to_txt(file_paths, output_txt):
    with open(output_txt, "w", encoding="utf-8") as f:
        f.write("#0\n")
        for path in file_paths:
            f.write(f"{path}\n")


def load_config():
    # Get the path to the current script (this file)
    current_dir = os.path.dirname(__file__)
    # Navigate to the parent directory and point to config.yml
    config_path = os.path.abspath(os.path.join(current_dir, "..", "config.yml"))
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    # Example: config is in parent directory
    # config_path = os.path.join('..', 'config.yml')  # or any custom path
    config = load_config()

    output_txt = "MediaFiles.txt"  # Output text file

    dirs = config.get("directories", [])
    media_files = get_media_files(dirs)
    write_file_list_to_txt(media_files, output_txt)

    print(f"Saved {len(media_files)} media file paths to {output_txt}")
