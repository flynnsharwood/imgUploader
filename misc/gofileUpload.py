import argparse
import hashlib
import logging
import os
import time
from datetime import date

import psycopg2
import requests

from dotenv import load_dotenv

load_dotenv()

parser = argparse.ArgumentParser(description="Image uploader to GoFile")
parser.add_argument(
    "--filelist",
    type=str,
    default="MediaFiles.txt",
    help="Path to file list (default: MediaFiles.txt)",
)
args = parser.parse_args()
filelist_path = args.filelist

def setup_logger(name=None):
    today = date.today().isoformat()
    logs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
    os.makedirs(logs_dir, exist_ok=True)
    log_file_path = f"fileUpload_{today}.log"

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        file_handler = logging.FileHandler(log_file_path, encoding="utf-8", mode="a")
        file_handler.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger

logger = setup_logger(__name__)
LIST_FILE_PATH = args.filelist

def connect_db():
    return psycopg2.connect(
        dbname="boards",
        user="postgres",
        password="password",
        host="localhost",
    )

def create_table_if_not_exists(cursor):
    logger.info("[DB] Ensuring image_cache table exists...")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS image_cache (
            hash TEXT PRIMARY KEY,
            link TEXT NOT NULL,
            filename TEXT
        )
        """
    )

def compute_hash(image_path):
    with open(image_path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()

def save_link(cursor, hash_val, link, filename):
    cursor.execute(
        """
        INSERT INTO image_cache (hash, link, filename)
        VALUES (%s, %s, %s)
        ON CONFLICT (hash) DO UPDATE
        SET link = EXCLUDED.link,
            filename = EXCLUDED.filename
        """,
        (hash_val, link, filename),
    )
    logger.info(f"[CACHE SAVE] {hash_val} → {link} ({filename})")

def upload_to_gofile(file_path):
    token = os.getenv("gofile")
    headers = {"Authorization": f"Bearer {token}"}

    # Get the best server
    try:
        response = requests.get("https://api.gofile.io/getServer", headers=headers)
        response.raise_for_status()
        server = response.json()["data"]["server"]
    except Exception as e:
        logger.error(f"[GOFILE ERROR] Failed to get server: {e}")
        return None

    upload_url = f"https://{server}.gofile.io/uploadFile"

    # Upload file
    try:
        with open(file_path, "rb") as f:
            files = {"file": f}
            response = requests.post(upload_url, headers=headers, files=files)
            response.raise_for_status()
            data = response.json()["data"]
            content_id = data["code"]  # Fixed key from "id" to "code"
    except Exception as e:
        logger.error(f"[GOFILE ERROR] Failed to upload {file_path}: {e}")
        return None

    # Get direct download link
    try:
        content_url = f"https://api.gofile.io/getContent?contentId={content_id}&token={token}&websiteToken=websiteToken&cache=true"
        response = requests.get(content_url, headers=headers)
        response.raise_for_status()
        contents = response.json()["data"]["children"]

        for item in contents.values():
            if os.path.basename(file_path) in item["name"]:
                return item["directLink"]
    except Exception as e:
        logger.error(f"[GOFILE ERROR] Failed to get direct link for {file_path}: {e}")

    return None


def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]

def read_file_list(path):
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def remove_uploaded_and_missing(cursor, file_paths):
    remaining_paths = []
    removed_count = 0

    for path in file_paths:
        if not os.path.exists(path):
            removed_count += 1
            continue

        hash_val = compute_hash(path)
        cursor.execute("SELECT 1 FROM image_cache WHERE hash = %s", (hash_val,))
        row = cursor.fetchone()

        if row:
            removed_count += 1
        else:
            remaining_paths.append(path)

    return remaining_paths

def delete_uploaded_from_filelist(filelist_path, count):
    with open(filelist_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    with open(filelist_path, "w", encoding="utf-8") as f:
        f.writelines(lines[count:])

def upload_all():
    conn = connect_db()
    cur = conn.cursor()
    create_table_if_not_exists(cur)

    all_files = read_file_list(LIST_FILE_PATH)
    total_uploaded = 0

    for chunk in chunked(all_files, 100):
        logger.info(f"[CHUNK] Processing {len(chunk)} files")
        chunk_to_upload = remove_uploaded_and_missing(cur, chunk)

        if not chunk_to_upload:
            logger.info("[CHUNK] No files to upload in this chunk")
            delete_uploaded_from_filelist(filelist_path, len(chunk))
            continue

        for path in chunk_to_upload:
            try:
                link = upload_to_gofile(path)
                if link:
                    hash_val = compute_hash(path)
                    filename = os.path.basename(path)
                    save_link(cur, hash_val, link, filename)
                    conn.commit()
                    total_uploaded += 1
                    time.sleep(1)
            except Exception as e:
                logger.error(f"[ERROR] Failed to upload {path}: {e}")

        all_files = read_file_list(LIST_FILE_PATH)
        remaining = [f for f in all_files if f not in chunk]
        with open(LIST_FILE_PATH, "w", encoding="utf-8") as f:
            for path in remaining:
                f.write(path + "\n")

if __name__ == "__main__":
    start_time = time.time()
    logger.info(f"starting app at {start_time:.2f}")
    try:
        upload_all()
        elapsed_time = time.time() - start_time
        logger.info(f"[FINISHED] Time taken: {elapsed_time:.2f} sec")
    except Exception as e:
        logger.error(f"[FATAL] {e}")
