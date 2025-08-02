import argparse
import hashlib
import logging
import os
import time
from datetime import date

import psycopg2
import requests
from dotenv import load_dotenv

parser = argparse.ArgumentParser(description="Image uploader")
# other arguments...
parser.add_argument(
    "--filelist",
    # dest="filelist_path",
    type=str,
    default="MediaFiles.txt",
    help="Path to file list (default: MediaFiles.txt)",
)
args = parser.parse_args()

filelist_path = args.filelist


# --- Logging ---
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

# --- Load API Key ---
load_dotenv()
IMG_CHEST_API_KEY = os.getenv("IMG_CHEST_API_KEY")
print(IMG_CHEST_API_KEY)
HEADERS = {"Authorization": f"Bearer {IMG_CHEST_API_KEY}"}

LIST_FILE_PATH = args.filelist


def upload_to_catbox(image_paths):
    logger.info(f"[FALLBACK] Uploading {len(image_paths)} image(s) to Catbox...")
    uploaded_links = []

    for path in image_paths:
        with open(path, "rb") as f:
            files = {"fileToUpload": (os.path.basename(path), f)}
            data = {"reqtype": "fileupload"}
            try:
                resp = requests.post(
                    "https://catbox.moe/user/api.php", data=data, files=files
                )
                resp.raise_for_status()
                link = resp.text.strip()
                logger.info(f"[SUCCESS] {path} → {link}")
                uploaded_links.append(link)
            except Exception as e:
                logger.error(f"[CATBOX ERROR] Failed to upload {path}: {e}")
                uploaded_links.append(None)

    return uploaded_links


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
            link TEXT NOT NULL
        )
    """
    )


def compute_hash(image_path):
    logger.info(f"[HASH] Computing hash for: {image_path}")
    with open(image_path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


def save_link(cursor, hash_val, link):
    cursor.execute(
        """
        INSERT INTO image_cache (hash, link)
        VALUES (%s, %s)
        ON CONFLICT (hash) DO NOTHING
    """,
        (hash_val, link),
    )
    logger.info(f"[CACHE SAVE] {hash_val} → {link}")

import mimetypes

def upload_images(image_paths):
    logger.info(f"[UPLOAD] Uploading {len(image_paths)} image(s)...")
    files = []
    for image_path in image_paths:
        with open(image_path, "rb") as f:
            filename = os.path.basename(image_path)
            mime = mimetypes.guess_type(filename)[0] or "application/octet-stream"
            files.append(("images[]", (filename, f.read(), mime)))

    data = {"title": os.path.basename(image_paths[0])}
    resp = requests.post(
        "https://api.imgchest.com/v1/post",
        headers=HEADERS,
        files=files,
        data=data,
    )
    resp.raise_for_status()

    post_id = resp.json()["data"]["id"]
    info = requests.get(f"https://api.imgchest.com/v1/post/{post_id}", headers=HEADERS)
    info.raise_for_status()

    image_list = info.json()["data"]["images"]
    if not image_list or len(image_list) != len(image_paths):
        raise Exception("Mismatch in uploaded image count")

    for path, img in zip(image_paths, image_list):
        logger.info(f"[SUCCESS] {path} → {img['link']}")
    return [img["link"] for img in image_list]


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
            logger.info(f"[SKIP] Already uploaded (hash match): {path}")
            removed_count += 1
        else:
            remaining_paths.append(path)

    logger.info(f"[CLEANUP] Removed {removed_count} already-uploaded or missing files")
    return remaining_paths


# def remove_uploaded_and_missing(cursor, file_paths):
#     remaining_paths = []
#     removed_count = 0

#     for path in file_paths:
#         if not os.path.exists(path):
#             removed_count += 1
#             continue

#         hash_val = compute_hash(path)
#         cursor.execute("SELECT link FROM image_cache WHERE hash = %s", (hash_val,))
#         row = cursor.fetchone()

#         if row and os.path.basename(row[0]) == os.path.basename(path):
#             logger.info(f"[SKIP] Already uploaded: {path}")
#             removed_count += 1
#         else:
#             remaining_paths.append(path)

#     logger.info(f"[CLEANUP] Removed {removed_count} already-uploaded or missing files")
#     return remaining_paths


def try_upload_with_retries(paths_to_upload, retries=1, delay=2):
    for attempt in range(1, retries + 1):
        try:
            return upload_images(paths_to_upload)
        except Exception as e:
            logger.warning(f"[RETRY {attempt}] {e}")
            if attempt == retries:
                logger.warning("[FALLBACK] Trying Catbox instead...")
                return upload_to_catbox(paths_to_upload)
            time.sleep(delay)


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

        for mini_batch in chunked(chunk_to_upload, 20):
            try:
                uploaded_links = try_upload_with_retries(mini_batch, retries=1)
                for path, link in zip(mini_batch, uploaded_links):
                    hash_val = compute_hash(path)
                    save_link(cur, hash_val, link)
                conn.commit()
                logger.info("[BATCH] Upload and DB commit complete")
                total_uploaded += len(mini_batch)
                time.sleep(2)
            except Exception as e:
                logger.error(f"[ERROR] Failed mini-batch: {e}")
                # break  # Exit mini-batch loop

        # After chunk is processed, remove it from file
        all_files = read_file_list(LIST_FILE_PATH)
        remaining = [f for f in all_files if f not in chunk]
        with open(LIST_FILE_PATH, "w", encoding="utf-8") as f:
            for path in remaining:
                f.write(path + "\n")

    cur.close()
    conn.close()
    logger.info(f"[DONE] Uploaded {total_uploaded} file(s)")


if __name__ == "__main__":
    start_time = time.time()
    try:
        upload_all()
        elapsed_time = time.time() - start_time
        logger.info(f"[FINISHED] Time taken: {elapsed_time:.2f} sec")
    except Exception as e:
        logger.error(f"[FATAL] {e}")
