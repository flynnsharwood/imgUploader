import argparse
import hashlib
import logging
import os
import time
from datetime import date
import mimetypes

import psycopg2
import requests
from dotenv import load_dotenv


# ------------------ Config ------------------
BATCH_SIZE = 1000         # filelist batch size
MINI_BATCH_SIZE = 20      # upload mini-batch size

# ------------------ Globals -----------------
total_uploaded = 0
total_failed = 0

# ------------------ CLI ---------------------
parser = argparse.ArgumentParser(description="Image uploader")
parser.add_argument(
    "--filelist",
    type=str,
    default="MediaFiles.txt",
    help="Path to file list (default: MediaFiles.txt)",
)
args = parser.parse_args()
FILELIST_PATH = args.filelist


# ------------------ Logging -----------------
def setup_logger(name=None):
    today = date.today().isoformat()
    logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(logs_dir, exist_ok=True)
    log_file_path = os.path.join(logs_dir, f"fileUpload_{today}.log")

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


# ------------------ Env & API ---------------
load_dotenv()
IMG_CHEST_API_KEY = os.getenv("IMG_CHEST_API_KEY")
HEADERS = {"Authorization": f"Bearer {IMG_CHEST_API_KEY}"} if IMG_CHEST_API_KEY else {}
aiven_host = os.getenv("aiven_host")
port = 27191
aiven_password = os.getenv("aiven_password")
if not IMG_CHEST_API_KEY:
    logger.warning("[WARN] IMG_CHEST_API_KEY not set. Imgchest uploads will fail; script will rely on Catbox fallback.")



# ------------------ DB helpers --------------

def connect_aiven():
    return psycopg2.connect(
        host=aiven_host,
        port="...",
        dbname="defaultdb",
        user="avnadmin",
        password="...",
        sslmode="require",
    )

def connect_db():
    return psycopg2.connect(
        dbname="boards",
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
    )


def create_table_if_not_exists(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS public.image_cache (
            hash TEXT PRIMARY KEY,
            link TEXT NOT NULL,
            filename TEXT,
            uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )

    # cursor.execute("""
    #     ALTER TABLE public.image_cache
    #     ADD COLUMN IF NOT EXISTS uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    # """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_image_cache_filename ON public.image_cache(filename)")

    


def get_existing_filenames(cur, basenames):
    """
    Return a set of filenames that already exist in DB.
    """
    if not basenames:
        return set()
    cur.execute("SELECT filename FROM public.image_cache WHERE filename = ANY(%s)", (basenames,))
    return {row[0] for row in cur.fetchall()}


def compute_md5(image_path):
    h = hashlib.md5()
    with open(image_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def get_existing_hashes(cur, hashes):
    if not hashes:
        return set()

    cur.execute(
        "SELECT hash FROM public.image_cache WHERE hash = ANY(%s)",
        (hashes,)
    )
    return {row[0] for row in cur.fetchall()}


def save_link(cur, hash_val, link, filename_or_none):
    """
    Upsert link by hash. Preserve existing filename; only set it if currently NULL
    and we are allowed to supply one (i.e., not a duplicate filename case).
    """
    cur.execute(
        """
        INSERT INTO public.image_cache
            (hash, link, filename, uploaded_at)
        VALUES
            (%s, %s, %s, NOW())
        ON CONFLICT (hash) DO UPDATE
        SET
            link = EXCLUDED.link,
            filename = COALESCE(
                public.image_cache.filename,
                EXCLUDED.filename
            )
        """,
        (hash_val, link, filename_or_none),
    )


# ------------------ Upload helpers ----------
def upload_to_catbox(image_paths):
    """
    Fallback uploader (one-by-one). Returns a list of links (None on failure for a file).
    """
    logger.info(f"[FALLBACK] Uploading {len(image_paths)} image(s) to Catbox...")
    uploaded_links = []
    for path in image_paths:
        try:
            with open(path, "rb") as f:
                files = {"fileToUpload": (os.path.basename(path), f)}
                data = {"reqtype": "fileupload"}
                resp = requests.post("https://catbox.moe/user/api.php", data=data, files=files, timeout=120)
                resp.raise_for_status()
                link = resp.text.strip()
                logger.info(f"[CATBOX OK] {path} → {link}")
                uploaded_links.append(link)
        except Exception as e:
            logger.error(f"[CATBOX ERROR] {path}: {e}")
            uploaded_links.append(None)
    return uploaded_links


def upload_images(image_paths):
    """
    Your original Imgchest logic (bulk per mini-batch).
    Returns list of links (order corresponds to image_paths).
    Raises on failure.
    """
    logger.info(f"[UPLOAD] Uploading {len(image_paths)} image(s) to Imgchest...")
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
        timeout=300,
    )
    resp.raise_for_status()

    post_id = resp.json()["data"]["id"]
    info = requests.get(f"https://api.imgchest.com/v1/post/{post_id}", headers=HEADERS, timeout=120)
    info.raise_for_status()

    image_list = info.json()["data"]["images"]
    if not image_list or len(image_list) != len(image_paths):
        raise Exception("Mismatch in uploaded image count")

    for path, img in zip(image_paths, image_list):
        logger.info(f"[IMGChest OK] {path} → {img['link']}")
    return [img["link"] for img in image_list]


# ------------------ Filelist helpers --------
def read_all_lines(path):
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def write_all_lines(path, lines):
    with open(path, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


# ------------------ Main processing ---------
def process_once(cur, duplicate_report):
    
    global total_uploaded, total_failed

    """
    Process a single 5000-size batch from filelist.
    Returns True if something was processed (i.e., filelist had data), else False.
    """
    all_lines = read_all_lines(FILELIST_PATH)
    if not all_lines:
        return False

    batch = all_lines[:BATCH_SIZE]
    remainder = all_lines[BATCH_SIZE:]

    # # Filter out those already in DB by filename
    # basenames = [os.path.basename(p) for p in batch]
    # existing = get_existing_filenames(cur, basenames)
    # prefiltered = [p for p in batch if os.path.basename(p) not in existing]

    # if existing:
    #     logger.info(f"[CLEANUP] Removed {len(batch) - len(prefiltered)} paths (filename already in DB)")

    # Compute hashes first
    path_hashes = {}

    for path in batch:
        if os.path.exists(path):
            try:
                path_hashes[path] = compute_md5(path)
            except Exception as e:
                logger.error(f"[HASH ERROR] {path}: {e}")

    existing_hashes = get_existing_hashes(
        cur,
        list(path_hashes.values())
    )

    prefiltered = [
        path
        for path, hash_val in path_hashes.items()
        if hash_val not in existing_hashes
    ]

    removed = len(path_hashes) - len(prefiltered)

    if removed:
        logger.info(
            f"[CLEANUP] Removed {removed} files already present by hash"
        )

    # Drop missing files
    prefiltered_existing_paths = [p for p in prefiltered if os.path.exists(p)]
    missing = len(prefiltered) - len(prefiltered_existing_paths)
    if missing:
        logger.info(f"[CLEANUP] Dropped {missing} missing files in this batch")

    paths_to_upload = prefiltered_existing_paths
    success_paths = []
    failed_paths = []

    # Upload in mini-batches of 20
    for mini_batch in chunked(paths_to_upload, MINI_BATCH_SIZE):
        links = None
        try:
            links = upload_images(mini_batch)
            elapsed = time.time() - start
            per_img = elapsed / total_uploaded if total_uploaded else 0
            logger.info(f"[FINISHED] batch. Time spent till now: {elapsed:.2f} sec, {per_img:.2f} sec per img, total imgs = {total_uploaded}")
        except Exception as e:
            logger.warning(f"[IMGChest FAIL] {e} → Falling back to Catbox")
            links = upload_to_catbox(mini_batch)

        # Save results to DB
        for path, link in zip(mini_batch, links):
            if not link:
                failed_paths.append(path)
                total_failed += 1
                continue

            hash_val = path_hashes[path]
            filename = os.path.basename(path)

            # Duplicate filename detection: same filename already used by a different hash
            cur.execute("SELECT hash FROM public.image_cache WHERE filename = %s", (filename,))
            row = cur.fetchone()
            allow_filename = True
            if row and row[0] != hash_val:
                allow_filename = False
                duplicate_report.append((filename, path))

            # Save (never overwrite an existing filename; only fill if empty and allowed)
            try:
                logger.info(
                                f"[INSERT] hash={hash_val} "
                                f"filename={filename} "
                                f"allow_filename={allow_filename}"
                            )
                save_link(cur, hash_val, link, filename if allow_filename else None)
                success_paths.append(path)
                total_uploaded += 1
            except Exception as e:
                logger.error(f"[DB ERROR] {path}: {e}")
                failed_paths.append(path)

        # Commit after each mini-batch
        cur.connection.commit()
        logger.info(f"[COMMIT] Mini-batch of {len(mini_batch)} committed.")

        # Be nice to APIs
        time.sleep(1)

    # Rebuild filelist: keep failed from this batch + untouched remainder
    new_filelist = failed_paths + remainder
    write_all_lines(FILELIST_PATH, new_filelist)
    logger.info(
        f"[BATCH DONE] Uploaded OK (this batch): {len(success_paths)}, "
        f"Failed (this batch): {len(failed_paths)}, "
        f"Remaining in file: {len(new_filelist)}, "
        f"TOTAL Uploaded: {total_uploaded}, TOTAL Failed: {total_failed}"
    )
    return True


def run():
    conn = connect_db()
    cur = conn.cursor()
    create_table_if_not_exists(cur)

    duplicate_report = []  # (filename, path) entries where filename wasn't set due to duplicate

    # Keep processing until the filelist is exhausted or no progress can be made
    while True:
        progressed = process_once(cur, duplicate_report)
        if not progressed:
            break

    cur.close()
    conn.close()

    if duplicate_report:
        logger.warning("[DUPLICATE FILENAMES] The following filenames were NOT written to the DB due to duplicates:")
        for fname, path in duplicate_report:
            logger.warning(f" - {fname} :: {path}")


# ------------------ Entrypoint --------------
if __name__ == "__main__":
    start = time.time()
    logger.info(f"[START] Uploader starting at {start:.2f}")
    try:
        run()
        elapsed = time.time() - start
        logger.info(f"[FINISHED] Time taken: {elapsed:.2f} sec")
    except Exception as e:
        logger.error(f"[FATAL] {e}")
