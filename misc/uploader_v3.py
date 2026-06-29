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
from typing import List, Dict, Tuple, Optional, Set

from concurrent.futures import ThreadPoolExecutor

executor = ThreadPoolExecutor(max_workers=4)  # tune pool size


# ------------------ Config ------------------
BATCH_SIZE = 1000  # filelist batch size
MINI_BATCH_SIZE = 19  # upload mini-batch size


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
if not IMG_CHEST_API_KEY:
    logger.warning(
        "[WARN] IMG_CHEST_API_KEY not set. Imgchest uploads will fail; script will rely on Catbox fallback."
    )


# ------------------ DB helpers --------------
def connect_db():
    return psycopg2.connect(
        dbname="boards",
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
    )


def create_table_if_not_exists(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.image_cache (
            hash TEXT PRIMARY KEY,
            link TEXT NOT NULL,
            filename TEXT
        )
        """)
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_image_cache_filename ON public.image_cache(filename)"
    )


def get_existing_filenames(cur, basenames):
    """
    Return a set of filenames that already exist in DB.
    """
    if not basenames:
        return set()
    cur.execute(
        "SELECT filename FROM public.image_cache WHERE filename = ANY(%s)", (basenames,)
    )
    return {row[0] for row in cur.fetchall()}


def compute_md5(image_path):
    h = hashlib.md5()
    with open(image_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def save_link(cur, hash_val, link, filename_or_none):
    """
    Upsert link by hash. Preserve existing filename; only set it if currently NULL
    and we are allowed to supply one (i.e., not a duplicate filename case).
    """
    cur.execute(
        """
        INSERT INTO public.image_cache (hash, link, filename)
        VALUES (%s, %s, %s)
        ON CONFLICT (hash) DO UPDATE
        SET link = EXCLUDED.link,
            filename = COALESCE(public.image_cache.filename, EXCLUDED.filename)
        """,
        (hash_val, link, filename_or_none),
    )
    logger.debug(f"[CACHE SAVE] {hash_val} -> {link} ({filename_or_none})")


# >>> NEW helper to store results, prefer Imgchest link if available
def _save_dual_results(
    cur,
    dual_result: Dict[str, Dict[str, Optional[str]]],
    success_paths: List[str],
    failed_paths: List[str],
    duplicate_report: List[Tuple[str, str]],
):
    """
    Persist per-file results from a dual upload call.
    Preference: store Imgchest link if present; else Catbox.
    Also keep your duplicate-filename guard logic.
    """
    for path, links in dual_result.items():
        img_link = links.get("imgchest")
        cat_link = links.get("catbox")
        chosen = img_link or cat_link

        if not chosen:
            failed_paths.append(path)
            logger.error(f"[UPLOAD FAIL] Both services failed: {path}")
            continue

        hash_val = compute_md5(path)
        filename = os.path.basename(path)

        # Duplicate filename detection: same filename already used by a different hash
        cur.execute(
            "SELECT hash FROM public.image_cache WHERE filename = %s", (filename,)
        )
        row = cur.fetchone()
        allow_filename = True
        if row and row[0] != hash_val:
            allow_filename = False
            duplicate_report.append((filename, path))

        try:
            # We only have a single 'link' column; prefer Imgchest as canonical when available.
            save_link(cur, hash_val, chosen, filename if allow_filename else None)

            # Log both (for observability)
            if img_link and cat_link:
                logger.info(f"[BOTH OK] {path} → IMG:{img_link} | CAT:{cat_link}")
            elif img_link:
                logger.info(f"[ONLY IMGChest] {path} → {img_link}")
            else:
                logger.info(f"[ONLY CATBOX] {path} → {cat_link}")

            success_paths.append(path)
        except Exception as e:
            logger.error(f"[DB ERROR] {path}: {e}")
            failed_paths.append(path)


# ------------------ Upload helpers ----------
# ------------------ Dual upload helpers -----  # >>> NEW
def upload_batch_imgchest(mini_batch: List[str]) -> List[Optional[str]]:
    """
    Try Imgchest single batch. Returns list of links aligned to mini_batch.
    Raises on whole-batch failure.
    """
    return upload_images(mini_batch)


def upload_batch_catbox(mini_batch: List[str]) -> List[Optional[str]]:
    """
    Catbox doesn't have a multi-file endpoint; we loop but consider this a 'batch'.
    Returns list of links aligned to mini_batch (None on per-file failure).
    """
    return upload_to_catbox(mini_batch)


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
                resp = requests.post(
                    "https://catbox.moe/user/api.php",
                    data=data,
                    files=files,
                    timeout=120,
                )
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
    info = requests.get(
        f"https://api.imgchest.com/v1/post/{post_id}", headers=HEADERS, timeout=120
    )
    info.raise_for_status()

    image_list = info.json()["data"]["images"]
    if not image_list or len(image_list) != len(image_paths):
        raise Exception("Mismatch in uploaded image count")

    for path, img in zip(image_paths, image_list):
        logger.info(f"[IMGChest OK] {path} → {img['link']}")
    return [img["link"] for img in image_list]


def upload_dual_for_batch(mini_batch, primary: str):
    """
    Upload mini_batch to both Imgchest and Catbox asynchronously.
    - Launch both in parallel.
    - If primary fails entirely, switch batch to other.
    """
    result = {p: {"imgchest": None, "catbox": None} for p in mini_batch}

    futures = {}
    if primary == "imgchest":
        futures["imgchest"] = executor.submit(upload_batch_imgchest, mini_batch)
        futures["catbox"] = executor.submit(upload_batch_catbox, mini_batch)
    else:
        futures["catbox"] = executor.submit(upload_batch_catbox, mini_batch)
        futures["imgchest"] = executor.submit(upload_batch_imgchest, mini_batch)

    # collect results
    for svc, fut in futures.items():
        try:
            links = fut.result()  # wait for this upload to finish
            for p, ln in zip(mini_batch, links):
                result[p][svc] = ln
        except Exception as e:
            logger.warning(f"[{svc.upper()} FAIL] {e}")

    # if primary failed (all None), but secondary has links → switch
    prim = primary
    if all(result[p][prim] is None for p in mini_batch):
        sec = "catbox" if prim == "imgchest" else "imgchest"
        if any(result[p][sec] for p in mini_batch):
            logger.info(
                f"[SWITCH] Using {sec} links for this batch (primary {prim} failed)."
            )

    return result


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
        yield seq[i : i + size]


# ------------------ Main processing ---------
def process_once(cur, duplicate_report):
    """
    Process a single batch from filelist using top/bottom strategy and dual uploads.
    Returns True if something was processed, else False.
    """
    all_lines = read_all_lines(FILELIST_PATH)
    if not all_lines:
        return False

    batch = all_lines[:BATCH_SIZE]
    remainder = all_lines[BATCH_SIZE:]

    # Filter out those already in DB by filename
    basenames = [os.path.basename(p) for p in batch]
    existing = get_existing_filenames(cur, basenames)
    prefiltered = [p for p in batch if os.path.basename(p) not in existing]

    if existing:
        logger.info(
            f"[CLEANUP] Removed {len(batch) - len(prefiltered)} paths (filename already in DB)"
        )

    # Drop missing files
    prefiltered_existing_paths = [p for p in prefiltered if os.path.exists(p)]
    missing = len(prefiltered) - len(prefiltered_existing_paths)
    if missing:
        logger.info(f"[CLEANUP] Dropped {missing} missing files in this batch")

    paths = prefiltered_existing_paths
    if not paths:
        # Nothing to do in this batch; rebuild filelist as remainder and return
        write_all_lines(FILELIST_PATH, remainder)
        return True

    success_paths: List[str] = []
    failed_paths: List[str] = []

    # Top/bottom pointers and scheduler to avoid both sides picking the same file
    i, j = 0, len(paths) - 1
    scheduled: Set[str] = set()
    x = 0

    while i <= j:
        if i == 0:
            x = 19
        else:
            x = 0
        # ---- Build TOP mini-batch (Imgchest-primary)
        top_batch: List[str] = []
        while i <= j and len(top_batch) < MINI_BATCH_SIZE:
            if paths[i] not in scheduled:
                top_batch.append(paths[i])
                scheduled.add(paths[i])
            i += 1

        # ---- Build BOTTOM mini-batch (Catbox-primary)
        bottom_batch: List[str] = []
        while j >= i and len(bottom_batch) < MINI_BATCH_SIZE:
            if paths[j] not in scheduled:
                bottom_batch.append(paths[j])
                scheduled.add(paths[j])
            j -= 1
        bottom_batch.reverse()  # preserve filelist order in logging

        # Nothing selected? break
        if not top_batch and not bottom_batch:
            break

        # ---- Process TOP (primary Imgchest, also upload to Catbox)
        if top_batch:
            logger.info(
                f"[TOP BATCH] {len(top_batch)} file(s) (Imgchest-primary + Catbox)"
            )
            dual_result_top = upload_dual_for_batch(top_batch, primary="imgchest")
            _save_dual_results(
                cur, dual_result_top, success_paths, failed_paths, duplicate_report
            )

            cur.connection.commit()
            logger.info(f"[COMMIT] Top mini-batch of {len(top_batch)} committed.")
            elapsed = time.time() - start
            total_done = len(success_paths) + len(failed_paths) + x
            per_img = elapsed / total_done if total_done else 0
            logger.info(
                f"[FINISHED] batch. Time spent till now: {elapsed:.2f} sec, {per_img:.2f} sec per img, total imgs = {total_done}"
            )

            time.sleep(1)

        # ---- Process BOTTOM (primary Catbox, also upload to Imgchest)
        if bottom_batch:
            logger.info(
                f"[BOTTOM BATCH] {len(bottom_batch)} file(s) (Catbox-primary + Imgchest)"
            )
            dual_result_bottom = upload_dual_for_batch(bottom_batch, primary="catbox")
            _save_dual_results(
                cur, dual_result_bottom, success_paths, failed_paths, duplicate_report
            )

            cur.connection.commit()
            logger.info(f"[COMMIT] Bottom mini-batch of {len(bottom_batch)} committed.")
            elapsed = time.time() - start
            total_done = len(success_paths) + len(failed_paths)
            per_img = elapsed / total_done if total_done else 0
            logger.info(
                f"[FINISHED] batch. Time spent till now: {elapsed:.2f} sec, {per_img:.2f} sec per img"
            )
            time.sleep(1)

    # Rebuild filelist: keep failed from this batch + untouched remainder
    new_filelist = failed_paths + remainder
    write_all_lines(FILELIST_PATH, new_filelist)
    logger.info(
        f"[BATCH DONE] Uploaded OK: {len(success_paths)}, Failed: {len(failed_paths)}, Remaining in file: {len(new_filelist)}"
    )

    return True


def run():
    conn = connect_db()
    cur = conn.cursor()
    create_table_if_not_exists(cur)

    duplicate_report = (
        []
    )  # (filename, path) entries where filename wasn't set due to duplicate

    # Keep processing until the filelist is exhausted or no progress can be made
    while True:
        progressed = process_once(cur, duplicate_report)
        if not progressed:
            break

    cur.close()
    conn.close()

    if duplicate_report:
        logger.warning(
            "[DUPLICATE FILENAMES] The following filenames were NOT written to the DB due to duplicates:"
        )
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
