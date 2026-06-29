import os
import hashlib
import psycopg2
import argparse
from dotenv import load_dotenv

# --- Args ---
parser = argparse.ArgumentParser(description="Sync image filenames to DB by hash")
parser.add_argument(
    "--filelist",
    type=str,
    default="MediaFiles.txt",
    help="Path to file list (default: MediaFiles.txt)",
)
args = parser.parse_args()

FILELIST_PATH = args.filelist

# --- Load environment ---
load_dotenv()

def connect_db():
    return psycopg2.connect(
        dbname="boards",
        user="postgres",
        password="password",
        host="localhost",
    )

def compute_hash(image_path):
    with open(image_path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()

def iter_file_list(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped:
                yield stripped

def cleanup_filelist(cur):
    """Remove paths from filelist if filename already exists in DB."""
    print("[STEP] Cleaning up filelist against DB filenames...")

    # Fetch all known filenames from DB
    cur.execute("SELECT filename FROM image_cache WHERE filename IS NOT NULL")
    db_filenames = set(row[0] for row in cur.fetchall())

    if not db_filenames:
        print("[INFO] No filenames in DB yet.")
        return

    # Filter list file
    with open(FILELIST_PATH, "r", encoding="utf-8") as f:
        paths = [line.strip() for line in f if line.strip()]

    remaining = [p for p in paths if os.path.basename(p) not in db_filenames]

    removed_count = len(paths) - len(remaining)
    if removed_count > 0:
        print(f"[CLEANUP] Removed {removed_count} already-known filenames from filelist.")

    # Write back only remaining
    with open(FILELIST_PATH, "w", encoding="utf-8") as f:
        for path in remaining:
            f.write(path + "\n")

def backfill_filenames(cur):
    """Backfill missing/different filenames using hash lookup."""
    updated_count = 0
    skipped_count = 0
    missing_count = 0
    
    count = 0

    for path in iter_file_list(FILELIST_PATH):
        if not os.path.exists(path):
            print(f"[SKIP] Missing file: {path}")
            missing_count += 1
            continue

        hash_val = compute_hash(path)
        filename = os.path.basename(path)

        cur.execute("SELECT filename FROM image_cache WHERE hash = %s", (hash_val,))
        row = cur.fetchone()
        

        if not row:
            if count > 999:
                print(f"[MISSING] No DB entry for {count} images")
                count = 0
            else:
                count+=1
            continue

        db_filename = row[0]
        if not db_filename or db_filename != filename:
            cur.execute(
                "UPDATE image_cache SET filename = %s WHERE hash = %s",
                (filename, hash_val),
            )
            updated_count += 1
            print(f"[UPDATED] {hash_val} → {filename} (was {db_filename})")
        else:
            skipped_count += 1

    print(f"[DONE] Updated {updated_count}, skipped {skipped_count}, missing {missing_count}")

def sync_filenames():
    conn = connect_db()
    cur = conn.cursor()

    cleanup_filelist(cur)      # Step 1: remove known filenames
    backfill_filenames(cur)    # Step 2: process remaining

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    sync_filenames()
