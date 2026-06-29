import hashlib
import logging
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
import yaml

# --- Load config.yml ---
current_dir = os.path.dirname(__file__)
config_path = os.path.abspath(
    os.path.join(current_dir, "..", "masonryBoard_v2", "config.yml")
)
with open(config_path, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

tableName = config["tableName"]
directories = config.get("directories", [])
if not directories:
    raise ValueError("No 'directories' key found in config.yml")

# --- Connect to DB ---
def connect_db():
    return psycopg2.connect(
        dbname="boards",
        user="postgres",
        password="password",
        host="localhost",
    )

conn = connect_db()
cur = conn.cursor()

# --- Ensure 'filename' column exists ---
cur.execute(
    f"""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='{tableName}' AND column_name='filename'
        ) THEN
            ALTER TABLE {tableName} ADD COLUMN filename TEXT UNIQUE;
        END IF;
    END;
    $$;
"""
)
conn.commit()

# --- Process files in parallel ---
def hash_file(file: Path):
    try:
        with open(file, "rb") as f:
            data = f.read()
        file_hash = hashlib.md5(data).hexdigest()
        return file_hash, file.name
    except Exception as e:
        logging.warning(f"Failed to process file: {file} → {e}")
        return None

def collect_all_files():
    files = []
    for path in directories:
        p = Path(path)
        if not p.exists():
            logging.debug(f"Skipping missing path: {path}")
            continue
        files.extend([f for f in p.rglob("*") if f.is_file()])
    return files

def process_files(files):
    updates = 0
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(hash_file, f): f for f in files}

        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            if not result:
                continue

            file_hash, filename = result

            # Check if hash has filename set already
            cur.execute(
                f"SELECT filename FROM {tableName} WHERE hash = %s", (file_hash,)
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                continue  # hash already has filename

            # Check if filename is already in use
            cur.execute(
                f"SELECT 1 FROM {tableName} WHERE filename = %s", (filename,)
            )
            if cur.fetchone():
                logging.info(f"Skipping {filename} — already used by another hash.")
                continue  # Skip filename conflict

            # Update with filename
            cur.execute(
                f"UPDATE {tableName} SET filename = %s WHERE hash = %s",
                (filename, file_hash),
            )
            updates += 1

            if updates % 100 == 0:
                conn.commit()
                logging.info(f"Committed {updates} updates so far...")

    conn.commit()
    logging.info(f"Finished. Total updated rows: {updates}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    all_files = collect_all_files()
    print(f"Total files found: {len(all_files)}")
    process_files(all_files)
    cur.close()
    conn.close()
