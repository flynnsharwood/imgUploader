import hashlib
import logging
from pathlib import Path

import psycopg2
import yaml

# --- Load config.yml ---
with open("config.yml", "r", encoding="utf-8") as f:
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


# --- Process files ---
def process_directory(path: str):
    p = Path(path)
    if not p.exists():
        logging.debug(f"Skipping missing path: {path}")
        return

    for file in p.rglob("*"):
        if not file.is_file():
            continue

        try:
            with open(file, "rb") as f:
                data = f.read()
            file_hash = hashlib.md5(data).hexdigest()
            filename = file.name

            # Check if this hash exists in DB
            cur.execute(f"SELECT link FROM {tableName} WHERE hash = %s", (file_hash,))
            result = cur.fetchone()
            if result:
                # Backfill filename
                cur.execute(
                    f"UPDATE {tableName} SET filename = %s WHERE hash = %s",
                    (filename, file_hash),
                )
                logging.debug(f"Updated: {filename} → {file_hash}")
            else:
                logging.info(f"Not in DB, skipping: {filename}")

        except Exception as e:
            logging.info(f"Error processing {file}: {e}")

    conn.commit()


# --- Run on each directory ---
for dir_path in directories:
    print(f"\n--- Processing: {dir_path} ---")
    process_directory(dir_path)

cur.close()
conn.close()
