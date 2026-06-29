import time
from datetime import date
from loggingUtils import logger

from db import create_table_if_not_exists, connect_db, connect_aiven

from processing import process_once

from foldermanifest import build_manifest, save_manifest

from arguments import args

BATCH_SIZE = 1000         # filelist batch size
MINI_BATCH_SIZE = 20      # upload mini-batch size

total_uploaded = 0
total_failed = 0

def run():

    if args.yml:
        folders = build_manifest()
        save_manifest(folders)
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