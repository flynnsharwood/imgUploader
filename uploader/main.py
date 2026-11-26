import time
from loggingUtils import logger

from db import create_table_if_not_exists, connect_db

from processing import process_once

from foldermanifest import build_manifest, save_manifest

from arguments import args
import sys
import os


def run():
    manifest = build_manifest()
    save_manifest(manifest)
    # print(manifest)
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
