from foldermanifest import read_manifest, save_manifest
from hashing import get_existing_hashes, compute_md5
from uploaders.imgchest import upload_folder_to_imgchest
from uploaders.catbox import upload_to_catbox

BATCH_SIZE = 1000  # filelist batch size
MINI_BATCH_SIZE = 20  # upload mini-batch size

total_uploaded = 0
total_failed = 0

from loggingUtils import logger

import os


def process_once(cur, duplicate_report):
    global total_uploaded, total_failed

    manifest = read_manifest()

    folders = manifest.get("folders", [])

    if not folders:
        return False

    batch = folders[:BATCH_SIZE]
    remainder = folders[BATCH_SIZE:]

    failed_folders = []
    uploaded_folders = 0

    for folder in batch:

        folder_path = folder["path"]
        folder_files = folder["files"]

        logger.info(
            f"[FOLDER] Processing {folder_path} " f"({len(folder_files)} files)"
        )

        path_hashes = {}

        for path in folder_files:
            if os.path.exists(path):
                try:
                    path_hashes[path] = compute_md5(path)
                except Exception as e:
                    logger.error(f"[HASH ERROR] {path}: {e}")

        existing_hashes = get_existing_hashes(cur, list(path_hashes.values()))

        paths_to_upload = [
            path
            for path, hash_val in path_hashes.items()
            if hash_val not in existing_hashes
        ]

        if not paths_to_upload:
            logger.info(f"[SKIP] Folder already uploaded: {folder_path}")
            uploaded_folders += 1
            continue

        try:

            posts = upload_folder_to_imgchest(
                os.path.basename(folder_path), paths_to_upload
            )

            image_index = 0

            for sequence, post in enumerate(posts, start=1):

                save_imgchest_post(
                    cur, folder_path, post["post_id"], sequence, len(post["links"])
                )

                for link in post["links"]:

                    path = paths_to_upload[image_index]
                    image_index += 1

                    hash_val = path_hashes[path]
                    filename = os.path.basename(path)

                    cur.execute(
                        """
                        SELECT hash
                        FROM public.image_cache
                        WHERE filename = %s
                        """,
                        (filename,),
                    )

                    row = cur.fetchone()

                    allow_filename = True

                    if row and row[0] != hash_val:
                        allow_filename = False
                        duplicate_report.append((filename, path))

                    save_link(cur, hash_val, link, filename if allow_filename else None)

                    total_uploaded += 1

            cur.connection.commit()

            uploaded_folders += 1

            logger.info(f"[FOLDER OK] {folder_path}")

        except Exception as e:

            logger.warning(f"[FOLDER FAIL] {folder_path}: {e}")

            failed_folders.append(folder)

            total_failed += len(paths_to_upload)

            cur.connection.rollback()

    new_manifest = {"folders": failed_folders + remainder}

    write_manifest(FILELIST_PATH, new_manifest)

    logger.info(
        f"[BATCH DONE] "
        f"Folders Uploaded={uploaded_folders} "
        f"Folders Failed={len(failed_folders)} "
        f"Remaining={len(new_manifest['folders'])} "
        f"Total Images Uploaded={total_uploaded}"
    )

    return True
