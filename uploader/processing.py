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