
def upload_images(folder_name, image_paths):
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

    data = {"title": os.path.basename(folder_name)}
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
