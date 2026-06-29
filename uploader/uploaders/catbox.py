import requests
from loggingUtils import logger


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
