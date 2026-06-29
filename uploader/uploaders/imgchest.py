import requests
import os
import mimetypes

from arguments import HEADERS
from loggingUtils import logger


def upload_imgchest_post(folder_name, image_paths):
    """
    Uploads ONE ImgChest post.

    image_paths must contain <= 20 files.

    Returns:
    {
        "post_id": "...",
        "links": [...]
    }
    """

    if len(image_paths) > 20:
        raise ValueError(
            f"ImgChest post cannot contain more than 20 images ({len(image_paths)} supplied)"
        )

    files = []

    for image_path in image_paths:
        with open(image_path, "rb") as f:
            filename = os.path.basename(image_path)

            mime = (
                mimetypes.guess_type(filename)[0]
                or "application/octet-stream"
            )

            files.append(
                (
                    "images[]",
                    (
                        filename,
                        f.read(),
                        mime
                    )
                )
            )

    resp = requests.post(
        "https://api.imgchest.com/v1/post",
        headers=HEADERS,
        files=files,
        data={
            "title": folder_name
        },
        timeout=300
    )

    resp.raise_for_status()

    post_id = resp.json()["data"]["id"]

    info = requests.get(
        f"https://api.imgchest.com/v1/post/{post_id}",
        headers=HEADERS,
        timeout=120
    )

    info.raise_for_status()

    image_list = info.json()["data"]["images"]

    if len(image_list) != len(image_paths):
        raise RuntimeError(
            f"Expected {len(image_paths)} images, got {len(image_list)}"
        )

    return {
        "post_id": post_id,
        "links": [
            img["link"]
            for img in image_list
        ]
    }


from loggingUtils import logger


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def upload_folder_to_imgchest(folder_name, image_paths):
    """
    Upload entire folder.

    Splits folder into 20-image ImgChest posts.

    Returns:

    [
        {
            "post_id": "...",
            "links": [...]
        },
        ...
    ]
    """

    posts = []

    total_batches = (
        len(image_paths) + 19
    ) // 20

    for batch_number, batch in enumerate(
        chunked(image_paths, 20),
        start=1
    ):

        logger.info(
            f"[IMGCHEST] "
            f"{folder_name} "
            f"batch {batch_number}/{total_batches} "
            f"({len(batch)} images)"
        )

        post = upload_imgchest_post(
            folder_name,
            batch
        )

        posts.append(post)

    return posts