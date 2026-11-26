"""Image hosts: imgchest (posts of up to 20 images) and catbox (single files)."""

from __future__ import annotations

import logging
import mimetypes
import os
import re
from contextlib import ExitStack
from dataclasses import dataclass

import requests

logger = logging.getLogger(__name__)

IMGCHEST_API = "https://api.imgchest.com/v1"
CATBOX_API = "https://catbox.moe/user/api.php"
MAX_IMAGES_PER_POST = 20


class HostError(Exception):
    """An upload failed; the next folder may still work."""


class StopUploads(HostError):
    """Every further upload would fail too, so the run should stop."""


class AuthError(StopUploads):
    """Bad token or storage quota exceeded (HTTP 401/403)."""


class RateLimited(StopUploads):
    """Too many uploads for now (HTTP 429); ``retry_after`` is in seconds."""

    def __init__(self, msg: str, retry_after: int):
        super().__init__(msg)
        self.retry_after = retry_after


def _retry_after(resp, detail: str) -> int:
    """Seconds to wait: the Retry-After header, else "try again in N minutes", else an hour."""
    header = resp.headers.get("Retry-After", "") if getattr(resp, "headers", None) else ""
    if header.isdigit():
        return int(header)
    m = re.search(r"in (\d+) minute", detail or "")
    return int(m.group(1)) * 60 if m else 3600


@dataclass
class Post:
    id: str
    links: list[str]


def _check(resp: requests.Response, what: str) -> None:
    if resp.ok:
        return
    try:
        detail = resp.json().get("error") or resp.json().get("message")
    except ValueError:
        detail = resp.text[:200]
    msg = f"{what}: HTTP {resp.status_code} {detail or ''}".strip()
    if resp.status_code == 429:
        raise RateLimited(msg, _retry_after(resp, str(detail)))
    if resp.status_code in (401, 403):
        raise AuthError(msg + " (check IMG_CHEST_API_KEY and your storage quota)")
    raise HostError(msg)


class ImgChest:
    def __init__(self, token: str, session: requests.Session | None = None, timeout: int = 300):
        if not token:
            raise AuthError("IMG_CHEST_API_KEY is not set")
        self.session = session or requests.Session()
        self.session.headers["Authorization"] = f"Bearer {token}"
        self.timeout = timeout

    def create_post(self, title: str, paths: list[str]) -> Post:
        """Upload ``paths`` as one post; links come back in the same order as ``paths``."""
        if not 0 < len(paths) <= MAX_IMAGES_PER_POST:
            raise ValueError(f"a post holds 1-{MAX_IMAGES_PER_POST} images, got {len(paths)}")
        with ExitStack() as stack:
            files = [
                (
                    "images[]",
                    (
                        os.path.basename(p),
                        stack.enter_context(open(p, "rb")),
                        mimetypes.guess_type(p)[0] or "application/octet-stream",
                    ),
                )
                for p in paths
            ]
            try:
                resp = self.session.post(
                    f"{IMGCHEST_API}/post", files=files, data={"title": title}, timeout=self.timeout
                )
            except requests.RequestException as e:
                raise HostError(f"imgchest upload: {e}") from e
        _check(resp, "imgchest upload")
        post_id = resp.json()["data"]["id"]

        # The upload response does not list the images reliably, so read the post back.
        try:
            info = self.session.get(f"{IMGCHEST_API}/post/{post_id}", timeout=120)
        except requests.RequestException as e:
            raise HostError(f"imgchest post {post_id}: {e}") from e
        _check(info, f"imgchest post {post_id}")
        images = info.json()["data"]["images"]
        if len(images) != len(paths):
            raise HostError(f"imgchest post {post_id} has {len(images)} images, expected {len(paths)}")
        return Post(post_id, [img["link"] for img in images])


class Catbox:
    def __init__(self, userhash: str | None = None, session: requests.Session | None = None):
        self.userhash = userhash
        self.session = session or requests.Session()

    def upload(self, path: str) -> str:
        data = {"reqtype": "fileupload"}
        if self.userhash:
            data["userhash"] = self.userhash
        with open(path, "rb") as f:
            try:
                resp = self.session.post(
                    CATBOX_API, data=data, files={"fileToUpload": (os.path.basename(path), f)}, timeout=120
                )
            except requests.RequestException as e:
                raise HostError(f"catbox: {e}") from e
        if not resp.ok or not resp.text.startswith("https://"):
            raise HostError(f"catbox: HTTP {resp.status_code} {resp.text[:200]}")
        return resp.text.strip()
