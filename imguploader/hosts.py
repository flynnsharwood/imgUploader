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


def user_agent() -> str:
    """An honest client name. files.catbox.moe drops connections from the default
    ``python-requests`` agent; naming the tool is enough (never pretend to be a browser)."""
    from imguploader import __version__

    return f"imguploader/{__version__} (+https://github.com/flynnsharwood/imgUploader)"


def new_session() -> requests.Session:
    session = requests.Session()
    session.headers["User-Agent"] = user_agent()
    return session


class HostError(Exception):
    """An upload failed; the next folder may still work."""


class StopUploads(HostError):
    """Every further upload would fail too, so the run should stop."""


class AuthError(StopUploads):
    """Bad token or storage quota exceeded (HTTP 401/403)."""

    def __init__(self, msg: str, status: int | None = None):
        super().__init__(msg)
        self.status = status


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
    account: str | None = None


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
        raise AuthError(msg + " (check the API key and the account's storage quota)", resp.status_code)
    raise HostError(msg)


class ImgChest:
    def __init__(self, token: str, session: requests.Session | None = None, timeout: int = 300):
        if not token:
            raise AuthError("IMG_CHEST_API_KEY is not set")
        self.session = session or new_session()
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
    RETRIES = 3  # catbox sometimes answers 200 with an empty body, or 5xx; those are retried

    def __init__(self, userhash: str | None = None, session: requests.Session | None = None, sleep=None):
        import time

        self.userhash = userhash
        self.session = session or new_session()
        self.sleep = sleep or time.sleep

    def upload(self, path: str) -> str:
        for attempt in range(1, self.RETRIES + 1):
            try:
                return self._upload_once(path)
            except RateLimited:
                raise
            except HostError as e:
                transient = getattr(e, "transient", False)
                if not transient or attempt == self.RETRIES:
                    raise
                logger.debug("catbox attempt %d failed (%s), retrying", attempt, e)
                self.sleep(5 * attempt)
        raise AssertionError("unreachable")

    def _upload_once(self, path: str) -> str:
        data = {"reqtype": "fileupload"}
        if self.userhash:
            data["userhash"] = self.userhash
        with open(path, "rb") as f:
            try:
                resp = self.session.post(
                    CATBOX_API, data=data, files={"fileToUpload": (os.path.basename(path), f)}, timeout=120
                )
            except requests.RequestException as e:
                raise _transient(HostError(f"catbox: {e}")) from e
        if resp.status_code == 429:
            raise RateLimited(f"catbox: HTTP 429 {resp.text[:200]}", _retry_after(resp, resp.text))
        text = resp.text.strip()
        if resp.ok and text.startswith("https://"):
            return text
        error = HostError(f"catbox: HTTP {resp.status_code} {text[:200] or '(empty reply)'}")
        if resp.status_code >= 500 or (resp.ok and not text):
            _transient(error)
        raise error


def _transient(error: HostError) -> HostError:
    error.transient = True
    return error
